package merger

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
)

// fakeSeriesServer is an in-process storepb.Store_SeriesServer that collects
// every SeriesResponse the custom store emits. Only Send and Context are
// exercised by customStore.Series; the rest of grpc.ServerStream is embedded as
// a nil interface (never called) so we satisfy the interface without a real
// gRPC connection.
type fakeSeriesServer struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*storepb.SeriesResponse
}

func (f *fakeSeriesServer) Send(r *storepb.SeriesResponse) error {
	f.responses = append(f.responses, r)
	return nil
}

func (f *fakeSeriesServer) Context() context.Context {
	if f.ctx == nil {
		return context.Background()
	}
	return f.ctx
}

// ingestFragments POSTs the given fragments through the real HTTP ingest
// handler (gzip body), asserting a 200.
func ingestFragments(t *testing.T, s *Storage, frags ...gorilla.Fragment) {
	t.Helper()
	frame := gorilla.EncodeFragmentBatch(frags)

	ingester := NewIngester(s, nil)
	srv := httptest.NewServer(http.HandlerFunc(ingester.HandleIngest))
	t.Cleanup(srv.Close)

	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	if _, werr := gw.Write(frame); werr != nil {
		t.Fatalf("gzip write: %v", werr)
	}
	if cerr := gw.Close(); cerr != nil {
		t.Fatalf("gzip close: %v", cerr)
	}

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/ingest/gorilla", &gzBuf)
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ingest: expected 200, got %d", resp.StatusCode)
	}
}

// labelsOf converts a response's ZLabels back to a sorted prom labels set by
// COPYING (so the test never depends on the unsafe ZLabelsToPromLabels path).
func labelsOf(t *testing.T, zls []labelpb.ZLabel) labels.Labels {
	t.Helper()
	b := labels.NewBuilder(labels.EmptyLabels())
	for _, z := range zls {
		b.Set(z.Name, z.Value)
	}
	return b.Labels()
}

// samplesFromChunks decodes all XOR chunks in a response into time-ordered
// samples, asserting the chunk encoding is XOR.
func samplesFromChunks(t *testing.T, chks []storepb.AggrChunk) []sample {
	t.Helper()
	var out []sample
	for _, c := range chks {
		if c.Raw == nil {
			t.Fatalf("chunk has no Raw payload")
		}
		if c.Raw.Type != storepb.Chunk_XOR {
			t.Fatalf("chunk type = %v, want XOR", c.Raw.Type)
		}
		chk, err := chunkenc.FromData(chunkenc.EncXOR, c.Raw.Data)
		if err != nil {
			t.Fatalf("decode xor chunk: %v", err)
		}
		it := chk.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			tt, vv := it.At()
			out = append(out, sample{t: tt, v: vv})
		}
		if it.Err() != nil {
			t.Fatalf("chunk iterator: %v", it.Err())
		}
	}
	return out
}

// TestCustomStoreSeriesRoundTrip is the key proof for the crash fix: it drives
// the custom Series RPC over real ingested data with a fake stream server and
// asserts (1) NO crash/OOM, (2) labels = metric labels + external labels once
// (no dups), (3) the returned chunks decode back to the ingested samples.
func TestCustomStoreSeriesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStorage(StorageOptions{Dir: dir})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ext := labels.FromStrings("merger", "test-merger", "tier", "cold")
	st.SetExternalLabels(ext)

	base := time.Now().UnixMilli()
	fragA := makeFragment(t, "http_requests_total",
		map[string]string{"job": "api", "instance": "a"}, "agent-1",
		[]sample{{base, 1}, {base + 1000, 2}, {base + 2000, 3}})
	fragB := makeFragment(t, "http_requests_total",
		map[string]string{"job": "api", "instance": "b"}, "agent-2",
		[]sample{{base, 10}, {base + 1000, 20}})
	ingestFragments(t, st, fragA, fragB)

	cs := newCustomStore(st.DB, ext, nil)

	req := &storepb.SeriesRequest{
		MinTime: base - 60_000,
		MaxTime: base + 60_000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: labels.MetricName, Value: "http_requests_total"},
		},
	}
	fss := &fakeSeriesServer{ctx: context.Background()}

	// THE CRASH TEST: with thanos store.TSDBStore this Series call OOMs inside
	// ReAllocZLabelsStrings. The custom store must complete cleanly.
	if err := cs.Series(req, fss); err != nil {
		t.Fatalf("Series returned error: %v", err)
	}

	type got struct {
		lset    labels.Labels
		samples []sample
	}
	var results []got
	for _, r := range fss.responses {
		series := r.GetSeries()
		if series == nil {
			// Tolerate warnings/hints but there should be none here.
			continue
		}
		results = append(results, got{
			lset:    labelsOf(t, series.Labels),
			samples: samplesFromChunks(t, series.Chunks),
		})
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 series, got %d", len(results))
	}

	// Series must be sorted by label set (Querier sorted=true + consistent ext
	// append). Verify ordering and exact label sets (metric+attrs+ext, deduped).
	wantA := labels.FromStrings(
		labels.MetricName, "http_requests_total",
		"job", "api", "instance", "a",
		"merger", "test-merger", "tier", "cold")
	wantB := labels.FromStrings(
		labels.MetricName, "http_requests_total",
		"job", "api", "instance", "b",
		"merger", "test-merger", "tier", "cold")

	if labels.Compare(results[0].lset, wantA) != 0 {
		t.Fatalf("series[0] labels:\n got  %s\n want %s", results[0].lset.String(), wantA.String())
	}
	if labels.Compare(results[1].lset, wantB) != 0 {
		t.Fatalf("series[1] labels:\n got  %s\n want %s", results[1].lset.String(), wantB.String())
	}
	if labels.Compare(results[0].lset, results[1].lset) >= 0 {
		t.Fatalf("series not sorted: %s !< %s", results[0].lset.String(), results[1].lset.String())
	}

	// Verify no DUPLICATE label names crept in (each name appears once).
	for i, r := range results {
		seen := map[string]int{}
		r.lset.Range(func(l labels.Label) { seen[l.Name]++ })
		for n, c := range seen {
			if c != 1 {
				t.Fatalf("series[%d] label %q appears %d times (want 1)", i, n, c)
			}
		}
	}

	assertSamples(t, "A", results[0].samples, []sample{{base, 1}, {base + 1000, 2}, {base + 2000, 3}})
	assertSamples(t, "B", results[1].samples, []sample{{base, 10}, {base + 1000, 20}})
}

// TestCustomStoreSeriesSkipChunks verifies the SkipChunks path returns labels
// (with external labels appended once) and NO chunks, without crashing.
func TestCustomStoreSeriesSkipChunks(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStorage(StorageOptions{Dir: dir})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ext := labels.FromStrings("merger", "test-merger")
	st.SetExternalLabels(ext)

	base := time.Now().UnixMilli()
	ingestFragments(t, st, makeFragment(t, "up",
		map[string]string{"job": "api"}, "agent-1",
		[]sample{{base, 1}, {base + 1000, 1}}))

	cs := newCustomStore(st.DB, ext, nil)
	req := &storepb.SeriesRequest{
		MinTime:    base - 60_000,
		MaxTime:    base + 60_000,
		SkipChunks: true,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: labels.MetricName, Value: "up"},
		},
	}
	fss := &fakeSeriesServer{ctx: context.Background()}
	if err := cs.Series(req, fss); err != nil {
		t.Fatalf("Series(SkipChunks) error: %v", err)
	}

	var n int
	for _, r := range fss.responses {
		series := r.GetSeries()
		if series == nil {
			continue
		}
		n++
		if len(series.Chunks) != 0 {
			t.Fatalf("SkipChunks: expected 0 chunks, got %d", len(series.Chunks))
		}
		want := labels.FromStrings(labels.MetricName, "up", "job", "api", "merger", "test-merger")
		if labels.Compare(labelsOf(t, series.Labels), want) != 0 {
			t.Fatalf("SkipChunks labels:\n got  %s\n want %s", labelsOf(t, series.Labels).String(), want.String())
		}
	}
	if n != 1 {
		t.Fatalf("SkipChunks: expected 1 series, got %d", n)
	}
}

// TestCustomStoreSeriesExternalLabelGate verifies that a matcher on an external
// label whose value DOESN'T match the merger's value yields no series, and one
// that DOES match is satisfied without being passed to the querier.
func TestCustomStoreSeriesExternalLabelGate(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStorage(StorageOptions{Dir: dir})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ext := labels.FromStrings("merger", "m1")
	st.SetExternalLabels(ext)

	base := time.Now().UnixMilli()
	ingestFragments(t, st, makeFragment(t, "up", map[string]string{"job": "api"}, "agent-1",
		[]sample{{base, 1}}))

	cs := newCustomStore(st.DB, ext, nil)

	// Non-matching external value -> 0 series.
	reqMiss := &storepb.SeriesRequest{
		MinTime: base - 60_000, MaxTime: base + 60_000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: labels.MetricName, Value: "up"},
			{Type: storepb.LabelMatcher_EQ, Name: "merger", Value: "other"},
		},
	}
	fssMiss := &fakeSeriesServer{ctx: context.Background()}
	if err := cs.Series(reqMiss, fssMiss); err != nil {
		t.Fatalf("Series(miss) error: %v", err)
	}
	if got := countSeries(fssMiss.responses); got != 0 {
		t.Fatalf("non-matching external matcher: expected 0 series, got %d", got)
	}

	// Matching external value -> 1 series.
	reqHit := &storepb.SeriesRequest{
		MinTime: base - 60_000, MaxTime: base + 60_000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: labels.MetricName, Value: "up"},
			{Type: storepb.LabelMatcher_EQ, Name: "merger", Value: "m1"},
		},
	}
	fssHit := &fakeSeriesServer{ctx: context.Background()}
	if err := cs.Series(reqHit, fssHit); err != nil {
		t.Fatalf("Series(hit) error: %v", err)
	}
	if got := countSeries(fssHit.responses); got != 1 {
		t.Fatalf("matching external matcher: expected 1 series, got %d", got)
	}
}

// TestCustomStoreLabelNamesValues verifies LabelNames/LabelValues merge the
// external labels and return the stored ones.
func TestCustomStoreLabelNamesValues(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStorage(StorageOptions{Dir: dir})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ext := labels.FromStrings("merger", "m1")
	st.SetExternalLabels(ext)

	base := time.Now().UnixMilli()
	ingestFragments(t, st,
		makeFragment(t, "up", map[string]string{"job": "api"}, "a", []sample{{base, 1}}),
		makeFragment(t, "up", map[string]string{"job": "web"}, "b", []sample{{base, 1}}))

	cs := newCustomStore(st.DB, ext, nil)
	ctx := context.Background()

	ln, err := cs.LabelNames(ctx, &storepb.LabelNamesRequest{Start: base - 60_000, End: base + 60_000})
	if err != nil {
		t.Fatalf("LabelNames: %v", err)
	}
	gotNames := append([]string(nil), ln.Names...)
	sort.Strings(gotNames)
	wantNames := []string{labels.MetricName, "job", "merger"}
	if len(gotNames) != len(wantNames) {
		t.Fatalf("LabelNames = %v, want %v", gotNames, wantNames)
	}
	for i := range wantNames {
		if gotNames[i] != wantNames[i] {
			t.Fatalf("LabelNames = %v, want %v", gotNames, wantNames)
		}
	}

	// LabelValues for a stored label.
	lv, err := cs.LabelValues(ctx, &storepb.LabelValuesRequest{Label: "job", Start: base - 60_000, End: base + 60_000})
	if err != nil {
		t.Fatalf("LabelValues(job): %v", err)
	}
	gotJob := append([]string(nil), lv.Values...)
	sort.Strings(gotJob)
	if len(gotJob) != 2 || gotJob[0] != "api" || gotJob[1] != "web" {
		t.Fatalf("LabelValues(job) = %v, want [api web]", gotJob)
	}

	// LabelValues for the external label returns the merger's value.
	lvExt, err := cs.LabelValues(ctx, &storepb.LabelValuesRequest{Label: "merger", Start: base - 60_000, End: base + 60_000})
	if err != nil {
		t.Fatalf("LabelValues(merger): %v", err)
	}
	if len(lvExt.Values) != 1 || lvExt.Values[0] != "m1" {
		t.Fatalf("LabelValues(merger) = %v, want [m1]", lvExt.Values)
	}
}

func countSeries(responses []*storepb.SeriesResponse) int {
	n := 0
	for _, r := range responses {
		if r.GetSeries() != nil {
			n++
		}
	}
	return n
}
