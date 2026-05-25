package merger

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ProjectASAP/asap-gorilla-go/coldpart"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// writePartBytes serializes a cold part (via coldpart.WritePart) over the given
// series and returns the raw object bytes — the SYNTHETIC stand-in for what the
// agent-encode side will eventually POST.
func writePartBytes(t *testing.T, blockStartMs, blockEndMs int64, series []coldpart.Series) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := coldpart.WritePart(&buf, blockStartMs, blockEndMs, series, coldpart.Options{}); err != nil {
		t.Fatalf("WritePart: %v", err)
	}
	return buf.Bytes()
}

// coldSeries builds a coldpart.Series at 1s cadence starting at startMs.
func coldSeries(ls labels.Labels, startMs int64, vals ...float64) coldpart.Series {
	samples := make([]coldpart.Sample, len(vals))
	for i, v := range vals {
		samples[i] = coldpart.Sample{T: startMs + int64(i)*1000, V: v}
	}
	return coldpart.Series{Labels: ls, Samples: samples}
}

// matcher builds an =, !=, =~, !~ matcher or fails the test.
func matcher(t *testing.T, ty labels.MatchType, n, v string) *labels.Matcher {
	t.Helper()
	m, err := labels.NewMatcher(ty, n, v)
	if err != nil {
		t.Fatalf("NewMatcher: %v", err)
	}
	return m
}

// xorChunkSamples iterates a ColdSeries' chunks back to (t,v) samples.
func xorChunkSamples(t *testing.T, cs ColdSeries) []sample {
	t.Helper()
	var out []sample
	for _, cc := range cs.Chunks {
		if cc.Chunk.Encoding() != chunkenc.EncXOR {
			t.Fatalf("cold chunk encoding = %v, want XOR", cc.Chunk.Encoding())
		}
		it := cc.Chunk.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			tt, vv := it.At()
			out = append(out, sample{t: tt, v: vv})
		}
		if it.Err() != nil {
			t.Fatalf("cold chunk iterator: %v", it.Err())
		}
	}
	return out
}

// findCold returns the ColdSeries whose labels equal want, or fails.
func findCold(t *testing.T, got []ColdSeries, want labels.Labels) ColdSeries {
	t.Helper()
	for _, cs := range got {
		if labels.Compare(cs.Labels, want) == 0 {
			return cs
		}
	}
	t.Fatalf("cold series %s not found in result", want.String())
	return ColdSeries{}
}

func mustPut(t *testing.T, store *ColdPartStore, partBytes []byte) string {
	t.Helper()
	key, err := store.Put(context.Background(), partBytes)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	return key
}

// ---------------------------------------------------------------------------
// write-no-decode store
// ---------------------------------------------------------------------------

// TestColdPartStorePutVerbatim asserts Put stores the part BYTE-FOR-BYTE (no
// decode/re-encode), under the cold prefix, and registers it in the manifest.
func TestColdPartStorePutVerbatim(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	store := NewColdPartStore(bkt, nil)

	partBytes := writePartBytes(t, 1000, 2000, []coldpart.Series{
		coldSeries(labels.FromStrings(labels.MetricName, "cpu", "core", "0"), 1000, 1, 2, 3),
	})
	key := mustPut(t, store, partBytes)

	if !isPartKey(key) {
		t.Fatalf("assigned key %q is not a recognized part key", key)
	}
	stored := bkt.Objects()[key]
	if !bytes.Equal(stored, partBytes) {
		t.Fatalf("stored bytes differ from input: stored %d bytes, input %d bytes", len(stored), len(partBytes))
	}
	if store.NumParts() != 1 {
		t.Fatalf("manifest tracks %d parts, want 1", store.NumParts())
	}
}

// TestColdPartStorePutIdempotent asserts re-POSTing identical bytes yields the
// same key and a single manifest entry.
func TestColdPartStorePutIdempotent(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	store := NewColdPartStore(bkt, nil)

	partBytes := writePartBytes(t, 0, 1000, []coldpart.Series{
		coldSeries(labels.FromStrings(labels.MetricName, "m"), 0, 1, 2),
	})
	k1 := mustPut(t, store, partBytes)
	k2 := mustPut(t, store, partBytes)
	if k1 != k2 {
		t.Fatalf("idempotent Put gave different keys: %q vs %q", k1, k2)
	}
	if store.NumParts() != 1 {
		t.Fatalf("manifest tracks %d parts after duplicate Put, want 1", store.NumParts())
	}
	if n := len(bkt.Objects()); n != 1 {
		t.Fatalf("bucket has %d objects after duplicate Put, want 1", n)
	}
}

// TestColdPartStorePutRejectsInvalid asserts the write path validates (OpenPart)
// before storing: a corrupt/non-part body is rejected and nothing is written.
func TestColdPartStorePutRejectsInvalid(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	store := NewColdPartStore(bkt, nil)

	if _, err := store.Put(context.Background(), []byte("not a part")); err == nil {
		t.Fatalf("Put accepted an invalid part")
	}
	if n := len(bkt.Objects()); n != 0 {
		t.Fatalf("invalid Put wrote %d objects, want 0", n)
	}

	// A part with a flipped byte (bad crc) is also rejected.
	good := writePartBytes(t, 0, 1000, []coldpart.Series{
		coldSeries(labels.FromStrings(labels.MetricName, "m"), 0, 1),
	})
	corrupt := append([]byte(nil), good...)
	corrupt[len(corrupt)/2] ^= 0xFF
	if _, err := store.Put(context.Background(), corrupt); err == nil {
		t.Fatalf("Put accepted a crc-corrupt part")
	}
}

// TestColdPartStoreReload asserts a fresh store rediscovers parts already in the
// bucket (manifest rebuilt from object-store contents on startup).
func TestColdPartStoreReload(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	writer := NewColdPartStore(bkt, nil)
	mustPut(t, writer, writePartBytes(t, 0, 1000, []coldpart.Series{
		coldSeries(labels.FromStrings(labels.MetricName, "a"), 0, 1, 2),
	}))
	mustPut(t, writer, writePartBytes(t, 2000, 3000, []coldpart.Series{
		coldSeries(labels.FromStrings(labels.MetricName, "b"), 2000, 3, 4),
	}))

	// A second, empty store over the same bucket discovers both via Reload.
	reloaded := NewColdPartStore(bkt, nil)
	if reloaded.NumParts() != 0 {
		t.Fatalf("pre-reload manifest = %d, want 0", reloaded.NumParts())
	}
	if err := reloaded.Reload(context.Background()); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if reloaded.NumParts() != 2 {
		t.Fatalf("post-reload manifest = %d, want 2", reloaded.NumParts())
	}
}

// ---------------------------------------------------------------------------
// decode-on-read query path
// ---------------------------------------------------------------------------

// TestColdQueryRoundTrip is the headline round-trip: WritePart gauge/counter/
// float series -> store -> decode-on-read query with a matcher + a sub-window
// -> assert exactly the in-window samples come back as iterable XOR chunks.
func TestColdQueryRoundTrip(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	store := NewColdPartStore(bkt, nil)

	// base..base+9000 (10 points @1s). gauge (small ints), counter (monotonic),
	// and a high-precision float series (exercises the Gorilla-XOR codec path).
	const base = int64(1_700_000_000_000)
	gauge := coldSeries(labels.FromStrings(labels.MetricName, "temp", "kind", "gauge"),
		base, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29)
	counter := coldSeries(labels.FromStrings(labels.MetricName, "reqs_total", "kind", "counter"),
		base, 0, 5, 11, 18, 26, 35, 45, 56, 68, 81)
	fl := coldSeries(labels.FromStrings(labels.MetricName, "ratio", "kind", "float"),
		base, 0.1, 0.123456789, 3.14159265358979, 2.718281828, 1e-12, -42.5, 99.99, 0.0, 1.0, 1234.56789)
	partBytes := writePartBytes(t, base, base+9000, []coldpart.Series{gauge, counter, fl})
	mustPut(t, store, partBytes)

	q := NewColdQuerier(store)

	// Sub-window [base+2000, base+5000] inclusive -> indices 2..5 (4 points).
	mint, maxt := base+2000, base+5000
	res, err := q.Series(context.Background(),
		[]*labels.Matcher{matcher(t, labels.MatchEqual, labels.MetricName, "temp")}, mint, maxt)
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("matcher temp: got %d series, want 1", len(res))
	}
	gaugeWant := []sample{{base + 2000, 22}, {base + 3000, 23}, {base + 4000, 24}, {base + 5000, 25}}
	assertSamples(t, "gauge-window", xorChunkSamples(t, res[0]), gaugeWant)

	// All three series in a wider window, verifying counter + float round-trip
	// bit-exactly through the XOR re-encode.
	resAll, err := q.Series(context.Background(),
		[]*labels.Matcher{matcher(t, labels.MatchRegexp, "kind", "gauge|counter|float")}, base, base+9000)
	if err != nil {
		t.Fatalf("Series(all): %v", err)
	}
	if len(resAll) != 3 {
		t.Fatalf("regex kind: got %d series, want 3", len(resAll))
	}
	csCounter := findCold(t, resAll, labels.FromStrings(labels.MetricName, "reqs_total", "kind", "counter"))
	assertSamples(t, "counter", xorChunkSamples(t, csCounter),
		[]sample{{base, 0}, {base + 1000, 5}, {base + 2000, 11}, {base + 3000, 18}, {base + 4000, 26},
			{base + 5000, 35}, {base + 6000, 45}, {base + 7000, 56}, {base + 8000, 68}, {base + 9000, 81}})
	csFloat := findCold(t, resAll, labels.FromStrings(labels.MetricName, "ratio", "kind", "float"))
	assertSamples(t, "float", xorChunkSamples(t, csFloat),
		[]sample{{base, 0.1}, {base + 1000, 0.123456789}, {base + 2000, 3.14159265358979},
			{base + 3000, 2.718281828}, {base + 4000, 1e-12}, {base + 5000, -42.5},
			{base + 6000, 99.99}, {base + 7000, 0.0}, {base + 8000, 1.0}, {base + 9000, 1234.56789}})
}

// TestColdQueryClippingExclusive asserts the query clips to the inclusive window
// and drops series whose samples all fall outside it.
func TestColdQueryClippingExclusive(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	store := NewColdPartStore(bkt, nil)

	const base = int64(100_000)
	mustPut(t, store, writePartBytes(t, base, base+4000, []coldpart.Series{
		coldSeries(labels.FromStrings(labels.MetricName, "g"), base, 0, 1, 2, 3, 4),
	}))
	q := NewColdQuerier(store)

	// Window strictly inside: [base+1000, base+3000] -> 3 samples.
	res, err := q.Series(context.Background(),
		[]*labels.Matcher{matcher(t, labels.MatchEqual, labels.MetricName, "g")}, base+1000, base+3000)
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	assertSamples(t, "clip", xorChunkSamples(t, res[0]),
		[]sample{{base + 1000, 1}, {base + 2000, 2}, {base + 3000, 3}})

	// Window before the series entirely -> no series (even though the PART block
	// range nominally overlaps, no in-window samples survive clipping).
	resBefore, err := q.Series(context.Background(),
		[]*labels.Matcher{matcher(t, labels.MatchEqual, labels.MetricName, "g")}, base-5000, base-1)
	if err != nil {
		t.Fatalf("Series(before): %v", err)
	}
	if len(resBefore) != 0 {
		t.Fatalf("window before series: got %d series, want 0", len(resBefore))
	}
}

// TestColdQueryMatcherFiltering asserts =, !=, =~, !~ select the right series and
// a non-matching matcher yields nothing.
func TestColdQueryMatcherFiltering(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	store := NewColdPartStore(bkt, nil)

	const base = int64(0)
	mustPut(t, store, writePartBytes(t, base, base+1000, []coldpart.Series{
		coldSeries(labels.FromStrings(labels.MetricName, "http", "job", "api"), base, 1, 2),
		coldSeries(labels.FromStrings(labels.MetricName, "http", "job", "web"), base, 3, 4),
		coldSeries(labels.FromStrings(labels.MetricName, "mem", "job", "api"), base, 5, 6),
	}))
	q := NewColdQuerier(store)
	ctx := context.Background()

	cases := []struct {
		name     string
		matchers []*labels.Matcher
		want     int
	}{
		{"eq-name", []*labels.Matcher{matcher(t, labels.MatchEqual, labels.MetricName, "http")}, 2},
		{"eq-name-and-job", []*labels.Matcher{
			matcher(t, labels.MatchEqual, labels.MetricName, "http"),
			matcher(t, labels.MatchEqual, "job", "api"),
		}, 1},
		{"neq-job", []*labels.Matcher{
			matcher(t, labels.MatchEqual, labels.MetricName, "http"),
			matcher(t, labels.MatchNotEqual, "job", "web"),
		}, 1},
		{"regex-job", []*labels.Matcher{matcher(t, labels.MatchRegexp, "job", "a.*")}, 2},
		{"not-regex-name", []*labels.Matcher{matcher(t, labels.MatchNotRegexp, labels.MetricName, "mem")}, 2},
		{"no-match", []*labels.Matcher{matcher(t, labels.MatchEqual, labels.MetricName, "absent")}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := q.Series(ctx, c.matchers, base, base+1000)
			if err != nil {
				t.Fatalf("Series: %v", err)
			}
			if len(res) != c.want {
				t.Fatalf("%s: got %d series, want %d", c.name, len(res), c.want)
			}
		})
	}
}

// TestColdQueryMultipleOverlappingParts asserts a query that straddles two parts
// (same logical series spread across adjacent blocks) returns a contribution
// from each, and a part whose block range is outside the window is skipped.
func TestColdQueryMultipleOverlappingParts(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	store := NewColdPartStore(bkt, nil)

	ls := labels.FromStrings(labels.MetricName, "split")
	// Part 1: [0, 4000]. Part 2: [5000, 9000]. Part 3 (far future): [100000,..].
	mustPut(t, store, writePartBytes(t, 0, 4000, []coldpart.Series{coldSeries(ls, 0, 0, 1, 2, 3, 4)}))
	mustPut(t, store, writePartBytes(t, 5000, 9000, []coldpart.Series{coldSeries(ls, 5000, 5, 6, 7, 8, 9)}))
	mustPut(t, store, writePartBytes(t, 100000, 104000, []coldpart.Series{coldSeries(ls, 100000, 99)}))

	q := NewColdQuerier(store)
	// Window [2000, 7000] overlaps parts 1 and 2 only.
	res, err := q.Series(context.Background(),
		[]*labels.Matcher{matcher(t, labels.MatchEqual, labels.MetricName, "split")}, 2000, 7000)
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("straddling query: got %d series contributions, want 2", len(res))
	}
	// Concatenate both parts' in-window samples (parts returned in block order).
	var all []sample
	for _, cs := range res {
		all = append(all, xorChunkSamples(t, cs)...)
	}
	assertSamples(t, "straddle", all,
		[]sample{{2000, 2}, {3000, 3}, {4000, 4}, {5000, 5}, {6000, 6}, {7000, 7}})
}

// TestColdQueryEmptyStore asserts a query against a store with no parts (or a
// nil querier) returns no series and no error.
func TestColdQueryEmptyStore(t *testing.T) {
	store := NewColdPartStore(objstore.NewInMemBucket(), nil)
	q := NewColdQuerier(store)
	res, err := q.Series(context.Background(),
		[]*labels.Matcher{matcher(t, labels.MatchEqual, labels.MetricName, "x")}, 0, 1000)
	if err != nil {
		t.Fatalf("Series(empty): %v", err)
	}
	if len(res) != 0 {
		t.Fatalf("empty store: got %d series, want 0", len(res))
	}

	var nilQ *ColdQuerier
	if res, err := nilQ.Series(context.Background(), nil, 0, 1000); err != nil || res != nil {
		t.Fatalf("nil querier: got (%v, %v), want (nil, nil)", res, err)
	}
}

// ---------------------------------------------------------------------------
// HTTP put + StoreAPI union
// ---------------------------------------------------------------------------

// TestColdPartHTTPPut drives the write path through the real HTTP handler and
// asserts the part is stored and queryable.
func TestColdPartHTTPPut(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	store := NewColdPartStore(bkt, nil)
	srv := httptest.NewServer(http.HandlerFunc(store.HandlePut))
	t.Cleanup(srv.Close)

	partBytes := writePartBytes(t, 0, 1000, []coldpart.Series{
		coldSeries(labels.FromStrings(labels.MetricName, "posted"), 0, 7, 8),
	})
	resp, err := http.Post(srv.URL+"/ingest/coldpart", "application/octet-stream", bytes.NewReader(partBytes))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post status = %d, want 200", resp.StatusCode)
	}
	if store.NumParts() != 1 {
		t.Fatalf("after HTTP put manifest = %d, want 1", store.NumParts())
	}

	// An invalid body is rejected with 400 and stores nothing.
	bad, err := http.Post(srv.URL+"/ingest/coldpart", "application/octet-stream", bytes.NewReader([]byte("garbage")))
	if err != nil {
		t.Fatalf("post(bad): %v", err)
	}
	defer bad.Body.Close()
	if bad.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid post status = %d, want 400", bad.StatusCode)
	}
}

// TestStoreAPIUnionsColdAndTSDB is the end-to-end union proof: the same custom
// StoreServer streams both the open-window tsdb series AND the decode-on-read
// cold series for one Series RPC, with external labels appended to each.
func TestStoreAPIUnionsColdAndTSDB(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStorage(StorageOptions{Dir: dir})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	ext := labels.FromStrings("merger", "m1")
	st.SetExternalLabels(ext)

	// Hot (tsdb) series via the normal ingest path.
	const base = int64(1_700_000_100_000)
	ingestFragments(t, st, makeFragment(t, "metric", map[string]string{"src": "hot"}, "agent-1",
		[]sample{{base, 1}, {base + 1000, 2}}))

	// Cold series via a synthetic part in the cold store.
	bkt := objstore.NewInMemBucket()
	coldStore := NewColdPartStore(bkt, nil)
	mustPut(t, coldStore, writePartBytes(t, base, base+1000, []coldpart.Series{
		coldSeries(labels.FromStrings(labels.MetricName, "metric", "src", "cold"), base, 100, 200),
	}))

	cs := newCustomStore(st.DB, ext, nil)
	cs.setColdQuerier(NewColdQuerier(coldStore))

	req := &storepb.SeriesRequest{
		MinTime: base - 60_000,
		MaxTime: base + 60_000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: labels.MetricName, Value: "metric"},
		},
	}
	fss := &fakeSeriesServer{ctx: context.Background()}
	if err := cs.Series(req, fss); err != nil {
		t.Fatalf("Series: %v", err)
	}

	type got struct {
		lset    labels.Labels
		samples []sample
	}
	var results []got
	for _, r := range fss.responses {
		series := r.GetSeries()
		if series == nil {
			continue
		}
		results = append(results, got{
			lset:    labelsOf(t, series.Labels),
			samples: samplesFromChunks(t, series.Chunks),
		})
	}
	if len(results) != 2 {
		t.Fatalf("union: got %d series, want 2 (1 hot tsdb + 1 cold)", len(results))
	}

	wantHot := labels.FromStrings(labels.MetricName, "metric", "src", "hot", "merger", "m1")
	wantCold := labels.FromStrings(labels.MetricName, "metric", "src", "cold", "merger", "m1")
	var sawHot, sawCold bool
	for _, r := range results {
		switch {
		case labels.Compare(r.lset, wantHot) == 0:
			sawHot = true
			assertSamples(t, "hot", r.samples, []sample{{base, 1}, {base + 1000, 2}})
		case labels.Compare(r.lset, wantCold) == 0:
			sawCold = true
			assertSamples(t, "cold", r.samples, []sample{{base, 100}, {base + 1000, 200}})
		default:
			t.Fatalf("unexpected series labels: %s", r.lset.String())
		}
	}
	if !sawHot || !sawCold {
		t.Fatalf("union missing a tier: sawHot=%v sawCold=%v", sawHot, sawCold)
	}
}
