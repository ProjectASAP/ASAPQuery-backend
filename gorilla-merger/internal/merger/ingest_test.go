package merger

import (
	"bytes"
	"compress/gzip"
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type sample struct {
	t int64
	v float64
}

// makeFragment XOR-encodes the given samples into a gorilla.Fragment, mirroring
// asap_edge's StreamingFragmentEncoder (chunkenc.NewXORChunk + appender).
func makeFragment(t *testing.T, metric string, attrs map[string]string, source string, samples []sample) gorilla.Fragment {
	t.Helper()
	c := chunkenc.NewXORChunk()
	app, err := c.Appender()
	if err != nil {
		t.Fatalf("xor appender: %v", err)
	}
	minT, maxT := int64(math.MaxInt64), int64(math.MinInt64)
	for _, s := range samples {
		app.Append(s.t, s.v)
		if s.t < minT {
			minT = s.t
		}
		if s.t > maxT {
			maxT = s.t
		}
	}
	return gorilla.Fragment{
		MetricName: metric,
		Attributes: attrs,
		MinTime:    minT,
		MaxTime:    maxT,
		Count:      len(samples),
		Encoding:   "xor",
		Data:       append([]byte(nil), c.Bytes()...),
		Source:     source,
	}
}

// readBack queries the tsdb.DB for one series and returns its samples in time
// order.
func readBack(t *testing.T, s *Storage, want labels.Labels) []sample {
	t.Helper()
	q, err := s.DB.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("querier: %v", err)
	}
	defer q.Close()

	matchers := make([]*labels.Matcher, 0, want.Len())
	want.Range(func(l labels.Label) {
		matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, l.Name, l.Value))
	})

	ss := q.Select(context.Background(), false, nil, matchers...)
	var out []sample
	count := 0
	for ss.Next() {
		count++
		series := ss.At()
		it := series.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			tt, vv := it.At()
			out = append(out, sample{t: tt, v: vv})
		}
		if it.Err() != nil {
			t.Fatalf("series iterator: %v", it.Err())
		}
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("select err: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 matching series, got %d", count)
	}
	return out
}

func TestIngestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	storage, err := OpenStorage(StorageOptions{Dir: dir})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = storage.Close() })

	ext := labels.FromStrings("merger", "test-merger")
	storage.SetExternalLabels(ext)

	// Use timestamps near "now" so they fall inside the writable head window.
	base := time.Now().UnixMilli()
	fragA := makeFragment(t, "http_requests_total",
		map[string]string{"job": "api", "instance": "a"}, "agent-1",
		[]sample{{base, 1}, {base + 1000, 2}, {base + 2000, 3}})
	fragB := makeFragment(t, "http_requests_total",
		map[string]string{"job": "api", "instance": "b"}, "agent-2",
		[]sample{{base, 10}, {base + 1000, 20}})

	frame := gorilla.EncodeFragmentBatch([]gorilla.Fragment{fragA, fragB})

	// POST through the real HTTP handler (gzip-encoded body to exercise the
	// gunzip path).
	ingester := NewIngester(storage, nil)
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
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Series A: __name__ + its attrs + external labels.
	wantA := labels.FromStrings(
		labels.MetricName, "http_requests_total",
		"job", "api", "instance", "a", "merger", "test-merger")
	gotA := readBack(t, storage, wantA)
	wantSamplesA := []sample{{base, 1}, {base + 1000, 2}, {base + 2000, 3}}
	assertSamples(t, "A", gotA, wantSamplesA)

	// Series B: a distinct instance is a distinct series.
	wantB := labels.FromStrings(
		labels.MetricName, "http_requests_total",
		"job", "api", "instance", "b", "merger", "test-merger")
	gotB := readBack(t, storage, wantB)
	wantSamplesB := []sample{{base, 10}, {base + 1000, 20}}
	assertSamples(t, "B", gotB, wantSamplesB)
}

func TestIngestBadBodyReturns400(t *testing.T) {
	dir := t.TempDir()
	storage, err := OpenStorage(StorageOptions{Dir: dir})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = storage.Close() })

	ingester := NewIngester(storage, nil)
	srv := httptest.NewServer(http.HandlerFunc(ingester.HandleIngest))
	t.Cleanup(srv.Close)

	resp, err := http.Post(srv.URL+"/ingest/gorilla", "application/octet-stream",
		bytes.NewReader([]byte("not a valid ASAPFRG1 frame")))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed body, got %d", resp.StatusCode)
	}
}

func TestLabelsForExternalWins(t *testing.T) {
	ext := labels.FromStrings("merger", "m1", "shared", "external")
	got := labelsFor("metric", map[string]string{"shared": "frag", "job": "x"}, ext)
	want := labels.FromStrings(
		labels.MetricName, "metric",
		"job", "x", "merger", "m1", "shared", "external")
	if labels.Compare(got, want) != 0 {
		t.Fatalf("labelsFor mismatch:\n got  %s\n want %s", got.String(), want.String())
	}
}

func assertSamples(t *testing.T, name string, got, want []sample) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("series %s: got %d samples, want %d (%v vs %v)", name, len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].t != want[i].t || got[i].v != want[i].v {
			t.Fatalf("series %s sample %d: got (%d,%g), want (%d,%g)", name, i, got[i].t, got[i].v, want[i].t, want[i].v)
		}
	}
}
