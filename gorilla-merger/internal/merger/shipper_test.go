package merger

import (
	"context"
	"strings"
	"testing"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
)

// TestShipperUploadsBlock drives the full write path: ingest fragments ->
// tsdb.DB -> force a 2h block cut on disk -> shipper Sync -> assert exactly one
// block (chunks + index + meta.json with the Thanos thanos{} section) lands in
// the (in-memory) bucket.
func TestShipperUploadsBlock(t *testing.T) {
	dir := t.TempDir()
	storage, err := OpenStorage(StorageOptions{Dir: dir})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = storage.Close() })

	ext := labels.FromStrings("merger", "ship-test")
	storage.SetExternalLabels(ext)

	// Lay down samples across two adjacent 2h windows, all in the past, so that
	// db.Compact cuts the older, now-immutable window into an on-disk block.
	const twoHoursMs = int64(2 * 60 * 60 * 1000)
	now := time.Now().UnixMilli()
	// Align to a 2h boundary well in the past (4 windows back).
	oldBase := (now/twoHoursMs - 4) * twoHoursMs

	var frags []gorilla.Fragment
	for i := 0; i < 20; i++ {
		ts := oldBase + int64(i)*1000
		frags = append(frags, makeFragment(t, "cpu_seconds_total",
			map[string]string{"core": "0"}, "agent-1",
			[]sample{{ts, float64(i)}}))
	}
	// A couple of samples in the *current* window keep the head non-empty so
	// the older window is clearly compactable.
	frags = append(frags, makeFragment(t, "cpu_seconds_total",
		map[string]string{"core": "0"}, "agent-1",
		[]sample{{now, 999}}))

	frame := gorilla.EncodeFragmentBatch(frags)
	ingester := NewIngester(storage, nil)
	if _, ierr := ingester.IngestBatch(context.Background(), frame); ierr != nil {
		t.Fatalf("ingest: %v", ierr)
	}

	// Force the head to cut the old window into a persistent block.
	if cerr := storage.DB.Compact(context.Background()); cerr != nil {
		t.Fatalf("compact: %v", cerr)
	}
	if len(storage.DB.Blocks()) == 0 {
		t.Fatalf("expected at least one on-disk block after compaction, got 0")
	}

	// Wire a shipper against an in-memory bucket and sync once.
	bkt := objstore.NewInMemBucket()
	runner, err := newShipperRunnerWithBucket(bkt, ShipperOptions{
		Dir:            dir,
		ExternalLabels: ext,
	})
	if err != nil {
		t.Fatalf("shipper runner: %v", err)
	}
	runner.syncOnce(context.Background())

	objs := bkt.Objects()
	var metaCount, chunkCount, indexCount int
	for name := range objs {
		switch {
		case strings.HasSuffix(name, "/meta.json"):
			metaCount++
		case strings.Contains(name, "/chunks/"):
			chunkCount++
		case strings.HasSuffix(name, "/index"):
			indexCount++
		}
	}
	if metaCount < 1 || chunkCount < 1 || indexCount < 1 {
		t.Fatalf("expected a full block uploaded (>=1 meta/chunks/index); got meta=%d chunks=%d index=%d; objects=%v",
			metaCount, chunkCount, indexCount, keysOf(objs))
	}

	// The uploaded meta.json must carry the Thanos thanos{} section (this is
	// what makes the block store-gateway-queriable).
	var metaName string
	for name := range objs {
		if strings.HasSuffix(name, "/meta.json") {
			metaName = name
			break
		}
	}
	body := objs[metaName]
	if !strings.Contains(string(body), "\"thanos\"") {
		t.Fatalf("uploaded meta.json missing thanos{} section: %s", string(body))
	}
	if !strings.Contains(string(body), "ship-test") {
		t.Fatalf("uploaded meta.json missing external label value: %s", string(body))
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
