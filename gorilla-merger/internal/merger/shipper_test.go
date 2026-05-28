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

// TestShipperUploadsBlock drives the full write path: ingest fragments (decode-
// free) -> flush a closed window into a directly-built PENDING block on disk ->
// compactor promotes it (re-chunked) into the SHIPPED dir -> shipper (watching
// ONLY the shipped dir) Sync -> assert exactly one block (chunks + index +
// meta.json with the Thanos thanos{} section) lands in the (in-memory) bucket.
func TestShipperUploadsBlock(t *testing.T) {
	dir := t.TempDir()
	storage, err := OpenStorage(StorageOptions{Dir: dir})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = storage.Close() })

	ext := labels.FromStrings("merger", "ship-test")
	storage.SetExternalLabels(ext)

	// Lay down samples in a window well in the past so it is clearly closable.
	const twoHoursMs = int64(2 * 60 * 60 * 1000)
	now := time.Now().UnixMilli()
	oldBase := (now/twoHoursMs - 4) * twoHoursMs

	var frags []gorilla.Fragment
	for i := 0; i < 20; i++ {
		ts := oldBase + int64(i)*1000
		frags = append(frags, makeFragment(t, "cpu_seconds_total",
			map[string]string{"core": "0"}, "agent-1",
			[]sample{{ts, float64(i)}}))
	}

	frame := gorilla.EncodeFragmentBatch(frags)
	ingester := NewIngester(storage.Manager, nil)
	if _, ierr := ingester.IngestBatch(context.Background(), frame); ierr != nil {
		t.Fatalf("ingest: %v", ierr)
	}

	// Flush the closed window into a directly-built PENDING block on disk.
	built, ferr := storage.Manager.FlushAll()
	if ferr != nil {
		t.Fatalf("flush: %v", ferr)
	}
	if built == 0 {
		t.Fatalf("expected at least one block built from the closed window, got 0")
	}
	if len(storage.BlockStore().pendingBlockDirs()) == 0 {
		t.Fatalf("expected at least one on-disk pending block, got 0")
	}

	// Promote the pending block into the shipped dir (re-chunked). With
	// MinBlocks=1 even a lone pending block is promoted so it ships.
	comp, err := NewCompactor(CompactorOptions{Store: storage.BlockStore(), MinBlocks: 1})
	if err != nil {
		t.Fatalf("new compactor: %v", err)
	}
	if err := comp.CompactOnce(context.Background()); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if len(ulidDirs(t, storage.ShippedDir())) == 0 {
		t.Fatalf("expected at least one shipped block after compaction, got 0")
	}

	// Wire a shipper against an in-memory bucket and sync once. It watches ONLY
	// the shipped dir, so it uploads exactly the compacted block.
	bkt := objstore.NewInMemBucket()
	runner, err := newShipperRunnerWithBucket(bkt, ShipperOptions{
		Dir:            storage.ShippedDir(),
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
