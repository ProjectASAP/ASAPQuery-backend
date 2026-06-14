package merger

// merger_bench_test.go — end-to-end CPU/IO benchmark for the merger's four
// pipeline stages, so the Fig 11 "merger CPU/IO" gap is a measured number
// rather than a hand-wave. Each benchmark is self-contained: the synthetic
// workload (1k series x ~120 samples/window, gzipped ASAPFRG1 frames built via
// the asap-gorilla-go fragment codec the same way the edge emits them) is built
// in a NON-timed setup region, and only the stage under test runs in the timed
// loop (b.ResetTimer / b.StopTimer / b.StartTimer).
//
// Stages measured (one benchmark each):
//   - BenchmarkIngestFragmentBatch — the decode-free ingest hot path: gunzip the
//     wire frame + validate ASAPFRG1 structure + append raw chunks to the window
//     buffer + WAL fsync (Manager.Append). SetBytes => MB/s of the ingest path.
//   - BenchmarkWindowToPendingBlock — close a window: stitch the buffered raw XOR
//     chunks directly into a pending TSDB block (buildBlock: chunks+index+
//     tombstones+meta.json+atomic rename, incl. the chunk-segment fsync).
//   - BenchmarkCompactRechunk — compact the pending L1 blocks into a shipped,
//     re-chunked (~120 samples/chunk) L2 block (Compactor.CompactOnce ->
//     rechunkPopulator). The pending sources are rebuilt in a StopTimer region
//     each iteration because CompactOnce consumes them.
//   - BenchmarkShipBlock — ship one compacted block to the in-memory object
//     store (one PUT set per block: chunks + index + meta.json with thanos{}).
//
// Run:
//   GOPRIVATE='github.com/ProjectASAP/*' go test ./internal/merger/ -run '^$' \
//     -bench 'Ingest|WindowToPending|CompactRechunk|ShipBlock' -benchmem -benchtime=2x
//
// NOTE: /tmp on this machine is spinning disk, so the fsync-bearing stages
// (ingest WAL, block build, compact, ship) are IO-bound — their ns/op is
// dominated by Sync, not CPU; read allocs/op + B/op for the CPU/mem story.
// benchtime=2x => representative, not stability-grade.
//
// === MEASURED RESULTS ===
// Machine: INTEL(R) XEON(R) GOLD 5512U, linux/amd64, /tmp = spinning disk.
// Workload: 1000 series x 120 samples/series = 120,000 samples/frame; the
// decompressed ASAPFRG1 frame is ~0.87 MB (gzipped on the wire). benchtime=2x.
//
//   go test ./internal/merger/ -run '^$' \
//     -bench 'IngestFragmentBatch|WindowToPendingBlock|CompactRechunk|ShipBlock' \
//     -benchmem -benchtime=2x
//
//   benchmark                       ns/op        MB/s    B/op        allocs/op
//   BenchmarkIngestFragmentBatch    15,270,864   58.16   8,096,596   15,026
//   BenchmarkWindowToPendingBlock   24,059,805     -     34,464,860    5,542
//   BenchmarkCompactRechunk         48,317,116     -     42,768,616   92,990
//   BenchmarkShipBlock               2,737,326     -      4,680,164      377
//
// Reading it (Fig 11 "merger CPU/IO" per 120k-sample window):
//   - Ingest is the per-POST hot path: ~15 ms/frame, 58 MB/s, decode-free (only
//     ~15k allocs for 120k samples buffered as raw chunks + the WAL fsync).
//   - Window->block and Compact+rechunk are the amortized background stages; the
//     compact path's ~93k allocs/op is the deliberate decode+re-encode of the
//     ~120-sample re-chunk, which runs OFF the ingest path.
//   - Ship is one block's PUT set to object storage: ~2.7 ms, 377 allocs.
//   The ns/op of the fsync/disk-bearing stages is IO-bound on this spinning-disk
//   box; allocs/op + B/op are the portable CPU/mem story.

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
)

// The shared Fig-11 workload: numSeries series, each with one chunk of perChunk
// samples, packed into a single ASAPFRG1 frame (one "POST").
const (
	benchNumSeries = 1000
	benchPerChunk  = 120
)

// gzipFrame compresses a raw ASAPFRG1 frame the way the edge ships it
// (Content-Encoding: gzip).
func gzipFrame(b *testing.B, raw []byte) []byte {
	b.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(raw); err != nil {
		b.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

// gunzip decompresses one gzipped frame back to the raw ASAPFRG1 bytes (the
// merger's HTTP handler gunzip step).
func gunzip(b *testing.B, gz []byte) []byte {
	b.Helper()
	gr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		b.Fatal(err)
	}
	raw, err := io.ReadAll(gr)
	if err != nil {
		b.Fatal(err)
	}
	if err := gr.Close(); err != nil {
		b.Fatal(err)
	}
	return raw
}

// benchManager opens a Manager with a wide (24h) window so nothing auto-closes
// mid-benchmark; the ingest bench then measures only the append+WAL path.
func benchManager(b *testing.B) *Manager {
	b.Helper()
	dir := b.TempDir()
	const dayMs = int64(24 * 60 * 60 * 1000)
	st, err := OpenStorage(StorageOptions{Dir: dir, WindowMs: dayMs, ReorderGraceMs: 0})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = st.Close() })
	return st.Manager
}

// ----------------------------------------------------------------------------
// Stage 1 — Ingest: gunzip + ASAPFRG1 frame validate + windowBuffer append +
// WAL fsync. The decode-free ingest hot path (Manager.Append), the per-POST
// cost on the merger's critical path. SetBytes => MB/s of ingest.
// ----------------------------------------------------------------------------

func BenchmarkIngestFragmentBatch(b *testing.B) {
	corpus := buildBenchCorpus(benchNumSeries, 1, benchPerChunk)
	raw := corpus.frames[0]
	gz := gzipFrame(b, raw)
	mgr := benchManager(b)

	b.SetBytes(int64(len(raw))) // MB/s over the decompressed frame
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frame := gunzip(b, gz)
		if _, err := mgr.Append(frame); err != nil {
			b.Fatal(err)
		}
	}
}

// ----------------------------------------------------------------------------
// Stage 2 — Window -> pending block: stitch the buffered raw XOR chunks for a
// closed window directly into an on-disk TSDB block (buildBlock). Includes the
// chunk-segment fsync on Close + the index/tombstones/meta writes + atomic
// rename. No sample decode (the decode-free build).
// ----------------------------------------------------------------------------

func BenchmarkWindowToPendingBlock(b *testing.B) {
	corpus := buildBenchCorpus(benchNumSeries, 1, benchPerChunk)
	series := corpusToBufferedSeries(corpus) // bucketing is NOT the work we time
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		if _, err := buildBlock(dir, series); err != nil {
			b.Fatal(err)
		}
	}
}

// ----------------------------------------------------------------------------
// Stage 3 — Compact + re-chunk: fuse the pending L1 blocks into a single
// shipped, re-chunked (~120 samples/chunk) L2 block via the compactor's
// rechunkPopulator. CompactOnce consumes (removes) the pending sources, so they
// are rebuilt in a StopTimer region each iteration.
// ----------------------------------------------------------------------------

func BenchmarkCompactRechunk(b *testing.B) {
	corpus := buildBenchCorpus(benchNumSeries, 1, benchPerChunk)
	// Re-chunk only bites when there are several small chunks/series to fuse, so
	// split each series' 120 samples across a handful of tiny windows: build N
	// pending blocks that compaction then re-chunks back toward ~120/chunk.
	const nWindows = 5

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		st, _, shippedDir := fixedWindowStorageB(b, int64(benchPerChunk/nWindows)*1000, 0)
		buildPendingWindows(b, st, corpus, nWindows)
		comp, err := NewCompactor(CompactorOptions{
			Store:     st.BlockStore(),
			MinBlocks: 1,
			MaxSpanMs: int64(benchPerChunk) * 1000 << 20, // wide: fuse all windows
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		if err := comp.CompactOnce(context.Background()); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		if got := len(ulidDirsB(b, shippedDir)); got != 1 {
			b.Fatalf("expected 1 shipped block, got %d", got)
		}
		b.StartTimer()
	}
}

// ----------------------------------------------------------------------------
// Stage 4 — Ship one block to the in-memory object store: a single PUT set per
// block (chunks + index + meta.json with the thanos{} section). A fresh bucket
// per iteration so each measures exactly one block's upload.
// ----------------------------------------------------------------------------

func BenchmarkShipBlock(b *testing.B) {
	corpus := buildBenchCorpus(benchNumSeries, 1, benchPerChunk)
	// Build ONE shipped L2 block once, outside the timed loop.
	st, _, shippedDir := fixedWindowStorageB(b, int64(benchPerChunk)*1000, 0)
	buildPendingWindows(b, st, corpus, 1)
	comp, err := NewCompactor(CompactorOptions{Store: st.BlockStore(), MinBlocks: 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := comp.CompactOnce(context.Background()); err != nil {
		b.Fatal(err)
	}
	if got := len(ulidDirsB(b, shippedDir)); got != 1 {
		b.Fatalf("expected 1 shipped block to ship, got %d", got)
	}
	ext := labels.FromStrings("merger", "bench")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// The shipper persists its uploaded-set in <Dir>/thanos.shipper.json and
		// then skips already-shipped blocks. Remove that marker each iteration so
		// every iteration re-ships the one block — i.e. measures one block's
		// upload, the cost we want, against a fresh in-memory bucket.
		_ = os.Remove(filepath.Join(shippedDir, "thanos.shipper.json"))
		bkt := objstore.NewInMemBucket()
		runner, rerr := newShipperRunnerWithBucket(bkt, ShipperOptions{
			Dir:            shippedDir,
			ExternalLabels: ext,
		})
		if rerr != nil {
			b.Fatal(rerr)
		}
		b.StartTimer()

		// One Sync = one block's worth of PUTs to the bucket.
		if _, serr := runner.shipper.Sync(context.Background()); serr != nil {
			b.Fatal(serr)
		}

		b.StopTimer()
		if n := len(bkt.Objects()); n < 3 { // chunks + index + meta.json minimum
			b.Fatalf("expected a full block uploaded (>=3 objects), got %d", n)
		}
		_ = runner.Close()
		b.StartTimer()
	}
}

// ----------------------------------------------------------------------------
// *testing.B variants of the shared *testing.T helpers (the originals take a
// *testing.T, unusable from a benchmark) + the pending-block builder.
// ----------------------------------------------------------------------------

func fixedWindowStorageB(b *testing.B, windowMs, graceMs int64) (st *Storage, pendingDir, shippedDir string) {
	b.Helper()
	dir := b.TempDir()
	s, err := OpenStorage(StorageOptions{Dir: dir, WindowMs: windowMs, ReorderGraceMs: graceMs})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s, s.BlockStore().PendingDir(), s.BlockStore().ShippedDir()
}

func ulidDirsB(b *testing.B, dir string) []string {
	b.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		b.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, serr := os.Stat(filepath.Join(dir, e.Name(), metaFilenameConst)); serr != nil {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	return out
}

// buildPendingWindows ingests the corpus split across nWindows tiny windows and
// flushes them into nWindows pending L1 blocks, aligned to the compaction grid
// so they fuse into one shipped block. Each series' benchPerChunk samples are
// spread evenly across the windows.
func buildPendingWindows(b *testing.B, st *Storage, corpus benchCorpus, nWindows int) {
	b.Helper()
	per := corpus.perChunk / nWindows
	windowMs := int64(per) * 1000
	maxSpanMs := int64(corpus.perChunk) * 1000 << 20
	base := (int64(1_700_000_000_000) / maxSpanMs) * maxSpanMs

	for w := 0; w < nWindows; w++ {
		wStart := base + int64(w)*windowMs
		frags := make([]gorilla.Fragment, 0, corpus.numSeries)
		for s := 0; s < corpus.numSeries; s++ {
			lset := map[string]string{
				"job":      "node",
				"instance": fmt.Sprintf("host-%04d:9100", s),
			}
			samples := make([]sample, 0, per)
			val := float64(s)
			for k := 0; k < per; k++ {
				idx := w*per + k
				ts := wStart + int64(k)*(windowMs/int64(per))
				val += 1 + float64((idx*7)%3)*0.001
				samples = append(samples, sample{t: ts, v: val})
			}
			frags = append(frags, encodeFragment("node_cpu_seconds_total", lset, "agent-x", samples))
		}
		if _, err := st.Manager.Append(gorilla.EncodeFragmentBatch(frags)); err != nil {
			b.Fatalf("append window %d: %v", w, err)
		}
	}
	lastEnd := base + int64(nWindows)*windowMs
	built, err := st.Manager.FlushClosed(lastEnd + 1)
	if err != nil {
		b.Fatalf("flush closed: %v", err)
	}
	if built != nWindows {
		b.Fatalf("expected %d pending blocks, got %d", nWindows, built)
	}
}
