package merger

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// fixedWindowStorage opens a Storage whose window is small enough that adjacent
// fragment batches close into distinct blocks, and whose grace is zero so
// FlushClosed flushes them at a known "now". It returns the Storage plus the
// pending and shipped block dirs (per the pending/shipped split): per-window L1
// blocks land in pending; the compactor promotes them to shipped. Used by the
// compaction test.
func fixedWindowStorage(t *testing.T, windowMs, graceMs int64) (st *Storage, pendingDir, shippedDir string) {
	t.Helper()
	dir := t.TempDir()
	s, err := OpenStorage(StorageOptions{Dir: dir, WindowMs: windowMs, ReorderGraceMs: graceMs})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, s.BlockStore().PendingDir(), s.BlockStore().ShippedDir()
}

// countChunksAndSamples opens a block dir and returns, per series, the chunk
// count and per-chunk sample counts (decoding only for the assertion — this is
// the test, NOT the hot path).
func countChunksAndSamples(t *testing.T, blockDir string) (chunkCounts []int, sampleCounts []int) {
	t.Helper()
	b, err := tsdb.OpenBlock(nil, blockDir, nil, nil)
	if err != nil {
		t.Fatalf("open block %q: %v", blockDir, err)
	}
	defer b.Close()

	q, err := tsdb.NewBlockChunkQuerier(b, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("block chunk querier: %v", err)
	}
	defer q.Close()

	ss := q.Select(context.Background(), false, nil,
		labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))
	for ss.Next() {
		n := 0
		it := ss.At().Iterator(nil)
		for it.Next() {
			n++
			cit := it.At().Chunk.Iterator(nil)
			samples := 0
			for cit.Next() == chunkenc.ValFloat {
				samples++
			}
			sampleCounts = append(sampleCounts, samples)
		}
		if it.Err() != nil {
			t.Fatalf("chunk iter: %v", it.Err())
		}
		chunkCounts = append(chunkCounts, n)
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("select: %v", err)
	}
	return chunkCounts, sampleCounts
}

// managerOpts builds ManagerOptions with the pending/shipped split rooted under
// dir (pending blocks in <dir>/pending, shipped in <dir>/shipped). pendingDir
// is returned for the on-disk assertions the WAL tests make.
func managerOpts(t *testing.T, dir, walDir string, windowMs, graceMs int64) (ManagerOptions, string) {
	t.Helper()
	pendingDir := filepath.Join(dir, "pending")
	shippedDir := filepath.Join(dir, "shipped")
	return ManagerOptions{
		PendingDir:     pendingDir,
		ShippedDir:     shippedDir,
		WALDir:         walDir,
		WindowMs:       windowMs,
		ReorderGraceMs: graceMs,
	}, pendingDir
}

func ulidDirs(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
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

// TestBlockBuildIndexQueryable builds a block from two series with multiple
// chunks each and asserts the index is correct: both series queryable, samples
// in time order, and (b) the per-window block has one chunk per fragment (no
// re-chunk yet — that is the compactor's job).
func TestBlockBuildIndexQueryable(t *testing.T) {
	st, pendingDir, _ := fixedWindowStorage(t, defaultWindowMs, 0)

	// Two series, each fed by two fragments in the SAME window. With no
	// compaction the block should keep each fragment as its own chunk.
	base := (int64(1_700_000_000_000) / defaultWindowMs) * defaultWindowMs
	fragsA1 := makeFragment(t, "m", map[string]string{"s": "a"}, "ag", []sample{{base, 1}, {base + 1000, 2}})
	fragsA2 := makeFragment(t, "m", map[string]string{"s": "a"}, "ag", []sample{{base + 2000, 3}, {base + 3000, 4}})
	fragsB1 := makeFragment(t, "m", map[string]string{"s": "b"}, "ag", []sample{{base, 10}})

	if _, err := st.Manager.Append(gorilla.EncodeFragmentBatch([]gorilla.Fragment{fragsA1, fragsA2, fragsB1})); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := st.Manager.FlushAll(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	blocks := ulidDirs(t, pendingDir)
	if len(blocks) != 1 {
		t.Fatalf("expected exactly 1 pending block, got %d (%v)", len(blocks), blocks)
	}

	// Series A read back through the BlockStore: 4 samples in order.
	gotA := readBack(t, st, labels.FromStrings(labels.MetricName, "m", "s", "a"))
	assertSamples(t, "A", gotA, []sample{{base, 1}, {base + 1000, 2}, {base + 2000, 3}, {base + 3000, 4}})

	// Series A has two chunks (one per fragment) pre-compaction — proves the
	// raw chunks were stitched, not merged/re-encoded on the hot path.
	chunkCounts, _ := countChunksAndSamples(t, blocks[0])
	// Two series total.
	if len(chunkCounts) != 2 {
		t.Fatalf("expected 2 series in block, got %d", len(chunkCounts))
	}
	// One of them (series A) must have 2 chunks.
	saw2 := false
	for _, c := range chunkCounts {
		if c == 2 {
			saw2 = true
		}
	}
	if !saw2 {
		t.Fatalf("expected a series with 2 chunks (one per fragment), got chunk counts %v", chunkCounts)
	}
}

// TestCompactionMergesAndRechunks proves the merger-side ratio step: many small
// single-sample blocks for one series are compacted into a single block whose
// chunks are re-chunked toward Prometheus's ~120 samples/chunk target (so the
// many tiny chunks collapse into far fewer, larger chunks).
// TestCompactorRunCompactsBacklogOnStartup verifies Run() compacts an existing
// pending backlog immediately on startup, before the (here: 1h) interval fires —
// the restart-robustness fix so a merger that restarts more often than its
// compaction interval can't accumulate per-window blocks unbounded.
func TestCompactorRunCompactsBacklogOnStartup(t *testing.T) {
	const windowMs = int64(1000)
	st, pendingDir, shippedDir := fixedWindowStorage(t, windowMs, 0)
	const maxSpanMs = int64(1) << 40
	base := (int64(1_700_000_000_000) / maxSpanMs) * maxSpanMs
	for i := 0; i < 8; i++ {
		ts := base + int64(i)*windowMs
		frag := makeFragment(t, "m", map[string]string{"s": "x"}, "ag", []sample{{ts, float64(i)}})
		if _, err := st.Manager.Append(gorilla.EncodeFragmentBatch([]gorilla.Fragment{frag})); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if _, err := st.Manager.FlushAll(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(ulidDirs(t, pendingDir)) == 0 {
		t.Fatal("expected a pending backlog before Run")
	}

	// Hour-long interval: the only compaction that can fire within the test
	// window is the startup pass.
	comp, err := NewCompactor(CompactorOptions{
		Store:     st.BlockStore(),
		Interval:  time.Hour,
		MinBlocks: 1,
		MaxSpanMs: maxSpanMs,
	})
	if err != nil {
		t.Fatalf("new compactor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = comp.Run(ctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(ulidDirs(t, pendingDir)) == 0 && len(ulidDirs(t, shippedDir)) >= 1 {
			return // startup pass drained the backlog into shipped
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("startup compaction did not drain backlog: pending=%d shipped=%d",
		len(ulidDirs(t, pendingDir)), len(ulidDirs(t, shippedDir)))
}

func TestCompactionMergesAndRechunks(t *testing.T) {
	// Small window + zero grace so each batch closes into its own block.
	const windowMs = int64(1000)
	st, pendingDir, shippedDir := fixedWindowStorage(t, windowMs, 0)

	// The compactor groups blocks onto a fixed grid of width maxSpan (so
	// compacted blocks align to block boundaries, like Prometheus). Align base
	// to that grid so all 300 windows fall inside a SINGLE grid cell and fuse
	// into one block.
	const maxSpanMs = int64(1) << 40
	const nSamples = 150
	base := (int64(1_700_000_000_000) / maxSpanMs) * maxSpanMs
	for i := 0; i < nSamples; i++ {
		ts := base + int64(i)*windowMs // one per distinct window
		frag := makeFragment(t, "m", map[string]string{"s": "x"}, "ag", []sample{{ts, float64(i)}})
		if _, err := st.Manager.Append(gorilla.EncodeFragmentBatch([]gorilla.Fragment{frag})); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	// Flush ALL windows (FlushAll ignores grace) into individual blocks.
	if _, err := st.Manager.FlushAll(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	preBlocks := ulidDirs(t, pendingDir)
	if len(preBlocks) < nSamples/2 {
		t.Fatalf("expected many small per-window pending blocks before compaction, got %d", len(preBlocks))
	}

	// Pre-compaction: every block has a single 1-sample chunk for the series.
	var preChunks int
	for _, bd := range preBlocks {
		cc, _ := countChunksAndSamples(t, bd)
		for _, c := range cc {
			preChunks += c
		}
	}
	if preChunks < nSamples {
		t.Fatalf("expected >= %d tiny chunks pre-compaction, got %d", nSamples, preChunks)
	}

	// Compact: span wide enough to fuse all windows into one block.
	comp, err := NewCompactor(CompactorOptions{
		Store:     st.BlockStore(),
		MinBlocks: 2,
		MaxSpanMs: maxSpanMs,
	})
	if err != nil {
		t.Fatalf("new compactor: %v", err)
	}
	if err := comp.CompactOnce(context.Background()); err != nil {
		t.Fatalf("compact: %v", err)
	}

	postBlocks := ulidDirs(t, shippedDir)
	if len(postBlocks) != 1 {
		t.Fatalf("expected exactly 1 shipped block after compaction, got %d", len(postBlocks))
	}
	// The pending sources must have been removed after promotion.
	if rem := ulidDirs(t, pendingDir); len(rem) != 0 {
		t.Fatalf("expected pending dir empty after compaction, got %d", len(rem))
	}

	// Post-compaction: the 300 single-sample chunks must collapse into far
	// fewer chunks, each near the 120 samples/chunk target.
	chunkCounts, sampleCounts := countChunksAndSamples(t, postBlocks[0])
	if len(chunkCounts) != 1 {
		t.Fatalf("expected 1 series post-compaction, got %d", len(chunkCounts))
	}
	gotChunks := chunkCounts[0]
	if gotChunks >= nSamples {
		t.Fatalf("compaction did not re-chunk: %d chunks for %d samples (want far fewer)", gotChunks, nSamples)
	}
	// 300 samples at ~120/chunk -> ~3 chunks. Allow some slack.
	if gotChunks > 6 {
		t.Fatalf("re-chunk target ~120 samples/chunk not met: got %d chunks for %d samples", gotChunks, nSamples)
	}
	// Total samples preserved.
	total := 0
	biggest := 0
	for _, s := range sampleCounts {
		total += s
		if s > biggest {
			biggest = s
		}
	}
	if total != nSamples {
		t.Fatalf("sample count changed across compaction: got %d, want %d", total, nSamples)
	}
	if biggest < 100 {
		t.Fatalf("expected at least one chunk near the 120-sample target, biggest was %d", biggest)
	}

	// The compacted block must still be queryable end to end.
	got := readBack(t, st, labels.FromStrings(labels.MetricName, "m", "s", "x"))
	if len(got) != nSamples {
		t.Fatalf("readback after compaction: got %d samples, want %d", len(got), nSamples)
	}
}

// TestWALReplayReconstructsUnflushedWindows simulates a crash: fragments are
// accepted (WAL-logged + buffered) but NOT flushed, then the Storage is closed
// WITHOUT flushing and reopened. The WAL replay must re-buffer the fragments so
// a subsequent flush reconstructs the window and the data is queryable — all
// without re-decoding samples.
func TestWALReplayReconstructsUnflushedWindows(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	opts, pendingDir := managerOpts(t, dir, walDir, defaultWindowMs, 0)

	base := time.Now().UnixMilli()
	frag := makeFragment(t, "wal_metric", map[string]string{"k": "v"}, "ag",
		[]sample{{base, 7}, {base + 1000, 8}, {base + 2000, 9}})
	frame := gorilla.EncodeFragmentBatch([]gorilla.Fragment{frag})

	// (1) Open a manager, append (durably logs to WAL + buffers) but do NOT
	// flush, then drop the in-memory state WITHOUT FlushAll (simulated crash:
	// close the WAL + store handles directly, bypassing Manager.Close which
	// would flush).
	{
		mgr, err := NewManager(opts)
		if err != nil {
			t.Fatalf("manager 1: %v", err)
		}
		if _, err := mgr.Append(frame); err != nil {
			t.Fatalf("append: %v", err)
		}
		// Simulated crash: no FlushAll, no Checkpoint. Just drop handles.
		_ = mgr.WAL().Close()
		_ = mgr.Store().Close()
	}

	// No block should have been written.
	if dirs := ulidDirs(t, pendingDir); len(dirs) != 0 {
		t.Fatalf("expected no blocks before flush (crash before flush), got %d", len(dirs))
	}

	// (2) Reopen: WAL replay must re-buffer the un-flushed fragment. Flushing
	// then reconstructs the window into a block.
	mgr2, err := NewManager(opts)
	if err != nil {
		t.Fatalf("manager 2 (replay): %v", err)
	}
	st := &Storage{Manager: mgr2}
	t.Cleanup(func() { _ = st.Close() })

	if _, err := mgr2.FlushAll(); err != nil {
		t.Fatalf("flush after replay: %v", err)
	}
	if dirs := ulidDirs(t, pendingDir); len(dirs) != 1 {
		t.Fatalf("expected exactly 1 pending block after replay+flush, got %d", len(dirs))
	}

	got := readBack(t, st, labels.FromStrings(labels.MetricName, "wal_metric", "k", "v"))
	assertSamples(t, "wal", got, []sample{{base, 7}, {base + 1000, 8}, {base + 2000, 9}})
}

// TestWALReplayIdempotentAgainstPersistedBlocks proves replay does NOT rebuild a
// block for a window that was already flushed before a (simulated) crash: the
// WAL still holds the frame (no checkpoint happened), but replay skips it
// because a block already covers its window — so no duplicate/overlapping block
// is produced.
func TestWALReplayIdempotentAgainstPersistedBlocks(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	opts, pendingDir := managerOpts(t, dir, walDir, defaultWindowMs, 0)

	base := time.Now().UnixMilli()
	frag := makeFragment(t, "idem_metric", map[string]string{"k": "v"}, "ag",
		[]sample{{base, 1}, {base + 1000, 2}})
	frame := gorilla.EncodeFragmentBatch([]gorilla.Fragment{frag})

	// (1) Append + flush a window into a block, but DROP the handles WITHOUT a
	// WAL checkpoint (simulated crash after flush, before checkpoint), so the
	// WAL still contains the already-persisted frame.
	{
		mgr, err := NewManager(opts)
		if err != nil {
			t.Fatalf("manager 1: %v", err)
		}
		if _, err := mgr.Append(frame); err != nil {
			t.Fatalf("append: %v", err)
		}
		// Flush into a block (this writes the block) but simulate a crash before
		// the post-flush checkpoint by NOT calling Checkpoint: flush a single
		// window directly and reload, then drop handles.
		if ok, ferr := mgr.flushWindow(mgr.buf.windowStartFor(base)); ferr != nil || !ok {
			t.Fatalf("flush window: ok=%v err=%v", ok, ferr)
		}
		if rerr := mgr.Store().Reload(); rerr != nil {
			t.Fatalf("reload: %v", rerr)
		}
		_ = mgr.WAL().Close()
		_ = mgr.Store().Close()
	}

	preDirs := ulidDirs(t, pendingDir)
	if len(preDirs) != 1 {
		t.Fatalf("expected exactly 1 pending block before restart, got %d", len(preDirs))
	}

	// (2) Reopen: replay sees the frame in the WAL but the block already covers
	// the window, so it must be skipped. A subsequent flush builds nothing new.
	mgr2, err := NewManager(opts)
	if err != nil {
		t.Fatalf("manager 2 (replay): %v", err)
	}
	t.Cleanup(func() { _ = mgr2.Close() })

	built, ferr := mgr2.FlushAll()
	if ferr != nil {
		t.Fatalf("flush after replay: %v", ferr)
	}
	if built != 0 {
		t.Fatalf("replay rebuilt %d blocks for an already-persisted window (want 0)", built)
	}
	if dirs := ulidDirs(t, pendingDir); len(dirs) != 1 {
		t.Fatalf("expected exactly 1 pending block after idempotent replay, got %d (duplicate built)", len(dirs))
	}
}
