package merger

import (
	"context"
	"math"
	"testing"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/prometheus/prometheus/model/labels"
)

// selectAll queries the BlockStore (union of pending + shipped) for the given
// series WITHOUT flushing first (unlike readBack, which force-flushes), and
// returns its samples in time order plus the number of matching series. It is
// the production query path: customStore -> BlockStore.ChunkQuerier over both
// dirs. Used by the pipeline test to assert visibility + no-gap/no-dup directly.
func selectAll(t *testing.T, s *Storage, want labels.Labels) (out []sample, nSeries int) {
	t.Helper()
	q, err := s.BlockStore().ChunkQuerier(math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("chunk querier: %v", err)
	}
	defer q.Close()

	matchers := make([]*labels.Matcher, 0, want.Len())
	want.Range(func(l labels.Label) {
		matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, l.Name, l.Value))
	})

	ss := q.Select(context.Background(), false, nil, matchers...)
	for ss.Next() {
		nSeries++
		series := ss.At()
		it := series.Iterator(nil)
		for it.Next() {
			chk := it.At()
			cit := chk.Chunk.Iterator(nil)
			for cit.Next() != 0 { // chunkenc.ValNone == 0; loop over all valid samples
				tt, vv := cit.At()
				out = append(out, sample{t: tt, v: vv})
			}
			if cit.Err() != nil {
				t.Fatalf("chunk iterator: %v", cit.Err())
			}
		}
		if it.Err() != nil {
			t.Fatalf("series iterator: %v", it.Err())
		}
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("select err: %v", err)
	}
	return out, nSeries
}

// TestDefaultPipelineSmallWindowVisibilityAndShip exercises the DEFAULT
// production flow (small build window + decoupled, wide compaction span +
// pending/shipped split). It proves the fix for the "2h window hides recent
// data + compaction never runs + only tiny L1 blocks ship" gating bug:
//
//  1. Fragment batches spread across several SHORT windows are ingested.
//  2. After a single FlushClosed past the grace, the data is immediately
//     queryable via the BlockStore in the PENDING dir — recent data is NOT
//     stuck for a full (2h) window (the whole point of the small window).
//  3. One compactor pass merges + re-chunks the pending blocks into the SHIPPED
//     dir: the pending sources are removed, the BlockStore still returns ALL the
//     ingested samples across the transition (no gap, no dup), and the shipped
//     dir contains ONLY the compacted block (so the shipper, pointed at shipped,
//     uploads only that ratio-optimized block).
//  4. The compacted block's chunks are re-chunked toward ~120 samples/chunk.
func TestDefaultPipelineSmallWindowVisibilityAndShip(t *testing.T) {
	// Small window like the new default (2m in prod; 2s here for a fast,
	// deterministic test), zero grace so FlushClosed(now) closes every elapsed
	// window. The compaction span is DECOUPLED and wide enough that all windows
	// fuse into ONE shipped block (mirrors prod: window << compact-max-span).
	const windowMs = int64(2000)
	// Decoupled span: far wider than the window AND a multiple of it (so windows
	// tile the span cleanly and base, aligned to the span, is also window-aligned
	// — keeping one fragment-window == one buffer-window == one pending block).
	const maxSpanMs = windowMs << 30
	st, pendingDir, shippedDir := fixedWindowStorage(t, windowMs, 0)

	series := labels.FromStrings(labels.MetricName, "cpu_seconds_total", "core", "0")

	// 250 samples across 5 distinct short windows (50/window). Align base to the
	// compaction grid so all windows land in a single grid cell and fuse into one
	// compacted block. Each fragment is one window's worth of samples.
	const nWindows = 5
	const perWindow = 50
	const nSamples = nWindows * perWindow
	base := (int64(1_700_000_000_000) / maxSpanMs) * maxSpanMs

	want := make([]sample, 0, nSamples)
	i := 0
	for w := 0; w < nWindows; w++ {
		wStart := base + int64(w)*windowMs
		samples := make([]sample, 0, perWindow)
		for k := 0; k < perWindow; k++ {
			// Keep each window's samples inside that window: spread perWindow
			// samples over the windowMs span (windowMs/perWindow ms apart).
			ts := wStart + int64(k)*(windowMs/perWindow)
			samples = append(samples, sample{t: ts, v: float64(i)})
			want = append(want, sample{t: ts, v: float64(i)})
			i++
		}
		frag := makeFragment(t, "cpu_seconds_total", map[string]string{"core": "0"}, "agent-1", samples)
		if _, err := st.Manager.Append(gorilla.EncodeFragmentBatch([]gorilla.Fragment{frag})); err != nil {
			t.Fatalf("append window %d: %v", w, err)
		}
	}

	// (2) Recent-data visibility: a single FlushClosed at a "now" just past the
	// last window's end closes EVERY window into a pending block. The data must
	// then be queryable immediately — NOT hidden until a 2h window elapses.
	lastWindowEnd := base + int64(nWindows)*windowMs
	now := lastWindowEnd + 1 // grace is 0, so all nWindows windows are closable
	built, err := st.Manager.FlushClosed(now)
	if err != nil {
		t.Fatalf("flush closed: %v", err)
	}
	if built != nWindows {
		t.Fatalf("expected %d pending blocks (one per window) after flush, got %d", nWindows, built)
	}
	if got := len(ulidDirs(t, pendingDir)); got != nWindows {
		t.Fatalf("expected %d pending block dirs, got %d", nWindows, got)
	}
	if got := len(ulidDirs(t, shippedDir)); got != 0 {
		t.Fatalf("expected 0 shipped blocks before compaction, got %d", got)
	}

	// Queryable RIGHT NOW (recent data visible without waiting a full window).
	preGot, preSeries := selectAll(t, st, series)
	if preSeries != 1 {
		t.Fatalf("expected exactly 1 series visible post-flush, got %d", preSeries)
	}
	assertSamples(t, "pre-compaction", preGot, want)

	// (3) One compactor pass: merge + re-chunk the pending blocks into shipped.
	comp, err := NewCompactor(CompactorOptions{
		Store:     st.BlockStore(),
		MinBlocks: 1, // promote even a lone block (matches the new default)
		MaxSpanMs: maxSpanMs,
	})
	if err != nil {
		t.Fatalf("new compactor: %v", err)
	}
	if err := comp.CompactOnce(context.Background()); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Pending sources removed; exactly one compacted block in shipped.
	if got := len(ulidDirs(t, pendingDir)); got != 0 {
		t.Fatalf("expected pending dir empty after compaction, got %d", got)
	}
	shipped := ulidDirs(t, shippedDir)
	if len(shipped) != 1 {
		t.Fatalf("expected exactly 1 shipped (compacted) block, got %d", len(shipped))
	}

	// The shipped dir must hold ONLY compacted (Level > 1) blocks — this is what
	// makes the shipper (pointed at shipped) upload only ratio-optimized blocks.
	for _, bd := range shipped {
		meta, _, merr := readDirMeta(bd)
		if merr != nil {
			t.Fatalf("read shipped meta %q: %v", bd, merr)
		}
		if meta.Compaction.Level <= 1 {
			t.Fatalf("shipped block %q is Level %d (want > 1, i.e. compacted); a non-compacted block leaked into shipped",
				bd, meta.Compaction.Level)
		}
	}

	// (3 cont.) No gap, no dup across the pending->shipped transition: the
	// BlockStore (now serving from the shipped dir) still returns ALL samples.
	postGot, postSeries := selectAll(t, st, series)
	if postSeries != 1 {
		t.Fatalf("expected exactly 1 series post-compaction, got %d", postSeries)
	}
	assertSamples(t, "post-compaction", postGot, want)

	// (4) Re-chunked toward ~120 samples/chunk: 250 samples should collapse from
	// 5 tiny per-window blocks (50/chunk) into far fewer, larger chunks.
	chunkCounts, sampleCounts := countChunksAndSamples(t, shipped[0])
	if len(chunkCounts) != 1 {
		t.Fatalf("expected 1 series in compacted block, got %d", len(chunkCounts))
	}
	gotChunks := chunkCounts[0]
	// 250 samples at ~120/chunk -> ~2-3 chunks. Allow slack but require a real
	// re-chunk (fewer than the 5 input chunks, and a big chunk near the target).
	if gotChunks >= nWindows {
		t.Fatalf("compaction did not re-chunk: %d chunks (want < %d input chunks)", gotChunks, nWindows)
	}
	if gotChunks > 5 {
		t.Fatalf("re-chunk target ~120/chunk not met: %d chunks for %d samples", gotChunks, nSamples)
	}
	total, biggest := 0, 0
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
		t.Fatalf("expected a chunk near the 120-sample target, biggest was %d", biggest)
	}
}
