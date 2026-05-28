package merger

// ingest_bench_test.go — performance + compression-ratio measurement harness
// for the decode-free ingest path.
//
// Measurement 1: ingest CPU/mem — the NEW decode-free chunk-stitch path vs the
// OLD decode->head-Appender->Commit path (the pre-refactor approach).
//
// Measurement 2: compression ratio — tiny per-window L1 blocks vs the
// compacted/re-chunked L2 block produced by the compactor's rechunkPopulator.
//
// Run:
//   go test ./internal/merger -bench=Ingest  -benchmem -run=^$ -benchtime=200x -count=3
//   go test ./internal/merger -run=TestMeasureCompressionRatio -v

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// ----------------------------------------------------------------------------
// Shared corpus builder
// ----------------------------------------------------------------------------

// benchCorpus is the fixed, realistic input both ingest benchmarks consume: a
// set of ASAPFRG1 frames, each one a batch of XOR-chunk fragments (one fragment
// per series). Built ONCE, outside any timed loop.
type benchCorpus struct {
	frames     [][]byte // encoded ASAPFRG1 frames (one per "POST")
	frags      []gorilla.Fragment
	numSeries  int
	perChunk   int
	numSamples int
}

// buildBenchCorpus builds numSeries series, each emitting chunks of perChunk
// counter-like samples at 1s steps. Each chunk becomes one fragment; all
// fragments are packed into a single ASAPFRG1 frame (one big "POST"), which is
// what the merger's Append / old IngestBatch each consume.
func buildBenchCorpus(numSeries, chunksPerSeries, perChunk int) benchCorpus {
	base := int64(1_700_000_000_000)
	var frags []gorilla.Fragment
	for s := 0; s < numSeries; s++ {
		lset := map[string]string{
			"job":      "node",
			"instance": fmt.Sprintf("host-%03d.example.com:9100", s),
		}
		val := float64(s) // distinct counter base per series
		for c := 0; c < chunksPerSeries; c++ {
			samples := make([]sample, 0, perChunk)
			for k := 0; k < perChunk; k++ {
				idx := c*perChunk + k
				t := base + int64(idx)*1000
				// Counter-like, monotonically increasing with a little jitter so
				// XOR delta-of-delta has realistic (non-trivially-zero) work.
				val += 1 + float64((idx*7)%3)*0.001
				samples = append(samples, sample{t: t, v: val})
			}
			frags = append(frags, encodeFragment("node_cpu_seconds_total", lset, "agent-x", samples))
		}
	}
	frame := gorilla.EncodeFragmentBatch(frags)
	return benchCorpus{
		frames:     [][]byte{frame},
		frags:      frags,
		numSeries:  numSeries,
		perChunk:   perChunk,
		numSamples: numSeries * chunksPerSeries * perChunk,
	}
}

// encodeFragment is makeFragment without the *testing.T (usable from benchmarks
// and the corpus builder).
func encodeFragment(metric string, attrs map[string]string, source string, samples []sample) gorilla.Fragment {
	c := chunkenc.NewXORChunk()
	app, err := c.Appender()
	if err != nil {
		panic(err)
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

// ----------------------------------------------------------------------------
// Measurement 1a — NEW path codec cost (decode-free), CODEC ONLY: FromData +
// chunks.Meta accumulation, WITHOUT iterating samples and WITHOUT any disk
// writer. This is the pure CPU the PR's hot path spends vs the old path's
// decode->iterate->append loop. No fsync, no tempdir — isolates codec from disk.
// ----------------------------------------------------------------------------

func BenchmarkIngestDecodeFreeCodecOnly(b *testing.B) {
	corpus := buildBenchCorpus(100, 1, 120)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metas := make([]chunks.Meta, 0, len(corpus.frags))
		for fi := range corpus.frags {
			f := &corpus.frags[fi]
			// THE new hot path: wrap the raw XOR bytes WITHOUT iterating samples.
			chk, cerr := chunkenc.FromData(chunkenc.EncXOR, f.Data)
			if cerr != nil {
				b.Fatal(cerr)
			}
			metas = append(metas, chunks.Meta{Chunk: chk, MinTime: f.MinTime, MaxTime: f.MaxTime})
		}
		_ = metas
	}
}

// BenchmarkIngestDecodeFree is the codec PLUS the real chunks.Writer disk write
// (mirrors buildBlock's chunk-write step incl. the segment fsync on Close).
// NOTE: /tmp here is spinning ext3, so Close()'s mmap+fsync dominates wall-time;
// this is the "with disk" number — compare its allocs/op (not ns/op) to the old
// path to read the CPU/mem story, and see CodecOnly for the disk-free CPU.
func BenchmarkIngestDecodeFree(b *testing.B) {
	corpus := buildBenchCorpus(100, 1, 120)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		chunkw, err := chunks.NewWriter(dir)
		if err != nil {
			b.Fatal(err)
		}
		for fi := range corpus.frags {
			f := &corpus.frags[fi]
			chk, cerr := chunkenc.FromData(chunkenc.EncXOR, f.Data)
			if cerr != nil {
				b.Fatal(cerr)
			}
			meta := chunks.Meta{Chunk: chk, MinTime: f.MinTime, MaxTime: f.MaxTime}
			if werr := chunkw.WriteChunks(meta); werr != nil {
				b.Fatal(werr)
			}
		}
		if err := chunkw.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIngestDecodeFreeFullBlock runs the REAL buildBlock (chunks + index +
// tombstones + meta.json + atomic rename) per iteration, so the codec win can be
// compared against the full on-disk block-build cost both paths' "make it
// durable" step pays. This is the honest "full path" variant.
func BenchmarkIngestDecodeFreeFullBlock(b *testing.B) {
	corpus := buildBenchCorpus(100, 1, 120)
	// Pre-bucket the corpus into bufferedSeries once (this is the windowBuffer
	// state buildBlock consumes; the bucketing itself is a memcpy of bytes and is
	// NOT the codec work we are measuring, so do it outside the timed loop).
	series := corpusToBufferedSeries(corpus)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		if _, err := buildBlock(dir, series); err != nil {
			b.Fatal(err)
		}
	}
}

// corpusToBufferedSeries groups the corpus fragments into the sorted
// bufferedSeries slice buildBlock expects.
func corpusToBufferedSeries(corpus benchCorpus) []*bufferedSeries {
	byFP := map[uint64]*bufferedSeries{}
	for fi := range corpus.frags {
		f := &corpus.frags[fi]
		ls := labelsFor(f.MetricName, f.Attributes)
		fp := ls.Hash()
		bs, ok := byFP[fp]
		if !ok {
			bs = &bufferedSeries{lset: ls}
			byFP[fp] = bs
		}
		bs.chunks = append(bs.chunks, bufferedChunk{
			MinTime:    f.MinTime,
			MaxTime:    f.MaxTime,
			NumSamples: f.Count,
			Data:       append([]byte(nil), f.Data...),
		})
	}
	out := make([]*bufferedSeries, 0, len(byFP))
	for _, bs := range byFP {
		sort.Slice(bs.chunks, func(i, j int) bool { return bs.chunks[i].MinTime < bs.chunks[j].MinTime })
		out = append(out, bs)
	}
	sort.Slice(out, func(i, j int) bool { return labels.Compare(out[i].lset, out[j].lset) < 0 })
	return out
}

// ----------------------------------------------------------------------------
// Measurement 1b — OLD path: decode XOR -> per-sample head Appender.Append ->
// Commit, reconstructed faithfully from the deleted ingest.go (422082b^).
//
// Two variants:
//   - DecodeReencode      : WAL ON  (DefaultOptions) — exactly what the old prod
//                           path did (s.DB was opened with DefaultOptions, so the
//                           sample-level WAL fsynced on every Commit).
//   - DecodeReencodeNoWAL : WAL OFF (WALSegmentSize<0) — isolates the
//                           decode+re-encode+head-append CPU from the WAL fsync.
// ----------------------------------------------------------------------------

func BenchmarkIngestDecodeReencode(b *testing.B) {
	benchOldPath(b, false)
}

func BenchmarkIngestDecodeReencodeNoWAL(b *testing.B) {
	benchOldPath(b, true)
}

func benchOldPath(b *testing.B, disableWAL bool) {
	corpus := buildBenchCorpus(100, 1, 120)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		opts := tsdb.DefaultOptions()
		opts.NoLockfile = true
		// Wide block range so nothing head-compacts mid-benchmark.
		opts.MinBlockDuration = int64(2 * 60 * 60 * 1000)
		opts.MaxBlockDuration = int64(2 * 60 * 60 * 1000)
		opts.RetentionDuration = 0
		if disableWAL {
			opts.WALSegmentSize = -1 // WAL disabled
		}
		db, err := tsdb.Open(dir, nil, nil, opts, nil)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		// === reconstructed old IngestBatch hot loop ===
		app := db.Appender(context.Background())
		for fi := range corpus.frags {
			f := &corpus.frags[fi]
			chk, cerr := chunkenc.FromData(chunkenc.EncXOR, f.Data)
			if cerr != nil {
				b.Fatal(cerr)
			}
			ls := labelsFor(f.MetricName, f.Attributes)
			it := chk.Iterator(nil)
			var ref storage.SeriesRef
			for it.Next() == chunkenc.ValFloat {
				t, v := it.At()
				newRef, aerr := app.Append(ref, ls, t, v)
				if aerr != nil {
					b.Fatal(aerr)
				}
				ref = newRef
			}
			if itErr := it.Err(); itErr != nil {
				b.Fatal(itErr)
			}
		}
		if cerr := app.Commit(); cerr != nil {
			b.Fatal(cerr)
		}
		// === end old hot loop ===

		b.StopTimer()
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}

// ----------------------------------------------------------------------------
// Measurement 2 — compression ratio: tiny L1 blocks vs compacted/re-chunked L2.
// Driven as a regular test (not a benchmark) because it prints a table.
// ----------------------------------------------------------------------------

func TestMeasureCompressionRatio(t *testing.T) {
	// For each agent chunk size, build the per-window L1 blocks via the REAL
	// Manager/buildBlock path, then run the compactor's CompactOnce (rechunk
	// populator) to fuse + re-chunk them into a single L2 block, and compare
	// on-disk bytes/sample and avg samples/chunk.
	const numSeries = 50
	const totalSamplesPerSeries = 600 // fixed sample budget per series

	fmt.Println()
	fmt.Println("=== Measurement 2: compression ratio (L1 tiny chunks vs L2 re-chunked) ===")
	fmt.Println("chunkB/s = chunks-dir bytes per sample; totB/s = (chunks+index) bytes per sample.")
	fmt.Printf("%-10s | %-34s | %-34s | %-18s\n",
		"agentChk", "L1 (many tiny per-window blocks)", "L2 (1 compacted, ~120/chk)", "improvement")
	fmt.Printf("%-10s | %-34s | %-34s | %-18s\n",
		"(samp)", "chunkB/s totB/s avgS/chk nChk nBlk", "chunkB/s totB/s avgS/chk nChk nBlk", "chunkB/s  totB/s")
	fmt.Println("-----------+------------------------------------+------------------------------------+-------------------")

	for _, agentChunk := range []int{10, 30, 60} {
		l1, l2 := measureRatioForChunkSize(t, numSeries, totalSamplesPerSeries, agentChunk)
		chunkRatio := l1.bytesPerSample / l2.bytesPerSample
		totRatio := l1.totalBytesPerSample / l2.totalBytesPerSample
		fmt.Printf("%-10d | %7.2f %7.2f %7.1f %4d %4d | %7.2f %7.2f %7.1f %4d %4d | %6.2fx  %6.2fx\n",
			agentChunk,
			l1.bytesPerSample, l1.totalBytesPerSample, l1.avgSamplesPerChunk, l1.numChunks, l1.numBlocks,
			l2.bytesPerSample, l2.totalBytesPerSample, l2.avgSamplesPerChunk, l2.numChunks, l2.numBlocks,
			chunkRatio, totRatio)
	}
	fmt.Println()
}

type ratioStats struct {
	chunkBytes          int64
	indexBytes          int64
	totalBytes          int64
	numSamples          int
	numChunks           int
	numBlocks           int
	bytesPerSample      float64 // chunk bytes / sample
	totalBytesPerSample float64 // (chunk+index) bytes / sample
	avgSamplesPerChunk  float64
}

// measureRatioForChunkSize builds L1 blocks (one window per agentChunk-sized
// fragment span) then compacts to L2, returning on-disk stats for each.
func measureRatioForChunkSize(t *testing.T, numSeries, totalSamplesPerSeries, agentChunk int) (l1, l2 ratioStats) {
	t.Helper()
	// Small window so each agent chunk closes into its OWN pending block (the
	// "many tiny L1 blocks" scenario). Window == agentChunk seconds; grace 0.
	windowMs := int64(agentChunk) * 1000
	// Wide compaction span so ALL windows fuse into one L2 block.
	maxSpanMs := windowMs << 20
	st, pendingDir, shippedDir := fixedWindowStorage(t, windowMs, 0)

	chunksPerSeries := totalSamplesPerSeries / agentChunk
	// Align base to the compaction grid so windows tile cleanly.
	base := (int64(1_700_000_000_000) / maxSpanMs) * maxSpanMs

	// Emit one fragment per (series, agent-chunk), each fragment's samples inside
	// its own window. POST one frame per window (all series' chunk for that window).
	for c := 0; c < chunksPerSeries; c++ {
		wStart := base + int64(c)*windowMs
		frags := make([]gorilla.Fragment, 0, numSeries)
		for s := 0; s < numSeries; s++ {
			lset := map[string]string{
				"job":      "node",
				"instance": fmt.Sprintf("host-%03d:9100", s),
			}
			samples := make([]sample, 0, agentChunk)
			val := float64(s)
			for k := 0; k < agentChunk; k++ {
				ts := wStart + int64(k)*(windowMs/int64(agentChunk))
				val += 1 + float64((c*agentChunk+k)%3)*0.001
				samples = append(samples, sample{t: ts, v: val})
			}
			frags = append(frags, encodeFragment("node_cpu_seconds_total", lset, "agent-x", samples))
		}
		if _, err := st.Manager.Append(gorilla.EncodeFragmentBatch(frags)); err != nil {
			t.Fatalf("append window %d: %v", c, err)
		}
	}

	// Close every window into its own pending L1 block.
	lastEnd := base + int64(chunksPerSeries)*windowMs
	built, err := st.Manager.FlushClosed(lastEnd + 1)
	if err != nil {
		t.Fatalf("flush closed: %v", err)
	}
	if built != chunksPerSeries {
		t.Fatalf("agentChunk=%d: expected %d pending blocks, got %d", agentChunk, chunksPerSeries, built)
	}

	// L1 stats: sum across ALL pending block dirs.
	l1 = sumBlockStats(t, ulidDirs(t, pendingDir))

	// Compact to a single L2 block (re-chunk to ~120).
	comp, err := NewCompactor(CompactorOptions{
		Store:     st.BlockStore(),
		MinBlocks: 1,
		MaxSpanMs: maxSpanMs,
	})
	if err != nil {
		t.Fatalf("new compactor: %v", err)
	}
	if err := comp.CompactOnce(context.Background()); err != nil {
		t.Fatalf("compact: %v", err)
	}
	shipped := ulidDirs(t, shippedDir)
	if len(shipped) != 1 {
		t.Fatalf("agentChunk=%d: expected 1 shipped L2 block, got %d", agentChunk, len(shipped))
	}
	l2 = sumBlockStats(t, shipped)

	// Sanity: sample count must be conserved across the re-chunk.
	if l1.numSamples != l2.numSamples {
		t.Fatalf("agentChunk=%d: sample count changed L1=%d L2=%d", agentChunk, l1.numSamples, l2.numSamples)
	}
	return l1, l2
}

// sumBlockStats sums on-disk chunk+index bytes and decodes the blocks (test
// path only) to count chunks + samples across the given block dirs.
func sumBlockStats(t *testing.T, dirs []string) ratioStats {
	t.Helper()
	var st ratioStats
	st.numBlocks = len(dirs)
	for _, d := range dirs {
		st.chunkBytes += dirSize(t, filepath.Join(d, "chunks"))
		st.indexBytes += fileSize(t, filepath.Join(d, "index"))
		chunkCounts, sampleCounts := countChunksAndSamples(t, d)
		for _, c := range chunkCounts {
			st.numChunks += c
		}
		for _, s := range sampleCounts {
			st.numSamples += s
		}
	}
	st.totalBytes = st.chunkBytes + st.indexBytes
	if st.numSamples > 0 {
		// bytes/sample reported on the CHUNK bytes (the XOR payload, what the
		// re-chunk actually compresses); totalBytesPerSample folds in the per-block
		// index, which the "many tiny L1 blocks" case pays once PER block.
		st.bytesPerSample = float64(st.chunkBytes) / float64(st.numSamples)
		st.totalBytesPerSample = float64(st.totalBytes) / float64(st.numSamples)
	}
	if st.numChunks > 0 {
		st.avgSamplesPerChunk = float64(st.numSamples) / float64(st.numChunks)
	}
	return st
}

func dirSize(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir %q: %v", dir, err)
	}
	for _, e := range entries {
		info, ierr := e.Info()
		if ierr != nil {
			t.Fatalf("stat %q: %v", e.Name(), ierr)
		}
		if e.IsDir() {
			continue
		}
		total += info.Size()
	}
	return total
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	return info.Size()
}
