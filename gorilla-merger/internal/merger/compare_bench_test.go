package merger

// compare_bench_test.go — SAME-MACHINE, SAME-WORKLOAD throughput comparison of
// the merger's decode-free central ingest against the path Thanos Receive uses:
// the Prometheus TSDB head Appender (parse-done -> Append encodes into head XOR
// chunks + indexes each series -> Commit writes the head WAL). This is the
// per-sample CPU the merger OFFLOADS to the edge (the edge pre-encodes the XOR
// chunk; the merger ingests it decode-free and only pays the per-series index at
// block-build time).
//
// The Prometheus path decodes the SAME XOR frags the merger ingests, so both
// systems ingest byte-identical samples. Run all on tmpfs for a CPU-bound,
// apples-to-apples number (the fsync-bearing stages are IO-bound on spinning
// disk and would otherwise measure the disk, not the engine):
//
//   TMPDIR=/dev/shm GOPRIVATE='github.com/ProjectASAP/*' go test ./internal/merger/ \
//     -run '^$' -bench 'IngestFragmentBatch|WindowToPendingBlock|PrometheusTSDBAppendCommit' \
//     -benchmem -benchtime=10x -count=3
//
// Fair comparison metric = "central CPU per sample to make data queryable+indexed":
//   - Prometheus/Thanos: BenchmarkPrometheusTSDBAppendCommit (append+commit; head
//     is indexed & queryable immediately).
//   - Merger: BenchmarkIngestFragmentBatch + BenchmarkWindowToPendingBlock
//     (ingest is decode-free and NOT yet queryable; block-build is where the
//     merger pays the per-series index — that sum is the honest queryable gate,
//     NOT the raw ingest figure alone).
//
// === MEASURED, SAME MACHINE (Xeon Gold 5512U), SAME WORKLOAD (1000 series x 120
//     samples = 120k samples/window), SINGLE CORE, tmpfs (/dev/shm) ===
//
//   path (per core)                         per 120k     samples/s/core   basis
//   ASAP merger — decode-free ingest         23.8 ms       5.04 M         buffer pre-encoded XOR chunks + WAL (no per-sample parse/encode)
//   VictoriaMetrics v1.145 remote_write       —            3.20 M         snappy/proto parse + AddRows (bg part-merge deferred); SERVER-CONFIRMED 6.0M rows, 1000 series queryable
//   Prometheus TSDB v0.308 (Thanos core)     87.0 ms       1.38 M         per-sample encode + index + WAL commit (head queryable; input pre-decoded, so this is the pure TSDB-core cost)
//   ASAP merger — window->pending block     ~225  ms      ~0.53 M         full on-disk TSDB block per 2-min window (deferred/background; the merger's heaviest per-sample stage)
//
// Read (honest):
//   - Single-core ingest-to-queryable-buffer: merger 5.0M > VM 3.2M > Prometheus
//     1.4M. The merger leads because the EDGE pre-encoded the XOR chunk (offloaded
//     ~1.38 us/sample edge cost, Fig 11), so central ingest is decode-free. VM
//     beats Prometheus-TSDB because its ingest path is more optimised, not because
//     of any ASAP work.
//   - Persisting on-disk blocks is a DEFERRED/background cost for all three (VM
//     part-merge, Prometheus 2h head->block, merger per-window block-build). The
//     merger's block-build (~0.53M/s/core here) is where its deferred per-series
//     index lands and is its slowest stage — a real perf target, not a win.
//   - VM CAVEAT: remote_write timestamps MUST be within -retentionPeriod or VM
//     parses-then-DROPS them (an early run timestamped at the corpus's 2023 base
//     showed a bogus 11.5 M/s "accept" rate with 0 series stored). The 3.20 M/s
//     above is with near-now timestamps and is server-confirmed queryable.
//   - allocs/window: merger ingest 14.1k, Prometheus append 22.3k.

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// decodedSeries is the corpus expressed as raw (labels, samples) — the form a
// remote_write/scrape ingester (the Prometheus TSDB head that Thanos Receive
// embeds) consumes. Decoded ONCE, outside the timed loop, from the same XOR
// frags the merger ingests.
type decodedSeries struct {
	ls  labels.Labels
	ts  []int64
	val []float64
}

func decodeCorpus(tb testing.TB, c benchCorpus) []decodedSeries {
	tb.Helper()
	out := make([]decodedSeries, 0, len(c.frags))
	for fi := range c.frags {
		f := &c.frags[fi]
		chk, err := chunkenc.FromData(chunkenc.EncXOR, f.Data)
		if err != nil {
			tb.Fatal(err)
		}
		ds := decodedSeries{ls: labelsFor(f.MetricName, f.Attributes)}
		it := chk.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			t, v := it.At()
			ds.ts = append(ds.ts, t)
			ds.val = append(ds.val, v)
		}
		if err := it.Err(); err != nil {
			tb.Fatal(err)
		}
		out = append(out, ds)
	}
	return out
}

// BenchmarkPrometheusTSDBAppendCommit — the Thanos Receive central ingest path
// (Prometheus TSDB head Appender). Per 120k-sample window: Append every
// (series,t,v) (encodes into head XOR chunks + indexes each series) then Commit
// (head WAL). Same workload/machine/disk as the merger benches.
func BenchmarkPrometheusTSDBAppendCommit(b *testing.B) {
	corpus := buildBenchCorpus(benchNumSeries, 1, benchPerChunk)
	series := decodeCorpus(b, corpus)
	ctx := context.Background()
	b.SetBytes(int64(corpus.numSamples) * 16) // 16 B/sample (8B ts + 8B f64) raw
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions(), nil)
		if err != nil {
			b.Fatal(err)
		}
		// keep each iteration's window strictly forward of the last so the head
		// never rejects out-of-order (fresh DB per iter also guarantees this).
		off := int64(i+1) * int64(benchPerChunk) * 1000 * 2
		b.StartTimer()

		app := db.Appender(ctx)
		for si := range series {
			s := &series[si]
			var ref storage.SeriesRef
			for k := range s.ts {
				var aerr error
				ref, aerr = app.Append(ref, s.ls, s.ts[k]+off, s.val[k])
				if aerr != nil {
					b.Fatal(aerr)
				}
			}
		}
		if err := app.Commit(); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		_ = db.Close()
		b.StartTimer()
	}
}
