package merger

import (
	"fmt"
	"sort"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// samplesPerChunk is the Prometheus convention for re-encoded XOR chunks
// (storage.seriesToChunkEncoderSplit cuts a fresh chunk every 120 samples).
// We mirror it so merged chunks match what the background compactor produces.
const samplesPerChunk = 120

// sample is one decoded (timestamp, value) pair.
type decodedSample struct {
	t int64
	v float64
}

// chunksOverlap reports whether the given chunks (assumed sorted by MinTime)
// violate the strict-increasing, non-overlapping invariant the Prometheus
// index writer requires — i.e. some chunk starts at or before the running max.
func chunksOverlap(chunks []bufferedChunk) bool {
	lastMax := int64(-1 << 63)
	for _, c := range chunks {
		if c.MinTime <= lastMax {
			return true
		}
		if c.MaxTime > lastMax {
			lastMax = c.MaxTime
		}
	}
	return false
}

// deoverlapSeriesChunks makes a series' chunks satisfy the block writer's
// strict-increasing, non-overlapping invariant WITHOUT dropping any sample
// (the cold tier is the lossless raw backup). When the chunks are already
// disjoint it returns them untouched so the common case keeps the decode-free
// verbatim-bytes fast path. When they overlap it decodes every chunk, merges
// the samples by timestamp (de-duplicating identical timestamps — TSDB cannot
// store two values at one ts; last value wins, matching Prometheus head
// semantics), and re-encodes the merged stream into ordered ≤120-sample XOR
// chunks. XOR coding is exact for (int64 ts, float64 value), so the round-trip
// preserves every distinct sample.
//
// Returns (chunks, merged, mergedSamples): `merged` is true when a
// decode/re-encode happened; `mergedSamples` is the number of samples carried
// through that re-encode (for observability).
func deoverlapSeriesChunks(chunks []bufferedChunk) ([]bufferedChunk, bool, int, error) {
	if len(chunks) < 2 || !chunksOverlap(chunks) {
		return chunks, false, 0, nil
	}

	// 1. Decode every chunk into samples.
	all := make([]decodedSample, 0, len(chunks)*samplesPerChunk)
	for _, c := range chunks {
		chk, err := chunkenc.FromData(chunkenc.EncXOR, c.Data)
		if err != nil {
			return nil, false, 0, fmt.Errorf("deoverlap: wrap xor chunk: %w", err)
		}
		it := chk.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			t, v := it.At()
			all = append(all, decodedSample{t: t, v: v})
		}
		if err := it.Err(); err != nil {
			return nil, false, 0, fmt.Errorf("deoverlap: iterate xor chunk: %w", err)
		}
	}
	if len(all) == 0 {
		return nil, false, 0, fmt.Errorf("deoverlap: no samples decoded from %d chunks", len(chunks))
	}

	// 2. Stable-sort by timestamp; stability makes "last value wins" for a
	//    duplicated timestamp deterministic (the later-collected sample, i.e.
	//    the later-arriving fragment, is kept).
	sort.SliceStable(all, func(i, j int) bool { return all[i].t < all[j].t })

	// 3. De-duplicate identical timestamps, keeping the last occurrence.
	deduped := all[:0]
	for i, s := range all {
		if i > 0 && s.t == deduped[len(deduped)-1].t {
			deduped[len(deduped)-1] = s // last value wins
			continue
		}
		deduped = append(deduped, s)
	}

	// 4. Re-encode into ordered ≤samplesPerChunk XOR chunks.
	out := make([]bufferedChunk, 0, (len(deduped)+samplesPerChunk-1)/samplesPerChunk)
	for start := 0; start < len(deduped); start += samplesPerChunk {
		end := start + samplesPerChunk
		if end > len(deduped) {
			end = len(deduped)
		}
		seg := deduped[start:end]

		xc := chunkenc.NewXORChunk()
		app, err := xc.Appender()
		if err != nil {
			return nil, false, 0, fmt.Errorf("deoverlap: xor appender: %w", err)
		}
		for _, s := range seg {
			app.Append(s.t, s.v)
		}
		out = append(out, bufferedChunk{
			MinTime:    seg[0].t,
			MaxTime:    seg[len(seg)-1].t,
			NumSamples: len(seg),
			Data:       xc.Bytes(),
		})
	}
	return out, true, len(deduped), nil
}
