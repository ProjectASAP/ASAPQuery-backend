package merger

import (
	"testing"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// encodeXOR builds a raw EncXOR chunk from samples and returns the
// bufferedChunk (raw bytes + header) the merger would have buffered.
func encodeXOR(t *testing.T, samples []decodedSample) bufferedChunk {
	t.Helper()
	xc := chunkenc.NewXORChunk()
	app, err := xc.Appender()
	if err != nil {
		t.Fatalf("appender: %v", err)
	}
	for _, s := range samples {
		app.Append(s.t, s.v)
	}
	return bufferedChunk{
		MinTime:    samples[0].t,
		MaxTime:    samples[len(samples)-1].t,
		NumSamples: len(samples),
		Data:       xc.Bytes(),
	}
}

// decodeAll flattens a chunk list back into samples for assertions.
func decodeAll(t *testing.T, chunks []bufferedChunk) []decodedSample {
	t.Helper()
	var out []decodedSample
	for _, c := range chunks {
		chk, err := chunkenc.FromData(chunkenc.EncXOR, c.Data)
		if err != nil {
			t.Fatalf("fromdata: %v", err)
		}
		it := chk.Iterator(nil)
		for it.Next() == chunkenc.ValFloat {
			ts, v := it.At()
			out = append(out, decodedSample{t: ts, v: v})
		}
		if err := it.Err(); err != nil {
			t.Fatalf("iter: %v", err)
		}
	}
	return out
}

// assertStrictlyOrdered verifies the per-chunk non-overlap invariant the
// Prometheus index writer enforces: each chunk's MinTime > previous MaxTime.
func assertStrictlyOrdered(t *testing.T, chunks []bufferedChunk) {
	t.Helper()
	last := int64(-1 << 63)
	for i, c := range chunks {
		if c.MinTime <= last {
			t.Fatalf("chunk %d MinTime %d not > previous MaxTime %d", i, c.MinTime, last)
		}
		last = c.MaxTime
	}
}

// TestDeoverlapDisjointIsVerbatim: disjoint chunks are returned untouched
// (decode-free fast path), didMerge=false.
func TestDeoverlapDisjointIsVerbatim(t *testing.T) {
	chunks := []bufferedChunk{
		encodeXOR(t, []decodedSample{{10, 1}, {11, 2}}),
		encodeXOR(t, []decodedSample{{12, 3}, {13, 4}}),
	}
	out, merged, n, err := deoverlapSeriesChunks(chunks)
	if err != nil {
		t.Fatal(err)
	}
	if merged || n != 0 {
		t.Fatalf("expected no merge for disjoint chunks, got merged=%v n=%d", merged, n)
	}
	if len(out) != 2 {
		t.Fatalf("expected chunks unchanged, got %d", len(out))
	}
}

// TestDeoverlapInterleavedLossless: overlapping ranges with DISTINCT
// timestamps must keep every sample (the lossless requirement) and produce a
// strictly-ordered, non-overlapping chunk list.
func TestDeoverlapInterleavedLossless(t *testing.T) {
	// Two chunks whose [min,max] ranges overlap (11..14 vs 12..15) but whose
	// timestamps are all distinct — mirrors the gorilla-merger OOO failure.
	chunks := []bufferedChunk{
		encodeXOR(t, []decodedSample{{11, 1}, {13, 3}, {14, 4}}),
		encodeXOR(t, []decodedSample{{12, 2}, {15, 5}}),
	}
	if !chunksOverlap(chunks) {
		t.Fatal("test setup: chunks should overlap")
	}
	out, merged, n, err := deoverlapSeriesChunks(chunks)
	if err != nil {
		t.Fatal(err)
	}
	if !merged {
		t.Fatal("expected a lossless merge for overlapping chunks")
	}
	if n != 5 {
		t.Fatalf("expected 5 merged samples, got %d", n)
	}
	assertStrictlyOrdered(t, out)
	got := decodeAll(t, out)
	want := []decodedSample{{11, 1}, {12, 2}, {13, 3}, {14, 4}, {15, 5}}
	if len(got) != len(want) {
		t.Fatalf("lossy: got %d samples, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sample %d: got %+v want %+v", i, got[i], want[i])
		}
	}
}

// TestDeoverlapDuplicateTimestampLastWins: a duplicated timestamp can't be
// stored twice; the later-arriving value wins, all other samples survive.
func TestDeoverlapDuplicateTimestampLastWins(t *testing.T) {
	chunks := []bufferedChunk{
		encodeXOR(t, []decodedSample{{10, 1}, {20, 2}}),
		encodeXOR(t, []decodedSample{{20, 99}, {30, 3}}), // dup ts=20, newer value 99
	}
	out, merged, _, err := deoverlapSeriesChunks(chunks)
	if err != nil {
		t.Fatal(err)
	}
	if !merged {
		t.Fatal("expected merge")
	}
	assertStrictlyOrdered(t, out)
	got := decodeAll(t, out)
	want := []decodedSample{{10, 1}, {20, 99}, {30, 3}}
	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sample %d: got %+v want %+v", i, got[i], want[i])
		}
	}
}

// TestDeoverlapResplitsAt120: a merged run longer than samplesPerChunk is
// re-split into ≤120-sample chunks (matching Prometheus' convention).
func TestDeoverlapResplitsAt120(t *testing.T) {
	var a, b []decodedSample
	// 200 distinct even timestamps in chunk a, 200 odd in chunk b → overlap.
	for i := 0; i < 200; i++ {
		a = append(a, decodedSample{int64(2 * i), float64(i)})
		b = append(b, decodedSample{int64(2*i + 1), float64(i)})
	}
	chunks := []bufferedChunk{encodeXOR(t, a), encodeXOR(t, b)}
	out, merged, n, err := deoverlapSeriesChunks(chunks)
	if err != nil {
		t.Fatal(err)
	}
	if !merged || n != 400 {
		t.Fatalf("expected merge of 400 samples, got merged=%v n=%d", merged, n)
	}
	assertStrictlyOrdered(t, out)
	for i, c := range out {
		if c.NumSamples > samplesPerChunk {
			t.Fatalf("chunk %d has %d samples, exceeds %d", i, c.NumSamples, samplesPerChunk)
		}
	}
	if len(decodeAll(t, out)) != 400 {
		t.Fatal("lossy re-split")
	}
}
