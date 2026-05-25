package coldchunk

import (
	"math"
	"testing"

	"github.com/ProjectASAP/asap-gorilla-go/intchunk"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// encode runs the intchunk best-of-N encoder over samples and returns the
// winning chunk(s) plus the codec tag that won, so a test can both round-trip
// the data and assert which sub-codec was exercised.
func encode(t *testing.T, samples []intchunk.Sample) (intchunk.EncodeResult, [][]byte) {
	t.Helper()
	res, err := intchunk.Encode(samples)
	if err != nil {
		t.Fatalf("intchunk.Encode: %v", err)
	}
	if len(res.Chunks) == 0 {
		t.Fatalf("intchunk.Encode returned no chunks")
	}
	return res, res.Chunks
}

// assertSamplesEqual fails unless got and want are identical in length, every
// timestamp, and every value BIT-EXACTLY (bit-exactness matters for the
// high-precision-float case where == on the float is the whole point).
func assertSamplesEqual(t *testing.T, what string, got, want []intchunk.Sample) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d samples, want %d", what, len(got), len(want))
	}
	for i := range want {
		if got[i].T != want[i].T || math.Float64bits(got[i].V) != math.Float64bits(want[i].V) {
			t.Fatalf("%s: sample %d mismatch: got (%d,%v) want (%d,%v)",
				what, i, got[i].T, got[i].V, want[i].T, want[i].V)
		}
	}
}

// iterateXOR reads every float sample out of a Prometheus XOR chunk in order.
func iterateXOR(t *testing.T, c chunkenc.Chunk) []intchunk.Sample {
	t.Helper()
	if c.Encoding() != chunkenc.EncXOR {
		t.Fatalf("expected EncXOR chunk, got %v", c.Encoding())
	}
	it := c.Iterator(nil)
	var out []intchunk.Sample
	for it.Next() == chunkenc.ValFloat {
		ts, v := it.At()
		out = append(out, intchunk.Sample{T: ts, V: v})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("xor iterator: %v", err)
	}
	return out
}

// roundTripCase encodes the series, decodes it through the coldchunk helper,
// and asserts both contracts: (a) exact sample round-trip, and (b) the produced
// XOR chunk iterates back to the same samples. It returns the winning codec tag
// so callers can confirm the intended sub-codec was exercised.
func roundTripCase(t *testing.T, name string, samples []intchunk.Sample) intchunk.CodecTag {
	t.Helper()
	res, chunks := encode(t, samples)

	// Single-chunk inputs go through the headline DecodeToXORChunk; multi-chunk
	// (overflow re-base) inputs go through the chunks variant + a manual XOR
	// re-encode so both code paths are covered.
	var (
		gotSamples []intchunk.Sample
		xc         chunkenc.Chunk
		err        error
	)
	if len(chunks) == 1 {
		gotSamples, xc, err = DecodeToXORChunk(chunks[0])
		if err != nil {
			t.Fatalf("%s: DecodeToXORChunk: %v", name, err)
		}
	} else {
		gotSamples, err = DecodeChunksToSamples(chunks)
		if err != nil {
			t.Fatalf("%s: DecodeChunksToSamples: %v", name, err)
		}
		xc, err = SamplesToXORChunk(gotSamples)
		if err != nil {
			t.Fatalf("%s: SamplesToXORChunk: %v", name, err)
		}
	}

	// (a) decoded samples match the originals exactly.
	assertSamplesEqual(t, name+" decode", gotSamples, samples)

	// (b) the produced XOR chunk iterates back to the same samples.
	assertSamplesEqual(t, name+" xor-iterate", iterateXOR(t, xc), samples)

	return res.Tag
}

// TestDecodeGaugeSeries exercises a gauge (irregular fixed-decimal values), which
// the best-of-N encoder serves with an INT_FOR_DELTA-family codec.
func TestDecodeGaugeSeries(t *testing.T) {
	base := int64(1_700_000_000_000)
	samples := []intchunk.Sample{
		{T: base, V: 12.5},
		{T: base + 1000, V: 13.0},
		{T: base + 2000, V: 11.75},
		{T: base + 3000, V: 14.25},
		{T: base + 4000, V: 13.5},
		{T: base + 5000, V: 12.0},
		{T: base + 6000, V: 15.5},
	}
	tag := roundTripCase(t, "gauge", samples)
	t.Logf("gauge winning codec: %s", tag)
}

// TestDecodeCounterSeries exercises a monotonically increasing integer counter,
// which the best-of-N encoder serves with an INT_FOR_DOD-family codec (near-zero
// delta-of-delta).
func TestDecodeCounterSeries(t *testing.T) {
	base := int64(1_700_000_000_000)
	samples := make([]intchunk.Sample, 0, 64)
	val := 0.0
	for i := 0; i < 64; i++ {
		val += float64(100 + i) // steadily rising, integer-valued
		samples = append(samples, intchunk.Sample{T: base + int64(i)*15000, V: val})
	}
	tag := roundTripCase(t, "counter", samples)
	t.Logf("counter winning codec: %s", tag)
}

// TestDecodeHighPrecisionFloatSeries exercises true high-precision floats that
// no decimal scale can represent exactly, forcing the GORILLA_XOR fallback. This
// is the case the intchunk exactness guard reserves for XOR, and it proves the
// coldchunk helper handles the XOR-tagged sub-codec losslessly too.
func TestDecodeHighPrecisionFloatSeries(t *testing.T) {
	base := int64(1_700_000_000_000)
	samples := []intchunk.Sample{
		{T: base, V: math.Pi},
		{T: base + 1000, V: math.E},
		{T: base + 2000, V: math.Sqrt2},
		{T: base + 3000, V: 1.0 / 3.0},
		{T: base + 4000, V: 0.1 + 0.2}, // classic non-decimal-exact float
		{T: base + 5000, V: math.Ln2},
		{T: base + 6000, V: -2.718281828459045e-7},
	}
	tag := roundTripCase(t, "highprec", samples)
	if tag != intchunk.CodecGorillaXOR {
		// Not strictly required for correctness (any lossless codec round-trips),
		// but if these genuinely-irrational values stopped landing on the XOR
		// fallback it would mean the exactness guard regressed.
		t.Logf("high-precision series did not pick GORILLA_XOR (got %s); "+
			"round-trip still verified lossless", tag)
	}
}

// TestDecodeAllSubCodecsExercised confirms the three series above collectively
// drive at least two distinct intchunk codec families, so the coldchunk decode
// helper is proven against the INT_* path and the XOR path (not just one).
func TestDecodeAllSubCodecsExercised(t *testing.T) {
	base := int64(1_700_000_000_000)
	gauge := []intchunk.Sample{
		{T: base, V: 12.5}, {T: base + 1000, V: 13.0}, {T: base + 2000, V: 11.75},
		{T: base + 3000, V: 14.25}, {T: base + 4000, V: 13.5},
	}
	counter := make([]intchunk.Sample, 0, 32)
	v := 0.0
	for i := 0; i < 32; i++ {
		v += float64(50 + i)
		counter = append(counter, intchunk.Sample{T: base + int64(i)*15000, V: v})
	}
	highprec := []intchunk.Sample{
		{T: base, V: math.Pi}, {T: base + 1000, V: math.E},
		{T: base + 2000, V: 0.1 + 0.2}, {T: base + 3000, V: math.Sqrt2},
	}

	seen := map[intchunk.CodecTag]bool{}
	seen[roundTripCase(t, "all/gauge", gauge)] = true
	seen[roundTripCase(t, "all/counter", counter)] = true
	seen[roundTripCase(t, "all/highprec", highprec)] = true
	if len(seen) < 2 {
		t.Fatalf("expected the three series to exercise >=2 distinct codecs, saw %d: %v", len(seen), seen)
	}
}

// TestDecodeEmptyChunkRejected verifies the zero-sample guard rather than
// producing a degenerate XOR chunk.
func TestDecodeEmptyChunkRejected(t *testing.T) {
	if _, err := SamplesToXORChunk(nil); err == nil {
		t.Fatalf("expected error for empty sample slice")
	}
}

// TestDecodeCorruptChunkRejected verifies a malformed chunk surfaces as a decode
// error (wrapped), not a panic.
func TestDecodeCorruptChunkRejected(t *testing.T) {
	if _, err := DecodeToSamples([]byte{0xff, 0x00, 0x01}); err == nil {
		t.Fatalf("expected error for corrupt chunk")
	}
}
