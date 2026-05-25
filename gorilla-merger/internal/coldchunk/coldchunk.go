// Package coldchunk provides the decode-on-read inverse of the cold ingest
// path: it turns an intchunk-format value chunk (the best-of-N lossless cold
// codec from asap-gorilla-go/intchunk) into a standard Prometheus XOR
// (Gorilla) chunk that the rest of the merger — and a future decode-on-read
// Thanos StoreAPI — can iterate with plain chunkenc.
//
// This is the read-side mirror of internal/merger/ingest.go, which decodes
// ASAPFRG1 XOR fragments and appends them to the embedded tsdb.DB. Where ingest
// goes (XOR bytes -> samples -> tsdb), coldchunk goes (intchunk bytes ->
// samples -> XOR chunk). The edge agent does not emit intchunk yet, so this is
// not wired end-to-end; it is a tested, importable capability that the
// StoreAPI read path will build on.
//
// intchunk's value codecs (GORILLA_XOR, INT_FOR_DELTA/_DOD and their varint
// variants) are all bit-exact lossless, so re-encoding the decoded samples as a
// Prometheus XOR chunk reproduces the original float64 values exactly.
package coldchunk

import (
	"errors"
	"fmt"

	"github.com/ProjectASAP/asap-gorilla-go/intchunk"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// ErrNoSamples is returned when an intchunk decodes to zero samples; an empty
// chunk has no XOR representation worth producing.
var ErrNoSamples = errors.New("coldchunk: chunk decoded to zero samples")

// Sample is a re-export of intchunk.Sample so callers of this package do not
// have to import intchunk directly to read the decoded points.
type Sample = intchunk.Sample

// DecodeToSamples decodes a single self-contained intchunk-format chunk back to
// its (timestamp, value) samples in time order. It is a thin wrapper over
// intchunk.DecodeChunk kept here so the merger has one cold-read entry point.
func DecodeToSamples(chunk []byte) ([]Sample, error) {
	samples, err := intchunk.DecodeChunk(chunk)
	if err != nil {
		return nil, fmt.Errorf("decode intchunk: %w", err)
	}
	return samples, nil
}

// DecodeChunksToSamples decodes a sequence of concatenated intchunk chunks (e.g.
// the multiple chunks an overflow re-base cut can produce for one block) back to
// the full ordered sample stream.
func DecodeChunksToSamples(chunks [][]byte) ([]Sample, error) {
	samples, err := intchunk.DecodeChunks(chunks)
	if err != nil {
		return nil, fmt.Errorf("decode intchunks: %w", err)
	}
	return samples, nil
}

// SamplesToXORChunk re-encodes decoded samples as a standard Prometheus XOR
// (Gorilla) chunk by appending each point through the XOR Appender — the exact
// inverse of the iterate-the-XOR-chunk loop in ingest.go. The returned chunk is
// a chunkenc.Chunk (EncXOR) that iterates back to the same samples.
//
// Samples must be in non-decreasing timestamp order, which is the order both
// intchunk.DecodeChunk and the cold block layout already guarantee.
func SamplesToXORChunk(samples []Sample) (chunkenc.Chunk, error) {
	if len(samples) == 0 {
		return nil, ErrNoSamples
	}
	c := chunkenc.NewXORChunk()
	app, err := c.Appender()
	if err != nil {
		return nil, fmt.Errorf("xor appender: %w", err)
	}
	for _, s := range samples {
		app.Append(s.T, s.V)
	}
	return c, nil
}

// DecodeToXORChunk is the headline helper: it decodes an intchunk-format chunk
// and returns both the decoded samples and an equivalent Prometheus XOR chunk.
// Returning the samples alongside the chunk lets callers that only need the
// points skip re-iterating the XOR chunk, while callers feeding a chunk-based
// API (Thanos StoreAPI, tsdb append) get a ready-to-use EncXOR chunk.
func DecodeToXORChunk(chunk []byte) ([]Sample, chunkenc.Chunk, error) {
	samples, err := DecodeToSamples(chunk)
	if err != nil {
		return nil, nil, err
	}
	xc, err := SamplesToXORChunk(samples)
	if err != nil {
		return nil, nil, err
	}
	return samples, xc, nil
}
