// Package coldpart reads and writes the cold "Part" object: the on-disk /
// in-S3 container for a tenant's raw metric samples over one time block. A Part
// bundles many series, each stored as one or more best-of-N lossless value
// chunks (asap-gorilla-go/intchunk), behind a label index + symbol table so a
// reader can answer Series(matchers, mint, maxt) WITHOUT decoding any chunk
// body — chunk bodies are decoded lazily only for the series that match.
//
// This is the format-defining building block of the decode-on-read cold tier.
// It is intentionally standalone: it does NOT touch the existing fragment
// ingest -> tsdb path, and the agent does not emit this format yet. A future
// decode-on-read StoreAPI builds Series() responses on top of OpenPart +
// (*Part).Series.
//
// On-disk layout (all multi-byte integers little-endian unless noted uvarint):
//
//	[part header]  magic "ASAPCC1" | u8 version | i64 block_start_ms |
//	               i64 block_end_ms | uvarint series_count
//	[chunks]       per-series intchunk value chunks, concatenated. A series may
//	               occupy >1 intchunk chunk (intchunk cuts on residual overflow);
//	               its whole run is [chunk_off, chunk_off+chunk_len).
//	[index]        per series, sorted by labels: label refs into the symbol
//	               table, u64 chunk_off, u32 chunk_len, uvarint chunk_count,
//	               i64 min_ts, i64 max_ts.
//	[symbol table] deduped label strings; the index references them by ordinal.
//	[footer]       u64 index_off | u64 index_len | u64 symtab_off | u32 crc32c
//
// The crc32c (Castagnoli) covers every byte of the part before the crc field
// and is verified by OpenPart, so a corrupt object is rejected before any chunk
// is decoded.
package coldpart

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sort"

	"github.com/ProjectASAP/asap-gorilla-go/intchunk"
	"github.com/prometheus/prometheus/model/labels"
)

// Magic is the 7-byte part magic. Version 1 is the layout documented above.
const (
	Magic   = "ASAPCC1"
	Version = uint8(1)

	magicLen  = len(Magic)
	footerLen = 8 + 8 + 8 + 4 // index_off + index_len + symtab_off + crc32c
)

// crc32cTable is the Castagnoli polynomial table used for the footer checksum.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Errors returned by the package.
var (
	ErrShort        = errors.New("coldpart: buffer too short")
	ErrBadMagic     = errors.New("coldpart: bad magic")
	ErrBadVersion   = errors.New("coldpart: unsupported version")
	ErrBadCRC       = errors.New("coldpart: crc32c mismatch")
	ErrCorrupt      = errors.New("coldpart: corrupt part")
	ErrNoSamples    = errors.New("coldpart: series has no samples")
	ErrBadTimeRange = errors.New("coldpart: block_end_ms < block_start_ms")
)

// Sample is one timestamp (ms) / float64-value point. It re-exports
// intchunk.Sample so callers need not import intchunk to read decoded points.
type Sample = intchunk.Sample

// Series is one logical series: an immutable label set plus its time-ordered
// samples. Samples MUST be sorted by ascending timestamp before WritePart —
// the value codec and the [min_ts,max_ts] index entry assume time order.
type Series struct {
	Labels  labels.Labels
	Samples []Sample
}

// Options configures WritePart. It is reserved for future codec/layout knobs
// (e.g. the shared-timestamp grouped layout); the zero value is the default and
// is what every current caller should pass.
type Options struct{}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

// WritePart encodes header + per-series intchunk value chunks + label index +
// symbol table + footer (with crc32c) to w. Series may be supplied in any
// order; WritePart sorts them by label set so the index is canonical and a
// reader can rely on sorted-by-series order. Every series must have at least
// one sample.
//
// blockStartMs/blockEndMs describe the time block this part covers; they are
// recorded verbatim in the header and are not required to bound the per-series
// sample timestamps (the per-series [min_ts,max_ts] index entries are the
// authoritative time bounds used by Series()).
func WritePart(w io.Writer, blockStartMs, blockEndMs int64, series []Series, _ Options) error {
	if blockEndMs < blockStartMs {
		return ErrBadTimeRange
	}

	// Sort a copy by label set so we never mutate the caller's slice order.
	sorted := make([]Series, len(series))
	copy(sorted, series)
	sort.Slice(sorted, func(i, j int) bool {
		return labels.Compare(sorted[i].Labels, sorted[j].Labels) < 0
	})

	// Build the symbol table (deduped label strings) and remember each string's
	// ordinal so the index can reference symbols compactly.
	symtab := newSymbolTable()
	for i := range sorted {
		if len(sorted[i].Samples) == 0 {
			return fmt.Errorf("%w: series %d", ErrNoSamples, i)
		}
		sorted[i].Labels.Range(func(l labels.Label) {
			symtab.intern(l.Name)
			symtab.intern(l.Value)
		})
	}

	// Encode each series' chunk run and record its index entry. We assemble the
	// chunks region first (so chunk_off is known), then the index, then the
	// symbol table, then the footer.
	type idxEntry struct {
		labelRefs []uint64 // [name0,val0,name1,val1,...] ordinals into symtab
		chunkOff  uint64
		chunkLen  uint32
		chunkLens []int // per-intchunk-chunk byte lengths within the run
		minTS     int64
		maxTS     int64
	}

	var chunksRegion []byte
	entries := make([]idxEntry, len(sorted))
	for i := range sorted {
		s := sorted[i]
		res, err := intchunk.Encode(s.Samples)
		if err != nil {
			return fmt.Errorf("coldpart: encode series %d: %w", i, err)
		}
		off := uint64(len(chunksRegion))
		var runLen int
		lens := make([]int, 0, len(res.Chunks))
		for _, c := range res.Chunks {
			chunksRegion = append(chunksRegion, c...)
			runLen += len(c)
			lens = append(lens, len(c))
		}

		refs := make([]uint64, 0, sorted[i].Labels.Len()*2)
		s.Labels.Range(func(l labels.Label) {
			refs = append(refs, symtab.ref(l.Name), symtab.ref(l.Value))
		})

		entries[i] = idxEntry{
			labelRefs: refs,
			chunkOff:  off,
			chunkLen:  uint32(runLen),
			chunkLens: lens,
			minTS:     s.Samples[0].T,
			maxTS:     s.Samples[len(s.Samples)-1].T,
		}
	}

	// --- assemble the full byte image, then crc + emit ---
	var buf []byte
	bw := &writer{buf: &buf}

	// Header.
	bw.bytes([]byte(Magic))
	bw.u8(Version)
	bw.i64(blockStartMs)
	bw.i64(blockEndMs)
	bw.uvarint(uint64(len(sorted)))

	// Chunks region. Each entry's chunkOff was computed relative to the region
	// start; we add the region's absolute position so the index stores absolute
	// part offsets the reader can slice directly.
	chunksStart := uint64(len(buf))
	bw.bytes(chunksRegion)

	// Index.
	indexOff := uint64(len(buf))
	for i := range entries {
		e := entries[i]
		bw.uvarint(uint64(len(e.labelRefs) / 2)) // label count
		for _, r := range e.labelRefs {
			bw.uvarint(r)
		}
		bw.u64(chunksStart + e.chunkOff) // absolute offset of the series' chunk run
		bw.u32(e.chunkLen)
		bw.uvarint(uint64(len(e.chunkLens))) // chunk_count
		for _, cl := range e.chunkLens {
			bw.uvarint(uint64(cl)) // per-chunk byte length, in run order
		}
		bw.i64(e.minTS)
		bw.i64(e.maxTS)
	}
	indexLen := uint64(len(buf)) - indexOff

	// Symbol table.
	symtabOff := uint64(len(buf))
	bw.uvarint(uint64(len(symtab.syms)))
	for _, s := range symtab.syms {
		bw.uvarint(uint64(len(s)))
		bw.bytes([]byte(s))
	}

	// Footer (crc32c covers everything written so far).
	bw.u64(indexOff)
	bw.u64(indexLen)
	bw.u64(symtabOff)
	crc := crc32.Checksum(buf, crc32cTable)
	bw.u32(crc)

	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("coldpart: write: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Read path
// ---------------------------------------------------------------------------

// Part is a parsed, validated cold part. OpenPart populates its header and
// per-series index WITHOUT decoding any chunk body; chunk bodies are decoded
// lazily by Series(). A Part holds a reference to the underlying buffer; the
// caller must keep that buffer alive (and unmodified) for the Part's lifetime.
type Part struct {
	Version      uint8
	BlockStartMs int64
	BlockEndMs   int64

	buf     []byte
	symbols []string
	series  []indexSeries
}

// indexSeries is one parsed index entry. Labels are materialized eagerly (cheap
// — just symbol-table lookups); only the chunk body is deferred.
type indexSeries struct {
	lbls      labels.Labels
	chunkOff  uint64
	chunkLen  uint32
	chunkLens []uint64 // per-intchunk-chunk byte lengths within the run
	minTS     int64
	maxTS     int64
}

// SeriesData is a matched series with its decoded, time-ordered samples.
type SeriesData struct {
	Labels  labels.Labels
	Samples []Sample
}

// OpenPart validates the magic/version/crc and parses the footer -> index ->
// symbol table of b. It does NOT decode chunk bodies. The returned Part borrows
// b; do not mutate b while the Part is in use.
func OpenPart(b []byte) (*Part, error) {
	if len(b) < magicLen+1+8+8+1+footerLen {
		return nil, ErrShort
	}
	if string(b[:magicLen]) != Magic {
		return nil, ErrBadMagic
	}

	// Verify crc32c over everything but the trailing 4-byte crc field.
	stored := binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(b[:len(b)-4], crc32cTable) != stored {
		return nil, ErrBadCRC
	}

	// Header.
	r := &reader{buf: b, pos: magicLen}
	ver, err := r.u8()
	if err != nil {
		return nil, err
	}
	if ver != Version {
		return nil, fmt.Errorf("%w: %d", ErrBadVersion, ver)
	}
	blockStart, err := r.i64()
	if err != nil {
		return nil, err
	}
	blockEnd, err := r.i64()
	if err != nil {
		return nil, err
	}
	seriesCount, err := r.uvarint()
	if err != nil {
		return nil, err
	}

	// Footer.
	footerStart := len(b) - footerLen
	fr := &reader{buf: b, pos: footerStart}
	indexOff, _ := fr.u64()
	indexLen, _ := fr.u64()
	symtabOff, _ := fr.u64()
	if indexOff > uint64(len(b)) || symtabOff > uint64(len(b)) ||
		indexOff+indexLen > uint64(len(b)) || indexOff > symtabOff {
		return nil, fmt.Errorf("%w: footer offsets out of range", ErrCorrupt)
	}

	// Symbol table: [uvarint count] then count x ([uvarint len] bytes).
	sr := &reader{buf: b, pos: int(symtabOff)}
	symCount, err := sr.uvarint()
	if err != nil {
		return nil, err
	}
	symbols := make([]string, 0, symCount)
	for i := uint64(0); i < symCount; i++ {
		n, err := sr.uvarint()
		if err != nil {
			return nil, err
		}
		s, err := sr.take(int(n))
		if err != nil {
			return nil, err
		}
		symbols = append(symbols, string(s))
	}

	// Index: seriesCount entries, each ending before symtabOff.
	ir := &reader{buf: b, pos: int(indexOff)}
	parsed := make([]indexSeries, 0, seriesCount)
	for i := uint64(0); i < seriesCount; i++ {
		nLabels, err := ir.uvarint()
		if err != nil {
			return nil, err
		}
		lb := labels.NewBuilder(labels.EmptyLabels())
		for j := uint64(0); j < nLabels; j++ {
			nameRef, err := ir.uvarint()
			if err != nil {
				return nil, err
			}
			valRef, err := ir.uvarint()
			if err != nil {
				return nil, err
			}
			if nameRef >= symCount || valRef >= symCount {
				return nil, fmt.Errorf("%w: symbol ref out of range", ErrCorrupt)
			}
			lb.Set(symbols[nameRef], symbols[valRef])
		}
		chunkOff, err := ir.u64()
		if err != nil {
			return nil, err
		}
		chunkLen, err := ir.u32()
		if err != nil {
			return nil, err
		}
		chunkCount, err := ir.uvarint()
		if err != nil {
			return nil, err
		}
		chunkLens := make([]uint64, 0, chunkCount)
		var lensSum uint64
		for j := uint64(0); j < chunkCount; j++ {
			cl, err := ir.uvarint()
			if err != nil {
				return nil, err
			}
			chunkLens = append(chunkLens, cl)
			lensSum += cl
		}
		minTS, err := ir.i64()
		if err != nil {
			return nil, err
		}
		maxTS, err := ir.i64()
		if err != nil {
			return nil, err
		}
		if chunkOff+uint64(chunkLen) > uint64(len(b)) {
			return nil, fmt.Errorf("%w: chunk run out of range", ErrCorrupt)
		}
		if lensSum != uint64(chunkLen) {
			return nil, fmt.Errorf("%w: chunk length sum %d != run length %d", ErrCorrupt, lensSum, chunkLen)
		}
		parsed = append(parsed, indexSeries{
			lbls:      lb.Labels(),
			chunkOff:  chunkOff,
			chunkLen:  chunkLen,
			chunkLens: chunkLens,
			minTS:     minTS,
			maxTS:     maxTS,
		})
	}

	return &Part{
		Version:      ver,
		BlockStartMs: blockStart,
		BlockEndMs:   blockEnd,
		buf:          b,
		symbols:      symbols,
		series:       parsed,
	}, nil
}

// NumSeries reports how many series the part indexes (without decoding any).
func (p *Part) NumSeries() int { return len(p.series) }

// Series returns every indexed series whose [min_ts,max_ts] overlaps the
// half-open-ish inclusive window [mintMs,maxtMs] AND whose labels satisfy all
// matchers, each with its samples decoded. Chunk bodies are decoded lazily —
// only for the matched, time-overlapping series. The result is in part-stored
// (sorted-by-labels) order.
//
// Time filtering uses inclusive overlap: a series is included when
// series.min_ts <= maxtMs && series.max_ts >= mintMs. Matcher filtering applies
// every matcher with AND semantics via (*labels.Matcher).Matches against the
// series' value for that matcher's label name (the empty string when absent),
// which makes =, !=, =~, !~ behave exactly as Prometheus selectors do.
func (p *Part) Series(matchers []*labels.Matcher, mintMs, maxtMs int64) ([]SeriesData, error) {
	var out []SeriesData
	for i := range p.series {
		s := &p.series[i]
		// Time-window overlap (inclusive on both ends).
		if s.minTS > maxtMs || s.maxTS < mintMs {
			continue
		}
		if !matchesAll(s.lbls, matchers) {
			continue
		}
		samples, err := p.decodeSeries(s)
		if err != nil {
			return nil, err
		}
		out = append(out, SeriesData{Labels: s.lbls, Samples: samples})
	}
	return out, nil
}

// decodeSeries decodes the chunk run of one indexed series. The run is the
// concatenation of one-or-more self-contained intchunk chunks (intchunk cuts a
// series into several chunks on residual overflow). intchunk exposes no
// "bytes-consumed" boundary on a concatenation, so the part index records each
// chunk's byte length explicitly; decodeSeries re-slices the run on those
// boundaries and decodes each chunk with intchunk. The common single-chunk case
// decodes the whole run directly.
func (p *Part) decodeSeries(s *indexSeries) ([]Sample, error) {
	run := p.buf[s.chunkOff : s.chunkOff+uint64(s.chunkLen)]
	if len(s.chunkLens) <= 1 {
		samples, err := intchunk.DecodeChunk(run)
		if err != nil {
			return nil, fmt.Errorf("coldpart: decode chunk: %w", err)
		}
		return samples, nil
	}
	chunks := splitChunks(run, s.chunkLens)
	samples, err := intchunk.DecodeChunks(chunks)
	if err != nil {
		return nil, fmt.Errorf("coldpart: decode chunks: %w", err)
	}
	return samples, nil
}

// splitChunks slices run into the sub-chunks described by lens. The caller
// (OpenPart) has already validated that the lengths sum to len(run).
func splitChunks(run []byte, lens []uint64) [][]byte {
	chunks := make([][]byte, 0, len(lens))
	off := uint64(0)
	for _, l := range lens {
		chunks = append(chunks, run[off:off+l])
		off += l
	}
	return chunks
}

// matchesAll reports whether ls satisfies every matcher (AND semantics).
func matchesAll(ls labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if m == nil {
			continue
		}
		if !m.Matches(ls.Get(m.Name)) {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// symbol table (write side)
// ---------------------------------------------------------------------------

type symbolTable struct {
	syms []string
	idx  map[string]uint64
}

func newSymbolTable() *symbolTable {
	return &symbolTable{idx: make(map[string]uint64)}
}

// intern adds s to the table if absent.
func (t *symbolTable) intern(s string) {
	if _, ok := t.idx[s]; ok {
		return
	}
	t.idx[s] = uint64(len(t.syms))
	t.syms = append(t.syms, s)
}

// ref returns the ordinal of an already-interned string.
func (t *symbolTable) ref(s string) uint64 { return t.idx[s] }

// ---------------------------------------------------------------------------
// little-endian / varint writer + reader helpers
// ---------------------------------------------------------------------------

type writer struct{ buf *[]byte }

func (w *writer) bytes(b []byte) { *w.buf = append(*w.buf, b...) }
func (w *writer) u8(v uint8)     { *w.buf = append(*w.buf, v) }

func (w *writer) u32(v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	*w.buf = append(*w.buf, b[:]...)
}

func (w *writer) u64(v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	*w.buf = append(*w.buf, b[:]...)
}

func (w *writer) i64(v int64) { w.u64(uint64(v)) }

func (w *writer) uvarint(v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	*w.buf = append(*w.buf, tmp[:n]...)
}

type reader struct {
	buf []byte
	pos int
}

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.buf) {
		return nil, ErrShort
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

func (r *reader) u8() (uint8, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *reader) u32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *reader) u64() (uint64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *reader) i64() (int64, error) {
	v, err := r.u64()
	return int64(v), err
}

func (r *reader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.buf[r.pos:])
	if n <= 0 {
		return 0, ErrCorrupt
	}
	r.pos += n
	return v, nil
}
