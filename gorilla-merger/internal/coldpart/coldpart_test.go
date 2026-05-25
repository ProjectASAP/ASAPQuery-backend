package coldpart

import (
	"bytes"
	"hash/crc32"
	"math"
	"testing"

	"github.com/ProjectASAP/asap-gorilla-go/intchunk"
	"github.com/prometheus/prometheus/model/labels"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// lbls builds a label set from alternating name/value pairs.
func lbls(kv ...string) labels.Labels {
	if len(kv)%2 != 0 {
		panic("lbls: odd number of args")
	}
	pairs := make([]labels.Label, 0, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		pairs = append(pairs, labels.Label{Name: kv[i], Value: kv[i+1]})
	}
	return labels.New(pairs...)
}

// mustMatcher builds a matcher or fails the test.
func mustMatcher(t *testing.T, ty labels.MatchType, n, v string) *labels.Matcher {
	t.Helper()
	m, err := labels.NewMatcher(ty, n, v)
	if err != nil {
		t.Fatalf("NewMatcher(%v,%q,%q): %v", ty, n, v, err)
	}
	return m
}

// assertSamplesEqual fails unless got==want in length, every timestamp, and
// every value BIT-EXACTLY (bit-exactness is the whole point for the
// high-precision-float / Gorilla-fallback case).
func assertSamplesEqual(t *testing.T, what string, got, want []Sample) {
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

// findSeries returns the SeriesData whose labels equal want, or fails.
func findSeries(t *testing.T, got []SeriesData, want labels.Labels) SeriesData {
	t.Helper()
	for _, sd := range got {
		if labels.Compare(sd.Labels, want) == 0 {
			return sd
		}
	}
	t.Fatalf("series %s not found in result", want.String())
	return SeriesData{}
}

// seriesValues builds an ascending-timestamp series at 1s cadence from values.
func seriesValues(start int64, vals ...float64) []Sample {
	out := make([]Sample, len(vals))
	for i, v := range vals {
		out[i] = Sample{T: start + int64(i)*1000, V: v}
	}
	return out
}

// roundTrip writes the series to a part, reopens it, queries all series with
// the widest window, and returns the parsed Part plus the matched series.
func roundTrip(t *testing.T, blockStart, blockEnd int64, series []Series) (*Part, []SeriesData) {
	t.Helper()
	var buf bytes.Buffer
	if err := WritePart(&buf, blockStart, blockEnd, series, Options{}); err != nil {
		t.Fatalf("WritePart: %v", err)
	}
	p, err := OpenPart(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenPart: %v", err)
	}
	got, err := p.Series(nil, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	return p, got
}

// ---------------------------------------------------------------------------
// codec-exercising series (one per intchunk sub-codec)
// ---------------------------------------------------------------------------
//
// These value patterns were verified to select the named intchunk codec via
// best-of-N (see the codec assertions in TestSubCodecsRoundTrip).

func gaugeIntSeries() []Sample { // -> INT_FOR_DELTA (fixed-width)
	return seriesValues(1_000_000, 10, 11, 9, 12, 8, 15, 7, 20, 5, 25)
}

func counterLinearSeries() []Sample { // -> INT_FOR_DOD
	vals := make([]float64, 20)
	for i := range vals {
		vals[i] = float64(i * 7)
	}
	return seriesValues(1_000_000, vals...)
}

func decimalSeries() []Sample { // fixed-decimal gauge -> INT_FOR_DELTA
	return seriesValues(1_000_000, 1.1, 1.2, 1.3, 1.1, 1.5, 1.9, 1.2)
}

func skewedSeries() []Sample { // skewed residuals -> INT_FOR_DELTA_VARINT
	out := make([]Sample, 0, 30)
	base := int64(0)
	for i := 0; i < 30; i++ {
		if i == 15 {
			base += 1_000_000
		} else {
			base++
		}
		out = append(out, Sample{T: 1_000_000 + int64(i)*1000, V: float64(base)})
	}
	return out
}

func highPrecSeries() []Sample { // true high-precision floats -> GORILLA_XOR fallback
	return seriesValues(1_000_000,
		0.1234567890123456, 3.141592653589793,
		2.718281828459045, 1.4142135623730951, math.Pi*1e-7)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// TestSubCodecsRoundTrip writes one series per intchunk sub-codec into a single
// part and asserts each decodes back BIT-EXACTLY, and that our value patterns
// actually exercise the distinct codecs (gauge/counter/decimal/varint/float).
func TestSubCodecsRoundTrip(t *testing.T) {
	type tc struct {
		name    string
		lset    labels.Labels
		samples []Sample
		wantTag intchunk.CodecTag
	}
	cases := []tc{
		{"gauge", lbls("__name__", "cpu", "kind", "gauge"), gaugeIntSeries(), intchunk.CodecIntForDelta},
		{"counter", lbls("__name__", "reqs", "kind", "counter"), counterLinearSeries(), intchunk.CodecIntForDoD},
		{"decimal", lbls("__name__", "temp", "kind", "decimal"), decimalSeries(), intchunk.CodecIntForDelta},
		{"skewed", lbls("__name__", "skew", "kind", "varint"), skewedSeries(), intchunk.CodecIntForDeltaVarint},
		{"highprec", lbls("__name__", "ratio", "kind", "float"), highPrecSeries(), intchunk.CodecGorillaXOR},
	}

	// Assert each pattern selects the intended codec, so the round-trip below
	// genuinely exercises every sub-codec path.
	for _, c := range cases {
		res, err := intchunk.Encode(c.samples)
		if err != nil {
			t.Fatalf("%s: intchunk.Encode: %v", c.name, err)
		}
		if res.Tag != c.wantTag {
			t.Fatalf("%s: codec = %s, want %s", c.name, res.Tag, c.wantTag)
		}
	}

	series := make([]Series, len(cases))
	for i, c := range cases {
		series[i] = Series{Labels: c.lset, Samples: c.samples}
	}

	_, got := roundTrip(t, 1_000_000, 2_000_000, series)
	if len(got) != len(cases) {
		t.Fatalf("got %d series, want %d", len(got), len(cases))
	}
	for _, c := range cases {
		sd := findSeries(t, got, c.lset)
		assertSamplesEqual(t, c.name, sd.Samples, c.samples)
	}
}

// TestHeaderAndCount checks the parsed header fields and series count.
func TestHeaderAndCount(t *testing.T) {
	series := []Series{
		{Labels: lbls("__name__", "a"), Samples: seriesValues(0, 1, 2, 3)},
		{Labels: lbls("__name__", "b"), Samples: seriesValues(0, 4, 5, 6)},
	}
	p, got := roundTrip(t, 1000, 7_201_000, series)
	if p.Version != Version {
		t.Fatalf("version = %d, want %d", p.Version, Version)
	}
	if p.BlockStartMs != 1000 || p.BlockEndMs != 7_201_000 {
		t.Fatalf("block range = [%d,%d], want [1000,7201000]", p.BlockStartMs, p.BlockEndMs)
	}
	if p.NumSeries() != 2 {
		t.Fatalf("NumSeries = %d, want 2", p.NumSeries())
	}
	if len(got) != 2 {
		t.Fatalf("Series returned %d, want 2", len(got))
	}
}

// TestSeriesSortedByLabels verifies series come back canonical (sorted) order
// regardless of input order.
func TestSeriesSortedByLabels(t *testing.T) {
	series := []Series{
		{Labels: lbls("__name__", "zeta"), Samples: seriesValues(0, 1)},
		{Labels: lbls("__name__", "alpha"), Samples: seriesValues(0, 2)},
		{Labels: lbls("__name__", "mu"), Samples: seriesValues(0, 3)},
	}
	_, got := roundTrip(t, 0, 1000, series)
	want := []string{"alpha", "mu", "zeta"}
	if len(got) != len(want) {
		t.Fatalf("got %d series", len(got))
	}
	for i, w := range want {
		if name := got[i].Labels.Get("__name__"); name != w {
			t.Fatalf("series[%d] = %q, want %q", i, name, w)
		}
	}
}

// TestMatcherFiltering exercises =, !=, =~ matchers (AND semantics) and a
// matcher against an absent label name.
func TestMatcherFiltering(t *testing.T) {
	series := []Series{
		{Labels: lbls("__name__", "http_requests", "job", "api", "code", "200"), Samples: seriesValues(0, 1, 2)},
		{Labels: lbls("__name__", "http_requests", "job", "api", "code", "500"), Samples: seriesValues(0, 3, 4)},
		{Labels: lbls("__name__", "http_requests", "job", "web", "code", "200"), Samples: seriesValues(0, 5, 6)},
		{Labels: lbls("__name__", "cpu_seconds", "job", "api"), Samples: seriesValues(0, 7, 8)},
	}
	var buf bytes.Buffer
	if err := WritePart(&buf, 0, 10_000, series, Options{}); err != nil {
		t.Fatalf("WritePart: %v", err)
	}
	p, err := OpenPart(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenPart: %v", err)
	}

	full := int64(math.MaxInt64)
	min := int64(math.MinInt64)

	// =  exact name
	got, _ := p.Series([]*labels.Matcher{
		mustMatcher(t, labels.MatchEqual, "__name__", "http_requests"),
	}, min, full)
	if len(got) != 3 {
		t.Fatalf("= filter: got %d, want 3", len(got))
	}

	// = AND =  (name + job)
	got, _ = p.Series([]*labels.Matcher{
		mustMatcher(t, labels.MatchEqual, "__name__", "http_requests"),
		mustMatcher(t, labels.MatchEqual, "job", "api"),
	}, min, full)
	if len(got) != 2 {
		t.Fatalf("=&= filter: got %d, want 2", len(got))
	}

	// !=  excludes code 500
	got, _ = p.Series([]*labels.Matcher{
		mustMatcher(t, labels.MatchEqual, "__name__", "http_requests"),
		mustMatcher(t, labels.MatchNotEqual, "code", "500"),
	}, min, full)
	if len(got) != 2 {
		t.Fatalf("!= filter: got %d, want 2", len(got))
	}

	// =~  regex on job
	got, _ = p.Series([]*labels.Matcher{
		mustMatcher(t, labels.MatchRegexp, "job", "a.*"),
	}, min, full)
	// matches api (http x2) + api (cpu) = 3
	if len(got) != 3 {
		t.Fatalf("=~ filter: got %d, want 3", len(got))
	}

	// !~ regex on code, with code label absent on cpu_seconds.
	// MatchNotRegexp on an empty value: "200" !~ "5.." is true, "" !~ "5.." true.
	got, _ = p.Series([]*labels.Matcher{
		mustMatcher(t, labels.MatchNotRegexp, "code", "5.."),
	}, min, full)
	// excludes only the code=500 series => 3 remain
	if len(got) != 3 {
		t.Fatalf("!~ filter: got %d, want 3", len(got))
	}

	// = on an absent label matches the empty string: code="" hits cpu_seconds.
	got, _ = p.Series([]*labels.Matcher{
		mustMatcher(t, labels.MatchEqual, "code", ""),
	}, min, full)
	if len(got) != 1 || got[0].Labels.Get("__name__") != "cpu_seconds" {
		t.Fatalf(`code="" filter: got %d, want 1 (cpu_seconds)`, len(got))
	}
}

// TestTimeWindowOverlap verifies inclusive [min_ts,max_ts] vs [mint,maxt]
// overlap filtering and that decoded samples are NOT clipped to the window
// (the read primitive returns the whole matched series; clipping is the
// caller's concern, matching the design's chunk-granularity overlap).
func TestTimeWindowOverlap(t *testing.T) {
	series := []Series{
		{Labels: lbls("__name__", "early"), Samples: seriesValues(1000, 1, 2, 3)}, // [1000,3000]
		{Labels: lbls("__name__", "mid"), Samples: seriesValues(5000, 4, 5, 6)},   // [5000,7000]
		{Labels: lbls("__name__", "late"), Samples: seriesValues(10000, 7, 8, 9)}, // [10000,12000]
	}
	var buf bytes.Buffer
	if err := WritePart(&buf, 0, 20_000, series, Options{}); err != nil {
		t.Fatalf("WritePart: %v", err)
	}
	p, err := OpenPart(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenPart: %v", err)
	}

	// Window [4000,8000] overlaps only "mid".
	got, _ := p.Series(nil, 4000, 8000)
	if len(got) != 1 || got[0].Labels.Get("__name__") != "mid" {
		t.Fatalf("window [4000,8000]: got %d series, want only mid", len(got))
	}

	// Boundary-touch: window ending exactly at early's max_ts (3000) overlaps.
	got, _ = p.Series(nil, 3000, 3000)
	if len(got) != 1 || got[0].Labels.Get("__name__") != "early" {
		t.Fatalf("window [3000,3000]: got %d series, want only early", len(got))
	}

	// Window entirely before everything.
	got, _ = p.Series(nil, 0, 999)
	if len(got) != 0 {
		t.Fatalf("window [0,999]: got %d series, want 0", len(got))
	}

	// Wide window: all three, samples unclipped.
	got, _ = p.Series(nil, math.MinInt64, math.MaxInt64)
	if len(got) != 3 {
		t.Fatalf("wide window: got %d series, want 3", len(got))
	}
	mid := findSeries(t, got, lbls("__name__", "mid"))
	assertSamplesEqual(t, "mid unclipped", mid.Samples, seriesValues(5000, 4, 5, 6))
}

// TestCRCCorruption mutates a body byte and asserts OpenPart rejects it.
func TestCRCCorruption(t *testing.T) {
	series := []Series{{Labels: lbls("__name__", "x"), Samples: seriesValues(0, 1, 2, 3)}}
	var buf bytes.Buffer
	if err := WritePart(&buf, 0, 5000, series, Options{}); err != nil {
		t.Fatalf("WritePart: %v", err)
	}
	b := buf.Bytes()

	// Flip a byte in the chunks region (well before the footer).
	corrupt := append([]byte(nil), b...)
	corrupt[magicLen+1+8+8+1] ^= 0xFF // first chunk byte after header
	if _, err := OpenPart(corrupt); err != ErrBadCRC {
		t.Fatalf("corrupt body: got err %v, want ErrBadCRC", err)
	}

	// Flip a byte in the stored crc itself.
	corrupt2 := append([]byte(nil), b...)
	corrupt2[len(corrupt2)-1] ^= 0x01
	if _, err := OpenPart(corrupt2); err != ErrBadCRC {
		t.Fatalf("corrupt crc: got err %v, want ErrBadCRC", err)
	}

	// Untouched bytes still open fine.
	if _, err := OpenPart(b); err != nil {
		t.Fatalf("clean part failed to open: %v", err)
	}
}

// TestBadMagicAndVersion checks header validation.
func TestBadMagicAndVersion(t *testing.T) {
	series := []Series{{Labels: lbls("__name__", "x"), Samples: seriesValues(0, 1, 2)}}
	var buf bytes.Buffer
	if err := WritePart(&buf, 0, 5000, series, Options{}); err != nil {
		t.Fatalf("WritePart: %v", err)
	}
	b := buf.Bytes()

	badMagic := append([]byte(nil), b...)
	badMagic[0] = 'X'
	if _, err := OpenPart(badMagic); err != ErrBadMagic {
		t.Fatalf("bad magic: got %v, want ErrBadMagic", err)
	}

	// Bumping the version byte changes the body, so recompute the crc to isolate
	// the version check from the crc check.
	badVer := append([]byte(nil), b...)
	badVer[magicLen] = 99
	// recompute crc over all-but-last-4
	fixCRC(badVer)
	if _, err := OpenPart(badVer); err == nil || err == ErrBadCRC {
		t.Fatalf("bad version: got %v, want ErrBadVersion", err)
	}
}

// TestEmptyPart writes a part with zero series and reads it back.
func TestEmptyPart(t *testing.T) {
	var buf bytes.Buffer
	if err := WritePart(&buf, 100, 200, nil, Options{}); err != nil {
		t.Fatalf("WritePart(empty): %v", err)
	}
	p, err := OpenPart(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenPart(empty): %v", err)
	}
	if p.NumSeries() != 0 {
		t.Fatalf("NumSeries = %d, want 0", p.NumSeries())
	}
	if p.BlockStartMs != 100 || p.BlockEndMs != 200 {
		t.Fatalf("block range = [%d,%d], want [100,200]", p.BlockStartMs, p.BlockEndMs)
	}
	got, err := p.Series(nil, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Series(empty): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Series(empty) returned %d", len(got))
	}
}

// TestSingleSampleSeries covers the n==1 chunk edge case.
func TestSingleSampleSeries(t *testing.T) {
	series := []Series{
		{Labels: lbls("__name__", "one"), Samples: []Sample{{T: 42, V: 3.5}}},
	}
	_, got := roundTrip(t, 0, 100, series)
	if len(got) != 1 {
		t.Fatalf("got %d series, want 1", len(got))
	}
	assertSamplesEqual(t, "single", got[0].Samples, []Sample{{T: 42, V: 3.5}})
}

// TestEmptySamplesRejected: a series with no samples is an error at write time.
func TestEmptySamplesRejected(t *testing.T) {
	series := []Series{
		{Labels: lbls("__name__", "ok"), Samples: seriesValues(0, 1, 2)},
		{Labels: lbls("__name__", "empty"), Samples: nil},
	}
	var buf bytes.Buffer
	err := WritePart(&buf, 0, 1000, series, Options{})
	if err == nil {
		t.Fatal("WritePart with empty series: want error, got nil")
	}
}

// TestBadTimeRange: block_end < block_start is rejected.
func TestBadTimeRange(t *testing.T) {
	series := []Series{{Labels: lbls("__name__", "x"), Samples: seriesValues(0, 1)}}
	var buf bytes.Buffer
	if err := WritePart(&buf, 5000, 1000, series, Options{}); err != ErrBadTimeRange {
		t.Fatalf("bad range: got %v, want ErrBadTimeRange", err)
	}
}

// TestShortBuffer: a truncated buffer is rejected, not panicked.
func TestShortBuffer(t *testing.T) {
	if _, err := OpenPart([]byte("AS")); err != ErrShort {
		t.Fatalf("tiny buffer: got %v, want ErrShort", err)
	}
	if _, err := OpenPart(nil); err != ErrShort {
		t.Fatalf("nil buffer: got %v, want ErrShort", err)
	}
}

// TestSymbolDedup checks the symbol table dedups shared label strings: a part
// with many series sharing __name__/job must round-trip exactly while the
// symbol table stays far smaller than the naive sum of label strings.
func TestSymbolDedup(t *testing.T) {
	const n = 30
	var series []Series
	for i := 0; i < n; i++ {
		series = append(series, Series{
			// __name__ ("node_cpu") and job ("node-exporter") repeat across all
			// series; only the instance value is unique.
			Labels:  lbls("__name__", "node_cpu", "job", "node-exporter", "instance", "host-"+itoa(i)),
			Samples: seriesValues(int64(i)*1000, float64(i), float64(i)+1),
		})
	}
	var buf bytes.Buffer
	if err := WritePart(&buf, 0, 100_000, series, Options{}); err != nil {
		t.Fatalf("WritePart: %v", err)
	}
	p, err := OpenPart(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenPart: %v", err)
	}
	got, _ := p.Series(nil, math.MinInt64, math.MaxInt64)
	if len(got) != n {
		t.Fatalf("got %d series, want %d", len(got), n)
	}
	// Distinct symbols = {__name__, node_cpu, job, node-exporter, instance} (5)
	// + n unique instance values = n+5. The naive (non-deduped) count would be
	// n*6. Assert dedup happened.
	wantSyms := n + 5
	if len(p.symbols) != wantSyms {
		t.Fatalf("symbol table = %d symbols, want %d (deduped)", len(p.symbols), wantSyms)
	}
	// Every series still resolves by its unique instance via a matcher.
	for i := 0; i < n; i++ {
		want := lbls("__name__", "node_cpu", "job", "node-exporter", "instance", "host-"+itoa(i))
		sd := findSeries(t, got, want)
		assertSamplesEqual(t, "dedup series "+itoa(i), sd.Samples, seriesValues(int64(i)*1000, float64(i), float64(i)+1))
	}
}

// itoa is a tiny non-allocating-ish int->string for test label values.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

// TestSplitChunksMultiRun directly validates the multi-chunk run split path:
// the part index records per-chunk byte lengths so a series spanning >1
// intchunk chunk (intchunk's overflow re-base cut) decodes correctly. Because
// best-of-N rarely *picks* a multi-chunk INT encoding over Gorilla, we validate
// the splitter against a hand-built concatenation of two real intchunk chunks.
func TestSplitChunksMultiRun(t *testing.T) {
	a := seriesValues(0, 1, 2, 3, 4, 5)
	b := seriesValues(6000, 100, 101, 102)
	encA, err := intchunk.Encode(a)
	if err != nil {
		t.Fatalf("encode a: %v", err)
	}
	encB, err := intchunk.Encode(b)
	if err != nil {
		t.Fatalf("encode b: %v", err)
	}
	// Both are single-chunk; concatenate to simulate a 2-chunk run.
	var run []byte
	var lens []uint64
	for _, c := range append(append([][]byte{}, encA.Chunks...), encB.Chunks...) {
		run = append(run, c...)
		lens = append(lens, uint64(len(c)))
	}
	chunks := splitChunks(run, lens)
	if len(chunks) != len(lens) {
		t.Fatalf("split into %d, want %d", len(chunks), len(lens))
	}
	got, err := intchunk.DecodeChunks(chunks)
	if err != nil {
		t.Fatalf("DecodeChunks: %v", err)
	}
	want := append(append([]Sample{}, a...), b...)
	assertSamplesEqual(t, "multi-run", got, want)
}

// fixCRC recomputes the trailing crc32c over the part so a test can mutate
// header/index bytes and still pass the crc gate (to isolate other checks).
func fixCRC(b []byte) {
	crc := crc32.Checksum(b[:len(b)-4], crc32.MakeTable(crc32.Castagnoli))
	b[len(b)-4] = byte(crc)
	b[len(b)-3] = byte(crc >> 8)
	b[len(b)-2] = byte(crc >> 16)
	b[len(b)-1] = byte(crc >> 24)
}
