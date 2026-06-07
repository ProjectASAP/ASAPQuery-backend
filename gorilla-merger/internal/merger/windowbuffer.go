package merger

import (
	"math"
	"sort"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
)

// bufferedChunk is one raw XOR chunk buffered for a single series, captured
// WITHOUT decoding its samples. MinTime/MaxTime/NumSamples come straight from
// the ASAPFRG1 fragment header; Data is the raw chunkenc.EncXOR payload exactly
// as it arrived on the wire (the agent's Gorilla-XOR chunk).
type bufferedChunk struct {
	MinTime    int64
	MaxTime    int64
	NumSamples int
	Data       []byte
}

// bufferedSeries accumulates the raw chunks for one series within one window.
// labels are the series' own labels (no external labels — those are stamped at
// query time / block-ship time, matching the historic ingest behaviour).
type bufferedSeries struct {
	lset   labels.Labels
	chunks []bufferedChunk
}

// windowBuffer holds, per close-able window, the raw chunks buffered for every
// series seen in that window. It is the decode-free replacement for the old
// per-sample tsdb Appender: ingest drops raw chunk bytes here, and on window
// close blockbuild.go stitches them directly into a Prometheus block.
//
// Windows are keyed by their start timestamp (floor(maxt/window)*window). A
// fragment is assigned to the window its MaxTime falls into; on close, all
// series for that window are flushed.
type windowBuffer struct {
	mu       sync.Mutex
	windowMs int64
	// windows maps windowStart -> (series fingerprint -> bufferedSeries).
	windows map[int64]map[uint64]*bufferedSeries
}

func newWindowBuffer(windowMs int64) *windowBuffer {
	if windowMs <= 0 {
		windowMs = defaultWindowMs
	}
	return &windowBuffer{
		windowMs: windowMs,
		windows:  make(map[int64]map[uint64]*bufferedSeries),
	}
}

// windowStartFor returns the start timestamp of the window that t belongs to.
func (b *windowBuffer) windowStartFor(t int64) int64 {
	// Floor division that works for negative timestamps too.
	w := t / b.windowMs
	if t < 0 && t%b.windowMs != 0 {
		w--
	}
	return w * b.windowMs
}

// add buffers one raw chunk for the given series. The chunk is assigned to the
// window containing its MaxTime (the chunk's last sample), so a chunk is
// flushed once its window closes. ls must be the series' own (sorted) labels;
// data is copied so it does not alias the caller's frame buffer.
func (b *windowBuffer) add(ls labels.Labels, c bufferedChunk) {
	wStart := b.windowStartFor(c.MaxTime)
	fp := ls.Hash()

	cp := make([]byte, len(c.Data))
	copy(cp, c.Data)
	c.Data = cp

	b.mu.Lock()
	defer b.mu.Unlock()
	w, ok := b.windows[wStart]
	if !ok {
		w = make(map[uint64]*bufferedSeries)
		b.windows[wStart] = w
	}
	bs, ok := w[fp]
	if !ok {
		bs = &bufferedSeries{lset: ls}
		w[fp] = bs
	}
	bs.chunks = append(bs.chunks, c)
}

// closableWindows returns the start timestamps of every window whose end
// (start+window) plus grace is <= now, i.e. windows that can be flushed because
// no more in-order fragments are expected for them. Returned sorted ascending.
func (b *windowBuffer) closableWindows(now, graceMs int64) []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []int64
	for wStart := range b.windows {
		if wStart+b.windowMs+graceMs <= now {
			out = append(out, wStart)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// allWindows returns every buffered window start, sorted ascending. Used on
// shutdown to flush everything regardless of grace.
func (b *windowBuffer) allWindows() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]int64, 0, len(b.windows))
	for wStart := range b.windows {
		out = append(out, wStart)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// takeStats reports what de-overlap did to a window's series at flush time.
type takeStats struct {
	// mergedSeries is the number of series whose overlapping chunks were
	// decoded + re-encoded losslessly into ordered chunks.
	mergedSeries int
	// mergedSamples is the total samples carried through those re-encodes.
	mergedSamples int
	// droppedChunks counts chunks dropped by the defensive fallback when a
	// lossless merge errored (should be 0 in normal operation).
	droppedChunks int
}

// take removes and returns the buffered series for one window, each series'
// chunks sorted by MinTime and de-overlapped (the ordering AND non-overlap the
// block writer requires). Also returns stats describing any de-overlap work.
// Returns nil if the window is empty/absent.
//
// De-overlap is essential: the Prometheus index writer requires each series'
// chunks to be strictly increasing — a new chunk's MinTime must be HIGHER than
// the previous chunk's MaxTime. Raw-buffer producers keep per-event timestamps,
// so two cold fragments for the same hot series (from adjacent agent windows)
// can carry overlapping event-time ranges; without de-overlapping, the index
// writer rejects that one series and `buildBlock` aborts the WHOLE window's
// block — losing every series in it.
//
// The cold tier is the lossless raw backup, so we do NOT drop overlapping
// samples: `deoverlapSeriesChunks` decodes the overlapping chunks, merges their
// samples by timestamp, and re-encodes into ordered XOR chunks (XOR is exact
// for (int64 ts, float64 value)). Disjoint series keep their verbatim bytes
// (decode-free fast path). Only if a merge unexpectedly errors do we fall back
// to dropping the offending chunk — strictly to keep the rest of the window's
// block buildable rather than lose every series in it.
func (b *windowBuffer) take(wStart int64) ([]*bufferedSeries, takeStats) {
	b.mu.Lock()
	w, ok := b.windows[wStart]
	if ok {
		delete(b.windows, wStart)
	}
	b.mu.Unlock()
	if !ok || len(w) == 0 {
		return nil, takeStats{}
	}

	out := make([]*bufferedSeries, 0, len(w))
	var stats takeStats
	for _, bs := range w {
		sort.Slice(bs.chunks, func(i, j int) bool {
			if bs.chunks[i].MinTime != bs.chunks[j].MinTime {
				return bs.chunks[i].MinTime < bs.chunks[j].MinTime
			}
			return bs.chunks[i].MaxTime < bs.chunks[j].MaxTime
		})
		merged, didMerge, n, err := deoverlapSeriesChunks(bs.chunks)
		if err != nil {
			// Lossless merge failed (corrupt chunk?) — fall back to dropping
			// overlaps so one bad series can't abort the whole window's block.
			kept := bs.chunks[:0]
			lastMax := int64(math.MinInt64)
			for _, c := range bs.chunks {
				if c.MinTime <= lastMax {
					stats.droppedChunks++
					continue
				}
				kept = append(kept, c)
				lastMax = c.MaxTime
			}
			bs.chunks = kept
		} else if didMerge {
			bs.chunks = merged
			stats.mergedSeries++
			stats.mergedSamples += n
		}
		out = append(out, bs)
	}
	// Sort series by label set; the index writer requires AddSeries in
	// label-sorted order.
	sort.Slice(out, func(i, j int) bool {
		return labels.Compare(out[i].lset, out[j].lset) < 0
	})
	return out, stats
}

// empty reports whether nothing is buffered.
func (b *windowBuffer) empty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.windows) == 0
}
