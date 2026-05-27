package merger

import (
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

// take removes and returns the buffered series for one window, each series'
// chunks sorted by MinTime (the ordering the block writer requires). Returns
// nil if the window is empty/absent.
func (b *windowBuffer) take(wStart int64) []*bufferedSeries {
	b.mu.Lock()
	w, ok := b.windows[wStart]
	if ok {
		delete(b.windows, wStart)
	}
	b.mu.Unlock()
	if !ok || len(w) == 0 {
		return nil
	}

	out := make([]*bufferedSeries, 0, len(w))
	for _, bs := range w {
		sort.Slice(bs.chunks, func(i, j int) bool {
			if bs.chunks[i].MinTime != bs.chunks[j].MinTime {
				return bs.chunks[i].MinTime < bs.chunks[j].MinTime
			}
			return bs.chunks[i].MaxTime < bs.chunks[j].MaxTime
		})
		out = append(out, bs)
	}
	// Sort series by label set; the index writer requires AddSeries in
	// label-sorted order.
	sort.Slice(out, func(i, j int) bool {
		return labels.Compare(out[i].lset, out[j].lset) < 0
	})
	return out
}

// empty reports whether nothing is buffered.
func (b *windowBuffer) empty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.windows) == 0
}
