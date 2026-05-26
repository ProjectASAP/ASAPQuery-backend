package merger

import (
	"context"
	"fmt"

	"github.com/ProjectASAP/asap-gorilla-go/coldpart"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/ProjectASAP/asapquery-backend/gorilla-merger/internal/coldchunk"
)

// ColdChunk is one in-window XOR chunk for a cold series, with its own time
// bounds (the min/max sample timestamp it actually carries, in absolute ms).
type ColdChunk struct {
	Chunk   chunkenc.Chunk
	MinTime int64
	MaxTime int64
}

// ColdSeries is one matched cold series: its label set plus the XOR chunk(s)
// re-encoded from the in-window decoded samples. It is the decode-on-read
// analogue of a tsdb series the customStore streams; the customStore appends
// its external labels and converts the chunks to storepb.AggrChunk via the SAME
// safe label-copy path it uses for tsdb series.
type ColdSeries struct {
	Labels labels.Labels
	Chunks []ColdChunk
}

// ColdQuerier answers the decode-on-read cold query: it finds manifest parts
// overlapping the window, OpenParts each, calls coldpart.Part.Series (which
// applies matchers + part-level time overlap and decodes ONLY matched series),
// clips the decoded samples to [mintMs,maxtMs], and re-encodes them as standard
// Prometheus XOR chunks. It is the read mirror of ColdPartStore.Put.
type ColdQuerier struct {
	store *ColdPartStore
}

// NewColdQuerier wraps a part store with the decode-on-read query path.
func NewColdQuerier(store *ColdPartStore) *ColdQuerier {
	return &ColdQuerier{store: store}
}

// MinBlockStart returns the smallest block_start_ms across the cold store's
// tracked parts and true, or (0,false) when there is no cold data (or no
// store). The customStore folds this into its advertised StoreAPI MinTime so
// thanos-query does not prune the merger from a query whose window only covers
// old cold data.
func (q *ColdQuerier) MinBlockStart() (int64, bool) {
	if q == nil || q.store == nil {
		return 0, false
	}
	return q.store.MinBlockStart()
}

// Series returns every cold series matching all matchers with at least one
// sample in the inclusive window [mintMs,maxtMs], each as XOR chunk(s) over the
// CLIPPED in-window samples (coldpart.Part.Series returns the WHOLE matched
// series, so clipping is applied here). A series whose samples all fall outside
// the window after clipping is omitted. Timestamps are absolute ms throughout.
//
// When a label set is covered by multiple overlapping parts (e.g. the same
// series spread across adjacent blocks), each part contributes its own
// ColdSeries; the caller (StoreAPI/PromQL) handles the union/merge across them,
// exactly as it does for store-gateway + open-window overlaps today.
func (q *ColdQuerier) Series(ctx context.Context, matchers []*labels.Matcher, mintMs, maxtMs int64) ([]ColdSeries, error) {
	if q == nil || q.store == nil {
		return nil, nil
	}
	entries := q.store.PartsOverlapping(mintMs, maxtMs, matchers)
	var out []ColdSeries
	for _, e := range entries {
		b, err := q.store.fetch(ctx, e.Key)
		if err != nil {
			return nil, fmt.Errorf("coldquery: fetch part %q: %w", e.Key, err)
		}
		part, err := coldpart.OpenPart(b)
		if err != nil {
			return nil, fmt.Errorf("coldquery: open part %q: %w", e.Key, err)
		}
		matched, err := part.Series(matchers, mintMs, maxtMs)
		if err != nil {
			return nil, fmt.Errorf("coldquery: decode part %q: %w", e.Key, err)
		}
		for _, sd := range matched {
			cs, err := seriesToCold(sd, mintMs, maxtMs)
			if err != nil {
				return nil, fmt.Errorf("coldquery: encode series %s: %w", sd.Labels.String(), err)
			}
			if cs != nil {
				out = append(out, *cs)
			}
		}
	}
	return out, nil
}

// seriesToCold clips a decoded series' samples to the inclusive window
// [mintMs,maxtMs] and re-encodes them as a single XOR chunk. It returns nil
// (no error) when no sample falls in the window, so the caller can skip it.
func seriesToCold(sd coldpart.SeriesData, mintMs, maxtMs int64) (*ColdSeries, error) {
	clipped := clipSamples(sd.Samples, mintMs, maxtMs)
	if len(clipped) == 0 {
		return nil, nil
	}
	xc, err := coldchunk.SamplesToXORChunk(clipped)
	if err != nil {
		return nil, err
	}
	return &ColdSeries{
		Labels: sd.Labels,
		Chunks: []ColdChunk{{
			Chunk:   xc,
			MinTime: clipped[0].T,
			MaxTime: clipped[len(clipped)-1].T,
		}},
	}, nil
}

// clipSamples returns the slice of samples whose timestamp lies within the
// inclusive window [mintMs,maxtMs]. samples are assumed time-ordered ascending
// (coldpart guarantees this), so the result is a contiguous sub-slice.
func clipSamples(samples []coldpart.Sample, mintMs, maxtMs int64) []coldpart.Sample {
	lo := 0
	for lo < len(samples) && samples[lo].T < mintMs {
		lo++
	}
	hi := len(samples)
	for hi > lo && samples[hi-1].T > maxtMs {
		hi--
	}
	if lo >= hi {
		return nil
	}
	return samples[lo:hi]
}
