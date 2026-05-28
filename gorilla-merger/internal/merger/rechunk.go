package merger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
)

// rechunkPopulator is a tsdb.BlockPopulator that mirrors tsdb's
// DefaultBlockPopulator but RE-CHUNKS every output series to Prometheus's ~120
// samples/chunk target via storage.NewSeriesToChunkEncoder.
//
// WHY a custom populator: the stock LeveledCompactor only re-encodes chunks when
// series OVERLAP in time (the compacting merger chains non-overlapping chunks
// through verbatim — see storage.compactChunkIterator). The per-window blocks
// this merger builds hold many small, NON-overlapping chunks (the agent emits
// tiny chunks because it is resource-limited), so a plain Compact would fuse the
// blocks but leave the tiny chunks intact — no ratio win. Wrapping each merged
// ChunkSeries in NewSeriesToChunkEncoder forces a decode + re-encode that cuts a
// fresh chunk every 120 samples (storage.seriesToChunkEncoderSplit), which is
// exactly the offline/amortized "merger adjusts chunk size for ratio" step. This
// decode happens HERE, in the background compactor, never on the ingest path.
type rechunkPopulator struct{}

// PopulateBlock replicates DefaultBlockPopulator.PopulateBlock but re-chunks the
// emitted series. It uses only exported tsdb/storage APIs.
func (rechunkPopulator) PopulateBlock(
	ctx context.Context,
	metrics *tsdb.CompactorMetrics,
	logger *slog.Logger,
	chunkPool chunkenc.Pool,
	mergeFunc storage.VerticalChunkSeriesMergeFunc,
	blocks []tsdb.BlockReader,
	meta *tsdb.BlockMeta,
	indexw tsdb.IndexWriter,
	chunkw tsdb.ChunkWriter,
	postingsFunc tsdb.IndexReaderPostingsFunc,
) (err error) {
	if len(blocks) == 0 {
		return errors.New("rechunk: cannot populate block from no readers")
	}

	var (
		sets    []storage.ChunkSeriesSet
		symbols index.StringIter
		closers []io.Closer
	)
	defer func() {
		errs := tsdb_errors.NewMulti(err)
		if cerr := tsdb_errors.CloseAll(closers); cerr != nil {
			errs.Add(fmt.Errorf("close: %w", cerr))
		}
		err = errs.Err()
		if metrics != nil && metrics.PopulatingBlocks != nil {
			metrics.PopulatingBlocks.Set(0)
		}
	}()
	if metrics != nil && metrics.PopulatingBlocks != nil {
		metrics.PopulatingBlocks.Set(1)
	}

	for i, b := range blocks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		indexr, ierr := b.Index()
		if ierr != nil {
			return fmt.Errorf("rechunk: open index reader for %+v: %w", b.Meta(), ierr)
		}
		closers = append(closers, indexr)

		chunkr, cerr := b.Chunks()
		if cerr != nil {
			return fmt.Errorf("rechunk: open chunk reader for %+v: %w", b.Meta(), cerr)
		}
		closers = append(closers, chunkr)

		tombsr, terr := b.Tombstones()
		if terr != nil {
			return fmt.Errorf("rechunk: open tombstone reader for %+v: %w", b.Meta(), terr)
		}
		closers = append(closers, tombsr)

		postings := postingsFunc(ctx, indexr)
		// Block meta is half-open [min,max); subtract 1 from maxt like tsdb does.
		sets = append(sets, tsdb.NewBlockChunkSeriesSet(b.Meta().ULID, indexr, chunkr, tombsr, postings, meta.MinTime, meta.MaxTime-1, false))
		syms := indexr.Symbols()
		if i == 0 {
			symbols = syms
			continue
		}
		symbols = tsdb.NewMergedStringIter(symbols, syms)
	}

	for symbols.Next() {
		if serr := indexw.AddSymbol(symbols.At()); serr != nil {
			return fmt.Errorf("rechunk: add symbol: %w", serr)
		}
	}
	if serr := symbols.Err(); serr != nil {
		return fmt.Errorf("rechunk: next symbol: %w", serr)
	}

	var (
		ref      = storage.SeriesRef(0)
		chks     []chunks.Meta
		chksIter chunks.Iterator
	)

	set := sets[0]
	if len(sets) > 1 {
		set = storage.NewMergeChunkSeriesSet(sets, 0, mergeFunc)
	}

	for set.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		s := set.At()
		// THE re-chunk: decode the (possibly many tiny) chunks of this series and
		// re-encode them into ~120-sample chunks.
		s = storage.NewSeriesToChunkEncoder(&chunkSeriesAsSeries{s})

		chksIter = s.Iterator(chksIter)
		chks = chks[:0]
		for chksIter.Next() {
			chks = append(chks, chksIter.At())
		}
		if cerr := chksIter.Err(); cerr != nil {
			return fmt.Errorf("rechunk: chunk iter: %w", cerr)
		}
		if len(chks) == 0 {
			continue
		}

		if werr := chunkw.WriteChunks(chks...); werr != nil {
			return fmt.Errorf("rechunk: write chunks: %w", werr)
		}
		if aerr := indexw.AddSeries(ref, s.Labels(), chks...); aerr != nil {
			return fmt.Errorf("rechunk: add series: %w", aerr)
		}

		meta.Stats.NumChunks += uint64(len(chks))
		meta.Stats.NumSeries++
		for _, chk := range chks {
			samples := uint64(chk.Chunk.NumSamples())
			meta.Stats.NumSamples += samples
			switch chk.Chunk.Encoding() {
			case chunkenc.EncHistogram, chunkenc.EncFloatHistogram:
				meta.Stats.NumHistogramSamples += samples
			case chunkenc.EncXOR:
				meta.Stats.NumFloatSamples += samples
			}
		}
		ref++
	}
	if serr := set.Err(); serr != nil {
		return fmt.Errorf("rechunk: iterate compaction set: %w", serr)
	}
	return nil
}

// chunkSeriesAsSeries adapts a storage.ChunkSeries to a storage.Series so it can
// be fed to NewSeriesToChunkEncoder (which re-chunks a sample stream). It flattens
// the series' chunk stream into a single sample iterator. This decode is the
// deliberate, offline re-encode cost (it runs only in the background compactor).
type chunkSeriesAsSeries struct {
	cs storage.ChunkSeries
}

func (a *chunkSeriesAsSeries) Labels() labels.Labels { return a.cs.Labels() }

func (a *chunkSeriesAsSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	return &flattenChunkIterator{chkIt: a.cs.Iterator(nil)}
}

// flattenChunkIterator turns a chunks.Iterator (a stream of chunk metas) into a
// chunkenc.Iterator over all the samples in those chunks, in order.
//
// NOTE: `go vet`'s stdmethods check flags Seek(int64) ValueType because it
// collides with io.Seeker's name, but this is the REQUIRED signature of
// chunkenc.Iterator (Prometheus's own iterators trigger the same false
// positive). The compile-time assertion below is the real contract.
type flattenChunkIterator struct {
	chkIt   chunks.Iterator
	sampIt  chunkenc.Iterator
	started bool
}

var _ chunkenc.Iterator = (*flattenChunkIterator)(nil)

func (f *flattenChunkIterator) advanceChunk() bool {
	for f.chkIt.Next() {
		meta := f.chkIt.At()
		if meta.Chunk == nil {
			continue
		}
		f.sampIt = meta.Chunk.Iterator(nil)
		return true
	}
	return false
}

func (f *flattenChunkIterator) Next() chunkenc.ValueType {
	if !f.started {
		f.started = true
		if !f.advanceChunk() {
			return chunkenc.ValNone
		}
	}
	for {
		if f.sampIt != nil {
			if vt := f.sampIt.Next(); vt != chunkenc.ValNone {
				return vt
			}
		}
		if !f.advanceChunk() {
			return chunkenc.ValNone
		}
	}
}

func (f *flattenChunkIterator) Seek(t int64) chunkenc.ValueType {
	// The re-chunk encoder only calls Next(); a linear Seek is sufficient.
	for {
		vt := f.Next()
		if vt == chunkenc.ValNone {
			return chunkenc.ValNone
		}
		if f.AtT() >= t {
			return vt
		}
	}
}

func (f *flattenChunkIterator) At() (int64, float64) { return f.sampIt.At() }

func (f *flattenChunkIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	return f.sampIt.AtHistogram(h)
}

func (f *flattenChunkIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return f.sampIt.AtFloatHistogram(fh)
}

func (f *flattenChunkIterator) AtT() int64 {
	if f.sampIt == nil {
		return 0
	}
	return f.sampIt.AtT()
}

func (f *flattenChunkIterator) Err() error {
	if f.chkIt != nil {
		if err := f.chkIt.Err(); err != nil {
			return err
		}
	}
	if f.sampIt != nil {
		return f.sampIt.Err()
	}
	return nil
}
