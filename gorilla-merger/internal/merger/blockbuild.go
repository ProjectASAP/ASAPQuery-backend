package merger

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

// buildBlock stitches the buffered raw XOR chunks for a single window directly
// into a Prometheus TSDB block on disk, WITHOUT decoding any samples. It writes
// the chunks (chunks/000001..), the index (symbols + series + postings), an
// empty tombstones file, and a meta.json, then atomically renames the staging
// dir into destDir/<ULID>. Returns the created block's directory path.
//
// This is the hot-path-adjacent "ordered block build on window close" step of
// the decode-free design: the raw chunk bytes that the agent produced land
// verbatim in the block (verified by TestBlockBuildPreservesChunkBytes), so
// there is NO second Gorilla-XOR encode and NO per-sample materialization.
//
// series MUST be sorted by label set ascending and each series' chunks sorted
// by MinTime ascending (windowBuffer.take guarantees both).
func buildBlock(destDir string, series []*bufferedSeries) (string, error) {
	if len(series) == 0 {
		return "", fmt.Errorf("buildBlock: no series to write")
	}
	if err := os.MkdirAll(destDir, 0o777); err != nil {
		return "", fmt.Errorf("buildBlock: mkdir dest %q: %w", destDir, err)
	}

	uid := ulid.MustNew(ulid.Now(), rand.Reader)
	tmp := filepath.Join(destDir, uid.String()+".tmp")
	if err := os.MkdirAll(chunkDirOf(tmp), 0o777); err != nil {
		return "", fmt.Errorf("buildBlock: mkdir tmp chunk dir: %w", err)
	}
	// Best-effort cleanup of the staging dir on any error path.
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(tmp)
		}
	}()

	// 1. Write the chunks. WriteChunks assigns each Meta.Ref in place. We write
	//    one series' chunks at a time, in series-sorted order, so the assigned
	//    refs are monotonically non-decreasing across the whole series list —
	//    the ordering index.AddSeries requires.
	chunkw, err := chunks.NewWriter(chunkDirOf(tmp))
	if err != nil {
		return "", fmt.Errorf("buildBlock: chunk writer: %w", err)
	}

	type seriesChunks struct {
		lset  labels.Labels
		metas []chunks.Meta
	}
	prepared := make([]seriesChunks, 0, len(series))

	var (
		minTime    = int64(1<<63 - 1)
		maxTime    = int64(-1 << 63)
		numSamples uint64
		numChunks  uint64
	)

	for _, bs := range series {
		if len(bs.chunks) == 0 {
			continue
		}
		metas := make([]chunks.Meta, 0, len(bs.chunks))
		for _, c := range bs.chunks {
			// Wrap the raw bytes as a chunk WITHOUT iterating it: FromData just
			// reinterprets the byte slice; .Bytes() returns it unchanged.
			chk, cerr := chunkenc.FromData(chunkenc.EncXOR, c.Data)
			if cerr != nil {
				_ = chunkw.Close()
				return "", fmt.Errorf("buildBlock: wrap xor chunk: %w", cerr)
			}
			metas = append(metas, chunks.Meta{
				Chunk:   chk,
				MinTime: c.MinTime,
				MaxTime: c.MaxTime,
			})
			if c.MinTime < minTime {
				minTime = c.MinTime
			}
			if c.MaxTime > maxTime {
				maxTime = c.MaxTime
			}
			numSamples += uint64(c.NumSamples)
			numChunks++
		}
		if werr := chunkw.WriteChunks(metas...); werr != nil {
			_ = chunkw.Close()
			return "", fmt.Errorf("buildBlock: write chunks for %s: %w", bs.lset.String(), werr)
		}
		prepared = append(prepared, seriesChunks{lset: bs.lset, metas: metas})
	}
	if err := chunkw.Close(); err != nil {
		return "", fmt.Errorf("buildBlock: close chunk writer: %w", err)
	}
	if len(prepared) == 0 {
		return "", fmt.Errorf("buildBlock: no non-empty series to write")
	}

	// 2. Write the index: symbols (sorted, deduped) then series (label-sorted,
	//    increasing refs) then postings (index.Writer derives them from the
	//    AddSeries calls and writes them on Close).
	indexw, err := index.NewWriter(context.Background(), filepath.Join(tmp, indexFilename))
	if err != nil {
		return "", fmt.Errorf("buildBlock: index writer: %w", err)
	}

	// Collect and sort all symbols (label names + values).
	symSet := map[string]struct{}{}
	for _, p := range prepared {
		p.lset.Range(func(l labels.Label) {
			symSet[l.Name] = struct{}{}
			symSet[l.Value] = struct{}{}
		})
	}
	syms := make([]string, 0, len(symSet))
	for s := range symSet {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	for _, s := range syms {
		if aerr := indexw.AddSymbol(s); aerr != nil {
			_ = indexw.Close()
			return "", fmt.Errorf("buildBlock: add symbol %q: %w", s, aerr)
		}
	}

	// Series refs must be increasing; use the 1-based ordinal. The chunk refs
	// inside each series' metas were set by WriteChunks and are already
	// monotonic in series order.
	for i, p := range prepared {
		ref := storage.SeriesRef(i + 1)
		if aerr := indexw.AddSeries(ref, p.lset, p.metas...); aerr != nil {
			_ = indexw.Close()
			return "", fmt.Errorf("buildBlock: add series %s: %w", p.lset.String(), aerr)
		}
	}
	if err := indexw.Close(); err != nil {
		return "", fmt.Errorf("buildBlock: close index writer: %w", err)
	}

	// 3. Empty tombstones file (OpenBlock requires it to exist).
	if _, err := tombstones.WriteFile(nil, tmp, tombstones.NewMemTombstones()); err != nil {
		return "", fmt.Errorf("buildBlock: write tombstones: %w", err)
	}

	// 4. meta.json. Block intervals are half-open [MinTime, MaxTime); the
	//    convention is MaxTime = maxSampleTime + 1.
	meta := tsdb.BlockMeta{
		ULID:    uid,
		MinTime: minTime,
		MaxTime: maxTime + 1,
		Version: 1,
		Stats: tsdb.BlockStats{
			NumSamples: numSamples,
			NumSeries:  uint64(len(prepared)),
			NumChunks:  numChunks,
		},
		Compaction: tsdb.BlockMetaCompaction{
			Level:   1,
			Sources: []ulid.ULID{uid},
		},
	}
	if err := writeBlockMeta(tmp, &meta); err != nil {
		return "", fmt.Errorf("buildBlock: write meta: %w", err)
	}

	// 5. Atomic publish: rename staging dir to its final ULID dir.
	final := filepath.Join(destDir, uid.String())
	if err := os.Rename(tmp, final); err != nil {
		return "", fmt.Errorf("buildBlock: publish block dir: %w", err)
	}
	committed = true
	return final, nil
}

// writeBlockMeta marshals a tsdb.BlockMeta to <dir>/meta.json. We marshal
// ourselves because tsdb.writeMetaFile is unexported; the JSON shape is the
// stable, public meta.json format (tsdb.BlockMeta json tags), and tsdb.OpenBlock
// reads it back via readMetaFile.
func writeBlockMeta(dir string, meta *tsdb.BlockMeta) error {
	b, err := json.MarshalIndent(meta, "", "\t")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, metaFilenameConst)
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// chunkDirOf mirrors tsdb.chunkDir (unexported): the chunks subdir of a block.
func chunkDirOf(blockDir string) string { return filepath.Join(blockDir, "chunks") }

const (
	// indexFilename and metaFilenameConst mirror tsdb's unexported constants.
	indexFilename     = "index"
	metaFilenameConst = "meta.json"
)
