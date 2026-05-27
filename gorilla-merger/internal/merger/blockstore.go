package merger

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

// BlockStore manages the directly-built (and compacted) Prometheus blocks that
// live under a data directory, and serves queries over them. It is the
// query-serving engine for the decode-free gorilla path: it implements the
// chunkQueryable interface that customStore uses (ChunkQuerier / Querier /
// StartTime), fanning each query out across all currently-open blocks and
// merging the per-block series streams.
//
// WHY a BlockStore rather than feeding the blocks back into the embedded
// tsdb.DB: tsdb.DB.reloadBlocks (the method that picks up externally-placed
// block dirs) is UNEXPORTED, and db.Compact only reloads when its planner
// returns a non-empty plan, so there is no public, reliable way to make a
// running tsdb.DB serve a freshly-dropped block. tsdb.OpenBlock IS exported and
// yields a BlockReader; tsdb.NewBlockChunkQuerier / NewBlockQuerier +
// storage.NewMergeChunkQuerier / NewMergeQuerier are exported too, so a small
// fan-out store reuses all the Prometheus block-read + merge machinery while
// keeping full control over which blocks are visible.
type BlockStore struct {
	dir    string
	logger *slog.Logger

	mu     sync.RWMutex
	blocks map[ulid.ULID]*tsdb.Block
}

// NewBlockStore opens (or creates) the blocks directory and loads any blocks
// already present (e.g. from a previous run, or compacted blocks not yet
// shipped). dir is the SAME directory the shipper watches and the compactor
// writes into, so blocks built here are shipped + compacted without copying.
func NewBlockStore(dir string, logger *slog.Logger) (*BlockStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("blockstore: dir is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return nil, fmt.Errorf("blockstore: mkdir %q: %w", dir, err)
	}
	bs := &BlockStore{
		dir:    dir,
		logger: logger,
		blocks: make(map[ulid.ULID]*tsdb.Block),
	}
	if err := bs.Reload(); err != nil {
		return nil, err
	}
	return bs, nil
}

// Dir returns the blocks directory (used by the shipper and compactor).
func (bs *BlockStore) Dir() string { return bs.dir }

// Reload scans the directory and reconciles the open-block set with what is on
// disk: it opens any block dir not yet open and closes/forgets any open block
// whose dir disappeared (e.g. removed by the shipper after upload, or replaced
// by compaction). It is safe to call concurrently with queries.
func (bs *BlockStore) Reload() error {
	entries, err := os.ReadDir(bs.dir)
	if err != nil {
		return fmt.Errorf("blockstore: read dir %q: %w", bs.dir, err)
	}

	onDisk := make(map[ulid.ULID]struct{})
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		id, perr := ulid.Parse(e.Name())
		if perr != nil {
			// Not a block dir (e.g. a ".tmp" staging dir or wal dir); skip.
			continue
		}
		// A block dir without a meta.json is incomplete (mid-build); skip it.
		if _, serr := os.Stat(filepath.Join(bs.dir, e.Name(), metaFilenameConst)); serr != nil {
			continue
		}
		onDisk[id] = struct{}{}
	}

	bs.mu.Lock()
	defer bs.mu.Unlock()

	// Open newly-appeared blocks.
	for id := range onDisk {
		if _, ok := bs.blocks[id]; ok {
			continue
		}
		b, oerr := tsdb.OpenBlock(bs.logger, filepath.Join(bs.dir, id.String()), nil, nil)
		if oerr != nil {
			bs.logger.Warn("blockstore: open block failed (skipping)", "block", id.String(), "err", oerr)
			continue
		}
		bs.blocks[id] = b
	}

	// Close + forget blocks whose dir is gone.
	for id, b := range bs.blocks {
		if _, ok := onDisk[id]; ok {
			continue
		}
		if cerr := b.Close(); cerr != nil {
			bs.logger.Warn("blockstore: close retired block", "block", id.String(), "err", cerr)
		}
		delete(bs.blocks, id)
	}
	return nil
}

// snapshot returns the currently-open blocks under a read lock.
func (bs *BlockStore) snapshot() []*tsdb.Block {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	out := make([]*tsdb.Block, 0, len(bs.blocks))
	for _, b := range bs.blocks {
		out = append(out, b)
	}
	return out
}

// overlaps reports whether block b's [MinTime,MaxTime) intersects [mint,maxt].
func overlaps(b *tsdb.Block, mint, maxt int64) bool {
	m := b.Meta()
	return m.MinTime <= maxt && mint < m.MaxTime
}

// ChunkQuerier returns a storage.ChunkQuerier that fans out over every open
// block overlapping [mint,maxt] and merges them with the compacting chunk-series
// merger (so overlapping series across blocks are chained, matching what the
// store-gateway/PromQL would do). Implements the chunkQueryable interface.
func (bs *BlockStore) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	blocks := bs.snapshot()
	var qs []storage.ChunkQuerier
	for _, b := range blocks {
		if !overlaps(b, mint, maxt) {
			continue
		}
		q, err := tsdb.NewBlockChunkQuerier(b, mint, maxt)
		if err != nil {
			for _, q := range qs {
				_ = q.Close()
			}
			return nil, err
		}
		qs = append(qs, q)
	}
	if len(qs) == 0 {
		return storage.NoopChunkedQuerier(), nil
	}
	merger := storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)
	return storage.NewMergeChunkQuerier(qs, nil, merger), nil
}

// Querier returns a storage.Querier merging every overlapping open block (used
// by the LabelNames/LabelValues StoreAPI paths). Implements chunkQueryable.
func (bs *BlockStore) Querier(mint, maxt int64) (storage.Querier, error) {
	blocks := bs.snapshot()
	var qs []storage.Querier
	for _, b := range blocks {
		if !overlaps(b, mint, maxt) {
			continue
		}
		q, err := tsdb.NewBlockQuerier(b, mint, maxt)
		if err != nil {
			for _, q := range qs {
				_ = q.Close()
			}
			return nil, err
		}
		qs = append(qs, q)
	}
	if len(qs) == 0 {
		return storage.NoopQuerier(), nil
	}
	return storage.NewMergeQuerier(qs, nil, storage.ChainedSeriesMerge), nil
}

// StartTime returns the minimum MinTime across all open blocks, or
// math.MaxInt64 when empty (matching tsdb.DB.StartTime's empty-head sentinel,
// which customStore.timeRange already handles). Implements chunkQueryable.
func (bs *BlockStore) StartTime() (int64, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	min := int64(math.MaxInt64)
	for _, b := range bs.blocks {
		if m := b.Meta().MinTime; m < min {
			min = m
		}
	}
	return min, nil
}

// blockDirs returns the directories of all open blocks, sorted by MinTime. Used
// by the compactor to know which blocks to merge.
func (bs *BlockStore) blockDirs() []string {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	type bd struct {
		dir  string
		mint int64
	}
	bds := make([]bd, 0, len(bs.blocks))
	for id, b := range bs.blocks {
		bds = append(bds, bd{dir: filepath.Join(bs.dir, id.String()), mint: b.Meta().MinTime})
	}
	sort.Slice(bds, func(i, j int) bool { return bds[i].mint < bds[j].mint })
	out := make([]string, len(bds))
	for i := range bds {
		out[i] = bds[i].dir
	}
	return out
}

// CoversTime reports whether any currently-open block's half-open time range
// [MinTime,MaxTime) contains t. Used by WAL replay to skip a fragment whose
// window was already persisted as a block before a crash (idempotent replay).
func (bs *BlockStore) CoversTime(t int64) bool {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	for _, b := range bs.blocks {
		m := b.Meta()
		if t >= m.MinTime && t < m.MaxTime {
			return true
		}
	}
	return false
}

// Close closes every open block. The caller must ensure no queries are in
// flight; Block.Close blocks until pending readers drain.
func (bs *BlockStore) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	var firstErr error
	for id, b := range bs.blocks {
		if err := b.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(bs.blocks, id)
	}
	return firstErr
}
