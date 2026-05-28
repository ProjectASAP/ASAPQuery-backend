package merger

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
)

// Compactor merges the many small per-window blocks the decode-free ingest path
// produces into larger blocks AND re-chunks them to Prometheus's ~120
// samples/chunk target, for a better compression ratio.
//
// This is the offline/amortized "merger adjusts chunk size for ratio" step. The
// resource-limited agents cannot afford to emit large chunks, so the merger
// does the re-chunking here, OFF the ingest hot path: tsdb.LeveledCompactor
// decodes + re-encodes + re-chunks (storage's compacting merger cuts a new
// chunk every 120 samples — see storage.seriesToChunkEncoderSplit), which is
// acceptable precisely because it runs in the background, not per-fragment.
//
// After a successful compaction the source per-window block dirs (in the
// pending dir) are removed and the BlockStore is reloaded so the new (larger,
// re-chunked) block in the shipped dir becomes the served + shipped artifact.
// This pending->shipped split is what makes the shipper upload ONLY the
// compacted, ratio-optimized blocks (it watches the shipped dir only).
type Compactor struct {
	store     *BlockStore
	compactor *tsdb.LeveledCompactor
	interval  time.Duration
	// minBlocks is the smallest number of source blocks worth a compaction pass
	// (1 would re-chunk a single block; default 2 to actually merge).
	minBlocks int
	// maxSpanMs bounds how wide a compacted block may be (so we don't fuse far-
	// apart windows into one giant block). Defaults to the 2h Prometheus block
	// range, aligning with the shipper/store-gateway expectations.
	maxSpanMs int64
	logger    *slog.Logger
}

// CompactorOptions configures the background compactor.
type CompactorOptions struct {
	Store      *BlockStore
	Interval   time.Duration
	MinBlocks  int
	MaxSpanMs  int64
	Registerer prometheus.Registerer
	Logger     *slog.Logger
}

// NewCompactor builds a Compactor over the store's blocks directory.
func NewCompactor(opts CompactorOptions) (*Compactor, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("compactor: Store is required")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	minBlocks := opts.MinBlocks
	if minBlocks <= 0 {
		minBlocks = 2
	}
	maxSpan := opts.MaxSpanMs
	if maxSpan <= 0 {
		maxSpan = tsdb.DefaultBlockDuration // 2h
	}

	// Ranges drive how the leveled compactor groups; we drive grouping ourselves
	// (we hand it exact dir sets), so a single range covering the max span is
	// enough. The chunk pool + default merge func give the standard re-chunking.
	lc, err := tsdb.NewLeveledCompactor(
		context.Background(),
		opts.Registerer,
		logger,
		[]int64{maxSpan},
		nil, // default chunk pool
		nil, // default merge func (compacting, re-chunks at 120 samples)
	)
	if err != nil {
		return nil, fmt.Errorf("compactor: new leveled compactor: %w", err)
	}

	return &Compactor{
		store:     opts.Store,
		compactor: lc,
		interval:  interval,
		minBlocks: minBlocks,
		maxSpanMs: maxSpan,
		logger:    logger,
	}, nil
}

// Run drives CompactOnce on a ticker until ctx is cancelled.
func (c *Compactor) Run(ctx context.Context) error {
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := c.CompactOnce(ctx); err != nil {
				c.logger.Warn("compaction pass failed", "err", err)
			}
		}
	}
}

// CompactOnce groups the current PENDING blocks into runs that each span at
// most maxSpanMs and compacts every run that has >= minBlocks members. It
// returns the number of compacted runs. Compaction merges + re-chunks the
// pending sources into the shipped dir; the pending sources are then removed and
// the store reloaded so the promoted (re-chunked, shippable) block is served.
// With minBlocks=1 even a lone pending block is promoted to shipped so it ships.
func (c *Compactor) CompactOnce(ctx context.Context) (err error) {
	dirs := c.store.pendingBlockDirs()
	if len(dirs) < c.minBlocks {
		return nil
	}

	groups := c.groupBySpan(dirs)
	compacted := 0
	for _, g := range groups {
		if len(g) < c.minBlocks {
			continue
		}
		if cerr := c.compactGroup(ctx, g); cerr != nil {
			return cerr
		}
		compacted++
	}
	if compacted > 0 {
		c.logger.Info("compacted block runs", "runs", compacted)
	}
	return nil
}

// groupBySpan partitions the (MinTime-sorted) block dirs into runs whose total
// time span stays within maxSpanMs, so a compacted block never exceeds the 2h
// base range.
func (c *Compactor) groupBySpan(dirs []string) [][]string {
	var groups [][]string
	var cur []string
	var groupStart int64
	for _, d := range dirs {
		meta, _, merr := readDirMeta(d)
		if merr != nil {
			c.logger.Warn("compactor: read meta (skipping)", "dir", d, "err", merr)
			continue
		}
		if len(cur) == 0 {
			cur = []string{d}
			groupStart = alignDown(meta.MinTime, c.maxSpanMs)
			continue
		}
		// If this block's maxt would push the run past the aligned span, start a
		// new run.
		if meta.MaxTime-groupStart > c.maxSpanMs {
			groups = append(groups, cur)
			cur = []string{d}
			groupStart = alignDown(meta.MinTime, c.maxSpanMs)
			continue
		}
		cur = append(cur, d)
	}
	if len(cur) > 0 {
		groups = append(groups, cur)
	}
	return groups
}

func alignDown(t, span int64) int64 {
	w := t / span
	if t < 0 && t%span != 0 {
		w--
	}
	return w * span
}

// compactGroup compacts one run of PENDING block dirs into a single re-chunked
// block in the SHIPPED dir, removes the pending sources, and reloads the store.
// Ordering is write-dest -> Reload -> remove-sources -> Reload: during the brief
// overlap the chained merge querier dedups the duplicated samples, so there is
// no query gap and no double-count across the pending->shipped promotion.
func (c *Compactor) compactGroup(ctx context.Context, group []string) error {
	dest := c.store.ShippedDir()
	// Use the re-chunking populator so the merged block's chunks are re-encoded
	// to ~120 samples/chunk (the ratio step), not just concatenated.
	uids, err := c.compactor.CompactWithBlockPopulator(dest, group, nil, rechunkPopulator{})
	if err != nil {
		return fmt.Errorf("compactor: compact %v: %w", group, err)
	}
	// Remove the source dirs now that the merged block is written. (A compaction
	// that produced an empty block returns no uids; nothing to clean then.)
	if len(uids) == 0 {
		return nil
	}
	// Make the new block visible and drop the open handles on the sources first,
	// so removing their dirs does not race an open reader.
	if rerr := c.store.Reload(); rerr != nil {
		return fmt.Errorf("compactor: reload after compact: %w", rerr)
	}
	for _, d := range group {
		// The compacted block is a fresh ULID dir; never remove it.
		if isAnyOf(filepath.Base(d), uids) {
			continue
		}
		if rmErr := os.RemoveAll(d); rmErr != nil {
			c.logger.Warn("compactor: remove source block", "dir", d, "err", rmErr)
		}
	}
	// Reload again to forget the now-removed source blocks.
	if rerr := c.store.Reload(); rerr != nil {
		return fmt.Errorf("compactor: reload after source removal: %w", rerr)
	}
	return nil
}

func isAnyOf(name string, uids []ulid.ULID) bool {
	for _, u := range uids {
		if u.String() == name {
			return true
		}
	}
	return false
}

// readDirMeta reads a block dir's meta.json via the public OpenBlock-less path:
// we marshal/unmarshal the same meta.json tsdb writes. Reusing json keeps us off
// tsdb's unexported readMetaFile.
func readDirMeta(dir string) (*tsdb.BlockMeta, int64, error) {
	b, err := os.ReadFile(filepath.Join(dir, metaFilenameConst))
	if err != nil {
		return nil, 0, err
	}
	var m tsdb.BlockMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, 0, err
	}
	return &m, int64(len(b)), nil
}
