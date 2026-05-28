// Package merger implements the gorilla-merger: a Thanos-Receive-style
// component that ingests Gorilla XOR-chunk fragments from edge agents over
// HTTP. The hot path is DECODE-FREE: it durably logs each raw ASAPFRG1 frame to
// a block-level WAL and buffers the raw XOR chunks per window (no sample decode
// / re-encode). On a SMALL window's close it stitches the buffered chunks
// DIRECTLY into a Prometheus TSDB block (low-level chunks/index writers — still
// no decode) under <Dir>/pending/, where it is queryable within ~window+grace
// (recent data is NOT hidden for a full block range). A background compactor
// later merges the small per-window pending blocks (up to a DECOUPLED, larger
// compaction span) and re-chunks them to Prometheus's ~120 samples/chunk target
// for a better compression ratio (the "merger adjusts chunk size for ratio"
// step), writing the result into <Dir>/shipped/. The Thanos shipper watches
// ONLY <Dir>/shipped/, so exactly the compacted, ratio-optimized blocks reach
// object storage. The Thanos StoreAPI (gRPC) serves the UNION of pending +
// shipped so thanos-query can union recent + S3 data with no gap across the
// pending->shipped promotion.
package merger

import (
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/prometheus/prometheus/model/labels"
)

// Storage is the gorilla path's local state: the decode-free ingest Manager
// (window buffer + block-level WAL + directly-built blocks) and the BlockStore
// that serves those blocks to the StoreAPI. It replaces the old embedded
// tsdb.DB (which sample-Appended every fragment); no sample-level WAL or head
// Appender is used on the gorilla path anymore.
type Storage struct {
	Manager        *Manager
	externalLabels labels.Labels
}

// StorageOptions configures the local gorilla storage.
type StorageOptions struct {
	// Dir is the data directory root. The layout under it is:
	//   <Dir>/pending/ — per-window L1 blocks (served, NOT shipped)
	//   <Dir>/shipped/ — compacted + re-chunked L2 blocks (served AND shipped)
	//   <Dir>/wal/     — block-level fragment WAL (unless WALDir overrides)
	Dir string
	// WALDir is where the block-level fragment WAL lives. Defaults to
	// <Dir>/wal when empty.
	WALDir string
	// WindowMs is the buffering/close window size in ms (defaults to 2m).
	WindowMs int64
	// ReorderGraceMs is the post-window grace for late fragments (defaults 60s).
	ReorderGraceMs int64
	// Logger receives log lines.
	Logger *slog.Logger
	// RetentionDuration is retained for flag compatibility; local retention is
	// now governed by the shipper removing uploaded blocks. Unused here.
	RetentionDuration int64
}

// OpenStorage opens the decode-free ingest Manager + BlockStore over Dir,
// replaying the WAL so any accepted-but-unflushed fragments are recovered.
func OpenStorage(opts StorageOptions) (*Storage, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("storage: Dir is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	walDir := opts.WALDir
	if walDir == "" {
		walDir = filepath.Join(opts.Dir, "wal")
	}
	pendingDir := filepath.Join(opts.Dir, "pending")
	shippedDir := filepath.Join(opts.Dir, "shipped")

	mgr, err := NewManager(ManagerOptions{
		PendingDir:     pendingDir,
		ShippedDir:     shippedDir,
		WALDir:         walDir,
		WindowMs:       opts.WindowMs,
		ReorderGraceMs: opts.ReorderGraceMs,
		Logger:         opts.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: open manager at %q: %w", opts.Dir, err)
	}

	return &Storage{Manager: mgr}, nil
}

// BlockStore returns the query-serving block store (the StoreAPI backend).
func (s *Storage) BlockStore() *BlockStore { return s.Manager.Store() }

// ShippedDir returns the dir the shipper must watch (compacted L2 blocks only),
// so the shipper never sees the unshipped pending L1 blocks.
func (s *Storage) ShippedDir() string { return s.Manager.Store().ShippedDir() }

// SetExternalLabels records the merger's external labels. They are applied to
// every uploaded block and advertised by the StoreAPI; they are NOT stamped
// into stored series (the Thanos store appends them at query time).
func (s *Storage) SetExternalLabels(extLset labels.Labels) {
	s.externalLabels = extLset
}

// ExternalLabels returns the configured external labels (sorted).
func (s *Storage) ExternalLabels() labels.Labels {
	return s.externalLabels
}

// Close flushes any buffered windows and closes the Manager (WAL + blocks).
func (s *Storage) Close() error {
	if s.Manager == nil {
		return nil
	}
	return s.Manager.Close()
}
