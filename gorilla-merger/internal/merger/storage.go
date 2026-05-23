// Package merger implements the gorilla-merger: a Thanos-Receive-style
// component that ingests Gorilla XOR-chunk fragments from edge agents over
// HTTP, appends them into an embedded Prometheus tsdb.DB (2h block range),
// ships completed 2h blocks to object storage via the Thanos shipper (one PUT
// per block), and exposes a Thanos StoreAPI over the open (pending, <2h)
// window so thanos-query can union recent + S3 data.
package merger

import (
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
)

// Storage wraps an embedded Prometheus tsdb.DB configured for a 2h block
// range (the Prometheus/Thanos default). The head holds the pending window;
// the WAL provides durability. The head auto-compacts at the 2h boundary,
// producing on-disk blocks that the shipper then uploads.
type Storage struct {
	DB             *tsdb.DB
	externalLabels labels.Labels
}

// StorageOptions configures the embedded tsdb.DB.
type StorageOptions struct {
	// Dir is the tsdb data directory (WAL + blocks live here).
	Dir string
	// Logger receives tsdb log lines.
	Logger *slog.Logger
	// Registerer collects tsdb metrics (may be nil).
	Registerer prometheus.Registerer
	// RetentionDuration bounds local on-disk retention in ms. Local retention
	// is kept short: once a block is shipped and the store-gateway has it,
	// the merger no longer needs it locally. Defaults to 6h if <= 0.
	RetentionDuration int64
}

// OpenStorage opens (or creates) the tsdb.DB. It deliberately uses the
// Prometheus default 2h Min/MaxBlockDuration so blocks line up with the
// Thanos store-gateway / compactor expectations and so each block is a single
// S3 PUT set.
func OpenStorage(opts StorageOptions) (*Storage, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("storage: Dir is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	tsdbOpts := tsdb.DefaultOptions()
	// 2h head -> 2h block, matching the Prometheus/Thanos default. Do NOT
	// override to something exotic; store-gateway/compactor assume 2h base.
	tsdbOpts.MinBlockDuration = tsdb.DefaultBlockDuration
	tsdbOpts.MaxBlockDuration = tsdb.DefaultBlockDuration
	// WAL on for durability (DefaultOptions already enables it; be explicit by
	// leaving WALSegmentSize at its default, > 0).
	tsdbOpts.NoLockfile = false
	if opts.RetentionDuration > 0 {
		tsdbOpts.RetentionDuration = opts.RetentionDuration
	} else {
		tsdbOpts.RetentionDuration = int64(6 * 60 * 60 * 1000) // 6h in ms
	}

	db, err := tsdb.Open(opts.Dir, opts.Logger, opts.Registerer, tsdbOpts, nil)
	if err != nil {
		return nil, fmt.Errorf("storage: open tsdb at %q: %w", opts.Dir, err)
	}

	return &Storage{DB: db}, nil
}

// SetExternalLabels records the merger's external labels. They are applied to
// every ingested series so distinct agents/mergers fan into distinguishable
// series, and so the StoreAPI advertises them.
func (s *Storage) SetExternalLabels(extLset labels.Labels) {
	s.externalLabels = extLset
}

// ExternalLabels returns the configured external labels (sorted).
func (s *Storage) ExternalLabels() labels.Labels {
	return s.externalLabels
}

// Close flushes and closes the underlying tsdb.DB.
func (s *Storage) Close() error {
	if s.DB == nil {
		return nil
	}
	return s.DB.Close()
}
