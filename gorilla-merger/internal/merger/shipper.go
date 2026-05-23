package merger

import (
	"context"
	"fmt"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	objclient "github.com/thanos-io/objstore/client"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/shipper"
)

// ShipperRunner watches the tsdb data dir for newly cut 2h blocks and uploads
// each one to the configured object-storage bucket as a single PUT set
// (chunks + index + meta.json with the Thanos thanos{} meta section). The
// bucket must be the same one thanos-store-gateway watches.
type ShipperRunner struct {
	shipper  *shipper.Shipper
	bucket   objstore.Bucket
	interval time.Duration
	logger   kitlog.Logger
}

// ShipperOptions configures the shipper.
type ShipperOptions struct {
	// Dir is the tsdb data dir (same dir passed to OpenStorage).
	Dir string
	// ObjstoreConfigYAML is a Thanos/objstore bucket config (the same YAML
	// format thanos components consume, e.g. type: S3 with a config: block).
	ObjstoreConfigYAML []byte
	// ExternalLabels are attached to every uploaded block's Thanos meta so the
	// store-gateway/query layer can identify the source.
	ExternalLabels labels.Labels
	// Interval is how often Sync runs. Defaults to 1m if <= 0.
	Interval time.Duration
	// Registerer collects shipper + objstore metrics (may be nil).
	Registerer prometheus.Registerer
	// Logger receives shipper log lines.
	Logger kitlog.Logger
}

// NewShipperRunner builds the bucket from config and wires a Thanos shipper.
func NewShipperRunner(opts ShipperOptions) (*ShipperRunner, error) {
	if len(opts.ObjstoreConfigYAML) == 0 {
		return nil, fmt.Errorf("shipper: ObjstoreConfigYAML is required")
	}
	logger := opts.Logger
	if logger == nil {
		logger = kitlog.NewNopLogger()
	}
	bkt, err := objclient.NewBucket(logger, opts.ObjstoreConfigYAML, "gorilla-merger", nil)
	if err != nil {
		return nil, fmt.Errorf("shipper: build bucket: %w", err)
	}
	return newShipperRunnerWithBucket(bkt, opts)
}

// newShipperRunnerWithBucket wires a shipper over an already-built bucket. It
// is the shared core of NewShipperRunner and is also used by tests (which
// inject an in-memory bucket).
func newShipperRunnerWithBucket(bkt objstore.Bucket, opts ShipperOptions) (*ShipperRunner, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("shipper: Dir is required")
	}
	logger := opts.Logger
	if logger == nil {
		logger = kitlog.NewNopLogger()
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = time.Minute
	}

	extLset := opts.ExternalLabels
	s := shipper.New(
		bkt,
		opts.Dir,
		shipper.WithLogger(logger),
		shipper.WithRegisterer(opts.Registerer),
		shipper.WithSource(metadata.ReceiveSource),
		shipper.WithLabels(func() labels.Labels { return extLset }),
	)

	return &ShipperRunner{
		shipper:  s,
		bucket:   bkt,
		interval: interval,
		logger:   logger,
	}, nil
}

// Run drives Shipper.Sync on a ticker until ctx is cancelled. Each Sync
// uploads any new on-disk blocks (one PUT set per block).
func (r *ShipperRunner) Run(ctx context.Context) error {
	t := time.NewTicker(r.interval)
	defer t.Stop()

	// Sync once promptly on startup so a restart re-uploads any block left on
	// disk before the previous process exited.
	r.syncOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			r.syncOnce(ctx)
		}
	}
}

func (r *ShipperRunner) syncOnce(ctx context.Context) {
	uploaded, err := r.shipper.Sync(ctx)
	if err != nil {
		level.Warn(r.logger).Log("msg", "shipper sync failed", "err", err, "uploaded", uploaded)
		return
	}
	if uploaded > 0 {
		level.Info(r.logger).Log("msg", "shipper uploaded blocks", "uploaded", uploaded)
	}
}

// Bucket exposes the underlying bucket (used by tests).
func (r *ShipperRunner) Bucket() objstore.Bucket { return r.bucket }

// Close closes the bucket client.
func (r *ShipperRunner) Close() error {
	if r.bucket != nil {
		return r.bucket.Close()
	}
	return nil
}
