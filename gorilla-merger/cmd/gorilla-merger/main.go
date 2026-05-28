// Command gorilla-merger is a Thanos-Receive-style merger for ASAP edge agents.
//
// The hot path is DECODE-FREE. It ingests Gorilla XOR-chunk fragments over HTTP
// (POST /ingest/gorilla), durably logs each raw ASAPFRG1 frame to a block-level
// WAL (fsync before the 200 ack) and buffers the raw XOR chunks per SMALL window
// (no sample decode/re-encode). On a window's close it stitches the buffered
// chunks DIRECTLY into a Prometheus TSDB block under <tsdb.path>/pending/, where
// it becomes queryable within ~window+grace (the freshest data is NOT hidden for
// a full block range). A background compactor merges the small pending blocks
// (up to a DECOUPLED, larger --merge.compact-max-span) and re-chunks them to
// ~120 samples/chunk for ratio, writing the result into <tsdb.path>/shipped/.
// The Thanos shipper watches ONLY <tsdb.path>/shipped/, so exactly the
// compacted, ratio-optimized blocks reach object storage (one PUT set each). The
// Thanos StoreAPI (gRPC) serves the UNION of pending + shipped (plus the
// separate decode-on-read cold-part store) so thanos-query can union recent +
// S3 data with no gap across the pending->shipped promotion.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	kitslog "github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/ProjectASAP/asapquery-backend/gorilla-merger/internal/merger"
)

type config struct {
	httpAddr       string
	grpcAddr       string
	tsdbDir        string
	objstoreFile   string
	externalLabels string
	shipInterval   time.Duration
	retention      time.Duration

	// Decode-free merge knobs.
	mergeWindow      time.Duration
	mergeGrace       time.Duration
	mergeWALDir      string
	mergeFlushIntvl  time.Duration
	compactInterval  time.Duration
	compactMinBlocks int
	compactMaxSpan   time.Duration
}

func main() {
	cfg := parseConfig()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	kitLogger := kitslog.NewLogfmtLogger(kitslog.NewSyncWriter(os.Stderr))

	if err := run(cfg, logger, kitLogger); err != nil {
		logger.Error("gorilla-merger exited with error", "err", err)
		os.Exit(1)
	}
}

func parseConfig() config {
	var cfg config
	fs := flag.NewFlagSet("gorilla-merger", flag.ExitOnError)
	fs.StringVar(&cfg.httpAddr, "http-address", envOr("MERGER_HTTP_ADDRESS", ":10908"),
		"HTTP listen address for the /ingest/gorilla fragment frontend and /metrics.")
	fs.StringVar(&cfg.grpcAddr, "grpc-address", envOr("MERGER_GRPC_ADDRESS", ":10907"),
		"gRPC listen address for the Thanos StoreAPI (the open-window query surface).")
	fs.StringVar(&cfg.tsdbDir, "tsdb.path", envOr("MERGER_TSDB_PATH", "./data"),
		"Local data dir root. Layout: <path>/pending (per-window L1 blocks, served not shipped), <path>/shipped (compacted L2 blocks, served AND shipped), <path>/wal (fragment WAL).")
	fs.StringVar(&cfg.objstoreFile, "objstore.config-file", envOr("MERGER_OBJSTORE_CONFIG_FILE", ""),
		"Path to a Thanos objstore bucket config YAML. Same bucket that thanos-store-gateway watches. If empty, the shipper is disabled (write path + StoreAPI only).")
	fs.StringVar(&cfg.externalLabels, "external-labels", envOr("MERGER_EXTERNAL_LABELS", ""),
		"Comma-separated external labels applied to every series and uploaded block, e.g. 'merger=m1,tier=cold'. Distinct mergers must carry a distinguishing label.")
	fs.DurationVar(&cfg.shipInterval, "shipper.interval", envDurationOr("MERGER_SHIPPER_INTERVAL", time.Minute),
		"How often the shipper scans for and uploads new blocks.")
	fs.DurationVar(&cfg.retention, "tsdb.retention", envDurationOr("MERGER_TSDB_RETENTION", 6*time.Hour),
		"Local on-disk retention. Kept short since blocks live in object storage once shipped.")

	// Decode-free merge knobs.
	fs.DurationVar(&cfg.mergeWindow, "merge.window", envDurationOr("MERGER_MERGE_WINDOW", 2*time.Minute),
		"Buffering/close window for the decode-free path; one closed window -> one directly-built PENDING block, queryable within ~window+grace+flush-tick. Kept small (2m) so recent data is NOT hidden for a full block range; compaction span (--merge.compact-max-span) controls how wide compacted blocks grow, NOT this.")
	fs.DurationVar(&cfg.mergeGrace, "merge.reorder-grace", envDurationOr("MERGER_MERGE_REORDER_GRACE", time.Minute),
		"How long after a window's end to keep accepting late/out-of-order fragments before flushing it.")
	fs.StringVar(&cfg.mergeWALDir, "merge.wal-dir", envOr("MERGER_MERGE_WAL_DIR", ""),
		"Directory for the block-level fragment WAL. Defaults to <tsdb.path>/wal.")
	fs.DurationVar(&cfg.mergeFlushIntvl, "merge.flush-interval", envDurationOr("MERGER_MERGE_FLUSH_INTERVAL", 30*time.Second),
		"How often to check for closable windows and flush them into pending blocks. 30s keeps recent-data visibility latency low (window+grace+this tick).")
	fs.DurationVar(&cfg.compactInterval, "merge.compact-interval", envDurationOr("MERGER_MERGE_COMPACT_INTERVAL", 5*time.Minute),
		"How often the background compactor merges small per-window pending blocks and re-chunks them to ~120 samples/chunk for ratio, promoting them into the shipped dir.")
	fs.IntVar(&cfg.compactMinBlocks, "merge.compact-min-blocks", envIntOr("MERGER_MERGE_COMPACT_MIN_BLOCKS", 1),
		"Minimum number of pending source blocks in a run before the compactor promotes them. 1 so even a lone pending block is re-chunked + promoted to shipped (and thus shipped); multiple pending blocks in a span still merge into one.")
	fs.DurationVar(&cfg.compactMaxSpan, "merge.compact-max-span", envDurationOr("MERGER_MERGE_COMPACT_MAX_SPAN", 2*time.Hour),
		"Max time span a single compacted (shipped) block may cover, DECOUPLED from --merge.window. 2h aligns with the Prometheus/Thanos block base. Pending blocks within one span fuse into one shipped block.")

	_ = fs.Parse(os.Args[1:])
	return cfg
}

func run(cfg config, logger *slog.Logger, kitLogger kitslog.Logger) error {
	extLset, err := parseExternalLabels(cfg.externalLabels)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()

	// 1. Storage: decode-free ingest Manager (window buffer + block WAL +
	// directly-built blocks) + BlockStore that serves them. No embedded
	// sample-appending tsdb.DB on the gorilla path.
	storage, err := merger.OpenStorage(merger.StorageOptions{
		Dir:               cfg.tsdbDir,
		WALDir:            cfg.mergeWALDir,
		WindowMs:          cfg.mergeWindow.Milliseconds(),
		ReorderGraceMs:    cfg.mergeGrace.Milliseconds(),
		Logger:            logger,
		RetentionDuration: cfg.retention.Milliseconds(),
	})
	if err != nil {
		return err
	}
	storage.SetExternalLabels(extLset)
	defer func() {
		if cerr := storage.Close(); cerr != nil {
			logger.Error("closing storage", "err", cerr)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. Cold-part store (optional — needs the object store). Write-no-decode
	// part ingest + decode-on-read query path. Shares the SAME bucket the
	// shipper uses (cold parts live under their own "cold/" key prefix), so
	// cold parts and shipped 2h blocks coexist without colliding.
	var coldStore *merger.ColdPartStore
	var coldBucket merger.BucketCloser
	if cfg.objstoreFile != "" {
		objYAML, rerr := os.ReadFile(cfg.objstoreFile)
		if rerr != nil {
			return fmt.Errorf("read objstore config %q: %w", cfg.objstoreFile, rerr)
		}
		bkt, berr := merger.NewBucket(objYAML, "gorilla-merger-cold", reg, kitLogger)
		if berr != nil {
			return fmt.Errorf("cold-part bucket: %w", berr)
		}
		coldBucket = bkt
		defer func() { _ = coldBucket.Close() }()
		coldStore = merger.NewColdPartStore(bkt, kitLogger)
		// Rediscover any parts already in the bucket (header/index only) in the
		// BACKGROUND. Reload fetches + OpenParts every stored part, so with a large
		// accumulated cold tier (thousands of parts) it takes tens of seconds and
		// is memory-heavy. Running it synchronously here BLOCKS the StoreAPI and
		// HTTP frontend from starting (they launch below), so for the whole reload
		// window after a restart thanos-query sees the gRPC endpoint as down and a
		// cold (or warm) query returns empty with no streamColdSeries activity —
		// the served-empty symptom. The manifest is mutex-guarded and queried under
		// RLock, so a concurrent reload is safe: cold queries simply see a smaller
		// (growing) manifest until it completes, then the full set. New parts POSTed
		// during the reload still register live via Put; Reload's atomic manifest
		// swap may briefly drop a part POSTed mid-reload, but the next restart's
		// reload (or a re-POST) re-registers it, and the warm/open path is unaffected.
		go func() {
			if rerr := coldStore.Reload(context.Background()); rerr != nil {
				logger.Warn("cold manifest reload failed (continuing empty)", "err", rerr)
			}
		}()
	} else {
		logger.Warn("no objstore config provided; cold-part store disabled (decode-on-read unavailable)")
	}

	// 3. Ingest HTTP frontend (decode-free: WAL + buffer, no sample append).
	ingester := merger.NewIngester(storage.Manager, logger)
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest/gorilla", ingester.HandleIngest)
	if coldStore != nil {
		mux.HandleFunc("/ingest/coldpart", coldStore.HandlePut)
	}
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/-/healthy", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/-/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	httpSrv := &http.Server{Addr: cfg.httpAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	// 4. StoreAPI (gRPC) over the open window + decode-on-read cold parts.
	storeAPI, err := merger.NewStoreAPI(storage, extLset, kitLogger, cfg.grpcAddr, coldStore)
	if err != nil {
		return err
	}
	if err := storeAPI.Listen(); err != nil {
		return err
	}

	// 5. Shipper (optional — disabled when no objstore config is provided).
	var shipperRunner *merger.ShipperRunner
	if cfg.objstoreFile != "" {
		objYAML, rerr := os.ReadFile(cfg.objstoreFile)
		if rerr != nil {
			return fmt.Errorf("read objstore config %q: %w", cfg.objstoreFile, rerr)
		}
		shipperRunner, err = merger.NewShipperRunner(merger.ShipperOptions{
			// Watch ONLY the shipped dir (compacted L2 blocks). The pending dir
			// (per-window L1 blocks) is a sibling the shipper never sees, so only
			// re-chunked, ratio-optimized blocks reach object storage.
			Dir:                storage.ShippedDir(),
			ObjstoreConfigYAML: objYAML,
			ExternalLabels:     extLset,
			Interval:           cfg.shipInterval,
			Registerer:         reg,
			Logger:             kitLogger,
		})
		if err != nil {
			return err
		}
		defer func() { _ = shipperRunner.Close() }()
	} else {
		logger.Warn("no objstore config provided; shipper disabled (write path + StoreAPI only)")
	}

	// 6. Background compactor: merge small per-window blocks + re-chunk to ~120
	// samples/chunk for a better compression ratio (offline/amortized, OFF the
	// ingest hot path).
	compactor, err := merger.NewCompactor(merger.CompactorOptions{
		Store:      storage.BlockStore(),
		Interval:   cfg.compactInterval,
		MinBlocks:  cfg.compactMinBlocks,
		MaxSpanMs:  cfg.compactMaxSpan.Milliseconds(),
		Registerer: reg,
		Logger:     logger,
	})
	if err != nil {
		return err
	}

	errCh := make(chan error, 5)

	// Flush loop: close windows + build blocks on a ticker (flush everything on
	// shutdown).
	go func() {
		logger.Info("starting flush loop", "interval", cfg.mergeFlushIntvl,
			"window", cfg.mergeWindow, "grace", cfg.mergeGrace)
		if serr := storage.Manager.RunFlush(ctx, cfg.mergeFlushIntvl); serr != nil && serr != context.Canceled {
			errCh <- fmt.Errorf("flush loop: %w", serr)
		}
	}()

	go func() {
		logger.Info("starting compactor", "interval", cfg.compactInterval, "min_blocks", cfg.compactMinBlocks)
		if serr := compactor.Run(ctx); serr != nil && serr != context.Canceled {
			errCh <- fmt.Errorf("compactor: %w", serr)
		}
	}()

	go func() {
		logger.Info("starting HTTP ingest frontend", "addr", cfg.httpAddr)
		if serr := httpSrv.ListenAndServe(); serr != nil && serr != http.ErrServerClosed {
			errCh <- fmt.Errorf("http server: %w", serr)
		}
	}()

	go func() {
		logger.Info("starting Thanos StoreAPI", "addr", storeAPI.Addr())
		if serr := storeAPI.Serve(); serr != nil {
			errCh <- fmt.Errorf("storeapi: %w", serr)
		}
	}()

	if shipperRunner != nil {
		go func() {
			logger.Info("starting shipper", "interval", cfg.shipInterval, "dir", storage.ShippedDir())
			if serr := shipperRunner.Run(ctx); serr != nil && serr != context.Canceled {
				errCh <- fmt.Errorf("shipper: %w", serr)
			}
		}()
	}

	logger.Info("gorilla-merger up",
		"http", cfg.httpAddr, "grpc", storeAPI.Addr(), "tsdb", cfg.tsdbDir,
		"external_labels", extLset.String(), "shipper_enabled", shipperRunner != nil)

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case serr := <-errCh:
		logger.Error("component failed", "err", serr)
		err = serr
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	storeAPI.Stop()

	return err
}

// parseExternalLabels turns "k1=v1,k2=v2" into a sorted labels.Labels set.
func parseExternalLabels(s string) (labels.Labels, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return labels.EmptyLabels(), nil
	}
	bld := labels.NewBuilder(labels.EmptyLabels())
	pairs := strings.Split(s, ",")
	sort.Strings(pairs)
	for _, p := range pairs {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		k, v, ok := strings.Cut(p, "=")
		if !ok {
			return labels.EmptyLabels(), fmt.Errorf("external label %q is not in k=v form", p)
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if k == "" {
			return labels.EmptyLabels(), fmt.Errorf("external label has empty name in %q", p)
		}
		bld.Set(k, v)
	}
	return bld.Labels(), nil
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envDurationOr(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envIntOr(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
