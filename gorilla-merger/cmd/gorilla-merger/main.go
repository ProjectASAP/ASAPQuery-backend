// Command gorilla-merger is a Thanos-Receive-style merger for ASAP edge agents.
//
// It ingests Gorilla XOR-chunk fragments over HTTP (POST /ingest/gorilla),
// appends their samples to an embedded Prometheus tsdb.DB with a 2h block
// range, ships completed 2h blocks to object storage via the Thanos shipper
// (one PUT set per block), and exposes a Thanos StoreAPI (gRPC) over the open
// (<2h pending) window so thanos-query can union recent + S3 data.
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
		"Local directory for the embedded tsdb.DB (WAL + pending/unshipped blocks).")
	fs.StringVar(&cfg.objstoreFile, "objstore.config-file", envOr("MERGER_OBJSTORE_CONFIG_FILE", ""),
		"Path to a Thanos objstore bucket config YAML. Same bucket that thanos-store-gateway watches. If empty, the shipper is disabled (write path + StoreAPI only).")
	fs.StringVar(&cfg.externalLabels, "external-labels", envOr("MERGER_EXTERNAL_LABELS", ""),
		"Comma-separated external labels applied to every series and uploaded block, e.g. 'merger=m1,tier=cold'. Distinct mergers must carry a distinguishing label.")
	fs.DurationVar(&cfg.shipInterval, "shipper.interval", envDurationOr("MERGER_SHIPPER_INTERVAL", time.Minute),
		"How often the shipper scans for and uploads new blocks.")
	fs.DurationVar(&cfg.retention, "tsdb.retention", envDurationOr("MERGER_TSDB_RETENTION", 6*time.Hour),
		"Local on-disk retention. Kept short since blocks live in object storage once shipped.")
	_ = fs.Parse(os.Args[1:])
	return cfg
}

func run(cfg config, logger *slog.Logger, kitLogger kitslog.Logger) error {
	extLset, err := parseExternalLabels(cfg.externalLabels)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()

	// 1. Storage: embedded tsdb.DB, 2h block range + WAL.
	storage, err := merger.OpenStorage(merger.StorageOptions{
		Dir:               cfg.tsdbDir,
		Logger:            logger,
		Registerer:        reg,
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

	// 2. Ingest HTTP frontend.
	ingester := merger.NewIngester(storage, logger)
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest/gorilla", ingester.HandleIngest)
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/-/healthy", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/-/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	httpSrv := &http.Server{Addr: cfg.httpAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	// 3. StoreAPI (gRPC) over the open window.
	storeAPI, err := merger.NewStoreAPI(storage, extLset, kitLogger, cfg.grpcAddr)
	if err != nil {
		return err
	}
	if err := storeAPI.Listen(); err != nil {
		return err
	}

	// 4. Shipper (optional — disabled when no objstore config is provided).
	var shipperRunner *merger.ShipperRunner
	if cfg.objstoreFile != "" {
		objYAML, rerr := os.ReadFile(cfg.objstoreFile)
		if rerr != nil {
			return fmt.Errorf("read objstore config %q: %w", cfg.objstoreFile, rerr)
		}
		shipperRunner, err = merger.NewShipperRunner(merger.ShipperOptions{
			Dir:                cfg.tsdbDir,
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

	errCh := make(chan error, 3)

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
			logger.Info("starting shipper", "interval", cfg.shipInterval, "dir", cfg.tsdbDir)
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
