package merger

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/prometheus/prometheus/model/labels"
)

// maxIngestBody bounds the (decompressed) request body the handler will read.
const maxIngestBody = 256 << 20 // 256 MiB

// Ingester is the decode-free HTTP frontend for POST /ingest/gorilla. It does
// NOT decode samples: it hands each raw ASAPFRG1 frame to the Manager, which
// durably logs it (WAL) and buffers the raw XOR chunks per window for later
// direct block stitching. This replaces the old "decode XOR chunk -> append
// every sample through a tsdb Appender -> re-encode" double-codec hot path.
type Ingester struct {
	mgr    *Manager
	logger *slog.Logger
}

// NewIngester builds an Ingester over the given Manager.
func NewIngester(mgr *Manager, logger *slog.Logger) *Ingester {
	if logger == nil {
		logger = slog.Default()
	}
	return &Ingester{mgr: mgr, logger: logger}
}

// labelsFor builds the series labels for a fragment: __name__ = MetricName,
// plus the fragment Attributes. External labels (cluster/merger) are NOT stamped
// into the stored series: the Thanos store appends them at query time and the
// shipper writes them into each block's meta. Stamping them here too produced
// DUPLICATE cluster/merger labels that crashed the label decode path.
func labelsFor(metricName string, attrs map[string]string) labels.Labels {
	bld := labels.NewBuilder(labels.EmptyLabels())
	bld.Set(labels.MetricName, metricName)
	for k, v := range attrs {
		bld.Set(k, v)
	}
	return bld.Labels()
}

// ingestResult summarizes a successful ingest for logging/metrics. With the
// decode-free path "samples" is the sum of fragment sample COUNTS (taken from
// the headers — not decoded), reported for observability parity.
type ingestResult struct {
	fragments int
	samples   int
}

// IngestBatch durably logs the raw ASAPFRG1 frame and buffers its fragments'
// raw chunks WITHOUT decoding any samples. It returns only after the WAL fsync,
// so the caller may ack the agent on a nil error.
func (i *Ingester) IngestBatch(_ context.Context, raw []byte) (ingestResult, error) {
	// The Manager re-parses the frame structure to count fragments + buffer
	// chunks; do a cheap pre-count here only for the result/error attribution.
	buffered, err := i.mgr.Append(raw)
	if err != nil {
		return ingestResult{}, err
	}
	return ingestResult{fragments: buffered, samples: 0}, nil
}

// HandleIngest is the HTTP handler for POST /ingest/gorilla. It reads the body
// (gunzipping when Content-Encoding: gzip), durably logs the frame, and buffers
// its raw chunks. Returns 200 on success, 4xx on a malformed body, 5xx on a
// WAL/durability failure.
func (i *Ingester) HandleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var reader io.Reader = http.MaxBytesReader(w, r.Body, maxIngestBody)
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(reader)
		if err != nil {
			http.Error(w, fmt.Sprintf("gzip: %v", err), http.StatusBadRequest)
			return
		}
		defer gz.Close()
		reader = gz
	}

	raw, err := io.ReadAll(reader)
	if err != nil {
		http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
		return
	}

	res, err := i.IngestBatch(r.Context(), raw)
	if err != nil {
		// A decode/encoding error is the client's fault (4xx); a WAL failure is
		// ours (5xx).
		status := http.StatusInternalServerError
		if isClientError(err) {
			status = http.StatusBadRequest
		}
		i.logger.Error("ingest failed", "err", err, "status", status)
		http.Error(w, err.Error(), status)
		return
	}

	i.logger.Debug("buffered fragment batch", "fragments", res.fragments)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok fragments=%d\n", res.fragments)
}

// isClientError reports whether err originates from a malformed request body
// (decode/encoding problems) rather than a durability/storage failure.
func isClientError(err error) bool {
	msg := err.Error()
	for _, p := range []string{"decode fragment batch", "unsupported encoding"} {
		if len(msg) >= len(p) && containsPrefixWord(msg, p) {
			return true
		}
	}
	return false
}

func containsPrefixWord(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
