package merger

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// maxIngestBody bounds the (decompressed) request body the handler will read.
const maxIngestBody = 256 << 20 // 256 MiB

// appendable is the slice of tsdb.DB the ingest path needs. It exists so the
// handler can be unit-tested without a full DB if ever needed.
type appendable interface {
	Appender(ctx context.Context) storage.Appender
}

// Ingester decodes ASAPFRG1 fragment batches and appends their samples to the
// embedded tsdb.DB.
type Ingester struct {
	app            appendable
	externalLabels labels.Labels
	logger         *slog.Logger
}

// NewIngester builds an Ingester over the given storage.
func NewIngester(s *Storage, logger *slog.Logger) *Ingester {
	if logger == nil {
		logger = slog.Default()
	}
	return &Ingester{
		app:            s.DB,
		externalLabels: s.externalLabels,
		logger:         logger,
	}
}

// labelsFor builds the series labels for a fragment: __name__ = MetricName,
// plus the fragment Attributes, plus the merger's external labels. External
// labels win on conflict (they identify this merger/agent and must be stable).
func labelsFor(metricName string, attrs map[string]string, external labels.Labels) labels.Labels {
	bld := labels.NewBuilder(labels.EmptyLabels())
	bld.Set(labels.MetricName, metricName)
	for k, v := range attrs {
		bld.Set(k, v)
	}
	external.Range(func(l labels.Label) {
		bld.Set(l.Name, l.Value)
	})
	return bld.Labels()
}

// ingestResult summarizes a successful ingest for logging/metrics.
type ingestResult struct {
	fragments int
	samples   int
	dropped   int
}

// IngestBatch decodes a raw (already-decompressed) ASAPFRG1 frame and appends
// all of its samples through a single appender, committing once at the end so
// the whole batch is atomic and WAL-durable on return.
func (i *Ingester) IngestBatch(ctx context.Context, raw []byte) (ingestResult, error) {
	frags, err := gorilla.DecodeFragmentBatch(raw)
	if err != nil {
		return ingestResult{}, fmt.Errorf("decode fragment batch: %w", err)
	}

	app := i.app.Appender(ctx)
	res := ingestResult{fragments: len(frags)}

	for fi := range frags {
		f := &frags[fi]
		if f.Count == 0 || len(f.Data) == 0 {
			continue
		}
		if f.Encoding != "" && f.Encoding != "xor" {
			_ = app.Rollback()
			return ingestResult{}, fmt.Errorf("fragment %d: unsupported encoding %q", fi, f.Encoding)
		}
		chunk, cerr := chunkenc.FromData(chunkenc.EncXOR, f.Data)
		if cerr != nil {
			_ = app.Rollback()
			return ingestResult{}, fmt.Errorf("fragment %d: decode xor chunk: %w", fi, cerr)
		}
		ls := labelsFor(f.MetricName, f.Attributes, i.externalLabels)

		it := chunk.Iterator(nil)
		var ref storage.SeriesRef
		for it.Next() == chunkenc.ValFloat {
			t, v := it.At()
			newRef, aerr := app.Append(ref, ls, t, v)
			if aerr != nil {
				// Out-of-order / duplicate samples are expected when agents
				// overlap or replay; tolerate them rather than failing the
				// whole batch.
				if errors.Is(aerr, storage.ErrOutOfOrderSample) ||
					errors.Is(aerr, storage.ErrDuplicateSampleForTimestamp) ||
					errors.Is(aerr, storage.ErrOutOfBounds) {
					res.dropped++
					continue
				}
				_ = app.Rollback()
				return ingestResult{}, fmt.Errorf("fragment %d: append (%s @ %d): %w", fi, ls.String(), t, aerr)
			}
			ref = newRef
			res.samples++
		}
		if itErr := it.Err(); itErr != nil {
			_ = app.Rollback()
			return ingestResult{}, fmt.Errorf("fragment %d: iterate xor chunk: %w", fi, itErr)
		}
	}

	if cerr := app.Commit(); cerr != nil {
		return ingestResult{}, fmt.Errorf("commit appender: %w", cerr)
	}
	return res, nil
}

// HandleIngest is the HTTP handler for POST /ingest/gorilla. It reads the body
// (gunzipping when Content-Encoding: gzip), decodes the fragment batch, and
// appends the samples. Returns 200 on success, 4xx on a malformed body, 5xx on
// an append/commit failure.
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
		// A decode error is the client's fault (4xx); a commit error is ours
		// (5xx). We distinguish by message prefix kept simple: decode failures
		// surface as bad request.
		status := http.StatusInternalServerError
		if isClientError(err) {
			status = http.StatusBadRequest
		}
		i.logger.Error("ingest failed", "err", err, "status", status)
		http.Error(w, err.Error(), status)
		return
	}

	i.logger.Debug("ingested fragment batch",
		"fragments", res.fragments, "samples", res.samples, "dropped", res.dropped)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok fragments=%d samples=%d dropped=%d\n", res.fragments, res.samples, res.dropped)
}

// isClientError reports whether err originates from a malformed request body
// (decode/encoding/chunk problems) rather than a storage failure.
func isClientError(err error) bool {
	msg := err.Error()
	for _, p := range []string{"decode fragment batch", "decode xor chunk", "unsupported encoding", "iterate xor chunk"} {
		if len(msg) >= len(p) && containsPrefixWord(msg, p) {
			return true
		}
	}
	return false
}

func containsPrefixWord(s, sub string) bool {
	// Cheap substring check; the wrapped error messages embed these phrases.
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
