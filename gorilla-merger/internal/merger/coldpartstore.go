package merger

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/ProjectASAP/asap-gorilla-go/coldpart"
	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	objclient "github.com/thanos-io/objstore/client"
)

// BucketCloser is the subset of objstore.Bucket main needs to manage the cold
// bucket's lifecycle (build it, hand it to the store, close it on shutdown).
type BucketCloser = objstore.Bucket

// NewBucket builds an objstore.Bucket from a Thanos/objstore config YAML — the
// same config the shipper consumes — so the cold-part store can share the
// merger's existing S3/MinIO bucket. component names the metrics namespace.
func NewBucket(configYAML []byte, component string, reg prometheus.Registerer, logger kitlog.Logger) (objstore.Bucket, error) {
	if logger == nil {
		logger = kitlog.NewNopLogger()
	}
	if len(configYAML) == 0 {
		return nil, fmt.Errorf("coldpartstore: objstore config YAML is required")
	}
	bkt, err := objclient.NewBucket(logger, configYAML, component, nil)
	if err != nil {
		return nil, fmt.Errorf("coldpartstore: build bucket: %w", err)
	}
	return bkt, nil
}

// maxColdPartBody bounds the request body the cold-part POST handler will read.
const maxColdPartBody = 256 << 20 // 256 MiB

// ColdPartPrefix is the object-store key prefix under which serialized cold
// parts are stored. Keeping cold parts under a dedicated prefix keeps them
// cleanly separated from the Thanos 2h blocks the shipper writes to the same
// bucket (those live under their block-ULID directories), so the
// store-gateway's block discovery and the cold manifest's part discovery never
// collide.
const ColdPartPrefix = "cold/"

// partObjectSuffix is appended to every cold part object key.
const partObjectSuffix = ".part"

// ColdPartStore is the backend write-no-decode store for cold parts: it accepts
// a serialized part, validates it (OpenPart — header/version/crc, no chunk-body
// decode), stores the object VERBATIM to the object store, and registers it in
// an in-memory manifest keyed by [block_start,block_end] + the label sets it
// covers so the decode-on-read query path can find overlapping parts.
//
// It deliberately does NOT touch the fragment-ingest -> tsdb path. It reuses the
// SAME objstore.Bucket abstraction the shipper uses (so the S3/MinIO bucket
// built by objclient.NewBucket works in production, and objstore.NewInMemBucket
// works in tests), but writes under ColdPartPrefix so cold parts and shipped
// 2h blocks coexist in one bucket without colliding.
type ColdPartStore struct {
	bkt    objstore.Bucket
	logger kitlog.Logger

	mu  sync.RWMutex
	man *coldManifest
}

// NewColdPartStore builds a store over bkt. It does not eagerly scan the bucket;
// call Reload to (re)build the manifest from already-stored parts (e.g. on
// startup after a restart).
func NewColdPartStore(bkt objstore.Bucket, logger kitlog.Logger) *ColdPartStore {
	if logger == nil {
		logger = kitlog.NewNopLogger()
	}
	return &ColdPartStore{
		bkt:    bkt,
		logger: logger,
		man:    newColdManifest(),
	}
}

// partKey derives a deterministic, content-addressed object key for a part.
// The block range is embedded for human/debug readability and time-prefix
// listing; the content hash makes re-POSTing the identical part idempotent
// (same bytes -> same key -> overwrite, no duplicate manifest entry). The
// agent-encode side (the producer) MUST agree on this scheme — or POST the
// bytes and let the store assign the key (the store ignores any client key and
// derives its own from the validated part), which is the contract here.
func partKey(blockStartMs, blockEndMs int64, partBytes []byte) string {
	sum := sha256.Sum256(partBytes)
	return fmt.Sprintf("%s%020d-%020d/%s%s",
		ColdPartPrefix, blockStartMs, blockEndMs, hex.EncodeToString(sum[:]), partObjectSuffix)
}

// Put validates partBytes with OpenPart (rejecting a corrupt/oversized object
// before any storage write), stores the bytes VERBATIM under a derived key, and
// registers the part in the manifest. It returns the assigned object key.
//
// Put performs NO decode and NO re-encode: the stored object is byte-for-byte
// the input. Storing the same bytes twice is idempotent (same content-addressed
// key, single manifest entry).
func (s *ColdPartStore) Put(ctx context.Context, partBytes []byte) (string, error) {
	part, err := coldpart.OpenPart(partBytes)
	if err != nil {
		return "", fmt.Errorf("coldpartstore: validate part: %w", err)
	}

	key := partKey(part.BlockStartMs, part.BlockEndMs, partBytes)

	// Store verbatim. Upload is idempotent per the objstore contract; a re-POST
	// of identical bytes overwrites with the same content.
	if err := s.bkt.Upload(ctx, key, bytes.NewReader(partBytes)); err != nil {
		return "", fmt.Errorf("coldpartstore: upload %q: %w", key, err)
	}

	entry := manifestEntryFromPart(key, part)
	s.mu.Lock()
	s.man.add(entry)
	s.mu.Unlock()

	level.Debug(s.logger).Log("msg", "stored cold part", "key", key,
		"block_start_ms", part.BlockStartMs, "block_end_ms", part.BlockEndMs,
		"series", part.NumSeries())
	return key, nil
}

// Reload rebuilds the in-memory manifest from the parts currently in the bucket
// under ColdPartPrefix. Each part is fetched and OpenPart'd (header/index only —
// no chunk-body decode), so this is cheap relative to the stored data size. Use
// it on startup so a restarted merger rediscovers previously stored parts.
func (s *ColdPartStore) Reload(ctx context.Context) error {
	man := newColdManifest()
	var keys []string
	err := s.bkt.Iter(ctx, ColdPartPrefix, func(name string) error {
		keys = append(keys, name)
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return fmt.Errorf("coldpartstore: iter %q: %w", ColdPartPrefix, err)
	}

	for _, key := range keys {
		if !isPartKey(key) {
			continue
		}
		b, gerr := s.fetch(ctx, key)
		if gerr != nil {
			level.Warn(s.logger).Log("msg", "skip unreadable cold part", "key", key, "err", gerr)
			continue
		}
		part, perr := coldpart.OpenPart(b)
		if perr != nil {
			level.Warn(s.logger).Log("msg", "skip invalid cold part", "key", key, "err", perr)
			continue
		}
		man.add(manifestEntryFromPart(key, part))
	}

	s.mu.Lock()
	s.man = man
	s.mu.Unlock()
	level.Info(s.logger).Log("msg", "reloaded cold manifest", "parts", len(man.entries))
	return nil
}

// fetch reads a whole object from the bucket into memory.
func (s *ColdPartStore) fetch(ctx context.Context, key string) ([]byte, error) {
	rc, err := s.bkt.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

// PartsOverlapping returns the manifest entries whose [block_start,block_end]
// overlaps the inclusive window [mintMs,maxtMs] AND whose covered label sets
// could satisfy all matchers (a cheap pre-filter; the authoritative per-series
// matcher/time filtering happens in coldpart.Part.Series). Entries are returned
// in ascending block-start order for deterministic query output.
func (s *ColdPartStore) PartsOverlapping(mintMs, maxtMs int64, matchers []*labels.Matcher) []manifestEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.man.overlapping(mintMs, maxtMs, matchers)
}

// NumParts reports how many parts the manifest currently tracks.
func (s *ColdPartStore) NumParts() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.man.entries)
}

// MinBlockStart returns the smallest block_start_ms across all tracked parts
// and true, or (0,false) when the manifest is empty. The StoreAPI uses it to
// lower its advertised MinTime to the oldest cold data, so thanos-query routes
// queries for old (cold-only) windows to the merger instead of pruning it. The
// per-series [min_ts,max_ts] index entries (not the block range) remain the
// authoritative time filter inside the query path; this is only the advertised
// floor of what the merger MIGHT serve.
func (s *ColdPartStore) MinBlockStart() (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.man.entries) == 0 {
		return 0, false
	}
	min := s.man.entries[0].BlockStartMs
	for _, e := range s.man.entries[1:] {
		if e.BlockStartMs < min {
			min = e.BlockStartMs
		}
	}
	return min, true
}

// Bucket exposes the underlying bucket (used by the query path and tests).
func (s *ColdPartStore) Bucket() objstore.Bucket { return s.bkt }

// HandlePut is the HTTP handler for POST /ingest/coldpart. The request body is
// a single serialized cold part (the raw bytes WritePart produced); the handler
// validates and stores it verbatim (no decode/re-encode). It returns 200 with
// the assigned object key on success, 400 on an invalid part, 500 on a storage
// failure. This is the write-no-decode entry point the agent-encode side POSTs
// parts to.
func (s *ColdPartStore) HandlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	reader := http.MaxBytesReader(w, r.Body, maxColdPartBody)
	body, err := io.ReadAll(reader)
	if err != nil {
		http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
		return
	}

	key, err := s.Put(r.Context(), body)
	if err != nil {
		// A validation failure (OpenPart) is the client's fault (4xx); an upload
		// failure is ours (5xx). coldpart's validation errors are wrapped under
		// "validate part".
		status := http.StatusInternalServerError
		if isColdPartClientError(err) {
			status = http.StatusBadRequest
		}
		level.Error(s.logger).Log("msg", "cold part put failed", "err", err, "status", status)
		http.Error(w, err.Error(), status)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok key=%s\n", key)
}

// isColdPartClientError reports whether err came from part validation (a bad
// body) rather than a storage write. Put wraps validation failures under
// "validate part".
func isColdPartClientError(err error) bool {
	return strings.Contains(err.Error(), "validate part")
}

// isPartKey reports whether key is a cold part object (under the prefix, with
// the part suffix) rather than some other object that happens to share the
// prefix.
func isPartKey(key string) bool {
	return len(key) > len(ColdPartPrefix)+len(partObjectSuffix) &&
		key[:len(ColdPartPrefix)] == ColdPartPrefix &&
		key[len(key)-len(partObjectSuffix):] == partObjectSuffix
}

// ---------------------------------------------------------------------------
// manifest / index
// ---------------------------------------------------------------------------

// manifestEntry indexes one stored part for overlap queries: its object key,
// block time range, and the (sorted-by-labels) label sets of every series it
// covers. The label sets come from the part's index (no chunk decode), and let
// PartsOverlapping cheaply skip a part that cannot satisfy a query's matchers.
type manifestEntry struct {
	Key          string
	BlockStartMs int64
	BlockEndMs   int64
	SeriesLabels []labels.Labels
}

// manifestEntryFromPart builds an entry from a parsed (OpenPart'd) part. It
// reads only the part's header + index (label sets), never a chunk body.
func manifestEntryFromPart(key string, part *coldpart.Part) manifestEntry {
	return manifestEntry{
		Key:          key,
		BlockStartMs: part.BlockStartMs,
		BlockEndMs:   part.BlockEndMs,
		SeriesLabels: part.SeriesLabels(),
	}
}

// coldManifest is the in-memory index of stored parts. It is intentionally
// simple (a slice scanned per query): the cold tier holds relatively few large
// parts, and PartsOverlapping is called once per query, not per sample.
type coldManifest struct {
	entries []manifestEntry
	keys    map[string]struct{} // dedup by object key
}

func newColdManifest() *coldManifest {
	return &coldManifest{keys: make(map[string]struct{})}
}

// add registers an entry, ignoring a duplicate object key (idempotent re-Put).
func (m *coldManifest) add(e manifestEntry) {
	if _, ok := m.keys[e.Key]; ok {
		return
	}
	m.keys[e.Key] = struct{}{}
	m.entries = append(m.entries, e)
}

// overlapping returns entries whose block range overlaps [mintMs,maxtMs] and
// whose label sets could satisfy matchers, sorted by ascending block start.
func (m *coldManifest) overlapping(mintMs, maxtMs int64, matchers []*labels.Matcher) []manifestEntry {
	var out []manifestEntry
	for _, e := range m.entries {
		// Inclusive block-range overlap, mirroring coldpart.Part.Series.
		if e.BlockStartMs > maxtMs || e.BlockEndMs < mintMs {
			continue
		}
		if !entryCouldMatch(e, matchers) {
			continue
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].BlockStartMs != out[j].BlockStartMs {
			return out[i].BlockStartMs < out[j].BlockStartMs
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// entryCouldMatch reports whether any of the part's series satisfies every
// matcher. It is a pre-filter to skip whole parts that cannot contribute;
// coldpart.Part.Series re-applies the matchers authoritatively per series.
func entryCouldMatch(e manifestEntry, matchers []*labels.Matcher) bool {
	if len(matchers) == 0 {
		return true
	}
	for _, ls := range e.SeriesLabels {
		if coldpart.MatchesAll(ls, matchers) {
			return true
		}
	}
	return false
}
