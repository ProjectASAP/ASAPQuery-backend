package merger

import (
	"context"
	"math"
	"sort"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// customStore is a minimal storepb.StoreServer implemented directly over the
// merger's embedded *tsdb.DB, replacing thanos's store.TSDBStore.
//
// WHY a custom server instead of store.TSDBStore:
//
// Under Prometheus v0.308's default `stringlabels` build, labels.Labels is a
// single packed string (struct{ data string }), NOT a []Label slice. Thanos
// store.TSDBStore.Series builds storepb.Series labels via
// labelpb.ZLabelsFromPromLabels, an UNSAFE *(*[]ZLabel)(unsafe.Pointer(&lset))
// reinterpret that assumes the []Label layout. On stringlabels it reads the
// packed-string struct as a slice header, producing ZLabels whose string
// headers carry garbage lengths. TSDBStore.Series then wraps the stream in a
// resortingServer whose Send calls labelpb.ReAllocZLabelsStrings(.., false) ->
// string(noAllocBytes(name)), which tries to materialize a ~8EB string and the
// process dies with "runtime: out of memory". (thanos v0.41.0 is the latest
// release and requires Go 1.25, so there is no version/toolchain escape.)
//
// This server avoids BOTH unsafe paths:
//   - It never calls ZLabelsFromPromLabels / ReAllocZLabelsStrings / the
//     resortingServer/flushable wrappers.
//   - It builds every ZLabel by COPYING the Name/Value strings field-by-field
//     (see zLabelsCopy), and relies on Querier(sorted=true) for ordering, so no
//     post-hoc resort (and thus no ReAllocZLabelsStrings) is needed.
type customStore struct {
	db      chunkQueryable
	extLset labels.Labels
	logger  kitlog.Logger
	// cold is the decode-on-read cold-part query path. It is OPTIONAL: when nil
	// the store serves only the embedded tsdb.DB (open-window) path, exactly as
	// before. When set, Series unions the cold results in AFTER the tsdb series,
	// so thanos-query/PromQL merges them just like the store-gateway overlap.
	cold *ColdQuerier
	storepb.UnimplementedStoreServer
}

// chunkQueryable is the slice of *tsdb.DB the custom store needs. Narrowed to
// an interface so the Series path can be unit-tested without a full gRPC stack.
type chunkQueryable interface {
	ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error)
	Querier(mint, maxt int64) (storage.Querier, error)
	StartTime() (int64, error)
}

// newCustomStore builds the custom StoreServer over db with the given external
// labels (must be sorted; labels.New/labels.Builder already sorts).
func newCustomStore(db chunkQueryable, extLset labels.Labels, logger kitlog.Logger) *customStore {
	if logger == nil {
		logger = kitlog.NewNopLogger()
	}
	return &customStore{db: db, extLset: extLset, logger: logger}
}

// setColdQuerier attaches the decode-on-read cold-part query path. Passing nil
// leaves the store tsdb-only.
func (s *customStore) setColdQuerier(c *ColdQuerier) { s.cold = c }

// timeRange mirrors TSDBStore.TimeRange: min = head StartTime (the oldest
// sample currently held), max = +inf so the open window is always queried.
//
// The advertised MinTime is the MINIMUM of two lower bounds, whichever exist:
// the tsdb head StartTime and (when a cold querier is attached and tracks parts)
// the oldest cold part's block start. This is load-bearing: thanos-query prunes
// a store from a query's fan-out when the query window falls entirely below the
// store's advertised MinTime.
//
// Two failure modes this guards against, both of which leave stored cold parts
// served EMPTY with no streamColdSeries activity:
//
//   - Cold data OLDER than the head: a query for that old (cold-only) window is
//     pruned unless the cold floor lowers MinTime to cover it.
//
//   - An EMPTY tsdb head: tsdb.DB.StartTime() returns math.MaxInt64 for a head
//     holding no samples (e.g. right after a restart, before the first warm
//     fragment lands, while cold parts already exist in S3 and were reloaded).
//     Advertising MaxInt64 prunes the merger from EVERY query — no window can be
//     >= MaxInt64 — so even the cold parts are unreachable. We must NOT let the
//     empty-head sentinel win over the cold floor (or survive as the advertised
//     MinTime at all), hence taking the min of the two bounds rather than only
//     lowering when cold < head.
func (s *customStore) timeRange() (int64, int64) {
	// math.MaxInt64 means "no lower bound from this source": an empty tsdb head
	// reports StartTime == MaxInt64, which must NOT become the advertised floor.
	minTime := int64(math.MaxInt64)
	if st, err := s.db.StartTime(); err == nil {
		minTime = st
	}
	if s.cold != nil {
		if coldMin, ok := s.cold.MinBlockStart(); ok && coldMin < minTime {
			minTime = coldMin
		}
	}
	// If neither source supplied a real lower bound (empty head, no cold parts),
	// the store currently holds nothing; advertise MinInt64 so thanos-query does
	// not prune it (it simply returns no series for any window, which is correct
	// and cheap) and so it stays discoverable for when data arrives.
	if minTime == math.MaxInt64 {
		minTime = math.MinInt64
	}
	return minTime, math.MaxInt64
}

// labelSet advertises the merger's external labels to the Info service. Built
// via the SAFE copying path (ZLabelSetsFromPromLabels copies each Name/Value),
// never the unsafe ZLabelsFromPromLabels.
func (s *customStore) labelSet() []labelpb.ZLabelSet {
	if s.extLset.IsEmpty() {
		return []labelpb.ZLabelSet{}
	}
	return labelpb.ZLabelSetsFromPromLabels(s.extLset)
}

// tsdbInfos advertises this store's single TSDB (external labels + time range)
// to the Info service, mirroring TSDBStore.TSDBInfos.
func (s *customStore) tsdbInfos() []infopb.TSDBInfo {
	sets := s.labelSet()
	if len(sets) == 0 {
		return []infopb.TSDBInfo{}
	}
	mint, maxt := s.timeRange()
	return []infopb.TSDBInfo{
		{
			Labels:  labelpb.ZLabelSet{Labels: sets[0].Labels},
			MinTime: mint,
			MaxTime: maxt,
		},
	}
}

// zLabelsCopy converts prom labels to []labelpb.ZLabel by COPYING each
// Name/Value string. This is the SAFE analogue of labelpb.ZLabelsFromPromLabels
// (which is a zero-copy unsafe reinterpret that crashes on stringlabels). Go
// strings are immutable, so assigning lbl.Name/lbl.Value yields independent
// string headers pointing at validly-sized backing arrays.
func zLabelsCopy(lset labels.Labels) []labelpb.ZLabel {
	out := make([]labelpb.ZLabel, 0, lset.Len())
	lset.Range(func(l labels.Label) {
		out = append(out, labelpb.ZLabel{Name: l.Name, Value: l.Value})
	})
	return out
}

// aggrChunkFromTSDB converts an iterable tsdb/prometheus chunk to a
// storepb.AggrChunk over [minTime,maxTime], COPYING the chunk bytes (a querier
// may recycle/mmap-back them). It is shared by the tsdb-series and cold-series
// streaming paths so both encode chunks identically.
func aggrChunkFromTSDB(chunk chunkenc.Chunk, minTime, maxTime int64) storepb.AggrChunk {
	src := chunk.Bytes()
	data := make([]byte, len(src))
	copy(data, src)
	return storepb.AggrChunk{
		MinTime: minTime,
		MaxTime: maxTime,
		Raw: &storepb.Chunk{
			// storepb chunk encoding is one less than the tsdb one
			// (tsdb EncXOR=1 -> storepb Chunk_XOR=0).
			Type: storepb.Chunk_Encoding(chunk.Encoding() - 1),
			Data: data,
		},
	}
}

// completeLabels appends the merger's external labels to a series' own labels,
// external labels winning on conflict, returning a freshly-built (safely
// allocated) sorted labels.Labels. labelpb.ExtendSortedLabels uses a
// labels.Builder internally, so the result is a normal allocation (no aliasing
// of the querier's transient backing memory) and is safe to outlive the
// querier.
func completeLabels(seriesLset, extLset labels.Labels) labels.Labels {
	return labelpb.ExtendSortedLabels(seriesLset, extLset)
}

// promMatchers converts storepb matchers to prom matchers and applies the
// external-label gate: if a matcher targets an external label it must match the
// merger's value (else this store has nothing to contribute and we return
// match=false); matchers that don't touch external labels are passed to Select.
func (s *customStore) promMatchers(ms []storepb.LabelMatcher) (match bool, matchers []*labels.Matcher, err error) {
	tms, err := storepb.MatchersToPromMatchers(ms...)
	if err != nil {
		return false, nil, err
	}
	if s.extLset.IsEmpty() {
		return true, tms, nil
	}
	var kept []*labels.Matcher
	for _, tm := range tms {
		extVal := s.extLset.Get(tm.Name)
		if extVal == "" {
			kept = append(kept, tm)
			continue
		}
		if !tm.Matches(extVal) {
			// The external label exists but the matcher excludes our value;
			// this store has no matching series.
			return false, nil, nil
		}
		// Matcher targets an external label and matches our value: it is
		// satisfied by the appended external label, so drop it from the
		// querier matchers (the stored series do not carry external labels).
	}
	return true, kept, nil
}

// Series streams matching series (labels + raw XOR chunks) for the requested
// range. Series are emitted already sorted by label set (Querier sorted=true
// plus a consistent external-label append preserves order), so NO resorting
// wrapper is used.
func (s *customStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	match, matchers, err := s.promMatchers(r.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return nil
	}
	if len(matchers) == 0 {
		return status.Error(codes.InvalidArgument, "no matchers specified (excluding external labels)")
	}

	ctx := srv.Context()

	// Drop any external labels the caller asked to strip (replica dedup).
	extToRemove := map[string]struct{}{}
	for _, l := range r.WithoutReplicaLabels {
		extToRemove[l] = struct{}{}
	}
	finalExt := rmExtLabels(s.extLset, extToRemove)

	q, err := s.db.ChunkQuerier(r.MinTime, r.MaxTime)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer func() {
		if cerr := q.Close(); cerr != nil {
			level.Warn(s.logger).Log("msg", "close chunk querier", "err", cerr)
		}
	}()

	hints := &storage.SelectHints{
		Start:           r.MinTime,
		End:             r.MaxTime,
		Limit:           int(r.Limit),
		DisableTrimming: true,
	}
	set := q.Select(ctx, true /*sorted*/, hints, matchers...)

	for set.Next() {
		series := set.At()

		full := completeLabels(series.Labels(), finalExt)
		// SAFE label copy — never ZLabelsFromPromLabels.
		zls := zLabelsCopy(full)

		if r.SkipChunks {
			if err := srv.Send(storepb.NewSeriesResponse(&storepb.Series{Labels: zls})); err != nil {
				return status.Error(codes.Aborted, err.Error())
			}
			continue
		}

		var chks []storepb.AggrChunk
		chIt := series.Iterator(nil)
		for chIt.Next() {
			meta := chIt.At()
			if meta.Chunk == nil {
				return status.Errorf(codes.Internal, "customStore: unpopulated chunk at ref %v", meta.Ref)
			}
			chks = append(chks, aggrChunkFromTSDB(meta.Chunk, meta.MinTime, meta.MaxTime))
		}
		if err := chIt.Err(); err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		if err := srv.Send(storepb.NewSeriesResponse(&storepb.Series{Labels: zls, Chunks: chks})); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
	}
	if err := set.Err(); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Union the decode-on-read cold-part path AFTER the open-window tsdb series.
	// Emitting both as independent series streams lets thanos-query/PromQL merge
	// the same logical series across tsdb + cold + store-gateway, exactly as it
	// already merges the open-window and store-gateway overlaps.
	if err := s.streamColdSeries(ctx, r, finalExt, matchers, srv); err != nil {
		return err
	}

	for _, w := range set.Warnings().AsErrors() {
		if err := srv.Send(storepb.NewWarnSeriesResponse(w)); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
	}
	return nil
}

// streamColdSeries queries the decode-on-read cold path (if attached) and sends
// each matched cold series as a storepb.Series, reusing the SAME safe label
// copy/extend path as the tsdb series. It is a no-op when no cold querier is
// attached. matchers are the querier matchers (external-label matchers already
// stripped by promMatchers).
func (s *customStore) streamColdSeries(
	ctx context.Context,
	r *storepb.SeriesRequest,
	finalExt labels.Labels,
	matchers []*labels.Matcher,
	srv storepb.Store_SeriesServer,
) error {
	if s.cold == nil {
		return nil
	}
	level.Debug(s.logger).Log("msg", "streamColdSeries: querying cold parts",
		"mint", r.MinTime, "maxt", r.MaxTime, "matchers", len(matchers), "skip_chunks", r.SkipChunks)
	coldSeries, err := s.cold.Series(ctx, matchers, r.MinTime, r.MaxTime)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	level.Debug(s.logger).Log("msg", "streamColdSeries: emitting cold series",
		"cold_series", len(coldSeries), "mint", r.MinTime, "maxt", r.MaxTime)
	for _, cs := range coldSeries {
		full := completeLabels(cs.Labels, finalExt)
		zls := zLabelsCopy(full)

		if r.SkipChunks {
			if err := srv.Send(storepb.NewSeriesResponse(&storepb.Series{Labels: zls})); err != nil {
				return status.Error(codes.Aborted, err.Error())
			}
			continue
		}

		chks := make([]storepb.AggrChunk, 0, len(cs.Chunks))
		for _, cc := range cs.Chunks {
			chks = append(chks, aggrChunkFromTSDB(cc.Chunk, cc.MinTime, cc.MaxTime))
		}
		if err := srv.Send(storepb.NewSeriesResponse(&storepb.Series{Labels: zls, Chunks: chks})); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
	}
	return nil
}

// LabelNames returns the union of stored label names (matching the request
// range/matchers) and the merger's external label names.
func (s *customStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	match, matchers, err := s.promMatchers(r.Matchers)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return &storepb.LabelNamesResponse{}, nil
	}

	q, err := s.db.Querier(r.Start, r.End)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer func() { _ = q.Close() }()

	hints := &storage.LabelHints{Limit: int(r.Limit)}
	res, _, err := q.LabelNames(ctx, hints, matchers...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	extToRemove := map[string]struct{}{}
	for _, l := range r.WithoutReplicaLabels {
		extToRemove[l] = struct{}{}
	}
	seen := map[string]struct{}{}
	for _, n := range res {
		seen[n] = struct{}{}
	}
	s.extLset.Range(func(l labels.Label) {
		if _, drop := extToRemove[l.Name]; drop {
			return
		}
		if _, dup := seen[l.Name]; dup {
			return
		}
		res = append(res, l.Name)
		seen[l.Name] = struct{}{}
	})
	sort.Strings(res)

	// Copy out: block label memory may be freed when the querier closes.
	out := make([]string, len(res))
	for i := range res {
		out[i] = cloneString(res[i])
	}
	return &storepb.LabelNamesResponse{Names: out}, nil
}

// LabelValues returns values for one label name across the request range,
// including the merger's value for an external label.
func (s *customStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	if r.Label == "" {
		return nil, status.Error(codes.InvalidArgument, "label name parameter cannot be empty")
	}
	for _, l := range r.WithoutReplicaLabels {
		if l == r.Label {
			return &storepb.LabelValuesResponse{}, nil
		}
	}

	match, matchers, err := s.promMatchers(r.Matchers)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return &storepb.LabelValuesResponse{}, nil
	}

	q, err := s.db.Querier(r.Start, r.End)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer func() { _ = q.Close() }()

	// External label: its only value is the merger's, gated on there being a
	// matching series when extra matchers are present.
	if extVal := s.extLset.Get(r.Label); extVal != "" {
		if len(matchers) == 0 {
			return &storepb.LabelValuesResponse{Values: []string{extVal}}, nil
		}
		hints := &storage.SelectHints{Start: r.Start, End: r.End, Func: "series", Limit: int(r.Limit)}
		ss := q.Select(ctx, false, hints, matchers...)
		if ss.Next() {
			return &storepb.LabelValuesResponse{Values: []string{extVal}}, nil
		}
		if serr := ss.Err(); serr != nil {
			return nil, status.Error(codes.Internal, serr.Error())
		}
		return &storepb.LabelValuesResponse{}, nil
	}

	hints := &storage.LabelHints{Limit: int(r.Limit)}
	res, _, err := q.LabelValues(ctx, r.Label, hints, matchers...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := make([]string, len(res))
	for i := range res {
		out[i] = cloneString(res[i])
	}
	return &storepb.LabelValuesResponse{Values: out}, nil
}

// rmExtLabels returns extLset with any names in remove dropped, freshly built.
func rmExtLabels(extLset labels.Labels, remove map[string]struct{}) labels.Labels {
	if len(remove) == 0 {
		return extLset
	}
	b := labels.NewBuilder(extLset)
	for n := range remove {
		b.Del(n)
	}
	return b.Labels()
}

// cloneString returns an independent copy of s (detaches from any larger
// backing array a block querier may have handed us).
func cloneString(s string) string {
	return string([]byte(s))
}
