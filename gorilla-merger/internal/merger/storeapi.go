package merger

import (
	"fmt"
	"net"

	kitlog "github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/info"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
)

// StoreAPI serves the Thanos StoreAPI (gRPC) over the BlockStore (directly-built
// + compacted, un-shipped blocks). This is the recent/un-shipped query surface;
// thanos-query fans out to it alongside the store-gateway (which serves the S3
// blocks), then unions.
//
// It registers a CUSTOM storepb.StoreServer (customStore) rather than thanos's
// store.TSDBStore. store.TSDBStore.Series fatally OOMs under Prometheus's
// default `stringlabels` build because its label encoding (ZLabelsFromPromLabels
// + resortingServer/ReAllocZLabelsStrings) is a zero-copy unsafe reinterpret
// that assumes the []Label layout, not the packed-string one. See customstore.go.
type StoreAPI struct {
	srv      *grpc.Server
	store    *customStore
	listener net.Listener
	addr     string
}

// NewStoreAPI wraps the tsdb.DB in the custom StoreServer (component "receive")
// and prepares a gRPC server bound to addr. When coldStore is non-nil, the
// store also serves the decode-on-read cold-part path, unioned with the
// open-window tsdb series.
func NewStoreAPI(s *Storage, extLset labels.Labels, logger kitlog.Logger, addr string, coldStore *ColdPartStore) (*StoreAPI, error) {
	if logger == nil {
		logger = kitlog.NewNopLogger()
	}
	// The custom store requires the external label set to be sorted; labels.New
	// / labels.Builder already returns sorted labels. The query backend is the
	// BlockStore (directly-built + compacted blocks), which implements the same
	// chunkQueryable interface the embedded tsdb.DB used to.
	cs := newCustomStore(s.BlockStore(), extLset, logger)
	if coldStore != nil {
		cs.setColdQuerier(NewColdQuerier(coldStore))
	}

	grpcSrv := grpc.NewServer()
	storepb.RegisterStoreServer(grpcSrv, cs)

	// thanos-query (v0.41) discovers an endpoint via the Info service; a
	// Store-only server is reachable but undiscoverable ("neither info nor
	// store client found"), so register Info too — mirroring the sidecar.
	infoSrv := info.NewInfoServer(
		component.Receive.String(),
		info.WithLabelSetFunc(func() []labelpb.ZLabelSet { return cs.labelSet() }),
		info.WithStoreInfoFunc(func() (*infopb.StoreInfo, error) {
			mint, maxt := cs.timeRange()
			return &infopb.StoreInfo{
				MinTime:                      mint,
				MaxTime:                      maxt,
				SupportsSharding:             true,
				SupportsWithoutReplicaLabels: true,
				TsdbInfos:                    cs.tsdbInfos(),
			}, nil
		}),
	)
	info.RegisterInfoServer(infoSrv)(grpcSrv)

	return &StoreAPI{
		srv:   grpcSrv,
		store: cs,
		addr:  addr,
	}, nil
}

// Listen binds the configured address. Split from Serve so the caller can fail
// fast on a bad bind before launching the serve goroutine.
func (a *StoreAPI) Listen() error {
	lis, err := net.Listen("tcp", a.addr)
	if err != nil {
		return fmt.Errorf("storeapi: listen %q: %w", a.addr, err)
	}
	a.listener = lis
	return nil
}

// Serve blocks serving the StoreAPI on the bound listener.
func (a *StoreAPI) Serve() error {
	if a.listener == nil {
		if err := a.Listen(); err != nil {
			return err
		}
	}
	return a.srv.Serve(a.listener)
}

// Addr returns the actual bound address (useful when addr was ":0").
func (a *StoreAPI) Addr() string {
	if a.listener != nil {
		return a.listener.Addr().String()
	}
	return a.addr
}

// Stop gracefully stops the gRPC server. The underlying tsdb.DB is owned by
// Storage and closed by the caller, not here.
func (a *StoreAPI) Stop() {
	a.srv.GracefulStop()
}
