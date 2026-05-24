package merger

import (
	"fmt"
	"net"

	kitlog "github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/info"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
)

// StoreAPI serves the Thanos StoreAPI (gRPC) over the embedded tsdb.DB. This is
// the open-window (<2h pending) query surface; thanos-query fans out to it
// alongside the store-gateway (which serves the >=2h S3 blocks), then unions.
type StoreAPI struct {
	srv      *grpc.Server
	tsdbStr  *store.TSDBStore
	listener net.Listener
	addr     string
}

// NewStoreAPI wraps the tsdb.DB in a Thanos store.TSDBStore (component
// "receive") and prepares a gRPC server bound to addr.
func NewStoreAPI(s *Storage, extLset labels.Labels, logger kitlog.Logger, addr string) (*StoreAPI, error) {
	if logger == nil {
		logger = kitlog.NewNopLogger()
	}
	// store.TSDBStore requires the external label set to be sorted; labels.New
	// / labels.Builder already returns sorted labels.
	tsdbStore := store.NewTSDBStore(logger, s.DB, component.Receive, extLset)

	grpcSrv := grpc.NewServer()
	storepb.RegisterStoreServer(grpcSrv, tsdbStore)

	// thanos-query (v0.41) discovers an endpoint via the Info service; a
	// Store-only server is reachable but undiscoverable ("neither info nor
	// store client found"), so register Info too — mirroring the sidecar.
	infoSrv := info.NewInfoServer(
		component.Receive.String(),
		info.WithLabelSetFunc(func() []labelpb.ZLabelSet { return tsdbStore.LabelSet() }),
		info.WithStoreInfoFunc(func() (*infopb.StoreInfo, error) {
			mint, maxt := tsdbStore.TimeRange()
			return &infopb.StoreInfo{
				MinTime:                      mint,
				MaxTime:                      maxt,
				SupportsSharding:             true,
				SupportsWithoutReplicaLabels: true,
				TsdbInfos:                    tsdbStore.TSDBInfos(),
			}, nil
		}),
	)
	info.RegisterInfoServer(infoSrv)(grpcSrv)

	return &StoreAPI{
		srv:     grpcSrv,
		tsdbStr: tsdbStore,
		addr:    addr,
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

// Stop gracefully stops the gRPC server and closes the TSDBStore.
func (a *StoreAPI) Stop() {
	a.srv.GracefulStop()
	if a.tsdbStr != nil {
		a.tsdbStr.Close()
	}
}
