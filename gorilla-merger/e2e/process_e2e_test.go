package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func unusedAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

func waitReady(t *testing.T, url string, stderr *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("gorilla-merger did not become ready: %s", stderr.String())
}

func makeFrame(t *testing.T, timestamp int64) ([]byte, []byte) {
	t.Helper()
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		t.Fatalf("XOR appender: %v", err)
	}
	for i, value := range []float64{10, 20, 30} {
		appender.Append(timestamp+int64(i*10), value)
	}
	raw := append([]byte(nil), chunk.Bytes()...)
	frame := gorilla.EncodeFragmentBatch([]gorilla.Fragment{{
		MetricName: "gorilla_process_e2e_total",
		Attributes: map[string]string{"service": "checkout"},
		MinTime:    timestamp,
		MaxTime:    timestamp + 20,
		Count:      3,
		Encoding:   "xor",
		Data:       raw,
		Source:     "process-e2e-agent",
	}})
	return frame, raw
}

func queryRawChunk(t *testing.T, address string, minTime, maxTime int64) []byte {
	t.Helper()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("create StoreAPI client: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := storepb.NewStoreClient(conn).Series(ctx, &storepb.SeriesRequest{
		MinTime: minTime,
		MaxTime: maxTime,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: labels.MetricName, Value: "gorilla_process_e2e_total"},
			{Type: storepb.LabelMatcher_EQ, Name: "service", Value: "checkout"},
		},
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	})
	if err != nil {
		return nil
	}
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return nil
		}
		series := response.GetSeries()
		if series == nil {
			continue
		}
		for _, chunk := range series.Chunks {
			if chunk.Raw != nil && len(chunk.Raw.Data) > 0 {
				return chunk.Raw.Data
			}
		}
	}
}

func TestProductionProcessIngestsPersistsAndServesXORFragment(t *testing.T) {
	binary := os.Getenv("GORILLA_MERGER_E2E_BIN")
	if binary == "" {
		t.Fatal("GORILLA_MERGER_E2E_BIN is required; use ../../scripts/e2e.sh gorilla-merger")
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatalf("resolve binary: %v", err)
	}
	httpAddress := unusedAddress(t)
	grpcAddress := unusedAddress(t)
	tsdbDir := t.TempDir()
	var stderr bytes.Buffer
	cmd := exec.Command(binary,
		"--http-address", httpAddress,
		"--grpc-address", grpcAddress,
		"--tsdb.path", tsdbDir,
		"--merge.window", "100ms",
		"--merge.reorder-grace", "0s",
		"--merge.flush-interval", "20ms",
		"--merge.compact-interval", "1h",
	)
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start production gorilla-merger: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	waitReady(t, fmt.Sprintf("http://%s/-/ready", httpAddress), &stderr)

	base := time.Now().Add(-2 * time.Second).UnixMilli()
	frame, wantRaw := makeFrame(t, base)
	response, err := http.Post(
		fmt.Sprintf("http://%s/ingest/gorilla", httpAddress),
		"application/octet-stream",
		bytes.NewReader(frame),
	)
	if err != nil {
		t.Fatalf("POST Gorilla fragment: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("ingest returned %s", response.Status)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		gotRaw := queryRawChunk(t, grpcAddress, base-1000, base+1000)
		if bytes.Equal(gotRaw, wantRaw) {
			// A successful ingest response is issued only after the fragment WAL
			// fsync. Once the window becomes a durable block the committed WAL is
			// intentionally removed, so assert the post-flush durable artifact.
			blockMetas := 0
			_ = filepath.Walk(tsdbDir, func(_ string, info os.FileInfo, err error) error {
				if err == nil && info != nil && info.Name() == "meta.json" {
					blockMetas++
				}
				return nil
			})
			if blockMetas == 0 {
				t.Fatal("fragment was served but no durable TSDB block was written")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("StoreAPI never returned the exact ingested XOR chunk; process log:\n%s", stderr.String())
}
