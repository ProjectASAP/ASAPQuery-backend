package merger

// vmload_test.go — drives a REAL running VictoriaMetrics single-node via the
// Prometheus remote_write path (snappy + protobuf), the canonical high-throughput
// VM ingestion path, with the SAME corpus the merger/Prometheus benches use.
// Gated by VM_ADDR so it only runs when a VM is up.
//
//   # per-core VM: start VM with GOMAXPROCS=1
//   GOMAXPROCS=1 ./victoria-metrics-prod -storageDataPath=/dev/shm/vmdata -httpListenAddr=:8428 &
//   VM_ADDR=http://localhost:8428 VM_WINDOWS=40 GOPRIVATE='github.com/ProjectASAP/*' \
//     go test ./internal/merger/ -run TestVMRemoteWriteThroughput -v
//
// All request bodies are snappy-compressed BEFORE timing, so the timed loop is
// HTTP + server-side ingest only (no client marshal cost).

import (
	"bytes"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

func TestVMRemoteWriteThroughput(t *testing.T) {
	addr := os.Getenv("VM_ADDR")
	if addr == "" {
		t.Skip("set VM_ADDR=http://host:8428 to run the VM ingestion comparison")
	}
	windows := 40
	if v := os.Getenv("VM_WINDOWS"); v != "" {
		windows, _ = strconv.Atoi(v)
	}
	// Same workload as the merger/Prometheus benches: 1000 series x 120 samples.
	corpus := buildBenchCorpus(benchNumSeries, 1, benchPerChunk)
	series := decodeCorpus(t, corpus)

	// Pre-build snappy-compressed remote_write bodies, one per window. Timestamps
	// are stamped at ~NOW (not the corpus's 2023 base) so they fall INSIDE VM's
	// retention — otherwise VM parses then silently drops them as too old. Each
	// window is shifted forward 1s/sample so VM never sees out-of-order samples.
	bodies := make([][]byte, windows)
	samplesPerWindow := corpus.numSamples
	windowSpanMs := int64(benchPerChunk) * 1000 // 120 samples * 1s
	nowBase := time.Now().UnixMilli() - int64(windows)*windowSpanMs - 1000
	for w := 0; w < windows; w++ {
		var wr prompb.WriteRequest
		wr.Timeseries = make([]prompb.TimeSeries, len(series))
		for si := range series {
			s := &series[si]
			lbls := make([]prompb.Label, 0, s.ls.Len())
			s.ls.Range(func(l labels.Label) {
				lbls = append(lbls, prompb.Label{Name: l.Name, Value: l.Value})
			})
			smp := make([]prompb.Sample, len(s.ts))
			for k := range s.ts {
				smp[k] = prompb.Sample{Value: s.val[k], Timestamp: nowBase + int64(w)*windowSpanMs + int64(k)*1000}
			}
			wr.Timeseries[si] = prompb.TimeSeries{Labels: lbls, Samples: smp}
		}
		raw, err := wr.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		bodies[w] = snappy.Encode(nil, raw)
	}

	url := addr + "/api/v1/write"
	client := &http.Client{}
	post := func(body []byte) {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		req.Header.Set("Content-Encoding", "snappy")
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			t.Fatalf("VM write status %d", resp.StatusCode)
		}
	}

	// Warm one window (server-side series registration), not timed.
	post(bodies[0])

	start := time.Now()
	total := 0
	for w := 1; w < windows; w++ {
		post(bodies[w])
		total += samplesPerWindow
	}
	elapsed := time.Since(start)
	rate := float64(total) / elapsed.Seconds()
	t.Logf("VM remote_write: %d samples in %s => %.0f samples/sec (%.2f M/s), %.1f ns/sample",
		total, elapsed.Truncate(time.Millisecond), rate, rate/1e6, float64(elapsed.Nanoseconds())/float64(total))
}
