package runner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

// EncodeRemoteWrite produces the canonical Prometheus Remote Write v1 body.
// Callers encode once and reuse these bytes for both comparison targets.
func EncodeRemoteWrite(baseTimeMs int64, dataset Dataset) ([]byte, error) {
	request := &prompb.WriteRequest{Timeseries: make([]prompb.TimeSeries, 0, len(dataset.Series))}
	for _, series := range dataset.Series {
		labels := []prompb.Label{{Name: "__name__", Value: series.Metric}}
		for name, value := range series.Labels {
			labels = append(labels, prompb.Label{Name: name, Value: value})
		}
		sort.Slice(labels, func(left, right int) bool { return labels[left].Name < labels[right].Name })
		samples := make([]prompb.Sample, 0, len(series.Samples))
		for _, sample := range series.Samples {
			samples = append(samples, prompb.Sample{Value: sample.Value, Timestamp: baseTimeMs + int64(sample.OffsetSeconds*1000)})
		}
		request.Timeseries = append(request.Timeseries, prompb.TimeSeries{Labels: labels, Samples: samples})
	}
	encoded, err := proto.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal Remote Write: %w", err)
	}
	return snappy.Encode(nil, encoded), nil
}

func DecodeRemoteWrite(body []byte) (*prompb.WriteRequest, error) {
	decoded, err := snappy.Decode(nil, body)
	if err != nil {
		return nil, fmt.Errorf("snappy decode Remote Write: %w", err)
	}
	request := &prompb.WriteRequest{}
	if err := proto.Unmarshal(decoded, request); err != nil {
		return nil, fmt.Errorf("unmarshal Remote Write: %w", err)
	}
	return request, nil
}

// PushRemoteWrite sends an already encoded body unchanged to every target.
func PushRemoteWrite(ctx context.Context, body []byte, targets ...string) error {
	for _, target := range targets {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(target, "/")+"/api/v1/write", bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("build Remote Write request: %w", err)
		}
		request.Header.Set("Content-Type", "application/x-protobuf")
		request.Header.Set("Content-Encoding", "snappy")
		request.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return fmt.Errorf("push Remote Write to %s: %w", target, err)
		}
		if response.StatusCode/100 != 2 {
			message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			response.Body.Close()
			return fmt.Errorf("push Remote Write to %s: %s: %s", target, response.Status, message)
		}
		response.Body.Close()
	}
	return nil
}

// Drain closes finite backend input so comparisons never race precompute work.
func Drain(ctx context.Context, target string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(target, "/")+"/api/v1/precompute/drain", nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return fmt.Errorf("drain %s: %s", target, response.Status)
	}
	return nil
}
