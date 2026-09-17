package runner

import (
	"fmt"
	"sort"

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
