package runner

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CompareResponses compares successful Prometheus API responses by result
// semantics. Labels and timestamps are exact; only finite sample values use
// the configured tolerance.
func CompareResponses(reference, test QueryResponse, policy ComparisonPolicy) error {
	if reference.Status != "success" || test.Status != "success" {
		return fmt.Errorf("query status differs or failed: reference=%q/%q test=%q/%q", reference.Status, reference.Error, test.Status, test.Error)
	}
	left, err := normalizeResponse(reference)
	if err != nil {
		return fmt.Errorf("normalize reference response: %w", err)
	}
	right, err := normalizeResponse(test)
	if err != nil {
		return fmt.Errorf("normalize test response: %w", err)
	}
	return compareNormalized(left, right, policy)
}

type normalizedResult struct {
	Type    string
	Samples []normalizedSample
	Scalar  *float64
	Text    *string
}
type normalizedSample struct {
	Labels    string
	Timestamp int64
	Value     float64
}
type apiResult struct {
	ResultType string          `json:"resultType"`
	Result     json.RawMessage `json:"result"`
}
type apiSeries struct {
	Metric map[string]string `json:"metric"`
	Value  json.RawMessage   `json:"value"`
	Values []json.RawMessage `json:"values"`
}

func normalizeResponse(response QueryResponse) (normalizedResult, error) {
	var data apiResult
	if err := json.Unmarshal(response.Data, &data); err != nil {
		return normalizedResult{}, err
	}
	switch data.ResultType {
	case "vector", "matrix":
		var series []apiSeries
		if err := json.Unmarshal(data.Result, &series); err != nil {
			return normalizedResult{}, err
		}
		out := normalizedResult{Type: data.ResultType}
		for _, item := range series {
			values := item.Values
			if data.ResultType == "vector" {
				values = []json.RawMessage{item.Value}
			}
			for _, value := range values {
				ts, number, err := parsePoint(value)
				if err != nil {
					return normalizedResult{}, err
				}
				out.Samples = append(out.Samples, normalizedSample{Labels: canonicalResponseLabels(item.Metric), Timestamp: ts, Value: number})
			}
		}
		sort.Slice(out.Samples, func(i, j int) bool {
			if out.Samples[i].Labels != out.Samples[j].Labels {
				return out.Samples[i].Labels < out.Samples[j].Labels
			}
			return out.Samples[i].Timestamp < out.Samples[j].Timestamp
		})
		return out, nil
	case "scalar":
		_, value, err := parsePoint(data.Result)
		if err != nil {
			return normalizedResult{}, err
		}
		return normalizedResult{Type: data.ResultType, Scalar: &value}, nil
	case "string":
		var point []json.RawMessage
		if err := json.Unmarshal(data.Result, &point); err != nil {
			return normalizedResult{}, err
		}
		if len(point) != 2 {
			return normalizedResult{}, fmt.Errorf("string result has %d fields", len(point))
		}
		var text string
		if err := json.Unmarshal(point[1], &text); err != nil {
			return normalizedResult{}, err
		}
		return normalizedResult{Type: data.ResultType, Text: &text}, nil
	default:
		return normalizedResult{}, fmt.Errorf("unsupported result type %q", data.ResultType)
	}
}

func parsePoint(raw json.RawMessage) (int64, float64, error) {
	var point []json.RawMessage
	if err := json.Unmarshal(raw, &point); err != nil {
		return 0, 0, err
	}
	if len(point) != 2 {
		return 0, 0, fmt.Errorf("sample has %d fields", len(point))
	}
	var timestamp float64
	if err := json.Unmarshal(point[0], &timestamp); err != nil {
		return 0, 0, err
	}
	var text string
	if err := json.Unmarshal(point[1], &text); err != nil {
		return 0, 0, err
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, 0, err
	}
	return int64(math.Round(timestamp * 1000)), value, nil
}

func canonicalResponseLabels(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&b, "%s=%q,", key, labels[key])
	}
	return b.String()
}

func compareNormalized(left, right normalizedResult, policy ComparisonPolicy) error {
	if left.Type != right.Type {
		return fmt.Errorf("result type differs: reference=%s test=%s", left.Type, right.Type)
	}
	if left.Scalar != nil || right.Scalar != nil {
		if left.Scalar == nil || right.Scalar == nil || !equalFloat(*left.Scalar, *right.Scalar, policy.ValueTolerance) {
			return fmt.Errorf("scalar differs: reference=%v test=%v", left.Scalar, right.Scalar)
		}
		return nil
	}
	if left.Text != nil || right.Text != nil {
		if left.Text == nil || right.Text == nil || *left.Text != *right.Text {
			return fmt.Errorf("string differs: reference=%v test=%v", left.Text, right.Text)
		}
		return nil
	}
	if len(left.Samples) != len(right.Samples) {
		return fmt.Errorf("sample count differs: reference=%d test=%d", len(left.Samples), len(right.Samples))
	}
	for i := range left.Samples {
		a, b := left.Samples[i], right.Samples[i]
		if a.Labels != b.Labels || a.Timestamp != b.Timestamp {
			return fmt.Errorf("sample %d labels or timestamp differs: reference=%+v test=%+v", i, a, b)
		}
		if !equalFloat(a.Value, b.Value, policy.ValueTolerance) {
			return fmt.Errorf("sample %d value differs: reference=%v test=%v", i, a.Value, b.Value)
		}
	}
	return nil
}

func equalFloat(left, right float64, tolerance *Tolerance) bool {
	if math.IsNaN(left) || math.IsNaN(right) {
		return math.IsNaN(left) && math.IsNaN(right)
	}
	if math.IsInf(left, 0) || math.IsInf(right, 0) {
		return left == right
	}
	relative, absolute := 0.0, 0.0
	if tolerance != nil {
		if tolerance.Relative != nil {
			relative = *tolerance.Relative
		}
		if tolerance.Absolute != nil {
			absolute = *tolerance.Absolute
		}
	}
	return math.Abs(left-right) <= absolute+relative*math.Max(math.Abs(left), math.Abs(right))
}

// CompareRangeAtInstant checks that range-at-t and instant-at-t agree.
func CompareRangeAtInstant(rangeResponse, instantResponse QueryResponse, at time.Time, policy ComparisonPolicy) error {
	rangeValue, err := normalizeResponse(rangeResponse)
	if err != nil {
		return err
	}
	instantValue, err := normalizeResponse(instantResponse)
	if err != nil {
		return err
	}
	if rangeValue.Type != "matrix" {
		return fmt.Errorf("range response type is %q, want matrix", rangeValue.Type)
	}
	rangeValue.Type = "vector"
	wanted := at.UnixMilli()
	filtered := rangeValue.Samples[:0]
	for _, sample := range rangeValue.Samples {
		if sample.Timestamp == wanted {
			filtered = append(filtered, sample)
		}
	}
	rangeValue.Samples = filtered
	if instantValue.Type == "vector" {
		for i := range instantValue.Samples {
			instantValue.Samples[i].Timestamp = wanted
		}
	}
	return compareNormalized(rangeValue, instantValue, policy)
}
