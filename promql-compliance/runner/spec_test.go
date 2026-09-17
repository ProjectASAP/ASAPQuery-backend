package runner

import "testing"

func TestLoadSuiteRequiresExplicitEvaluationAndCarriesTolerance(t *testing.T) {
	suite, err := LoadSuite([]byte(`name: temporal
comparison_defaults:
  value_tolerance:
    relative: 0.01
queries:
  - name: request-rate
    expr: rate(http_requests_total[5m])
    instant_offsets_seconds: [300, 600]
    range:
      start_offset_seconds: 300
      end_offset_seconds: 600
      step_seconds: 60
`))
	if err != nil {
		t.Fatalf("LoadSuite: %v", err)
	}
	if got, want := len(suite.Queries), 1; got != want {
		t.Fatalf("query count = %d, want %d", got, want)
	}
	if got := suite.Queries[0].EffectiveTolerance(suite.ComparisonDefaults); got.ValueTolerance == nil || got.ValueTolerance.Relative == nil || *got.ValueTolerance.Relative != 0.01 {
		t.Fatalf("effective tolerance = %#v, want inherited relative tolerance", got)
	}
}

func TestLoadSuiteRejectsImplicitEvaluation(t *testing.T) {
	_, err := LoadSuite([]byte(`name: incomplete
queries:
  - name: no-evaluation
    expr: up
`))
	if err == nil {
		t.Fatal("LoadSuite accepted a query without an instant time or range")
	}
}

func TestLoadSuiteRejectsRemovedExpectErrorField(t *testing.T) {
	_, err := LoadSuite([]byte(`name: invalid
queries:
  - name: request-rate
    expr: rate(http_requests_total[5m])
    instant_offsets_seconds: [300]
    expect_error: true
`))
	if err == nil {
		t.Fatal("LoadSuite accepted the removed expect_error field")
	}
}

func TestLoadDatasetRejectsDuplicateSeriesAndUnorderedSamples(t *testing.T) {
	_, err := LoadDataset([]byte(`name: invalid
series:
  - metric: requests_total
    labels: {host: a}
    samples:
      - {offset_seconds: 60, value: 1}
      - {offset_seconds: 0, value: 0}
  - metric: requests_total
    labels: {host: a}
    samples:
      - {offset_seconds: 0, value: 0}
`))
	if err == nil {
		t.Fatal("LoadDataset accepted unordered or duplicate series")
	}
}

func TestEncodeRemoteWriteRoundTripsFixtureSamples(t *testing.T) {
	dataset, err := LoadDataset([]byte(`name: request
series:
  - metric: requests_total
    labels: {host: a}
    samples:
      - {offset_seconds: 0, value: 1}
      - {offset_seconds: 60, value: 2}
`))
	if err != nil {
		t.Fatal(err)
	}
	body, err := EncodeRemoteWrite(1_700_000_000_000, dataset)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRemoteWrite(body)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(decoded.Timeseries), 1; got != want {
		t.Fatalf("series = %d, want %d", got, want)
	}
	if got, want := decoded.Timeseries[0].Samples[1].Timestamp, int64(1_700_000_060_000); got != want {
		t.Fatalf("timestamp = %d, want %d", got, want)
	}
}
