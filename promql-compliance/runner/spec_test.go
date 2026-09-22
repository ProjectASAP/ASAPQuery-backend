package runner

import "testing"

func TestCheckedInFixturesMeetStrictContracts(t *testing.T) {
	for _, path := range []string{
		"../datasets/single-rate.yaml", "../datasets/sparse-checkout.yaml",
		"../datasets/aggregations.yaml", "../datasets/aggregations-dense-cadence.yaml",
		"../datasets/issue-702-one-second.yaml", "../datasets/issue-754.yaml",
	} {
		if _, err := LoadDatasetFile(path); err != nil {
			t.Fatalf("LoadDatasetFile(%q): %v", path, err)
		}
	}
	for _, path := range []string{"../suites/temporal.yaml", "../suites/aggregations.yaml", "../suites/issue-702.yaml", "../suites/issue-702-one-second.yaml", "../suites/issue-754.yaml"} {
		if _, err := LoadSuiteFile(path); err != nil {
			t.Fatalf("LoadSuiteFile(%q): %v", path, err)
		}
	}
}

func TestEncodeRemoteWriteExpandsGeneratedSamples(t *testing.T) {
	dataset, err := LoadDataset([]byte(`name: generated
series:
  - metric: data
    labels: {host: a}
    generated_samples:
      start_offset_seconds: 1
      end_offset_seconds: 3
      step_seconds: 1
      multiplier: 2
      base: 1
      modulo: 3
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
	samples := decoded.Timeseries[0].Samples
	if got, want := len(samples), 3; got != want {
		t.Fatalf("samples = %d, want %d", got, want)
	}
	if got, want := samples[0].Value, 4.0; got != want {
		t.Fatalf("first value = %v, want %v", got, want)
	}
	if got, want := samples[2].Value, 2.0; got != want {
		t.Fatalf("last value = %v, want %v", got, want)
	}
}

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

// The shared 100 ms fixture must retain its exact millisecond cadence in Remote Write.
func TestIssue754DenseFixtureCadence(t *testing.T) {
	dataset, err := LoadDatasetFile("../datasets/issue-754.yaml")
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
	if len(decoded.Timeseries) != 4 {
		t.Fatalf("series = %d, want 4", len(decoded.Timeseries))
	}
	for _, series := range decoded.Timeseries {
		if len(series.Samples) != 1801 {
			t.Fatalf("samples = %d, want 1801", len(series.Samples))
		}
		for i := 1; i < len(series.Samples); i++ {
			if got := series.Samples[i].Timestamp - series.Samples[i-1].Timestamp; got != 100 {
				t.Fatalf("sample %d cadence = %d ms, want 100", i, got)
			}
		}
	}
}
