package runner

import (
	"testing"
	"time"
)

func TestBuildPublicationRequestUsesSuiteQueriesAndFixtureMetric(t *testing.T) {
	dataset := Dataset{Name: "fixture", Series: []DatasetSeries{{Metric: "requests_total", Samples: []DatasetSample{{OffsetSeconds: 0, Value: 1}}}}}
	suite := Suite{Name: "suite", Queries: []QueryCase{{Name: "rate", Expr: "rate(requests_total[5m])", InstantOffsetsSeconds: []float64{300}}}}
	request, err := BuildPublicationRequest(dataset, suite, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	queries := request["queries"].([]any)
	query := queries[0].(map[string]any)
	if query["metric"] != "requests_total" || query["window_secs"] != uint64(300) {
		t.Fatalf("query = %#v", query)
	}
	if request["target"] != "backend_local_remote_write" || len(request["collector_ids"].([]string)) != 0 {
		t.Fatalf("request = %#v", request)
	}
}
