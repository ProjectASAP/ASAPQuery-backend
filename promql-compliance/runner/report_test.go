package runner

import "testing"

func TestCompareOutcomeRejectsPrometheusFallback(t *testing.T) {
	response := comparisonResponse(t, "vector", `[]`)
	response.ServedBy = "prometheus_fallback"
	outcome := compareOutcome(comparisonResponse(t, "vector", `[]`), response, nil, nil, ComparisonPolicy{})
	if outcome.Passed || outcome.Diff == "" {
		t.Fatalf("fallback must not count as an ASAPQuery comparison: %+v", outcome)
	}
}
