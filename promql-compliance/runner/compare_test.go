package runner

import (
	"encoding/json"
	"testing"
	"time"
)

func comparisonResponse(t *testing.T, resultType, result string) QueryResponse {
	t.Helper()
	return QueryResponse{Status: "success", Data: json.RawMessage(`{"resultType":"` + resultType + `","result":` + result + `}`)}
}

func TestCompareResponsesHonorsValueToleranceButNotLabels(t *testing.T) {
	left := comparisonResponse(t, "vector", `[{"metric":{"job":"api"},"value":[1,"100"]}]`)
	right := comparisonResponse(t, "vector", `[{"metric":{"job":"api"},"value":[1,"101"]}]`)
	relative := 0.02
	if err := CompareResponses(left, right, ComparisonPolicy{ValueTolerance: &Tolerance{Relative: &relative}}); err != nil {
		t.Fatalf("CompareResponses: %v", err)
	}
	differentLabels := comparisonResponse(t, "vector", `[{"metric":{"job":"worker"},"value":[1,"101"]}]`)
	if err := CompareResponses(left, differentLabels, ComparisonPolicy{ValueTolerance: &Tolerance{Relative: &relative}}); err == nil {
		t.Fatal("comparison accepted different labels")
	}
}

func TestCompareRangeAtInstantChecksRequestedGridPoint(t *testing.T) {
	rangeResponse := comparisonResponse(t, "matrix", `[{"metric":{"job":"api"},"values":[[1,"1"],[2,"2"]]}]`)
	instantResponse := comparisonResponse(t, "vector", `[{"metric":{"job":"api"},"value":[2,"2"]}]`)
	if err := CompareRangeAtInstant(rangeResponse, instantResponse, time.Unix(2, 0), ComparisonPolicy{}); err != nil {
		t.Fatalf("CompareRangeAtInstant: %v", err)
	}
}
