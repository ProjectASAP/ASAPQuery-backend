package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The benefit fixture's declared demand must describe its replay data, not the
// unrelated 100 samples/s compatibility default.
func TestBenefitSnapshotUsesReplayPopulation(t *testing.T) {
	suite, err := LoadSuiteFile("../suites/issue-754.yaml")
	if err != nil {
		t.Fatal(err)
	}
	dataset, err := LoadDatasetFile("../datasets/issue-754.yaml")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := BuildBenefitSnapshot(suite, dataset, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	data := snapshot["data_workload"].(map[string]any)
	if data["input_cardinality"].(map[string]any)["value"] != len(dataset.Series) {
		t.Fatal("wrong population")
	}
	if data["data_ingestion_interval"].(map[string]any)["value"] != int64(100) {
		t.Fatal("expected 100ms replay cadence")
	}
	if data["ingestion_rate"].(map[string]any)["value"] != float64(len(dataset.Series))*10 {
		t.Fatal("wrong sample rate")
	}
	if _, found := snapshot["workload_cost_evidence"]; found {
		t.Fatal("benchmark must not manufacture quotes")
	}
	dataset.Series[0].Samples = dataset.Series[0].ExpandedSamples()
	dataset.Series[0].GeneratedSamples = nil
	dataset.Series[0].Samples[1].OffsetSeconds += 0.01
	if _, err := BuildBenefitSnapshot(suite, dataset, time.Unix(1, 0)); err == nil {
		t.Fatal("ambiguous cadence accepted")
	}
}

// Supplying even an empty provider override must not bypass the automatic path.
func TestAutomaticSnapshotRejectsQuoteOverride(t *testing.T) {
	directory := t.TempDir()
	src := filepath.Join(directory, "input.json")
	dst := filepath.Join(directory, "output.json")
	for _, test := range []struct {
		contents string
		reject   bool
	}{{`{"snapshot_version":2}`, false}, {`{"snapshot_version":2,"workload_cost_evidence":{}}`, true}} {
		contents := test.contents
		if err := os.WriteFile(src, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		err := CopyAutomaticSnapshot(src, dst)
		if (err != nil) != test.reject {
			t.Fatalf("unexpected copy result %v", err)
		}
	}
}

// The gate catches missing coverage, arithmetic drift, fake provenance and a
// winner that is more expensive than another fully costed candidate.
func TestAutomaticCostGate(t *testing.T) {
	valid := []byte(`{"cost_comparison":{"model_version":"backend-workload-resources-v1","selected_plan_id":7,"selected_manifest":{"plan_id":7,"components":{"state:x:residency":{}}},"component_costs":{"state:x:residency":1},"alternatives":[{"plan_id":7,"status":"selected","total_cost":1,"automatic_cost":{"model_version":"backend-workload-resources-v1","weights":{"cpu_seconds":1,"memory_byte_seconds":1e-9,"network_bytes":1e-8},"components":{"state:x:residency":{"cpu_seconds":0,"memory_byte_seconds":1000000000,"network_bytes":0,"source":"analytical","erp_record_ids":[]}}}}]}}`)
	if err := ValidateAutomaticWorkloadCost(valid); err != nil {
		t.Fatal(err)
	}
	mutations := []func(map[string]any){
		func(c map[string]any) { delete(c, "component_costs") },
		func(c map[string]any) { c["component_costs"].(map[string]any)["state:x:residency"] = 2 },
		func(c map[string]any) { c["selected_plan_id"] = 8 },
		func(c map[string]any) { c["alternatives"].([]any)[0].(map[string]any)["total_cost"] = 2 },
		func(c map[string]any) { delete(c["alternatives"].([]any)[0].(map[string]any), "automatic_cost") },
		func(c map[string]any) {
			a := c["alternatives"].([]any)[0].(map[string]any)["automatic_cost"].(map[string]any)
			a["components"].(map[string]any)["state:x:residency"].(map[string]any)["source"] = "erp+analytical"
		},
		func(c map[string]any) {
			encoded, _ := json.Marshal(c["alternatives"].([]any)[0])
			var other map[string]any
			_ = json.Unmarshal(encoded, &other)
			other["status"] = "unselected"
			other["plan_id"] = 8
			other["total_cost"] = 0
			other["automatic_cost"].(map[string]any)["components"].(map[string]any)["state:x:residency"].(map[string]any)["memory_byte_seconds"] = 0
			c["alternatives"] = append(c["alternatives"].([]any), other)
		},
	}
	for i, mutate := range mutations {
		var plan map[string]any
		_ = json.Unmarshal(valid, &plan)
		mutate(plan["cost_comparison"].(map[string]any))
		encoded, _ := json.Marshal(plan)
		if err := ValidateAutomaticWorkloadCost(encoded); err == nil {
			t.Fatalf("mutation %d accepted", i)
		}
	}
}

// PR 761 uses the typed residual `kind` tag. Both exact subtree variants must
// still block local-only benefit claims.
func TestLocalPlanRejectsTypedExactSubqueries(t *testing.T) {
	for _, kind := range []string{"exact_subquery", "candidate_exact_subquery"} {
		encoded := []byte(`{"query_plan":{"entries":{"q":{"nodes":{"0":{"op":"logical","operator":{"kind":"` + kind + `"}}}}}}}`)
		if err := ValidateLocalPlan(encoded); err == nil {
			t.Fatalf("accepted %s", kind)
		}
	}
}
