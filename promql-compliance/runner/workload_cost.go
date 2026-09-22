package runner

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"
)

// BuildBenefitSnapshot prices the actual replay population without provider
// quotes. Generated samples are expanded before deriving workload facts.
func BuildBenefitSnapshot(suite Suite, dataset Dataset, now time.Time) (map[string]any, error) {
	snapshot := BuildPlanningSnapshot(suite, now)
	if len(dataset.Series) == 0 {
		return nil, fmt.Errorf("empty cost population")
	}
	var samples int
	var rate float64
	cadence := 0.0
	for _, series := range dataset.Series {
		values := series.ExpandedSamples()
		if len(values) < 2 {
			return nil, fmt.Errorf("%s needs at least two samples to establish cadence", series.Metric)
		}
		step := values[1].OffsetSeconds - values[0].OffsetSeconds
		if step <= 0 || math.IsNaN(step) || math.IsInf(step, 0) {
			return nil, fmt.Errorf("invalid source cadence")
		}
		for i := 1; i < len(values); i++ {
			if math.Abs(values[i].OffsetSeconds-values[i-1].OffsetSeconds-step) > 1e-6 {
				return nil, fmt.Errorf("benefit cost fixture requires uniform cadence")
			}
		}
		if cadence != 0 && math.Abs(cadence-step) > 1e-6 {
			return nil, fmt.Errorf("mixed source cadences")
		}
		cadence = step
		rate += 1 / step
		samples += len(values)
	}
	evidence := func(value any) map[string]any {
		return map[string]any{"value": value, "source": "declared", "observed_at_ms": nil, "valid_for_ms": nil}
	}
	data := snapshot["data_workload"].(map[string]any)
	data["ingestion_rate"] = evidence(rate)
	data["input_cardinality"] = evidence(len(dataset.Series))
	data["ingestion_volume"] = evidence(samples)
	data["data_ingestion_interval"] = evidence(int64(math.Round(cadence * 1000)))
	// Physical history sizing currently has whole-second granularity. Keep
	// exact replay cadence above, and round this retention bound upward.
	snapshot["implementation"].(map[string]any)["scrape_interval_ms"] = int64(math.Ceil(cadence) * 1000)
	return snapshot, nil
}

// CopyAutomaticSnapshot ensures the deployment exercises backend costing, not
// the synthetic quote producer used by older differential fixtures.
func CopyAutomaticSnapshot(template, destination string) error {
	encoded, err := os.ReadFile(template)
	if err != nil {
		return err
	}
	var snapshot map[string]json.RawMessage
	if err = json.Unmarshal(encoded, &snapshot); err != nil {
		return err
	}
	if _, found := snapshot["workload_cost_evidence"]; found {
		return fmt.Errorf("automatic costing fixture must omit workload_cost_evidence")
	}
	return os.WriteFile(destination, encoded, 0o600)
}

type resourceCost struct {
	CPU     float64  `json:"cpu_seconds"`
	Memory  float64  `json:"memory_byte_seconds"`
	Network float64  `json:"network_bytes"`
	Source  string   `json:"source"`
	Records []string `json:"erp_record_ids"`
}

// ValidateAutomaticWorkloadCost rejects bypassed costing, partial totals,
// inconsistent weighting, and selection of a more expensive feasible candidate.
func ValidateAutomaticWorkloadCost(encoded []byte) error {
	var plan struct {
		Cost *struct {
			Model    string `json:"model_version"`
			Selected uint64 `json:"selected_plan_id"`
			Manifest struct {
				Plan       uint64                     `json:"plan_id"`
				Components map[string]json.RawMessage `json:"components"`
			} `json:"selected_manifest"`
			Components map[string]float64 `json:"component_costs"`
			Candidates []struct {
				Plan      *uint64  `json:"plan_id"`
				Status    string   `json:"status"`
				Total     *float64 `json:"total_cost"`
				Automatic *struct {
					Model      string                     `json:"model_version"`
					Weights    map[string]json.RawMessage `json:"weights"`
					Components map[string]resourceCost    `json:"components"`
				} `json:"automatic_cost"`
			} `json:"alternatives"`
		} `json:"cost_comparison"`
	}
	if err := json.Unmarshal(encoded, &plan); err != nil {
		return err
	}
	c := plan.Cost
	if c == nil || c.Model != "backend-workload-resources-v1" || len(c.Components) == 0 || len(c.Candidates) == 0 {
		return fmt.Errorf("missing backend automatic workload costing")
	}
	if c.Manifest.Plan != c.Selected || len(c.Components) != len(c.Manifest.Components) {
		return fmt.Errorf("incomplete selected cost manifest")
	}
	selected := 0
	best := math.Inf(1)
	chosen := 0.0
	valid := func(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) && x >= 0 }
	equal := func(a, b float64) bool { return math.Abs(a-b) <= 1e-10*math.Max(1, math.Max(math.Abs(a), math.Abs(b))) }
	for _, candidate := range c.Candidates {
		if candidate.Total == nil {
			if candidate.Status == "selected" {
				return fmt.Errorf("selected uncosted candidate")
			}
			continue
		}
		a := candidate.Automatic
		if a == nil || a.Model != c.Model || len(a.Components) == 0 || !valid(*candidate.Total) {
			return fmt.Errorf("candidate missing automatic resource breakdown")
		}
		weights := map[string]float64{}
		for _, dimension := range []string{"cpu_seconds", "memory_byte_seconds", "network_bytes"} {
			raw, ok := a.Weights[dimension]
			if !ok {
				return fmt.Errorf("missing resource weight %s", dimension)
			}
			var w float64
			if err := json.Unmarshal(raw, &w); err != nil || !valid(w) {
				return fmt.Errorf("invalid weight")
			}
			weights[dimension] = w
		}
		if weights["cpu_seconds"] != 1 || weights["memory_byte_seconds"] != 1e-9 || weights["network_bytes"] != 1e-8 {
			return fmt.Errorf("unexpected versioned model weights")
		}
		total := 0.0
		for id, r := range a.Components {
			if !valid(r.CPU) || !valid(r.Memory) || !valid(r.Network) {
				return fmt.Errorf("invalid resource component %s", id)
			}
			if (r.Source != "analytical" && r.Source != "erp+analytical") || (r.Source == "erp+analytical") != (len(r.Records) > 0) {
				return fmt.Errorf("invalid resource provenance %s", id)
			}
			weighted := r.CPU*weights["cpu_seconds"] + r.Memory*weights["memory_byte_seconds"] + r.Network*weights["network_bytes"]
			if !valid(weighted) {
				return fmt.Errorf("resource total overflow")
			}
			total += weighted
			if candidate.Status == "selected" {
				amount, ok := c.Components[id]
				_, covered := c.Manifest.Components[id]
				if !ok || !covered || !equal(amount, weighted) {
					return fmt.Errorf("selected component mismatch %s", id)
				}
			}
		}
		if !valid(total) || !equal(total, *candidate.Total) {
			return fmt.Errorf("candidate total does not match resources")
		}
		best = math.Min(best, total)
		if candidate.Status == "selected" {
			selected++
			chosen = total
			if candidate.Plan == nil || *candidate.Plan != c.Selected || len(a.Components) != len(c.Components) {
				return fmt.Errorf("selected identity/coverage mismatch")
			}
		}
	}
	if selected != 1 || !equal(chosen, best) {
		return fmt.Errorf("selection is not the lowest fully costed candidate")
	}
	return nil
}
