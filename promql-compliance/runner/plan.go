package runner

import (
	"encoding/json"
	"fmt"
)

// ValidateLocalPlan rejects a selected plan whose answers require an exact
// external backend. A local HTTP provenance header alone cannot detect this.
func ValidateLocalPlan(encoded []byte) error {
	var plan struct {
		QueryPlan struct {
			Entries map[string]struct {
				Nodes map[string]struct {
					Op       string `json:"op"`
					Operator struct {
						Op string `json:"op"`
					} `json:"operator"`
				} `json:"nodes"`
			} `json:"entries"`
		} `json:"query_plan"`
	}
	if err := json.Unmarshal(encoded, &plan); err != nil {
		return err
	}
	if len(plan.QueryPlan.Entries) == 0 {
		return fmt.Errorf("selected plan has no query entries")
	}
	for query, entry := range plan.QueryPlan.Entries {
		if len(entry.Nodes) == 0 {
			return fmt.Errorf("%s has no query nodes", query)
		}
		for _, node := range entry.Nodes {
			if node.Op == "exact_fallback" || node.Op == "external_exact" || node.Operator.Op == "exact_subquery" {
				return fmt.Errorf("%s requires external exact execution (%s/%s)", query, node.Op, node.Operator.Op)
			}
		}
	}
	return nil
}
