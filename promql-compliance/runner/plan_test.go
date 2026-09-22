package runner

import "testing"

// External exact work must fail level 2 even when the router itself answers locally.
func TestValidateLocalPlanRejectsExternalExact(t *testing.T) {
	for _, op := range []string{"exact_fallback", "external_exact"} {
		plan := `{"query_plan":{"entries":{"q":{"nodes":{"0":{"op":"` + op + `"}}}}}}`
		if err := ValidateLocalPlan([]byte(plan)); err == nil {
			t.Fatalf("accepted %s", op)
		}
	}
	plan := `{"query_plan":{"entries":{"q":{"nodes":{"0":{"op":"logical","operator":{"op":"exact_subquery"}}}}}}}`
	if err := ValidateLocalPlan([]byte(plan)); err == nil {
		t.Fatal("accepted exact subquery")
	}
	warm := `{"query_plan":{"entries":{"q":{"nodes":{"0":{"op":"read_materialization"}}}}}}`
	if err := ValidateLocalPlan([]byte(warm)); err != nil {
		t.Fatal(err)
	}
}
