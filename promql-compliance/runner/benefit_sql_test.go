package runner

import (
	"strings"
	"testing"
)

// Every level-2 query must have an explicit ClickHouse SQL baseline.
func TestIssue754ClickHouseBaselinesCoverSharedSuite(t *testing.T) {
	suite, err := LoadSuiteFile("../suites/issue-754.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(suite.Queries) != 10 {
		t.Fatalf("queries = %d, want 10", len(suite.Queries))
	}
	for _, c := range suite.Queries {
		window, err := WindowMillis(c.Expr)
		if err != nil {
			t.Fatal(err)
		}
		sql, err := ClickHouseSQL(c.Name, 1_700_000_120_000, window)
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		if !strings.Contains(sql, "FORMAT JSON") || strings.Contains(sql, "promql_rate") || strings.Contains(sql, "quantileTDigest") {
			t.Fatalf("%s has an incomplete or approximate baseline", c.Name)
		}
		if strings.Contains(c.Expr, "rate(") && !strings.Contains(sql, "reset_correction") {
			t.Fatalf("%s omits counter reset correction", c.Name)
		}
	}
	if _, err := ClickHouseSQL("unknown", 1, 60_000); err == nil {
		t.Fatal("unknown query silently accepted")
	}
}
