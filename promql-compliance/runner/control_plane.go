package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// BuildPublicationRequest derives the controller workload from the same suite
// that will be evaluated. The finite fixture supplies the metric vocabulary;
// no second hand-written planning configuration is maintained.
func BuildPublicationRequest(dataset Dataset, suite Suite, now time.Time) (map[string]any, error) {
	plannerRevision, backendRevision, err := buildRevisions()
	if err != nil {
		return nil, err
	}
	metrics := make(map[string]struct{}, len(dataset.Series))
	for _, series := range dataset.Series {
		metrics[series.Metric] = struct{}{}
	}
	queries := make([]any, 0, len(suite.Queries))
	for index, query := range suite.Queries {
		metric, ok := metricInQuery(query.Expr, metrics)
		if !ok {
			return nil, fmt.Errorf("query %q does not reference a fixture metric", query.Name)
		}
		window := queryWindowSeconds(query.Expr)
		interval := uint32(60_000)
		if query.Range != nil {
			interval = uint32(query.Range.StepSeconds * 1000)
		}
		queries = append(queries, map[string]any{
			"query_id": fmt.Sprintf("%s-%d", suite.Name, index+1), "query_string": query.Expr, "metric": metric, "window_secs": window, "group_by": []string{}, "accuracy": map[string]any{"Epsilon": 0.01}, "evaluation_phase_ms": 0,
			"window_cost_model": map[string]any{"implementation_id": "promql-compliance", "cost": map[string]any{"model_version": "promql-compliance-v1", "workload_fingerprint": query.Name, "observed_at_unix_ms": now.UnixMilli(), "valid_for_ms": 600_000, "horizon_seconds": 300.0, "cpu_cost": 1.0, "weighted_cost": 1.0, "peak_memory_bytes": 4096, "network_bytes": 0, "storage_bytes": 2048, "source_scan_bytes": 0}},
			"lifecycle":         map[string]any{"evaluation_interval_ms": interval, "ingestion_rate_per_second": 100.0, "evidence_observed_at_unix_ms": now.UnixMilli(), "evidence_valid_for_ms": 600_000, "horizon_seconds": 300.0, "costs": map[string]any{"build": 10.0, "maintenance_per_update": 0.001, "read": 0.1, "retention_per_second": 0.001, "retirement": 1.0}},
		})
	}
	return map[string]any{"target": "backend_local_remote_write", "queries": queries, "collector_ids": []string{}, "capability_snapshot_id": "promql-compliance", "planner_revision": plannerRevision, "max_evidence_age_ms": 600_000, "plan_version": 1, "activation_unix_ms": now.UnixMilli(), "backend_compat": "asap-query-backend.v1", "apply_timeout_ms": 30_000, "backend_revision": backendRevision}, nil
}

// BuildPlanningSnapshot is the backend-local startup input. The data plane
// invokes the repository's pinned Planner and PhysicalPlanCompiler from this
// workload, so the fixture has one source of truth for planning and queries.
func BuildPlanningSnapshot(suite Suite, now time.Time) map[string]any {
	queries := make([]any, 0, len(suite.Queries))
	for _, query := range suite.Queries {
		interval := 60_000.0
		if query.Range != nil {
			interval = query.Range.StepSeconds * 1000
		}
		queries = append(queries, map[string]any{
			"query":          query.Expr,
			"demand":         map[string]any{"fixed_interval_at": map[string]any{"interval": interval, "evaluation_phase": 0}},
			"requirements":   map[string]any{"accuracy": map[string]any{"explicit": map[string]any{"EpsilonDelta": map[string]any{"epsilon": 0.01, "delta": 0.01}}}, "response_latency": "unspecified"},
			"predictability": map[string]any{"predictable": map[string]any{"known_at": nil}},
			"time_selection": map[string]any{"scope": "real_time", "lookback": nil, "as_of": nil},
		})
	}
	data := map[string]any{"arrival": "continuously_ingesting", "data_ingestion_interval": map[string]any{"value": 1000, "source": "declared", "observed_at_ms": nil, "valid_for_ms": nil}, "ingestion_volume": map[string]any{"value": nil, "source": "unknown", "observed_at_ms": nil, "valid_for_ms": nil}, "ingestion_rate": map[string]any{"value": 100.0, "source": "declared", "observed_at_ms": nil, "valid_for_ms": nil}, "input_cardinality": map[string]any{"value": nil, "source": "unknown", "observed_at_ms": nil, "valid_for_ms": nil}, "distribution": map[string]any{"value": nil, "source": "unknown", "observed_at_ms": nil, "valid_for_ms": nil}}
	return map[string]any{"snapshot_version": 2, "query_workload": map[string]any{"language": "promql", "query_batch": nil, "repeating_queries": queries}, "data_workload": data, "implementation": map[string]any{"lifecycle_costs": map[string]any{"build": 10.0, "maintenance_per_update": 0.001, "read": 0.1, "retention_per_second": 0.001, "retirement": 1.0}, "evidence_observed_at_unix_ms": now.UnixMilli(), "evidence_valid_for_ms": 600000, "horizon_seconds": 300.0, "window_cost_model": map[string]any{"implementation_id": "promql-compliance", "cost": map[string]any{"model_version": "promql-compliance-v1", "workload_fingerprint": suite.Name, "observed_at_unix_ms": now.UnixMilli(), "valid_for_ms": 600000, "horizon_seconds": 300.0, "cpu_cost": 1.0, "peak_memory_bytes": 4096, "network_bytes": 0, "storage_bytes": 2048, "source_scan_bytes": 0, "weighted_cost": 1.0}}, "scrape_interval_ms": 1000}, "environment": map[string]any{"target": "backend_local_remote_write", "collector_ids": []string{}, "capability_snapshot_id": "promql-compliance", "observed_at_unix_ms": now.UnixMilli(), "max_evidence_age_ms": 600000, "plan_version": 1, "activation_unix_ms": now.UnixMilli(), "expiry_unix_ms": nil, "backend_compat": "asap-query-backend.v1"}}
}

func buildRevisions() (string, string, error) {
	if planner, backend := os.Getenv("ASAP_PLANNER_REVISION"), os.Getenv("ASAPQUERY_BACKEND_REVISION"); planner != "" && backend != "" {
		return planner, backend, nil
	}
	root, err := repositoryRoot()
	if err != nil {
		return "", "", err
	}
	lock, err := os.ReadFile(filepath.Join(root, "Cargo.lock"))
	if err != nil {
		return "", "", err
	}
	match := regexp.MustCompile(`(?s)name = "asap-types".*?source = "git\+[^#]+#([0-9a-f]{40})"`).FindStringSubmatch(string(lock))
	if len(match) != 2 {
		return "", "", fmt.Errorf("find ASAPPlanner revision in Cargo.lock")
	}
	output, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", "", fmt.Errorf("read backend revision: %w", err)
	}
	return match[1], strings.TrimSpace(string(output)), nil
}

func repositoryRoot() (string, error) {
	directory, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "Cargo.lock")); err == nil {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", fmt.Errorf("find repository root from %q", directory)
		}
		directory = parent
	}
}

var metricToken = regexp.MustCompile(`[A-Za-z_:][A-Za-z0-9_:]*`)
var rangeToken = regexp.MustCompile(`\[([0-9]+)([smhd])\]`)

func metricInQuery(expr string, metrics map[string]struct{}) (string, bool) {
	for _, token := range metricToken.FindAllString(expr, -1) {
		if _, ok := metrics[token]; ok {
			return token, true
		}
	}
	return "", false
}
func queryWindowSeconds(expr string) uint64 {
	max := uint64(60)
	for _, match := range rangeToken.FindAllStringSubmatch(expr, -1) {
		var n uint64
		_, _ = fmt.Sscan(match[1], &n)
		factor := uint64(1)
		switch match[2] {
		case "m":
			factor = 60
		case "h":
			factor = 3600
		case "d":
			factor = 86400
		}
		if n*factor > max {
			max = n * factor
		}
	}
	return max
}

// PublishPhysicalPlan obtains controller manifests first, supplies unit quotes
// for the discovered components, then publishes the selected backend-local plan.
func PublishPhysicalPlan(ctx context.Context, controlPlaneURL string, request map[string]any) error {
	manifests, err := postJSON(ctx, controlPlaneURL+"/api/v1/physical-plan/cost-manifests", request)
	if err != nil {
		return fmt.Errorf("prepare controller quotes: %w", err)
	}
	var entries []struct {
		Components map[string]json.RawMessage `json:"components"`
	}
	if err := json.Unmarshal(manifests, &entries); err != nil {
		return fmt.Errorf("decode controller manifests: %w", err)
	}
	var rawManifests []map[string]any
	if err := json.Unmarshal(manifests, &rawManifests); err != nil {
		return fmt.Errorf("decode controller manifests: %w", err)
	}
	quotes := make([]any, 0, len(entries))
	for index, entry := range entries {
		costs := map[string]float64{}
		for component := range entry.Components {
			costs[component] = 1
		}
		quotes = append(quotes, map[string]any{"manifest": rawManifests[index], "executable": index == 0, "unit_costs": costs})
	}
	backendRevision, _ := request["backend_revision"].(string)
	delete(request, "backend_revision")
	request["workload_cost_evidence"] = map[string]any{"backend_revision": backendRevision, "planner_revision": request["planner_revision"], "data_snapshot_id": "promql-compliance", "model_version": "promql-compliance-unit-costs", "observed_at_unix_ms": time.Now().UnixMilli(), "valid_for_ms": 600_000, "quotes": quotes}
	if _, err := postJSON(ctx, controlPlaneURL+"/api/v1/physical-plan/compile-and-publish", request); err != nil {
		return fmt.Errorf("publish controller plan: %w", err)
	}
	return nil
}

func postJSON(ctx context.Context, endpoint string, value any) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	result, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if response.StatusCode/100 != 2 {
		return nil, fmt.Errorf("%s: %s", response.Status, strings.TrimSpace(string(result)))
	}
	return result, nil
}
