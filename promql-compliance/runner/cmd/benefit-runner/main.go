package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ProjectASAP/ASAPQuery-backend/promql-compliance/runner"
)

type fileList []string

func (f *fileList) String() string     { return fmt.Sprint([]string(*f)) }
func (f *fileList) Set(s string) error { *f = append(*f, s); return nil }

type latency struct {
	P50Ms     float64   `json:"p50Ms"`
	P95Ms     float64   `json:"p95Ms"`
	SamplesMs []float64 `json:"samplesMs"`
}
type targetReport struct {
	Queries         map[string]latency `json:"queries"`
	CPUUsec         uint64             `json:"cpuUsec"`
	MemoryPeakBytes uint64             `json:"memoryPeakBytes"`
}
type report struct {
	Suite         string                  `json:"suite"`
	Dataset       string                  `json:"dataset"`
	BaseTimeMs    int64                   `json:"baseTimeMs"`
	Warmups       int                     `json:"warmups"`
	Trials        int                     `json:"trials"`
	Targets       map[string]targetReport `json:"targets"`
	BenefitPassed bool                    `json:"benefitPassed"`
	Failures      []string                `json:"failures,omitempty"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var files fileList
	datasetPath := flag.String("dataset", "", "shared dataset YAML")
	suitePath := flag.String("suite", "", "shared suite YAML")
	promURL := flag.String("prometheus-url", "http://127.0.0.1:19090", "Prometheus URL")
	backendURL := flag.String("backend-url", "http://127.0.0.1:19091", "backend URL")
	vmURL := flag.String("victoria-url", "http://127.0.0.1:18428", "VictoriaMetrics URL")
	chURL := flag.String("clickhouse-url", "http://127.0.0.1:18123", "ClickHouse URL")
	output := flag.String("output", "benefit-report.json", "JSON report path")
	logs := flag.String("logs-dir", "", "retained Compose log directory")
	warmups := flag.Int("warmups", 3, "warmup requests per query and target")
	trials := flag.Int("trials", 10, "measured requests per query and target")
	baseMs := flag.Int64("base-time-ms", time.Now().Add(-30*time.Minute).UnixMilli(), "fixture base time")
	flag.Var(&files, "compose-file", "Compose file, repeated for overlays")
	flag.Parse()
	if *datasetPath == "" || *suitePath == "" || len(files) < 2 || *trials < 2 || *warmups < 0 {
		return fmt.Errorf("dataset, suite, base and benefit Compose files, and positive trials are required")
	}
	dataset, err := runner.LoadDatasetFile(*datasetPath)
	if err != nil {
		return err
	}
	suite, err := runner.LoadSuiteFile(*suitePath)
	if err != nil {
		return err
	}
	if len(suite.Queries) != 10 {
		return fmt.Errorf("level-3 contract requires ten shared cases, got %d", len(suite.Queries))
	}
	body, err := runner.EncodeRemoteWrite(*baseMs, dataset)
	if err != nil {
		return err
	}
	ctx := context.Background()
	directory, err := os.MkdirTemp("", "issue754-benefit-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	snapshot, err := json.Marshal(runner.BuildPlanningSnapshot(suite, time.Now().UTC()))
	if err != nil {
		return err
	}
	template := filepath.Join(directory, "planning-snapshot-template.json")
	selected := filepath.Join(directory, "planning-snapshot.json")
	if err := os.WriteFile(template, snapshot, 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(selected, nil, 0o600); err != nil {
		return err
	}
	lifecycle := runner.ComposeLifecycle{
		Files: files, Project: "issue754-benefit", LogsDirectory: *logs,
		PlanningSnapshot: selected, PlanningSnapshotTemplate: template,
		AdditionalServices: []string{"clickhouse", "victoria"},
	}
	defer lifecycle.Stop()
	if err := lifecycle.Start(ctx); err != nil {
		return err
	}
	for _, endpoint := range []string{*promURL + "/api/v1/status/runtimeinfo", *backendURL + "/api/v1/health", *vmURL + "/health", *chURL + "/ping"} {
		if err := runner.WaitForHTTP(ctx, endpoint); err != nil {
			return err
		}
	}
	if err := runner.PushRemoteWrite(ctx, body, *promURL, *backendURL, *vmURL); err != nil {
		return err
	}
	if err := runner.Drain(ctx, *backendURL); err != nil {
		return err
	}
	if err := seedClickHouse(ctx, *chURL, dataset, *baseMs); err != nil {
		return err
	}
	base := time.UnixMilli(*baseMs)
	prom := runner.HTTPQueryTarget{BaseURL: *promURL}
	backend := runner.HTTPQueryTarget{BaseURL: *backendURL, BackendTarget: true}
	vm := runner.HTTPQueryTarget{BaseURL: *vmURL}
	level2 := runner.CompareSuite(ctx, prom, backend, suite, dataset.Name, base)
	if !level2.Passed {
		return fmt.Errorf("level-2 semantic comparison failed; benchmark invalid")
	}
	if err := verifyBaselines(ctx, suite, base, prom, vm, *chURL); err != nil {
		return err
	}

	result := report{Suite: suite.Name, Dataset: dataset.Name, BaseTimeMs: *baseMs,
		Warmups: *warmups, Trials: *trials, Targets: map[string]targetReport{}, BenefitPassed: true}
	targets := []struct {
		name, service string
		query         func(context.Context, runner.QueryCase, time.Time) error
	}{
		{"backend", "data-plane", func(ctx context.Context, c runner.QueryCase, at time.Time) error {
			return queryPromQL(ctx, backend, c.Expr, at)
		}},
		{"prometheus", "prometheus", func(ctx context.Context, c runner.QueryCase, at time.Time) error {
			return queryPromQL(ctx, prom, c.Expr, at)
		}},
		{"victoria", "victoria", func(ctx context.Context, c runner.QueryCase, at time.Time) error {
			return queryPromQL(ctx, vm, c.Expr, at)
		}},
		{"clickhouse", "clickhouse", func(ctx context.Context, c runner.QueryCase, at time.Time) error {
			window, err := runner.WindowMillis(c.Expr)
			if err != nil {
				return err
			}
			sql, err := runner.ClickHouseSQL(c.Name, at.UnixMilli(), window)
			if err != nil {
				return err
			}
			_, err = clickhouseRows(ctx, *chURL, sql)
			return err
		}},
	}
	for _, target := range targets {
		before, err := lifecycle.Usage(ctx, target.service)
		if err != nil {
			return err
		}
		targetResult := targetReport{Queries: map[string]latency{}}
		for _, c := range suite.Queries {
			at := base.Add(time.Duration(c.InstantOffsetsSeconds[0] * float64(time.Second)))
			for i := 0; i < *warmups; i++ {
				if err := target.query(ctx, c, at); err != nil {
					return err
				}
			}
			measured := make([]float64, 0, *trials)
			for i := 0; i < *trials; i++ {
				start := time.Now()
				if err := target.query(ctx, c, at); err != nil {
					return err
				}
				measured = append(measured, float64(time.Since(start))/float64(time.Millisecond))
			}
			sorted := append([]float64(nil), measured...)
			sort.Float64s(sorted)
			targetResult.Queries[c.Name] = latency{P50Ms: percentile(sorted, 0.5), P95Ms: percentile(sorted, 0.95), SamplesMs: measured}
		}
		after, err := lifecycle.Usage(ctx, target.service)
		if err != nil {
			return err
		}
		targetResult.CPUUsec = after.CPUUsec - before.CPUUsec
		targetResult.MemoryPeakBytes = after.MemoryPeakBytes
		result.Targets[target.name] = targetResult
	}
	backendResult := result.Targets["backend"]
	for _, name := range []string{"prometheus", "victoria", "clickhouse"} {
		baseline := result.Targets[name]
		if backendResult.CPUUsec >= baseline.CPUUsec {
			result.Failures = append(result.Failures, fmt.Sprintf("backend CPU >= %s CPU", name))
		}
		if backendResult.MemoryPeakBytes >= baseline.MemoryPeakBytes {
			result.Failures = append(result.Failures, fmt.Sprintf("backend memory peak >= %s memory peak", name))
		}
		for _, c := range suite.Queries {
			if backendResult.Queries[c.Name].P95Ms >= baseline.Queries[c.Name].P95Ms {
				result.Failures = append(result.Failures, fmt.Sprintf("%s backend p95 latency >= %s", c.Name, name))
			}
		}
	}
	result.BenefitPassed = len(result.Failures) == 0
	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*output, encoded, 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote %s; benefit passed=%t\n", *output, result.BenefitPassed)
	if !result.BenefitPassed {
		return fmt.Errorf("issue 754 level-3 benefit assertions failed; inspect %s", *output)
	}
	return nil
}

func percentile(sorted []float64, p float64) float64 {
	return sorted[int(math.Ceil(p*float64(len(sorted))))-1]
}
func queryPromQL(ctx context.Context, target runner.HTTPQueryTarget, expr string, at time.Time) error {
	response, err := target.Instant(ctx, expr, at)
	if err != nil {
		return err
	}
	if response.Status != "success" {
		return fmt.Errorf("%s: %s", response.ErrorType, response.Error)
	}
	return nil
}
func clickhousePost(ctx context.Context, endpoint, sql string, body io.Reader) ([]byte, error) {
	if body == nil {
		body = strings.NewReader(sql)
		sql = ""
	}
	endpointURL := strings.TrimRight(endpoint, "/") + "/"
	if sql != "" {
		endpointURL += "?query=" + url.QueryEscape(sql)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL, body)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	content, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("ClickHouse %s: %s", resp.Status, content)
	}
	return content, nil
}
func seedClickHouse(ctx context.Context, endpoint string, dataset runner.Dataset, baseMs int64) error {
	if _, err := clickhousePost(ctx, endpoint,
		`CREATE TABLE IF NOT EXISTS samples (series_id UInt64, label_0 String, ts_ms Int64, value Float64) ENGINE = MergeTree ORDER BY (series_id, ts_ms)`, nil); err != nil {
		return err
	}
	var body bytes.Buffer
	for i, series := range dataset.Series {
		for _, sample := range series.ExpandedSamples() {
			row := struct {
				SeriesID int     `json:"series_id"`
				Label0   string  `json:"label_0"`
				TsMs     int64   `json:"ts_ms"`
				Value    float64 `json:"value"`
			}{i + 1, series.Labels["label_0"], baseMs + int64(math.Round(sample.OffsetSeconds*1000)), sample.Value}
			encoded, err := json.Marshal(row)
			if err != nil {
				return err
			}
			body.Write(encoded)
			body.WriteByte('\n')
		}
	}
	_, err := clickhousePost(ctx, endpoint, "INSERT INTO samples FORMAT JSONEachRow", &body)
	return err
}
func clickhouseRows(ctx context.Context, endpoint, sql string) ([]float64, error) {
	content, err := clickhousePost(ctx, endpoint, sql, nil)
	if err != nil {
		return nil, err
	}
	var payload struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(content, &payload); err != nil {
		return nil, err
	}
	values := make([]float64, 0, len(payload.Data))
	for _, row := range payload.Data {
		var value float64
		if err := json.Unmarshal(row["value"], &value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	sort.Float64s(values)
	return values, nil
}
func promValues(response runner.QueryResponse) ([]float64, error) {
	var data struct {
		Result []struct {
			Value []json.RawMessage `json:"value"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response.Data, &data); err != nil {
		return nil, err
	}
	values := make([]float64, 0, len(data.Result))
	for _, item := range data.Result {
		if len(item.Value) != 2 {
			return nil, fmt.Errorf("invalid Prometheus sample")
		}
		var text string
		if err := json.Unmarshal(item.Value[1], &text); err != nil {
			return nil, err
		}
		value, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	sort.Float64s(values)
	return values, nil
}
func verifyBaselines(ctx context.Context, suite runner.Suite, base time.Time,
	prom, vm runner.HTTPQueryTarget, clickhouse string) error {
	for _, c := range suite.Queries {
		at := base.Add(time.Duration(c.InstantOffsetsSeconds[0] * float64(time.Second)))
		promResponse, err := prom.Instant(ctx, c.Expr, at)
		if err != nil {
			return err
		}
		vmResponse, err := vm.Instant(ctx, c.Expr, at)
		if err != nil {
			return err
		}
		if err := runner.CompareResponses(promResponse, vmResponse, c.EffectiveTolerance(suite.ComparisonDefaults)); err != nil {
			return fmt.Errorf("VictoriaMetrics %s differs from Prometheus: %w", c.Name, err)
		}
		window, err := runner.WindowMillis(c.Expr)
		if err != nil {
			return err
		}
		sql, err := runner.ClickHouseSQL(c.Name, at.UnixMilli(), window)
		if err != nil {
			return err
		}
		actual, err := clickhouseRows(ctx, clickhouse, sql)
		if err != nil {
			return fmt.Errorf("ClickHouse %s: %w", c.Name, err)
		}
		expected, err := promValues(promResponse)
		if err != nil {
			return err
		}
		if len(actual) != len(expected) {
			return fmt.Errorf("ClickHouse %s returned %d values, Prometheus %d", c.Name, len(actual), len(expected))
		}
		for i := range actual {
			if math.Abs(actual[i]-expected[i]) > 1e-8+1e-8*math.Abs(expected[i]) {
				return fmt.Errorf("ClickHouse %s value %d: %g != Prometheus %g", c.Name, i, actual[i], expected[i])
			}
		}
	}
	return nil
}
