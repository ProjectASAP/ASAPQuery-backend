package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ProjectASAP/ASAPQuery-backend/promql-compliance/runner"
)

type stringList []string

func (items *stringList) String() string         { return fmt.Sprint([]string(*items)) }
func (items *stringList) Set(value string) error { *items = append(*items, value); return nil }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var composeFiles stringList
	datasetPath := flag.String("dataset", "", "dataset YAML")
	suitePath := flag.String("suite", "", "query suite YAML")
	reference := flag.String("reference-url", "", "Prometheus URL")
	test := flag.String("test-url", "", "backend URL")
	baseMillis := flag.Int64("base-time-ms", time.Now().Add(-30*time.Minute).UnixMilli(), "fixture base time")
	reportPath := flag.String("output", "differential-report.json", "JSON report path")
	composeProject := flag.String("compose-project", "asapquery-backend-promql-compliance", "Compose project name")
	logsDirectory := flag.String("logs-dir", "", "directory for retained Compose logs")
	keepServices := flag.Bool("keep-services", false, "leave Compose services running after the run")
	flag.Var(&composeFiles, "compose-file", "Compose file to start; may be repeated")
	flag.Parse()
	if *datasetPath == "" || *suitePath == "" || *reference == "" || *test == "" {
		flag.Usage()
		return fmt.Errorf("dataset, suite, reference-url, and test-url are required")
	}
	dataset, err := runner.LoadDatasetFile(*datasetPath)
	if err != nil {
		return err
	}
	suite, err := runner.LoadSuiteFile(*suitePath)
	if err != nil {
		return err
	}
	body, err := runner.EncodeRemoteWrite(*baseMillis, dataset)
	if err != nil {
		return err
	}
	ctx := context.Background()
	log.Printf("loaded dataset %q and suite %q", dataset.Name, suite.Name)
	runDirectory, err := os.MkdirTemp("", "asapquery-promql-compliance-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(runDirectory)
	snapshot, err := json.Marshal(runner.BuildPlanningSnapshot(suite, time.Now().UTC()))
	if err != nil {
		return err
	}
	snapshotTemplatePath := filepath.Join(runDirectory, "planning-snapshot-template.json")
	if err := os.WriteFile(snapshotTemplatePath, snapshot, 0o600); err != nil {
		return err
	}
	snapshotPath := filepath.Join(runDirectory, "planning-snapshot.json")
	// Docker creates a directory when a bind-mounted file target is absent.
	// The planner helper overwrites this placeholder with complete cost evidence.
	if err := os.WriteFile(snapshotPath, nil, 0o600); err != nil {
		return err
	}
	log.Printf("wrote suite-derived planning snapshot template to %s", snapshotTemplatePath)
	lifecycle := runner.ComposeLifecycle{Files: composeFiles, Project: *composeProject, LogsDirectory: *logsDirectory, PlanningSnapshot: snapshotPath, PlanningSnapshotTemplate: snapshotTemplatePath}
	if len(composeFiles) > 0 {
		log.Printf("building and starting Compose services; the first run may take several minutes")
	}
	if len(composeFiles) > 0 && !*keepServices {
		defer lifecycle.Stop()
	}
	if err := lifecycle.Start(ctx); err != nil {
		return err
	}
	log.Printf("waiting for Prometheus at %s", *reference)
	if err := runner.WaitForHTTP(ctx, *reference+"/api/v1/status/runtimeinfo"); err != nil {
		return err
	}
	log.Printf("waiting for backend at %s", *test)
	if err := runner.WaitForHTTP(ctx, *test+"/api/v1/health"); err != nil {
		return err
	}
	log.Printf("seeding identical Remote Write payloads")
	if err := runner.PushRemoteWrite(ctx, body, *reference, *test); err != nil {
		return err
	}
	log.Printf("draining backend precompute work")
	if err := runner.Drain(ctx, *test); err != nil {
		return err
	}
	base := time.UnixMilli(*baseMillis)
	refTarget, testTarget := runner.HTTPQueryTarget{BaseURL: *reference}, runner.HTTPQueryTarget{BaseURL: *test}
	log.Printf("comparing %d query cases", len(suite.Queries))
	report := runner.CompareSuite(ctx, refTarget, testTarget, suite, dataset.Name, base)
	if err := os.MkdirAll(filepath.Dir(*reportPath), 0o755); err != nil && filepath.Dir(*reportPath) != "." {
		return err
	}
	file, err := os.Create(*reportPath)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(file).Encode(report); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	log.Printf("wrote comparison report to %s", *reportPath)
	fmt.Printf("dataset=%s suite=%s passed=%t report=%s\n", report.Dataset, report.Suite, report.Passed, *reportPath)
	if !report.Passed {
		return fmt.Errorf("PromQL comparison failed; report: %s", *reportPath)
	}
	return nil
}
