package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ProjectASAP/ASAPQuery-backend/promql-compliance/runner"
)

func main() {
	datasetPath := flag.String("dataset", "", "dataset YAML")
	suitePath := flag.String("suite", "", "query suite YAML")
	reference := flag.String("reference-url", "", "Prometheus URL")
	test := flag.String("test-url", "", "backend URL")
	baseMillis := flag.Int64("base-time-ms", time.Now().Add(-30*time.Minute).UnixMilli(), "fixture base time")
	reportPath := flag.String("output", "differential-report.json", "JSON report path")
	flag.Parse()
	if *datasetPath == "" || *suitePath == "" || *reference == "" || *test == "" {
		flag.Usage()
		os.Exit(2)
	}
	dataset, err := runner.LoadDatasetFile(*datasetPath)
	if err != nil {
		fatal(err)
	}
	suite, err := runner.LoadSuiteFile(*suitePath)
	if err != nil {
		fatal(err)
	}
	body, err := runner.EncodeRemoteWrite(*baseMillis, dataset)
	if err != nil {
		fatal(err)
	}
	ctx := context.Background()
	if err := runner.PushRemoteWrite(ctx, body, *reference, *test); err != nil {
		fatal(err)
	}
	if err := runner.Drain(ctx, *test); err != nil {
		fatal(err)
	}
	base := time.UnixMilli(*baseMillis)
	refTarget, testTarget := runner.HTTPQueryTarget{BaseURL: *reference}, runner.HTTPQueryTarget{BaseURL: *test}
	report := runner.CompareSuite(ctx, refTarget, testTarget, suite, dataset.Name, base)
	if err := os.MkdirAll(filepath.Dir(*reportPath), 0o755); err != nil && filepath.Dir(*reportPath) != "." {
		fatal(err)
	}
	file, err := os.Create(*reportPath)
	if err != nil {
		fatal(err)
	}
	if err := json.NewEncoder(file).Encode(report); err != nil {
		_ = file.Close()
		fatal(err)
	}
	if err := file.Close(); err != nil {
		fatal(err)
	}
	fmt.Printf("dataset=%s suite=%s passed=%t report=%s\n", report.Dataset, report.Suite, report.Passed, *reportPath)
	if !report.Passed {
		os.Exit(1)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
