package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ProjectASAP/ASAPQuery-backend/promql-compliance/runner"
)

func main() {
	datasetPath := flag.String("dataset", "", "dataset YAML")
	suitePath := flag.String("suite", "", "query suite YAML")
	reference := flag.String("reference-url", "", "Prometheus URL")
	test := flag.String("test-url", "", "backend URL")
	baseMillis := flag.Int64("base-time-ms", time.Now().Add(-30*time.Minute).UnixMilli(), "fixture base time")
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
	failed := false
	for _, query := range suite.Queries {
		if query.Range != nil {
			left, leftErr := refTarget.Range(ctx, query.Expr, *query.Range, base)
			right, rightErr := testTarget.Range(ctx, query.Expr, *query.Range, base)
			if leftErr != nil || rightErr != nil || runner.CompareResponses(left, right, query.EffectiveTolerance(suite.ComparisonDefaults)) != nil {
				fmt.Fprintf(os.Stderr, "FAIL %s range: reference=%v test=%v\n", query.Name, leftErr, rightErr)
				failed = true
			}
		}
		for _, at := range query.InstantOffsetsSeconds {
			when := base.Add(time.Duration(at * float64(time.Second)))
			left, leftErr := refTarget.Instant(ctx, query.Expr, when)
			right, rightErr := testTarget.Instant(ctx, query.Expr, when)
			if leftErr != nil || rightErr != nil || runner.CompareResponses(left, right, query.EffectiveTolerance(suite.ComparisonDefaults)) != nil {
				fmt.Fprintf(os.Stderr, "FAIL %s at %.0fs: reference=%v test=%v\n", query.Name, at, leftErr, rightErr)
				failed = true
			}
		}
	}
	if failed {
		os.Exit(1)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
