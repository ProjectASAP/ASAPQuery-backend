package runner

import (
	"context"
	"fmt"
	"time"
)

// Report is written even when query comparisons fail so a manual CI run has
// evidence for every attempted evaluation.
type Report struct {
	Suite    string        `json:"suite"`
	Dataset  string        `json:"dataset"`
	BaseTime time.Time     `json:"baseTime"`
	Queries  []QueryReport `json:"queries"`
	Passed   bool          `json:"passed"`
}

type QueryReport struct {
	Name            string              `json:"name"`
	Expr            string              `json:"expr"`
	Tolerance       ComparisonPolicy    `json:"tolerance"`
	Range           *ComparisonOutcome  `json:"range,omitempty"`
	RangeResponses  *ResponsePair       `json:"rangeResponses,omitempty"`
	Instant         []InstantComparison `json:"instant,omitempty"`
	ReferenceParity []InstantComparison `json:"referenceParity,omitempty"`
	BackendParity   []InstantComparison `json:"backendParity,omitempty"`
	Passed          bool                `json:"passed"`
}

type InstantComparison struct {
	OffsetSeconds float64           `json:"offsetSeconds"`
	Time          time.Time         `json:"time"`
	Comparison    ComparisonOutcome `json:"comparison"`
	Responses     ResponsePair      `json:"responses"`
}

// ResponsePair preserves the raw public API payloads behind each comparison.
// It lets a passing report show exactly what Prometheus and ASAPQuery returned.
type ResponsePair struct {
	Reference QueryResponse `json:"reference"`
	Backend   QueryResponse `json:"backend"`
}
type ComparisonOutcome struct {
	Passed         bool   `json:"passed"`
	Diff           string `json:"diff,omitempty"`
	ReferenceError string `json:"referenceError,omitempty"`
	BackendError   string `json:"backendError,omitempty"`
}

func CompareSuite(ctx context.Context, reference, backend HTTPQueryTarget, suite Suite, dataset string, base time.Time) Report {
	report := Report{Suite: suite.Name, Dataset: dataset, BaseTime: base, Passed: true}
	for _, query := range suite.Queries {
		item := CompareQuery(ctx, reference, backend, query, suite.ComparisonDefaults, base)
		report.Queries = append(report.Queries, item)
		report.Passed = report.Passed && item.Passed
	}
	return report
}

func CompareQuery(ctx context.Context, reference, backend HTTPQueryTarget, query QueryCase, defaults ComparisonPolicy, base time.Time) QueryReport {
	policy := query.EffectiveTolerance(defaults)
	report := QueryReport{Name: query.Name, Expr: query.Expr, Tolerance: policy, Passed: true}
	var referenceRange, backendRange QueryResponse
	if query.Range != nil {
		left, leftErr := reference.Range(ctx, query.Expr, *query.Range, base)
		right, rightErr := backend.Range(ctx, query.Expr, *query.Range, base)
		outcome := compareOutcome(left, right, leftErr, rightErr, policy)
		report.Range, report.Passed = &outcome, report.Passed && outcome.Passed
		report.RangeResponses = &ResponsePair{Reference: left, Backend: right}
		referenceRange, backendRange = left, right
	}
	for _, offset := range query.InstantOffsetsSeconds {
		at := base.Add(time.Duration(offset * float64(time.Second)))
		left, leftErr := reference.Instant(ctx, query.Expr, at)
		right, rightErr := backend.Instant(ctx, query.Expr, at)
		outcome := compareOutcome(left, right, leftErr, rightErr, policy)
		report.Instant = append(report.Instant, InstantComparison{OffsetSeconds: offset, Time: at, Comparison: outcome, Responses: ResponsePair{Reference: left, Backend: right}})
		report.Passed = report.Passed && outcome.Passed
		if query.Range == nil || leftErr != nil || rightErr != nil || referenceRange.Status != "success" || backendRange.Status != "success" || left.Status != "success" || right.Status != "success" {
			continue
		}
		refParity := parityOutcome(referenceRange, left, at, policy)
		backendParity := parityOutcome(backendRange, right, at, policy)
		report.ReferenceParity = append(report.ReferenceParity, InstantComparison{OffsetSeconds: offset, Time: at, Comparison: refParity})
		report.BackendParity = append(report.BackendParity, InstantComparison{OffsetSeconds: offset, Time: at, Comparison: backendParity})
		report.Passed = report.Passed && refParity.Passed && backendParity.Passed
	}
	return report
}

func compareOutcome(left, right QueryResponse, leftErr, rightErr error, policy ComparisonPolicy) ComparisonOutcome {
	outcome := ComparisonOutcome{}
	if leftErr != nil {
		outcome.ReferenceError = leftErr.Error()
	}
	if rightErr != nil {
		outcome.BackendError = rightErr.Error()
	}
	if leftErr == nil && rightErr == nil {
		if err := CompareResponses(left, right, policy); err != nil {
			outcome.Diff = err.Error()
		}
	}
	outcome.Passed = outcome.Diff == "" && outcome.ReferenceError == "" && outcome.BackendError == ""
	return outcome
}

func parityOutcome(rangeResponse, instantResponse QueryResponse, at time.Time, policy ComparisonPolicy) ComparisonOutcome {
	err := CompareRangeAtInstant(rangeResponse, instantResponse, at, policy)
	if err == nil {
		return ComparisonOutcome{Passed: true}
	}
	return ComparisonOutcome{Diff: fmt.Sprintf("range/instant parity: %v", err)}
}
