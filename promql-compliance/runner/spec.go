// Package runner owns the declarative inputs for backend PromQL differential
// tests. The runner will derive both control-plane publication input and query
// evaluations from this single suite definition.
package runner

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type Suite struct {
	Name               string           `yaml:"name"`
	ComparisonDefaults ComparisonPolicy `yaml:"comparison_defaults"`
	Queries            []QueryCase      `yaml:"queries"`
}

// Dataset contains source samples expressed relative to the run's base time.
// The same expanded Remote Write payload will later be sent to both targets.
type Dataset struct {
	Name   string          `yaml:"name"`
	Series []DatasetSeries `yaml:"series"`
}

type DatasetSeries struct {
	Metric  string            `yaml:"metric"`
	Labels  map[string]string `yaml:"labels"`
	Samples []DatasetSample   `yaml:"samples"`
}

type DatasetSample struct {
	OffsetSeconds float64 `yaml:"offset_seconds"`
	Value         float64 `yaml:"value"`
}

type QueryCase struct {
	Name                  string            `yaml:"name"`
	Expr                  string            `yaml:"expr"`
	InstantOffsetsSeconds []float64         `yaml:"instant_offsets_seconds"`
	Range                 *RangeSpec        `yaml:"range"`
	Comparison            *ComparisonPolicy `yaml:"comparison"`
	ExpectError           bool              `yaml:"expect_error"`
}

type RangeSpec struct {
	StartOffsetSeconds float64 `yaml:"start_offset_seconds"`
	EndOffsetSeconds   float64 `yaml:"end_offset_seconds"`
	StepSeconds        float64 `yaml:"step_seconds"`
}

type ComparisonPolicy struct {
	ValueTolerance *Tolerance `yaml:"value_tolerance"`
}

type Tolerance struct {
	Relative *float64 `yaml:"relative"`
	Absolute *float64 `yaml:"absolute"`
}

// LoadSuite rejects implicit evaluation windows. Fixed evaluation timestamps
// are required so that Prometheus and the backend see the same query input.
func LoadSuite(contents []byte) (Suite, error) {
	var suite Suite
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(&suite); err != nil {
		return Suite{}, fmt.Errorf("parse query suite: %w", err)
	}
	if suite.Name == "" {
		return Suite{}, fmt.Errorf("query suite has no name")
	}
	if len(suite.Queries) == 0 {
		return Suite{}, fmt.Errorf("query suite %q has no queries", suite.Name)
	}
	for index := range suite.Queries {
		query := &suite.Queries[index]
		if query.Name == "" || query.Expr == "" {
			return Suite{}, fmt.Errorf("query %d requires name and expr", index)
		}
		if len(query.InstantOffsetsSeconds) == 0 && query.Range == nil {
			return Suite{}, fmt.Errorf("query %q has neither instant times nor a range", query.Name)
		}
		if query.Range != nil {
			if err := query.Range.validate(); err != nil {
				return Suite{}, fmt.Errorf("query %q: %w", query.Name, err)
			}
			for _, offset := range query.InstantOffsetsSeconds {
				if !finite(offset) || offset < query.Range.StartOffsetSeconds || offset > query.Range.EndOffsetSeconds {
					return Suite{}, fmt.Errorf("query %q has instant offset outside its range", query.Name)
				}
			}
		} else {
			for _, offset := range query.InstantOffsetsSeconds {
				if !finite(offset) {
					return Suite{}, fmt.Errorf("query %q has non-finite instant offset", query.Name)
				}
			}
		}
		if err := validateTolerance(query.EffectiveTolerance(suite.ComparisonDefaults).ValueTolerance); err != nil {
			return Suite{}, fmt.Errorf("query %q: %w", query.Name, err)
		}
	}
	return suite, nil
}

// LoadDataset validates the properties needed to construct deterministic
// Remote Write batches: one unique label set per metric and strictly ordered,
// finite samples per series.
func LoadDataset(contents []byte) (Dataset, error) {
	var dataset Dataset
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(&dataset); err != nil {
		return Dataset{}, fmt.Errorf("parse dataset: %w", err)
	}
	if dataset.Name == "" || len(dataset.Series) == 0 {
		return Dataset{}, fmt.Errorf("dataset requires name and series")
	}
	seen := make(map[string]struct{}, len(dataset.Series))
	for index, series := range dataset.Series {
		if series.Metric == "" || len(series.Samples) == 0 {
			return Dataset{}, fmt.Errorf("series %d requires metric and samples", index)
		}
		key := series.Metric + "\x00" + canonicalLabels(series.Labels)
		if _, duplicate := seen[key]; duplicate {
			return Dataset{}, fmt.Errorf("dataset has duplicate series %q", series.Metric)
		}
		seen[key] = struct{}{}
		var previous float64
		for sampleIndex, sample := range series.Samples {
			if !finite(sample.OffsetSeconds) || !finite(sample.Value) {
				return Dataset{}, fmt.Errorf("series %q sample %d is non-finite", series.Metric, sampleIndex)
			}
			if sampleIndex > 0 && sample.OffsetSeconds <= previous {
				return Dataset{}, fmt.Errorf("series %q samples are not strictly ordered", series.Metric)
			}
			previous = sample.OffsetSeconds
		}
	}
	return dataset, nil
}

func LoadSuiteFile(path string) (Suite, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Suite{}, err
	}
	return LoadSuite(contents)
}
func LoadDatasetFile(path string) (Dataset, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Dataset{}, err
	}
	return LoadDataset(contents)
}

func (q QueryCase) EffectiveTolerance(defaults ComparisonPolicy) ComparisonPolicy {
	if q.Comparison == nil || q.Comparison.ValueTolerance == nil {
		return defaults
	}
	result := defaults
	if result.ValueTolerance == nil {
		result.ValueTolerance = &Tolerance{}
	}
	merged := *result.ValueTolerance
	if q.Comparison.ValueTolerance.Relative != nil {
		merged.Relative = q.Comparison.ValueTolerance.Relative
	}
	if q.Comparison.ValueTolerance.Absolute != nil {
		merged.Absolute = q.Comparison.ValueTolerance.Absolute
	}
	result.ValueTolerance = &merged
	return result
}

func (r RangeSpec) validate() error {
	if !finite(r.StartOffsetSeconds) || !finite(r.EndOffsetSeconds) || !finite(r.StepSeconds) {
		return fmt.Errorf("range offsets and step must be finite")
	}
	if r.EndOffsetSeconds <= r.StartOffsetSeconds || r.StepSeconds <= 0 {
		return fmt.Errorf("range end must be after start and step must be positive")
	}
	return nil
}

func validateTolerance(tolerance *Tolerance) error {
	if tolerance == nil {
		return nil
	}
	for _, value := range []*float64{tolerance.Relative, tolerance.Absolute} {
		if value != nil && (!finite(*value) || *value < 0) {
			return fmt.Errorf("tolerance must be finite and non-negative")
		}
	}
	return nil
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

func canonicalLabels(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+labels[key])
	}
	return strings.Join(parts, "\x00")
}
