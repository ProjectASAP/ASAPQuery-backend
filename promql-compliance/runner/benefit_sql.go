package runner

import (
	"fmt"
	"regexp"
	"strconv"
)

var promqlRange = regexp.MustCompile(`\[(\d+)([smhd])\]`)

// WindowMillis extracts the range selector used by the shared benchmark case.
func WindowMillis(expr string) (int64, error) {
	match := promqlRange.FindStringSubmatch(expr)
	if match == nil {
		return 60_000, nil
	}
	amount, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil {
		return 0, err
	}
	unit := map[string]int64{"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}[match[2]]
	return amount * unit, nil
}

// ClickHouseSQL compiles one shared level-2 case to the exact float-sample SQL
// baseline defined by issue #754. Unknown cases fail rather than run a proxy query.
func ClickHouseSQL(name string, evaluationMs, windowMs int64) (string, error) {
	if windowMs <= 0 {
		return "", fmt.Errorf("positive window required")
	}
	prefix := fmt.Sprintf(`WITH %d AS t_ms, %d AS window_ms,
instant_samples AS (
  SELECT series_id, label_0, argMax(value, ts_ms) AS value
  FROM samples WHERE ts_ms <= t_ms AND ts_ms >= t_ms - 300000
  GROUP BY series_id, label_0
), window_samples AS (
  SELECT series_id, label_0, ts_ms, value FROM samples
  WHERE ts_ms > t_ms - window_ms AND ts_ms <= t_ms
)`, evaluationMs, windowMs)
	counter := `,
ordered AS (
  SELECT series_id, label_0, ts_ms, value,
    row_number() OVER (PARTITION BY series_id ORDER BY ts_ms) AS sample_index,
    lag(value, 1, 0.) OVER (PARTITION BY series_id ORDER BY ts_ms) AS previous_value
  FROM window_samples
), corrected AS (
  SELECT series_id, label_0, count() AS sample_count,
    min(ts_ms) AS first_ms, max(ts_ms) AS last_ms,
    argMin(value, ts_ms) AS first_value, argMax(value, ts_ms) AS last_value,
    sum(if(sample_index > 1 AND value < previous_value, previous_value, 0.)) AS reset_correction
  FROM ordered GROUP BY series_id, label_0 HAVING sample_count >= 2
), durations AS (
  SELECT *, last_value - first_value + reset_correction AS corrected_delta,
    (last_ms - first_ms) / 1000. AS sampled_seconds,
    (first_ms - (t_ms - window_ms)) / 1000. AS gap_start_seconds,
    (t_ms - last_ms) / 1000. AS gap_end_seconds
  FROM corrected
), thresholds AS (
  SELECT *, sampled_seconds / (sample_count - 1) AS average_gap_seconds FROM durations
), adjusted AS (
  SELECT *,
    if(gap_start_seconds >= 1.1 * average_gap_seconds, average_gap_seconds / 2, gap_start_seconds) AS start_seconds,
    if(gap_end_seconds >= 1.1 * average_gap_seconds, average_gap_seconds / 2, gap_end_seconds) AS end_seconds
  FROM thresholds
), extrapolated AS (
  SELECT *, if(corrected_delta > 0 AND first_value >= 0,
    least(start_seconds, sampled_seconds * first_value / corrected_delta),
    start_seconds) AS zero_adjusted_start_seconds FROM adjusted
), per_series_counter AS (
  SELECT series_id, label_0,
    corrected_delta * (sampled_seconds + zero_adjusted_start_seconds + end_seconds)
      / sampled_seconds AS increase_value,
    corrected_delta * (sampled_seconds + zero_adjusted_start_seconds + end_seconds)
      / sampled_seconds / (window_ms / 1000.) AS rate_value
  FROM extrapolated
)`
	var final string
	switch name {
	case "spatial-sum":
		final = `SELECT label_0, sum(value) AS value FROM instant_samples GROUP BY label_0`
	case "spatial-topk":
		final = `SELECT series_id, label_0, value FROM instant_samples ORDER BY label_0, value DESC, series_id LIMIT 3 BY label_0`
	case "spatial-quantile":
		final = `SELECT label_0, quantileExactInclusive(0.9)(value) AS value FROM instant_samples GROUP BY label_0`
	case "temporal-sum":
		final = `SELECT series_id, sum(value) AS value FROM window_samples GROUP BY series_id`
	case "temporal-quantile":
		final = `SELECT series_id, quantileExactInclusive(0.9)(value) AS value FROM window_samples GROUP BY series_id`
	case "temporal-rate":
		prefix += counter
		final = `SELECT series_id, rate_value AS value FROM per_series_counter`
	case "grouped-rate":
		prefix += counter
		final = `SELECT label_0, sum(rate_value) AS value FROM per_series_counter GROUP BY label_0`
	case "grouped-temporal-sum":
		final = `SELECT label_0, sum(series_sum) AS value FROM (SELECT series_id, label_0, sum(value) AS series_sum FROM window_samples GROUP BY series_id, label_0) GROUP BY label_0`
	case "topk-rate":
		prefix += counter
		final = `SELECT series_id, label_0, rate_value AS value FROM per_series_counter ORDER BY label_0, rate_value DESC, series_id LIMIT 3 BY label_0`
	case "quantile-ratio":
		final = `SELECT series_id, quantileExactInclusive(0.9)(value) / quantileExactInclusive(0.5)(value) AS value FROM window_samples GROUP BY series_id`
	default:
		return "", fmt.Errorf("no ClickHouse baseline for %q", name)
	}
	return prefix + "\n" + final + " FORMAT JSON", nil
}
