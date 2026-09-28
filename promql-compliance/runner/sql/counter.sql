,
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
)