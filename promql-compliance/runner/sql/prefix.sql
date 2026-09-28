WITH {evaluation_ms} AS t_ms, {window_ms} AS window_ms,
instant_samples AS (
  SELECT series_id, label_0, argMax(value, ts_ms) AS value
  FROM samples WHERE ts_ms <= t_ms AND ts_ms >= t_ms - 300000
  GROUP BY series_id, label_0
), window_samples AS (
  SELECT series_id, label_0, ts_ms, value FROM samples
  WHERE ts_ms > t_ms - window_ms AND ts_ms <= t_ms
)