"""Matched Prometheus-result comparison and explicitly scoped process measurements."""
import math
import os
from pathlib import Path
import statistics


def result_samples(response):
    if response.get("status") != "success":
        raise ValueError("query did not succeed")
    if response.get("warnings"):
        raise ValueError("response warnings require review for partial results")
    data = response["data"]
    kind = data["resultType"]
    items = data["result"]
    if kind == "scalar":
        items = [{"metric": {}, "value": items}]
    elif kind not in ("vector", "matrix"):
        raise ValueError(f"unsupported comparison result type: {kind}")
    result, groups = {}, set()
    for item in items:
        labels = tuple(sorted(item["metric"].items()))
        if labels in groups:
            raise ValueError("duplicate series")
        groups.add(labels)
        for timestamp, value in item["values"] if kind == "matrix" else [item["value"]]:
            key = (labels, float(timestamp))
            if key in result or not math.isfinite(key[1]):
                raise ValueError("duplicate or invalid sample timestamp")
            result[key] = float(value)
    return kind, groups, result


def compare_results(actual, expected):
    try:
        ak, ag, a = result_samples(actual)
        ek, eg, e = result_samples(expected)
        if ak != ek:
            raise ValueError("different result types")
    except (ValueError, TypeError, KeyError) as error:
        return {"comparable": False, "equal": False, "reason": str(error)}
    absolute, relative, zero, nonfinite = [], [], 0, 0
    for key in a.keys() & e.keys():
        x, y = a[key], e[key]
        if not math.isfinite(x) or not math.isfinite(y):
            if not (x == y or (math.isnan(x) and math.isnan(y))):
                nonfinite += 1
            continue
        error = abs(x - y)
        if not math.isfinite(error):
            nonfinite += 1
            continue
        absolute.append(error)
        if y:
            ratio = error / abs(y)
            if math.isfinite(ratio):
                relative.append(ratio)
            else:
                nonfinite += 1
        elif error:
            zero += 1
    missing, extra = len(e.keys() - a.keys()), len(a.keys() - e.keys())
    return {"comparable": True,
            "equal": ag == eg and not (missing or extra or zero or nonfinite or any(absolute)),
            "missing_series": len(eg - ag), "extra_series": len(ag - eg),
            "missing_samples": missing, "extra_samples": extra,
            "completeness": (len(a.keys() & e.keys()) / len(e)) if e else (1.0 if not a else 0.0),
            "max_absolute_error": max(absolute, default=None),
            "max_relative_error": max(relative, default=None),
            "zero_baseline_mismatches": zero, "nonfinite_mismatches": nonfinite}


def distribution(values):
    if not values:
        return None
    values = sorted(values)
    return {"count": len(values), "min_ns": values[0], "median_ns": statistics.median(values),
            "p95_ns": values[math.ceil(0.95 * len(values)) - 1], "max_ns": values[-1],
            "mean_ns": statistics.mean(values), "stddev_ns": statistics.stdev(values) if len(values) > 1 else None}


def summarize(rows):
    comparisons = [compare_results(row["response"], row.get("exact", {}).get("response", {})) for row in rows]
    eligible = bool(rows) and all(
        c["equal"] and r["execution"] in ("warm", "exact_fallback") and r["exact"].get("http_status") == 200
        for r, c in zip(rows, comparisons))
    actual = sum(r["elapsed_ns"] for r in rows)
    exact = sum(r.get("exact", {}).get("elapsed_ns", 0) for r in rows)
    return {"occurrences": len(rows),
            "execution_counts": {k: sum(r["execution"] == k for r in rows) for k in ("warm", "exact_fallback", "failed")},
            "equal_results": sum(c["equal"] for c in comparisons),
            "uncomparable_results": sum(not c["comparable"] for c in comparisons),
            "comparisons": comparisons,
            "backend_latency": distribution([r["elapsed_ns"] for r in rows]),
            "exact_latency": distribution([r["exact"]["elapsed_ns"] for r in rows if "exact" in r]),
            "successful_backend_requests_per_query_service_second":
                sum(r["execution"] != "failed" for r in rows) * 1e9 / actual if actual else None,
            "matched_query_latency_ratio": exact / actual if eligible and actual else None,
            "end_to_end_benefit": None,
            "scope": "sequential HTTP service time including failures; not concurrent/system throughput; ratio only for wholly exact-equal matched results"}


def process_snapshot(pid):
    """Linux process scope, not retained summary heap. PID reuse invalidates deltas."""
    try:
        root = Path(f"/proc/{pid}")
        stat = (root / "stat").read_text().rsplit(")", 1)[1].split()
        status = dict(line.split(":", 1) for line in (root / "status").read_text().splitlines() if ":" in line)
        return {"pid": pid, "start_ticks": int(stat[19]),
                "cpu_ns": int((int(stat[11]) + int(stat[12])) * 1e9 / os.sysconf("SC_CLK_TCK")),
                "rss_bytes": int(status["VmRSS"].split()[0]) * 1024,
                "process_lifetime_peak_rss_bytes": int(status["VmHWM"].split()[0]) * 1024,
                "cpu_affinity": status.get("Cpus_allowed_list", "").strip(),
                "cgroup": (root / "cgroup").read_text(),
                "retained_summary_state_bytes": None}
    except (OSError, KeyError, ValueError, IndexError):
        return None


def process_delta(before, after):
    if not before or not after or (before["pid"], before["start_ticks"]) != (after["pid"], after["start_ticks"]):
        return None
    return {"cpu_ns": after["cpu_ns"] - before["cpu_ns"],
            "rss_before_bytes": before["rss_bytes"], "rss_after_bytes": after["rss_bytes"],
            "process_lifetime_peak_rss_bytes": after["process_lifetime_peak_rss_bytes"],
            "scope": "whole process including background work; CPU tick resolution; HWM is lifetime, not phase peak"}
