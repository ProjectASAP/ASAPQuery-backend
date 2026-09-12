#!/usr/bin/env python3
"""Plan explicit offline scale cells without allocating their datasets."""
import argparse
import json
import math
from pathlib import Path

from accuracy_suite import corpus
from generate import WINDOWS


# Small correctness fixtures are deliberately distinct from benefit experiments.
PROFILES = {
    "smoke": (10, 4, "1m", 1),
    "benefit": (1000, 16, "1h", 30),
    "scale": (10000, 16, "6h", 60),
    "cardinality": (1000000, 4, "1m", 1),
}


def plan(profile="benefit", groups=None, members=None, window=None,
         evaluation_minutes=None, start_ms=1700000000000,
         measured_bytes_per_sample=None):
    defaults = PROFILES[profile]
    groups = defaults[0] if groups is None else groups
    members = defaults[1] if members is None else members
    window = defaults[2] if window is None else window
    minutes = defaults[3] if evaluation_minutes is None else evaluation_minutes
    if min(groups, members, minutes) < 1 or start_ms < 0:
        raise ValueError("groups, members and evaluation minutes must be positive; start must be nonnegative")
    if measured_bytes_per_sample is not None and (not math.isfinite(measured_bytes_per_sample) or measured_bytes_per_sample <= 0):
        raise ValueError("measured bytes per sample must be positive and finite")
    window_ms = WINDOWS[window]
    span = minutes * 60000
    duration = window_ms + span
    series = groups * members
    samples = 2 * series * (duration // 100 + 1)
    return {
        "schema_version": 1, "profile": profile, "dataset": "synthetic",
        "purpose": "correctness smoke only" if profile == "smoke" else "candidate benefit experiment; advantage is not guaranteed",
        "groups": groups, "members": members, "window": window, "scrape_ms": 100,
        "start_ms": start_ms, "duration_ms": duration,
        "evaluation_start_ms": start_ms + window_ms,
        "evaluation_end_ms": start_ms + duration,
        "evaluation_minutes": minutes, "temporal_occurrences_per_query": minutes + 1,
        "spatial_occurrences_per_query": span // 1000 + 1,
        "series_per_metric": series, "total_series": 2 * series, "total_samples": samples,
        "unfiltered_temporal_samples_per_query": series * (window_ms // 100),
        "single_group_filtered_temporal_samples_per_query": members * (window_ms // 100),
        "spatial_input_series_per_query": series, "spatial_sum_output_groups": groups,
        "numeric_payload_bytes_uncompressed": samples * 16,
        "measured_store_bytes_per_sample": measured_bytes_per_sample,
        "estimated_store_bytes": math.ceil(samples * measured_bytes_per_sample) if measured_bytes_per_sample else None,
        "storage_scope": "16-byte timestamp/value payload excludes labels and indexes and is not a disk estimate; optional store estimate uses caller-supplied pilot measurement for one store, not all system copies",
    }


def query_manifest(scale):
    manifest = corpus("synthetic")
    manifest["queries"] = [q for q in manifest["queries"]
                           if q["interval_ms"] == 1000 or q["window"] == scale["window"]]
    manifest["scale"] = scale
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=PROFILES, default="benefit")
    parser.add_argument("--groups", type=int)
    parser.add_argument("--members", type=int)
    parser.add_argument("--window", choices=WINDOWS)
    parser.add_argument("--evaluation-minutes", type=int)
    parser.add_argument("--start-ms", type=int, default=1700000000000)
    parser.add_argument("--measured-bytes-per-sample", type=float)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    scale = plan(args.profile, args.groups, args.members, args.window,
                 args.evaluation_minutes, args.start_ms, args.measured_bytes_per_sample)
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "scale.json").write_text(json.dumps(scale, indent=2))
    (args.output / "queries.json").write_text(json.dumps(query_manifest(scale), indent=2))
    # Explicit requested cardinality/window matrix; planning never generates data.
    matrix = [plan(args.profile, 10 ** exponent, scale["members"], window,
                   scale["evaluation_minutes"], args.start_ms, args.measured_bytes_per_sample)
              for exponent in range(1, 7) for window in WINDOWS]
    (args.output / "matrix.json").write_text(json.dumps(matrix, indent=2))
    print(json.dumps(scale))


if __name__ == "__main__":
    main()
