#!/usr/bin/env python3
"""Replace enumeration seeds with a conservative measured shared CPU-only model.

The current snapshot schema has one implementation profile, not per-state costs.
This uses maxima of inclusive candidate measurements as conservative coarse
inputs. Zero network/scan fields are explicitly excluded model dimensions, not
claims of zero bytes. Complete final quotes still require actual candidate runs.
"""
import argparse
import hashlib
import json
from pathlib import Path
from calibrate import nonnegative


def update(snapshot, measurements, sample_count):
    rows = [r for r in measurements["candidates"] if r.get("executable")]
    if measurements.get("units") != "cpu_ns" or not rows or sample_count <= 0:
        raise ValueError("requires executable CPU measurements and positive sample count")
    horizon = snapshot["implementation"]["horizon_seconds"]
    if any(r["horizon_seconds"] != horizon for r in rows):
        raise ValueError("measurement horizon does not match snapshot")
    def phase(name):
        return max(nonnegative(r["horizon_phases"][name]["cpu_ns"], name) for r in rows)
    reads = []
    for row in rows:
        for query in row["queries"].values():
            if query["evaluations"] <= 0 or not query.get("correct") or query.get("classification") not in ("warm", "hybrid", "exact_fallback"):
                raise ValueError("profile requires successful correct query measurements")
            reads.append(nonnegative(query["cpu_ns"], "query CPU") / query["evaluations"])
    costs = {"build": phase("install"), "maintenance_per_update": phase("ingest_and_build") / sample_count,
             "read": max(reads), "retention_per_second": phase("residency") / horizon,
             "retirement": phase("retirement")}
    cpu = sum(phase(p) for p in ("install", "ingest_and_build", "residency", "retirement"))
    impl = snapshot["implementation"]
    impl["lifecycle_costs"] = costs
    impl["implementation_cost"].update(model_version="measured-inclusive-coarse-cpu-only-v1", cpu_cost=cpu,
        weighted_cost=cpu, peak_memory_bytes=int(max(nonnegative(r["resources"]["peak_memory_bytes"], "peak memory") for r in rows)),
        storage_bytes=int(max(nonnegative(r["resources"]["storage_bytes"], "storage") for r in rows)),
        network_bytes=0, source_scan_bytes=0)
    snapshot.pop("workload_cost_evidence", None)
    return snapshot, {"model": "shared conservative inclusive CPU-only profile",
        "method": "maximum observed complete-candidate phase costs; ingestion/build divided by actual sample count; query read maximum inclusive CPU per evaluation",
        "limitations": ["Global profile is not per-state timing; inclusive costs can overestimate each maintained state",
                        "Residency CPU is measured accelerated replay wall time, not extrapolated to the logical horizon",
                        "network_bytes and source_scan_bytes zero fields are excluded from this CPU-only model, not measured zero traffic/scans",
                        "Changing this profile requires candidate regeneration and verification before final quote creation"],
        "sample_count": sample_count, "lifecycle_costs": costs}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--measurements", type=Path, required=True)
    parser.add_argument("--sample-count", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    snapshot, audit = update(json.loads(args.snapshot.read_text()), json.loads(args.measurements.read_text()), args.sample_count)
    audit["measurement_sha256"] = hashlib.sha256(args.measurements.read_bytes()).hexdigest()
    args.output.write_text(json.dumps(snapshot, indent=2, allow_nan=False) + "\n")
    args.output.with_suffix(".profile.json").write_text(json.dumps(audit, indent=2, allow_nan=False) + "\n")


if __name__ == "__main__":
    main()
