#!/usr/bin/env python3
"""Run five isolated ERP evaluation arms under one immutable contract."""
import argparse, hashlib, json, os, pathlib, subprocess, tempfile, time

ARM_NAMES = ["autosketch_per_query", "planner_analytical", "planner_erp", "asap_no_sharing", "exact"]
REQUIRED_METRICS = {"state_bytes", "max_error"}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_manifest(manifest, root):
    if manifest.get("schema_version") != 1:
        raise ValueError("unsupported manifest schema_version")
    dataset = manifest["dataset"]
    path = (root / dataset["path"]).resolve() if not pathlib.Path(dataset["path"]).is_absolute() else pathlib.Path(dataset["path"])
    actual = sha256_file(path)
    if actual != dataset["sha256"]:
        raise ValueError(f"dataset checksum mismatch: expected {dataset['sha256']}, got {actual}")
    constraints = manifest["constraints"]
    for key in (
        "candidate_space", "memory_budget_bytes", "accuracy", "window",
        "erp_observation", "erp_selection_policy",
    ):
        if key not in constraints:
            raise ValueError(f"missing common constraint {key}")
    if constraints["memory_budget_bytes"] <= 0 or not constraints["candidate_space"]:
        raise ValueError("candidate space and memory budget must be non-empty/positive")
    validate_erp_contract(constraints["erp_observation"], constraints["erp_selection_policy"])
    arms = manifest["arms"]
    if [arm.get("name") for arm in arms] != ARM_NAMES:
        raise ValueError(f"arms must be ordered exactly as {ARM_NAMES}")
    if any(not isinstance(arm.get("command"), list) or not arm["command"] for arm in arms):
        raise ValueError("every arm requires a non-empty argv command")
    return path, actual, constraints


def validate_erp_contract(observation, policy):
    if observation.get("cardinality", 0) <= 0 or observation.get("observed_events", 0) <= 0:
        raise ValueError("ERP observation cardinality/events must be positive")
    fits = observation.get("fits")
    if not isinstance(fits, list) or not fits:
        raise ValueError("ERP observation requires at least one fitted family")
    families = set()
    for fit in fits:
        family = fit.get("family")
        if not isinstance(family, str) or not family or family in families:
            raise ValueError("ERP fit families must be unique and non-empty")
        families.add(family)
        parameters = fit.get("parameters")
        goodness, confidence = fit.get("goodness_of_fit"), fit.get("confidence")
        if not isinstance(parameters, dict) or any(not isinstance(v, (int, float)) for v in parameters.values()):
            raise ValueError("ERP fit parameters must be numeric")
        if not isinstance(goodness, (int, float)) or goodness < 0:
            raise ValueError("ERP goodness_of_fit must be non-negative")
        if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise ValueError("ERP confidence must be within [0,1]")
    fingerprint = observation.get("empirical_fingerprint")
    if fingerprint is not None and (not isinstance(fingerprint, str) or not fingerprint):
        raise ValueError("ERP empirical_fingerprint must be absent or non-empty")
    required = {
        "minimum_benchmark_events", "max_log2_cardinality_distance",
        "max_parameter_distance", "max_goodness_of_fit", "minimum_confidence",
        "minimum_confidence_margin",
    }
    if required - set(policy):
        raise ValueError(f"ERP selection policy missing {sorted(required - set(policy))}")
    if policy["minimum_benchmark_events"] <= 0:
        raise ValueError("ERP minimum benchmark events must be positive")
    for key in ("max_log2_cardinality_distance", "max_parameter_distance", "max_goodness_of_fit"):
        if not isinstance(policy[key], (int, float)) or policy[key] < 0:
            raise ValueError(f"ERP {key} must be non-negative")
    for key in ("minimum_confidence", "minimum_confidence_margin"):
        if not isinstance(policy[key], (int, float)) or not 0 <= policy[key] <= 1:
            raise ValueError(f"ERP {key} must be within [0,1]")


def validate_result(name, result, expected_contract, candidate_space):
    if result.get("contract") != expected_contract:
        raise ValueError(f"{name} did not execute the identical evaluation contract")
    missing = REQUIRED_METRICS - set(result.get("metrics", {}))
    if missing or "selected_plan" not in result or "selected_candidates" not in result:
        raise ValueError(f"{name} result missing selection/metrics: {sorted(missing)}")
    selected = result["selected_candidates"]
    if not isinstance(selected, list) or (name != "exact" and not selected):
        raise ValueError(f"{name} must report selected candidate records")
    legal = {canonical(candidate) for candidate in candidate_space}
    if any(canonical(candidate) not in legal for candidate in selected):
        raise ValueError(f"{name} selected a candidate outside the common space")
    metrics = result["metrics"]
    state_bytes, error = metrics["state_bytes"], metrics["max_error"]
    if not isinstance(state_bytes, int) or state_bytes < 0 or not isinstance(error, (int, float)):
        raise ValueError(f"{name} reported invalid state/error metrics")
    if name != "exact" and state_bytes > expected_contract["memory_budget_bytes"]:
        raise ValueError(f"{name} exceeded the common memory budget")
    accuracy = expected_contract["accuracy"]
    if accuracy.get("metric") != "max_error" or "upper_bound" not in accuracy:
        raise ValueError("accuracy contract must define max_error upper_bound")
    if error > accuracy["upper_bound"]:
        raise ValueError(f"{name} violated the common accuracy constraint")


def run_arm(arm, expected_contract, candidate_space, cwd, dataset_path=None):
    with tempfile.NamedTemporaryFile(prefix="asap-figure1-time-", delete=False) as timing:
        timing_path = timing.name
    argv = ["/usr/bin/time", "-f", '{"user_seconds":%U,"system_seconds":%S,"peak_rss_kb":%M}', "-o", timing_path, "--"] + arm["command"]
    started = time.monotonic_ns()
    env = os.environ.copy()
    env["ASAP_FIGURE1_CONTRACT_JSON"] = canonical(expected_contract)
    if dataset_path is not None:
        env["ASAP_FIGURE1_DATASET"] = str(dataset_path)
    proc = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, env=env)
    wall_ns = time.monotonic_ns() - started
    try:
        timing_lines = pathlib.Path(timing_path).read_text().splitlines()
        resources = json.loads(timing_lines[-1])
    finally:
        pathlib.Path(timing_path).unlink(missing_ok=True)
    record = {"name": arm["name"], "command": arm["command"], "exit_code": proc.returncode,
              "measured_resources": {**resources, "wall_seconds": wall_ns / 1e9}}
    if proc.returncode:
        record.update({"status": "failed", "stderr": proc.stderr})
        return record
    result = json.loads(proc.stdout)
    validate_result(arm["name"], result, expected_contract, candidate_space)
    record.update({"status": "completed", "selected_plan": result["selected_plan"],
                   "metrics": result["metrics"], "provenance": result.get("provenance", {})})
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists")
    manifest_path = args.manifest.resolve(); root = manifest_path.parent
    manifest = json.loads(manifest_path.read_text())
    dataset_path, dataset_sha, constraints = validate_manifest(manifest, root)
    candidate_sha = hashlib.sha256(canonical(constraints["candidate_space"]).encode()).hexdigest()
    contract = {"dataset_sha256": dataset_sha, "candidate_space_sha256": candidate_sha,
                "memory_budget_bytes": constraints["memory_budget_bytes"],
                "accuracy": constraints["accuracy"], "window": constraints["window"],
                "erp_observation": constraints["erp_observation"],
                "erp_selection_policy": constraints["erp_selection_policy"]}
    report = {"schema_version": 1, "manifest_sha256": hashlib.sha256(canonical(manifest).encode()).hexdigest(),
              "dataset": {"path": str(dataset_path), "sha256": dataset_sha}, "contract": contract,
              "host": {"uname": list(os.uname())}, "arms": []}
    for arm in manifest["arms"]:
        report["arms"].append(
            run_arm(arm, contract, constraints["candidate_space"], root, dataset_path)
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "x") as sink:
        json.dump(report, sink, indent=2); sink.write("\n")
    if any(row["status"] != "completed" for row in report["arms"]):
        raise SystemExit(1)

if __name__ == "__main__": main()
