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
    for key in ("candidate_space", "memory_budget_bytes", "accuracy", "window"):
        if key not in constraints:
            raise ValueError(f"missing common constraint {key}")
    if constraints["memory_budget_bytes"] <= 0 or not constraints["candidate_space"]:
        raise ValueError("candidate space and memory budget must be non-empty/positive")
    arms = manifest["arms"]
    if [arm.get("name") for arm in arms] != ARM_NAMES:
        raise ValueError(f"arms must be ordered exactly as {ARM_NAMES}")
    if any(not isinstance(arm.get("command"), list) or not arm["command"] for arm in arms):
        raise ValueError("every arm requires a non-empty argv command")
    return path, actual, constraints


def run_arm(arm, expected_contract, cwd, dataset_path=None):
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
        resources = json.loads(pathlib.Path(timing_path).read_text())
    finally:
        pathlib.Path(timing_path).unlink(missing_ok=True)
    record = {"name": arm["name"], "command": arm["command"], "exit_code": proc.returncode,
              "measured_resources": {**resources, "wall_seconds": wall_ns / 1e9}}
    if proc.returncode:
        record.update({"status": "failed", "stderr": proc.stderr})
        return record
    result = json.loads(proc.stdout)
    if result.get("contract") != expected_contract:
        raise ValueError(f"{arm['name']} did not execute the identical evaluation contract")
    missing = REQUIRED_METRICS - set(result.get("metrics", {}))
    if missing or "selected_plan" not in result:
        raise ValueError(f"{arm['name']} result missing selected_plan/metrics: {sorted(missing)}")
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
                "accuracy": constraints["accuracy"], "window": constraints["window"]}
    report = {"schema_version": 1, "manifest_sha256": hashlib.sha256(canonical(manifest).encode()).hexdigest(),
              "dataset": {"path": str(dataset_path), "sha256": dataset_sha}, "contract": contract,
              "host": {"uname": list(os.uname())}, "arms": []}
    for arm in manifest["arms"]:
        report["arms"].append(run_arm(arm, contract, root, dataset_path))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "x") as sink:
        json.dump(report, sink, indent=2); sink.write("\n")
    if any(row["status"] != "completed" for row in report["arms"]):
        raise SystemExit(1)

if __name__ == "__main__": main()
