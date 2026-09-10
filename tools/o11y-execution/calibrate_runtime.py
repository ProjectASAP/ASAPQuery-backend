#!/usr/bin/env python3
"""Measure unselected candidate artifacts in isolated backend/fallback processes.

This produces calibration observations, never selection overrides. Accelerated
input replay is explicitly distinct from a wall-clock deployment horizon.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import time
import urllib.parse

import replay as runner
from compare import compare_results, process_delta, process_snapshot


def wait_ready(url, child):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise RuntimeError(f"process exited {child.returncode}: {url}")
        if runner._http_request(url)["http_status"] == 200:
            return
        time.sleep(.1)
    raise RuntimeError(f"readiness timeout: {url}")


def snapshots(children):
    return {name: process_snapshot(child.pid) for name, child in children.items()}


def phase(folder, name, before, after, elapsed_ns):
    deltas = {key: process_delta(before.get(key), after.get(key)) for key in before}
    if not all(value is not None for value in deltas.values()):
        raise RuntimeError(f"unreadable process counters in {name}")
    raw = folder / f"phase-{name}.json"
    runner.write_json(raw, {"before": before, "after": after, "deltas": deltas, "wall_ns": elapsed_ns})
    return {"cpu_ns": sum(value["cpu_ns"] for value in deltas.values()), "raw_measurement_file": str(raw.resolve())}


def file_bytes(root):
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def measure(args, artifact, corpus, snapshot, folder):
    folder.mkdir()
    manifest = artifact["manifest"]
    row = {"plan_id": manifest["plan_id"], "manifest": manifest, "executable": False,
           "horizon_seconds": manifest["horizon_seconds"], "horizon_phases": {}, "queries": {}}
    children, logs = {}, []
    cpus = {int(value) for value in args.cpu_affinity.split(",")}
    def limits():
        os.sched_setaffinity(0, cpus)
    def launch(name, command):
        log = (folder / f"{name}.log").open("w")
        logs.append(log)
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, preexec_fn=limits)
        children[name] = child
        return child
    fallback = f"http://127.0.0.1:{args.fallback_port}"
    backend = f"http://127.0.0.1:{args.backend_port}"
    install = folder / "install.json"
    runner.write_json(install, artifact["install_request"])
    config = folder / "prometheus.yml"
    config.write_text('global:\n  scrape_interval: 1h\nscrape_configs: []\n')
    children_cpu_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    try:
        start = time.perf_counter_ns()
        prom = launch("fallback", [str(args.prometheus.resolve()), f"--config.file={config.resolve()}",
                     f"--storage.tsdb.path={(folder / 'prometheus-data').resolve()}",
                     f"--web.listen-address=127.0.0.1:{args.fallback_port}", "--web.enable-remote-write-receiver",
                     "--storage.tsdb.retention.time=1000000h"])
        # Backend startup validates its exact service immediately.
        wait_ready(fallback + "/-/ready", prom)
        dp = launch("backend", [str(args.data_plane.resolve()), "--profile", "asapquery", "--physical-plan", str(install.resolve()),
                    "--prometheus-server", fallback, "--forward-unsupported-queries", "--http-port", str(args.backend_port),
                    "--output-dir", str((folder / "backend-data").resolve()), "--precompute-allowed-lateness-ms", "0",
                    "--precompute-flush-interval-ms", "25"])
        wait_ready(backend + "/api/v1/health", dp)
        after = snapshots(children)
        # Fresh processes: cumulative CPU from exec includes all install/startup work.
        raw = folder / "phase-install.json"
        runner.write_json(raw, {"after": after, "wall_ns": time.perf_counter_ns() - start})
        row["horizon_phases"]["install"] = {"cpu_ns": sum(v["cpu_ns"] for v in after.values()), "raw_measurement_file": str(raw.resolve())}
        before, start = after, time.perf_counter_ns()
        runner.PROCESS_IDS.clear()
        runner.PROCESS_IDS.update({"backend": dp.pid, "fallback_service": prom.pid})
        runner.ingest_sample_file(args.metrics, [fallback, backend], folder)
        drained = runner.request(backend + "/api/v1/precompute/drain", b"")
        runner.write_json(folder / "drain.json", drained)
        if drained["http_status"] != 200 or drained["response"].get("complete") is not True:
            raise RuntimeError("precompute drain did not complete")
        after = snapshots(children)
        row["horizon_phases"]["ingest_and_build"] = phase(folder, "ingest_and_build", before, after, time.perf_counter_ns() - start)
        before, start = after, time.perf_counter_ns()
        time.sleep(args.residency_seconds)
        after = snapshots(children)
        row["horizon_phases"]["residency"] = phase(folder, "residency", before, after, time.perf_counter_ns() - start)
        original_by_id = {f"compat-query-{i}": entry["query"] for i, entry in enumerate(snapshot["query_workload"]["repeating_queries"])}
        for qid in manifest["workload"]:
            occurrences = [q for q in corpus["queries"] if q["query"] == original_by_id[qid]]
            if not occurrences:
                raise RuntimeError(f"no original corpus occurrences for {qid}")
            exact = {}
            for occurrence in occurrences:
                params = urllib.parse.urlencode({"query": occurrence["query"], "time": f'{occurrence["eval_timestamp_ms"] / 1000:.3f}'})
                exact[occurrence["id"]] = runner._http_request(args.reference_url.rstrip("/") + "/api/v1/query?" + params)
            records, before, start = [], snapshots(children), time.perf_counter_ns()
            repeat = 0
            measured_cpu = 0
            while repeat < args.repetitions or (measured_cpu < args.minimum_query_cpu_ns and repeat < args.max_repetitions):
                for occurrence in occurrences:
                    params = urllib.parse.urlencode({"query": occurrence["query"], "time": f'{occurrence["eval_timestamp_ms"] / 1000:.3f}'})
                    answer = runner._http_request(backend + "/api/v1/query?" + params)
                    reference = exact[occurrence["id"]]
                    route = runner.classify(answer["response"], answer["headers"]) if answer["http_status"] == 200 else "failed"
                    comparison = compare_results(answer["response"], reference["response"], args.relative_tolerance, args.absolute_tolerance)
                    records.append({**occurrence, "repetition": repeat, "execution": route,
                                    "execution_provenance": runner.execution_provenance(answer["response"], answer["headers"]), **answer,
                                    "exact": reference, "comparison": comparison})
                repeat += 1
                if repeat % 10 == 0 or repeat >= args.repetitions:
                    current = snapshots(children)
                    measured_cpu = sum(current[key]["cpu_ns"] - before[key]["cpu_ns"] for key in before)
            after = snapshots(children)
            query_phase = phase(folder, "query-" + qid, before, after, time.perf_counter_ns() - start)
            raw = folder / f"queries-{qid}.json"
            runner.write_json(raw, records)
            validate_candidate_topk_execution(candidate, records)
            routes = {record["execution"] for record in records}
            correct = all(record["comparison"]["equal"] and record["exact"]["http_status"] == 200 for record in records)
            row["queries"][qid] = {"cpu_ns": query_phase["cpu_ns"], "evaluations": len(records),
                "classification": next(iter(routes)) if len(routes) == 1 else "mixed", "correct": correct,
                "raw_measurement_file": str(raw.resolve()), "resource_measurement_file": query_phase["raw_measurement_file"],
                "cpu_resolution_censored": query_phase["cpu_ns"] < args.minimum_query_cpu_ns}
        state = runner.request(backend + "/api/v1/store/metrics")
        runner.write_json(folder / "store.json", state)
        final = snapshots(children)
        row["resources"] = {"peak_memory_bytes": sum(v["process_lifetime_peak_rss_bytes"] for v in final.values()),
                            "storage_bytes": file_bytes(folder / "prometheus-data") + file_bytes(folder / "backend-data"),
                            "backend_state": state, "processes": final, "source_scan_bytes": None, "network_bytes": None,
                            "residency_wall_seconds": args.residency_seconds,
                            "scope": "accelerated finite-input replay; no extrapolation of short idle residency to logical data horizon; process HWM sum is conservative"}
        before_total = sum(v["cpu_ns"] for v in final.values())
        for child in children.values():
            child.terminate()
        for child in children.values():
            child.wait(timeout=30)
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        total_cpu = int((usage.ru_utime + usage.ru_stime - children_cpu_before.ru_utime - children_cpu_before.ru_stime) * 1e9)
        retirement = folder / "phase-retirement.json"
        runner.write_json(retirement, {"wait4_children_cpu_ns": total_cpu, "proc_before_retirement_cpu_ns": before_total,
                                      "note": "difference includes /proc tick rounding; nonnegative clamp below tick resolution"})
        row["horizon_phases"]["retirement"] = {"cpu_ns": max(0, total_cpu - before_total), "raw_measurement_file": str(retirement.resolve())}
        invalid = [qid for qid, q in row["queries"].items() if not q["correct"] or q["classification"] not in ("warm", "exact_fallback") or q["cpu_resolution_censored"]]
        if invalid:
            row["unavailable_reason"] = "failed/mixed/incorrect or CPU below measurement resolution: " + ",".join(invalid)
        else:
            row["executable"] = True
    except Exception as error:
        row["unavailable_reason"] = str(error)
    finally:
        for child in children.values():
            if child.poll() is None:
                child.kill()
                child.wait()
        for log in logs:
            log.close()
        runner.PROCESS_IDS.clear()
        runner.write_json(folder / "measurement.json", row)
    return row



def _candidate_topk_inputs(nodes, root):
    bindings, visiting, visited = set(), set(), set()
    def visit(node_id):
        node_id = str(node_id)
        if node_id in visiting:
            raise ValueError("CandidateTopK input DAG contains a cycle")
        if node_id in visited:
            return
        if node_id not in nodes:
            raise ValueError(f"CandidateTopK input DAG references missing node {node_id}")
        visiting.add(node_id)
        node = nodes[node_id]
        if node.get("op") == "exact_fallback":
            raise ValueError("CandidateTopK input contains ExactFallback")
        if node.get("op") == "read_materialization":
            bindings.add(str(node["binding"]["materialization"]))
        children = [str(value) for value in node.get("inputs", [])]
        if "input" in node:
            children.append(str(node["input"]))
        for child in children:
            visit(child)
        visiting.remove(node_id)
        visited.add(node_id)
    visit(root)
    return bindings


def validate_candidate_topk_artifact(artifact):
    """Reject CandidateTopK plans whose membership sidecar is not locally installed."""
    request = artifact.get("install_request", {})
    schemas = {str(row["materialization"]): row for row in request.get("precompute_plan", {}).get("schemas", [])}
    for entry in request.get("query_plan", {}).get("entries", {}).values():
        nodes = entry.get("nodes", {})
        for node in nodes.values():
            if node.get("op") != "candidate_top_k":
                continue
            inputs = node.get("inputs", [])
            if len(inputs) != 2:
                raise ValueError("CandidateTopK requires membership and exact-value inputs")
            membership_bindings = _candidate_topk_inputs(nodes, inputs[0])
            value_bindings = _candidate_topk_inputs(nodes, inputs[1])
            heap_bindings = []
            for materialization in membership_bindings:
                schema = schemas.get(materialization)
                family = json.dumps((schema or {}).get("family", {}), sort_keys=True)
                if "CmsWithHeap" in family or "CountSketchWithHeap" in family:
                    heap_bindings.append(materialization)
            if not heap_bindings:
                raise ValueError("CandidateTopK membership input has no installed heap materialization")
            if not any("exact" in json.dumps((schemas.get(mid) or {}).get("family", {})).lower()
                       and any(kind in json.dumps((schemas.get(mid) or {}).get("family", {})).lower()
                               for kind in ("counter", "rate", "increase"))
                       for mid in value_bindings):
                raise ValueError("CandidateTopK value input has no installed ExactCounter materialization")


def validate_candidate_topk_execution(artifact, records):
    has_candidate_topk = any(node.get("op") == "candidate_top_k"
        for entry in artifact.get("install_request", {}).get("query_plan", {}).get("entries", {}).values()
        for node in entry.get("nodes", {}).values())
    if not has_candidate_topk:
        return
    for record in records:
        provenance = record.get("execution_provenance", {})
        if record.get("execution") != "warm":
            raise ValueError("CandidateTopK execution was not warm")
        for key in ("exact_subquery_rpcs", "exact_subquery_evaluations", "exact_branch_evaluations"):
            if provenance.get(key, 0) != 0:
                raise ValueError(f"CandidateTopK execution used exact path: {key}")
        if provenance.get("summary_readout_evaluations", 0) < 2:
            raise ValueError("CandidateTopK execution did not read both summary branches")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["candidates", "metrics", "queries", "snapshot", "data-plane", "prometheus", "output"]:
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--reference-url", required=True, help="separate already-loaded exact Prometheus using identical input")
    parser.add_argument("--cpu-affinity", required=True)
    parser.add_argument("--backend-port", type=int, default=19210)
    parser.add_argument("--fallback-port", type=int, default=19211)
    parser.add_argument("--repetitions", type=int, default=50)
    parser.add_argument("--max-repetitions", type=int, default=1000)
    parser.add_argument("--minimum-query-cpu-ns", type=int, default=100000000, help="ten Linux 100Hz CPU ticks by default")
    parser.add_argument("--residency-seconds", type=float, default=1)
    parser.add_argument("--relative-tolerance", type=float, default=0.0)
    parser.add_argument("--absolute-tolerance", type=float, default=0.0)
    args = parser.parse_args()
    if args.repetitions < 1 or args.max_repetitions < args.repetitions or args.minimum_query_cpu_ns <= 0 or args.residency_seconds < 0:
        parser.error("positive repetitions and nonnegative residency required")
    args.output.mkdir(parents=True, exist_ok=False)
    sample_count = runner.validate_sample_file(args.metrics)
    corpus, snapshot = json.loads(args.queries.read_text()), json.loads(args.snapshot.read_text())
    runner.validate_workload(snapshot, corpus)
    result = {"units": "cpu_ns", "data_snapshot_id": "sha256:" + hashlib.sha256(args.metrics.read_bytes()).hexdigest(),
              "scope": "accelerated finite-input calibration; measured wall residency is not full logical-horizon residency", "validated_sample_count": sample_count, "candidates": []}
    candidates = json.loads(args.candidates.read_text())["candidates"]
    for candidate in candidates:
        if "manifest" in candidate and "install_request" in candidate:
            validate_candidate_topk_artifact(candidate)
    for index, candidate in enumerate(candidates):
        if "manifest" not in candidate or "install_request" not in candidate:
            continue
        result["candidates"].append(measure(args, candidate, corpus, snapshot, args.output / f"candidate-{index}"))
        runner.write_json(args.output / "measurements.json", result)


if __name__ == "__main__":
    main()
