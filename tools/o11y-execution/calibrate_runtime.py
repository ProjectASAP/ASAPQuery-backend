#!/usr/bin/env python3
"""Measure unselected candidate artifacts in isolated backend/fallback processes.

This produces calibration observations, never selection overrides. Accelerated
input replay is explicitly distinct from a wall-clock deployment horizon.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import subprocess
import time
import urllib.parse

import replay as runner
from compare import compare_results, process_delta, process_snapshot


def input_inventory(path):
    counts, last = {}, {}
    first = None
    with path.open() as stream:
        for labels, _, timestamp in runner.iter_samples(stream):
            key = tuple(sorted(labels.items()))
            counts[key] = counts.get(key, 0) + 1
            last[key] = max(last.get(key, timestamp), timestamp)
            first = timestamp if first is None else min(first, timestamp)
    return counts, last, first, max(last.values())


def verify_vm_visibility(url, inventory, folder):
    counts, last, first, end = inventory
    expected = [counts, {key: timestamp / 1000 for key, timestamp in last.items()}]
    attempts = []
    deadline = time.monotonic() + 60
    while True:
        valid = True
        for index, function in enumerate(("count_over_time", "tlast_over_time")):
            query = function + '({__name__!=""}[' + str(end - first + 1) + 'ms]) keep_metric_names'
            response = runner._http_request(url + "/api/v1/query?" + urllib.parse.urlencode({"query":query,"time":end/1000,"nocache":1}))
            rows = response["response"].get("data", {}).get("result", [])
            actual = {tuple(sorted(row["metric"].items())): float(row["value"][1]) for row in rows}
            matched = response["http_status"] == 200 and actual == expected[index]
            attempts.append({"query":query,"matched":matched,**response})
            valid = valid and matched
        if valid or time.monotonic() >= deadline:
            runner.write_json(folder / "exact-visibility.json", {"complete":valid,"attempts":attempts})
            if not valid:
                raise RuntimeError("VictoriaMetrics input counts/last timestamps are incomplete")
            return
        time.sleep(.1)


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


def exact_service_command(args, folder, port):
    if getattr(args, "victoriametrics", None):
        command = [str(args.victoriametrics.resolve()),
                f"-storageDataPath={(folder / 'exact-data').resolve()}",
                f"-httpListenAddr=127.0.0.1:{port}", "-retentionPeriod=1y",
                f"-memory.allowedBytes={args.exact_cache_bytes}"]
        if args.disable_result_cache:
            command.append("-search.disableCache")
        return command
    config = folder / "prometheus.yml"
    config.write_text('global:\n  scrape_interval: 1h\nscrape_configs: []\n')
    return [str(args.prometheus.resolve()), f"--config.file={config.resolve()}",
            f"--storage.tsdb.path={(folder / 'exact-data').resolve()}",
            f"--web.listen-address=127.0.0.1:{port}", "--web.enable-remote-write-receiver",
            "--storage.tsdb.retention.time=1000000h"]


def query_parameters(occurrence, disable_cache):
    params = {"query": occurrence["query"], "time": f'{occurrence["eval_timestamp_ms"] / 1000:.3f}'}
    if disable_cache:
        params["nocache"] = "1"
    return urllib.parse.urlencode(params)


def comparison_tolerances(occurrence, args):
    contract = occurrence.get("accuracy_validation")
    if contract is None:
        return args.relative_tolerance, args.absolute_tolerance
    bound = contract.get("bound")
    if not isinstance(bound, (float, int)) or not math.isfinite(bound) or bound < 0:
        raise ValueError("invalid per-query accuracy bound")
    metric = contract.get("metric")
    if metric == "relative":
        return bound, 0.0
    if metric == "absolute_bits":
        return 0.0, bound
    if metric == "exact" and bound == 0:
        return 0.0, 0.0
    raise ValueError("unsupported per-query accuracy metric")


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
    query_backend = f"http://127.0.0.1:{args.metricsql_port}" if args.victoriametrics else backend
    children_cpu_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    try:
        start = time.perf_counter_ns()
        prom = launch("fallback", exact_service_command(args, folder, args.fallback_port))
        wait_ready(fallback + ("/health" if args.victoriametrics else "/-/ready"), prom)
        command = [str(args.data_plane.resolve()), "--profile", "asapquery", "--physical-plan", str(install.resolve()),
                   "--prometheus-server", fallback, "--forward-unsupported-queries", "--http-port", str(args.backend_port),
                   "--output-dir", str((folder / "backend-data").resolve()), "--precompute-allowed-lateness-ms", "0",
                   "--precompute-flush-interval-ms", "25"]
        if args.victoriametrics:
            command += ["--victoriametrics-url", fallback, "--victoriametrics-http-port", str(args.metricsql_port)]
        dp = launch("backend", command)
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
        if args.victoriametrics:
            # Finite replay barrier, charged to build: imported samples may still
            # be buffered and invisible to queries after the write is accepted.
            flushed = runner._http_request(fallback + "/internal/force_flush")
            runner.write_json(folder / "exact-flush.json", flushed)
            if flushed["http_status"] != 200:
                raise RuntimeError("VictoriaMetrics finite replay flush failed")
            verify_vm_visibility(fallback, args.input_inventory, folder)
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
            records, before, start = [], snapshots(children), time.perf_counter_ns()
            repeat = 0
            measured_cpu = 0
            while repeat < args.repetitions or (measured_cpu < args.minimum_query_cpu_ns and repeat < args.max_repetitions):
                for occurrence in occurrences:
                    params = query_parameters(occurrence, args.disable_result_cache)
                    answer = runner._http_request(query_backend + "/api/v1/query?" + params)
                    if occurrence["id"] not in exact:
                        exact[occurrence["id"]] = runner._http_request(args.reference_url.rstrip("/") + "/api/v1/query?" + params)
                    reference = exact[occurrence["id"]]
                    route = runner.classify(answer["response"], answer["headers"]) if answer["http_status"] == 200 else "failed"
                    comparison = compare_results(answer["response"], reference["response"], *comparison_tolerances(occurrence, args))
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
            validate_candidate_topk_execution(artifact, records)
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
                            "storage_bytes": file_bytes(folder / "exact-data") + file_bytes(folder / "backend-data"),
                            "backend_state": state, "processes": final, "source_scan_bytes": None, "network_bytes": None,
                            "residency_wall_seconds": args.residency_seconds,
                            "scope": "accelerated finite-input replay; no extrapolation of short idle residency to logical data horizon; process HWM sum is conservative"}
        before_total = sum(v["cpu_ns"] for v in final.values())
        for child in children.values():
            child.terminate()
        for child in children.values():
            child.wait(timeout=None if args.wait_for_completion else 30)
        row["resources"]["storage_before_shutdown_bytes"] = row["resources"]["storage_bytes"]
        row["resources"]["storage_after_shutdown"] = {
            "exact_bytes": file_bytes(folder / "exact-data"),
            "backend_bytes": file_bytes(folder / "backend-data"),
        }
        row["resources"]["storage_bytes"] = sum(row["resources"]["storage_after_shutdown"].values())
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        total_cpu = int((usage.ru_utime + usage.ru_stime - children_cpu_before.ru_utime - children_cpu_before.ru_stime) * 1e9)
        retirement = folder / "phase-retirement.json"
        runner.write_json(retirement, {"wait4_children_cpu_ns": total_cpu, "proc_before_retirement_cpu_ns": before_total,
                                      "note": "difference includes /proc tick rounding; nonnegative clamp below tick resolution"})
        row["horizon_phases"]["retirement"] = {"cpu_ns": max(0, total_cpu - before_total), "raw_measurement_file": str(retirement.resolve())}
        invalid = [qid for qid, q in row["queries"].items() if not q["correct"] or q["classification"] not in ("warm", "hybrid", "exact_fallback") or q["cpu_resolution_censored"]]
        if invalid:
            row["unavailable_reason"] = "failed/mixed/incorrect or CPU below measurement resolution: " + ",".join(invalid)
        else:
            row["executable"] = True
    except Exception as error:
        row["unavailable_reason"] = str(error)
    finally:
        for child in children.values():
            if child.poll() is None:
                if args.wait_for_completion:
                    child.terminate()
                else:
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
    modes = set()
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
            value_node = nodes.get(str(inputs[1]), {})
            operator = value_node.get("operator", {}) if value_node.get("op") == "logical" else {}
            if operator.get("kind") == "candidate_exact_subquery":
                modes.add("candidate_filtered_exact")
                if value_node.get("inputs") != [inputs[0]]:
                    raise ValueError("candidate exact input must be the shared membership node")
                if not operator.get("query") or not operator.get("item_label"):
                    raise ValueError("candidate exact operator lacks query or item label")
                heap_families = [json.dumps(row.get("family", {}), sort_keys=True)
                                 for row in schemas.values()]
                if sum("CmsWithHeap" in family or "CountSketchWithHeap" in family
                       for family in heap_families) != 1 or len(heap_bindings) != 1:
                    raise ValueError("candidate-filtered TopK requires exactly one installed heap materialization")
                if any("exact" in family.lower() and any(kind in family.lower()
                       for kind in ("counter", "rate", "increase")) for family in heap_families):
                    raise ValueError("candidate-filtered TopK must not install an ExactCounter materialization")
            else:
                modes.add("local_exact")
                if not any("exact" in json.dumps((schemas.get(mid) or {}).get("family", {})).lower()
                           and any(kind in json.dumps((schemas.get(mid) or {}).get("family", {})).lower()
                                   for kind in ("counter", "rate", "increase"))
                           for mid in value_bindings):
                    raise ValueError("CandidateTopK value input has no installed ExactCounter materialization")
    return modes


def validate_candidate_topk_execution(artifact, records):
    modes = validate_candidate_topk_artifact(artifact)
    if not modes:
        return
    if len(modes) != 1:
        raise ValueError("mixed CandidateTopK execution contracts are not calibratable together")
    mode = next(iter(modes))
    for record in records:
        provenance = record.get("execution_provenance", {})
        if provenance.get("raw_scan_evaluations", 0) not in (0, None):
            raise ValueError("CandidateTopK execution used a forbidden local raw scan")
        if mode == "candidate_filtered_exact":
            if record.get("execution") != "hybrid" or provenance.get("detail") != "hybrid":
                raise ValueError("candidate-filtered TopK did not report hybrid execution")
            expected = {"summary_readout_evaluations": 1, "exact_subquery_rpcs": 1,
                        "exact_subquery_evaluations": 1, "exact_branch_evaluations": 1}
            for key, value in expected.items():
                if provenance.get(key) != value:
                    raise ValueError(f"candidate-filtered TopK has invalid provenance: {key}")
        else:
            if record.get("execution") != "warm" or provenance.get("detail") not in (None, "asap"):
                raise ValueError("local CandidateTopK execution was not warm")
            for key in ("exact_subquery_rpcs", "exact_subquery_evaluations", "exact_branch_evaluations"):
                if provenance.get(key, 0) != 0:
                    raise ValueError(f"CandidateTopK execution used exact path: {key}")
            if provenance.get("summary_readout_evaluations", 0) < 2:
                raise ValueError("CandidateTopK execution did not read both summary branches")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["candidates", "metrics", "queries", "snapshot", "data-plane", "output"]:
        parser.add_argument("--" + name, type=Path, required=True)
    engines = parser.add_mutually_exclusive_group(required=True)
    engines.add_argument("--prometheus", type=Path)
    engines.add_argument("--victoriametrics", type=Path)
    parser.add_argument("--metricsql-port", type=int, default=19212)
    parser.add_argument("--exact-cache-bytes", type=int, default=268435456, help="VictoriaMetrics cache budget, not an RSS limit")
    parser.add_argument("--disable-result-cache", action="store_true", help="send nocache=1 to both query endpoints")
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
    parser.add_argument("--wait-for-completion", action="store_true", help="wait without client deadlines or forced shutdown kills")
    args = parser.parse_args()
    runner.HTTP_TIMEOUT = None if args.wait_for_completion else 60
    if args.repetitions < 1 or args.max_repetitions < args.repetitions or args.minimum_query_cpu_ns <= 0 or args.residency_seconds < 0:
        parser.error("positive repetitions and nonnegative residency required")
    if len({args.backend_port, args.fallback_port, args.metricsql_port}) != 3 or args.exact_cache_bytes <= 0:
        parser.error("distinct listener ports and positive exact cache budget required")
    args.output.mkdir(parents=True, exist_ok=False)
    args.input_inventory = input_inventory(args.metrics) if args.victoriametrics else None
    sample_count = sum(args.input_inventory[0].values()) if args.input_inventory else runner.validate_sample_file(args.metrics)
    corpus, snapshot = json.loads(args.queries.read_text()), json.loads(args.snapshot.read_text())
    runner.validate_workload(snapshot, corpus)
    candidate_document = json.loads(args.candidates.read_text())
    result = {"units": "cpu_ns", "compiler_identity": candidate_document.get("compiler_identity"), "data_snapshot_id": "sha256:" + hashlib.sha256(args.metrics.read_bytes()).hexdigest(),
              "runtime_binary_sha256": hashlib.sha256(args.data_plane.read_bytes()).hexdigest(),
              "exact_binary_sha256": hashlib.sha256((args.victoriametrics or args.prometheus).read_bytes()).hexdigest(),
              "exact_engine": "victoriametrics" if args.victoriametrics else "prometheus", "result_cache_disabled": args.disable_result_cache,
              "scope": "accelerated finite-input calibration; measured wall residency is not full logical-horizon residency", "validated_sample_count": sample_count, "candidates": []}
    candidates = candidate_document["candidates"]
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
