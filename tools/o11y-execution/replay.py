#!/usr/bin/env python3
"""Replay supplied o11ybench inputs through the production backend. No winner overrides."""
import argparse
from decimal import Decimal
import hashlib
import json
import math
import os
import resource
from pathlib import Path
import re
import socket
import struct
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

from compare import compare_results, process_snapshot, process_delta, summarize

PROCESS_IDS = {}


def constrain_process(pid, cpus, address_space_bytes=None):
    """Match schedulable CPUs for all existing threads, including Go workers."""
    if cpus:
        for task in Path(f"/proc/{pid}/task").iterdir():
            try:
                os.sched_setaffinity(int(task.name), cpus)
            except ProcessLookupError:
                pass
    if address_space_bytes:
        resource.prlimit(pid, resource.RLIMIT_AS, (address_space_bytes, address_space_bytes))


def classify(response, headers=None):
    if response.get("status") != "success":
        return "failed"
    declared = (headers or {}).get("x-asap-execution")
    if declared in ("exact_fallback", "failed"):
        return declared
    detail = (headers or {}).get("x-asap-execution-detail")
    if detail in ("hybrid", "local_raw"):
        return "exact_fallback"
    if detail == "invalid_provenance":
        return "failed"
    sources = {x for x in response.get("infos", []) if isinstance(x, str) and x.startswith("data_source:")}
    if sources == {"data_source: asap_query"}:
        return "warm"
    if sources == {"data_source: exact_fallback"}:
        return "exact_fallback"
    return "failed"


def execution_provenance(response, headers=None):
    headers = headers or {}
    route = classify(response, headers)
    detail = headers.get("x-asap-execution-detail")
    if detail is None:
        detail = "asap" if route == "warm" else "external_exact" if route == "exact_fallback" else "failed"
    counts = {}
    for name, header in (("raw_scan_evaluations", "x-asap-raw-scan-evaluations"),
                         ("summary_readout_evaluations", "x-asap-summary-readout-evaluations"),
                         ("memo_hits", "x-asap-memo-hits")):
        value = headers.get(header)
        counts[name] = int(value) if value is not None and value.isdigit() else None
    return {"detail": detail, **counts}


def validate_workload(snapshot, corpus):
    rows = corpus["queries"]
    if not corpus.get("upstream_revision") or not rows:
        raise ValueError("a versioned, nonempty upstream query corpus is required")
    ids = [row["id"] for row in rows]
    if len(set(ids)) != len(ids):
        raise ValueError("query occurrence IDs must be unique")
    for row in rows:
        if not isinstance(row["eval_timestamp_ms"], int) or row["eval_timestamp_ms"] < 0:
            raise ValueError("each query needs its original nonnegative evaluation timestamp")
    workload = snapshot["query_workload"]
    if workload.get("query_batch"):
        raise ValueError("this runner currently accepts repeating workload registrations only")
    registered = {row["query"] for row in workload["repeating_queries"]}
    if registered != {row["query"] for row in rows}:
        raise ValueError("snapshot registrations must match the complete unique corpus exactly")
    return rows


_SAMPLE = re.compile(r'([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)\s+(\d+(?:\.\d+)?)')
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\[\\"n])*)"')


def parse_samples(lines):
    """Strict OpenMetrics subset: seconds converted losslessly to Remote Write milliseconds."""
    rows, seen, latest = [], {}, -1
    for number, line in enumerate(lines, 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = _SAMPLE.fullmatch(line)
        if not match:
            raise ValueError(f"unsupported sample at line {number}")
        metric, raw_labels, value, timestamp = match.groups()
        labels = {"__name__": metric}
        rest = raw_labels or ""
        while rest:
            label = _LABEL.match(rest)
            if not label or label[1] in labels:
                raise ValueError(f"invalid or duplicate label at line {number}")
            labels[label[1]] = re.sub(r'\\([\\"n])', lambda m: '\n' if m[1] == 'n' else m[1], label[2])
            rest = rest[label.end():]
            if rest:
                if not rest.startswith(",") or len(rest) == 1:
                    raise ValueError(f"invalid label separator at line {number}")
                rest = rest[1:]
        millis = Decimal(timestamp) * 1000
        if millis != millis.to_integral_value():
            raise ValueError(f"submillisecond timestamp at line {number}")
        value, timestamp = float(value), int(millis)
        key = tuple(sorted(labels.items()))
        if not math.isfinite(value) or timestamp > 2**63 - 1 or timestamp < latest or timestamp <= seen.get(key, -1):
            raise ValueError(f"nonfinite, duplicate, or out-of-order sample at line {number}")
        latest, seen[key] = timestamp, timestamp
        rows.append((labels, value, timestamp))
    if not rows:
        raise ValueError("empty dataset")
    return rows


def varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def field(number, payload):
    return varint(number * 8 + 2) + varint(len(payload)) + payload


def encode_write(rows):
    """Remote Write v1 protobuf in an uncompressed-literal raw Snappy block."""
    series = {}
    for labels, value, timestamp in rows:
        series.setdefault(tuple(sorted(labels.items())), []).append((value, timestamp))
    wire = bytearray()
    for labels, samples in series.items():
        ts = b"".join(field(1, field(1, k.encode()) + field(2, v.encode())) for k, v in labels)
        ts += b"".join(field(2, b"\x09" + struct.pack("<d", v) + b"\x10" + varint(t)) for v, t in samples)
        wire.extend(field(1, ts))
    length = len(wire)
    if not length:
        raise ValueError("cannot encode empty batch")
    n = length - 1
    if n < 60:
        literal = bytes([n << 2])
    else:
        size = (n.bit_length() + 7) // 8
        literal = bytes([(59 + size) << 2]) + n.to_bytes(size, "little")
    return varint(length) + literal + wire


def _http_request(url, data=None, headers=None):
    start = time.perf_counter_ns()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers or {}), timeout=60) as response:
            body, status, received = response.read(), response.status, dict(response.headers.items())
        try:
            body = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            body = {"raw": body.decode(errors="replace")}
        return {"http_status": status, "response": body, "headers": {k.lower(): v for k, v in received.items()},
                "elapsed_ns": time.perf_counter_ns() - start}
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            body = {"status": "error", "error": str(error), "raw": raw.decode(errors="replace")}
        return {"http_status": error.code, "response": body,
                "headers": {k.lower(): v for k, v in error.headers.items()},
                "elapsed_ns": time.perf_counter_ns() - start}
    except (OSError, urllib.error.URLError) as error:
        return {"http_status": getattr(error, "code", None), "response": {"status": "error", "error": str(error)},
                "headers": {}, "elapsed_ns": time.perf_counter_ns() - start}


def process_snapshots():
    return {name: process_snapshot(pid) for name, pid in PROCESS_IDS.items()}


def request(url, data=None, headers=None):
    before = process_snapshots()
    result = _http_request(url, data, headers)
    after = process_snapshots()
    result["process_resources"] = {name: process_delta(before.get(name), after.get(name)) for name in PROCESS_IDS}
    return result


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def ingest(rows, endpoints, output):
    batches = []
    for offset in range(0, len(rows), 5000):
        payload = encode_write(rows[offset:offset + 5000])
        batch = {"offset": offset, "sample_count": len(rows[offset:offset + 5000]),
                 "payload_sha256": hashlib.sha256(payload).hexdigest(), "endpoints": {}}
        batches.append(batch)
        for endpoint in endpoints:
            # Persist intent first: a timeout can follow partial acceptance. Never retry silently.
            batch["endpoints"][endpoint] = {"status": "attempting"}
            write_json(output / "ingestion.json", batches)
            result = request(endpoint.rstrip("/") + "/api/v1/write", payload, {
                "Content-Type": "application/x-protobuf", "Content-Encoding": "snappy",
                "X-Prometheus-Remote-Write-Version": "0.1.0"})
            batch["endpoints"][endpoint] = result
            write_json(output / "ingestion.json", batches)
            if not result["http_status"] or not 200 <= result["http_status"] < 300:
                raise RuntimeError(f"ingestion failed at batch {offset}; inspect partial acceptance before retry")


def replay(queries, backend, output, repetitions, exact_url=None, relative_tolerance=0.0, absolute_tolerance=0.0):
    rows = []
    for repeat in range(repetitions):
        for query_index, query in enumerate(queries):
            exact_first = (repeat + query_index) % 2 == 0
            params = urllib.parse.urlencode({"query": query["query"], "time": f'{query["eval_timestamp_ms"] / 1000:.3f}'})
            # Alternate paired order to expose, rather than always favor, cache/order effects.
            exact = None
            if exact_url and exact_first:
                exact = request(exact_url.rstrip("/") + "/api/v1/query?" + params)
            answer = request(backend.rstrip("/") + "/api/v1/query?" + params)
            if exact_url and exact is None:
                exact = request(exact_url.rstrip("/") + "/api/v1/query?" + params)
            route = classify(answer["response"], answer["headers"])
            if answer["http_status"] != 200:
                route = "failed"
            rows.append({**query, "repetition": repeat, "phase": "first_pass" if repeat == 0 else "repeat",
                         "execution": route, "execution_provenance": execution_provenance(answer["response"], answer["headers"]), **answer})
            if exact is not None:
                rows[-1]["exact"] = exact
                rows[-1]["comparison"] = compare_results(answer["response"], exact["response"], relative_tolerance, absolute_tolerance)
                rows[-1]["pair_order"] = "exact_first" if exact_first else "backend_first"
            write_json(output / "queries.json", rows)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--compiler", type=Path, required=True)
    parser.add_argument("--data-plane", type=Path, required=True)
    parser.add_argument("--exact-url", required=True, help="dedicated empty Prometheus with Remote Write receiver enabled")
    parser.add_argument("--compare", action="store_true", help="execute a matched exact request for every corpus occurrence")
    parser.add_argument("--exact-pid", type=int, help="local Prometheus PID for Linux CPU/RSS evidence; never stopped by this runner")
    parser.add_argument("--exact-storage", type=Path, help="baseline Prometheus data directory for logical on-disk byte count")
    parser.add_argument("--fallback-storage", type=Path, help="fallback Prometheus data directory for logical on-disk byte count")
    parser.add_argument("--fallback-url", help="separate fresh Prometheus for backend fallback; defaults to exact-url")
    parser.add_argument("--fallback-pid", type=int)
    parser.add_argument("--cpu-affinity", help="comma-separated permitted CPU IDs; enforced on backend and supplied Prometheus PIDs")
    parser.add_argument("--address-space-bytes", type=int, help="same RLIMIT_AS for backend and supplied Prometheus; virtual memory, not RSS cap")
    parser.add_argument("--port", type=int, default=18089)
    parser.add_argument("--settle-seconds", type=float, default=0, help="deprecated; readiness uses explicit precompute drain")
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--relative-tolerance", type=float, default=0.0)
    parser.add_argument("--absolute-tolerance", type=float, default=0.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.repetitions < 1 or not 0 <= args.settle_seconds <= 60:
        parser.error("positive repetitions and settle-seconds in [0, 60] required")
    cpus = {int(x) for x in args.cpu_affinity.split(",")} if args.cpu_affinity else None
    if cpus and not cpus <= os.sched_getaffinity(0):
        parser.error("requested CPUs must be available to the runner")
    if args.address_space_bytes is not None and args.address_space_bytes <= 0:
        parser.error("address-space-bytes must be positive")
    if (cpus or args.address_space_bytes) and not args.exact_pid:
        parser.error("enforced resource comparison requires --exact-pid")
    if args.fallback_url and args.fallback_url.rstrip("/") == args.exact_url.rstrip("/"):
        parser.error("fallback-url must be a separate service")
    if args.fallback_url and (cpus or args.address_space_bytes) and not args.fallback_pid:
        parser.error("resource enforcement also requires --fallback-pid")
    fallback_url = args.fallback_url or args.exact_url
    for pid in [args.exact_pid, args.fallback_pid]:
        if pid is not None:
            if process_snapshot(pid) is None:
                parser.error("service PID must be readable and live")
            constrain_process(pid, cpus, args.address_space_bytes)
    corpus = json.loads(args.queries.read_text())
    queries = validate_workload(json.loads(args.snapshot.read_text()), corpus)
    samples = parse_samples(args.metrics.read_text().splitlines())
    if args.exact_pid is not None and process_snapshot(args.exact_pid) is None:
        parser.error("exact-pid must name a readable live local process")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", args.port))
    args.output.mkdir(parents=True, exist_ok=False)
    provenance = {"schema_version": 1, "upstream_revision": corpus["upstream_revision"],
                  "inputs": {str(p.resolve()): hashlib.sha256(p.read_bytes()).hexdigest()
                             for p in [args.metrics, args.queries, args.snapshot, args.compiler, args.data_plane]},
                  "samples": len(samples), "timestamp_min_ms": samples[0][2], "timestamp_max_ms": samples[-1][2],
                  "query_occurrences": len(queries), "configuration": {k: str(v) for k, v in vars(args).items()},
                  "limitations": ["generator provenance must be supplied separately", "first pass is not a guaranteed cold cache", "finite-input drain closes trailing partial windows; query correctness is checked against Prometheus"]}
    write_json(args.output / "run.json", provenance)
    planning_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    with (args.output / "planning.stderr").open("w") as log:
        compiled = subprocess.run([str(args.compiler.resolve()), str(args.snapshot.resolve())], check=True,
                                  stdout=subprocess.PIPE, stderr=log, text=True)
    planning_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    planning_resources = {"cpu_ns": int(((planning_after.ru_utime + planning_after.ru_stime) -
                                         (planning_before.ru_utime + planning_before.ru_stime)) * 1e9),
                          "children_lifetime_peak_rss_bytes": planning_after.ru_maxrss * 1024}
    plan = json.loads(compiled.stdout)
    write_json(args.output / "planning.json", plan)
    artifact = args.output / "install.json"
    write_json(artifact, plan["install_request"])
    backend = f"http://127.0.0.1:{args.port}"
    command = [str(args.data_plane.resolve()), "--profile", "asapquery", "--physical-plan", str(artifact.resolve()),
               "--prometheus-server", fallback_url, "--forward-unsupported-queries", "--http-port", str(args.port),
               "--output-dir", str((args.output / "backend").resolve()), "--precompute-allowed-lateness-ms", "0",
               "--precompute-flush-interval-ms", "25"]
    write_json(args.output / "command.json", command)
    with (args.output / "backend.log").open("w") as log:
        def limits():
            if cpus:
                os.sched_setaffinity(0, cpus)
            if args.address_space_bytes:
                resource.setrlimit(resource.RLIMIT_AS, (args.address_space_bytes, args.address_space_bytes))
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, preexec_fn=limits)
        PROCESS_IDS["backend"] = child.pid
        if args.exact_pid is not None:
            PROCESS_IDS["exact_service"] = args.exact_pid
        if args.fallback_pid is not None:
            PROCESS_IDS["fallback_service"] = args.fallback_pid
        phases = {"startup": process_snapshots()}
        try:
            for _ in range(120):
                if child.poll() is not None:
                    raise RuntimeError("data plane exited; see backend.log")
                if request(backend + "/api/v1/health")["http_status"] == 200:
                    break
                time.sleep(0.25)
            else:
                raise RuntimeError("data plane readiness timeout")
            installed = request(backend + "/api/v1/physical-plan/status")
            write_json(args.output / "installed.json", installed)
            if installed["http_status"] != 200:
                raise RuntimeError("could not read installed plan status")
            envelope = plan["envelope"]
            if not any(p["plan_id"] == envelope["plan_id"] and p["plan_version"] == envelope["plan_version"]
                       and p["phase"] == "active" for p in installed["response"].get("plans", [])):
                raise RuntimeError("runtime has not activated the selected plan generation")
            phases["before_ingest"] = process_snapshots()
            ingest_start = time.perf_counter_ns()
            ingest(samples, list(dict.fromkeys([args.exact_url, fallback_url, backend])), args.output)
            drained = request(backend + "/api/v1/precompute/drain", b"")
            write_json(args.output / "drain.json", drained)
            if drained["http_status"] != 200 or drained["response"].get("complete") is not True:
                raise RuntimeError("finite-input materialization drain failed; see drain.json")
            ingest_elapsed = time.perf_counter_ns() - ingest_start
            phases["after_ingest_and_settle"] = process_snapshots()
            write_json(args.output / "store-after-build.json", request(backend + "/api/v1/store/metrics"))
            write_json(args.output / "process-phases.json", phases)
            results = replay(queries, backend, args.output, args.repetitions, args.exact_url if args.compare else None,
                             args.relative_tolerance, args.absolute_tolerance)
            phases["after_queries"] = process_snapshots()
            write_json(args.output / "process-phases.json", phases)
            store = request(backend + "/api/v1/store/metrics")
            write_json(args.output / "store.json", store)
            def disk_bytes(path):
                return sum(p.stat().st_size for p in path.rglob("*") if p.is_file()) if path else None
            storage = {"backend_output_bytes": disk_bytes(args.output / "backend"),
                       "baseline_prometheus_bytes": disk_bytes(args.exact_storage),
                       "fallback_prometheus_bytes": disk_bytes(args.fallback_storage),
                       "backend_store": store,
                       "scope": "logical file bytes including WAL; backend output also contains logs; concurrent snapshots are approximate"}
            write_json(args.output / "storage.json", storage)
            write_json(args.output / "completion.json", {"complete": True,
                       "execution_counts": {k: sum(r["execution"] == k for r in results) for k in ["warm", "exact_fallback", "failed"]},
                       "execution_detail_counts": {k: sum(r["execution_provenance"]["detail"] == k for r in results) for k in ["asap", "hybrid", "local_raw", "external_exact", "failed", "invalid_provenance"]},
                       "benefit_claim": None})
            if args.compare:
                report = {"schema_version": 1, "all_requests": summarize(results),
                          "by_phase": {phase: summarize([r for r in results if r["phase"] == phase])
                                       for phase in ["first_pass", "repeat"]},
                          "by_query_occurrence": {query["id"]: summarize([r for r in results if r["id"] == query["id"]])
                                                  for query in queries},
                          "by_execution": {route: summarize([r for r in results if r["execution"] == route])
                                           for route in ["warm", "exact_fallback", "failed"]},
                          "by_execution_detail": {detail: summarize([r for r in results if r["execution_provenance"]["detail"] == detail])
                                                  for detail in ["asap", "hybrid", "local_raw", "external_exact"]},
                          "estimated_cost": plan["cost_comparison"],
                          "measurement_units": {"latency": "nanoseconds", "cpu": "process CPU nanoseconds", "memory": "bytes"},
                          "resource_limits": {"cpu_affinity": sorted(cpus) if cpus else None,
                                              "address_space_bytes": args.address_space_bytes,
                                              "scope": "per process; backend fallback service charged separately"},
                          "isolated_baseline_service": bool(args.fallback_url),
                          "measured_ingest_and_settle_wall_ns": ingest_elapsed,
                          "planning_wall_ns": plan["planning_elapsed_ns"],
                          "process_phases": phases,
                          "planning_resources": planning_resources,
                          "storage": storage,
                          "phase_resources": {name: {service: process_delta(phases[before].get(service), phases[after].get(service))
                                                     for service in PROCESS_IDS}
                                              for name, before, after in [
                                                  ("startup", "startup", "before_ingest"),
                                                  ("ingest_and_build", "before_ingest", "after_ingest_and_settle"),
                                                  ("queries", "after_ingest_and_settle", "after_queries")]},
                          "estimated_vs_measured_cost_ratio": None,
                          "acceptance_complete": False,
                          "limitations": ["No common conversion from provider cost units to measured resource units",
                              *([] if args.fallback_url else ["Exact service is shared with fallback; caches are not isolated"]),
                              *([] if cpus else ["CPU affinity is not enforced"]),
                              "RLIMIT_AS is virtual address space, not physical-memory or aggregate multi-process CPU enforcement",
                              "Raw process RSS is not summary state size; store.json retains backend counters",
                              "Service startup before supplied PID attachment and isolated cold-cache runs remain unmeasured",
                              "Query equality on one dataset is not a formal approximation confidence guarantee"]}
                write_json(args.output / "comparison.json", report)
        finally:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


if __name__ == "__main__":
    main()
