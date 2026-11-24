#!/usr/bin/env python3
"""One engine ALONE under N simulated dashboard users, with a fair, classified outcome.

usage: userload2.py <engine: victoriametrics|asap> <prep dir> <out dir> [N,N,...]

Placement (40 cores): engine on cpus 0-11 (cgroup v2 for memory), a REFERENCE
VictoriaMetrics on 12-23 that only ingests the same feed (its own feeder) and is
queried after the run, untimed; feeders and this script on 24-39.

Load: after a warm-up of the longest lookback, phases of PHASE_S (600) s for each N.
Every user sends the whole query set every REFRESH_S (15) s (users spread over the
15 s), each query at its latest evaluation time on its own grid at least DELAY_S (6)
s old; requests are not awaited (several in flight), nothing is skipped; client
timeout 60 s. Both engines get exactly this offered load.

Per request: HTTP status, latency, timeout, VictoriaMetrics' isPartial, and the answer
(gzip jsonl). At each (query, T) when user 0 sends, a visibility probe:
VictoriaMetrics: sum(count_over_time(<metric>[<window>])) at T on the loaded engine;
ASAP: its watermark (asap_watermark_lag_seconds), since it does not answer
count_over_time. After the run: the reference's untimed answer and its count_over_time
for every (query, T). The reference answer uses the engine's own semantics: ASAP is
checked against PromQL (rate/increase as *_prometheus); VictoriaMetrics against its own
MetricsQL query, so its "inaccurate" means missing samples or limits.

Classification of every request:
  answered_ok          200, latency <= the query's interval (the SLO), within 5% of the
                       reference on every reference series (topk: every reference series present)
  answered_inaccurate  200 within the SLO but outside the target; cause: "missing samples"
                       when the engine's visible fraction at T was < 0.999, else "approximation"
  slow                 200 but latency > the SLO
  rejected             HTTP 4xx/5xx (VictoriaMetrics 503 / too many requests, ASAP refusals)
  timeout              no answer within 60 s
  partial              VictoriaMetrics answered with isPartial=true
Goodput = answered_ok per second, and as a fraction of offered requests.

VictoriaMetrics engine flags: defaults (concurrency limits, -search.maxQueueDuration,
-search.maxQueryDuration, sample caps), except -search.latencyOffset=<DELAY_S>s (its
30 s default would hide the newest 30 s; we give it our uniform 6 s query delay) and
-retentionPeriod=1d. Its /metrics self-protection counters and ASAP's refusal/ingest
counters are sampled every 5 s and diffed per phase.
"""
import collections, concurrent.futures, gzip, json, os, re, statistics, subprocess, sys, threading, time
import urllib.error, urllib.parse, urllib.request
from pathlib import Path

ENGINE, PREP, OUT = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
NS = [int(x) for x in (sys.argv[4] if len(sys.argv) > 4 else "1,4,8,16").split(",")]
BIN = Path(os.environ["BIN_DIR"])
VM_BIN = Path(os.environ["BIN_DIR"] + "") / f"victoria-metrics-{os.environ.get('VICTORIAMETRICS_VERSION', 'v1.156.0')}"
PHASE_S = int(os.environ.get("PHASE_S", "600"))
DELAY_S = float(os.environ.get("DELAY_S", "6"))
REFRESH_S, TIMEOUT_S, TARGET = 15, 60, 0.05
ENG_CPUS, REF_CPUS, AUX_CPUS = "0-11", "12-23", "24-39"
TICK = os.sysconf("SC_CLK_TCK")
RATE = re.compile(r"\b(rate|increase)\(")
METRIC = re.compile(r"\b(data_total|data)\b")
VM_KEEP = re.compile(r"^(vm_concurrent_select_|vm_concurrent_insert_|vm_slow_queries_total|vm_http_request_errors_total"
                     r"|vm_http_requests_total\{path=\"/api/v1/(query|write)|vm_rows_inserted_total|vm_rows_ignored_total"
                     r"|vm_too_long_lines|vm_rollup_result_cache_(full_hits|partial_hits|miss)_total|vm_cache_requests_total"
                     r"\{type=\"promql/rollupResult|vm_search_query_duration|vm_memory_intensive_queries_total"
                     r"|vm_max_unique_timeseries|vm_rows_read_per_query|vm_concurrent_select_limit)")
ASAP_KEEP = re.compile(r"^(asap_samples_total|asap_windows_total|asap_precompute_backlog_windows|asap_samples_queued"
                       r"|asap_watermark_lag_seconds|asap_query|asap_remote_write|asap_http)")
OUT.mkdir(parents=True, exist_ok=True)
queries = json.load(open(PREP / "queries.json"))["queries"]
manifest = json.load(open(PREP / "manifest.json"))
SERIES = manifest["data_facts"]["series"]["leaf_count"]
SPS = manifest["samples_per_sec_per_series"]
lookback_s = max(q["lookback_ms"] for q in queries) // 1000
QBY = {q["id"]: q for q in queries}


def http(url, timeout=TIMEOUT_S):
    """(status or None, body bytes or None, ms, timeout?)"""
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, r.read(), (time.monotonic() - t0) * 1e3, False
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
        except Exception:  # noqa: BLE001
            body = b""
        return e.code, body, (time.monotonic() - t0) * 1e3, False
    except Exception as e:  # noqa: BLE001
        ms = (time.monotonic() - t0) * 1e3
        return None, str(e).encode()[:200], ms, ("timed out" in str(e) or ms >= timeout * 1e3 - 500)


def proc_stat(pid):
    try:
        f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        rss = int(open(f"/proc/{pid}/statm").read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
        return (int(f[11]) + int(f[12])) / TICK, rss
    except OSError:
        return None, None


def cgroup(name, pid):
    path = Path("/sys/fs/cgroup/asapbench") / name
    script = ("echo '+memory +cpu +io' > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null; mkdir -p /sys/fs/cgroup/asapbench "
              "&& echo '+memory +cpu +io' > /sys/fs/cgroup/asapbench/cgroup.subtree_control; "
              f"mkdir -p {path} && echo {pid} > {path}/cgroup.procs")
    ok = subprocess.run(["sudo", "-n", "sh", "-c", script], capture_output=True).returncode == 0
    return path if ok else None


def cg_mem(path):
    if not path:
        return None
    try:
        out = {"current": int((path / "memory.current").read_text())}
        for line in (path / "memory.stat").read_text().splitlines():
            k, v = line.split()
            if k in ("anon", "file", "active_file", "inactive_file", "kernel", "shmem"):
                out[k] = int(v)
        out["working_set"] = max(0, out["current"] - out.get("inactive_file", 0))
        return out
    except (OSError, ValueError):
        return None


def metrics_lines(base, keep):
    st, body, _, _ = http(base + "/metrics", 10)
    if st != 200:
        return {}
    out = {}
    for line in body.decode(errors="replace").splitlines():
        if line and not line.startswith("#") and keep.search(line):
            k, _, v = line.rpartition(" ")
            try:
                out[k] = float(v)
            except ValueError:
                pass
    return out


def vm_flags(datadir, port, reference):
    flags = [f"-storageDataPath={datadir}", f"-httpListenAddr=127.0.0.1:{port}", "-retentionPeriod=1d",
             f"-search.latencyOffset={int(DELAY_S)}s", "-loggerLevel=WARN"]
    if reference:  # untimed ground truth: no query limits
        flags += ["-search.maxQueryDuration=1h", "-search.maxSamplesPerQuery=100000000000",
                  "-search.maxSamplesPerSeries=1000000000", "-search.maxUniqueTimeseries=100000000",
                  "-search.maxConcurrentRequests=64", "-search.maxMemoryPerQuery=0"]
    return flags


def series_map(result):
    res = (result or {}).get("data", {}).get("result", [])
    return {tuple(sorted((k, v) for k, v in r["metric"].items() if k != "__name__")): float(r["value"][1]) for r in res}


def main():
    start_ms = (int(time.time()) + 60) // 60 * 60 * 1000 + 60_000
    ref_port = 19700
    ref = subprocess.Popen(["taskset", "-c", REF_CPUS, str(VM_BIN), *vm_flags(OUT / "refdata", ref_port, True)],
                           stdout=open(OUT / "reference-vm.log", "w"), stderr=subprocess.STDOUT)
    if ENGINE == "victoriametrics":
        port = 19500
        cmd = [str(VM_BIN), *vm_flags(OUT / "vmdata", port, False)]
        ready = "/health"
    else:
        port = 19601
        cmd = [str(BIN / "data_plane"), "--plan", str(PREP / "backend/plan.json"), "--prometheus-url",
               "http://127.0.0.1:9", "--listen", f"127.0.0.1:{port}", "--allowed-lateness-ms", "2000",
               "--stream-start-ms", str(start_ms), "--no-forward", "--view-dir", str(OUT / "views"),
               *(["--precompute-threads", os.environ["PRECOMPUTE_THREADS"]] if os.environ.get("PRECOMPUTE_THREADS") else [])]
        ready = "/-/ready"
    base, refbase = f"http://127.0.0.1:{port}", f"http://127.0.0.1:{ref_port}"
    eng = subprocess.Popen(["taskset", "-c", ENG_CPUS, *cmd], stdout=open(OUT / "engine.log", "w"),
                           stderr=subprocess.STDOUT, env=dict(os.environ, RUST_LOG="warn"))
    cg = cgroup(f"userload-{os.getpid()}-{ENGINE}", eng.pid)
    for url in (base + ready, refbase + "/health"):
        for _ in range(240):
            if http(url, 2)[0] == 200:
                break
            if eng.poll() is not None:
                sys.exit("engine exited")
            time.sleep(0.5)
    duration = lookback_s + PHASE_S * len(NS) + 120
    feeders = []
    for name, sink in (("feeder", base), ("feeder-ref", refbase)):
        feeders.append(subprocess.Popen(
            ["taskset", "-c", AUX_CPUS, str(BIN / "paced_feeder"), "--sink", "remote-write", "--url", f"{sink}/api/v1/write",
             "--workload-table", str(PREP / "feeder_table.json"), "--seed", "862", "--duration-secs", str(duration),
             "--start-ms", str(start_ms), "--stats-out", str(OUT / f"{name}-stats.json")],
            stdout=open(OUT / f"{name}.log", "w"), stderr=subprocess.STDOUT))
    meta = {"engine": ENGINE, "start_ms": start_ms, "series": SERIES, "samples_per_sec_per_series": SPS,
            "phase_s": PHASE_S, "delay_s": DELAY_S, "refresh_s": REFRESH_S, "timeout_s": TIMEOUT_S, "ns": NS,
            "engine_cmd": cmd, "reference_vm_flags": vm_flags("…", ref_port, True), "cpus": {
                "engine": ENG_CPUS, "reference_vm": REF_CPUS, "feeders_and_client": AUX_CPUS},
            "cgroup": str(cg) if cg else None, "bin_dir": str(BIN),
            "vm_engine_flags_note": "defaults except -search.latencyOffset=<delay>s and -retentionPeriod=1d"}
    (OUT / "meta.json").write_text(json.dumps(meta, indent=1))
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=768)
    lock = threading.Lock()
    reqs = open(OUT / "requests.jsonl", "w")
    answers = gzip.open(OUT / "answers.jsonl.gz", "wt", compresslevel=1)
    probes, msamples, cgs = [], [], []
    stop_sampler = threading.Event()
    keep = VM_KEEP if ENGINE == "victoriametrics" else ASAP_KEEP

    def sampler():
        while not stop_sampler.wait(5):
            row = {"t": time.time(), "m": metrics_lines(base, keep)}
            cpu, rss = proc_stat(eng.pid)
            row.update(cpu=cpu, rss=rss, cg=cg_mem(cg))
            with lock:
                msamples.append(row)
    threading.Thread(target=sampler, daemon=True).start()
    rid = iter(range(10 ** 9))

    def ask(n, user, q, t_ms):
        i = next(rid)
        url = f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": q["promql"], "time": f"{t_ms / 1000:.3f}"})
        sent = time.time()
        st, body, ms, to = http(url)
        row = {"i": i, "n": n, "user": user, "q": q["id"], "t_ms": t_ms, "sent": sent, "ms": ms, "status": st,
               "timeout": to, "partial": False}
        if st == 200:
            try:
                res = json.loads(body)
                row["ok"] = res.get("status") == "success"
                row["partial"] = bool(res.get("isPartial"))
                if row["ok"]:
                    with lock:
                        answers.write(json.dumps({"i": i, "r": series_map(res).__repr__()}) + "\n")
            except ValueError:
                row["ok"] = False
        else:
            row["ok"] = False
            row["error"] = (body or b"")[:200].decode(errors="replace")
        with lock:
            reqs.write(json.dumps(row) + "\n")

    def probe(n, q, t_ms):
        row = {"n": n, "q": q["id"], "t_ms": t_ms, "at": time.time()}
        if ENGINE == "victoriametrics":
            m = METRIC.search(q["promql"]).group(1)
            url = f"{base}/api/v1/query?" + urllib.parse.urlencode(
                {"query": f"sum(count_over_time({m}[{q['window_ms'] // 1000}s]))", "time": f"{t_ms / 1000:.3f}"})
            st, body, _, _ = http(url)
            try:
                r = json.loads(body)["data"]["result"] if st == 200 else []
                row["count"] = float(r[0]["value"][1]) if r else 0.0
            except (ValueError, KeyError, TypeError):
                row["count"] = None
        else:
            lag = metrics_lines(base, re.compile(r"^asap_watermark_lag_seconds")).get("asap_watermark_lag_seconds")
            row["watermark_lag_s"] = lag
            row["visible_until_ms"] = (row["at"] - lag) * 1000 if lag is not None else None
        with lock:
            probes.append(row)

    phases = []
    while time.time() < start_ms / 1000 + lookback_s:
        time.sleep(1)
    for n in NS:
        p0 = time.time()
        nxt = [p0 + i * REFRESH_S / n for i in range(n)]
        while time.time() < p0 + PHASE_S:
            now = time.time()
            for u in range(n):
                if now >= nxt[u]:
                    for q in queries:
                        iv = q["interval_ms"]
                        t_ms = int((now - DELAY_S) * 1000) // iv * iv
                        pool.submit(ask, n, u, q, t_ms)
                        if u == 0:
                            pool.submit(probe, n, q, t_ms)
                    nxt[u] += REFRESH_S
            time.sleep(0.02)
        phases.append({"n": n, "start": p0, "end": time.time()})
        print(f"phase N={n} done", flush=True)
    time.sleep(TIMEOUT_S + 5)
    pool.shutdown(wait=True)
    stop_sampler.set()
    reqs.close()
    answers.close()
    # Reference: its feeder finishes (it was never queried, so it is on time), then untimed answers.
    feeders[1].wait()
    feeders[0].terminate()
    eng.terminate()
    try:
        eng.wait(60)
    except subprocess.TimeoutExpired:
        eng.kill()
    time.sleep(3)
    reqs_l = [json.loads(l) for l in open(OUT / "requests.jsonl")]
    keys = sorted({(r["q"], r["t_ms"]) for r in reqs_l})
    refans, refcount = {}, {}
    for qid, t_ms in keys:
        q = QBY[qid]
        # Ground truth in the engine's own query semantics: PromQL for ASAP (rate/increase
        # as *_prometheus); for VictoriaMetrics its own MetricsQL text, untimed and complete,
        # so a difference is missing samples or limits, not MetricsQL's rate definition.
        promql = q["promql"] if ENGINE == "victoriametrics" else RATE.sub(lambda m: m.group(1) + "_prometheus(", q["promql"])
        st, body, _, _ = http(f"{refbase}/api/v1/query?" + urllib.parse.urlencode(
            {"query": promql, "time": f"{t_ms / 1000:.3f}"}), 3600)
        refans[(qid, t_ms)] = series_map(json.loads(body)) if st == 200 else None
        m = METRIC.search(q["promql"]).group(1)
        st, body, _, _ = http(f"{refbase}/api/v1/query?" + urllib.parse.urlencode(
            {"query": f"sum(count_over_time({m}[{q['window_ms'] // 1000}s]))", "time": f"{t_ms / 1000:.3f}"}), 3600)
        try:
            r = json.loads(body)["data"]["result"]
            refcount[(qid, t_ms)] = float(r[0]["value"][1]) if r else None
        except (ValueError, KeyError, TypeError):
            refcount[(qid, t_ms)] = None
    ref.terminate()
    # Visible fraction per (query, T).
    visible = {}
    for p in probes:
        key = (p["q"], p["t_ms"])
        if ENGINE == "victoriametrics":
            rc = refcount.get(key)
            visible[key] = (p["count"] / rc) if (rc and p.get("count") is not None) else None
        else:
            vu = p.get("visible_until_ms")
            w = QBY[p["q"]]["window_ms"]
            visible[key] = None if vu is None else max(0.0, min(1.0, (vu - (p["t_ms"] - w)) / w))
    # Classify.
    ans = {}
    with gzip.open(OUT / "answers.jsonl.gz", "rt") as f:
        for line in f:
            a = json.loads(line)
            ans[a["i"]] = eval(a["r"])  # noqa: S307 - our own repr of a dict of tuples
    for r in reqs_l:
        q = QBY[r["q"]]
        slo_ms = q["interval_ms"]
        if r["timeout"]:
            r["class"] = "timeout"
        elif r["status"] is None or r["status"] >= 400 or not r.get("ok"):
            r["class"] = "rejected"
        elif r["partial"]:
            r["class"] = "partial"
        elif r["ms"] > slo_ms:
            r["class"] = "slow"
        else:
            refv, a = refans.get((r["q"], r["t_ms"])), ans.get(r["i"], {})
            if refv is None:
                r["class"] = "unverified"
            else:
                errs = [0.0 if a.get(k) == v else abs(a.get(k, float("inf")) - v) / max(abs(v), 1e-12)
                        for k, v in refv.items()]
                r["max_err"] = max(errs, default=0.0)
                if r["max_err"] <= TARGET:
                    r["class"] = "answered_ok"
                else:
                    vis = visible.get((r["q"], r["t_ms"]))
                    r["visible_fraction"] = vis
                    r["class"] = "inaccurate_missing_samples" if (vis is not None and vis < 0.999) \
                        else "inaccurate_approximation"
    with open(OUT / "requests_classified.jsonl", "w") as f:
        for r in reqs_l:
            f.write(json.dumps(r) + "\n")
    try:
        fs = json.load(open(OUT / "feeder-stats.json"))
    except Exception:  # noqa: BLE001
        fs = {"ticks": []}
    out = []
    for ph in phases:
        rs = [r for r in reqs_l if r["n"] == ph["n"]]
        dur = ph["end"] - ph["start"]
        cls = collections.Counter(r["class"] for r in rs)
        lat = sorted(r["ms"] for r in rs if r["status"] == 200)
        ticks = [t for t in fs["ticks"] if ph["start"] <= start_ms / 1000 + t["tick"] < ph["end"]]
        ms_ = [m for m in msamples if ph["start"] <= m["t"] <= ph["end"]]
        diff = {}
        if len(ms_) >= 2:
            a, b = ms_[0]["m"], ms_[-1]["m"]
            for k in b:
                if k.endswith("_total") or "_total{" in k:
                    diff[k] = b[k] - a.get(k, 0.0)
                else:
                    diff[k + " (max)"] = max(m["m"].get(k, 0.0) for m in ms_)
        row = {"n": ph["n"], "duration_s": dur, "offered": len(rs), "classes": dict(cls),
               "goodput_per_s": cls.get("answered_ok", 0) / dur,
               "goodput_fraction": cls.get("answered_ok", 0) / len(rs) if rs else None,
               "p50_ms": lat[len(lat) // 2] if lat else None,
               "p99_ms": lat[min(len(lat) - 1, int(0.99 * len(lat)))] if lat else None,
               "engine_cpu_cores": ((ms_[-1]["cpu"] - ms_[0]["cpu"]) / (ms_[-1]["t"] - ms_[0]["t"])) if len(ms_) >= 2 else None,
               "rss_max": max((m["rss"] or 0) for m in ms_) if ms_ else None,
               "working_set_median": statistics.median([m["cg"]["working_set"] for m in ms_ if m.get("cg")]) if any(m.get("cg") for m in ms_) else None,
               "anon_median": statistics.median([m["cg"]["anon"] for m in ms_ if m.get("cg")]) if any(m.get("cg") for m in ms_) else None,
               "feeder_max_start_lag_s": max((t["start_lag_ms"] for t in ticks), default=0) / 1000,
               "feeder_overrun_ticks": sum(1 for t in ticks if t["elapsed_ms"] > 1000),
               "engine_counters": diff,
               "per_query": {q["id"]: dict(collections.Counter(r["class"] for r in rs if r["q"] == q["id"])) for q in queries}}
        vs = sorted(v for (qid, t), v in visible.items() if v is not None and ph["start"] * 1000 - 120000 <= t <= ph["end"] * 1000)
        row["visible_fraction"] = {"min": vs[0] if vs else None, "p10": vs[len(vs) // 10] if vs else None,
                                   "median": vs[len(vs) // 2] if vs else None, "n": len(vs)}
        out.append(row)
    (OUT / "phases.json").write_text(json.dumps(out, indent=1))
    (OUT / "metrics_samples.jsonl").write_text("\n".join(json.dumps(m) for m in msamples) + "\n")
    (OUT / "probes.jsonl").write_text("\n".join(json.dumps(p) for p in probes) + "\n")
    print(f"| N | offered | answered_ok | inaccurate (missing/approx) | slow | rejected | timeout | partial | goodput/s | p50/p99 ms |")
    for r in out:
        c = r["classes"]
        print(f"| {r['n']} | {r['offered']} | {c.get('answered_ok', 0)} | {c.get('inaccurate_missing_samples', 0)}/"
              f"{c.get('inaccurate_approximation', 0)} | {c.get('slow', 0)} | {c.get('rejected', 0)} | "
              f"{c.get('timeout', 0)} | {c.get('partial', 0)} | {r['goodput_per_s']:.2f} | "
              f"{r['p50_ms'] and round(r['p50_ms'])}/{r['p99_ms'] and round(r['p99_ms'])} |")
    if cg:
        subprocess.run(["sudo", "-n", "rmdir", str(cg)], capture_output=True)
    print("USERLOAD_DONE", flush=True)


if __name__ == "__main__":
    main()
