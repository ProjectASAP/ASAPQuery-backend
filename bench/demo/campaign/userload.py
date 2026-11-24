#!/usr/bin/env python3
"""One engine ALONE under a multi-user Grafana-like query load, ingesting at the same time.

usage: userload.py <engine: victoriametrics|asap> <prep dir> <out dir> [N,N,...]

One feeder (the prep's feeder table, seed 862, both metrics) feeds the engine in real
time from an aligned start S. After a warm-up of the longest lookback (1 h), the
load steps through N = 1, 4, 8, 16 simulated users, PHASE_S (600) seconds each.
Every user, every 15 s (users' phases spread over the 15 s), sends the whole query
set at once without waiting for earlier requests (several in flight, nothing
skipped), each query at its latest evaluation time on its own interval grid that
is at least DELAY_S (6) seconds old, as the campaign's runs do. Timeout 60 s.

Per phase it records:
- query latency p50/p99/max, timeouts and errors;
- engine CPU (core-s/s) and RSS, from /proc, on the 12-core engine cpuset;
- feeder lag: max start lag and overrun ticks of the ticks inside the phase, from
  the feeder's stats (written when the feeder ends);
- visibility, every 15 s:
  - VictoriaMetrics: sum(count_over_time(m[1m])) at now - DELAY_S vs the samples
    sent for that minute (series x samples/s x 60), per metric;
  - ASAP: asap_watermark_lag_seconds, asap_precompute_backlog_windows,
    asap_samples_queued (the data plane does not answer count_over_time).
Writes <out>/phases.json and prints a table.
"""
import concurrent.futures, json, os, statistics, subprocess, sys, threading, time, urllib.parse, urllib.request
from pathlib import Path

ENGINE, PREP, OUT = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
NS = [int(x) for x in (sys.argv[4] if len(sys.argv) > 4 else "1,4,8,16").split(",")]
BIN = Path(os.environ["BIN_DIR"])
VM_BIN = Path(os.environ["BIN_DIR"] + "") / f"victoria-metrics-{os.environ.get('VICTORIAMETRICS_VERSION', 'v1.156.0')}"
PHASE_S = int(os.environ.get("PHASE_S", "600"))
DELAY_S = float(os.environ.get("DELAY_S", "6"))
REFRESH_S = 15
TIMEOUT_S = 60
CORES = int(os.environ.get("ARM_CORES", "12"))
NCPU = os.cpu_count()
ENG_CPUS, AUX_CPUS = f"0-{CORES - 1}", f"{CORES}-{NCPU - 1}"
TICK = os.sysconf("SC_CLK_TCK")
OUT.mkdir(parents=True, exist_ok=True)
queries = json.load(open(PREP / "queries.json"))["queries"]
manifest = json.load(open(PREP / "manifest.json"))
SERIES = manifest["data_facts"]["series"]["leaf_count"]
SPS = manifest["samples_per_sec_per_series"]
lookback_s = max(q["lookback_ms"] for q in queries) // 1000


def get(url, timeout=TIMEOUT_S):
    t0 = time.monotonic()
    with urllib.request.urlopen(url, timeout=timeout) as r:
        body = r.read()
        return r.status, body, (time.monotonic() - t0) * 1e3


def proc_stat(pid):
    try:
        f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        rss = int(open(f"/proc/{pid}/statm").read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
        return (int(f[11]) + int(f[12])) / TICK, rss
    except OSError:
        return None, None


def main():
    start_ms = (int(time.time()) + 60) // 60 * 60 * 1000 + 60_000
    if ENGINE == "victoriametrics":
        port = 19500
        cmd = [str(VM_BIN), f"-storageDataPath={OUT / 'vmdata'}", f"-httpListenAddr=127.0.0.1:{port}",
               "-retentionPeriod=1d", f"-search.latencyOffset={max(1, int(DELAY_S) - 1)}s",
               "-search.maxQueryDuration=120s", "-search.maxSamplesPerQuery=100000000000",
               "-search.maxSamplesPerSeries=1000000000", "-loggerLevel=WARN"]
        base, ready = f"http://127.0.0.1:{port}", "/health"
    else:
        port = 19601
        cmd = [str(BIN / "data_plane"), "--plan", str(PREP / "backend/plan.json"),
               "--prometheus-url", "http://127.0.0.1:9", "--listen", f"127.0.0.1:{port}",
               "--allowed-lateness-ms", "2000", "--stream-start-ms", str(start_ms), "--no-forward",
               "--view-dir", str(OUT / "views"),
               *(["--precompute-threads", os.environ["PRECOMPUTE_THREADS"]] if os.environ.get("PRECOMPUTE_THREADS") else [])]
        base, ready = f"http://127.0.0.1:{port}", "/-/ready"
    eng = subprocess.Popen(["taskset", "-c", ENG_CPUS, *cmd], stdout=open(OUT / "engine.log", "w"),
                           stderr=subprocess.STDOUT, env=dict(os.environ, RUST_LOG="warn"))
    for _ in range(240):
        try:
            get(base + ready, 2); break
        except Exception:  # noqa: BLE001
            if eng.poll() is not None:
                sys.exit(f"engine exited: see {OUT / 'engine.log'}")
            time.sleep(0.5)
    else:
        sys.exit("engine not ready")
    duration = lookback_s + PHASE_S * len(NS) + 120
    feeder = subprocess.Popen(
        ["taskset", "-c", AUX_CPUS, str(BIN / "paced_feeder"), "--sink", "remote-write",
         "--url", f"{base}/api/v1/write", "--workload-table", str(PREP / "feeder_table.json"),
         "--seed", "862", "--duration-secs", str(duration), "--start-ms", str(start_ms),
         "--stats-out", str(OUT / "feeder-stats.json")],
        stdout=open(OUT / "feeder.log", "w"), stderr=subprocess.STDOUT)
    meta = {"engine": ENGINE, "start_ms": start_ms, "series": SERIES, "samples_per_sec_per_series": SPS,
            "phase_s": PHASE_S, "delay_s": DELAY_S, "refresh_s": REFRESH_S, "ns": NS, "cmd": cmd,
            "engine_cpus": ENG_CPUS, "aux_cpus": AUX_CPUS, "bin_dir": str(BIN)}
    (OUT / "meta.json").write_text(json.dumps(meta, indent=1))
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=512)
    results, lock = [], threading.Lock()
    vis = []

    def ask(n, user, q, t_ms):
        url = f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": q["promql"], "time": f"{t_ms / 1000:.3f}"})
        t0 = time.monotonic()
        try:
            status, body, ms = get(url)
            ok = status == 200 and json.loads(body).get("status") == "success"
            row = {"n": n, "q": q["id"], "t_ms": t_ms, "ms": ms, "ok": ok, "timeout": False}
        except Exception as e:  # noqa: BLE001
            ms = (time.monotonic() - t0) * 1e3
            row = {"n": n, "q": q["id"], "t_ms": t_ms, "ms": ms, "ok": False, "timeout": ms >= TIMEOUT_S * 1e3 - 500,
                   "error": str(e)[:120]}
        with lock:
            results.append(row)

    def probe(n, now):
        t = now - DELAY_S
        row = {"n": n, "t": t}
        if ENGINE == "victoriametrics":
            for m in ("data", "data_total"):
                url = f"{base}/api/v1/query?" + urllib.parse.urlencode(
                    {"query": f"sum(count_over_time({m}[1m]))", "time": f"{t:.3f}"})
                try:
                    res = json.loads(get(url)[1])["data"]["result"]
                    row[m] = float(res[0]["value"][1]) / (SERIES * SPS * 60) if res else 0.0
                except Exception as e:  # noqa: BLE001
                    row[m] = None
        else:
            try:
                text = get(base + "/metrics", 10)[1].decode()
                for line in text.splitlines():
                    for k in ("asap_watermark_lag_seconds", "asap_precompute_backlog_windows", "asap_samples_queued"):
                        if line.startswith(k + " "):
                            row[k] = float(line.split()[1])
            except Exception:  # noqa: BLE001
                pass
        with lock:
            vis.append(row)

    phases = []
    warm_end = start_ms / 1000 + lookback_s
    while time.time() < warm_end:
        time.sleep(1)
    for n in NS:
        p0 = time.time()
        cpu0, _ = proc_stat(eng.pid)
        rss_max = 0
        next_round = [p0 + i * REFRESH_S / n for i in range(n)]
        next_probe = p0
        while time.time() < p0 + PHASE_S:
            now = time.time()
            for u in range(n):
                if now >= next_round[u]:
                    for q in queries:
                        iv = q["interval_ms"]
                        t_ms = int((now - DELAY_S) * 1000) // iv * iv
                        pool.submit(ask, n, u, q, t_ms)
                    next_round[u] += REFRESH_S
            if now >= next_probe:
                pool.submit(probe, n, now)
                _, rss = proc_stat(eng.pid)
                rss_max = max(rss_max, rss or 0)
                next_probe += REFRESH_S
            time.sleep(0.05)
        p1 = time.time()
        cpu1, rss1 = proc_stat(eng.pid)
        phases.append({"n": n, "start": p0, "end": p1, "cpu_cores": (cpu1 - cpu0) / (p1 - p0),
                       "rss_end": rss1, "rss_max": rss_max})
        print(f"phase N={n} done", flush=True)
    time.sleep(TIMEOUT_S + 5)  # let the last requests finish
    pool.shutdown(wait=True)
    feeder.terminate()
    try:
        feeder.wait(60)
    except subprocess.TimeoutExpired:
        feeder.kill()
    eng.terminate()
    try:
        eng.wait(60)
    except subprocess.TimeoutExpired:
        eng.kill()
    try:
        fs = json.load(open(OUT / "feeder-stats.json"))
    except Exception:  # noqa: BLE001
        fs = {"ticks": []}
    out = []
    for ph in phases:
        rs = [r for r in results if r["n"] == ph["n"]]
        lat = sorted(r["ms"] for r in rs if r["ok"])
        ticks = [t for t in fs["ticks"]
                 if ph["start"] <= (start_ms / 1000 + t["tick"]) < ph["end"]]
        vs = [v for v in vis if v["n"] == ph["n"]]
        row = {**ph, "requests": len(rs), "ok": len(lat), "timeouts": sum(r["timeout"] for r in rs),
               "errors": sum((not r["ok"]) and not r["timeout"] for r in rs),
               "p50_ms": lat[len(lat) // 2] if lat else None,
               "p99_ms": lat[min(len(lat) - 1, int(0.99 * len(lat)))] if lat else None,
               "max_ms": lat[-1] if lat else None,
               "feeder_ticks": len(ticks),
               "feeder_max_start_lag_s": max((t["start_lag_ms"] for t in ticks), default=0) / 1000,
               "feeder_p50_start_lag_s": (sorted(t["start_lag_ms"] for t in ticks)[len(ticks) // 2] / 1000) if ticks else None,
               "feeder_overrun_ticks": sum(1 for t in ticks if t["elapsed_ms"] > 1000)}
        if ENGINE == "victoriametrics":
            for m in ("data", "data_total"):
                xs = [v[m] for v in vs if v.get(m) is not None]
                row[f"visible_{m}_min"] = min(xs) if xs else None
                row[f"visible_{m}_mean"] = statistics.mean(xs) if xs else None
        else:
            for k in ("asap_watermark_lag_seconds", "asap_precompute_backlog_windows", "asap_samples_queued"):
                xs = [v[k] for v in vs if k in v]
                row[f"{k}_max"] = max(xs) if xs else None
        row["per_query"] = {}
        for q in queries:
            l = sorted(r["ms"] for r in rs if r["q"] == q["id"] and r["ok"])
            row["per_query"][q["id"]] = {"ok": len(l), "fail": sum(1 for r in rs if r["q"] == q["id"] and not r["ok"]),
                                        "p50_ms": l[len(l) // 2] if l else None,
                                        "p99_ms": l[min(len(l) - 1, int(0.99 * len(l)))] if l else None}
        out.append(row)
    (OUT / "phases.json").write_text(json.dumps(out, indent=1))
    (OUT / "requests.jsonl").write_text("\n".join(json.dumps(r) for r in results) + "\n")
    (OUT / "visibility.jsonl").write_text("\n".join(json.dumps(v) for v in vis) + "\n")
    for r in out:
        print(json.dumps({k: v for k, v in r.items() if k != "per_query"}))
    print("USERLOAD_DONE", flush=True)


if __name__ == "__main__":
    main()
