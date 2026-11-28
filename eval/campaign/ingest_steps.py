#!/usr/bin/env python3
"""Ingest upper bound: step the input rate for one engine at a time.

For each workload and series count (at the workload's sample rate), plan it
(prepare.py, as run_vm_demo.sh does), start the engine alone on cpus 0..N-1,
feed it from the remaining cpus (one feeder per metric: data, data_total) for
STEP_S seconds, no queries, and sample its /metrics and /proc every 5 s.
An arm stops stepping at its first saturated step:
  asap: precompute backlog windows growing, samples queued at the cap, or any
        Remote Write request failing (503);
  victoriametrics: rows inserted below 97% of the target rate, or the feeder
        overrunning ticks (VM answering Remote Write slower than real time).
A step where the feeders themselves cannot keep up while the engine has idle
cores is labelled feeder-bound.
Writes one JSON line per step to OUT/steps.jsonl.
"""
import json, os, re, subprocess, sys, time, urllib.request
from pathlib import Path

BENCH = Path(os.environ["ASAPQ_BENCH"] + "")
BIN = Path(os.environ["BIN_DIR"])
OUT = Path(sys.argv[1])
WORKLOADS = sys.argv[2].split(",")          # e.g. dashboard-heavy,typical-dashboard-10hz
SERIES = [int(x) for x in sys.argv[3].split(",")]
ARMS = sys.argv[4].split(",") if len(sys.argv) > 4 else ["asap", "victoriametrics"]
STEP_S = int(os.environ.get("STEP_S", "240"))
CORES = int(os.environ.get("ARM_CORES", "12"))
NCPU = os.cpu_count()
ENGINE_CPUS = f"0-{CORES - 1}"
AUX_CPUS = f"{CORES}-{NCPU - 1}"
OUT.mkdir(parents=True, exist_ok=True)
TICK = os.sysconf("SC_CLK_TCK")


def get(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode()


def metric(text, name):
    tot, found = 0.0, False
    for line in text.splitlines():
        if line.startswith(name) and (len(line) == len(name) or line[len(name)] in " {"):
            tot += float(line.rsplit(" ", 1)[1]); found = True
    return tot if found else None


def cpu_rss(pid):
    try:
        f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        cpu = (int(f[11]) + int(f[12])) / TICK
        rss = int(open(f"/proc/{pid}/statm").read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
        return cpu, rss
    except OSError:
        return None, None


def wait_ready(url, s=120):
    end = time.time() + s
    while time.time() < end:
        try:
            get(url, 2); return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError(url)


def step(workload, series, arm):
    tag = f"{workload}.s{series}.{arm}"
    d = OUT / tag
    subprocess.run(["rm", "-rf", str(d)]); d.mkdir(parents=True)
    prep = BENCH / "prep" / f"ingest.{workload}.s{series}"
    if not (prep / "backend/plan.json").exists() or not (prep / "feeder_table.json").exists():
        r = subprocess.run(["python3", "prepare.py", "--set", "demo", "--workload", workload,
                            "--label0", str(series // 200), "--out", str(prep)],
                           cwd=BENCH / "harness", capture_output=True, text=True)
        (d / "prepare.log").write_text(r.stdout + r.stderr)
        if not (prep / "backend/plan.json").exists():
            return {"error": "planning failed"}
    manifest = json.load(open(prep / "manifest.json"))
    sps = manifest["samples_per_sec_per_series"]
    target = series * sps * 2
    start_ms = (int(time.time()) + 20) * 1000
    log = open(d / "engine.log", "w")
    if arm == "asap":
        port = 19601
        cmd = [BIN / "data_plane", "--plan", prep / "backend/plan.json", "--prometheus-url", "http://127.0.0.1:9",
               "--listen", f"127.0.0.1:{port}", "--allowed-lateness-ms", "2000", "--stream-start-ms", str(start_ms),
               "--no-forward", *(["--no-view-persistence"] if os.environ.get("NO_VIEW_PERSISTENCE") == "1"
                                 else ["--view-dir", d / "views"])]
        env = dict(os.environ, RUST_LOG="warn")
        ready, metrics_url, sink = f"http://127.0.0.1:{port}/-/ready", f"http://127.0.0.1:{port}/metrics", \
            f"http://127.0.0.1:{port}/api/v1/write"
    else:
        port = 19500
        cmd = [BIN / ("victoria-metrics-" + os.environ.get("VICTORIAMETRICS_VERSION", "v1.155.0")), f"-storageDataPath={d / 'vmdata'}", f"-httpListenAddr=127.0.0.1:{port}",
               "-retentionPeriod=1d", "-search.latencyOffset=2s", "-search.maxQueryDuration=120s",
               "-search.maxSamplesPerQuery=100000000000", "-search.maxSamplesPerSeries=1000000000", "-loggerLevel=WARN"]
        env = dict(os.environ)
        ready, metrics_url, sink = f"http://127.0.0.1:{port}/health", f"http://127.0.0.1:{port}/metrics", \
            f"http://127.0.0.1:{port}/api/v1/write"
    eng = subprocess.Popen(["taskset", "-c", ENGINE_CPUS, *map(str, cmd)], stdout=log, stderr=subprocess.STDOUT, env=env)
    feeders = []
    try:
        wait_ready(ready)
        for m in ("data", "data_total"):
            feeders.append(subprocess.Popen(
                ["taskset", "-c", AUX_CPUS, str(BIN / "paced_feeder"), "--sink", "remote-write", "--url", sink,
                 "--workload-table", str(prep / "feeder_table.json"), "--metric", m, "--seed", "862",
                 "--duration-secs", str(STEP_S), "--start-ms", str(start_ms),
                 "--stats-out", str(d / f"feeder-{m}.json")],
                stdout=open(d / f"feeder-{m}.log", "w"), stderr=subprocess.STDOUT))
        samples = []
        while any(f.poll() is None for f in feeders):
            time.sleep(5)
            try:
                text = get(metrics_url)
            except Exception as e:  # noqa: BLE001
                text = ""
            cpu, rss = cpu_rss(eng.pid)
            fcpu = sum((cpu_rss(f.pid)[0] or 0) for f in feeders)
            s = {"t": time.time(), "cpu": cpu, "rss": rss, "feeder_cpu": fcpu,
                 "feeding": all(f.poll() is None for f in feeders)}
            for n in ("asap_precompute_backlog_windows", "asap_samples_queued", "asap_watermark_lag_seconds",
                      "asap_samples_total", "vm_rows_inserted_total", "vm_pending_rows",
                      "vm_http_request_errors_total", "vm_rows_added_to_storage_total",
                      "asap_view_sync_seconds_total", "asap_view_persisted_bytes_total", "asap_view_bytes"):
                v = metric(text, n)
                if v is not None:
                    s[n] = v
            samples.append(s)
        time.sleep(3)
        text = ""
        try:
            text = get(metrics_url)
        except Exception:  # noqa: BLE001
            pass
        (d / "metrics_end.txt").write_text(text)
    finally:
        cpu_end, rss_end = cpu_rss(eng.pid)
        eng.terminate()
        try:
            eng.wait(30)
        except subprocess.TimeoutExpired:
            eng.kill()
        for f in feeders:
            if f.poll() is None:
                f.kill()
    (d / "samples.json").write_text(json.dumps(samples))
    fst = {}
    for m in ("data", "data_total"):
        try:
            fst[m] = json.load(open(d / f"feeder-{m}.json"))
        except Exception:  # noqa: BLE001
            fst[m] = {}
    def tot(k):
        return sum((v.get(k) or 0) for v in fst.values() if isinstance(v.get(k), (int, float)))
    achieved = tot("achieved_rows_per_sec") if any("achieved_rows_per_sec" in v for v in fst.values()) else None
    feeder_tail = {m: (open(d / f"feeder-{m}.log").read().strip().splitlines() or [""])[-1] for m in fst}
    # Middle-to-end engine CPU rate (skip the first 30 s of warm-up).
    warm = [s for s in samples if s["feeding"] and s["t"] - samples[0]["t"] >= 30] if samples else []
    cpu_rate = None
    if len(warm) >= 2 and warm[-1]["cpu"] is not None and warm[0]["cpu"] is not None:
        cpu_rate = (warm[-1]["cpu"] - warm[0]["cpu"]) / (warm[-1]["t"] - warm[0]["t"])
    rows_rate = None
    key = "vm_rows_inserted_total" if arm == "victoriametrics" else "asap_samples_total"
    if len(warm) >= 2 and key in warm[-1] and key in warm[0]:
        rows_rate = (warm[-1][key] - warm[0][key]) / (warm[-1]["t"] - warm[0]["t"])
    backlog = [s.get("asap_precompute_backlog_windows") for s in samples if "asap_precompute_backlog_windows" in s]
    queued = [s.get("asap_samples_queued") for s in samples if "asap_samples_queued" in s]
    third = max(1, len(backlog) // 3)
    backlog_growing = bool(backlog) and max(backlog[-third:]) > max(1.0, max(backlog[:third])) and max(backlog[-third:]) >= 3
    failed = tot("failed_requests")
    overruns = tot("overrun_ticks")
    res = {
        "workload": workload, "series": series, "samples_per_sec_per_series": sps, "arm": arm,
        "target_samples_per_s": target, "feeder_achieved_samples_per_s": achieved,
        "engine_ingested_samples_per_s": rows_rate, "engine_cpu_cores": cpu_rate,
        "engine_cpu_total_s": cpu_end, "engine_rss_end_bytes": rss_end,
        "engine_peak_rss_bytes": max((s["rss"] or 0) for s in samples) if samples else None,
        "feeder_cpu_cores": (max(s["feeder_cpu"] for s in samples) / STEP_S) if samples else None,
        "backlog_max": max(backlog) if backlog else None, "backlog_last": backlog[-1] if backlog else None,
        "samples_queued_max": max(queued) if queued else None,
        "backlog_growing": backlog_growing, "failed_requests": failed, "overrun_ticks": overruns,
        "max_start_lag_ms": max((v.get("max_start_lag_ms") or 0) for v in fst.values()) if fst else None,
        "feeder_done": feeder_tail,
    }
    sat = []
    if arm == "asap":
        if backlog_growing: sat.append("backlog growing")
        if failed: sat.append(f"{failed:.0f} failed Remote Write requests (503)")
    else:
        if rows_rate is not None and rows_rate < 0.95 * target: sat.append("rows inserted < 95% of target")
    if overruns and overruns > 0.05 * STEP_S:
        busy = cpu_rate is not None and cpu_rate > 0.9 * CORES
        sat.append("feeder overrun ticks" + ("" if busy or arm == "victoriametrics" else " (feeder-bound?)"))
    res["saturated"] = sat
    return res


def main():
    with open(OUT / "steps.jsonl", "a") as out:
        for w in WORKLOADS:
            for arm in ARMS:
                for n in SERIES:
                    r = step(w, n, arm)
                    r["time"] = time.strftime("%FT%TZ", time.gmtime())
                    # Stop on hard saturation only (overrun ticks alone are recorded, not a stop).
                    hard = [x for x in r.get("saturated", []) if "overrun" not in x]
                    ach = r.get("feeder_achieved_samples_per_s") or 0
                    if ach < 0.95 * r.get("target_samples_per_s", 0):
                        hard.append("feeder achieved < 95% of target")
                        r["saturated"].append("feeder achieved < 95% of target")
                    out.write(json.dumps(r) + "\n"); out.flush()
                    print(json.dumps(r), flush=True)
                    if r.get("error") or hard:
                        break


if __name__ == "__main__":
    main()
