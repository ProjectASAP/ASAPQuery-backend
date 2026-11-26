#!/usr/bin/env python3
"""sweep_summary.py <run_dir>...: one block per sweep step (one arm per run dir, usually
victoriametrics): CPU (average and per-minute max cores from cgroup cpu.stat samples, cpu.max),
memory (working set, anon, peak), ingest (feeders' achieved rate vs target, max start lag,
final lag, overrun ticks), and per query: requests, mean/p50/p99/max latency, timeouts,
rejections (non-200), slow (> interval)."""
import json
import statistics
import sys
from pathlib import Path


def pct(v, p):
    v = sorted(x for x in v if x is not None)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))] if v else None


def fmt(x, f=".1f"):
    return "-" if x is None else format(x, f)


for run in map(Path, sys.argv[1:]):
    q = json.load(open(next(run.parent.glob(f"../prep/*")) if False else run / "victoriametrics" / "resources.json")) \
        if (run / "victoriametrics").exists() else None
    for arm_dir in sorted(d for d in run.iterdir() if d.is_dir() and (d / "resources.json").exists()):
        r = json.load(open(arm_dir / "resources.json"))
        print(f"## {run.name} / {arm_dir.name}: series(data)={r['series']}, users={r['query_users']}, "
              f"run {r['query_phase_s']:.0f} s, wall {r['wall_seconds']:.0f} s")
        cg = [json.loads(l) for l in open(arm_dir / "cgroup_samples.jsonl")]
        cs = [(c["t"], c["cpu_usage_usec"]) for c in cg if "cpu_usage_usec" in c]
        if len(cs) > 1:
            avg = (cs[-1][1] - cs[0][1]) / 1e6 / (cs[-1][0] - cs[0][0])
            per_min, i = [], 0
            for j in range(len(cs)):
                while cs[j][0] - cs[i][0] > 60:
                    i += 1
                if cs[j][0] - cs[i][0] >= 50:
                    per_min.append((cs[j][1] - cs[i][1]) / 1e6 / (cs[j][0] - cs[i][0]))
            last5 = [(b[1] - a[1]) / 1e6 / (b[0] - a[0]) for a, b in zip(cs, cs[1:]) if b[0] - cs[-1][0] > -300]
            print(f"  CPU: average {avg:.1f} cores, max 1-min {fmt(max(per_min, default=None))} cores, "
                  f"last 5 min {fmt(statistics.mean(last5) if last5 else None)} cores (cpu.max {r['limits'] and r['limits'].get('cpu_cores')})")
        else:
            comps = r["components"]
            tot = sum(c["ingest_phase_cpu_s"] + c["query_phase_cpu_s"] for k, c in comps.items() if k != "harness")
            print(f"  CPU: {tot:.0f} core-s over {r['wall_seconds']:.0f} s")
        m = r["memory"]
        ws = [c.get("working_set") for c in cg]
        an = [c.get("anon") for c in cg]
        print(f"  memory: working set steady {fmt((m.get('working_set_steady_bytes') or 0) / 2**30, '.2f')} GiB, "
              f"max {fmt(max(filter(None, ws), default=0) / 2**30, '.2f')} GiB; anon max "
              f"{fmt(max(filter(None, an), default=0) / 2**30, '.2f')} GiB; cgroup peak "
              f"{fmt((m.get('current_peak_bytes') or m.get('memory_peak_bytes') or 0) / 2**30, '.2f')} GiB; "
              f"events {m.get('memory_events')}")
        target, achieved, lags, final, over = 0, 0, [], [], 0
        for f in sorted(arm_dir.glob("feeder*-stats.json")):
            st = json.load(open(f))
            ticks = st.get("ticks", [])
            achieved += st.get("achieved_rows_per_sec") or 0
            lags.append(max((t.get("start_lag_ms", 0) for t in ticks), default=0))
            final.append(ticks[-1].get("start_lag_ms", 0) if ticks else None)
            over += sum(1 for t in ticks if t.get("elapsed_ms", 0) > 1000)
        print(f"  ingest: feeders achieved {achieved / 1e6:.2f}M samples/s; start lag max {fmt(max(lags, default=0) / 1e3)} s, "
              f"at the end {fmt(max(filter(None, final), default=0) / 1e3)} s; overrun ticks {over}; "
              f"rows ignored/pending: see engine counters")
        net = r.get("network") or {}
        if net:
            print(f"  network rx mean {fmt(net.get('query_rx_bytes_per_s_mean', 0) / 1e6)} MB/s, max "
                  f"{fmt(net.get('query_rx_bytes_per_s_max', 0) / 1e6)} MB/s")
        rows = [json.loads(l) for l in open(arm_dir / "answers.jsonl")]
        qs = {}
        for a in rows:
            qs.setdefault(a["query"], []).append(a)
        print("  | query | n | mean s | p50 s | p99 s | max s | timeouts | rejected | slow (> interval) |")
        print("  |---|---|---|---|---|---|---|---|---|")
        intervals = {}
        prep_q = arm_dir.parent / "queries.json"
        for qid, qr in sorted(qs.items()):
            lat = [a["latency_ms"] / 1e3 for a in qr if a["latency_ms"] is not None]
            to = sum(1 for a in qr if a["status"] is None and "timed out" in str((a.get("result") or {}).get("error", "")))
            rej = sum(1 for a in qr if a["status"] is not None and a["status"] != 200)
            iv = 10 if qid.startswith(("q01", "q03")) else 15
            slow = sum(1 for x in lat if x > iv)
            print(f"  | {qid} | {len(qr)} | {fmt(statistics.mean(lat) if lat else None, '.2f')} | {fmt(pct(lat, 50), '.2f')} | "
                  f"{fmt(pct(lat, 99), '.2f')} | {fmt(max(lat, default=None), '.2f')} | {to} | {rej} | {slow} |")
        print()
