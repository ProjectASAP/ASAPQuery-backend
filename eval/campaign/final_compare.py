#!/usr/bin/env python3
"""final_compare.py <run_dir> [checkpoint_h ...]: ASAP vs VictoriaMetrics for one final demo run.
Per arm: CPU core-seconds since 0 and cores over the last 5 min (cgroup cpu.stat), working set /
anon / sampled peak, disk in use and bytes written (cgroup io.stat), feeder start lag per 30 min,
per query latency (mean/p50/p99/max) and status (ok, 429, other HTTP, timeout, slow > interval),
goodput (report.json), up to each checkpoint (default 3 and 4 h)."""
import collections
import json
import sys
from pathlib import Path

run = Path(sys.argv[1])
cps = [float(x) for x in sys.argv[2:]] or [3.0, 4.0]
report = json.load(open(run / "report.json")) if (run / "report.json").exists() else {"arms": {}}


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))] if v else None


for arm in ("asap", "victoriametrics"):
    d = run / arm
    if not (d / "resources.json").exists():
        continue
    r = json.load(open(d / "resources.json"))
    start_s = r["start_ms"] / 1e3
    cg = [json.loads(l) for l in open(d / "cgroup_samples.jsonl")]
    print(f"\n######## {arm} (users {r['query_users']}, engine {r.get('engine_host')})")
    for cp in cps:
        end = start_s + cp * 3600
        s = [c for c in cg if c["t"] <= end + 5]
        if not s:
            continue
        cpu = s[-1].get("cpu_usage_usec", 0) / 1e6
        last5 = [c for c in s if c["t"] >= s[-1]["t"] - 300]
        cores_end = (last5[-1]["cpu_usage_usec"] - last5[0]["cpu_usage_usec"]) / 1e6 / max(1, last5[-1]["t"] - last5[0]["t"])
        ws = s[-1].get("working_set", 0) / 2**20
        anon = s[-1].get("anon", 0) / 2**20
        peak = max(c.get("current", 0) for c in s) / 2**20
        wspeak = max(c.get("working_set", 0) for c in s) / 2**20
        wb = s[-1].get("io_wbytes", 0) / 2**30
        print(f"  @ {cp:g} h: CPU {cpu:,.0f} core-s since 0 (avg {cpu / (s[-1]['t'] - s[0]['t'] + 1e-9):.1f} cores), "
              f"{cores_end:.1f} cores over the last 5 min; working set {ws:,.0f} MiB (peak {wspeak:,.0f}), anon {anon:,.0f} MiB, "
              f"cgroup peak {peak:,.0f} MiB; disk bytes written {wb:.1f} GiB")
    du = r.get("data_dir_usage") or {}
    print("  disk in use at end: " + ", ".join(f"{k} {v['used_bytes'] / 2**30:.1f} GiB" for k, v in du.items() if v))
    me = (r.get("memory") or {}).get("memory_events")
    print(f"  memory events: {me}; exit codes {r.get('engine_exit_codes')}")
    # feeder lag per 30 min
    lag = collections.defaultdict(float)
    for f in d.glob("feeder*-stats.json"):
        for k, t in enumerate(json.load(open(f)).get("ticks", [])):
            lag[k // 1800] = max(lag[k // 1800], t.get("start_lag_ms", 0) / 1e3)
    print("  feeder start lag, max per 30 min (s): " + " ".join(f"{(k + 1) * 0.5:g}h:{v:.1f}" for k, v in sorted(lag.items())))
    rows = [json.loads(l) for l in open(d / "answers.jsonl")]
    late = sorted(x["send_lateness_ms"] for x in rows if x.get("send_lateness_ms") is not None)
    if late:
        print(f"  send lateness (actual - scheduled send), ms: p50 {pct(late, 50):.0f}, p90 {pct(late, 90):.0f}, "
              f"p99 {pct(late, 99):.0f}, max {late[-1]:.0f} (n {len(late)}; in-flight limit {r.get('query_max_inflight')})")
    else:
        print("  send lateness: not recorded (run before 2026-11-23)")
    for cp in cps:
        rs = [x for x in rows if x["offset_ms"] <= cp * 3600e3]
        by = collections.defaultdict(list)
        for x in rs:
            by[x["query"]].append(x)
        tot = collections.Counter()
        print(f"  -- requests up to {cp:g} h --")
        print("  | query | n | ok | 429 | other err | timeout | slow | mean ms | p50 | p99 | max |")
        for q, xs in sorted(by.items()):
            iv = 10000 if q.startswith(("q01", "q03")) else 15000
            lat = [x["latency_ms"] for x in xs if x["latency_ms"] is not None]
            to = sum(1 for x in xs if x["status"] is None)
            e429 = sum(1 for x in xs if x["status"] == 429)
            oth = sum(1 for x in xs if x["status"] not in (None, 200, 429))
            slow = sum(1 for x in xs if x["status"] == 200 and x["latency_ms"] and x["latency_ms"] > iv)
            ok = sum(1 for x in xs if x["status"] == 200) - slow
            tot.update(n=len(xs), ok=ok, e429=e429, oth=oth, to=to, slow=slow)
            print(f"  | {q} | {len(xs)} | {ok} | {e429} | {oth} | {to} | {slow} | {sum(lat) / max(1, len(lat)):.0f} | "
                  f"{pct(lat, 50) or 0:.0f} | {pct(lat, 99) or 0:.0f} | {max(lat, default=0):.0f} |")
        print(f"  total: {dict(tot)}")
    ra = report["arms"].get(arm, {})
    tot = {k: v for k, v in (ra.get("classes") or {}).items()} if isinstance(ra.get("classes"), dict) else ra.get("classes")
    print(f"  report.py classes (whole run): {tot}; goodput {ra.get('goodput_per_s')} /s ({ra.get('goodput_fraction')})")
