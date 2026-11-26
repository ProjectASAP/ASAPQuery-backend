#!/usr/bin/env python3
"""sweep_fit.py <run_dir>...: from VictoriaMetrics-alone sweep steps (queries from second 0, 1 user),
fit CPU(t) = ingest + sum_q k_q * min(t, w_q) per step, and project the CPU at the end of a 6 h run
per scale. Only minutes before the engine saturates (CPU < 44 cores, feeder lag < 5 s) are used.

k_q: each query's share of the query CPU is taken proportional to its latency x its request rate
(latency ~ core-seconds / cores used per request); the shares are scaled so that the sum matches
the measured CPU growth. A query's cost is assumed to grow linearly with the data in its window
(min(t, window)); range query q05 reads span + window."""
import os
import json
import statistics
import sys
from pathlib import Path

RUN_H = float(__import__("os").environ.get("RUN_H", "3"))
CAP = 48.0


def minutes(arm):
    cg = [json.loads(l) for l in open(arm / "cgroup_samples.jsonl")]
    cs = [(c["t"], c["cpu_usage_usec"]) for c in cg if "cpu_usage_usec" in c]
    t0 = cs[0][0]
    out, i = {}, 0
    for j in range(len(cs)):
        if cs[j][0] - cs[i][0] >= 60:
            out[int((cs[j][0] - t0) // 60)] = (cs[j][1] - cs[i][1]) / 1e6 / (cs[j][0] - cs[i][0])
            i = j
    return out, t0


def feeder_lag_by_minute(arm, t0):
    lag = {}
    for f in arm.glob("feeder*-stats.json"):
        st = json.load(open(f))
        for k, t in enumerate(st.get("ticks", [])):
            m = k // 60
            lag[m] = max(lag.get(m, 0), t.get("start_lag_ms", 0) / 1e3)
    return lag


rows = []
for run in map(Path, sys.argv[1:]):
    arm = run / "victoriametrics"
    r = json.load(open(arm / "resources.json"))
    qs = {q["id"]: q for q in json.load(open(next((run.parent.parent / "prep").glob(f"demo.demo48.s{r['series']}")) / "queries.json"))["queries"]} \
        if False else None
    prep = Path(os.environ["ASAPQ_BENCH"] + "/prep") / f"demo.demo48.s{r['series']}"
    qs = {q["id"]: q for q in json.load(open(prep / "queries.json"))["queries"]}
    cpu, t0 = minutes(arm)
    lag = feeder_lag_by_minute(arm, t0)
    ok = sorted(m for m, c in cpu.items() if c < 44 and lag.get(m, 0) < 5 and m >= 2)
    scale = r["series"] / 40000
    # latency per query over the unsaturated minutes, per minute of window fill
    ans = [json.loads(l) for l in open(arm / "answers.jsonl")]
    start = r["start_ms"]
    w = {}
    for a in ans:
        if a["latency_ms"] is None or a["status"] != 200:
            continue
        m = a["offset_ms"] // 60000
        if m in ok:
            q = qs[a["query"]]
            fill = min(a["offset_ms"], q["lookback_ms"]) / 60000  # minutes of data read
            if fill > 0:
                w.setdefault(a["query"], []).append(a["latency_ms"] / 1e3 / fill / (q["interval_ms"] / 1e3))
    weight = {q: statistics.median(v) for q, v in w.items()}
    # CPU growth over unsaturated minutes: linear fit CPU = a + b * t (t in minutes)
    xs, ys = ok, [cpu[m] for m in ok]
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
    a = my - b * mx
    # b = sum_q k_q for queries whose window is still filling (all with window > the fitted range)
    filling = [q for q in weight if qs[q]["lookback_ms"] / 60000 > xs[-1]]
    tot = sum(weight[q] for q in filling)
    k = {q: b * weight[q] / tot for q in filling}
    # queries already full in the fit range: their cost is in `a`
    end_min = RUN_H * 60
    proj = a + sum(k[q] * (min(end_min, qs[q]["lookback_ms"] / 60000) - 0) for q in k) - sum(k[q] * 0 for q in k)
    rows.append((scale, r["series"], a, b, xs[0], xs[-1], proj, k, qs))
    print(f"## s{r['series']} (scale {scale:g}): unsaturated minutes {xs[0]}..{xs[-1]} ({n}); CPU = {a:.1f} + {b:.2f}/min * t")
    print(f"   CPU at minutes: " + " ".join(f"{m}:{cpu[m]:.1f}" for m in sorted(cpu)))
    print(f"   feeder lag max per minute: " + " ".join(f"{m}:{lag[m]:.0f}" for m in sorted(lag) if m % 3 == 0))
    top = sorted(k.items(), key=lambda kv: -kv[1] * min(end_min, qs[kv[0]]["lookback_ms"] / 60000))[:6]
    print("   projected end-of-run CPU: %.0f cores; largest at the end: %s" % (proj, ", ".join(
        f"{q} {v * min(end_min, qs[q]['lookback_ms'] / 60000):.0f}" for q, v in top)))
if rows:
    print("\nProjection by scale (CPU linear in scale, fitted per step):")
    for scale, series, a, b, x0, x1, proj, k, qs in rows:
        target = scale * CAP * 0.9 / proj
        print(f"  from s{series}: end-of-run CPU {proj:.0f} cores at scale {scale:g}; scale for ~43 cores "
              f"(90% of 48): {target:.3f} (SERIES ~ {int(target * 40000 // 200 * 200)})")
