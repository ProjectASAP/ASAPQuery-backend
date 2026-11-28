#!/usr/bin/env python3
"""vm_stale.py <run_dir> [arm]: per query, the requests the arm (default victoriametrics) received
before its own ingestion had reached the evaluation time T: its answer then misses the newest
T - ingested_through of data (stale). Ingested-through at wall time w is start + 1 s for every tick
all feeders had finished sending by w (a tick's samples are accepted when its requests return).
Send time of a request: T + query delay + user u's spread (u / users of the interval)."""
import bisect
import collections
import json
import sys
from pathlib import Path

run = Path(sys.argv[1])
arm = sys.argv[2] if len(sys.argv) > 2 else "victoriametrics"
d = run / arm
r = json.load(open(d / "resources.json"))
start, delay, users = r["start_ms"] / 1e3, r["query_delay_s"], r["query_users"]
ends = None
for f in sorted(d.glob("feeder*-stats.json")):
    st = json.load(open(f))
    e = []
    for t in st["ticks"]:
        e.append(start + t["tick"] + (t.get("start_lag_ms", 0) + t.get("elapsed_ms", 0)) / 1e3)
    # finishing times are not monotone in principle; ingested-through needs all ticks <= k done
    run_max, out = 0.0, []
    for x in e:
        run_max = max(run_max, x)
        out.append(run_max)
    ends = out if ends is None else [max(a, b) for a, b in zip(ends, out)][:min(len(ends), len(out))]
queries = {q["id"]: q for q in json.load(open(run.parent.parent / "prep" / "demo.demo48.s2800" / "queries.json"))["queries"]} \
    if (run.parent.parent / "prep" / "demo.demo48.s2800" / "queries.json").exists() else {}
stale, total, behind = collections.Counter(), collections.Counter(), collections.defaultdict(list)
for line in open(d / "answers.jsonl"):
    a = json.loads(line)
    q = a["query"]
    iv = queries.get(q, {}).get("interval_ms", 15000) / 1e3
    T = start + a["offset_ms"] / 1e3
    send = T + delay + a.get("user", 0) * iv / users
    k = bisect.bisect_right(ends, send)  # ticks fully ingested by send
    through = start + k
    total[q] += 1
    if through < T:
        stale[q] += 1
        behind[q].append(T - through)
print(f"{run.name}/{arm}: requests sent before the arm had ingested up to T (stale), per query")
print("| query | requests | stale | stale % | data missing at T: median / max (s) |")
for q in sorted(total):
    b = sorted(behind[q])
    print(f"| {q} | {total[q]} | {stale[q]} | {100 * stale[q] / total[q]:.1f}% | "
          f"{(b[len(b) // 2] if b else 0):.0f} / {(b[-1] if b else 0):.0f} |")
print(f"total: {sum(stale.values())} of {sum(total.values())} stale")
