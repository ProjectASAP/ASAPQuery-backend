#!/usr/bin/env python3
"""live_status.py <run_dir>: the arm in progress: per query status counts, mean/max latency (last 10 min),
the worst feeder start lag now, and errors seen."""
import json, sys, time, collections
from pathlib import Path
run = Path(sys.argv[1])
for arm in sorted(d for d in run.iterdir() if d.is_dir()):
    ans = arm / "answers.jsonl"
    if not ans.exists():
        continue
    rows = [json.loads(l) for l in open(ans)]
    if not rows:
        continue
    last = max(r["offset_ms"] for r in rows)
    print(f"== {arm.name}: {len(rows)} answers, latest evaluation at +{last / 60000:.1f} min")
    c = collections.defaultdict(collections.Counter); lat = collections.defaultdict(list); err = {}
    for r in rows:
        c[r["query"]][r["status"]] += 1
        if r["offset_ms"] > last - 600000 and r["latency_ms"] is not None:
            lat[r["query"]].append(r["latency_ms"])
        if r["status"] != 200:
            err[r["query"]] = str((r.get("result") or {}).get("error"))[:150]
    for q in sorted(c):
        l = lat[q]
        print(f"  {q:24s} {dict(c[q])}  last10min mean {sum(l) / max(1, len(l)):.0f} ms max {max(l, default=0):.0f} ms")
    if err:
        print("  errors:", json.dumps(err)[:1500])
    lags = []
    for f in arm.glob("feeder*.log"):
        tail = open(f).read().splitlines()[-1:] or [""]
        if "start lag" in tail[0]:
            lags.append(float(tail[0].split("start lag ")[1].split(" ms")[0]) / 1e3)
    print(f"  feeder start lag now: {max(lags, default=0):.1f} s ({len(lags)} feeders)")
    late = sorted(r["send_lateness_ms"] for r in rows if r.get("send_lateness_ms") is not None and r["offset_ms"] > last - 600000)
    if late:
        print(f"  send lateness last 10 min: p50 {late[len(late) // 2]:.0f} ms, p99 {late[int(0.99 * (len(late) - 1))]:.0f} ms, max {late[-1]:.0f} ms")
