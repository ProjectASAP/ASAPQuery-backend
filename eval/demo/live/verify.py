#!/usr/bin/env python3
"""Summarize the requests Grafana sent through the live proxies
(demo/live/logs/requests-*.jsonl): per data source and query, how many were
answered, refused or failed, which start/end/step they carried (steps per
request, whether start and end are on the step grid, how far end was behind
the wall clock), and the refusal reasons."""
import argparse
import collections
import glob
import json
import os
import time


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--logs", required=True)
    ap.add_argument("--since-minutes", type=float, default=60)
    ap.add_argument("--brief", action="store_true")
    args = ap.parse_args()
    since = time.time() - args.since_minutes * 60
    groups = collections.defaultdict(list)
    for path in sorted(glob.glob(os.path.join(args.logs, "requests-*.jsonl"))):
        for line in open(path):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r["t"] >= since:
                groups[(r["ds"], r["path"], r["query"])].append(r)
    if not groups:
        print(f"no Grafana requests in the last {args.since_minutes:g} min")
        return
    print(f"requests in the last {args.since_minutes:g} min")
    for (ds, path, query), rows in sorted(groups.items()):
        status = collections.Counter("client_gone" if r.get("client_gone") else r["status"] for r in rows)
        ok = status.get(200, 0)
        line = f"  {ds:20} {path.rsplit('/', 1)[-1]:12} {query[:60]:60} n={len(rows):4} ok={ok:4} other={dict((k, v) for k, v in status.items() if k != 200)}"
        print(line)
        if args.brief:
            continue
        ranges = [r for r in rows if r.get("step")]
        if ranges:
            steps = collections.Counter(r["step"] for r in ranges)
            spans = collections.Counter(round(float(r["end"]) - float(r["start"])) for r in ranges)
            step_s = [float(str(r["step"]).rstrip("s")) for r in ranges]
            on_grid = sum(1 for r, s in zip(ranges, step_s)
                          if abs(float(r["start"]) / s - round(float(r["start"]) / s)) < 1e-6
                          and abs(float(r["end"]) / s - round(float(r["end"]) / s)) < 1e-6)
            lags = [r["t"] - float(r["end"]) for r in ranges]
            lat = sorted(r["latency_ms"] for r in ranges)
            print(f"      step {dict(steps)}  span_s {dict(spans)}  start&end on step grid {on_grid}/{len(ranges)}  "
                  f"end behind wall clock {min(lags):.1f}..{max(lags):.1f} s  latency {lat[0]:.0f}..{lat[-1]:.0f} ms "
                  f"(mean {sum(lat) / len(lat):.0f})")
            last = ranges[-1]
            print(f"      last: start={last['start']} end={last['end']} step={last['step']} status={last['status']} "
                  f"execution={last.get('execution')}")
        for reason, n in collections.Counter(r.get("error", "")[:200] for r in rows if r["status"] != 200).items():
            print(f"      {n} x {r_status(rows, reason)}: {reason}")


def r_status(rows, reason):
    return next(r["status"] for r in rows if r["status"] != 200 and r.get("error", "")[:200] == reason)


if __name__ == "__main__":
    main()
