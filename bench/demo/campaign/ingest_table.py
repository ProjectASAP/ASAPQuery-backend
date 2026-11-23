#!/usr/bin/env python3
"""Markdown tables of the ingest upper-bound steps (c7-ingest*/steps.jsonl)."""
import os
import json, sys
from pathlib import Path

root = Path(sys.argv[1] if len(sys.argv) > 1 else os.environ["ASAPQ_BENCH"] + "/campaign-2026-11-20/c7-ingest")
for f in sorted(root.glob("*/steps.jsonl")):
    print(f"#### {f.parent.name}\n")
    print("| workload | arm | series | offered samples/s | achieved samples/s | engine CPU (cores) | peak RSS (MiB) "
          "| backlog max | queued max | 503s | overrun ticks | saturation |")
    print("|" + "---|" * 12)
    for line in f.read_text().splitlines():
        r = json.loads(line)
        cores = r.get("engine_cpu_cores")
        if cores is None and r.get("engine_cpu_total_s"):
            cores = r["engine_cpu_total_s"] / 240  # whole step (feed 240 s)
        rss = r.get("engine_peak_rss_bytes") or 0
        print(f"| {r['workload']} | {r['arm']} | {r['series']} | {r['target_samples_per_s']:,} | "
              f"{(r.get('feeder_achieved_samples_per_s') or 0):,.0f} | {cores if cores is None else round(cores, 2)} | "
              f"{rss / 2**20:.0f} | {r.get('backlog_max')} | {r.get('samples_queued_max')} | "
              f"{r.get('failed_requests')} | {r.get('overrun_ticks')} | {'; '.join(r.get('saturated') or []) or 'none'} |")
    print()
