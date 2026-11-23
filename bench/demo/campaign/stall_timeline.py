#!/usr/bin/env python3
"""Per-minute timeline of an arm: persistence write volume, dirty/writeback, reclaim
pressure and the ingest/precompute/persist queues, to see whether writes plus reclaim
stall ingest.  usage: stall_timeline.py <run dir>/<arm>"""
import json, sys
from collections import defaultdict
from pathlib import Path

arm = Path(sys.argv[1])
eng = [json.loads(l) for l in open(arm / "engine_counters.jsonl")] if (arm / "engine_counters.jsonl").exists() else []
cg = [json.loads(l) for l in open(arm / "cgroup_samples.jsonl")] if (arm / "cgroup_samples.jsonl").exists() else []
t0 = min([e["t"] for e in eng] + [c["t"] for c in cg] or [0])
minutes = defaultdict(dict)


def put(minute, key, value, how):
    d = minutes[minute]
    if key not in d:
        d[key] = value
    elif how == "max":
        d[key] = max(d[key], value)
    elif how == "last":
        d[key] = value


for e in eng:
    m = int((e["t"] - t0) // 60)
    put(m, "phase", e["phase"], "last")
    mm = e["m"]
    put(m, "persisted_bytes_total", mm.get("asap_view_persisted_bytes_total", mm.get('vm_rows_inserted_total{type="promremotewrite"}', 0.0)), "last")
    for k, name in (("asap_watermark_lag_seconds", "wm_lag_s"), ("asap_precompute_backlog_windows", "precompute_backlog"),
                    ("asap_samples_queued", "samples_queued"), ("asap_view_bytes", "view_bytes")):
        if k in mm:
            put(m, name, mm[k], "max")
    for k, v in mm.items():
        if k.startswith("asap_persist_backlog"):
            put(m, k.replace("asap_", ""), v, "max")
for c in cg:
    m = int((c["t"] - t0) // 60)
    put(m, "dirty_wb_mib", (c.get("file_dirty", 0) + c.get("file_writeback", 0)) / 2**20, "max")
    put(m, "working_set_mib", c.get("working_set", 0) / 2**20, "max")
    put(m, "inactive_file_mib", c.get("inactive_file", 0) / 2**20, "max")
    put(m, "io_wbytes", c.get("io_wbytes", 0), "last")
prev_p = prev_w = None
cols = ["min", "phase", "persist_MiB/min", "io_write_MiB/min", "dirty+wb_MiB", "working_set_MiB", "inactive_file_MiB",
        "wm_lag_s", "precompute_backlog", "samples_queued", "persist_backlog"]
print("| " + " | ".join(cols) + " |")
print("|" + "---|" * len(cols))
for m in sorted(minutes):
    d = minutes[m]
    p, w = d.get("persisted_bytes_total"), d.get("io_wbytes")
    dp = (p - prev_p) / 2**20 if (p is not None and prev_p is not None) else None
    dw = (w - prev_w) / 2**20 if (w is not None and prev_w is not None) else None
    prev_p, prev_w = p if p is not None else prev_p, w if w is not None else prev_w
    pb = {k: v for k, v in d.items() if k.startswith("persist_backlog")}
    f = lambda v, n=0: "-" if v is None else f"{v:.{n}f}"  # noqa: E731
    print(f"| {m} | {d.get('phase', '')} | {f(dp)} | {f(dw)} | {f(d.get('dirty_wb_mib'))} | {f(d.get('working_set_mib'))} | "
          f"{f(d.get('inactive_file_mib'))} | {f(d.get('wm_lag_s'), 1)} | {f(d.get('precompute_backlog'))} | "
          f"{f(d.get('samples_queued'))} | {', '.join(f'{k}={v:g}' for k, v in pb.items()) or '-'} |")
