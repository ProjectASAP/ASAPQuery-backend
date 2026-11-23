#!/usr/bin/env python3
"""SUMMARY.md = tools/header.md + per-config tables (summarize.py) + ingest steps + typical-dashboard predictions."""
import os
import io, json, subprocess, sys
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(os.environ["ASAPQ_BENCH"] + "/campaign-2026-11-20")
TOOLS = ROOT / "tools"
sys.path.insert(0, str(TOOLS))
import summarize  # noqa: E402

# Config directory -> one-line label (backend, VM version, delay, arms).
LABELS = {
    "cdiag-1c67a13c-dh-s10000-range": "DIAGNOSTIC: 1c67a13c (result cache, per-request snapshot, sharded ingest) | VM v1.156.0 | delay 6 s | 2 arms concurrent | harness 5849215bcdea8c99",
    "c1-dh-s1000-instant": "c405b5d2 | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c1n-dh-s1000-instant-dae1ffef": "dae1ffef | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c1f-dh-s1000-instant": "dae1ffef | VM v1.156.0 | delay 6 s | 3 arms concurrent (final-style)",
    "c2-dh-s10000-instant": "c405b5d2 | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c2d6-dh-s10000-instant-delay6": "c405b5d2 | VM v1.155.0 | delay 6 s (diagnostic) | 3 arms concurrent",
    "c2n-dh-s10000-instant-dae1ffef": "dae1ffef | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c2nd6-dh-s10000-instant-dae1ffef-vm156-delay6": "dae1ffef | VM v1.156.0 | delay 6 s | 3 arms concurrent (final-style)",
    "c3-dh-s1000-range": "c405b5d2 | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c3n-dh-s1000-range-dae1ffef": "STALE for range: dae1ffef, before result cache / per-request snapshot; dae1ffef | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c3f-dh-s1000-range": "STALE for range: dae1ffef, before result cache / per-request snapshot; dae1ffef | VM v1.156.0 | delay 6 s | 2 arms concurrent (final-style)",
    "c4-dh-s10000-range": "c405b5d2 | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c4n-dh-s10000-range-dae1ffef": "STALE for range: dae1ffef, before result cache / per-request snapshot; dae1ffef | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c4f-dh-s10000-range": "STALE for range: dae1ffef, before result cache / per-request snapshot; dae1ffef | VM v1.156.0 | delay 6 s | 2 arms concurrent (final-style)",
    "c5-seed862-s10000-instant": "c405b5d2 | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c5n-seed862-s10000-instant-dae1ffef": "dae1ffef | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c5f-seed862-s10000-instant": "dae1ffef | VM v1.156.0 | delay 6 s | 3 arms concurrent (final-style)",
    "c6-typical-s10000-instant": "c405b5d2 | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c6n-typical-s10000-instant-dae1ffef": "dae1ffef | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c6f-typical-s10000-instant": "dae1ffef | VM v1.156.0 | delay 6 s | 3 arms concurrent (final-style)",
    "c6f60-typical60s-s10000-instant": "dae1ffef | VM v1.156.0 | delay 6 s | 3 arms concurrent (final-style)",
    "c8-seed862-s1000-instant": "c405b5d2 | VM v1.155.0 | delay 3 s | 3 arms concurrent",
    "c8f-seed862-s1000-instant": "dae1ffef | VM v1.156.0 | delay 6 s | 2 arms concurrent (final-style)",
}


def main():
    out = [(TOOLS / "header.md").read_text().rstrip(), ""]
    out.append("## Per-config tables\n")
    out.append("Columns: engine CPU in core-seconds over the ingest phase and over the query window "
               "(\"CPU during query window\": ingest continues during it); ASAP's query-execution, Remote Write "
               "decode and precompute CPU are whole-run counters from the data plane's /metrics. RSS peak/steady, "
               "disk bytes written by the engine, its data directory at the end. Latency is mean/max per query "
               "(ms); a failed query is a refusal (ASAP) or a 60 s harness timeout.\n")
    for d in sorted(p for p in ROOT.iterdir() if p.is_dir() and p.name[:1] == "c" and not p.name.startswith("c7")):
        out.append(f"### {d.name}\n")
        out.append(f"`{LABELS.get(d.name, '?')}`\n")
        for meta in sorted(d.glob("*.meta")):
            out.append("```\n" + meta.read_text().strip() + "\n```\n")
        tabs = sorted(d.glob("*.demo_table.txt"))
        if not tabs:
            out.append("(no finished run collected)\n")
        for t in tabs:
            out.append("\n".join(summarize.run_tables(d, t)))
    out.append("## Ingest upper bound (12 engine cores, ingest only, no queries)\n")
    buf = io.StringIO()
    with redirect_stdout(buf):
        subprocess.run  # noqa: B018
    r = subprocess.run(["python3", str(TOOLS / "ingest_table.py"), str(ROOT / "c7-ingest")],
                       capture_output=True, text=True)
    out.append(r.stdout)
    pred = TOOLS / "typical_predictions.md"
    if pred.exists():
        out.append(pred.read_text())
    (ROOT / "SUMMARY.md").write_text("\n".join(out) + "\n")
    print(ROOT / "SUMMARY.md")


if __name__ == "__main__":
    main()
