#!/usr/bin/env python3
"""Build SUMMARY tables for each collected config of the 2026-11-20 campaign.

Reads campaign-2026-11-20/<config>/ (collected by collect.sh): per run
<tag>.demo_table.txt, <run>/<arm>/resources.json, <run>/asap/engine_metrics_end.txt,
<cfg>.meta. Prints Markdown to stdout.
"""
import os
import json, re, sys
from pathlib import Path

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else os.environ["ASAPQ_BENCH"] + "/campaign-2026-11-20")
MiB = 1024 * 1024
ENGINES = {"asap": ["data_plane"], "victoriametrics": ["victoria-metrics"], "prometheus": ["prometheus"]}


def metrics(path):
    out = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        if line.startswith("#") or " " not in line:
            continue
        k, v = line.rsplit(" ", 1)
        try:
            out[k] = float(v)
        except ValueError:
            pass
    return out


def msum(m, prefix, contains=None):
    return sum(v for k, v in m.items() if k.startswith(prefix) and (contains is None or contains in k))


def per_query(table):
    rows, on = [], False
    for line in table.splitlines():
        if line.startswith("Per query"):
            on = True; continue
        if on:
            if not line.strip():
                if rows: break
                continue
            m = re.match(r"\s+(\S+)\s+(asap|victoriametrics|prometheus)\s+(\d+)\s+([\d.]+|-)\s+([\d.]+|-)\s+(\d+)\s+(.*)", line)
            if m:
                rows.append(m.groups())
    return rows


def fmt(x, nd=1):
    return "-" if x is None else f"{x:.{nd}f}"


def run_tables(cfg_dir, tag_file):
    tag = tag_file.name[: -len(".demo_table.txt")]
    run = cfg_dir / tag
    table = tag_file.read_text()
    out = [f"#### {tag}", "", "```", table.splitlines()[0] if table else "", "```", ""]
    out.append("| arm | engine CPU ingest (core-s) | CPU during query window (core-s) | engine CPU total | "
               "ASAP query-exec CPU, whole run (phase query*) | ASAP Remote Write decode CPU, whole run | ASAP precompute CPU, whole run | "
               "peak RSS (MiB) | steady RSS (MiB) | disk write (MiB) | data dir (MiB) |")
    out.append("|" + "---|" * 11)
    anomalies = []
    for arm in ("asap", "victoriametrics", "prometheus"):
        if not (run / arm).exists():
            continue  # arm not in this run (2-arm runs have no Prometheus)
        rp = run / arm / "resources.json"
        if not rp.exists():
            out.append(f"| {arm} | (no resources.json) |" + " |" * 9); anomalies.append(f"{arm}: no resources.json")
            continue
        r = json.loads(rp.read_text())
        comps = r.get("components", {})
        ci = cq = pk = st = dw = 0.0
        for name in ENGINES[arm]:
            c = comps.get(name, {})
            ci += c.get("ingest_phase_cpu_s", 0); cq += c.get("query_phase_cpu_s", 0)
            pk = max(pk, c.get("peak_rss_bytes", 0)); st = max(st, c.get("steady_rss_bytes", 0))
            dw += c.get("ingest_phase_disk_write_bytes", 0) + c.get("query_phase_disk_write_bytes", 0)
        storage = sum(v for v in (r.get("storage_bytes") or {}).values() if isinstance(v, (int, float)))
        qx = rw = pc = None
        if arm == "asap":
            m = metrics(run / arm / "engine_metrics_end.txt")
            qx = msum(m, "asap_phase_cpu_seconds_total{phase=\"query")
            rw = msum(m, "asap_phase_cpu_seconds_total{phase=\"remote_write_decode\"")
            pc = msum(m, "asap_phase_cpu_seconds_total{phase=\"precompute\"")
            bl = m.get("asap_precompute_backlog_windows")
            sync = m.get("asap_view_sync_seconds_total")
            pers = m.get("asap_view_persisted_bytes_total")
            notp = m.get("asap_view_windows_not_persisted_total")
            thr = {k.split('"')[1]: v for k, v in m.items() if k.startswith("asap_thread_cpu_seconds_total")}
            anomalies.append(
                f"asap end-of-run: backlog_windows={fmt(bl,0)}, samples_queued={fmt(m.get('asap_samples_queued'),0)}, "
                f"view_bytes={fmt((m.get('asap_view_bytes') or 0)/MiB)} MiB, view_persisted={fmt((pers or 0)/MiB)} MiB, "
                f"view_sync={fmt(sync)} s, windows_not_persisted={fmt(notp,0)}, "
                f"threads(core-s)={', '.join(f'{k}={v:.1f}' for k, v in sorted(thr.items()))}")
            if bl and bl > 2:
                anomalies.append(f"**asap precompute backlog {bl:.0f} windows at end**")
        fs = run / arm / "feeder0-stats.json"
        if fs.exists():
            f = json.loads(fs.read_text())
            lag = f.get("max_start_lag_ms") or 0
            anomalies.append(f"{arm} feeder: achieved {f.get('achieved_rows_per_sec', 0):,.0f}/{f.get('target_rows_per_sec', 0):,} rows/s, "
                             f"overrun ticks {f.get('overrun_ticks')}, max start lag {lag / 1000:.1f} s, failed requests {f.get('failed_requests')}")
            if lag > 6000:
                anomalies.append(f"**{arm}: ingestion fell behind (feeder lag up to {lag / 1000:.0f} s > query delay); its in-run answers are stale**")
        out.append(f"| {arm} | {ci:.1f} | {cq:.1f} | {ci + cq:.1f} | {fmt(qx)} | {fmt(rw)} | {fmt(pc)} | "
                   f"{pk / MiB:.0f} | {st / MiB:.0f} | {dw / MiB:.1f} | {storage / MiB:.1f} |")
    out += ["", "| query | arm | n | mean ms | max ms | failed | accuracy |", "|---|---|---|---|---|---|---|"]
    for q, arm, n, mean, mx, failed, acc in per_query(table):
        out.append(f"| {q} | {arm} | {n} | {mean} | {mx} | {failed} | {acc.strip()} |")
        if int(failed) > 0:
            anomalies.append(f"**{q} {arm}: {failed} failed of {n}**")
        if arm == "asap" and "meets target: no" in acc:
            anomalies.append(f"**{q} asap misses the 5% target: {acc.strip()}**")
    acc = cfg_dir / f"{tag}.accuracy_vs_vm.txt"
    if acc.exists():
        out += ["", "ASAP accuracy against VictoriaMetrics as ground truth (tools/accuracy_vs_vm.py; post-run untimed "
                "query on VM's stored data when kept, rate/increase as *_prometheus; else VM's in-run answer):", ""]
        out += [l for l in acc.read_text().splitlines() if l.startswith("|")]
    plan = [l for l in table.splitlines() if l.startswith("planning")]
    if plan:
        out += ["", plan[0]]
    out += ["", "Anomalies / engine counters:", *[f"- {a}" for a in anomalies], ""]
    return out


def main():
    print("<!-- generated by tools/summarize.py -->")
    for cfg_dir in sorted(p for p in ROOT.iterdir() if p.is_dir() and p.name.startswith("c")):
        print(f"### {cfg_dir.name}\n")
        for meta in cfg_dir.glob("*.meta"):
            print("```\n" + meta.read_text().strip() + "\n```\n")
        tabs = sorted(cfg_dir.glob("*.demo_table.txt"))
        if not tabs:
            print("(no finished run collected)\n")
        for t in tabs:
            print("\n".join(run_tables(cfg_dir, t)))


if __name__ == "__main__":
    main()
