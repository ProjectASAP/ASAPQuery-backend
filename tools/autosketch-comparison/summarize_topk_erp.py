#!/usr/bin/env python3
"""Validate measured artifacts and produce report/figures without hand-entered results."""
import argparse
import json
import statistics
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

LABELS = {"autosketch_per_query": "AutoSketch per query", "asapplanner_erp": "ASAP ERP",
          "asapplanner_erp_no_sharing": "ASAP ERP no sharing", "asapplanner_analytical": "ASAP analytical", "exact_hash_scan": "Exact hash scan"}


def runtime(row):
    t = row["timing"]
    keys = ["update_seconds", "eviction_seconds"]
    keys += ["exact_query_seconds"] if not row["configs"] else ["merge_seconds", "topk_readout_seconds"]
    return sum(t[k] for k in keys)


def summarize(document):
    grouped = {}
    assert document["schema_version"] == 2
    assert len(document["trials"]) == 3
    args = document["args"]
    expected = {(end, w) for end in range(args["calibration_panes"] + 1, args["calibration_panes"] + args["refreshes"] + 1) for w in [2, 10, 30, 120]}
    for trial in document["trials"]:
        for row in trial["rows"]:
            assert row["planning_seconds"] >= 0
            samples = row["query_samples"]
            assert len(samples) == len(expected)
            assert {(s["end_pane"], s["window_panes"]) for s in samples} == expected
            if row["configs"]:
                assert abs(sum(s["merge_seconds"] for s in samples) - row["timing"]["merge_seconds"]) < 1e-7
                assert abs(sum(s["readout_seconds"] for s in samples) - row["timing"]["topk_readout_seconds"]) < 1e-7
                assert row["accuracy_violations"] == sum(s["recall"] < args["min_recall_at_10"] for s in samples)
                assert row["logical_payload_bytes"] <= args["total_memory_budget_bytes"]
            grouped.setdefault(row["method"], []).append(row)
    result = []
    for method, rows in grouped.items():
        med = lambda f: statistics.median(f(r) for r in rows)
        result.append({"method": method, "label": LABELS.get(method, method), "planning": med(lambda r:r["planning_seconds"]),
                       "runtime": med(runtime), "memory": med(lambda r:r["logical_payload_bytes"]) / 1048576,
                       "recall": med(lambda r:r["mean_recall_at_10"]), "violations": med(lambda r:r["accuracy_violations"]),
                       "any_violations": any(r["accuracy_violations"] for r in rows),
                       "update": med(lambda r:r["timing"]["update_seconds"]), "merge": med(lambda r:r["timing"]["merge_seconds"])})
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("directory", type=Path)
    args = p.parse_args()
    root = args.directory
    text = ["# Measured ERP Top-K dashboard comparison (v2)", "",
            "Supersedes the withdrawn v1 measurements. Values below are generated from release-mode raw data; they are not smoke-test results.", "",
            "Four TopK(10) frequency queries cover the latest 1, 5, 15, and 60 minutes. A new 30-second pane arrives before every dashboard refresh. Each trial evaluates 100 refreshes (400 panel queries), with exactly the same endpoints for all methods. Calibration uses the first 120 panes; held-out endpoints are 121–220. Update time includes warm-up and ingestion of all 220 panes. Events update sketches in their original order, one event per update.", "",
            "Synthetic: 10,000,000 events from a truncated Zipf(1.1), domain size 100,000, seeds 42–44. Independent profile seeds are 1000–1002; the uniform alternative uses 2000–2002. Google: collection_id frequencies from instance_usage, start_time timestamps, 30-second panes, the previously selected consecutive interval starting at pane 77774. The committed replay has 1,971 events and 683 keys. This sparse single-shard interval is not representative of full Google production traffic.", "",
            "ERP consumes a persisted catalog in sketch-bench's ERP-v1 record format plus window-loss and empirical-shape metadata. The backend Top-K benchmark adapter generates these window-conditioned records because atomic frequency error is insufficient evidence for merged Top-K recall. Shape matching compares observed cardinality, events per pane, and top-1/10/100/1000 probability mass; it does not hard-classify a trace as Zipf or uniform. Distance >0.5 or an ambiguity margin <0.05 rejects a match. ASAPPlanner's ErpArtifact.select selects the least retained-memory measured configuration passing all relevant window-loss constraints. Full ERP compares shared versus independent pane layouts; the no-sharing arm optimizes each window independently.", "",
            "Both empirical methods minimize logical retained memory and use the same 72 configurations: CMS or CountSketch, depth {3,5,7}, width {128,256,512,1024}, heap {16,32,64}. Per-instance budget is 128 KiB and total retained-sketch budget is 16 MiB. AutoSketch uses per-family discrete LHS, numeric-neighbor search, direction stopping and memory pruning; it benchmarks each window independently. This adapts Algorithm 4 to CPU sketches with a heap-capacity dimension, omitting P4 stage/ALU allocation. Analytical sizing rounds upward to the shared grid and supplies additive error parameters; those bounds do not certify Recall@10.", "",
            "Accuracy target is Recall@10 ≥0.8 on each observed endpoint. Boundary ties are exchangeable only in remaining boundary slots; missing a strictly heavier key still counts as an error. Benchmark evidence is empirical, not a guarantee on unseen data. Any nonzero held-out violation count marks the point as failing the per-query target; do not compare it as an accuracy-equivalent winner.", "",
            "Memory is a common logical proxy: counters plus 32 bytes per heap slot, multiplied by retained panes. It excludes heap strings, allocator overhead and transient query state. Exact retains raw u32 keys and performs a timed hash-group scan; it is a reference microbenchmark, not the production backend exact path. Exact can exceed the approximate deployment budget and is marked separately.", ""]
    text += ["The uniform alternative catalog uses 1,000,000 generated events (545,500 calibration events); its lower per-pane rate is part of matching and it is not treated as interchangeable with the 10M-event Zipf scenario.", "",
             "Algorithm reference: [AutoSketch, §5 and Algorithm 4](https://www.usenix.org/system/files/nsdi24-sun.pdf). Data source: [Google instance_usage shard](https://storage.googleapis.com/clusterdata_2019_a/instance_usage-000000000000.parquet.gz); download/checksum instructions are in download_google_cluster_data_2019.sh.", ""]
    for name in ["synthetic", "google"]:
        document = json.loads((root/f"{name}.json").read_text())
        rows = summarize(document)
        # Loading is measured once per executable invocation and charged in
        # full to each ERP arm, rather than silently omitted from planning.
        text += [f"{name.title()} catalog loading: {document['catalog_load_seconds']:.6g}s, measured once and charged in full to every ERP planning row below. Online matching/selection alone is separately recorded in each erp_decision.", ""]
        text += [f"## {name.title()} — median of three trials", "", "| Method | Planning s | Timed operations s | Update s | Merge s | MiB | Recall | Violations / 400 |", "|---|---:|---:|---:|---:|---:|---:|---:|"]
        for r in rows:
            text.append(f"| {r['label']} | {r['planning']:.6g} | {r['runtime']:.6g} | {r['update']:.6g} | {r['merge']:.6g} | {r['memory']:.4f} | {r['recall']:.4f} | {r['violations']} |")
        failed_searches = sum(not s["calibration_feasible"] for t in document["trials"] for s in t["autosketch_searches"])
        text += ["", f"AutoSketch calibration-infeasible searches: {failed_searches}/12. Algorithm 4's best observed point in such a search is not a feasible configuration."]
        if name == "google":
            occupied = {int(line.split()[0]) for line in Path(document["args"]["input_tsv"]).read_text().splitlines()}
            empty = sum(not any(p in occupied for p in range(end-w, end)) for end in range(121,221) for w in [2,10,30,120])
            text += ["", f"Empty exact windows: {empty}/400 per trial. Empty exact and predicted sets receive recall/precision 1; this convention is shared by every baseline."]
            erp=next((r for r in document["trials"][0]["rows"] if r["method"]=="asapplanner_erp"),None)
            if erp:
                by_window={w:sum(s["recall"]<document["args"]["min_recall_at_10"] for s in erp["query_samples"] if s["window_panes"]==w) for w in [2,10,30,120]}
                text += ["",f"First-trial ERP failures by window (1m/5m/15m/60m): {list(by_window.values())}. The 60m query has only one complete calibration endpoint in a 60m prefix; three timing repetitions of that endpoint do not establish temporal robustness. These results do not demonstrate Google held-out SLA compliance. Longer independent calibration histories and uncertainty/drift validation require a separate, preregistered follow-up; thresholds were not tuned on these held-out failures."]
        text += ["", "Planning includes catalog deserialization and online selection for ERP. Profile construction is additional and reported below. Timed operations sum update, eviction, merge and readout (or exact scanning); this excludes harness/window-view setup, query-state teardown outside those timers, and scoring-oracle time for approximate methods. It is not client end-to-end latency. Merge includes sketch-library candidate reconciliation; its cost cannot be separated through the current portable API.", "",
                 "Selected configurations and match distances for every trial are in `erp_decisions`; AutoSketch logs each window's planning time, evaluated candidate count, calibration score and selected configuration in `autosketch_searches`.", ""]
        fig, axes = plt.subplots(1, 4, figsize=(16, 4.5))
        for ax, field, title in zip(axes, ["planning", "runtime", "memory", "violations"], ["Planning (s)", "Timed operations (s)", "Logical payload (MiB)", "SLA failures / 400"]):
            values=[r[field] for r in rows]
            colors=["#c44e52" if r["any_violations"] or (r["method"] == "autosketch_per_query" and failed_searches) else "#4c72b0" for r in rows]
            if field == "violations":
                ax.barh(range(len(rows)), values, color=colors)
                ax.set_xlim(0,400)
            else:
                ax.scatter(values,range(len(rows)),color=colors,s=45)
                ax.set_xscale("log")
                ax.set_xlim(min(values)*0.6,max(values)*4)
            ax.set_yticks(range(len(rows)),[r["label"] for r in rows])
            for i,value in enumerate(values):
                left=field!="violations" and value>max(values)/5
                ax.annotate(f"{value:.3g}",(value,i),xytext=(-6 if left else 6,0),textcoords="offset points",ha="right" if left else "left",va="center",fontsize=8)
            ax.set_title(title)
            ax.invert_yaxis()
        fig.suptitle(f"{name}: measured ERP; red = calibration or held-out accuracy failures")
        fig.tight_layout()
        fig.savefig(root/f"{name}.svg")
        fig.savefig(root/f"{name}.png", dpi=140)
        plt.close(fig)
        text += [f"![{name} measured results]({name}.svg)", ""]
    catalog=json.loads((root/"catalog.json").read_text())
    text += ["## Profile construction and scope", "", "| Catalog source | Measured construction seconds |", "|---|---:|"]
    for source in catalog["provenance"]["sources"]:
        text.append(f"| {source['name']} | {source['generation_seconds']:.6g} |")
    text += ["", "For a user dataset without an existing profile, cold-start cost includes its profile construction plus loading and selection. Google profiling replays the same calibration prefix three times; these are timing repetitions, not independent distribution samples. Synthetic profiles use three independent streams. No held-out events enter profile construction or shape observation.", "",
             "This PR evaluates measured configuration selection through the real Planner ERP selector and shared/independent 30-second pane execution. It does not implement production online shape observation, arbitrary pane-width search, drift-triggered replanning, or a formal recall guarantee. The benchmark adapter emits ERP-compatible evidence; it is not an invocation of the sketch-bench executable. Memory is minimized first; empirical CPU costs are composed and recorded as estimates, not used as a competing optimization objective. Timings are sequential wall measurements on a shared host; consult manifest.json for revisions, commands and checksums.", ""]
    (root/"report.md").write_text("\n".join(text))


if __name__ == "__main__":
    main()
