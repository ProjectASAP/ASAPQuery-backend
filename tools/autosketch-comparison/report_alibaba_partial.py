#!/usr/bin/env python3
"""Report stopped real runs without passing them off as the complete matrix."""
import argparse
import json
from pathlib import Path
from summarize_alibaba import LABELS, METHODS, load, validate_run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    root = args.directory
    data = load(root / 'dataset-manifest.json')['files']
    expected = sum(r['events'] for r in data[100:240])
    runs = {}
    for path in sorted(root.glob('*-run.json')):
        stem = path.name.removesuffix('-run.json')
        workload, rest = stem.split('-', 1)
        method, trial = rest.rsplit('-trial', 1)
        if workload != 'service':
            raise ValueError('this stopped report is explicitly scoped to service runs')
        plan = load(root / f'{stem}-plan.json')
        runs[method, int(trial)] = validate_run(load(path), plan, workload, expected, 120, 240)
        if method.startswith('exact-') and runs[method, int(trial)]['violations']:
            raise ValueError('exact reference violated accuracy')
    lines = ['# Alibaba dashboard: partial real-data results (stopped)', '',
             'Audience: experiment reviewers. Execution stopped at the user’s request. This is **not** the completed 54-run experiment or a three-trial aggregate. No smoke-test measurements are included.', '',
             f'Completed runs: **{len(runs)}/54**, all service workload. AutoSketch service trial 2 was interrupted and has no completed result; ERP-NoSharing and ERP trial 2 were not started. Edge and latency catalogs exist, but their held-out replays were not started. Automatic execution/publication is stopped.', '',
             '## Data and queries', '',
             f'The first 240 complete three-minute Alibaba microservices-v2022 CallGraph archives contain **{sum(r["events"] for r in data):,}** retained observations after full-row deduplication and removal of missing downstream IDs. These are call observations, not unique logical requests. Source URLs, SHA-256 hashes, malformed-row counts and exclusions are retained in `dataset-manifest.json`.', '',
             'Calibration scans hours 0–6 and retains a deterministic 10,000,000-observation reservoir. Held-out replay is unsampled. Each completed service run ingests hours 5–6 for warmup and hours 6–12 for evaluation: **1,753,353,646 observations**. Native event timestamps are used, without a synthetic scrape schedule.', '',
             'The dashboard has six panels: downstream-service counts and Top-3 services by count, each over 1m, 10m and 60m half-open windows. Refresh is every minute: 360 dashboard refreshes and 2160 panel queries per completed run. Minute panes are composed before TopK readout. Raw samples include each window’s event count and cardinality.', '',
             '## First complete trial: measured performance', '',
             'This table uses trial 0 consistently across all six baselines. CPU columns are total process-CPU seconds in timed primitive regions. Dashboard time is the mean summed wall time of composition plus all six readouts; shared merges are counted once. It is not network/client latency. Memory is retained logical payload, excluding allocator overhead and query scratch.', '',
             '| Baseline | Planning s | Update CPU s | Merge CPU s | Readout CPU s | Dashboard mean ms | Retained MiB | Violations / 2160 |',
             '|---|---:|---:|---:|---:|---:|---:|---:|']
    summary = []
    for method in METHODS:
        r = runs[method, 0]
        t = r['timing']
        q = sum(d['timed_query_operations_seconds'] for d in r['dashboard_samples']) / 360 * 1000
        label = LABELS[method] + (' → Exact fallback' if method == 'erp' else '')
        lines.append(f'| {label} | {r["deployment"]["planning_seconds"]:.6g} | {t["update_cpu_seconds"]:.2f} | {t["merge_cpu_seconds"]:.2f} | {t["readout_cpu_seconds"]:.2f} | {q:.2f} | {r["peak_retained_payload_bytes"]/2**20:.2f} | {r["violations"]} |')
        summary.append({'baseline': method, 'trial': 0, 'planning_seconds': r['deployment']['planning_seconds'],
                        'timing': t, 'dashboard_mean_wall_ms': q, 'retained_payload_bytes': r['peak_retained_payload_bytes'],
                        'violations': r['violations'], 'queries': len(r['samples'])})
    lines += ['', 'Important interpretation:', '',
              '- AutoSketch and ERP-NoSharing maintain six independent summaries, causing six physical updates per input observation. Shared deployments update one store.',
              '- Exact-scan updates only retain required raw keys. Its raw scan/group-by is charged to query readout; zero merge time is not zero query work.',
              '- ERP shared-or-local selected **shared Exact fallback**, because no shared sketch met calibration accuracy. Its zero violations do not demonstrate an accurate approximate-sketch win.',
              '- Approximate accuracy failures remain visible: query latency alone cannot establish superiority. Count targets are L1/total ≤2%; Top-3 requires tie-aware recall ≥80%.',
              '- Update includes one hour of warmup and six held-out hours. Query/merge cover only held-out refreshes. Input decoding, oracle IO/validation, planning and eviction are excluded from these three CPU columns; separate wall/CPU/eviction values remain in raw JSON.', '',
              '## Completed-run inventory', '',
              '| Baseline | Completed trials |', '|---|---|']
    for method in METHODS:
        lines.append(f'| {LABELS[method]} | {", ".join(str(i) for m, i in runs if m == method)} |')
    lines += ['', 'All completed `*-run.json` files and their plans are preserved, including trials not used in the first-trial table. The interrupted run has only a log/plan and is excluded. Do not pool the unbalanced repetition counts as a three-trial comparison.', '',
              '## Offline cost and scope', '',
              '| Catalog | Construction wall seconds |', '|---|---:|']
    for w in ['service', 'edge', 'latency']:
        lines.append(f'| {w} | {load(root / f"{w}-catalog.json")["construction_seconds"]:.3f} |')
    manifest = load(root / 'manifest.json')
    lines += ['', f'Shared calibration preparation: {manifest["calibration"]["preparation_seconds"]:.3f} wall seconds. Catalog construction is separate from online planning and is not free.', '',
              'The Rust harness invokes actual ASAPPlanner ERP selection with a **custom calibration catalog** and a memory objective. It does not demonstrate nearest-shape matching against a pre-existing synthetic catalog, production backend throughput, live drift fallback, or arbitrary pane-width optimization. Analytical sizing calls planner sizing functions, not a full analytical CPU-cost optimizer. AutoSketch is a CPU adaptation with independent per-query LHS/neighbor search and explicit exact fallback; KLL/DDSketch are extensions, not paper support claims.', '',
              '## Evidence and verification', '',
              f'Artifact validation passed for all {len(runs)} complete runs: manifest event counts, endpoint coverage, plan correspondence, finite timers, violation accounting and single charging of shared merges. Completed exact references have zero accuracy violations. Correctness fixtures are documented separately and are not performance evidence.', '',
              '`partial-figure.svg` and `partial-figure.png` show the same first-trial results, not the unfinished final figure. `partial-summary.json` contains their numerical inputs. Full source provenance and executed commands are in `manifest.json`. Original archives, compact binary data and oracle caches remain outside Git; source hashes and preparation scripts make them reproducible.', '',
              f'Executable source revision: `{manifest["backend_revision"]}`. Binary SHA-256: `{manifest["binary_sha256"]}`. No experimental source was changed during the completed measurements.', '']
    (root / 'partial-report.md').write_text('\n'.join(lines))
    (root / 'partial-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    for ax, field, label in zip(axes, ['update_cpu_seconds', 'merge_cpu_seconds', 'readout_cpu_seconds'],
                               ['Update CPU (s)', 'Merge CPU (s)', 'Readout CPU (s)']):
        values = [r['timing'][field] for r in summary]
        ax.bar(range(6), values)
        ax.set_yscale('symlog', linthresh=.1)
        ax.set_title(label)
        ax.set_xticks(range(6), [r['baseline'] + ('\nExact fallback' if r['baseline']=='erp' else '') + f'\nviol={r["violations"]}' for r in summary], rotation=45, ha='right')
        ax.grid(axis='y', alpha=.2)
    fig.suptitle('PARTIAL: Alibaba service dashboard, trial 0 only — accuracy violations shown')
    fig.tight_layout()
    fig.savefig(root / 'partial-figure.svg'); fig.savefig(root / 'partial-figure.png', dpi=150)
    svg = root / 'partial-figure.svg'
    svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines()) + '\n')
    print(f'Validated and reported {len(runs)} complete real runs; no replay started.')


if __name__ == '__main__':
    main()
