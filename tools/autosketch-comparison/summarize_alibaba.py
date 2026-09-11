#!/usr/bin/env python3
"""Validate complete real-data artifacts before producing tables or a PR body."""
import csv
import json
import math
from pathlib import Path
import statistics
import sys

METHODS = ['exact-pane', 'exact-scan', 'analytical', 'auto', 'erp-no-sharing', 'erp']
WORKLOADS = ['service', 'edge', 'latency']
LABELS = {'exact-pane': 'Exact-pane', 'exact-scan': 'Exact-scan',
          'analytical': 'ASAP analytical sizing', 'auto': 'AutoSketch CPU extension',
          'erp-no-sharing': 'ERP no-sharing (custom)', 'erp': 'ERP shared-or-local (custom)'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_text())


def finite(value):
    return isinstance(value, (int, float)) and math.isfinite(value) and value >= 0


def validate_run(row, plan, workload, expected_events, calibration_files, total_files):
    """Reject missing endpoints, altered plans, bad timers, and fabricated success."""
    require(row['status'] == 'complete', 'incomplete run')
    require(row['workload'].lower() == workload, 'wrong workload')
    require(row['deployment'] == plan, 'plan/run mismatch')
    require(finite(plan['planning_seconds']), 'invalid planning time')
    require(row['events'] == expected_events, 'input event count differs from dataset manifest')
    panels = 18 if workload == 'latency' else 6
    ends = list(range(calibration_files * 3 + 1, total_files * 3 + 1))
    require(len(row['samples']) == len(ends) * panels, 'missing query samples')
    require({(s['end_minute'], s['panel_id']) for s in row['samples']} ==
            {(end, panel) for end in ends for panel in range(panels)}, 'wrong query endpoints')
    require([s['end_minute'] for s in row['dashboard_samples']] == ends, 'wrong dashboard endpoints')
    require(all(finite(t) for t in row['timing'].values()), 'invalid timing value')
    for sample in row['samples']:
        require(sample['panel']['window'] == [1, 10, 60][sample['panel_id'] // (6 if workload == 'latency' else 2)], 'wrong window')
        require(finite(sample['normalized_loss']), 'invalid loss')
        require(finite(sample['readout_seconds']) and finite(sample['readout_cpu_seconds']), 'invalid readout time')
        require(0 <= sample['window_keys'] <= sample['window_events'], 'invalid cardinality')
    require(row['violations'] == sum(s['normalized_loss'] > 1 for s in row['samples']), 'violation count mismatch')
    measured = 0
    for dashboard in row['dashboard_samples']:
        samples = [s for s in row['samples'] if s['end_minute'] == dashboard['end_minute']]
        groups = dashboard['merge_groups']
        require(sorted(i for group in groups for i in group['panels']) == list(range(panels)), 'missing/duplicated merge assignment')
        for group in groups:
            require(finite(group['seconds']) and finite(group['cpu_seconds']), 'invalid composition time')
            require(all(s['merge_group'] == group['id'] for s in samples if s['panel_id'] in group['panels']), 'merge link mismatch')
        seconds = sum(s['readout_seconds'] for s in samples) + sum(g['seconds'] for g in groups)
        require(math.isclose(seconds, dashboard['timed_query_operations_seconds'], rel_tol=1e-8, abs_tol=1e-8), 'shared merge double-counted')
        measured += seconds
    require(math.isclose(measured, row['timing']['merge_seconds'] + row['timing']['readout_seconds'], rel_tol=1e-8, abs_tol=1e-7), 'query aggregate mismatch')
    return row


def median(values):
    return statistics.median(values)


def percentile(values, p):
    values = sorted(values)
    return values[math.ceil(len(values) * p) - 1]


def summarize(root):
    manifest = load(root / 'manifest.json')
    a = manifest['arguments']
    require((a['calibration_files'], a['total_files'], a['trials'], a['profile_trials'], a['calibration_events']) ==
            (120, 240, 3, 3, 10_000_000), 'only preregistered full real-data runs can produce the final report')
    data = load(root / 'dataset-manifest.json')['files']
    require(len(data) == 240 and [r['index'] for r in data] == list(range(240)), 'incomplete dataset')
    for i, record in enumerate(data):
        require(record['source']['url'] == f'https://aliopentrace.oss-cn-beijing.aliyuncs.com/v2022MicroservicesTraces/CallGraph/CallGraph_{i}.tar.gz', 'not the declared Alibaba source')
        require(len(record['source']['sha256']) == 64 and len(record['uncompressed_sha256']) == 64, 'missing source digest')
        require(record['source']['parsed_rows'] == record['exact_duplicate_rows_removed'] + record['missing_downstream_rows_removed'] + record['events'], 'row accounting mismatch')
    rows, runs = [], {}
    for workload in WORKLOADS:
        selected = data[100:240]  # One hour warmup plus six held-out hours.
        excluded = {'service': None, 'edge': 'missing_upstream_rows_retained_for_service_queries',
                    'latency': 'invalid_latency_rows_retained_for_counts'}[workload]
        expected = sum(r['events'] - (r[excluded] if excluded else 0) for r in selected)
        for method in METHODS:
            trials = []
            for trial in range(3):
                stem = root / f'{workload}-{method}-trial{trial}'
                plan = load(Path(str(stem) + '-plan.json'))
                run = validate_run(load(Path(str(stem) + '-run.json')), plan, workload, expected, 120, 240)
                if method.startswith('exact-'):
                    require(run['violations'] == 0, 'exact reference has accuracy violations')
                trials.append(run)
            runs[workload, method] = trials
            timing = lambda key: median([r['timing'][key] for r in trials])
            dashboard = [d['timed_query_operations_seconds'] for r in trials for d in r['dashboard_samples']]
            rows.append({'workload': workload, 'baseline': method,
                         'planning_seconds': median([r['deployment']['planning_seconds'] for r in trials]),
                         'planning_min_seconds': min(r['deployment']['planning_seconds'] for r in trials),
                         'planning_max_seconds': max(r['deployment']['planning_seconds'] for r in trials),
                         **{k: timing(k) for k in trials[0]['timing']},
                         'peak_retained_payload_bytes': max(r['peak_retained_payload_bytes'] for r in trials),
                         'peak_query_payload_bytes': max(r['peak_query_payload_bytes'] for r in trials),
                         'dashboard_p50_seconds': median(dashboard), 'dashboard_p95_seconds': percentile(dashboard, .95),
                         'violations': sum(r['violations'] for r in trials),
                         'queries': sum(len(r['samples']) for r in trials), 'events_per_trial_including_warmup': expected,
                         'nonfinite_exact_groups': sum(s['nonfinite_exact_groups'] for r in trials for s in r['samples'])})
    (root / 'summary.json').write_text(json.dumps(rows, indent=2) + '\n')
    with (root / 'summary.csv').open('w') as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    report = ['# Alibaba dashboard results', '',
              'Audience: experiment reviewers. All numbers below are measured on the full declared trace; tests and two-hour qualification runs are excluded.', '',
              f"Backend revision: `{manifest['backend_revision']}`. Binary SHA-256: `{manifest['binary_sha256']}`.", '',
              '## Workload and data', '',
              'First 240 complete three-minute CallGraph archives (12 hours). Calibration: hours 0–6, a deterministic 10,000,000-event reservoir from the complete prefix; held-out: hours 6–12 without event sampling. Each timed replay ingests hour 5–6 as warmup and hours 6–12 as evaluation. Native timestamps; refresh every minute; half-open 1m/10m/60m windows.', '',
              'Service and service-pair dashboards each contain count-by-key and Top-3-by-count at each window (6 panels). Latency contains per-downstream-service p50/p75/p90/p95/p99 and p90/p50 at each window (18 panels). There are 360 refreshes per trial and 3 timing trials of the same trace. These are windowed call observations, not unique logical requests, arbitrary PromQL, or 100ms scrapes.', '',
              f"Parseable source rows: {sum(r['source']['parsed_rows'] for r in data):,}; malformed rows excluded: {sum(r['source']['malformed_rows']['count'] for r in data):,}; exact duplicate rows removed: {sum(r['exact_duplicate_rows_removed'] for r in data):,}; missing downstream removed: {sum(r['missing_downstream_rows_removed'] for r in data):,}; retained observations: {sum(r['events'] for r in data):,}.", '',
              'Complete source URLs, archive hashes, per-file exclusions and replay hashes are in `dataset-manifest.json`. Binary dataset and oracle caches are not vendored into Git. Per-query `window_events` and `window_keys` are in each raw run JSON.', '',
              '## Measured comparison', '',
              'Times are seconds. Planning and total operation times are medians of 3 trials; payload memory is the maximum across trials in MiB. Dashboard p95 includes each shared merge exactly once plus readout, not client/network latency. Violations are query endpoints exceeding the preregistered normalized error target, summed across trials. Lower memory/time with violations is not a valid accuracy-preserving win.', '',
              '| Workload | Baseline | Plan | Update CPU | Merge CPU | Readout CPU | Retained MiB | Dashboard p95 | Violations / queries |',
              '|---|---|---:|---:|---:|---:|---:|---:|---:|']
    for r in rows:
        report.append(f"| {r['workload']} | {LABELS[r['baseline']]} | {r['planning_seconds']:.6g} | {r['update_cpu_seconds']:.4g} | {r['merge_cpu_seconds']:.4g} | {r['readout_cpu_seconds']:.4g} | {r['peak_retained_payload_bytes']/2**20:.4g} | {r['dashboard_p95_seconds']:.4g} | {r['violations']} / {r['queries']} |")
    report += ['', 'Wall times, eviction CPU, planning min/max, scratch payload and every raw refresh are included in `summary.json`, `summary.csv`, and `*-run.json`. Exact-scan charges raw scan/group-by to readout, not sketch merge.', '',
               '## Calibration and offline cost', '',
               f"Shared prefix preparation: {manifest['calibration']['preparation_seconds']:.3f} s; source observations scanned: {manifest['calibration']['source_events']:,}. This is separate from online planning and data download/projection.", '',
               '| Workload | Catalog construction seconds | Sample observations after workload filtering | Configurations including exact fallback |',
               '|---|---:|---:|---:|']
    for workload in WORKLOADS:
        c = load(root / f'{workload}-catalog.json')
        require(c['trials'] == 3 and c['workload'].lower() == workload, 'catalog mismatch')
        report.append(f"| {workload} | {c['construction_seconds']:.3f} | {c['calibration_events']:,} | {len(c['records'])} |")
    report += ['', 'AutoSketch planning includes independent per-panel searches on that sample and does not consume the ERP catalog. ERP planning includes catalog loading/selection; its separately reported offline construction must be included when evaluating first-use cost. Calibration and catalog costs are not hidden in a zero-cost assumption.', '',
               '## Interpretation and limitations', '',
               '- This is a native Rust operator/selector experiment, not deployed backend end-to-end throughput. Shared-host wall times and true process-CPU times are separate; oracle generation/loading and input decoding are outside operator timers. Whole-process RSS includes harness/oracle memory, so it is not sketch-only memory.',
               '- AutoSketch is a CPU adaptation with per-query LHS/neighbor search, an explicit exact fallback, and KLL/DDSketch extensions. It is not the original P4 compiler or a claim that the paper supports quantiles. Every window-query pair searches independently.',
               '- ERP uses the actual ASAPPlanner ERP selector with a custom calibration catalog, minimizing estimated retained bytes. Shared-or-local compares one shared 60-pane store with fully independent stores, not all partial-sharing layouts or arbitrary pane granularities.',
               '- The analytical baseline calls ASAPPlanner analytical sizing; it is not a full analytical CPU-cost optimizer. No compatible existing synthetic ERP catalog was applied to these query/error/window contracts. This experiment therefore does not demonstrate observed-shape nearest-profile matching, production drift detection, or generalization from a synthetic catalog.',
               '- Sampled calibration can underestimate full-stream cardinality, memory and errors. Held-out violations are reported, never repaired by tuning on held-out data. The 16 GB retained-payload budget excludes query scratch and allocator overhead; Exact-scan is an uncapped reference.',
               '- Quantiles preserve zero latency using an exact zero counter with positive-only DDSketch. Final-query loss targets: count L1/total ≤2%; tie-aware Top-3 recall ≥80%; each grouped quantile error / max(|truth|,1ms) ≤10%; p90/p50 error / max(|truth|,1) ≤20%. Undefined ratios must match IEEE NaN/Inf classes and are counted separately. Rank-distance diagnostics are reported but not the common sizing target.', '',
               '## Figures and reproducibility', '',
               '`figure-1-alibaba.svg` / `.png` show planning time, update CPU, retained payload and dashboard p95 for all methods, with violation counts visible. `manifest.json` records commands, source revisions, binary digest, host and trial order. See `docs/evaluation/alibaba-dashboard-evaluation.md` for the execution contract.', '']
    (root / 'report.md').write_text('\n'.join(report))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(3, 4, figsize=(20, 13))
    metrics = [('planning_seconds', 'Planning (s)'), ('update_cpu_seconds', 'Update CPU (s)'),
               ('peak_retained_payload_bytes', 'Retained payload (MiB)'), ('dashboard_p95_seconds', 'Dashboard p95 (s)')]
    for i, workload in enumerate(WORKLOADS):
        selected = [r for r in rows if r['workload'] == workload]
        for j, (key, title) in enumerate(metrics):
            ax = axes[i, j]
            values = [r[key] / (2**20 if 'bytes' in key else 1) for r in selected]
            ax.bar(range(len(values)), values)
            ax.set_yscale('log'); ax.set_title(f'{workload}: {title}')
            ax.set_xticks(range(len(values)), [r['baseline'] + f"\nviol={r['violations']}" for r in selected], rotation=45, ha='right')
            ax.grid(axis='y', alpha=.2)
    fig.suptitle('Alibaba real trace — custom ERP versus AutoSketch CPU extension\n3 timing trials; accuracy failures remain visible; not production end-to-end latency')
    fig.tight_layout(rect=[0, 0, 1, .95])
    fig.savefig(root / 'figure-1-alibaba.svg'); fig.savefig(root / 'figure-1-alibaba.png', dpi=150)
    relative = root.relative_to(Path.cwd()).as_posix()
    body = f'''## Why

Evaluate recurring window dashboards on real Alibaba call observations with explicit accuracy and planning costs.

## What

Add the Rust comparison harness, reproducible data preparation, and full 12-hour measurements for service counts/Top-3, service-pair counts/Top-3, and grouped latency quantiles/ratios.

## How

Use complete source archives, full-row deduplication, a 6-hour calibration prefix and unsampled 6-hour held-out replay. Compare six baselines with identical query endpoints and three timing trials; retain raw samples and separately measured CPU/wall costs.

## Before this PR

The comparison did not execute this Alibaba observation contract and dashboard workload matrix.

## After this PR

Reviewers can reproduce preparation/search/replay and inspect [the report]({relative}/report.md), source hashes, raw measurements and figures. This PR is stacked on the existing dashboard-comparison branch.

## Verification

All 54 real-data runs passed artifact completeness, endpoint, row-accounting and timing checks. Exact references have zero accuracy violations. Approximate violations are reported, not treated as successful accuracy-preserving results. See the report for measured values and the evaluation design for correctness-test commands. Visual product screenshots: not applicable.

## Limitations

Custom-calibration ERP, not synthetic nearest-shape matching or deployed backend execution. AutoSketch includes explicit CPU/quantile/exact-fallback extensions. Analytical sizing is not full cost-model optimization. Memory is logical payload, not isolated RSS. No claim that ASAP wins every workload.
'''
    (root / 'pr-body.md').write_text(body)
    return rows


if __name__ == '__main__':
    summarize(Path(sys.argv[1]).resolve())
