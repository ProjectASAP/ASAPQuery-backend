"""Render checked-in Level 1 exports without changing plans or assertions."""
import json
from pathlib import Path

BASE = Path(__file__).parent

def compact(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False)

def cell(value):
    return '`' + compact(value).replace('|', '\\|') + '`'

def operation(payload):
    if payload['kind'] == 'fallback':
        expr = payload['expression']
        time = expr.get('TimeRange', {})
        scan = time.get('child', expr).get('Scan')
        if scan is not None:
            return {'source': scan['source'], 'predicates': scan['predicates'],
                    'range': time.get('range')}
    return payload

def label(node):
    payload = node['payload']
    kind = payload['kind']
    if kind == 'fallback':
        return 'Source / time range'
    if kind == 'summary_agg':
        return 'SummaryAgg ' + compact(payload['family'])
    if kind == 'value':
        op = payload['operation']
        return next(iter(op)) if isinstance(op, dict) else op
    return kind

def dtype(value):
    if isinstance(value, dict) and 'Plain' in value:
        return value['Plain']
    return compact(value)

def block(value):
    return '```json\n' + json.dumps(value, indent=2) + '\n```\n'

rows = []
planner_revisions = set()
for path in sorted(BASE.glob('*.json')):
    plan = json.loads(path.read_text())
    planner_revisions.add(plan['envelope']['planner_revision'])
    entry = next(iter(plan['query_plan']['entries'].values()))
    lines = ['# ' + path.stem, '', '`' + entry['canonical_query'] + '`', '',
             f'[Raw selected plan]({path.name}) · [DAG DOT]({path.stem}.dot)', '',
             '## Selected computation: logical provenance', '',
             'IDs below are Planner node IDs; QueryPlan adapter IDs are shown separately.', '']
    for dag in plan['query_plan']['selected_dags'].values():
        lines += [f"Root: `{dag['root']}`.", '', '| Node | Dependencies (producer, edge role) | Timing | Operation | Output fields (index: name/type) |', '| --- | --- | --- | --- | --- |']
        diagram = ['```mermaid', 'flowchart LR']
        for node in dag['nodes']:
            text = f"{node['id']}: {label(node)}"
            text = text.replace('&', '&amp;').replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
            diagram.append(f'  N{node["id"]}["{text}"]')
        for edge in dag['edges']:
            diagram.append(f"  N{edge['producer']} --> N{edge['consumer']}")
        diagram += ['```', '']
        lines[-2:-2] = diagram
        for node in dag['nodes']:
            deps = [[e['producer'], e['role']] for e in dag['edges'] if e['consumer'] == node['id']]
            fields = [f"{i}: {f['name']}/{dtype(f['dtype'])}" for i, f in enumerate(node['output_schema']['fields'])]
            lines.append(f"| {node['id']} | {cell(deps)} | {cell(node['output_state'])} | {cell(operation(node['payload']))} | {cell(fields)} |")
        lines += ['', '### Sort expressions', '']
        found = False
        for node in dag['nodes']:
            sort_operation = node['payload'].get('operation', {})
            if not isinstance(sort_operation, dict) or 'Sort' not in sort_operation:
                continue
            found = True
            lines += [f"Node `{node['id']}`: {cell(sort_operation['Sort'])}", '']
            for edge in dag['edges']:
                if edge['consumer'] == node['id']:
                    producer = next(n for n in dag['nodes'] if n['id'] == edge['producer'])
                    lines += [f"Its input is node `{producer['id']}`: {cell(producer['payload'])}. Column indices refer to that producer's output schema above.", '']
        if not found:
            lines += ['No standalone Sort node in this selected DAG; any ranking readout is shown in the operation table.', '']
    physical = entry.get('physical_dag')
    if physical:
        lines += ['## Installed native physical program', '',
                  'This program is compiled before candidate pricing and installation. Serving restores its operators and binds its declared inputs.', '',
                  f"Roots: {cell(physical['roots'])}.", '',
                  '| Node | Dependencies | Operator / input contract | Output fields |',
                  '| --- | --- | --- | --- |']
        diagram = ['```mermaid', 'flowchart LR']
        for node_id, node in physical['nodes'].items():
            if 'Input' in node:
                dependencies = []
                payload = {'Input': node['Input']['properties']}
                fields = node['Input']['schema']['fields']
                name = 'Bound physical input'
            else:
                operator = node['Operator']
                dependencies = operator['inputs']
                payload = operator['operator']['kind']
                fields = operator['operator']['output']['fields']
                name = next(iter(payload)) if isinstance(payload, dict) else payload
            names = [f"{i}: {field['name']}/{dtype(field['dtype'])}" for i, field in enumerate(fields)]
            lines.append(f"| {node_id} | {cell(dependencies)} | {cell(payload)} | {cell(names)} |")
            diagram.append(f'  P{node_id}["{node_id}: {name}"]')
            for parent in dependencies:
                diagram.append(f'  P{parent} --> P{node_id}')
        diagram += ['```', '']
        lines += ['', *diagram]
    report = plan.get('cost_comparison') or {}
    lines += ['## Candidate admission and costing', '',
              'Costs below come from the controlled Level 1 fixture, not production measurements.', '']
    traces = report.get('logical_selection', [])
    for trace in traces:
        if trace.get('stage') == 'planner.physical_candidate':
            lines += ['### Planner physical candidate', '',
                      f"Logical root: {cell(trace.get('logical_root_id'))}.", '',
                      trace.get('rationale', '').replace('alternatives', 'candidates'), '',
                      'Guarantee: ' + cell(trace.get('guarantee')), '']
            programs = [('Query physical DAG', trace.get('physical_dag'))]
            split = trace.get('physical_candidate')
            if split:
                lines += ['Materialized boundaries: ' + cell(split['materialized_outputs']), '']
                programs = [('Maintenance physical DAG', split.get('precompute')), ('Query physical DAG', split['query'])]
            for title, program in programs:
                if not program:
                    continue
                lines += [f'#### {title}', '', '| Node | Dependencies | Native operator / input |', '| --- | --- | --- |']
                for node_id, node in program['nodes'].items():
                    op = node.get('Operator')
                    lines.append(f"| {node_id} | {cell(op['inputs'] if op else [])} | {cell(op['operator']['kind'] if op else {'Input': node['Input']})} |")
                lines += ['', f"Roots: {cell(program['roots'])}.", '']
            if trace.get('reason'):
                lines += [trace['reason'], '']
        if trace.get('computation_search_scope'):
            lines += [block(trace['computation_search_scope'])]
        for group in trace.get('groups', []):
            for rejected in group.get('rejected', []):
                description = rejected.get('description', '').split(' — ', 1)[0]
                lines += [f"- {description}: {rejected.get('reason', '')}"]
    lines += ['', '| Candidate | Logical root IDs | Status | Fixture cost | Rejection / unavailable reason |',
              '| --- | --- | --- | --- | --- |']
    for index, candidate in enumerate(report.get('candidates', [])):
        lines.append(f"| {index} | {cell(candidate.get('logical_root_ids'))} | {cell(candidate.get('status'))} | {cell(candidate.get('total_cost'))} | {cell(candidate.get('unavailable_reason'))} |")
    exported = sorted((BASE / 'candidates').glob(path.stem + '-*.json'))
    if exported:
        lines += ['', 'Successfully compiled candidate plans: ' + ', '.join(
            f'[{candidate.stem}](candidates/{candidate.name})' for candidate in exported), '']
    lines += ['## Persisted boundaries', '']
    if not plan['precompute_plan'].get('executable_dags', {}):
        lines += ['No precompute executable DAG is installed for this selected candidate.', '']
    for qid, installed in plan['precompute_plan'].get('executable_dags', {}).items():
        lines += [f'Binding for `{qid}`:', '', block(installed['binding'])]
    for output_id, output in plan['summary_catalog']['outputs'].items():
        lines += [f"Stored output `{output_id}` → semantic definition `{output['definition_id']}`.", '']
    if not plan['summary_catalog']['outputs']:
        lines += ['No summary stored-output binding. Maintained current-series input, when present, is visible in the DAG and QueryPlan.', '']
    lines += ['### Maintenance configuration', '', block([{k:v for k,v in m.items() if k not in ('semantic_fragment','original_yaml')} for m in plan['precompute_plan']['materializations']]),
              '## Bound query execution', '',
              'This is the emitted adapter representation, including dependencies, pane size, readout lookback and stored-output references. Legacy wire names are preserved so the export remains auditable.', '', block(entry)]
    (BASE / (path.stem + '.md')).write_text('\n'.join(lines))
    rows.append(f"| [{path.stem}]({path.stem}.md) | `{entry['canonical_query']}` |")

intro = """# Issue #754 plans for human review

These are actual plans exported by the Level 1 fixture. No human approval is
recorded. Selected plans and successfully compiled candidate plans are retained
as JSON/DOT; each page shows accuracy rejections and deployment cost decisions.

Backend source revision is in [source-commit.txt](source-commit.txt). Planner:
<PLANNER_REVISION>.

## Current implementation boundary

These exports describe the current Backend implementation. They do **not** prove
completion of the Planner physical-candidate installation/execution handoff.
Logical provenance and installed native physical programs are shown separately.
Spatial TopK binds a complete current-series snapshot and runs a persisted native
Sort → Limit program. Planner also exposes a CountSketch-with-heap physical
candidate over the same snapshot. This fixture has no enforced distinct-item
bound or score-separation proof, so its heap candidate is explicitly rejected
before pricing. The inherited `native_snapshot_topk` tests cover certified heap
admission, cost-dependent selection and analytical workspace costing; a separate
real-process test covers Remote Write and heap HTTP results. Signed samples do
not authorize CMS. Rate TopK also exposes native exact ranking, CMS and
CountSketch heaps above its bound per-series Rate readout. This fixture lacks
heap proof and records the rejection. Separate controlled tests select each
program through Backend costs; process E2E tests verify both heap families over
durable counter SDS, including reset, window changes and process restart.
Planner also exposes fixed-window candidates that finalize Rate and build the
heap during precompute. Their typed stored boundary and both physical DAGs are
listed below. Certified process E2E tests cover CMS and CountSketch heap SDS,
multiple groups, resets, missing windows and restart. This placement requires
the finite complete-input barrier; it does not claim continuous completeness.
Other PromQL paths retain the documented Backend adapter representation.

Candidate discovery preserves each root's admitted computations. Deployment
currently evaluates single-root substitutions in a preferred workload context;
it does not exhaustively enumerate joint workload combinations. Fixture costs
are not evidence of production-optimal placement.

## Review order

1. Check source, value transformations, grouping, windows and readouts.
2. Compare candidate rejection reasons with the query's accuracy requirement.
3. Check persisted boundaries, definition IDs and requested pane coverage.
4. For topk-rate, check the actual rate-value sort expression and partition keys.
5. Compare costs and rejected candidates before reviewing the selected adapter.

The strict spatial-quantile fixture rejects the default KLL guarantee. A separate
assertion with relaxed accuracy verifies that both KLL and DDSketch reach costing
when admitted. The grouped-temporal-sum test checks cost-dependent placement;
it does not establish this for every query. Sort-key mutation tests reject ranking
by timestamp or label instead of the finalized rate value.

| Query | PromQL |
| --- | --- |
"""
intro = intro.replace('<PLANNER_REVISION>', ', '.join(f'`{revision}`' for revision in sorted(planner_revisions)))
ending = '''

## Reproduce

From the #728 worktree:

```sh
ASAP_LEVEL1_ARTIFACT_DIR=/tmp/issue754-plans \\
  cargo test -p control_plane --test issue754_level1 --locked -- --test-threads=1
```

The test also exports enumerated candidates into `candidates/`; the ten selected
plans are at the top level. Copy those `.json` and `.dot` files here and run
`python3 docs/evaluation/issue754-human-review/render.py` to regenerate the pages.
Actual execution and recovery evidence is maintained in the downstream
[bound SDS validation report](https://github.com/ProjectASAP/ASAPQuery-backend/blob/test/issue754-level3/docs/evaluation/bound-sds-2026-09-26/README.md).
'''
(BASE / 'README.md').write_text(intro + '\n'.join(rows) + ending)
