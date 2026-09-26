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

def dtype(value):
    if isinstance(value, dict) and 'Plain' in value:
        return value['Plain']
    return compact(value)

def block(value):
    return '```json\n' + json.dumps(value, indent=2) + '\n```\n'

rows = []
for path in sorted(BASE.glob('*.json')):
    plan = json.loads(path.read_text())
    entry = next(iter(plan['query_plan']['entries'].values()))
    lines = ['# ' + path.stem, '', '`' + entry['canonical_query'] + '`', '',
             f'[Raw selected plan]({path.name}) · [DAG DOT]({path.stem}.dot)', '',
             '## Planner-selected computation', '',
             'IDs below are Planner node IDs; QueryPlan adapter IDs are shown separately.', '']
    for dag in plan['query_plan']['selected_dags'].values():
        lines += [f"Root: `{dag['root']}`.", '', '| Node | Dependencies (producer, edge role) | Timing | Operation | Output fields (index: name/type) |', '| --- | --- | --- | --- | --- |']
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

intro = '''# Issue #754 plans for human review

These are the ten **actual selected plans** exported by #728's existing Level 1
fixture, not hand-written expected plans. This artifact does not record human
approval. No plan behavior or test assertion was changed for this export.

Backend source revision is in [source-commit.txt](source-commit.txt); Planner is
pinned to `c27cd14b8e052ce1f4641c619488ad539ad71f56`. The source revision differs
from the export execution revision only by inherited documentation merges.

Each page contains the full Planner operation payloads, dependency edges, output
column indices/types, timing, sort expressions, persisted boundaries, maintenance
windows and the installed query adapter. Raw JSON and DOT are retained beside it.
`fallback` in a Planner source payload is an IR tag: it does not by itself prove
that execution forwards to an exact backend. Inspect the bound QueryPlan and
Level 2 provenance to determine execution behavior.

## Review order

1. Follow source → transformation → grouping/window → readout for each query.
2. Check persisted producer nodes against read bindings and pane coverage.
3. For topk-rate, inspect the Sort input and column index, not just descending.
4. Check guarantees and admission evidence in raw JSON for approximate queries.

The topk-rate export explicitly sorts `Column(1)` descending, partitioned by
column 2 (`label_0`). Its producer is `FinalizeExactAccumulator` over per-series
Rate state, with column 1 named `value`. The QueryPlan adapter has an implicit
value sort and does not itself serialize an explicit sort-key expression.
This exposes the relevant mapping for review; it is not an additional assertion.

The grouped-temporal-sum cost-reversal test covers only that query. The other
selected plans use fixture costs; these exports do not prove production-optimal
placement or an exhaustive search over maintenance/query-time alternatives.

Legacy `residual` module/type names still exist in code. They are not presented
here as a new architectural layer; renaming or removing that adapter is separate
from the bound-query SDS migration.

| Query | PromQL |
| --- | --- |
'''
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
