# Top-K dashboard results

The v1 measurements are withdrawn. They used hardcoded ERP parameters and
contained sampling, event-order, timestamp-alignment, and memory-accounting
errors. Their raw files remain recoverable in git history at `c91d19e2` but must
not be used as valid comparison evidence.

The replacement [measured ERP report](data/topk-dashboard-v2/report.md) is
generated from release-mode synthetic and Google trials. It includes profile
construction cost, per-baseline planning time, retained memory, measured
runtime, and held-out accuracy failures. See the [evaluation
plan](../../docs/evaluation/autosketch-topk-dashboard-plan.md) for implemented
scope and remaining experiments.

Reproduce from this backend worktree after building the release example:

```sh
python3 tools/autosketch-comparison/reproduce_topk_erp.py \
  --output /tmp/topk-erp-fresh \
  --google-replay tools/autosketch-comparison/data/topk-dashboard-v2/google-replay.tsv \
  --revision 1c8e2ad8e800a93adf00e86112ad7bd67a806d24
python3 tools/autosketch-comparison/summarize_topk_erp.py /tmp/topk-erp-fresh
```

Use the revision of the binary being tested; the command above names the
recorded runner revision. The replay interval is reproducible with
`prepare_google_cluster_trace.py --pane-seconds 30 --start-pane 77774 --panes 220`.
