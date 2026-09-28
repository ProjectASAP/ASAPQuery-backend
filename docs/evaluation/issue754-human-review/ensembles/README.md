# Query ensemble candidate exports

Generated from Backend `af507e08` and Planner
`bccc837f5caf21c888b2cce73c64a1be283b679a`, including explicit logical dataset
identity, using the Level 1 fixture contracts.
All three Level 1 tests passed. These are structural exports, not cost selections
or human approval. JSON files are gzip-compressed; DOT files show each bound DAG.
Admission reports retain rejected candidates and reasons.

- `shared-rate`: temporal-rate, grouped-rate, topk-rate.
- `shared-quantiles`: temporal-quantile, quantile-ratio.
- `all-ten`: the complete issue-754 workload.

Review query roots, dependencies, window contracts and shared stored-output
identity in each candidate. Level 2 separately prices whole workload candidates;
#775 installs each admitted candidate and checks all its query results.
