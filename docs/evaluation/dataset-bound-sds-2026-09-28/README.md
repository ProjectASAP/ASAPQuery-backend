# Dataset-bound SDS acceptance evidence

The implementation follows the logical dataset, same-version recovery and query-wide
consistency contracts in #737. This run uses synthetic candidate prices; online ERP,
cross-version state adoption and ad-hoc SDS discovery remain deferred.

## Accepted behavior

| Contract | Evidence |
| --- | --- |
| Same expression, different dataset | Different semantic definition IDs; mismatched or absent installed input identity is rejected. |
| Same dataset, relocated input | Changing the collector binding preserves the semantic definitions. |
| Same-version recovery | A real process restarts from disk and returns 15 without re-ingestion. |
| New-version warm-up | Identical definitions do not grant access to old state; fresh input warms the new version and returns 30. |
| Query-wide consistency | Publishing between two query branches invalidates the whole result. |
| Cost isolation | Synthetic ranking rejects quotes from another dataset. |

Planner carries an explicit namespace/dataset in semantic fragment version 2.
Backend snapshots use version 3 and installed catalogs use schema 6. One input
channel has one declared logical dataset; the trusted source authority supplies
its identity. The implementation does not infer tenants from metric names or bytes.

## Data-plane sweep

Every admitted fixture candidate was made cheapest using synthetic prices,
compiled into deployment plans, installed, ingested through Remote Write, drained,
and compared with Prometheus. Ensembles share one installation per candidate.

| Workload | Candidate runs |
| --- | --- |
| query:spatial-sum | 1 |
| query:spatial-topk | 1 |
| query:spatial-quantile | 1 |
| query:temporal-sum | 1 |
| query:temporal-quantile | 1 |
| query:temporal-rate | 1 |
| query:grouped-rate | 3 |
| query:grouped-temporal-sum | 2 |
| query:topk-rate | 2 |
| query:quantile-ratio | 1 |
| shared-rate | 2 |
| shared-quantiles | 1 |
| full-ensemble | 3 |

Total: 20 candidate runs and 52 query comparisons; all passed.

Admission reports retain rejected candidates. This covers the admitted fixture
inventory, not every theoretical plan, production cost optimality, or human review.

## Validation and provenance

- Backend runtime and runner built at `ea809e00c88729a7a623d62020ba94166b5045fe`.
- Later commits before this report only propagate documentation and plan exports.
- Planner code pin: `bccc837f5caf21c888b2cce73c64a1be283b679a`.
- Three Level 1 tests and the full synthetic Level 2 ranking test passed.
- Focused binding, snapshot-version, adoption, recovery, query-fence and installation tests passed.
- Strict all-target Clippy passed for control_plane, data_plane and promql-compliance.
- Deferred test stack passed an all-target compile check; its production-cost acceptance was not rerun.
- Cross-version metadata regression failed before the fix; both logs are retained.
- Existing runtime generation filtering was already strict; the fix rejects the obsolete metadata marker.

- `data_plane` SHA256: `b3d4d37f6520670955d2b3f9fd2afa208ccb001ec108370a542f47727e5d5206`.
- `differential-runner` SHA256: `0b8a87d439b6943e178672f01a544adf90ea2644f3065dc2bb9bad1e7941b6e2`.

Run `make candidates` in `promql-compliance/runner` to reproduce the sweep.
The retained Compose file used the built binaries and durable state. Logs and
JSON are gzip-compressed; `SHA256SUMS` covers all retained artifacts. Absolute
paths in raw reports refer to the original run; relative directory structure is preserved.
