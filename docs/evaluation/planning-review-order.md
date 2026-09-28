# Planning tests: review order

| PR | Review question |
| --- | --- |
| [#728](https://github.com/ProjectASAP/ASAPQuery-backend/pull/728) | Does every supported candidate have the correct structure or an explicit binding rejection, without prices selecting the expected shape? |
| [#742](https://github.com/ProjectASAP/ASAPQuery-backend/pull/742) | Given synthetic complete costs, does every admissible candidate win when cheapest, independent of inventory order, while missing/infeasible quotes are excluded? |
| [#775](https://github.com/ProjectASAP/ASAPQuery-backend/pull/775) | Does the exact installed plan execute correctly? Seven container differential runs and raw results are retained. |
| [#776](https://github.com/ProjectASAP/ASAPQuery-backend/pull/776) | Do workload facts preserve scope, units, freshness, missingness and distinct-event accounting? |
| [#777](https://github.com/ProjectASAP/ASAPQuery-backend/pull/777) | Is accuracy evidence applicable independently of candidate prices? |
| [#778](https://github.com/ProjectASAP/ASAPQuery-backend/pull/778) | Are resource samples paired, valid and correctly normalized/composed? |
| [#759](https://github.com/ProjectASAP/ASAPQuery-backend/pull/759) | Does a real-trace experiment select a measured-best feasible plan and return accurate results, separately for individual queries and the complete workload? |

The Git stack follows this order. The three input contracts are conceptually
independent; the resource tests reuse the preceding accuracy fixture to avoid
duplicating its setup. Execution correctness is separate from cost correctness.

## Verification status

All new contract tests pass locally: Level 1 (2 tests), Level 2 (1 test covering
ten queries and every unique admitted manifest), statistics (4), accuracy (3),
resource composition (1), resource export (9), and selection-audit rejection /
comparison logic (8). Strict Clippy passes for the five Rust integration targets.
The [seven execution runs](execution-2026-09-28/README.md) also passed.

These counts are not production evidence. The Level 3 audit is implemented,
but the real-trace experiment with matched, applicable measurements for every
admitted candidate has not been completed. #759 remains draft for that reason.
Historical cross-engine fixture benefit reports remain preserved separately.
Human review of the candidate plans is still required; no automated test records
human acceptance.
