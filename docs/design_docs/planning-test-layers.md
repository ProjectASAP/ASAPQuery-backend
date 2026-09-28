# Current planning and execution validation

Audience: implementers and reviewers.

The current scope is a complete deterministic path from a workload to correct
execution. Backend uses explicit synthetic prices for candidate selection. Online
ERP collection, feedback-driven replanning and deployment switching are deferred.

| PR | Input | Assertion |
| --- | --- | --- |
| #728: structure | workload and declared execution/accuracy contracts | Membership: required shapes appear in the inventory and resolve as declared (bind, or refuse for a named policy reason); every other rejection is a recorded defect with a fixed occurrence count. No prices, no winner |
| #742: ranking | same candidates plus synthetic complete quotes | Selection follows costs, reverses with costs, and excludes unavailable candidates |
| #775: execution | workload, synthetic quotes, finite fixture data | Mutate quotes to select each admitted candidate, install and execute it; validate every query result and execution provenance |

```text
workload → Planner physical candidates
         → Backend selection with synthetic costs
         → Deployment Plan → data plane → checked results
```

Planner owns computation semantics and physical candidate construction. Backend
owns deployment selection and binding. Serving loads the selected typed plan;
it must not silently re-plan it at startup. The executor's existing operator,
shared-producer, window, bound-SDS and recovery tests remain part of the baseline.
Synthetic pricing does not relax query accuracy admission or SDS identity checks.

## Workload ensembles

Each layer tests the ten individual queries plus three workload ensembles:
shared-rate (temporal-rate, grouped-rate, topk-rate), shared-quantiles
(temporal-quantile, quantile-ratio), and all ten queries together. Structure checks
preserve every query and validate shared output identities. Ranking checks change
prices for whole workload candidates. Execution installs each selected workload
once, ingests its fixture once, and checks every query in that workload.

“All candidates” means the supported inventory exposed by Planner and admitted
under the fixture's contracts, not an exhaustive Cartesian product of hypothetical
query plans. Binding rejections remain visible; prices cannot override them.

Required shapes are asserted in both directions: the strict fixture must refuse
the heap candidates for a named policy reason, and a certified companion fixture
must admit the same shapes to pricing. A refusal asserted alone would also pass
if the candidate had vanished.

A rejection is not self-justifying. Level 1 separates *policy* refusals — the
fixture declining to certify a candidate, which are asserted as required
outcomes — from *defects*, which are bugs recorded with exact occurrence counts
so that neither a new instance nor a silent fix can pass unnoticed. Level 1's
enumerated candidate plans and ensembles are CI artifacts, not repository
content: they are fully derived, and their identities change with the Planner
pin. Only the per-query admission reports are committed.

## Synthetic cost scope

Prices are deterministic test inputs, not resource measurements. The execution
fixture quotes every component of each compilable candidate. A candidate requiring
external execution is marked infeasible for the local-execution fixture. Backend
selects from that inventory using its production quote selection path. Retain the
quoted snapshot, selected plan and installed plan for inspection. Contract tests
must reject missing prices, a changed cost-model identity or a different installed
generation. Human plan approval remains a separate review.

## Deferred work

ERP means Error–Resource Profile. In future, offline benchmark evidence or online
measurements may inform candidate costs. Query-time merged sketch statistics can
supply merge/readout resource observations, but do not alone establish accuracy.
An online loop would additionally require valid observation scope, update policy,
replanning triggers and safe deployment replacement. None is required now.

#776 (statistics), #777 (accuracy evidence), #778 (resource measurements) and #759
(real-evidence selection audit) are follow-up PRs, not prerequisites for the
current #728 → #742 → #775 path. Their contract tests are not live telemetry or
production validation. Independent measurement correctness fixes may be reviewed
separately. The broader real-trace experiment remains deferred; its prior CPU
objective is not a gate for the current fixture execution milestone.
