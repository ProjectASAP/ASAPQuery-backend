# Current planning and execution validation

Audience: implementers and reviewers.

The current scope is a complete deterministic path from a workload to correct
execution. Backend uses explicit synthetic prices for candidate selection. Online
ERP collection, feedback-driven replanning and deployment switching are deferred.

| PR | Input | Assertion |
| --- | --- | --- |
| #728: structure | workload and declared execution/accuracy contracts | Every supported candidate has a valid physical DAG or an explicit binding rejection; no prices or winner assertions |
| #742: ranking | same candidates plus synthetic complete quotes | Selection follows costs, reverses with costs, and excludes unavailable candidates |
| #775: execution | workload, synthetic quotes, finite fixture data | Select, install and execute the exact physical plan; validate results and execution provenance |

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
