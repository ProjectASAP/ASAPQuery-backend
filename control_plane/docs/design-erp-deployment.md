# ERP deployment integration

The compile-and-publish request and backend-local planning snapshot may carry
an optional `erp` object. It contains the versioned sketch-bench artifact, the
deployment's current distribution descriptor, workload operation counts,
resource weights, error metric, minimum trial count, accuracy mode, and runtime
capabilities.

Planning is query-workload-aware: ASAPPlanner still owns legal families and the
materialization DAG, while the backend adapter translates the selected ERP
parameter JSON into its typed runtime `SketchParams`.

Hybrid mode has three observable outcomes:

1. exact distribution and implementation match, sufficient trials, acceptable
   measured error, and supported parameters: use the least-cost ERP point;
2. ERP miss, invalid artifact, unparseable parameters, or distribution drift:
   use ASAPPlanner's theoretical parameters when runtime capabilities accept
   them; and
3. neither empirical nor theoretical state is deployable: preserve the
   pre-ASAP subtree for exact execution.

Empirical mode does not silently claim a theoretical fallback: an ERP miss
goes directly to exact execution. Selection and fallback reasons are emitted as
structured tracing events. ERP observations remain empirical and must not be
rendered as formal `(epsilon, delta)` guarantees.

ERP v1 matches the complete distribution JSON by equality. A caller detects
drift by supplying its latest descriptor with every new plan generation. A
changed descriptor cannot reuse the old profile accidentally.
