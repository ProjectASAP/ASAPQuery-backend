# Migration plan: ASAPController L1-L3 merge + BackendPlan cutover

Companion to
[`design-backend-plan-wire-format.md`](design-backend-plan-wire-format.md)
(design rationale, schema sketches, open questions — this file is the
phase-by-phase execution plan). Read that doc first.

> **Note on `ASAPController/docs/migration-plan.md`.** That document is
> stale — its premise ("Sources: `ASAPQuery-backend/asap-planner-rs/`")
> describes a directory that no longer exists; control_plane absorbed the
> DC controller in-tree independently of that plan. This document does not
> extend or correct that one; it is a separate, current plan scoped to the
> IR merge and wire-format cutover only. Do not use the old document's
> phase numbers, LOC estimates, or "as landed" reconciliation notes as
> inputs here — every number below was re-measured against current source
> on 2026-07-17.

## Premise

- **Direction**: `control_plane` (in `ASAPQuery-backend`) adopts
  ASAPController's L1-L3 as canonical, fixes the D1-D5 capability gap, and
  replaces `StreamingConfig`/`AggregationConfig` with `BackendPlan`.
- **Sources**: `ASAPController/crates/{ir,l2,frontend-promql,frontend-sql}`,
  current `main` as of this writing — not any cached snapshot.
- **Scope boundary**: ASAPCollector + ASAPQuery-backend only. No changes to
  the public `ASAPQuery` repo. `Topology`/`CostModel` stay pure interfaces
  in ASAPController core; all edge/gateway/OpAMP/stage-allocation/cost-model
  code stays exactly where it is today.
- **Deploy constraint**: none on the wire format — `control_plane` and
  `data_plane` are built and released atomically (confirmed), so
  `design.md` §10's "streaming-config is an unchanged hard contract" does
  not bind this plan.
- **Real constraint**: don't regress anything `analyzer-parity-matrix.md`'s
  18-query corpus currently asserts, except where a phase explicitly and
  intentionally changes a row (D1-D5 fixes are exactly this — track them
  by name, not as accidental drift).

## Phase 0 — Design sign-off

**Scope:** Resolve the open questions in `design-backend-plan-wire-format.md`
§9 before any code lands.

**Work — decided:**
- `Frequency` → **corrected during Phase 1 implementation (#391)**: kept,
  not folded. `AggIntent::Frequency` (control_plane's standalone
  point-frequency-via-CMS query, `count(*) WHERE key = k`, ~12 real call
  sites in `bind_cms_count.rs` and elsewhere) and `RankingMeasure::Frequency`
  (ASAPController's classifier for what a `TopK` ranks by) are unrelated
  capabilities that happen to share a name — folding one into the other
  would have deleted a real, currently-used capability. Both are kept:
  `Frequency` as control_plane-only, `RankingMeasure` adopted additively.
- `irate`/`rate` → adopt ASAPController's fold (both lower to `AggIntent::Rate`,
  estimation-method distinction pushed to L4).
- **control_plane gains SQL support** as a first-class query surface, not
  just an IR-only merge. This is a bigger scope item than it looks —
  `data_plane` currently has no SQL query entry point at all (`drivers/query`
  implements the Prometheus HTTP API only); merging `frontend-sql` into the
  IR does not by itself give users a way to *send* a SQL query. Scoping
  call: **this plan covers merging `frontend-sql` into the shared IR only**
  (Phase 2, item 7). Exposing an actual SQL query endpoint in `data_plane`
  is a separate follow-on phase, not included in Phases 1-7 below — flag it
  as future work rather than silently expanding this plan's scope.
- **CSE moves to L4** (ASAPController's `crates/plan` placement, not
  control_plane's current L3 placement). This reorders control_plane's
  existing optimizer rule firing — Phase 2 item 5 below now treats this as
  a real behavior change requiring its own regression pass, not an optional
  "keep as-is unless there's a reason to move it."
- **Standing tie-break rule for everything else this merge touches**:
  wherever control_plane and ASAPController diverge, adopt
  ASAPController's current version. The only exception is functionality
  that's genuinely control_plane-only with no ASAPController equivalent —
  that gets ported onto the ASAPController-based version, not used to keep
  control_plane's side of a shared file. This replaces Phase 2's earlier
  "re-diff and decide per file" framing — the diffs still happen, but their
  purpose is now "confirm nothing control_plane-only gets silently
  dropped," not "pick a winner."
- Rollup algebra and `MaterializationPayload` extensibility stay deferred
  (per the design doc) — not blocking for Phase 0.

**Exit criteria:** met — decisions recorded in this document's Decision
log (§ below) and in `design-backend-plan-wire-format.md` §4.

**Risk:** none — no code changes.

---

## Phase 1 — `agg_intent.rs` merge

**Scope:** The smallest safe first cut. Replace `control_plane`'s 25-variant
`AggIntent` with ASAPController's current ~40+-variant version (native
histogram accessors, math/trig transforms, time/calendar accessors,
extended range-vector reducers — none of which any existing control_plane
query pattern uses today, so this should be additive, not disruptive), and
resolve the two known representational conflicts from Phase 0.

**Work:**
1. **Blast-radius reconnaissance first** — `grep -rn "AggIntent::" control_plane/src | wc -l`
   and enumerate every match site (expected: `sketch_algebra/rules/*.rs`,
   `sketch_algebra/capability.rs::capability_for`, `optimizer/engine.rs`'s
   R1-R12 rules — several reference `agg_is_exact`/`agg_is_mergeable` which
   pattern-match on `AggIntent` — `physical/*`, and every existing unit
   test that constructs an `AggIntent` literal). This list is the actual
   PR surface, not just the enum definition.
2. Port ASAPController's `agg_intent.rs` into
   `control_plane/src/intent_algebra/agg_intent.rs`, applying the Phase 0
   decisions for `Frequency` / `irate`.
3. Fix every call site from step 1 to compile against the new variant set.
   For control_plane-only variants that map 1:1 onto something
   ASAPController already renamed (e.g. `HoltWinters` ↔
   `DoubleExpSmoothing`), rename at the call site, don't keep an alias.
4. `Absent`/`Present` in control_plane vs `Absent`/`AbsentOverTime`/
   `PresentOverTime` in ASAPController — ASAPController's split is more
   precise (instant vs range-vector forms); audit control_plane's current
   `Absent`/`Present` call sites to determine which of the two new variants
   each one should become. Not automatic — needs a semantic read per call
   site, not a mechanical rename.

**Testing:**
- Full existing `control_plane` unit + integration test suite must pass
  unchanged (this phase is a vocabulary swap, not a behavior change).
- `analyzer-parity-matrix.md`'s 18-query corpus: **zero row changes**. Any
  row that changes here is a bug in this phase, not an intended D1-D5 fix
  (those come in Phase 3).

**Risk:** medium. Mechanically bounded (compiler enumerates every break),
but `Absent`/`Present` splitting and any control_plane-specific downstream
logic keyed on the old variant shapes needs a human read, not just
find-and-replace.

**PR size:** est. 800-1500 LOC (mostly call-site updates, not new logic).

---

## Phase 2 — Remaining L1-L3 files

**Do not reuse `ASAPController/docs/intent-algebra-reconciliation.md`'s
per-file base recommendations.** Re-measuring current source shows several
have flipped which side is bigger since that doc was written — it is not a
reliable guide anymore, only a source of *questions to re-ask*. Per the
Phase 0 tie-break rule, the answer to "which side wins" is now always
"ASAPController" — the per-file diff below exists to catch anything
control_plane-only that needs porting onto ASAPController's version, not
to relitigate which side is richer:

| File | control_plane (current) | ASAPController (current) | Note |
|---|---:|---:|---|
| `schema.rs` | 332 | 412 | Reconciliation doc claimed "byte-identical" — **no longer true**, re-diff from scratch |
| `query_expr.rs` | 1187 | 1690 | Was CP > ASAP; **now flipped** |
| `relational.rs` | 944 | 452 (in `l2`) | CP still bigger — plausible CP still has more node types, but unverified since the reconciliation snapshot |
| `lower.rs` | 847 | 974 (in `l2`) | Was CP > ASAP; **now flipped** |
| `binder.rs` | 298 | 258 | Close; re-diff for semantic drift, not just size |
| `column_resolution.rs` | 458 | 395 | Same |
| `cse.rs` | 324 (lives in `intent_algebra`, i.e. L3) | 259 (lives in `crates/plan`, i.e. L4) | **Structural difference, not just content** — see below |
| `expr_ir.rs` | doesn't exist (scalars inlined in `query_expr.rs`/`relational.rs`) | 257 (separate file) | ASAPController already made the D2 call from the reconciliation doc; adopt it |
| `canonicalize.rs` | no direct analog by name | 527 (in `l2`) | May correspond to some subset of control_plane's `optimizer/engine.rs` R1-R12 rules (`WindowMerge`, `PartitionElim`, `SetOpFusion` look canonicalization-shaped) — needs investigation, not assumed to be new work |

**Work, in order:**
1. `schema.rs` — re-diff (not "free" as previously assumed); reconcile any
   drift.
2. `expr_ir.rs` — introduce as a new file in `control_plane`, adopting
   ASAPController's split-out scalar IR (D2 decision from the
   reconciliation doc, already validated by ASAPController's own
   evolution). Extend it to cover any scalar shape control_plane's inlined
   version has that ASAPController's `expr_ir.rs` doesn't.
3. `query_expr.rs` / `relational.rs` — since sizes have flipped, do a fresh
   node-by-node diff rather than picking a base a priori. Point remaining
   scalar references at `expr_ir::L3Expr` per the D2 decision.
4. `binder.rs` / `column_resolution.rs` — fresh diff, adopt whichever is
   semantically ahead per-function (may not be the same side for every
   function).
5. **`cse.rs` moves to L4** (decided in Phase 0) — port it into
   control_plane's `optimizer` module (mirroring ASAPController's
   `crates/plan` placement), not `intent_algebra`. This reorders when CSE
   fires relative to control_plane's existing `optimizer/engine.rs` R1-R12
   rules. **Required regression pass**: re-run every existing optimizer
   test with CSE now running after sketch binding instead of before, and
   specifically check the rules that do fan-in/reuse detection
   (`CommonSubexprElim`/R8 itself may become partially redundant with the
   relocated CSE pass — reconcile rather than run both) — do not treat
   this as a mechanical file move.
6. `lower.rs` — the one genuinely medium-risk file per the original
   reconciliation doc's assessment (per-branch binding, accuracy
   threading) — re-verify that assessment still holds given the size
   flip, then adopt ASAPController's version per the tie-break rule.
7. Retarget the PromQL frontend onto the merged IR. **SQL frontend**
   (decided in Phase 0: control_plane gains SQL) — merge
   ASAPController's `frontend-sql` into the shared IR and wire it into
   control_plane's own parsing entry points (`query_parser`). Per Phase
   0's scoping note: this phase delivers SQL *parsing/lowering* inside
   control_plane; it does not add a SQL query HTTP endpoint to
   `data_plane` — that's explicitly out of scope here and should be
   tracked as separate future work if wanted.

**Testing:**
- Golden-file round-trip test (parse → lower → `QueryExpr` JSON) — port
  ASAPController's existing corpus, add control_plane-specific query shapes
  it doesn't cover.
- L3-intent-only invariant test (post-lowering `AggIntent` carries no
  sketch type/params — type-enforced per the L3 design rule).
- Full `analyzer-parity-matrix.md` corpus, same zero-unintended-change bar
  as Phase 1.

**Risk:** medium-high — this is the largest phase, and (per the table
above) the "which side is richer" assumption from the old reconciliation
doc cannot be trusted file-by-file. Budget time for re-diffing, not just
merging.

**PR size:** unknown until step 1's fresh diffs are done — do not estimate
from the old doc's stale LOC deltas. Recommend splitting into one PR per
numbered work item above rather than a single PR, given the size
uncertainty.

---

## Phase 3 — Fix `capability_for()` to close D1-D5

> **Blocker found during Phase 1 (#391): the corpus this phase's testing
> bar depends on no longer exists under its documented name.**
> `analyzer-parity-matrix.md` names its acceptance test as
> `data_plane/src/query_engines/asap_query_engine/engine.rs::analyzer_parity_tests`;
> that module isn't present in current `data_plane` (grepped, not found).
> Before starting this phase: locate whether the corpus moved/was renamed,
> or rebuild it from the doc's 18 queries + D1-D5 table as a fresh test.
> Either way, resolve this first — don't start the `capability_for()` change
> assuming the gate already exists.

**Scope:** `control_plane/src/sketch_algebra/capability.rs`. Independent
of Phase 1-2 mechanically (touches a different file), but should land
*after* Phase 1-2 stabilize the `AggIntent` vocabulary it matches on, to
avoid rebasing this phase's work through a moving enum. Can be developed in
parallel on a branch and rebased once Phase 2 lands.

**Work:**
- For `Sum`, `Count`, `Rate`, `Increase`, `TopK` (heavy-hitter form) at
  "exact" accuracy: instead of `UnsupportedAggIntent`, return a capability
  that routes to an exact materialization (this is what
  `MaterializationPayload::ExactAggregate`, from the design doc, exists to
  carry once Phase 4 lands — but `capability_for()` returning the right
  *decision* doesn't itself require `BackendPlan` to exist yet; it can be
  built and tested against the current `AggregationConfig`'s existing
  `AggregationType::Sum`/`Increase`/`MinMax` variants first, since those
  wire-level types already exist and are simply unused today).
- Re-run `analyzer-parity-matrix.md`'s D1-D5 rows: each should flip from
  `ctrl: MISS` to `ctrl: OK`, matching the `engine` column exactly (that's
  the parity contract the doc itself defines for this kind of change).

**Testing:** `analyzer-parity-matrix.md`'s full 18-query corpus,
specifically asserting D1-D5 now read `✅` instead of their current
divergence codes.

**Risk:** low-medium — scoped to one function, but the correctness bar
(must match the *legacy* engine's behavior for these five categories, not
just "produce something plausible") is real; use the existing corpus as
the literal acceptance test, not a new one.

**PR size:** est. 300-600 LOC + test updates.

---

## Phase 4 — `BackendPlan` proto + `Materialization`/`RoutingEntry` types

**Scope:** Per `design-backend-plan-wire-format.md` §5. Depends on Phase
1-3 (needs the merged `AggIntent` vocabulary and the fixed
`capability_for()` to know what a `Materialization`/`RoutingEntry` actually
needs to carry — building the wire types before the planning logic that
populates them risks designing the schema around the wrong shape).

**Work:**
- `proto/asap_control.proto` (or extend it) — `BackendPlan`,
  `Materialization`, `MaterializationPayload` (`oneof Sketch |
  ExactAggregate`), `RoutingEntry`.
- Reuse `PolicyFingerprint`, `Capability`, `SketchKind`/`SketchParams`,
  `StorageBackend`, `MonitorSpec` as-is — no reinvention.
- Emitter: replace `emit_backend_streaming_config_json`/
  `emit_backend_storage_routing` (in `emit/stage_config.rs`) with a
  `BackendPlan` emitter built from the L4/L5 output.

**Testing:** round-trip serialization tests; a fixture-based test that a
known `SketchExpr` (from the design doc's worked example) emits the
expected `BackendPlan` bytes.

**Risk:** low — mostly new code, not a rewrite of existing logic, once
Phase 1-3 have stabilized what it needs to express.

**PR size:** est. 600-1000 LOC.

---

## Phase 5 — `RoutingIndex` in `data_plane`

**Scope:** Per `design-backend-plan-wire-format.md` §6. **Resolve the
rollup-algebra open question (§9) before implementing match-algorithm step
2** (which `AggIntent`s roll up safely across a coarser group-by without
re-estimation error — `Sum`/`Count` yes, `Quantile` generally no). This is
a correctness question, not an implementation detail; get it reviewed
before coding the rollup path, ship the non-rollup match logic first if
that unblocks faster.

**Work:**
- `RoutingIndex` struct (Tier 1 exact-fingerprint map + Tier 2 columnar
  per-metric buckets with interned `GroupShapeId`/`FilterId`), consistent
  with the interning pattern in `storage_engines/sketch_db/index/epoch_columnar.rs`.
- Wire `data_plane`'s query path through the *same* frontend/lowering
  crates `control_plane` uses post-merge (this is the step that makes
  D1-D5-style divergence structurally impossible going forward, not just
  fixed for the current 18 queries).
- Reuse existing window-merge/closest-pane logic
  (`storage_engines/sketch_db/query/{window_merger,sketch_reducer}.rs`) for
  match-algorithm step 3 rather than reimplementing it.

**Testing:**
- `analyzer-parity-matrix.md`'s corpus, but now run through
  `RoutingIndex` directly rather than the legacy engine analyzer — this
  *is* the γ step referenced in that document, done structurally instead
  of as a hand-written shim.
- New tests for Tier-2 rollup/window/filter-subsumption matching, keyed to
  the resolved rollup algebra.

**Risk:** medium-high — new query-time hot path in a latency-sensitive
service; needs a performance pass (the whole pitch of the warm tier is
µs-ms latency) in addition to correctness.

**PR size:** est. 1500-2500 LOC.

---

## Phase 6 — Delete the duplicate legacy analyzer

**Scope:** `ASAPQueryEngine::parse_and_match_promql`, `Statistic`,
`QueryPatternType`, and the ~1100 LOC of analyzer-shaped code in
`data_plane/src/query_engines/asap_query_engine/engine.rs` that
`analyzer-parity-matrix.md` documents as the "duplicate."

**Gate:** `RoutingIndex` (Phase 5) reaches 100% parity on the existing
18-query corpus for at least the same kind of soak period the project
already uses elsewhere for cutovers (staging burn-in before a one-way
deletion — see the general pattern in `ASAPController/docs/migration-plan.md`'s
own rollback strategy, which is sound even though that document's specific
phase content is stale).

**Work:** delete the legacy code; `analyzer-parity-matrix.md` collapses
from a two-column comparison to a single `unified` column (as the document
itself anticipates for its own δ phase).

**Risk:** medium — one-way once merged, but low blast radius if Phase 5's
soak period is honored.

**PR size:** mostly deletions, est. -1000 to -1500 LOC net.

---

## Phase 7 — Cutover to `BackendPlan`

**Scope:** Joint release (atomic deploy, confirmed) swapping
`StreamingConfig`/`AggregationConfig` for `BackendPlan` on both sides.

**Work:**
- `control_plane` emits `BackendPlan` instead of `StreamingConfig`.
- `data_plane` consumes `BackendPlan` via `RoutingIndex` (already built in
  Phase 5) instead of the old `AggregationConfig` ingestion path.
- Delete `crates/asap_types/src/streaming_config.rs` and
  `aggregation_config.rs`'s untyped-`parameters` shape.

**Exit criteria:** full end-to-end verification against a live stack
(mirroring `TODO.md`'s existing "live verification" pattern — a real
`curl` against `/api/v1/query` with a query from each of the five sketch
families plus at least one D1-D5 exact-aggregate shape, confirming
`infos[]` still carries correct accuracy/window annotations).

**Risk:** high in isolation, but low in practice given Phases 1-6 already
prove every component individually — this phase is the wiring-together
step, not new logic.

---

## Timeline (rough, one engineer, high uncertainty on Phase 2/5 per the notes above)

| Phase | Depends on | Est. wall time |
|---|---|---|
| 0. Design sign-off | — | 2-3 days (review + decisions, not implementation) |
| 1. `agg_intent.rs` merge | 0 | 3-5 days |
| 2. Remaining L1-L3 | 1 | **unknown — re-diff first**; guess 2-4 weeks pending that |
| 3. Fix `capability_for()` | 1-2 (semantically) | 3-5 days, parallelizable with late Phase 2 |
| 4. `BackendPlan` types | 1-3 | 1 week |
| 5. `RoutingIndex` | 4 | 1.5-2 weeks |
| 6. Delete duplicate analyzer | 5 + soak period | 2-3 days work + soak time |
| 7. Cutover | 1-6 | 3-5 days |

Do not treat this table as a commitment — Phase 2 in particular needs its
own re-scoping pass (step 1 of that phase) before any number here is
trustworthy.

## Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Phase 2's "which side is richer" assumption is wrong per-file (as it already was for several files vs the old reconciliation doc) | High | Wasted merge work, subtle semantic regressions | Fresh diff per file before merging, not before starting the phase |
| `cse.rs` L3-vs-L4 placement change silently reorders optimizer rule firing | Medium | Wrong sketch selected for some query shapes | Keep CSE where control_plane's existing rule ordering assumes it unless there's a specific reason to move it (§ Phase 2 step 5) |
| D1-D5 fix in Phase 3 doesn't exactly match legacy engine behavior | Medium | Query answers change for `sum by`/`topk`/etc. shapes silently | Use `analyzer-parity-matrix.md` as literal acceptance test, not a new corpus |
| `RoutingIndex` (Phase 5) is slower than the old pattern-match lookup | Medium | Latency regression in the warm tier's core value prop | Performance pass required before Phase 6 deletes the fallback |
| Rollup algebra (Phase 5 blocker) turns out to be non-trivial for more `AggIntent`s than expected | Medium | Phase 5 slips | Ship non-rollup matching first, land rollup as a fast-follow, don't block Phase 5 entirely on it |

## Decision log

| # | Question | Status |
|---|---|---|
| 1 | `Frequency`: standalone intent or `TopK::RankingMeasure`? | **Corrected (#391) — both, kept separate.** Not the same capability; see Phase 1 work item above. |
| 2 | `irate`/`rate`: fold at L3 or keep distinct? | **Decided — fold, per ASAPController.** |
| 3 | Does merging `frontend-sql` mean `control_plane` regains SQL as a first-class surface? | **Decided — yes.** IR/parsing merge only in this plan (Phase 2 item 7); a `data_plane` SQL query endpoint is separate future work. |
| 4 | `cse.rs`: L3 (current control_plane) or L4 (ASAPController's `crates/plan`)? | **Decided — L4.** See Phase 2 item 5 for the required regression pass. |
| 5 | Rollup algebra — which `AggIntent`s roll up safely across a coarser group-by? | Open — blocks Phase 5 step 2, not earlier phases |
| 6 | `MaterializationPayload` extensibility (closed enum vs URN-tagged)? | Deferred indefinitely per the design doc — revisit only on real need |
| 7 | General tie-break for any other control_plane/ASAPController divergence found during Phase 1-2? | **Decided — adopt ASAPController's version by default; exception only for control_plane-only functionality with no ASAPController equivalent.** |

## Rollback strategy

Every phase lands as its own PR (or several, per Phase 2's note); nothing
here is behind a runtime feature flag because `control_plane`/`data_plane`
deploy atomically and rollback is `git revert` + redeploy, same as any
other change in this repo. Phase 6 (deleting the duplicate analyzer) is the
only one-way door — gate it on a soak period per that phase's exit
criteria, not just green tests.
