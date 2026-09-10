//! Serving-time execution model for the post-ASAP IR — the counterpart to
//! `asap_aware_mapping::bind`'s planning-time `QueryExpr -> SummaryNode`.
//!
//! **Vendored, not upstream.** This module used to be
//! `asap_sketch::exec` (`crates/sketch/src/exec.rs`) in what's now
//! ASAPPlanner. It was deleted there in
//! [ASAPPlanner#190](https://github.com/ProjectASAP/ASAPPlanner/issues/190)
//! (folded into #197) on the grounds that "the only implementors anywhere
//! in this workspace are `MockExecutor`/`ReductionSpyExecutor`, both
//! `#[cfg(test)]`-only... no bin, example, or downstream crate in this
//! repo constructs or runs a real executor" — true from ASAPPlanner's own
//! repo, but this module's very own [`SummaryExecutor`] trait *is*
//! implemented for real by
//! [`summary_executor::AsapSummaryExecutor`](crate::query_engines::asap_query_engine::summary_executor),
//! just in this (different) repo, which upstream's search couldn't see.
//! ASAPPlanner's own README now explicitly scopes itself away from
//! execution ("not doing any physical query planning") and lists
//! connecting its output to a real backend as an open question (#1) — see
//! `control_plane/docs/design-asapplanner-pin-migration.md` §3.2 for the
//! full account. Ported here verbatim in spirit (same trait shape, same
//! tree-walking algorithm, same tests) with one adaptation: ASAPPlanner
//! split the old flat `SummaryKind`/`SummaryParams` pair into a
//! per-family `SummaryFamilyType` tagged union
//! (ASAPPlanner#218) before `exec.rs` was deleted, so `SummaryAgg`'s
//! `kind`/`params` fields collapsed into one `family: SummaryFamilyType`
//! field on the real, current `SummaryExpr::SummaryAgg` — this module's
//! `find_candidates`/`ExecOutcome::State` follow that shape rather than
//! the pre-split one the last upstream copy of `exec.rs` had.

use std::collections::BTreeMap;

use planner_types::pre_asap::{ColumnRef, QueryExpr, Reduction};

use planner_types::post_asap::{SketchQuery, SummaryExpr, SummaryFamilyType, SummaryNode};

/// Deployment-supplied backend for executing a [`SummaryNode`] tree
/// against materialized state.
pub trait SummaryExecutor {
    /// Reference to one materialized summary instance (e.g. a sid).
    type Handle: Clone;
    /// Decoded, in-memory state for one instance (or an already-merged
    /// group of them).
    type State;
    /// A query answer.
    type Value;
    /// Deployment error type.
    type Error;
    /// Opaque per-group identity for a `SummaryAgg`'s grouping — e.g. a
    /// label-value map for a PromQL/time-series deployment. `Ord` so
    /// `execute` can fold same-group handles/states deterministically;
    /// `Default` for the single-group case (`Reduction::Reduce` with an
    /// empty `by`, or a `Logical` leaf, which has no grouping concept at
    /// all) — every outcome is always a per-group list, and the
    /// single-group case is simply a list of one entry under
    /// `GroupKey::default()`.
    type GroupKey: Clone + Ord + Default;

    /// Resolve a `SummaryAgg` leaf to matching materialized-instance
    /// handles, each tagged with the group it belongs to. Must only
    /// return handles whose committed summary family is exactly
    /// `family` (same `(kind, params)` pair, whichever family it is —
    /// `ExactAggregate`/`Sketch`/`Sample`/`Wavelet`/`StatModel`; never
    /// `Plain`, since a `SummaryAgg` always commits a real family — see
    /// the field's own doc on `SummaryExpr::SummaryAgg`).
    ///
    /// `reduction` (the same [`Reduction`] the pre-ASAP `Aggregate` node
    /// this was bound from carried) tells you which of two shapes to
    /// produce — the two are **not** interchangeable (ASAPPlanner#163):
    /// - `Reduction::Reduce(by)`: a genuine cross-series reduction. Tag
    ///   every returned handle with the `GroupKey` for its `by` values;
    ///   when `by` is empty there is exactly one group — every handle
    ///   should share one deployment-chosen `GroupKey` (e.g.
    ///   `GroupKey::default()`), and they will all be merged together.
    /// - `Reduction::PerEntity`: there is no grouping concept for this
    ///   shape at all. Tag every returned handle with its own, distinct
    ///   `GroupKey` (its full entity/series identity) — handles must
    ///   never share a `GroupKey` here, since `execute` merges same-group
    ///   handles together and merging unrelated entities would silently
    ///   produce a wrong answer.
    #[allow(clippy::type_complexity)]
    fn find_candidates(
        &self,
        family: &SummaryFamilyType,
        col: &ColumnRef,
        reduction: &Reduction,
        child: &SummaryNode,
    ) -> Result<Vec<(Self::GroupKey, Self::Handle)>, Self::Error>;

    /// Fetch/decode one handle's raw state.
    fn fetch_state(&self, handle: &Self::Handle) -> Result<Self::State, Self::Error>;

    /// Merge two or more same-family, same-group states.
    fn merge_states(&self, states: Vec<Self::State>) -> Result<Self::State, Self::Error>;

    /// Read a query out of one group's built state.
    fn readout(&self, state: &Self::State, query: &SketchQuery)
        -> Result<Self::Value, Self::Error>;

    /// Handle a `SummaryExpr::Logical` node (nothing committed at the
    /// post-ASAP level). Ungrouped by construction — `Logical` defers
    /// entirely to the deployment, which decides for itself whether/how
    /// the underlying `QueryExpr` groups.
    fn logical(&self, expr: &QueryExpr) -> Result<Self::Value, Self::Error>;
}

/// Result of executing one [`SummaryNode`] — one summary state per group
/// (tagged with the shared family), or one final value per group. Always
/// a list, even when there's exactly one (ungrouped) group — see
/// [`SummaryExecutor::GroupKey`].
pub enum ExecOutcome<E: SummaryExecutor + ?Sized> {
    State(Vec<(E::GroupKey, E::State, SummaryFamilyType)>),
    Value(Vec<(E::GroupKey, E::Value)>),
}

/// Errors [`execute`] itself can raise, on top of the deployment's own
/// [`SummaryExecutor::Error`].
#[derive(Debug)]
pub enum ExecError<Inner> {
    /// [`SummaryExecutor::find_candidates`] returned no handles.
    NoCandidates,
    /// A `SummaryMerge` child produced a `Value`, not a `State`.
    MergeChildNotState,
    /// `SummaryMerge` children disagreed on their summary family — checked
    /// globally across every group from every child, not just within one
    /// group, because family agreement is a planning-time property of the
    /// `SummaryMerge` node itself (fixed before any group value is known),
    /// not something that can validly vary per group.
    MergeKindParamsMismatch,
    /// A `SummaryMerge` had zero children.
    EmptyMerge,
    /// `SummaryJoin`/`SummarySubtract`/`SummaryDelete` — unreachable today.
    NotYetSupported(&'static str),
    /// The deployment's own error.
    Executor(Inner),
}

impl<Inner> From<Inner> for ExecError<Inner> {
    fn from(e: Inner) -> Self {
        ExecError::Executor(e)
    }
}

/// Recursively execute `node` against `exec`.
pub fn execute<E: SummaryExecutor>(
    node: &SummaryNode,
    exec: &E,
) -> Result<ExecOutcome<E>, ExecError<E::Error>> {
    match &node.expr {
        SummaryExpr::KeepPreAsap(qe) => Ok(ExecOutcome::Value(vec![(
            E::GroupKey::default(),
            exec.logical(qe)?,
        )])),

        SummaryExpr::SummaryAgg {
            child,
            family,
            input,
            reduction,
            ..
        } => {
            // This legacy adapter forwards one value column, not a complete
            // keyed item/weight contract. Do not silently treat a unit-weight
            // count as a sample-value sum or drop the weight of a keyed update.
            let col = match (&input.item, &input.weight) {
                (None, planner_types::post_asap::SummaryInputExpr::Column(col)) => col.clone(),
                _ => {
                    return Err(ExecError::NotYetSupported(
                        "keyed or non-column summary update",
                    ))
                }
            };
            let tagged = exec.find_candidates(family, &col, reduction, child)?;
            if tagged.is_empty() {
                return Err(ExecError::NoCandidates);
            }
            // Group by `GroupKey` first (a `SummaryAgg` may see several
            // handles for the same group, e.g. cross-host instances of
            // the same metric/group-by value that need folding), then
            // fold each group's handles independently — a group's state
            // must never be combined with another group's.
            let mut by_group: BTreeMap<E::GroupKey, Vec<E::Handle>> = BTreeMap::new();
            for (key, handle) in tagged {
                by_group.entry(key).or_default().push(handle);
            }
            let mut out = Vec::with_capacity(by_group.len());
            for (key, handles) in by_group {
                let states = handles
                    .iter()
                    .map(|h| exec.fetch_state(h))
                    .collect::<Result<Vec<_>, _>>()?;
                let state = fold_states(states, exec)?;
                out.push((key, state, family.clone()));
            }
            Ok(ExecOutcome::State(out))
        }

        SummaryExpr::SummaryEstimate {
            summary_input,
            query,
        } => {
            let groups = expect_state(execute(summary_input, exec)?)?;
            let mut out = Vec::with_capacity(groups.len());
            for (key, state, _family) in groups {
                out.push((key, exec.readout(&state, query)?));
            }
            Ok(ExecOutcome::Value(out))
        }

        SummaryExpr::SummaryMerge { children } => {
            if children.is_empty() {
                return Err(ExecError::EmptyMerge);
            }
            let mut agreed: Option<SummaryFamilyType> = None;
            // Collect every child's (group -> state) pairs first, so a
            // group present in only some children still gets folded from
            // whatever states exist for it (mirrors today's per-(group,
            // window) ExactAgg merge: fold whatever's present, don't
            // require every child to cover every group).
            let mut by_group: BTreeMap<E::GroupKey, Vec<E::State>> = BTreeMap::new();
            for child in children {
                let groups = expect_state(execute(child, exec)?)?;
                for (key, state, family) in groups {
                    match &agreed {
                        None => agreed = Some(family),
                        Some(f) if *f == family => {}
                        Some(_) => return Err(ExecError::MergeKindParamsMismatch),
                    }
                    by_group.entry(key).or_default().push(state);
                }
            }
            // `children` non-empty => at least one child produced a
            // `State` outcome (checked above) => that outcome had at
            // least one group (a `SummaryAgg`/`SummaryMerge` never
            // produces an empty group list — `SummaryAgg` errors via
            // `NoCandidates` first, and `SummaryMerge` recursively bottoms
            // out at a `SummaryAgg`) => `agreed` is always `Some` here.
            let family = agreed.expect("checked non-empty above");
            let mut out = Vec::with_capacity(by_group.len());
            for (key, states) in by_group {
                let merged = fold_states(states, exec)?;
                out.push((key, merged, family.clone()));
            }
            Ok(ExecOutcome::State(out))
        }

        SummaryExpr::SummaryJoin { .. } => Err(ExecError::NotYetSupported("SummaryJoin")),
        // CandidateTopK is lowered to the deployed QueryPlan DAG, where both
        // row inputs retain labels for intersection and exact reranking. This
        // legacy generic adapter exposes opaque GroupKey values and cannot
        // implement that contract without losing label identity.
        SummaryExpr::CandidateTopK { .. } => Err(ExecError::NotYetSupported("CandidateTopK")),
        SummaryExpr::BinaryOp { .. } => Err(ExecError::NotYetSupported("BinaryOp")),
        SummaryExpr::ValueOperation { .. } => Err(ExecError::NotYetSupported("ValueOperation")),
        SummaryExpr::SummarySubtract { .. } => Err(ExecError::NotYetSupported("SummarySubtract")),
        SummaryExpr::SummaryDelete { .. } => Err(ExecError::NotYetSupported("SummaryDelete")),
    }
}

/// Fold one-or-more same-family, same-group states into one.
fn fold_states<E: SummaryExecutor>(
    mut states: Vec<E::State>,
    exec: &E,
) -> Result<E::State, ExecError<E::Error>> {
    if states.len() == 1 {
        Ok(states.pop().expect("len == 1"))
    } else {
        Ok(exec.merge_states(states)?)
    }
}

/// Unwrap an [`ExecOutcome::State`], erroring if it was actually a
/// `Value`.
#[allow(clippy::type_complexity)]
fn expect_state<E: SummaryExecutor>(
    outcome: ExecOutcome<E>,
) -> Result<Vec<(E::GroupKey, E::State, SummaryFamilyType)>, ExecError<E::Error>> {
    match outcome {
        ExecOutcome::State(groups) => Ok(groups),
        ExecOutcome::Value(_) => Err(ExecError::MergeChildNotState),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::post_asap::{SummaryField, SummarySchema};
    use planner_types::pre_asap::{Column, DataType, Schema, Source};
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    fn scan() -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries { metric: "m".into() },
            predicates: vec![],
            schema: Schema::with_time_index(
                vec![
                    Column::new("ts", DataType::Timestamp, false),
                    Column::new("value", DataType::Float64, false),
                ],
                0,
                vec![],
            ),
        }
    }

    fn lift(schema_fields: Vec<&str>) -> SummarySchema {
        SummarySchema {
            fields: schema_fields
                .into_iter()
                .map(|n| SummaryField {
                    name: n.into(),
                    dtype: SummaryFamilyType::Plain(DataType::Float64),
                    nullable: false,
                })
                .collect(),
            time_index: None,
        }
    }

    fn logical_node() -> Rc<SummaryNode> {
        Rc::new(SummaryNode {
            expr: SummaryExpr::KeepPreAsap(Rc::new(scan())),
            schema: lift(vec!["ts", "value"]),
            guarantee: None,
        })
    }

    /// Builds a `SummaryAgg` with `Reduction::Reduce(vec![])` (an ordinary,
    /// ungrouped cross-series reduction) — the shape every pre-#163 test in
    /// this module exercises. Use [`agg_node_with_reduction`] to exercise
    /// `Reduction` itself.
    fn agg_node(family: SummaryFamilyType, child: Rc<SummaryNode>) -> Rc<SummaryNode> {
        agg_node_with_reduction(family, child, Reduction::by(vec![]))
    }

    fn agg_node_with_reduction(
        family: SummaryFamilyType,
        child: Rc<SummaryNode>,
        reduction: Reduction,
    ) -> Rc<SummaryNode> {
        Rc::new(SummaryNode {
            expr: SummaryExpr::SummaryAgg {
                child,
                family,
                input: planner_types::post_asap::SummaryUpdate {
                    item: None,
                    weight: planner_types::post_asap::SummaryInputExpr::Column(
                        ColumnRef::SampleValue,
                    ),
                    weight_domain: Default::default(),
                },
                reduction,
                grouping: planner_types::post_asap::GroupingStrategy::default(),
            },
            schema: lift(vec!["value"]),
            guarantee: None,
        })
    }

    fn estimate_node(summary_input: Rc<SummaryNode>, query: SketchQuery) -> Rc<SummaryNode> {
        Rc::new(SummaryNode {
            expr: SummaryExpr::SummaryEstimate {
                summary_input,
                query,
            },
            schema: lift(vec!["value"]),
            guarantee: None,
        })
    }

    fn merge_node(children: Vec<Rc<SummaryNode>>) -> Rc<SummaryNode> {
        Rc::new(SummaryNode {
            expr: SummaryExpr::SummaryMerge { children },
            schema: lift(vec!["value"]),
            guarantee: None,
        })
    }

    /// Trivial in-memory executor for exercising `execute`'s tree-walking,
    /// grouping, and merge-precondition logic — `State`/`Value` are just
    /// `f64`s, `GroupKey` is `String` (empty string = the ungrouped
    /// default), and `merge_states` is `sum`, so tests can assert on
    /// concrete numbers without any real sketch-math dependency.
    ///
    /// `find_candidates` intentionally ignores `family`/`reduction`/`child`
    /// and returns every registered handle tagged with its own registered
    /// group — real deployments filter on those; these tests only
    /// exercise `execute`'s own tree-walking and grouping/merge logic, not
    /// a real candidate search. See
    /// `reduction_is_passed_through_to_find_candidates_unmodified` below
    /// for a test that *does* inspect `reduction`.
    struct MockExecutor {
        /// handle -> (group, value).
        values: RefCell<HashMap<u64, (String, f64)>>,
        next_handle: RefCell<u64>,
    }

    impl MockExecutor {
        fn new() -> Self {
            Self {
                values: RefCell::new(HashMap::new()),
                next_handle: RefCell::new(0),
            }
        }

        /// Register one materialized instance under the default
        /// (ungrouped) group, returning its handle.
        fn register(&self, value: f64) -> u64 {
            self.register_grouped("", value)
        }

        /// Register one materialized instance under an explicit group.
        fn register_grouped(&self, group: &str, value: f64) -> u64 {
            let mut h = self.next_handle.borrow_mut();
            let handle = *h;
            *h += 1;
            self.values
                .borrow_mut()
                .insert(handle, (group.to_string(), value));
            handle
        }
    }

    impl SummaryExecutor for MockExecutor {
        type Handle = u64;
        type State = f64;
        type Value = f64;
        type Error = String;
        type GroupKey = String;

        fn find_candidates(
            &self,
            _family: &SummaryFamilyType,
            _col: &ColumnRef,
            _reduction: &Reduction,
            _child: &SummaryNode,
        ) -> Result<Vec<(Self::GroupKey, Self::Handle)>, Self::Error> {
            Ok(self
                .values
                .borrow()
                .iter()
                .map(|(h, (g, _))| (g.clone(), *h))
                .collect())
        }

        fn fetch_state(&self, handle: &Self::Handle) -> Result<Self::State, Self::Error> {
            self.values
                .borrow()
                .get(handle)
                .map(|(_, v)| *v)
                .ok_or_else(|| "unknown handle".to_string())
        }

        fn merge_states(&self, states: Vec<Self::State>) -> Result<Self::State, Self::Error> {
            Ok(states.into_iter().sum())
        }

        fn readout(
            &self,
            state: &Self::State,
            _query: &SketchQuery,
        ) -> Result<Self::Value, Self::Error> {
            Ok(*state)
        }

        fn logical(&self, _expr: &QueryExpr) -> Result<Self::Value, Self::Error> {
            Ok(-1.0)
        }
    }

    fn kll() -> SummaryFamilyType {
        use planner_types::post_asap::{
            GroupingStrategy, SketchAlgorithm, SketchKind, SketchParams,
        };
        SummaryFamilyType::Sketch(
            SketchKind::new(SketchAlgorithm::Kll, SketchParams::Kll { k: 200 }),
            GroupingStrategy::default(),
        )
    }

    fn sum() -> SummaryFamilyType {
        use planner_types::post_asap::{ExactKind, ExactParams};
        SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum)
    }

    /// Extract the single (ungrouped) output's value, asserting there's
    /// exactly one group -- the shape every pre-grouping test expects.
    fn only<T>(v: Vec<(String, T)>) -> T {
        assert_eq!(v.len(), 1, "expected exactly one (ungrouped) output group");
        v.into_iter().next().unwrap().1
    }

    #[test]
    fn single_agg_reads_out_directly() {
        let exec = MockExecutor::new();
        exec.register(7.0);
        let tree = estimate_node(agg_node(sum(), logical_node()), SketchQuery::Cardinality);
        let ExecOutcome::Value(v) = execute(&tree, &exec).unwrap() else {
            panic!("expected a value");
        };
        assert_eq!(only(v), 7.0);
    }

    /// Legacy candidate lookup cannot discard keyed items or constant weights
    /// when adapting the planner's typed update contract.
    #[test]
    fn unsupported_update_semantics_fail_before_candidate_lookup() {
        use planner_types::post_asap::{SummaryInputExpr, SummaryUpdate};
        let exec = MockExecutor::new();
        exec.register(7.0);
        for input in [
            SummaryUpdate {
                item: Some(SummaryInputExpr::Column(ColumnRef::Named("key".into()))),
                weight: SummaryInputExpr::Column(ColumnRef::SampleValue),
                weight_domain: Default::default(),
            },
            SummaryUpdate {
                item: None,
                weight: SummaryInputExpr::Constant(1.0),
                weight_domain: Default::default(),
            },
        ] {
            let mut tree = agg_node(sum(), logical_node());
            let SummaryExpr::SummaryAgg { input: update, .. } = &mut Rc::make_mut(&mut tree).expr
            else {
                unreachable!()
            };
            *update = input;
            assert!(matches!(
                execute(&tree, &exec),
                Err(ExecError::NotYetSupported(
                    "keyed or non-column summary update"
                ))
            ));
        }
    }

    /// Binary summaries remain explicit unsupported execution until value and
    /// label/time matching are implemented by the warm tier.
    #[test]
    fn binary_summary_is_not_executed_as_one_operand() {
        let child = logical_node();
        let tree = SummaryNode {
            expr: SummaryExpr::BinaryOp {
                lhs: child.clone(),
                rhs: child.clone(),
                operator: planner_types::post_asap::BinaryOperator {
                    kind: planner_types::pre_asap::BinaryOpKind::Arithmetic(
                        planner_types::pre_asap::ArithmeticOpKind::Div,
                    ),
                    vector_match: None,
                },
            },
            schema: child.schema.clone(),
            guarantee: None,
        };
        assert!(matches!(
            execute(&tree, &MockExecutor::new()),
            Err(ExecError::NotYetSupported("BinaryOp"))
        ));
    }

    #[test]
    fn multiple_candidates_for_one_agg_are_merged() {
        let exec = MockExecutor::new();
        exec.register(3.0);
        exec.register(4.0);
        let tree = estimate_node(agg_node(sum(), logical_node()), SketchQuery::Cardinality);
        let ExecOutcome::Value(v) = execute(&tree, &exec).unwrap() else {
            panic!("expected a value");
        };
        assert_eq!(only(v), 7.0); // MockExecutor::merge_states sums
    }

    #[test]
    fn logical_leaf_defers_to_executor() {
        let exec = MockExecutor::new();
        let tree = logical_node();
        let ExecOutcome::Value(v) = execute(&tree, &exec).unwrap() else {
            panic!("expected a value");
        };
        assert_eq!(only(v), -1.0);
    }

    #[test]
    fn nested_summary_agg_recurses_through_both_levels() {
        // quantile(0.9, sum by (job) (m)) shape: Kll wraps Sum.
        let exec = MockExecutor::new();
        exec.register(10.0);
        let inner = agg_node(sum(), logical_node());
        let outer = agg_node(kll(), inner);
        let tree = estimate_node(outer, SketchQuery::Quantile { q: 0.9 });
        let ExecOutcome::Value(v) = execute(&tree, &exec).unwrap() else {
            panic!("expected a value");
        };
        // MockExecutor::find_candidates ignores the tree shape and always
        // returns every registered handle -- the point of this test is
        // that `execute` actually reaches both levels (no panic/short
        // circuit on the nested SummaryAgg), not the specific value.
        assert_eq!(only(v), 10.0);
    }

    #[test]
    fn merge_of_two_aggs_with_matching_kind_params_sums() {
        let exec = MockExecutor::new();
        let a = agg_node(sum(), logical_node());
        let b = agg_node(sum(), logical_node());
        // Each SummaryAgg independently pulls every registered handle from
        // the shared MockExecutor (2 handles, same default group), so the
        // merge sees two already-summed inputs. Register after building
        // the tree so each `find_candidates` call sees the same two
        // handles.
        exec.register(1.0);
        exec.register(2.0);
        let merged = merge_node(vec![a, b]);
        let tree = estimate_node(merged, SketchQuery::Cardinality);
        let ExecOutcome::Value(v) = execute(&tree, &exec).unwrap() else {
            panic!("expected a value");
        };
        // Each child sums to 3.0 (1.0 + 2.0); the outer merge sums the two
        // children's results: 3.0 + 3.0.
        assert_eq!(only(v), 6.0);
    }

    #[test]
    fn merge_of_merges_nests_arbitrarily_deep() {
        let exec = MockExecutor::new();
        exec.register(5.0);
        let leaf_a = agg_node(sum(), logical_node());
        let leaf_b = agg_node(sum(), logical_node());
        let inner_merge = merge_node(vec![leaf_a, leaf_b]);
        let leaf_c = agg_node(sum(), logical_node());
        let outer_merge = merge_node(vec![inner_merge, leaf_c]);
        let tree = estimate_node(outer_merge, SketchQuery::Cardinality);
        assert!(execute(&tree, &exec).is_ok());
    }

    #[test]
    fn merge_rejects_mismatched_kind_params() {
        let exec = MockExecutor::new();
        exec.register(1.0);
        let a = agg_node(sum(), logical_node());
        let b = agg_node(kll(), logical_node());
        let merged = merge_node(vec![a, b]);
        let tree = estimate_node(merged, SketchQuery::Cardinality);
        match execute(&tree, &exec) {
            Err(ExecError::MergeKindParamsMismatch) => {}
            other => panic!(
                "expected MergeKindParamsMismatch, got a different outcome: {}",
                other.is_ok()
            ),
        }
    }

    #[test]
    fn merge_rejects_a_value_producing_child() {
        let exec = MockExecutor::new();
        exec.register(1.0);
        let state_child = agg_node(sum(), logical_node());
        let value_child = estimate_node(logical_node(), SketchQuery::Cardinality);
        let merged = merge_node(vec![state_child, value_child]);
        let tree = estimate_node(merged, SketchQuery::Cardinality);
        match execute(&tree, &exec) {
            Err(ExecError::MergeChildNotState) => {}
            other => panic!(
                "expected MergeChildNotState, got a different outcome: {}",
                other.is_ok()
            ),
        }
    }

    #[test]
    fn no_candidates_is_a_distinct_error() {
        let exec = MockExecutor::new(); // nothing registered
        let tree = estimate_node(agg_node(sum(), logical_node()), SketchQuery::Cardinality);
        match execute(&tree, &exec) {
            Err(ExecError::NoCandidates) => {}
            other => panic!(
                "expected NoCandidates, got a different outcome: {}",
                other.is_ok()
            ),
        }
    }

    #[test]
    fn grouped_summary_agg_produces_one_series_per_group() {
        // The gap ASAPPlanner#159 flagged: `quantile by (zone) (m)` must
        // produce one output series per zone, not one series merging
        // every zone's sketches together.
        let exec = MockExecutor::new();
        exec.register_grouped("us-east", 3.0);
        exec.register_grouped("us-west", 4.0);
        let tree = estimate_node(agg_node(sum(), logical_node()), SketchQuery::Cardinality);
        let ExecOutcome::Value(mut v) = execute(&tree, &exec).unwrap() else {
            panic!("expected a value");
        };
        v.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            v,
            vec![("us-east".to_string(), 3.0), ("us-west".to_string(), 4.0)]
        );
    }

    #[test]
    fn grouped_summary_merge_folds_within_group_not_across() {
        // Two children (e.g. edge + gateway tiers of the same SummaryAgg
        // shape) each see both groups' handles; the merge must fold
        // us-east's two values together and us-west's two values
        // together, WITHOUT ever mixing a us-east state into us-west's
        // output or vice versa.
        let exec = MockExecutor::new();
        let a = agg_node(sum(), logical_node());
        let b = agg_node(sum(), logical_node());
        exec.register_grouped("us-east", 1.0);
        exec.register_grouped("us-west", 10.0);
        let merged = merge_node(vec![a, b]);
        let tree = estimate_node(merged, SketchQuery::Cardinality);
        let ExecOutcome::Value(mut v) = execute(&tree, &exec).unwrap() else {
            panic!("expected a value");
        };
        v.sort_by(|x, y| x.0.cmp(&y.0));
        // us-east: both children see the one us-east handle (1.0) -> each
        // child's us-east state is 1.0 -> merge sums the two children:
        // 1.0 + 1.0 = 2.0. Same for us-west: 10.0 + 10.0 = 20.0. Neither
        // group's value (2.0, 20.0) equals the cross-group sum (11.0) --
        // proves the merge didn't mix groups.
        assert_eq!(
            v,
            vec![("us-east".to_string(), 2.0), ("us-west".to_string(), 20.0)]
        );
    }

    /// A wiring test for issue ASAPPlanner#163: `execute` must pass a
    /// `SummaryAgg`'s `reduction` through to `find_candidates` completely
    /// unmodified — not re-derive it, not silently drop it in favor of a
    /// bare `by` list. `MockExecutor` above ignores `reduction` entirely
    /// (it isn't testing this), so this test uses its own executor that
    /// records exactly what it received.
    struct ReductionSpyExecutor {
        seen: RefCell<Vec<Reduction>>,
    }

    impl SummaryExecutor for ReductionSpyExecutor {
        type Handle = u64;
        type State = f64;
        type Value = f64;
        type Error = String;
        type GroupKey = u32;

        fn find_candidates(
            &self,
            _family: &SummaryFamilyType,
            _col: &ColumnRef,
            reduction: &Reduction,
            _child: &SummaryNode,
        ) -> Result<Vec<(Self::GroupKey, Self::Handle)>, Self::Error> {
            self.seen.borrow_mut().push(reduction.clone());
            Ok(vec![(0, 0)])
        }

        fn fetch_state(&self, _handle: &Self::Handle) -> Result<Self::State, Self::Error> {
            Ok(1.0)
        }

        fn merge_states(&self, states: Vec<Self::State>) -> Result<Self::State, Self::Error> {
            Ok(states.into_iter().sum())
        }

        fn readout(
            &self,
            state: &Self::State,
            _query: &SketchQuery,
        ) -> Result<Self::Value, Self::Error> {
            Ok(*state)
        }

        fn logical(&self, _expr: &QueryExpr) -> Result<Self::Value, Self::Error> {
            Ok(-1.0)
        }
    }

    #[test]
    fn reduction_is_passed_through_to_find_candidates_unmodified() {
        let exec = ReductionSpyExecutor {
            seen: RefCell::new(vec![]),
        };
        let tree = agg_node_with_reduction(sum(), logical_node(), Reduction::PerEntity);
        execute(&tree, &exec).unwrap();
        assert_eq!(exec.seen.borrow().as_slice(), &[Reduction::PerEntity]);
    }
}
