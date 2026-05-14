//! The L3 **Binder** — name resolution as an explicit pass.
//!
//! ## Why this exists
//!
//! Mature query engines all have one explicit boundary where symbolic
//! column / table *names* are resolved against a schema source, and
//! everything downstream of that boundary is fully resolved:
//!
//! - **ClickHouse** — `QueryAnalyzer` rewrites `IdentifierNode` →
//!   `ColumnNode` against the real `StorageSnapshot`.
//! - **Trino** — `Analyzer` produces an `Analysis` side-table, resolving
//!   identifiers against the catalog `Metadata`; the planner then uses
//!   opaque, plan-local `Symbol`s.
//! - **RisingWave** — `Binder` resolves names to positional
//!   `InputRef { index, data_type }` against its `Catalog`.
//!
//! Our canonical L3 IR (`query_expr::QueryExpr`) already commits to
//! positional column identity — `Aggregate.by: Vec<ColumnId>` — exactly
//! like RisingWave's `InputRef`. What was missing was the *pass* that
//! produces it: resolution was smeared into `legacy_to_canonical::convert`
//! with a hardcoded synthesized `(ts, value)` schema, so any query
//! referencing a real label / column name (`price`, `host`, …) errored.
//!
//! This module is that missing pass. [`Binder::bind`] produces the
//! complete, self-contained [`Schema`] every `ColumnId` in the converted
//! canonical tree indexes into — the IR's own "RelationType" (Trino) /
//! bind scope (RisingWave). The converter then becomes purely
//! *structural*: it threads the Binder's schema, and positional
//! resolution downstream is **total** — it never errors.
//!
//! ## The `SchemaCatalog` seam
//!
//! design.md §6 ("three distinct metadata sources") already names the
//! abstraction: the DB / source schema is "exposed through a
//! `SchemaCatalog` interface." [`SchemaCatalog`] is that interface.
//!
//! The default [`UsageDerivedCatalog`] knows nothing — every schema is
//! derived purely from what the query itself references. That is the
//! honest state for the observability domain: metric label sets are
//! open-ended and data-dependent, there is no closed catalog to resolve
//! against (unlike a SQL database's `information_schema`). A
//! registry-backed `SchemaCatalog` is future work — and crucially, the
//! `Binder` pass itself does not change when it lands; only the catalog
//! impl swaps.
//!
//! ## Where it sits
//!
//! Today the Binder runs at the L2→L3 (legacy → canonical) conversion
//! boundary — `legacy_to_canonical::convert_root` calls it. Once the
//! legacy IR is retired it moves into the `core::lower` L1→L2→L3 passes
//! proper (the `lower_*(ast, schema)` signatures in design.md §6).

use crate::intent_algebra::legacy_expr::QueryExpr as LQueryExpr;
use crate::intent_algebra::schema::{Column, DataType, Schema};

/// The DB / source-schema metadata source from design.md §6 "three
/// distinct metadata sources" — resolves a source (metric / table) name
/// to its known columns.
pub trait SchemaCatalog {
    /// Columns known for `source`. `None` when the source is unknown to
    /// this catalog — the [`Binder`] then falls back to a usage-derived
    /// column set.
    fn columns_for(&self, source: &str) -> Option<Vec<Column>>;
}

/// The default catalog: knows nothing. Every schema the [`Binder`]
/// produces is derived purely from the query's own usage.
///
/// This is the honest state for the observability domain — see the
/// module doc. A registry-backed `SchemaCatalog` is the future; the
/// `Binder` pass does not change when it lands.
pub struct UsageDerivedCatalog;

impl SchemaCatalog for UsageDerivedCatalog {
    fn columns_for(&self, _source: &str) -> Option<Vec<Column>> {
        None
    }
}

/// The L3 Binder — the explicit name-resolution pass. See the module doc.
pub struct Binder<C: SchemaCatalog = UsageDerivedCatalog> {
    catalog: C,
}

impl Default for Binder<UsageDerivedCatalog> {
    fn default() -> Self {
        Self::new()
    }
}

impl Binder<UsageDerivedCatalog> {
    /// A Binder with the default usage-derived catalog.
    pub fn new() -> Self {
        Self {
            catalog: UsageDerivedCatalog,
        }
    }
}

impl<C: SchemaCatalog> Binder<C> {
    /// A Binder backed by an explicit [`SchemaCatalog`].
    pub fn with_catalog(catalog: C) -> Self {
        Self { catalog }
    }

    /// Resolve the complete [`Schema`] in scope for a query rooted at
    /// `tree`.
    ///
    /// The result contains the time axis, the synthetic `value` column,
    /// and one column per distinct name referenced anywhere in the tree
    /// — so positional `ColumnId` resolution downstream
    /// (`resolve_column_ref` / `resolve_named_keys`) is **total** and
    /// never errors. This is the IR's own self-contained "RelationType".
    pub fn bind(&self, tree: &LQueryExpr) -> Schema {
        // Base columns: from the catalog if it knows the source, else the
        // conventional PromQL leaf shape `(ts, value)`.
        let mut columns: Vec<Column> = tree
            .source_name()
            .and_then(|name| self.catalog.columns_for(name))
            .unwrap_or_else(default_leaf_columns);

        // Ensure the (ts, value) floor is present — the canonical lowering
        // resolves `ColumnRef::SampleValue` against a column literally
        // named `value`, and `Window` requires a time index.
        for floor in default_leaf_columns() {
            if !columns.iter().any(|c| c.name == floor.name) {
                columns.push(floor);
            }
        }

        // Append one column per referenced-but-unknown name. These are
        // group-by keys / sketch-target columns the converter resolves
        // positionally; with them in the schema, resolution cannot fail.
        for name in collect_referenced_columns(tree) {
            if !columns.iter().any(|c| c.name == name) {
                columns.push(Column {
                    name,
                    dtype: DataType::Utf8, // labels / group keys are strings
                    nullable: true,
                });
            }
        }

        let time_index = columns.iter().position(|c| c.name == "ts");
        Schema {
            columns,
            time_index,
            unique_keys: Vec::new(),
        }
    }
}

/// The conventional PromQL leaf column shape: `(ts: Timestamp, value: Float64)`.
fn default_leaf_columns() -> Vec<Column> {
    vec![
        Column {
            name: "ts".into(),
            dtype: DataType::Timestamp,
            nullable: false,
        },
        Column {
            name: "value".into(),
            dtype: DataType::Float64,
            nullable: false,
        },
    ]
}

/// Walk the legacy tree and collect every distinct group-key name the
/// legacy → canonical converter resolves positionally: `Aggregate.keys`,
/// `TopK.by`, and `Partition.keys`. Sorted + de-duplicated for a stable,
/// deterministic column order.
///
/// `AggItem.col` (the statistic's *input* column) is deliberately not
/// collected — the converter never resolves it positionally; it only
/// ever resolves group-by keys.
fn collect_referenced_columns(tree: &LQueryExpr) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    tree.walk(&mut |node| match node {
        LQueryExpr::Aggregate { keys, .. } => out.extend(keys.iter().cloned()),
        LQueryExpr::TopK { by, .. } => out.extend(by.iter().cloned()),
        LQueryExpr::Partition { keys, .. } => out.extend(keys.keys().iter().cloned()),
        _ => {}
    });
    out.sort();
    out.dedup();
    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::legacy_expr::{
        AggFunc, AggItem, ColumnRef as LColumnRef, PartitionKeys, QueryExpr as LQueryExpr,
        SourceSpec,
    };

    fn src(name: &str) -> LQueryExpr {
        LQueryExpr::Source(SourceSpec { name: name.into() })
    }

    #[test]
    fn bare_source_yields_ts_value_floor() {
        let schema = Binder::new().bind(&src("m"));
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "ts");
        assert_eq!(schema.columns[1].name, "value");
        assert_eq!(schema.time_index, Some(0));
    }

    #[test]
    fn aggregate_keys_and_partition_keys_land_in_schema() {
        // Aggregate { keys: ["region"] } and Partition { By(["host"]) }.
        let tree = LQueryExpr::Partition {
            keys: PartitionKeys::By(vec!["host".into()]),
            input: Box::new(LQueryExpr::Aggregate {
                keys: vec!["region".into()],
                aggs: vec![AggItem {
                    alias: "c".into(),
                    func: AggFunc::Count,
                    col: LColumnRef::Wildcard,
                    distinct: false,
                }],
                having: None,
                input: Box::new(src("hits")),
            }),
        };
        let schema = Binder::new().bind(&tree);
        assert!(schema.column_id("region").is_some());
        assert!(schema.column_id("host").is_some());
    }

    #[test]
    fn topk_by_keys_land_in_schema() {
        let tree = LQueryExpr::TopK {
            k: 10,
            by: vec!["symbol".into(), "exchange".into()],
            input: Box::new(src("m")),
        };
        let schema = Binder::new().bind(&tree);
        assert!(schema.column_id("symbol").is_some());
        assert!(schema.column_id("exchange").is_some());
    }

    #[test]
    fn referenced_names_are_deduplicated() {
        // Same name referenced twice → one column.
        let tree = LQueryExpr::Aggregate {
            keys: vec!["region".into(), "region".into()],
            aggs: vec![],
            having: None,
            input: Box::new(src("m")),
        };
        let schema = Binder::new().bind(&tree);
        let region_cols = schema
            .columns
            .iter()
            .filter(|c| c.name == "region")
            .count();
        assert_eq!(region_cols, 1);
    }

    #[test]
    fn custom_catalog_supplies_base_columns() {
        struct FixedCatalog;
        impl SchemaCatalog for FixedCatalog {
            fn columns_for(&self, source: &str) -> Option<Vec<Column>> {
                if source == "known_metric" {
                    Some(vec![
                        Column {
                            name: "ts".into(),
                            dtype: DataType::Timestamp,
                            nullable: false,
                        },
                        Column {
                            name: "value".into(),
                            dtype: DataType::Float64,
                            nullable: false,
                        },
                        Column {
                            name: "datacenter".into(),
                            dtype: DataType::Utf8,
                            nullable: false,
                        },
                    ])
                } else {
                    None
                }
            }
        }
        let tree = src("known_metric");
        let schema = Binder::with_catalog(FixedCatalog).bind(&tree);
        // `datacenter` came from the catalog, not usage-synthesis — and it
        // is non-nullable, unlike a usage-derived column.
        let dc = schema.column_id("datacenter").and_then(|id| schema.columns.get(id));
        assert!(matches!(dc, Some(c) if !c.nullable));
    }
}
