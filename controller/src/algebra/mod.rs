//! Sketch algebra — the 5-layer query translation pipeline.
//!
//! # Layer architecture
//!
//! | Layer | Module | Role |
//! |-------|--------|------|
//! | **3. Sketch Logical Plan** | [`expr`] | `QueryExpr` + `AggIntent` — implementation-independent algebra |
//! | **3. Sketch Logical Plan** | [`directory`] | Candidate sketch types per `AggIntent`, memory estimation |
//! | **4. Sketch Optimizer** | [`optimizer`] | 12 algebraic rewrite rules (fixed-point iteration) |
//! | **5. Physical Plan** | [`physical`] | `PhysicalAggOp` — resolves `AggIntent` → concrete `SketchType` + `SketchParams` |
//! | **5. Physical Plan** | [`allocator`] | `SketchAllocator` — assigns physical ops to pipeline stages |
//! | **5. Physical Plan** | [`plan`] | `PlanNode` tree with cost estimates and stage annotations |
//!
//! Layers 1–2 (language parsing) live in `query_parser/`.
//!
//! # Typical usage
//!
//! ```rust,ignore
//! use controller::query_parser;
//! use controller::algebra::{expr::QueryExpr, optimizer::QueryOptimizer, physical};
//!
//! // Layers 1–3: parse query string → sketch logical plan.
//! let query_expr = query_parser::parse_query_expr("quantile_over_time(0.99, latency[5m])")?;
//!
//! // Layer 4: optimise (algebraic rewrite rules).
//! let (opt_expr, _iters) = QueryOptimizer::new(raw_bps).optimize(query_expr);
//!
//! // Layer 5: resolve logical AggIntent → physical SketchType + SketchParams.
//! // (done automatically by stage_split / allocator via physical::resolve)
//! ```

pub mod allocator;
pub mod directory;
pub mod expr;
pub mod lower;
pub mod optimizer;
pub mod physical;
pub mod plan;

// Convenience re-exports.
pub use allocator::SketchAllocator;
pub use expr::{AggFunc, AggIntent, BinaryOpKind, QueryExpr, ScalarExpr, WindowKind, WindowSpec};
pub use optimizer::QueryOptimizer;
pub use plan::{CostEstimate, ExecutionMode, PipelineStage, PlanNode, PlanSummary};
