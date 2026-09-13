# ASAPQuery-backend 架构与命名审查

审查基线：GitHub `main`，`b1a58ca810d7b7347f2cc5f30924db56532bbb6b`，2026-09-13。关联 Issue：[#709](https://github.com/ProjectASAP/ASAPQuery-backend/issues/709)。面向维护规划、共享契约、安装及执行代码的开发者。

结论：#709 应从“术语替换表”改为“组件边界和数据语义对齐”。优先澄清逻辑候选、物理候选、报价对象、部署状态和存储身份，再决定具体命名。不能因为几个对象都包含计划字段，就把它们合成一个类型；也不能因为都叫 `SummaryNode`，就认为它们只表示近似查询。

本文件保留实施前的架构审查与决策依据。当前实现及迁移规则见 [Planning terminology and architecture](planning-terminology.md)。其中明确列出此次命名迁移与保留为后续工作的结构调整。

**本 PR 的最终落地与迁移方式**

下表记录最终采用的名称；后文保留审查时的备选与理由。实施基线已更新为 `cb3153a8`（#710），继续复用 Planner 的精度类型。

| 所属边界 / 源码 | 原名称 → 最终名称 |
|---|---|
| [编译输入与编译器](../../../control_plane/src/physical/compiler.rs) | `BackendLocalPlanningSnapshot` → `BackendLocalPlanningInput`；`BackendLocalImplementation` → `BackendLocalPhysicalInputs`；`PlanningQuery` → `QueryCompilationInput`；`PlanningRequest` → `PhysicalCompilationRequest`；`PhysicalCompiler` → `PhysicalPlanCompiler`；`PhysicalPlan` → `CompiledPhysicalPlan` |
| 同上：查询语义 | `post_asap` → `selected_plan_root`；`source` → `legacy_query_source`；`window_secs` → `query_lookback_seconds`；`group_by` → `group_by_labels`；`accuracy` → `accuracy_target`；`lifecycle` → `summary_lifecycle_inputs`；`runtime_policy` → `materialization_runtime_policy` |
| 同上：候选与证据 | `logical_selection` → `planner_selection_trace`；`materialization_policy` → `enabled_materialization_keys`；`evidence` → `topk_membership_evidence_by_query_id`；`hybrid_execution` → `allow_mixed_summary_and_exact_execution`；`synthesized_window_queries` → `compiler_priced_window_query_ids` |
| 同上：窗口与成本 | `WindowImplementationCandidate` → `WindowRealizationCandidate`；`ImplementationCostEvidence` → `WindowRealizationCostQuote`；`LifecycleCostEvidence` → `LifecycleUnitCosts`；`LifecyclePlanningInput` → `SummaryLifecyclePlanningInputs`；`window_implementations` → `window_realization_candidates` |
| 同上：外层配置 | `snapshot_version` → `schema_version`；`implementation` → `physical_inputs`；`window_candidates` → `window_realization_candidates_by_query`；默认 `window_implementation_id` → `default_window_realization_id`；`implementation_cost` → `default_window_cost_quote`；`max_retained_summary_bytes` → `retained_summary_memory_budget_bytes` |
| 同上：部署与保留 | `DeploymentEnvironment` → `PhysicalDeploymentContext`；`collector_ids` → `target_collector_ids`；`query_staleness_margin_ms` → `query_retention_margin_ms` |
| [候选定价与选择](../../../control_plane/src/physical/workload_cost.rs) | `CostDemand` → `CostComponentDemand`；`unit` → `pricing_basis`；`multiplicity` → `occurrences_per_horizon`；`AlternativeCost` → `CandidatePlanEvaluation`；`WorkloadCostComparison` → `CandidatePlanSelectionReport`；`alternative_id` → `candidate_id`；`physical_alternative_id` → `physical_candidate_id`；`alternatives` → `candidate_evaluations` |
| 同上：方法 | `with_exact_alternative` → `enumerate_exact_and_materialized_candidates`；`bind_alternative` → `compile_candidate_for_pricing`；`prepare_manifests` → `compile_candidates_for_pricing`；`select` → `select_lowest_cost_candidate`；`select_metricsql` → `select_lowest_cost_metricsql_candidate` |
| [诊断状态](../../../control_plane/src/physical/workload_cost/status.rs) | `status` 使用 `CandidateEvaluationStatus`；覆盖范围使用 `CandidateSearchScope`；兼容未知字符串与原缺省值。原 `PhysicalQueryFrontend` 移到共享编译入口并命名为 `QueryFrontend`，替代内部布尔参数 |
| [共享运行时算子](../../../crates/asap_types/src/query_plan/residual.rs) | `query_plan::logical::LogicalOperator` → `query_plan::residual::ResidualQueryOperator`；backend 对应模块迁移到 `residual`，保留旧模块导出 |
| [运行时计划与句柄](../../../data_plane/src/storage_engines/types/hot_reload_config.rs) | `ActivePhysicalPlan` → `RuntimePhysicalPlan`；`HotReloadActivePhysicalPlan` → `ActivePhysicalPlanHandle`；`HotReloadStreamingConfig` → `StreamingConfigHandle`；`runtime_config` → `streaming_config`；active handle 的 `snapshot` → `active_snapshot`；`from_active` → `from_active_physical_plan`；`retire_drained` → `mark_drained_plan_retired` |
| [存储元数据](../../../data_plane/src/storage_engines/sketch_db/index/mod.rs) | `SketchInstanceMetadata` → `SummarySeriesMetadata`；`sketch_index` 字段与变量 → `summary_store`；保留 `SketchStore` 类型 |
| 其余调用边界 | `types::QueryWorkload` → `LegacyMetricWorkload`；`ClickHouseSqlWorkload.sds` → `summary_catalog`；`aggregation_configs` → `materializations_by_policy_fingerprint`；`aggregation_id_for_key/value` → `key_policy_fingerprint/value_policy_fingerprint`；`data_plane::monitor` → `update_sampling` |

迁移规则：**Rust 名称更新，输出 wire 名称保持原样**；反序列化接受新名称作为 alias。旧公共类型导入及主要入口提供 deprecated 转发，但 Rust struct literal 的旧字段拼写无法通过类型别名兼容，源码消费者需要按表迁移。`None` / 空候选集合、浮点频次、报价身份、候选排序、严格小于的选择规则及计划生命周期均保持原义。

`erp` 保留并明确为 Error–Resource Profile；`WorkloadQuote.executable` 保留，因为它不代表安装或部署已经通过验证。双模式 streaming handle 的读写行为只补充说明；拆分写 API、删除 legacy 路径、统一 SQL 定价以及 typed ID / 时间单位迁移留作独立工作。英文流程图与边界说明见 [Planning terminology and architecture](planning-terminology.md)。

**一、组件职责与当前真实路径**

| 组件 | 实际职责 / 输入输出 | 命名判断 |
|---|---|---|
| 外部 ASAPPlanner | 解析与语义 IR，合法 summary/exact 候选，精度推理，逻辑选择；backend 提供具体成本和能力约束 | Planner 的语义选择与 backend 的物理候选比较是不同层次，不是两个重复 planner |
| `control_plane::planner_selection` | 适配 Planner 的选择调用、精度及证据；输出语义 DAG 与诊断 trace | `selection` 必须说明是 logical 还是 physical；trace 不是决定执行行为的配置 |
| `physical::compiler` | 输入规范化、窗口候选校验、调用逻辑选择，以及绑定具体物理实现，生成多个一致的计划投影 | `PhysicalPlanCompiler` 比 `PhysicalCompiler` 清楚；整个模块当前职责仍比单纯 lowering 更宽 |
| `physical::workload_cost` | 枚举工作负载级候选、编译、生成报价清单、核验报价、选择最低成本可行候选 | manifest、quote、evaluation、selection report 不应相互替代 |
| `physical::erp` | Error–Resource Profile 的部署适配、分布匹配、经验参数与资源估计 | `empirical_runtime_profile` 是错误展开；输入还包含策略与观测，不只一个 profile |
| `control_plane::clickhouse` | SQL frontend、逻辑选择、物理绑定；支持已有 catalog 输入与自动生成 materialization 两条路径 | `ClickHouseSqlWorkload.sds` 实际是 `SummaryCatalog`；SQL 当前没有走同一套 `workload_cost::select` 整计划报价流程 |
| `asap_types` | 跨组件共享的 catalog、SDS、生产/传输/预计算/查询/发布契约与验证逻辑 | 是共享契约的实际定义方；`control_plane` 中若仅 re-export，不是重复类型 |
| `physical::publication` + backend client + OpAMP | 从编译结果构造发布制品，安装请求、collector 发布与应用确认、backend 激活 | publication 是制品或发布过程；install request 是命令；不是 active runtime 对象 |
| data-plane HTTP 安装 + `PhysicalPlanLifecycle` | 验证跨计划一致性，stage、activate、drain、retire | `ActivePhysicalPlan` 被用于 staged map，名称把类型和生命周期阶段混在一起 |
| OTLP / Remote Write drivers + `SeriesIdResolver` | 协议接入、生产者/世代/帧校验、分配并解析物理 series 身份 | `collector_id` 是特定生产者身份；不能把所有 producer 无条件改叫 collector |
| `PrecomputeEngine` + workers + maintenance runtime | 按安装的 materialization 更新状态；派生维护消费已完成的源状态 | `PrecomputeEngineConfig` 是 worker/队列等引擎设置，`StreamingConfig` 是 materialization 的运行时视图，不是重复配置 |
| `SketchStore` + backfill / persistence | 保存 sketch 和精确聚合状态；SID 索引、覆盖与完整性、恢复和历史填充 | `SketchStore` 已比名称更广；领域表述宜用 summary store。全仓类型迁移可独立进行 |
| query engines / routing / summary execution | 用同一计划快照执行已安装 DAG、读取指定状态、执行精确子树或回退 | 生产路径不能在读请求里重新选择 materialization；legacy helper 应标明边界 |
| `control_plane::monitor` / `replan` | 旧工作负载路径上的指标抓取、违规/过期触发重规划与发布 | 与运行时 ERP 观测不是同一条自动闭环，不应画成一个通用 feedback planner |
| `data_plane::monitor` | 给边缘生产者协调 update-sampling grant | `monitor` 太宽，与 CP 的指标/SLA monitor 不同；建议模块领域名 `update_sampling` |
| `asap_otel_proto` | OTLP 等生成协议类型 | 保持协议边界；不能把协议 DTO 当成语义 catalog |
| `tools` / `scripts` / demos | 校准、离线证据、回放、评测与操作工具；有 JSON 消费者 | 序列化迁移必须审计这些调用方；它们不是运行时执行组件 |

英文流程架构图见 [Planning terminology and architecture](planning-terminology.md)。下文表格的“当前名称”指审查基线上的名称，右栏是迁移建议。

**二、不要把系统画成一条所有入口都相同的流水线**

PromQL / MetricsQL 的主要计划路径是：

`工作负载与证据 → Planner 语义选择 → 工作负载级物理候选 → 编译结果与 manifest → provider quote → 最低成本可行候选 → publication → runtime generation`

这里有三个不同范围的选择：语义 DAG 的选择、单个窗口物理实现的选择、整个工作负载物理候选的成本比较。它们并非重复实现。`with_exact_alternative` 的有界枚举也不保证找到所有未枚举实现的全局最优值。相同成本时，当前严格 `<` 比较保留先出现的候选。[编译器][C1]、[候选与定价][C2]

`prepare_manifests()` 编译后丢弃 plan，仅返回 manifests 和诊断行；部署选择阶段重新编译候选，再与证据中的 manifest 做完整相等比较。这是“发现/采集成本”和“使用成本部署”两个阶段，不应因为函数都做 compile 就直接删除其中一个。[C2]

ClickHouse 的自动路径直接从 SQL 选择与绑定构建 `PhysicalPlanPublication`；已有 catalog 路径接收 `sds + precompute_plan + transmission_plan`。它们与 PromQL 共用安装及运行时契约，但不能画成已经共用相同的整计划报价选择过程。建议两个请求对象分别叫 `ClickHouseCompilationInput`、`ClickHouseBindingInput`，或保留现名并明确 automatic / catalog-bound 区别。[C3]

旧 flat workload → `CollectionPlan` / stage emission 路径仍有实际调用；`PhysicalPlanner`、`PlanNode`、`PostAsapPlan` 等旧模块也留在公开模块树中。不要据名字推断它们都与当前 `PhysicalPlan` 等价，也不要据旧模块存在就断定它们全部处于主路径。[C4]

**三、#709 中应该修正或收紧的映射**

| 当前值 / 提议 | 实际语义 | 建议 |
|---|---|---|
| `erp` → `empirical_runtime_profile` | ERP = **Error–Resource Profile**；`ErpPlanningInput` 还含观测、匹配策略、mode、权重和能力 | 保留 `erp` 并写明定义，或字段 `error_resource_planning_input`；不要使用错误全称 |
| `PlanningQuery` → `SelectedQueryInput` | 对象先构造，再被 `select_workload_roots_with_trace(&mut queries, ...)` 更新；并非整个生命周期都已 selected | 倾向 `QueryCompilationInput`，不用类型名承诺当前代码没有保证的阶段 |
| `post_asap` → `selected_summary_plan_root` | Planner 选择的语义 DAG 根，也可装 `KeepPreAsap` exact 根 | `selected_plan_root` / `selected_logical_root`；不要暗示必定 materialized 或 approximate |
| `materialization_candidate_keys()` → `optional_materialization_ids()` | 编译前枚举用的候选 key，与 catalog 中的 `SummaryDefinitionId` 不是一类身份 | `eligible_materialization_keys()`；保留 key 与已绑定 ID 的区别 |
| `materialization_policy` → `enabled_optional_materializations` | `Option<BTreeSet<String>>`：`None` 启用全部 eligible，`Some(empty)` 不启用任何可选项 | `enabled_materialization_keys`，保留并明确三态语义；不要默认成普通空集合 |
| `materialization_leaf_contract` → 候选 keys | 函数实际上返回原始 source 的 metric、可选 window、filter contract | `raw_materialization_input_contract`；真正枚举 keys 的函数另行命名 |
| 所有 `leaf` 改为 materialization | DAG 结构叶节点、物化候选、执行中的 exact subtree 是不同对象 | 按消费点改；真正的图论 leaf 可保留，不能机械替换 Planner 术语 |
| `CostDemand.unit` → `cost_unit` | 值为 `horizon` / `query_evaluation`，表达报价的工作量基准，并非 CPU 秒、字节或货币单位 | `pricing_basis` / `demand_basis`；可定义 `PricingBasis` 枚举 |
| `multiplicity` → `occurrences_per_horizon` | 基准工作在 horizon 内的乘数；允许按频率得到浮点估计 | 支持；不可顺手改为整数。总成本是 quote × occurrences |
| `LifecycleCostEvidence` → `LifecycleCostRates` | 同时包含 build/read/retirement 单次成本和 maintenance/retention rate | `LifecycleCostModel` / `LifecycleUnitCosts`，不应全称 rates |
| `WorkloadQuote.executable` → `deployable` | provider 对匹配 manifest 的执行可行性声明；还需 compiler、证据、安装与发布验证 | 保留 `executable` 或用 `provider_feasible`；不要暗示已通过端到端部署检查 |
| `synthesized_window_queries` → `queries_with_derived_window_candidates` | 编译器生成报价的 query ID 集合，决定能否做共享 pane 重定价 | `compiler_priced_window_query_ids`；derived 已用于“由 summary 派生 summary”，易混淆 |
| `collector_ids` → `eligible_collector_ids` | 分布式模式下，每个 ID 都生成 `CollectorPlan` | `target_collector_ids`；代码没有在这些 eligible collector 中再选子集 |
| `query_staleness_margin_ms` → `max_query_staleness_ms` | 为滞后的查询增加保留状态量；自身不实现请求拒绝规则 | `query_retention_margin_ms`，或保留现名并说明用途；避免暗示已有 admission enforcement |
| `BackendLocalImplementation` → `BackendLocalPlanningInputs` | 外层也是 planning input；内层含成本、窗口候选、证据、ERP 与保留约束 | 外层 `BackendLocalPlanningInput`，内层倾向 `BackendLocalPhysicalInputs`；不为这些分类额外创建一组 wrapper |
| `ActivePhysicalPlan` 保持不动，只改 handle | 同一类型也用于 staged 计划和 draining 的旧快照 | 类型倾向 `RuntimePhysicalPlan`；handle 用 `ActivePhysicalPlanHandle`，phase 由 lifecycle 管理 |
| `build_active_physical_plan` → `validate_and_build_active_physical_plan` | 校验与构建，但不执行 activation | `validate_and_build_runtime_plan`；否则名字仍把构建和激活混淆 |
| 泛化的 `snapshot()` → `active_snapshot()` | active-plan handle 与独立 streaming-config handle 的快照含义不同 | 只在能保证 active 语义的 handle 上使用；不可全仓替换 |
| `runtime_policy` → `materialization_runtime_policy` | `RuntimeRulePolicy` 具体控制 sampling、delta、GOS 与 adaptation | 可接受；跨组件说明是 producer/update/transmission policy，不能与候选选择策略混淆 |

依据：[编译输入与选择 C1][C1]、[候选与定价 C2][C2]、[ERP C5][C5]、[候选 key C6][C6]、[pane 重定价 C7][C7]、[运行时生命周期 C8][C8]。

以下映射方向正确，可以作为同一轮小范围迁移：

| 当前名称 | 建议名称 / 约束 |
|---|---|
| `PlanningRequest` | `PhysicalCompilationRequest`；包含 workload 上下文，不是单 query |
| `PhysicalCompiler` | `PhysicalPlanCompiler` |
| `PhysicalPlan` | `CompiledPhysicalPlan`；候选和获选结果可继续复用此类型，无须新增 `SelectedPhysicalPlan` wrapper |
| `logical_selection` | `planner_selection_trace`；说明只做诊断 |
| `window_implementations` | `window_realization_candidates`；对象 `WindowImplementationCandidate` 也应相应命名 |
| `ImplementationCostEvidence` | `WindowRealizationCostQuote`；保留 model、时效与 workload scope |
| `window_candidates` | `window_realization_candidates_by_query`；注明 key 为注册查询文本，不是 query ID |
| `window_implementation_id` | 默认模板处用 `default_window_realization_id`，已选估计项用 `window_realization_id`，不能全仓统一加 default |
| `PlanningQuery.window_secs` | `query_lookback_seconds`；不可连带重命名其他类型的所有 window 字段 |
| `PlanningQuery.source` | `legacy_query_source` 可接受，但该值也写入 workload cost manifest，不能借改名删除或改变其报价身份 |
| `group_by` / `accuracy` | 在相应输入上用 `group_by_labels` / `accuracy_target`；输出精度结果不能改叫 target |
| `lifecycle` / `LifecyclePlanningInput` | `summary_lifecycle_inputs` / `SummaryLifecyclePlanningInputs` |
| `PlanningRequest.evidence` | `topk_membership_evidence_by_query_id`；输入 map 的 `topk_evidence` 是按查询文本，不应加 by_query_id |
| `hybrid_execution` | `allow_mixed_summary_and_exact_execution`；是允许编译的模式，不是本次请求必然采用混合执行 |
| `bind_alternative` / `prepare_manifests` | `compile_candidate_for_pricing` / `compile_candidates_for_pricing`；后者返回清单及诊断，不返回编译 plans |
| `with_exact_alternative` | `enumerate_exact_and_materialized_candidates`；保留有界搜索与枚举顺序 |
| `AlternativeCost` | `CandidatePlanEvaluation` 可接受，但必须允许尚未定价或编译失败；更中性的 `CandidatePlanDiagnostic` 也符合现有全阶段用途 |
| `WorkloadCostComparison` | `CandidatePlanSelectionReport` |
| `select` / `select_metricsql` | 可用 `select_lowest_cost_candidate` / `select_lowest_cost_metricsql_candidate`；文档写明 feasible 与枚举范围 |
| `metricsql: bool` | PromQL/MetricQL 路径使用 `QueryFrontend`；先审计已有 `PhysicalQueryFrontend`，不要再建第三份枚举；SQL 并未因此自动适配此入口 |
| `CostDemand` | `CostComponentDemand` |
| `publication()` | `to_publication_artifact()`，或沿用 `publication()`；它构造并校验制品，不执行 HTTP 发布 |
| `compile_transmission_plan` | `build_transmission_plan`；从 precompute 和 runtime policies 构造，不一定要把全部参数编码进长函数名 |
| `DeploymentEnvironment` | `PhysicalDeploymentContext`；target、capability、time、generation 都是上下文 |
| `max_retained_summary_bytes` | `retained_summary_memory_budget_bytes`，保留旧缺省值、别名与具体计量定义 |
| `ActivePhysicalPlan.runtime_config` | `streaming_config`，准确表达持有的配置类型 |
| `aggregation_configs` | `materializations_by_policy_fingerprint`；目前 key 是 `u64` 形式 fingerprint，改名不等于已经升级 typed key |
| `get_all_aggregation_configs()` | `materializations()`；明确当前仍返回 map |
| `from_active()` | `from_active_physical_plan()` |
| `retire_drained()` | `mark_drained_plan_retired()`；它标记 lifecycle status，不执行存储 GC |

**四、全仓中 #709 漏掉的歧义与重复**

| 对象 | 判断 | 最小处理 |
|---|---|---|
| backend `types::QueryWorkload` 与 Planner `workload::QueryWorkload` | **同名不同义**：前者是单 metric 的 flat legacy 意图；后者是 canonical 工作负载 | legacy 侧叫 `LegacyMetricWorkload` 或导入时显式 alias |
| `PlanNode` / `CollectionPlan` / `PhysicalPlan` / `QueryPlan` | **不同层级**：旧 annotated stage tree、旧采集方案、完整物理 bundle、工作负载的查询执行条目集合 | 优先给旧类型/模块加 legacy 或 stage 语义；保留新契约的区别 |
| `QueryPlan` 与 `QueryPlanEntry` | 前者实际上是按 canonical identity 查找的一组查询执行计划 | 可将前者描述为 query execution catalog；若做 API 迁移再考虑 `QueryExecutionCatalog` / `QueryExecutionPlan`，#709 不必强行扩展 |
| `query_plan::logical::LogicalOperator` | **阶段误导**：内容是已安装的 residual 运行时算子，包括 exact subquery、scan、aggregate | `ResidualQueryOperator`，模块 `residual`；对应 `prepare_logical` 按实际职责命名 |
| `ClickHouseSqlWorkload.sds` | **层级混淆**：保存整个 `SummaryCatalog`，不是单个 Self-Describing Summary | `summary_catalog` |
| `AggregationConfig` 与 `PrecomputeMaterialization` | **同一个类型的兼容别名**，不是两种配置对象 | 新代码用 `PrecomputeMaterialization`；保留旧 alias 过渡，不复制定义 |
| `AggregationIdInfo.aggregation_id_for_key/value` | **历史身份名**：注释明确已是 policy fingerprint 的 u64 | 使用 `key_policy_fingerprint` / `value_policy_fingerprint`；typed ID 迁移单独评估 |
| `SummaryDefinitionId` 与 `PolicyFingerprint` | **合理的语义 wrapper**：前者是 catalog 中的定义引用，后者是兼容内容 fingerprint | 跨 catalog API 优先前者；不能把它与 SID 或 descriptor ID 合并 |
| `SketchInstanceMetadata` | **instance 的粒度错误**：按 SID 管理长期 series 元数据；真正 SDS instance 另外包含具体时间范围 | `SummarySeriesMetadata`，避免与 `SummaryInstance` 混为一个窗口对象 |
| `SketchStore` / `sketch_index` | **名称比职责窄**：对象还保存 exact accumulator、持久化、生命周期及状态读取 | 文档统一称 summary store；`sketch_index` 变量可先改 `summary_store`，全面类型迁移另行处理 |
| `SummaryCatalog` / `sketch_catalog` / SQL catalog / `PolicyRegistry` | **不是重复 catalog**：已安装定义、算法候选/默认参数、源表 schema、运行时 config 派生 lookup | 分别明确 `summary catalog`、`sketch capabilities`、`source schema catalog`、`materialization lookup` |
| `PrecomputePlan.materializations` 与 `StreamingConfig.aggregation_configs` | **合理投影**：安装验证后构建的运行时 map，不是两个应独立编辑的配置真源 | 文档声明 runtime config 来源；保证同一世代，不因字段重复删除验证 |
| `PhysicalPlan` / Publication / InstallRequest / Runtime plan | **合理边界重复**：编译诊断、共享发布制品、带 routing/evidence 的安装命令、持有 Arc 的运行时对象 | 继续使用边界构造和交叉验证；无需一个承载全部阶段的大对象 |
| `HotReloadStreamingConfig.inner` 与 `.active` | **真实双来源**：active 模式读 active plan，`swap` 仍写 inner，写入不成为 snapshot 的结果 | 先将模式写清；若拆为 legacy 可写句柄与 active 只读视图，必须独立定义 API 行为与测试 |
| `summary_exec` / `summary_executor` | 通用执行接口/调度与具体 store-backed 实现，名称近似但不是可直接去重的函数集合 | 更明确的 module docs；若迁移可用 `summary_execution` / `store_summary_reader`，保持执行语义边界 |
| `control_plane::monitor` 与 `data_plane::monitor` | **同名不同功能**：指标/违规监控 vs 更新采样分配 | 用职责名区分，不将它们都当作 ERP 实时反馈 |
| `summary_catalog` / `sds` / `canonical` 中的 re-export | **兼容入口，不是重复实现** | 建立 canonical import 路径后逐步减少旧入口；不要复制 shared DTO |

依据：[旧 workload C4][C4]、[共享 materialization C9][C9]、[SDS C10][C10]、[catalog C11][C11]、[runtime config C12][C12]、[存储元数据 C13][C13]、[residual operators C14][C14]、[shared publication C15][C15]。

**五、必须保留的身份、时间和生命周期区别**

`plan_id / plan_version` 标识部署决定与世代；`CatalogGeneration` 额外携带 catalog 的摘要。`SummaryDescriptorId` 表达算法/状态语义，`DataDescriptorId` 表达输入 population；`SummaryDefinitionId` 绑定这些定义与物理布局。SID 表达具体物理 series 生命周期，而 `SummaryInstance` 表达具体时间范围和 group 的状态，并携带物理引用、来源与完整性。不能因为底层有些都用 u64，就全部命名为 `id` 或 `materialization_id`。[SDS][C10]、[Catalog][C11]、[存储元数据][C13]

`window` 至少有 query lookback、evaluation interval、stored pane duration、slide、origin、retention、producer emission cadence 等含义。`PrecomputeMaterialization::stored_window_ms()` 对 FullWindow 与 pane layout 的处理不同，不能全仓把 `window_size` 替换成 `query_lookback_seconds`。改名应保留各字段当前单位；单位转换属于额外行为变更。[C9]

生命周期至少有三条轴：计划 `Staged / Active / Draining / Retired`，materialization `Materializing / Ready / Serving`，以及具体实例的 completeness。一个计划已 Active，并不表示其每个查询范围都已完整；读路径仍需校验状态。`retire_drained` 只是状态标记，不表示已经回收所有持久化数据。[计划生命周期][C8]、[SDS][C10]

`snapshot` 对 immutable catalog 和 `Arc` 读取是合适术语，不应一律删除。对于 `BackendLocalPlanningSnapshot`，代码主要将其消费为输入，改成 input 合理；但要同时决定 `snapshot_version` 的 schema 名称和兼容策略。

**六、迁移与验收建议**

1. 先写下字段语义、所属阶段、key 范围和单位；修正 ERP、成本基准、runtime/staged、target collectors 等会误导实现的名称。把 #709 正文与补充评论合并为一份不冲突的映射。
2. 做内部函数/局部变量改名，优先保持数据结构和算法原样。不要为每个生命周期步骤引入一个新 wrapper。`physical` 的旧路径隔离、hot-reload handle 拆分和 shared schema 版本升级分别处理。
3. 公共 Rust 字段改名不能靠 type alias 或 forwarding method 保持兼容。先识别实际外部消费者；crate 根文档也声明部分 public module 仅供 workspace 内使用，因此不需要不加区分地给每个 public symbol 建长期兼容层。
4. 保留 wire 输出时，用 `serde(rename = "old_name")` 固定旧名称，视需要接受新名 alias。仅 `alias = "old_name"` 无法保护旧消费者读取新输出。还需检查手工 JSON/YAML 生成、Python 校准与回放工具、CLI 输出和已有 fixture。
5. 名称变化可能影响身份：catalog digest 对序列化内容求 SHA-256，编译计划也存在对序列化 materialization 求 hash 的路径。验收应检查 plan/materialization/catalog identity、component keys、manifest 匹配不变，不只是能够 deserialize。
6. 枚举 `status` 与 `search_scope` 时保留原 wire 表达，或明确版本迁移。现有 status 含 `bind_failed`、`bound`、`evidence_missing`、`rejected`、`evidence_invalid`、`unselected`、`selected`，且 `status` 缺省为空字符串。typed scope 还应保留“bounded inventory 不承诺未枚举最优”的解释信息。
7. 运行既有 compiler、cost selection、publication、安装 lifecycle、query execution 与 backend process E2E。针对实际 wire/identity 变动补兼容断言：双向读写、缺省行为、None/empty 集合、同成本选择顺序、staged 不生效、旧读者 drain。纯局部改名无需逐字段补镜像测试。

额外维护问题：`docs/developer_docs/data-structure-ownership-audit.md` 仍说多个 wire DTO 在 control_plane、尚待搬迁，但当前已经在 `asap_types`；`PrecomputeEngine` 注释还称 Remote Write 已删除，而 driver 与启动代码中已存在。命名 PR 应同步修正相关陈旧说明，否则新的词汇表仍会被旧架构描述覆盖。

[C1]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/control_plane/src/physical/compiler.rs
[C2]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/control_plane/src/physical/workload_cost.rs
[C3]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/control_plane/src/clickhouse.rs
[C4]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/control_plane/src/types.rs#L266
[C5]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/control_plane/src/physical/erp.rs
[C6]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/control_plane/src/query_plan/logical.rs#L1105
[C7]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/control_plane/src/physical/pane_reuse.rs
[C8]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/data_plane/src/storage_engines/types/hot_reload_config.rs
[C9]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/crates/asap_types/src/aggregation_config.rs
[C10]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/crates/asap_types/src/sds.rs
[C11]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/crates/asap_types/src/summary_catalog.rs
[C12]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/data_plane/src/storage_engines/types/streaming_config.rs
[C13]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/data_plane/src/storage_engines/sketch_db/index/mod.rs#L209
[C14]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/crates/asap_types/src/query_plan/logical.rs
[C15]: https://github.com/ProjectASAP/ASAPQuery-backend/blob/b1a58ca810d7b7347f2cc5f30924db56532bbb6b/crates/asap_types/src/plan_publication.rs
