use crate::precompute_engine::accumulator_factory::{
    create_accumulator_updater, AccumulatorUpdater,
};
use crate::precompute_engine::config::LateDataPolicy;
use crate::precompute_engine::metrics::record_late_input;
use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
use crate::precompute_engine::output_sink::OutputSink;
use crate::precompute_engine::series_router::WorkerMessage;
use crate::precompute_engine::window_manager::WindowManager;
use crate::storage_engines::types::{
    AggregateCore, HotReloadStreamingConfig, KeyByLabelValues, PrecomputedOutput,
};
use asap_types::aggregation_config::AggregationConfig;
use asap_types::PolicyFingerprint;
use std::collections::{BTreeMap, HashMap};
// (PolicyFingerprint is used for both `PolicyFingerprint::from_config(...)`
//  on the emit path and the `policy_fp` field of GroupState below.)
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, debug_span, info, warn};

/// Per-bucket aggregation state: window manager + active pane accumulators.
///
/// B7.6 (schema-retirement #5): one `GroupState` per `sid`, where `sid` is
/// the registry-allocated identity for `(metric, attrs, agg_kind)`. The
/// legacy `(agg_id, group_key)` tuple folds into this single u64 — the
/// grouping label values participate in `attrs`, and the source policy
/// participates in `agg_kind`, so distinct buckets always carry distinct
/// sids. `policy_fp` and `group_key` are held here so the worker can
/// recover the source config (for window shape / late-data policy) and
/// the emit-time `KeyByLabelValues` without re-parsing the sid.
///
/// All raw series sharing the same sid feed into the same accumulator,
/// producing one output per (sid, window) — exactly like Arroyo's
/// `GROUP BY window, key`.
struct GroupState {
    config: Arc<AggregationConfig>,
    /// Source policy fingerprint that minted this sid. Held so
    /// `evict_orphaned_groups` can check liveness against the streaming
    /// config snapshot (a sid stays alive only while its source policy is
    /// still configured), and so the worker can re-derive the
    /// `PolicyFingerprint` on the emit path without a second config
    /// fingerprint pass.
    policy_fp: PolicyFingerprint,
    /// Grouping label values joined by semicolons. Held so the emit path
    /// can render the output's `KeyByLabelValues` without consulting the
    /// sid → attrs reverse mapping. Format matches the input messages'
    /// `group_key` field.
    group_key: String,
    window_manager: WindowManager,
    /// Active panes for raw-sample accumulation, keyed by pane_start_ms.
    active_panes: BTreeMap<i64, Box<dyn AccumulatorUpdater>>,
    /// Active panes for pre-built accumulator inputs (e.g. OTLP-delivered
    /// sketches), keyed by pane_start_ms. Each entry is the running merge
    /// of every accumulator that landed in that pane's time range. Kept
    /// separate from `active_panes` because sketches come in as opaque
    /// `Box<dyn AggregateCore>` objects and do not share the updater
    /// machinery used for incremental sample updates.
    sketch_panes: BTreeMap<i64, Box<dyn AggregateCore>>,
    /// Maximum event timestamp actually observed from input for this group.
    /// Periodic flushes never modify this value.
    max_event_time_ms: i64,
    /// Monotonic watermark through which windows have already been closed.
    /// This is separate from observed event time because wall-clock policies
    /// may close a window without manufacturing a later input timestamp.
    closure_watermark_ms: i64,
    /// First and latest wall-clock input times for each open pane. The former
    /// enforces an absolute lifetime; the latter supports idle closure.
    pane_wall_clock: BTreeMap<i64, PaneWallClock>,
}

#[derive(Clone, Copy)]
struct PaneWallClock {
    first_touch_ms: i64,
    last_touch_ms: i64,
}

impl GroupState {
    fn touch_pane(&mut self, pane_start_ms: i64, now_ms: i64) {
        self.pane_wall_clock
            .entry(pane_start_ms)
            .and_modify(|clock| clock.last_touch_ms = now_ms)
            .or_insert(PaneWallClock {
                first_touch_ms: now_ms,
                last_touch_ms: now_ms,
            });
    }

    fn prune_pane_wall_clock(&mut self) {
        let active = &self.active_panes;
        let sketch = &self.sketch_panes;
        self.pane_wall_clock
            .retain(|ps, _| active.contains_key(ps) || sketch.contains_key(ps));
    }
}

/// Runtime configuration for a Worker, grouping non-structural parameters.
pub struct WorkerRuntimeConfig {
    pub max_buffer_per_series: usize,
    pub allowed_lateness_ms: i64,
    pub pass_raw_samples: bool,
    pub raw_mode_aggregation_id: u64,
    pub late_data_policy: LateDataPolicy,
    pub wall_clock_idle_grace_period_ms: i64,
    pub wall_clock_max_open_grace_period_ms: i64,
}

/// Worker that processes samples for a shard of the sid space.
///
/// Unlike the old per-series design, this worker maintains accumulators
/// keyed by `sid` (B7.6 — was `(agg_id, group_key)`). Multiple raw series
/// with the same grouping label values share a single accumulator,
/// producing one merged output per window — matching Arroyo's `GROUP BY`
/// semantics. The grouping label values participate in the sid via the
/// `(metric, attrs_fingerprint, agg_kind_canonical)` identity contract on
/// `SeriesIdResolver`, so one sid uniquely names one bucket.
pub struct Worker {
    id: usize,
    receiver: mpsc::Receiver<WorkerMessage>,
    output_sink: Arc<dyn OutputSink>,
    /// Map from sid to per-bucket state. One entry per active sid this
    /// worker shard owns.
    group_states: HashMap<u64, GroupState>,
    /// Hot-reload handle — workers read config directly from ArcSwap
    /// instead of holding a local copy. All components see the same
    /// config at the same time.
    hot_reload: HotReloadStreamingConfig,
    /// Allowed lateness in ms.
    allowed_lateness_ms: i64,
    /// When true, skip aggregation and pass raw samples through.
    pass_raw_samples: bool,
    /// Aggregation ID stamped on each raw-mode output.
    raw_mode_aggregation_id: u64,
    /// Policy for handling late samples that arrive after their window has closed.
    late_data_policy: LateDataPolicy,
    /// This worker's maximum observed event-time watermark, exposed only for
    /// diagnostics. It is never propagated into another group's closure state.
    worker_watermark: Arc<AtomicI64>,
    /// Externally-readable group count for diagnostics.
    group_count: Arc<AtomicUsize>,
    wall_clock_idle_grace_period_ms: i64,
    wall_clock_max_open_grace_period_ms: i64,
    /// Injectable clock returning current wall-clock time in milliseconds
    /// since the unix epoch. Production uses `SystemTime::now`; tests
    /// override with a deterministic fake. The closure runs under
    /// `&mut self` only on `flush_all` and pane creation, so a single
    /// non-`Sync` cell behind a mutex is fine — but we keep the bound
    /// `Send + Sync` for clarity since `Worker` itself is `Send`.
    now_ms_fn: Box<dyn Fn() -> i64 + Send + Sync>,
}

impl Worker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: usize,
        receiver: mpsc::Receiver<WorkerMessage>,
        output_sink: Arc<dyn OutputSink>,
        hot_reload: HotReloadStreamingConfig,
        runtime_config: WorkerRuntimeConfig,
        group_count: Arc<AtomicUsize>,
        worker_watermark: Arc<AtomicI64>,
    ) -> Self {
        let WorkerRuntimeConfig {
            max_buffer_per_series: _,
            allowed_lateness_ms,
            pass_raw_samples,
            raw_mode_aggregation_id,
            late_data_policy,
            wall_clock_idle_grace_period_ms,
            wall_clock_max_open_grace_period_ms,
        } = runtime_config;
        Self {
            id,
            receiver,
            output_sink,
            group_states: HashMap::new(),
            hot_reload,
            allowed_lateness_ms,
            pass_raw_samples,
            raw_mode_aggregation_id,
            late_data_policy,
            worker_watermark,
            group_count,
            wall_clock_idle_grace_period_ms,
            wall_clock_max_open_grace_period_ms,
            now_ms_fn: Box::new(default_now_ms),
        }
    }

    /// Test/diagnostic-only setter for the wall-clock source. Replaces
    /// the default `SystemTime::now`-backed clock with a deterministic
    /// fake so unit tests can drive the wall-clock fallback in
    /// `flush_all` without `std::thread::sleep`. Production code never
    /// calls this.
    #[cfg(test)]
    pub fn set_now_ms_fn(&mut self, f: Box<dyn Fn() -> i64 + Send + Sync>) {
        self.now_ms_fn = f;
    }

    /// Run the worker loop. Blocks until shutdown.
    pub async fn run(mut self) {
        info!("Worker {} started", self.id);

        while let Some(msg) = self.receiver.recv().await {
            match msg {
                WorkerMessage::GroupSamples {
                    sid,
                    policy_fp,
                    group_key,
                    samples,
                    ingest_received_at,
                } => {
                    let sample_count = samples.len();
                    let _span = debug_span!(
                        "worker_process_group",
                        worker_id = self.id,
                        sid,
                        policy_fp = %policy_fp,
                        group = %group_key,
                        sample_count,
                    )
                    .entered();
                    if let Err(e) = self.process_group_samples(sid, policy_fp, &group_key, samples)
                    {
                        warn!(
                            "Worker {} error processing sid={} (policy_fp={}, group={}): {}",
                            self.id, sid, policy_fp, group_key, e
                        );
                    }
                    debug!(
                        e2e_latency_us = ingest_received_at.elapsed().as_micros() as u64,
                        "e2e: ingest->worker complete"
                    );
                }
                WorkerMessage::RawSamples {
                    series_key,
                    samples,
                    ingest_received_at,
                } => {
                    let _span = debug_span!(
                        "worker_process_raw",
                        worker_id = self.id,
                        series = %series_key,
                        sample_count = samples.len(),
                    )
                    .entered();
                    if let Err(e) = self.process_samples_raw(&series_key, samples) {
                        warn!("Worker {} raw error for {}: {}", self.id, series_key, e);
                    }
                    debug!(
                        e2e_latency_us = ingest_received_at.elapsed().as_micros() as u64,
                        "e2e: ingest->worker complete (raw)"
                    );
                }
                WorkerMessage::AccumulatorInput {
                    sid,
                    policy_fp,
                    group_key,
                    timestamp_ms,
                    accumulator,
                    ingest_received_at,
                } => {
                    let _span = debug_span!(
                        "worker_process_accumulator",
                        worker_id = self.id,
                        sid,
                        policy_fp = %policy_fp,
                        group = %group_key,
                        timestamp_ms,
                        accumulator_type = accumulator.type_name(),
                    )
                    .entered();
                    if let Err(e) = self.process_accumulator_input(
                        sid,
                        policy_fp,
                        &group_key,
                        timestamp_ms,
                        accumulator,
                    ) {
                        warn!(
                            "Worker {} accumulator input error for sid={} (policy_fp={}, group={}): {}",
                            self.id, sid, policy_fp, group_key, e
                        );
                    }
                    debug!(
                        e2e_latency_us = ingest_received_at.elapsed().as_micros() as u64,
                        "e2e: ingest->worker complete (accumulator)"
                    );
                }
                WorkerMessage::Flush => {
                    if let Err(e) = self.flush_all() {
                        warn!("Worker {} flush error: {}", self.id, e);
                    }
                    // Evict orphaned GroupStates whose agg_id has been
                    // removed from the config. Panes that still have
                    // data are kept until they drain (flush_all already
                    // closed their windows); empty ones are freed.
                    self.evict_orphaned_groups();
                }
                WorkerMessage::Shutdown => {
                    info!("Worker {} shutting down", self.id);
                    if let Err(e) = self.flush_all() {
                        warn!("Worker {} final flush error: {}", self.id, e);
                    }
                    // Force-close any windows still open after the final flush.
                    // The wall-clock fallback may not yet be due for a one-shot
                    // batch, so the trailing window can remain open. No more
                    // samples will arrive after shutdown; close every pane.
                    if let Err(e) = self.force_close_all() {
                        warn!("Worker {} shutdown force-close error: {}", self.id, e);
                    }
                    break;
                }
            }
        }

        info!(
            "Worker {} stopped, {} active groups",
            self.id,
            self.group_states.len()
        );
    }

    /// Get or create the GroupState for a sid.
    ///
    /// B7.6 — buckets are now keyed by `sid` (a single u64) rather than
    /// `(agg_id, group_key)`. `policy_fp` is the source config's
    /// fingerprint, used to fetch the `AggregationConfig` from the
    /// hot-reload snapshot the first time we see this sid; `group_key` is
    /// remembered on the `GroupState` for emit-time label rendering.
    ///
    /// Reads config directly from the `HotReloadStreamingConfig`
    /// ArcSwap handle, so new policies from a config swap are visible
    /// immediately — no message passing, no delay.
    /// Returns None if `policy_fp` has no matching config (e.g. arrived
    /// after the policy was retired).
    fn get_or_create_group_state(
        &mut self,
        sid: u64,
        policy_fp: PolicyFingerprint,
        group_key: &str,
    ) -> Option<&mut GroupState> {
        if !self.group_states.contains_key(&sid) {
            let snap = self.hot_reload.snapshot();
            let cfg = snap.get_aggregation_config(policy_fp.as_u64())?;
            let config = Arc::new(cfg.clone());
            let gs = GroupState {
                window_manager: WindowManager::new(config.window_size, config.slide_interval),
                config,
                policy_fp,
                group_key: group_key.to_string(),
                active_panes: BTreeMap::new(),
                sketch_panes: BTreeMap::new(),
                max_event_time_ms: i64::MIN,
                closure_watermark_ms: i64::MIN,
                pane_wall_clock: BTreeMap::new(),
            };
            self.group_states.insert(sid, gs);
            self.group_count
                .store(self.group_states.len(), Ordering::Relaxed);
        }
        self.group_states.get_mut(&sid)
    }

    /// Process a batch of samples for a specific sid bucket.
    /// All samples in the batch feed into the same shared accumulator.
    ///
    /// This is the core of the Arroyo-equivalent GROUP BY logic.
    /// B7.6 — buckets are keyed by `sid`; `policy_fp` is the source
    /// `AggregationConfig` fingerprint used to resolve the bucket's
    /// config on first sight; `group_key` is held on the resulting
    /// `GroupState` for emit-time label rendering.
    pub fn process_group_samples(
        &mut self,
        sid: u64,
        policy_fp: PolicyFingerprint,
        group_key: &str,
        samples: Vec<(String, i64, f64)>, // (series_key, timestamp_ms, value)
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let worker_id = self.id;
        let allowed_lateness_ms = self.allowed_lateness_ms;
        let late_data_policy = self.late_data_policy;
        let now_ms = (self.now_ms_fn)();

        if self
            .get_or_create_group_state(sid, policy_fp, group_key)
            .is_none()
        {
            warn!(
                "Worker {} skipping samples for unknown policy_fp={} (sid={}, group_key={})",
                self.id, policy_fp, sid, group_key
            );
            return Ok(());
        }
        let state = self.group_states.get_mut(&sid).unwrap();

        // Find the timestamp span in this batch. A first batch may contain
        // several windows (Prometheus commonly sends catch-up samples after
        // startup), so its minimum timestamp is also the initial closure
        // scan boundary.
        let batch_min_ts = samples
            .iter()
            .map(|(_, ts, _)| *ts)
            .min()
            .unwrap_or(i64::MIN);
        let batch_max_ts = samples
            .iter()
            .map(|(_, ts, _)| *ts)
            .max()
            .unwrap_or(i64::MIN);
        let previous_event_time = state.max_event_time_ms;
        let current_event_time = if batch_max_ts > previous_event_time {
            batch_max_ts
        } else {
            previous_event_time
        };
        let event_watermark = watermark_for_event_time(current_event_time, allowed_lateness_ms);
        let previous_closure_watermark = state.closure_watermark_ms;

        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> = Vec::new();

        // Route each sample to its pane
        for (series_key, ts, val) in &samples {
            let too_late = previous_event_time != i64::MIN
                && *ts < watermark_for_event_time(previous_event_time, allowed_lateness_ms);
            let pane_start = state.window_manager.pane_start_for(*ts);
            let pane_end = pane_start + state.window_manager.slide_interval_ms();
            let pane_closed = !state.active_panes.contains_key(&pane_start)
                && previous_closure_watermark >= pane_start + state.window_manager.window_size_ms();

            if too_late || pane_closed {
                let window_start = pane_start;
                let window_end = pane_start + state.window_manager.window_size_ms();
                match late_data_policy {
                    LateDataPolicy::Drop => {
                        record_late_input("drop", "raw_sample");
                        debug!(
                            "Worker {} dropping late sample for sid={} (group={}): \
                             ts={} observed_event_time={} pane=[{}, {})",
                            worker_id,
                            sid,
                            group_key,
                            ts,
                            previous_event_time,
                            pane_start,
                            pane_end
                        );
                        continue;
                    }
                    LateDataPolicy::ForwardToStore => {
                        record_late_input("append_correction", "raw_sample");
                        let mut updater = create_accumulator_updater(&state.config);
                        apply_sample(&mut *updater, series_key, *val, *ts, &state.config);
                        let key = build_group_key_label_values(group_key);
                        let output = PrecomputedOutput::new(
                            window_start as u64,
                            window_end as u64,
                            Some(key),
                            PolicyFingerprint::from_config(&state.config),
                        );
                        emit_batch.push((output, updater.take_accumulator()));
                        debug!(
                            "Forwarding late sample to store for evicted pane [{}, {})",
                            pane_start, pane_end
                        );
                        continue;
                    }
                }
            }

            // Normal path: route sample to its single pane accumulator.
            // Refresh the pane's wall-clock last-touch time so the fallback
            // only closes an idle pane, not a long-running bulk ingest whose
            // records share one event timestamp.
            state.touch_pane(pane_start, now_ms);
            let updater = state
                .active_panes
                .entry(pane_start)
                .or_insert_with(|| create_accumulator_updater(&state.config));

            apply_sample(&mut **updater, series_key, *val, *ts, &state.config);
        }

        // Check for closed windows
        let closure_scan_start = if previous_closure_watermark == i64::MIN {
            batch_min_ts
        } else {
            previous_closure_watermark
        };
        let closed = state
            .window_manager
            .closed_windows(closure_scan_start, event_watermark);

        for window_start in &closed {
            let (_, window_end) = state.window_manager.window_bounds(*window_start);
            let pane_starts = state.window_manager.panes_for_window(*window_start);

            if let Some(accumulator) = merge_panes_for_window(&mut state.active_panes, &pane_starts)
            {
                let key = build_group_key_label_values(group_key);
                let output = PrecomputedOutput::new(
                    *window_start as u64,
                    window_end as u64,
                    Some(key),
                    PolicyFingerprint::from_config(&state.config),
                );
                emit_batch.push((output, accumulator));
            }
        }

        state.max_event_time_ms = current_event_time;
        if event_watermark > state.closure_watermark_ms {
            state.closure_watermark_ms = event_watermark;
        }
        state.prune_pane_wall_clock();

        // Emit to output sink
        if !emit_batch.is_empty() {
            debug!(
                "Worker {} emitting {} outputs for sid={} (group={})",
                worker_id,
                emit_batch.len(),
                sid,
                group_key
            );
            self.output_sink.emit_batch(emit_batch)?;
        }

        Ok(())
    }

    /// Process a pre-built accumulator (e.g. an OTLP-delivered sketch) for a
    /// specific sid bucket's pane.
    ///
    /// The incoming accumulator is merged into `sketch_panes[pane_start]` via
    /// `AggregateCore::merge_with`. If the pane is empty the accumulator is
    /// installed as-is. If the pane's window has already closed, the late
    /// data policy decides whether to drop it or forward it to the sink as
    /// a standalone output so no data is silently lost.
    ///
    /// Unlike `process_group_samples`, this path does not touch
    /// `active_panes` — sketches live in their own pane map and get merged
    /// at window close (see `merge_sketch_panes_for_window`).
    ///
    /// `policy_fp` / `group_key` carry the same semantics as on
    /// `process_group_samples` — policy lookup + emit-time label rendering.
    pub fn process_accumulator_input(
        &mut self,
        sid: u64,
        policy_fp: PolicyFingerprint,
        group_key: &str,
        timestamp_ms: i64,
        incoming: Box<dyn AggregateCore>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let worker_id = self.id;
        let allowed_lateness_ms = self.allowed_lateness_ms;
        let late_data_policy = self.late_data_policy;
        let now_ms = (self.now_ms_fn)();

        if self
            .get_or_create_group_state(sid, policy_fp, group_key)
            .is_none()
        {
            warn!(
                "Worker {} skipping accumulator input for unknown policy_fp={} (sid={}, group_key={})",
                self.id, policy_fp, sid, group_key
            );
            return Ok(());
        }
        let state = self.group_states.get_mut(&sid).unwrap();

        let previous_event_time = state.max_event_time_ms;
        let current_event_time = if timestamp_ms > previous_event_time {
            timestamp_ms
        } else {
            previous_event_time
        };
        let event_watermark = watermark_for_event_time(current_event_time, allowed_lateness_ms);
        let previous_closure_watermark = state.closure_watermark_ms;

        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> = Vec::new();

        // Late-arrival check against the existing watermark.
        let too_late = previous_event_time != i64::MIN
            && timestamp_ms < watermark_for_event_time(previous_event_time, allowed_lateness_ms);
        let pane_start = state.window_manager.pane_start_for(timestamp_ms);
        let pane_closed = !state.sketch_panes.contains_key(&pane_start)
            && previous_closure_watermark >= pane_start + state.window_manager.window_size_ms();

        if too_late || pane_closed {
            match late_data_policy {
                LateDataPolicy::Drop => {
                    record_late_input("drop", "prebuilt_sketch");
                    debug!(
                        "Worker {} dropping late accumulator input for sid={} (group={}): ts={} watermark={}",
                        worker_id, sid, group_key, timestamp_ms, previous_event_time
                    );
                }
                LateDataPolicy::ForwardToStore => {
                    record_late_input("append_correction", "prebuilt_sketch");
                    let window_start = pane_start;
                    let window_end = pane_start + state.window_manager.window_size_ms();
                    let key = build_group_key_label_values(group_key);
                    let output = PrecomputedOutput::new(
                        window_start as u64,
                        window_end as u64,
                        Some(key),
                        PolicyFingerprint::from_config(&state.config),
                    );
                    emit_batch.push((output, incoming));
                    debug!(
                        "Forwarding late accumulator input to store for evicted pane [{}, {})",
                        pane_start,
                        pane_start + state.window_manager.slide_interval_ms()
                    );
                    self.output_sink.emit_batch(emit_batch)?;
                }
            }
            return Ok(());
        }

        // Refresh the pane's wall-clock last-touch time so an active sketch
        // stream with a fixed event timestamp is not force-closed mid-ingest.
        state.touch_pane(pane_start, now_ms);

        // Merge into the sketch pane covering this timestamp.
        match state.sketch_panes.remove(&pane_start) {
            Some(existing) => {
                let merged = existing
                    .merge_with(incoming.as_ref())
                    .map_err(|e| format!("merge_with failed for pane {pane_start}: {e}"))?;
                state.sketch_panes.insert(pane_start, merged);
            }
            None => {
                state.sketch_panes.insert(pane_start, incoming);
            }
        }

        // Check for closed windows and emit merged outputs.
        let closed = state
            .window_manager
            .closed_windows(previous_closure_watermark, event_watermark);
        for window_start in &closed {
            let (_, window_end) = state.window_manager.window_bounds(*window_start);
            let pane_starts = state.window_manager.panes_for_window(*window_start);

            // Emit from the raw-sample pane map (in case both sources are
            // populated for the same group; rare but supported).
            if let Some(accumulator) = merge_panes_for_window(&mut state.active_panes, &pane_starts)
            {
                let key = build_group_key_label_values(group_key);
                let output = PrecomputedOutput::new(
                    *window_start as u64,
                    window_end as u64,
                    Some(key),
                    PolicyFingerprint::from_config(&state.config),
                );
                emit_batch.push((output, accumulator));
            }

            // Emit from the sketch pane map.
            if let Some(accumulator) =
                merge_sketch_panes_for_window(&mut state.sketch_panes, &pane_starts)
            {
                let key = build_group_key_label_values(group_key);
                let output = PrecomputedOutput::new(
                    *window_start as u64,
                    window_end as u64,
                    Some(key),
                    PolicyFingerprint::from_config(&state.config),
                );
                emit_batch.push((output, accumulator));
            }
        }

        state.max_event_time_ms = current_event_time;
        if event_watermark > state.closure_watermark_ms {
            state.closure_watermark_ms = event_watermark;
        }
        state.prune_pane_wall_clock();

        if !emit_batch.is_empty() {
            debug!(
                "Worker {} emitting {} sketch outputs for sid={} (group={})",
                worker_id,
                emit_batch.len(),
                sid,
                group_key
            );
            self.output_sink.emit_batch(emit_batch)?;
        }

        Ok(())
    }

    /// Raw fast-path: emit each sample as a standalone `SumAccumulator`.
    pub fn process_samples_raw(
        &self,
        series_key: &str,
        samples: Vec<(i64, f64)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> =
            Vec::with_capacity(samples.len());

        for (ts, val) in samples {
            // Raw-mode path does not carry an `AggregationConfig` for
            // the source aggregation (synthetic agg_id, no source
            // config). After the PR-6 follow-up retired
            // `PrecomputedOutput.aggregation_id`, the sink's fallback
            // branch is gone — outputs carrying `PolicyFingerprint::UNSET`
            // are dropped at the sink with a warn. Raw-mode is
            // dev/test-only today (default `raw_mode_aggregation_id=0`),
            // so this path effectively writes nothing in production;
            // wiring raw mode to a real policy is a separate concern.
            let output =
                PrecomputedOutput::new(ts as u64, ts as u64, None, PolicyFingerprint::UNSET);
            let _ = self.raw_mode_aggregation_id;
            let accumulator = SumAccumulator::with_sum(val);
            emit_batch.push((output, Box::new(accumulator)));
        }

        if !emit_batch.is_empty() {
            debug!(
                "Worker {} raw-emitting {} samples for {}",
                self.id,
                emit_batch.len(),
                series_key
            );
            self.output_sink.emit_batch(emit_batch)?;
        }

        Ok(())
    }

    /// Flush all groups with cross-group watermark propagation.
    ///
    /// 1. Compute worker watermark = max(all group watermarks)
    /// 2. Publish it for cross-worker reads
    /// 3. Compute global watermark = min(all worker watermarks)
    /// 4. Advance idle groups to the global watermark, closing due windows
    ///
    /// Remove GroupStates whose source policy is no longer in the
    /// current config (i.e. the control plane removed the
    /// aggregation). Liveness is checked against each bucket's stored
    /// `policy_fp` — a sid stays alive only while its minting policy is
    /// still configured. Buckets with non-empty panes are kept until
    /// flush_all closes their windows; once both pane maps are empty,
    /// the GroupState shell is freed.
    fn evict_orphaned_groups(&mut self) {
        let snap = self.hot_reload.snapshot();
        let before = self.group_states.len();
        self.group_states.retain(|&sid, gs| {
            if snap.contains(gs.policy_fp.as_u64()) {
                return true; // policy still in config, keep
            }
            // Policy retired — keep only if there's residual data
            // that flush_all hasn't drained yet.
            let has_data = !gs.active_panes.is_empty() || !gs.sketch_panes.is_empty();
            if !has_data {
                debug!(
                    "evicting orphaned bucket (sid={}, policy_fp={})",
                    sid, gs.policy_fp
                );
            }
            has_data
        });
        let after = self.group_states.len();
        if before != after {
            info!(
                "Worker {} evicted {} orphaned buckets ({} → {})",
                self.id,
                before - after,
                before,
                after
            );
            self.group_count.store(after, Ordering::Relaxed);
        }
    }

    fn flush_all(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.pass_raw_samples {
            return Ok(());
        }

        let now_ms = (self.now_ms_fn)();
        let idle_grace_ms = self.wall_clock_idle_grace_period_ms;
        let max_open_grace_ms = self.wall_clock_max_open_grace_period_ms;

        // Publish the largest observed event-time watermark for diagnostics.
        // It must not be fed back into another group's closure decision: group
        // timestamps are independent unless an explicit source barrier says
        // otherwise.
        let worker_wm = self
            .group_states
            .values()
            .map(|s| watermark_for_event_time(s.max_event_time_ms, self.allowed_lateness_ms))
            .filter(|&wm| wm != i64::MIN)
            .max()
            .unwrap_or(i64::MIN);
        self.worker_watermark.store(worker_wm, Ordering::Release);

        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> = Vec::new();

        for (&sid, state) in &mut self.group_states {
            let _ = sid; // sid is the bucket key; group_key/policy_fp live on `state`
            if state.max_event_time_ms == i64::MIN {
                continue; // No samples received yet — no panes to close.
            }
            // group_key/policy_fp travelled in on the message and are
            // stored on `state` so the emit path can reach them without
            // re-keying the bucket. Clone so the body below can borrow
            // `state` mutably for pane drains.
            let group_key = state.group_key.clone();

            // Start from this group's existing closure watermark. A timer tick
            // alone must not advance event time.
            let mut effective_wm = state.closure_watermark_ms;

            // Wall-clock policy is independent of observed event time. Idle
            // closure handles completed finite input; the optional first-touch
            // deadline bounds freshness even for a continuously touched pane.
            if idle_grace_ms > 0 || max_open_grace_ms > 0 {
                let window_size_ms = state.window_manager.window_size_ms();
                for (&pane_start, &clock) in &state.pane_wall_clock {
                    let idle_due = idle_grace_ms > 0
                        && now_ms.saturating_sub(clock.last_touch_ms)
                            >= window_size_ms.saturating_add(idle_grace_ms);
                    // An absolute close can be followed by more input for the
                    // same event-time window. Enable it only when those inputs
                    // are emitted as mergeable corrections.
                    let deadline_due = self.late_data_policy == LateDataPolicy::ForwardToStore
                        && max_open_grace_ms > 0
                        && now_ms.saturating_sub(clock.first_touch_ms)
                            >= window_size_ms.saturating_add(max_open_grace_ms);
                    if idle_due || deadline_due {
                        let force_to = pane_start.saturating_add(window_size_ms);
                        if force_to > effective_wm {
                            effective_wm = force_to;
                        }
                    }
                }
            }

            let closed = state
                .window_manager
                .closed_windows(state.closure_watermark_ms, effective_wm);

            for window_start in &closed {
                let (_, window_end) = state.window_manager.window_bounds(*window_start);
                let pane_starts = state.window_manager.panes_for_window(*window_start);

                if let Some(accumulator) =
                    merge_panes_for_window(&mut state.active_panes, &pane_starts)
                {
                    let key = build_group_key_label_values(&group_key);
                    let output = PrecomputedOutput::new(
                        *window_start as u64,
                        window_end as u64,
                        Some(key),
                        PolicyFingerprint::from_config(&state.config),
                    );
                    emit_batch.push((output, accumulator));
                }

                if let Some(accumulator) =
                    merge_sketch_panes_for_window(&mut state.sketch_panes, &pane_starts)
                {
                    let key = build_group_key_label_values(&group_key);
                    let output = PrecomputedOutput::new(
                        *window_start as u64,
                        window_end as u64,
                        Some(key),
                        PolicyFingerprint::from_config(&state.config),
                    );
                    emit_batch.push((output, accumulator));
                }
            }

            if effective_wm > state.closure_watermark_ms {
                state.closure_watermark_ms = effective_wm;
            }

            state.prune_pane_wall_clock();
        }

        if !emit_batch.is_empty() {
            debug!(
                "Worker {} flush emitting {} outputs",
                self.id,
                emit_batch.len()
            );
            self.output_sink.emit_batch(emit_batch)?;
        }

        Ok(())
    }

    /// Force-close every window still open on shutdown.
    ///
    /// Unlike `flush_all` — which preserves observed event time and only applies
    /// configured wall-clock closure — this emits every remaining pane because
    /// no further samples will arrive once the engine is shutting down. Without
    /// it, a one-shot batch whose records all fall in a single window (so event-time
    /// never advances past the window end) would leave that window open forever
    /// and never write it to the store. Covers both `active_panes` (sample
    /// aggregation) and `sketch_panes` (OTLP-delivered sketches).
    ///
    /// To advance past the open windows we use a *finite* bound derived from
    /// the largest open pane (`max_pane + window_size_ms`) rather than
    /// `i64::MAX`: `WindowManager::closed_windows` enumerates window starts up
    /// to `current_wm` one slide at a time, so passing `i64::MAX` would loop
    /// ~`i64::MAX / slide` times and overflow. `max_pane + window_size_ms` is
    /// the smallest watermark that closes the latest open window.
    ///
    /// Idempotent: closed panes are drained from both pane maps and their
    /// wall-clock bookkeeping is pruned, so a second call emits nothing.
    fn force_close_all(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.pass_raw_samples {
            return Ok(());
        }

        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> = Vec::new();

        for (&sid, state) in &mut self.group_states {
            let _ = sid; // sid is the bucket key; group_key/policy_fp live on `state`
            if state.max_event_time_ms == i64::MIN {
                continue; // never received data — nothing to close
            }

            // The latest window start equals the largest open pane start across
            // both pane maps; closing `[start, start + size)` needs
            // `wm >= start + size`.
            let max_active = state.active_panes.keys().next_back().copied();
            let max_sketch = state.sketch_panes.keys().next_back().copied();
            let max_pane = match (max_active, max_sketch) {
                (Some(a), Some(b)) => a.max(b),
                (Some(a), None) => a,
                (None, Some(b)) => b,
                (None, None) => continue, // no open panes
            };
            let force_wm = max_pane.saturating_add(state.window_manager.window_size_ms());

            let group_key = state.group_key.clone();
            let closed = state
                .window_manager
                .closed_windows(state.closure_watermark_ms, force_wm);

            for window_start in &closed {
                let (_, window_end) = state.window_manager.window_bounds(*window_start);
                let pane_starts = state.window_manager.panes_for_window(*window_start);

                if let Some(accumulator) =
                    merge_panes_for_window(&mut state.active_panes, &pane_starts)
                {
                    let key = build_group_key_label_values(&group_key);
                    let output = PrecomputedOutput::new(
                        *window_start as u64,
                        window_end as u64,
                        Some(key),
                        PolicyFingerprint::from_config(&state.config),
                    );
                    emit_batch.push((output, accumulator));
                }

                if let Some(accumulator) =
                    merge_sketch_panes_for_window(&mut state.sketch_panes, &pane_starts)
                {
                    let key = build_group_key_label_values(&group_key);
                    let output = PrecomputedOutput::new(
                        *window_start as u64,
                        window_end as u64,
                        Some(key),
                        PolicyFingerprint::from_config(&state.config),
                    );
                    emit_batch.push((output, accumulator));
                }
            }

            if force_wm > state.closure_watermark_ms {
                state.closure_watermark_ms = force_wm;
            }
            state.prune_pane_wall_clock();
        }

        if !emit_batch.is_empty() {
            debug!(
                "Worker {} shutdown force-close emitting {} outputs",
                self.id,
                emit_batch.len()
            );
            self.output_sink.emit_batch(emit_batch)?;
        }

        Ok(())
    }
}

/// Build a `KeyByLabelValues` from a semicolon-delimited group key string.
/// e.g. "constant" → KeyByLabelValues { labels: ["constant"] }
/// e.g. "us-east;svc-a" → KeyByLabelValues { labels: ["us-east", "svc-a"] }
/// e.g. "" → KeyByLabelValues { labels: [""] }
/// Default wall-clock-now source: milliseconds since the unix epoch.
/// Used by `Worker::new`. Tests override via `set_now_ms_fn`.
fn default_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        // Pre-1970 wall clock (only happens if the host clock is
        // grossly misconfigured) — fall back to 0 so the fallback
        // simply doesn't trigger rather than panicking.
        .unwrap_or(0)
}

/// Convert observed event time to the watermark that is safe to close through.
/// A negative configured lateness is treated as zero rather than advancing the
/// watermark beyond any timestamp actually observed.
fn watermark_for_event_time(max_event_time_ms: i64, allowed_lateness_ms: i64) -> i64 {
    if max_event_time_ms == i64::MIN {
        i64::MIN
    } else {
        max_event_time_ms.saturating_sub(allowed_lateness_ms.max(0))
    }
}

fn build_group_key_label_values(group_key: &str) -> KeyByLabelValues {
    let labels: Vec<String> = group_key.split(';').map(|s| s.to_string()).collect();
    KeyByLabelValues::new_with_labels(labels)
}

/// Extract the metric name from a series key like `"metric_name{key1=\"val1\"}"`.
pub fn extract_metric_name(series_key: &str) -> &str {
    match series_key.find('{') {
        Some(pos) => &series_key[..pos],
        None => series_key,
    }
}

/// Extract grouping label values from a series key string based on the
/// aggregation config's `grouping_labels`.
///
/// The series key format is: `metric_name{label1="val1",label2="val2",...}`
pub fn extract_key_from_series(series_key: &str, config: &AggregationConfig) -> KeyByLabelValues {
    let labels = parse_labels_from_series_key(series_key);
    let mut values = Vec::new();

    for label_name in &config.grouping_labels.labels {
        if let Some(val) = labels.get(label_name.as_str()) {
            values.push(val.to_string());
        } else {
            values.push(String::new());
        }
    }

    KeyByLabelValues::new_with_labels(values)
}

/// Parse label key-value pairs from a series key string.
/// `"metric{a=\"b\",c=\"d\"}"` → `{("a", "b"), ("c", "d")}`
///
/// The returned `&str` value is the **raw, still-escaped** slice
/// between the opening and closing quote — e.g. for `k="a\"b"` the
/// value is the four bytes `a\"b`, not the decoded `a"b`. Call
/// [`decode_label_value`] if you need the decoded form. Most live
/// callers compare against literal config values that never contain
/// escapable characters (`"`, `\`, `\n`), so the un-decoded slice
/// suffices and saves an allocation per label per sample.
///
/// The closing-quote scan walks past `\\`, `\"`, `\n` escape pairs
/// emitted by [`format_series_key`] / `render_series_key`, so a
/// value containing embedded `"` no longer terminates parsing
/// prematurely (pre-fix bug — see PR following #284).
pub fn parse_labels_from_series_key(series_key: &str) -> HashMap<&str, &str> {
    let mut labels = HashMap::new();

    let start = match series_key.find('{') {
        Some(pos) => pos + 1,
        None => return labels,
    };
    let end = match series_key.rfind('}') {
        Some(pos) => pos,
        None => return labels,
    };

    if start >= end {
        return labels;
    }

    let label_str = &series_key[start..end];

    // Parse comma-separated key="value" pairs.
    let mut remaining = label_str;
    while !remaining.is_empty() {
        let eq_pos = match remaining.find('=') {
            Some(pos) => pos,
            None => break,
        };
        let key = remaining[..eq_pos].trim();

        let after_eq = &remaining[eq_pos + 1..];
        if !after_eq.starts_with('"') {
            break;
        }

        // Walk after the opening quote looking for the closing quote,
        // skipping over `\<x>` escape pairs so that values containing
        // embedded `"` (escaped as `\"`) don't terminate early. ASCII-
        // byte scan; safe because `\` and `"` are single-byte UTF-8
        // and never appear as continuation bytes inside a multi-byte
        // scalar — so byte indexing into a `&str` always lands on a
        // char boundary at the chosen positions.
        let value_start = 1; // skip opening quote
        let bytes = after_eq.as_bytes();
        let mut i = value_start;
        let value_end = loop {
            if i >= bytes.len() {
                // No closing quote — malformed input, abandon parse.
                return labels;
            }
            match bytes[i] {
                b'\\' if i + 1 < bytes.len() => {
                    // Skip the escape body byte (\", \\, \n, …).
                    i += 2;
                }
                b'"' => break i,
                _ => i += 1,
            }
        };

        let value = &after_eq[value_start..value_end];
        labels.insert(key, value);

        let consumed = value_end + 1;
        remaining = &after_eq[consumed..];
        if remaining.starts_with(',') {
            remaining = &remaining[1..];
        }
    }

    labels
}

/// Decode a `parse_labels_from_series_key` value slice into its
/// original textual form by undoing the `\"`, `\\`, `\n` escapes
/// emitted by `format_series_key` / `render_series_key`.
///
/// Returns a borrowed `Cow` when the slice has no `\` byte (the
/// common case — most label values are alphanumeric / dotted /
/// dashed), avoiding allocation. Only allocates when an escape is
/// present.
pub fn decode_label_value(s: &str) -> std::borrow::Cow<'_, str> {
    if !s.contains('\\') {
        return std::borrow::Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    // Walk by `char` boundaries so multi-byte UTF-8 scalars round-
    // trip intact. Escape recognition operates on ASCII metas (`\`,
    // `"`, `n`) which are always single-byte chars in UTF-8.
    let mut it = s.chars().peekable();
    while let Some(c) = it.next() {
        if c == '\\' {
            match it.next() {
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                Some('n') => out.push('\n'),
                Some(other) => {
                    // Unknown escape — pass the backslash + body
                    // through verbatim so we don't silently drop data.
                    out.push('\\');
                    out.push(other);
                }
                None => {
                    // Trailing backslash with no escape body — keep
                    // it so round-tripping is lossless even for
                    // malformed input.
                    out.push('\\');
                }
            }
        } else {
            out.push(c);
        }
    }
    std::borrow::Cow::Owned(out)
}

/// Route a single sample to `updater`, dispatching keyed vs. non-keyed based on config.
///
/// For keyed accumulators (MultipleSum, CMS, HydraKLL), the key is extracted
/// from the series' **aggregated_labels** — these are the labels that become
/// the key dimension *inside* the sketch (e.g., which bucket in a CMS, which
/// entry in a MultipleSumAccumulator's HashMap). This matches the Arroyo SQL
/// pattern: `udf(concat_ws(';', aggregated_labels), value)`.
pub(crate) fn apply_sample(
    updater: &mut dyn AccumulatorUpdater,
    series_key: &str,
    val: f64,
    ts: i64,
    config: &AggregationConfig,
) {
    if updater.is_keyed() {
        // Planner's PromQL Top-K item is the series identity. When no
        // explicit aggregated labels are projected, retain the canonical
        // series key instead of collapsing every series onto an empty item.
        let key = if config.aggregated_labels.labels.is_empty()
            && matches!(
                config.aggregation_type,
                crate::storage_engines::types::AggregationType::CountMinSketchWithHeap
                    | crate::storage_engines::types::AggregationType::CountSketchWithHeap
            ) {
            KeyByLabelValues::new_with_labels(vec![series_key.to_string()])
        } else {
            extract_aggregated_key_from_series(series_key, config)
        };
        updater.update_keyed(&key, val, ts);
    } else {
        updater.update_single(val, ts);
    }
}

/// Extract aggregated label values from a series key string.
/// These are the labels that form the key dimension *inside* keyed accumulators
/// (MultipleSum, CMS, HydraKLL), matching Arroyo's `agg_columns`.
fn extract_aggregated_key_from_series(
    series_key: &str,
    config: &AggregationConfig,
) -> KeyByLabelValues {
    let labels = parse_labels_from_series_key(series_key);
    let mut values = Vec::new();

    for label_name in &config.aggregated_labels.labels {
        if let Some(val) = labels.get(label_name.as_str()) {
            values.push(val.to_string());
        } else {
            values.push(String::new());
        }
    }

    KeyByLabelValues::new_with_labels(values)
}

/// Merge the pane accumulators that constitute a closed window.
///
/// The oldest pane (index 0) is taken destructively from `active_panes`
/// (no future window needs it). All later panes are snapshot-read
/// (non-destructive; they are shared by newer overlapping windows).
///
/// Returns `None` if all panes for the window are absent.
fn merge_panes_for_window(
    active_panes: &mut BTreeMap<i64, Box<dyn AccumulatorUpdater>>,
    pane_starts: &[i64],
) -> Option<Box<dyn AggregateCore>> {
    let mut merged: Option<Box<dyn AggregateCore>> = None;

    for (i, &ps) in pane_starts.iter().enumerate() {
        let pane_acc = if i == 0 {
            // Oldest pane: evict and MOVE the accumulator out (no clone).
            active_panes
                .remove(&ps)
                .map(|updater| updater.into_accumulator())
        } else {
            // Shared pane: non-destructive snapshot
            active_panes
                .get(&ps)
                .map(|updater| updater.snapshot_accumulator())
        };

        if let Some(acc) = pane_acc {
            merged = Some(match merged {
                None => acc,
                Some(existing) => existing.merge_with(acc.as_ref()).unwrap_or(existing),
            });
        }
    }

    merged
}

/// Merge pre-built accumulator panes for a window.
///
/// Equivalent to `merge_panes_for_window` but operating on the sketch pane
/// map (`Box<dyn AggregateCore>` directly). The oldest pane is destructively
/// taken (it will never be needed by a later window); subsequent panes are
/// cloned so that still-open overlapping windows can still read them.
fn merge_sketch_panes_for_window(
    sketch_panes: &mut BTreeMap<i64, Box<dyn AggregateCore>>,
    pane_starts: &[i64],
) -> Option<Box<dyn AggregateCore>> {
    let mut merged: Option<Box<dyn AggregateCore>> = None;

    for (i, &ps) in pane_starts.iter().enumerate() {
        let pane_acc: Option<Box<dyn AggregateCore>> = if i == 0 {
            // Oldest pane: destructive take + evict
            sketch_panes.remove(&ps)
        } else {
            // Shared pane: non-destructive clone
            sketch_panes.get(&ps).map(|acc| acc.clone_boxed_core())
        };

        if let Some(acc) = pane_acc {
            merged = Some(match merged {
                None => acc,
                Some(existing) => existing.merge_with(acc.as_ref()).unwrap_or(existing),
            });
        }
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    use flate2::{write::GzEncoder, Compression};
    use serde_json::json;
    use std::io::Write;

    #[test]
    fn test_extract_metric_name() {
        assert_eq!(
            extract_metric_name("http_requests_total{method=\"GET\"}"),
            "http_requests_total"
        );
        assert_eq!(extract_metric_name("up"), "up");
        assert_eq!(
            extract_metric_name("cpu_usage{host=\"a\",zone=\"us\"}"),
            "cpu_usage"
        );
    }

    #[test]
    fn test_parse_labels() {
        let labels = parse_labels_from_series_key("metric{method=\"GET\",status=\"200\"}");
        assert_eq!(labels.get("method"), Some(&"GET"));
        assert_eq!(labels.get("status"), Some(&"200"));
    }

    #[test]
    fn test_parse_labels_no_labels() {
        let labels = parse_labels_from_series_key("metric");
        assert!(labels.is_empty());
    }

    #[test]
    fn test_parse_labels_empty_braces() {
        let labels = parse_labels_from_series_key("metric{}");
        assert!(labels.is_empty());
    }

    #[test]
    fn test_parse_labels_skips_escaped_closing_quote() {
        // The closing-quote scan must walk past `\"` rather than
        // terminating the value early. Regression for the
        // `format_series_key` ↔ `parse_labels_from_series_key`
        // roundtrip bug — see PR #284's discovery and the
        // `series_key_roundtrip_tests` module in
        // `drivers/ingest/otel.rs`.
        let labels = parse_labels_from_series_key(r#"metric{msg="a\"b",svc="x"}"#);
        // Raw (un-decoded) values are returned; `decode_label_value`
        // un-escapes them.
        assert_eq!(labels.get("msg"), Some(&r#"a\"b"#));
        assert_eq!(labels.get("svc"), Some(&"x"));
    }

    #[test]
    fn test_decode_label_value_unescapes_known_pairs() {
        assert_eq!(decode_label_value("plain"), "plain");
        assert_eq!(decode_label_value(r#"a\"b"#), r#"a"b"#);
        assert_eq!(decode_label_value(r"a\\b"), r"a\b");
        assert_eq!(decode_label_value(r"line1\nline2"), "line1\nline2");
        // Unknown escapes pass through unchanged so we don't silently
        // drop producer-side data.
        assert_eq!(decode_label_value(r"a\xb"), r"a\xb");
    }

    #[test]
    fn test_decode_label_value_borrows_when_no_escapes() {
        // Borrowed for the common case — no allocation.
        let s = "no_escapes_here";
        match decode_label_value(s) {
            std::borrow::Cow::Borrowed(b) => assert_eq!(b, s),
            std::borrow::Cow::Owned(_) => panic!("expected borrowed, no `\\` in input"),
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    use crate::precompute_engine::config::LateDataPolicy;
    use crate::precompute_engine::operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;
    use crate::precompute_engine::operators::multiple_sum_accumulator::MultipleSumAccumulator;
    use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
    use crate::precompute_engine::output_sink::CapturingOutputSink;
    use crate::storage_engines::types::StreamingConfig;
    use asap_sketchlib::KllSketch;
    use asap_types::enums::WindowKind;
    use asap_types::AggregationType;

    fn make_agg_config(
        id: u64,
        metric: &str,
        agg_type: AggregationType,
        agg_sub_type: &str,
        window_secs: u64,
        slide_secs: u64,
        grouping: Vec<&str>,
    ) -> AggregationConfig {
        make_agg_config_full(
            id,
            metric,
            agg_type,
            agg_sub_type,
            window_secs,
            slide_secs,
            grouping,
            vec![],
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn make_agg_config_full(
        _id: u64,
        metric: &str,
        agg_type: AggregationType,
        agg_sub_type: &str,
        window_secs: u64,
        slide_secs: u64,
        grouping: Vec<&str>,
        aggregated: Vec<&str>,
    ) -> AggregationConfig {
        // `_id` is unused after PR 5 — identity is content-addressed
        // via `PolicyFingerprint::from_config`. Callers below build the
        // streaming-config map by reading `config.policy_fp_u64()`
        // from the returned value.
        let window_type = if slide_secs == 0 || slide_secs == window_secs {
            WindowKind::Tumbling
        } else {
            WindowKind::Sliding
        };
        AggregationConfig::new(
            agg_type,
            agg_sub_type.to_string(),
            HashMap::new(),
            asap_types::KeyByLabelNames::new(grouping.iter().map(|s| s.to_string()).collect()),
            asap_types::KeyByLabelNames::new(aggregated.iter().map(|s| s.to_string()).collect()),
            asap_types::KeyByLabelNames::new(vec![]),
            String::new(),
            window_secs,
            slide_secs,
            window_type,
            metric.to_string(),
            metric.to_string(),
            None,
            None,
            None,
        )
    }

    fn make_worker(
        agg_configs: HashMap<u64, AggregationConfig>,
        sink: Arc<CapturingOutputSink>,
        pass_raw: bool,
        raw_agg_id: u64,
        late_policy: LateDataPolicy,
    ) -> Worker {
        make_worker_with_lateness(agg_configs, sink, pass_raw, raw_agg_id, late_policy, 0)
    }

    fn make_worker_with_lateness(
        agg_configs: HashMap<u64, AggregationConfig>,
        sink: Arc<CapturingOutputSink>,
        pass_raw: bool,
        raw_agg_id: u64,
        late_policy: LateDataPolicy,
        allowed_lateness_ms: i64,
    ) -> Worker {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        Worker::new(
            0,
            rx,
            sink,
            make_hot_reload(agg_configs),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms,
                pass_raw_samples: pass_raw,
                raw_mode_aggregation_id: raw_agg_id,
                late_data_policy: late_policy,
                wall_clock_idle_grace_period_ms: 0,
                wall_clock_max_open_grace_period_ms: 0,
            },
            Arc::new(AtomicUsize::new(0)),
            wm,
        )
    }

    /// Build a fresh `HotReloadStreamingConfig` from a map of agg_id
    /// → AggregationConfig. Worker::new takes this handle instead of
    /// the old `HashMap<u64, Arc<AggregationConfig>>`. Tests use this
    /// helper instead of constructing the handle inline at every
    /// callsite.
    fn make_hot_reload(
        configs: HashMap<u64, AggregationConfig>,
    ) -> crate::storage_engines::types::HotReloadStreamingConfig {
        crate::storage_engines::types::HotReloadStreamingConfig::new(
            crate::storage_engines::types::StreamingConfig::new(configs),
        )
    }

    /// Helper to make GroupSamples from simple (ts, val) pairs for a single series.
    fn group_samples(series_key: &str, samples: Vec<(i64, f64)>) -> Vec<(String, i64, f64)> {
        samples
            .into_iter()
            .map(|(ts, val)| (series_key.to_string(), ts, val))
            .collect()
    }

    // -----------------------------------------------------------------------
    // Test: raw mode — each sample forwarded as SumAccumulator with sum==value
    // -----------------------------------------------------------------------

    #[test]
    fn test_raw_mode_forwarding() {
        let sink = Arc::new(CapturingOutputSink::new());
        let worker = make_worker(HashMap::new(), sink.clone(), true, 99, LateDataPolicy::Drop);

        let samples = vec![(1000_i64, 1.5_f64), (2000, 2.5), (3000, 7.0)];
        worker
            .process_samples_raw("cpu{host=\"a\"}", samples.clone())
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 3, "should emit one output per raw sample");

        for ((ts, val), (output, acc)) in samples.iter().zip(captured.iter()) {
            assert_eq!(output.start_timestamp as i64, *ts);
            assert_eq!(output.end_timestamp as i64, *ts);
            // Raw mode emits PolicyFingerprint::UNSET (no source
            // AggregationConfig in the raw-mode fast path). The sink
            // drops UNSET outputs with a warn — verified separately
            // via integration tests.
            assert!(output.policy_fp.is_unset());
            let sum_acc = acc
                .as_any()
                .downcast_ref::<SumAccumulator>()
                .expect("should be SumAccumulator");
            assert!(
                (sum_acc.sum - val).abs() < 1e-10,
                "sum should equal sample value"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test: tumbling window — correct window boundaries and sum
    // -----------------------------------------------------------------------

    #[test]
    fn test_tumbling_window_correctness() {
        // 10s tumbling window
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(1, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Samples in window [0, 10000ms): sum should be 1+2+3=6.
        // All go to the same bucket (sid=1, group_key="")
        let pf = PolicyFingerprint(1);
        worker
            .process_group_samples(1, pf, "", group_samples("cpu", vec![(1000, 1.0)]))
            .unwrap();
        worker
            .process_group_samples(1, pf, "", group_samples("cpu", vec![(5000, 2.0)]))
            .unwrap();
        worker
            .process_group_samples(1, pf, "", group_samples("cpu", vec![(9000, 3.0)]))
            .unwrap();
        assert_eq!(sink.len(), 0);

        // Sample at t=10000ms closes [0, 10000)
        worker
            .process_group_samples(1, pf, "", group_samples("cpu", vec![(10000, 100.0)]))
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "exactly one window should close");

        let (output, acc) = &captured[0];
        // PR-6 follow-up: `aggregation_id` field is gone; the worker
        // now emits the config's policy fingerprint. Non-UNSET asserts
        // the emit path threaded the source config through.
        assert!(!output.policy_fp.is_unset());
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);

        let sum_acc = acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("should be SumAccumulator");
        assert!(
            (sum_acc.sum - 6.0).abs() < 1e-10,
            "sum should be 1+2+3=6, got {}",
            sum_acc.sum
        );
    }

    // -----------------------------------------------------------------------
    // Test: GROUP BY — multiple series merged into same group accumulator
    // -----------------------------------------------------------------------

    #[test]
    fn test_group_by_merges_series() {
        // SingleSubpopulation Sum with no grouping labels
        // Two different series in the same group → both feed same accumulator
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(1, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Two different series, same bucket (sid=1, group_key="")
        // Both feed into the same accumulator
        let pf = PolicyFingerprint(1);
        worker
            .process_group_samples(
                1,
                pf,
                "",
                vec![
                    ("cpu{host=\"A\"}".to_string(), 1000, 10.0),
                    ("cpu{host=\"B\"}".to_string(), 2000, 20.0),
                ],
            )
            .unwrap();
        assert_eq!(sink.len(), 0);

        // Close the window
        worker
            .process_group_samples(
                1,
                pf,
                "",
                group_samples("cpu{host=\"A\"}", vec![(10000, 0.0)]),
            )
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "one output per group per window");

        let (output, acc) = &captured[0];
        // PR-6 follow-up: `aggregation_id` field is gone; the worker
        // now emits the config's policy fingerprint. Non-UNSET asserts
        // the emit path threaded the source config through.
        assert!(!output.policy_fp.is_unset());
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);

        let sum_acc = acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("should be SumAccumulator");
        assert!(
            (sum_acc.sum - 30.0).abs() < 1e-10,
            "sum should be 10+20=30, got {} (both series merged)",
            sum_acc.sum
        );
    }

    // -----------------------------------------------------------------------
    // Test: GROUP BY with grouping labels — different groups produce separate outputs
    // -----------------------------------------------------------------------

    #[test]
    fn test_different_groups_separate_outputs() {
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec!["pattern"],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(1, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Two distinct group_keys → two distinct sids (sid IS the bucket
        // identity; the legacy `(agg_id, group_key)` tuple folds in).
        let pf = PolicyFingerprint(1);
        let sid_constant = 11_u64;
        let sid_sine = 12_u64;
        // Bucket sid_constant gets samples
        worker
            .process_group_samples(
                sid_constant,
                pf,
                "constant",
                group_samples("cpu{pattern=\"constant\"}", vec![(1000, 5.0)]),
            )
            .unwrap();
        // Bucket sid_sine gets samples
        worker
            .process_group_samples(
                sid_sine,
                pf,
                "sine",
                group_samples("cpu{pattern=\"sine\"}", vec![(2000, 7.0)]),
            )
            .unwrap();

        // Close both buckets' windows
        worker
            .process_group_samples(
                sid_constant,
                pf,
                "constant",
                group_samples("cpu{pattern=\"constant\"}", vec![(10000, 0.0)]),
            )
            .unwrap();
        worker
            .process_group_samples(
                sid_sine,
                pf,
                "sine",
                group_samples("cpu{pattern=\"sine\"}", vec![(10000, 0.0)]),
            )
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 2, "two groups → two outputs");

        let mut sums_by_key: HashMap<String, f64> = HashMap::new();
        for (output, acc) in &captured {
            let sum_acc = acc.as_any().downcast_ref::<SumAccumulator>().unwrap();
            let key = output.key.as_ref().unwrap().labels.join(";");
            sums_by_key.insert(key, sum_acc.sum);
        }
        assert!((sums_by_key["constant"] - 5.0).abs() < 1e-10);
        assert!((sums_by_key["sine"] - 7.0).abs() < 1e-10);
    }

    // -----------------------------------------------------------------------
    // Test: KLL GROUP BY — multiple series merged into one KLL sketch per group
    // -----------------------------------------------------------------------

    #[test]
    fn test_kll_group_by_merges_series() {
        let mut config = make_agg_config(
            1,
            "latency",
            AggregationType::DatasketchesKLL,
            "",
            10,
            0,
            vec!["pattern"],
        );
        config
            .parameters
            .insert("K".to_string(), serde_json::Value::from(20_u64));
        let mut agg_configs = HashMap::new();
        agg_configs.insert(1, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Three different series all in group "constant" — all feed one KLL
        let pf = PolicyFingerprint(1);
        worker
            .process_group_samples(
                1,
                pf,
                "constant",
                vec![
                    (
                        "latency{pattern=\"constant\",host=\"a\"}".to_string(),
                        1000,
                        10.0,
                    ),
                    (
                        "latency{pattern=\"constant\",host=\"b\"}".to_string(),
                        2000,
                        20.0,
                    ),
                    (
                        "latency{pattern=\"constant\",host=\"c\"}".to_string(),
                        3000,
                        30.0,
                    ),
                ],
            )
            .unwrap();

        // Close the window
        worker
            .process_group_samples(
                1,
                pf,
                "constant",
                group_samples(
                    "latency{pattern=\"constant\",host=\"a\"}",
                    vec![(10000, 0.0)],
                ),
            )
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "one KLL output for the whole group");

        let (output, acc) = &captured[0];
        // PR-6 follow-up: `aggregation_id` field is gone; the worker
        // now emits the config's policy fingerprint. Non-UNSET asserts
        // the emit path threaded the source config through.
        assert!(!output.policy_fp.is_unset());
        let kll = acc
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .expect("should be KLL");
        assert_eq!(
            kll.inner.count(),
            3,
            "KLL should contain all 3 series' samples"
        );
    }

    // -----------------------------------------------------------------------
    // Test: sliding window pane sharing
    // -----------------------------------------------------------------------

    #[test]
    fn test_sliding_window_pane_sharing() {
        // 30s window, 10s slide → W=3 panes per window
        let config = make_agg_config(
            2,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            30,
            10,
            vec![],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(2, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Sample at t=15000ms → goes to pane 10000ms
        let pf = PolicyFingerprint(2);
        worker
            .process_group_samples(2, pf, "", group_samples("cpu", vec![(15_000, 42.0)]))
            .unwrap();
        assert_eq!(sink.len(), 0);

        // Sample at t=45000ms → advances watermark to 45000ms
        // Closes windows [0, 30000) and [10000, 40000)
        worker
            .process_group_samples(2, pf, "", group_samples("cpu", vec![(45_000, 0.0)]))
            .unwrap();

        let captured = sink.drain();
        assert_eq!(
            captured.len(),
            2,
            "two windows containing the pane should emit"
        );

        let window_starts: Vec<u64> = captured.iter().map(|(o, _)| o.start_timestamp).collect();
        assert!(window_starts.contains(&0));
        assert!(window_starts.contains(&10_000));

        for (_output, acc) in &captured {
            let sum_acc = acc
                .as_any()
                .downcast_ref::<SumAccumulator>()
                .expect("should be SumAccumulator");
            assert!(
                (sum_acc.sum - 42.0).abs() < 1e-10,
                "window should have sum=42 via pane sharing, got {}",
                sum_acc.sum
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test: MultipleSubpopulation — keyed accumulator with aggregated labels
    // Matches planner output: grouping=[], aggregated=[host]
    // All series go to one group, host is the key dimension INSIDE the sketch
    // -----------------------------------------------------------------------

    #[test]
    fn test_keyed_accumulator_aggregated_labels() {
        // Like planner output for `sum by (host) (cpu)`:
        // grouping=[] (empty), aggregated=[host] (key inside MultipleSumAccumulator)
        let config = make_agg_config_full(
            3,
            "cpu",
            AggregationType::MultipleSubpopulation,
            "Sum",
            10,
            0,
            vec![],       // grouping: empty — one output group
            vec!["host"], // aggregated: host is the key INSIDE the sketch
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(3, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Both series go to the SAME bucket (group_key="" since grouping is empty).
        // The host label is extracted as the aggregated key inside the accumulator.
        let pf = PolicyFingerprint(3);
        worker
            .process_group_samples(
                3,
                pf,
                "",
                vec![
                    ("cpu{host=\"A\"}".to_string(), 1000, 10.0),
                    ("cpu{host=\"B\"}".to_string(), 2000, 20.0),
                ],
            )
            .unwrap();

        // Close the single bucket's window
        worker
            .process_group_samples(
                3,
                pf,
                "",
                group_samples("cpu{host=\"A\"}", vec![(10000, 0.0)]),
            )
            .unwrap();

        let captured = sink.drain();
        assert_eq!(
            captured.len(),
            1,
            "one group → one output (both hosts inside)"
        );

        let (_output, acc) = &captured[0];
        let ms_acc = acc
            .as_any()
            .downcast_ref::<MultipleSumAccumulator>()
            .expect("should be MultipleSumAccumulator");

        // The MultipleSumAccumulator should have two internal keys: "A" and "B"
        assert_eq!(ms_acc.sums.len(), 2, "two host keys inside one accumulator");

        let mut found_a = false;
        let mut found_b = false;
        for (key, &sum) in &ms_acc.sums {
            if key.labels == vec!["A".to_string()] {
                assert!((sum - 10.0).abs() < 1e-10);
                found_a = true;
            }
            if key.labels == vec!["B".to_string()] {
                assert!((sum - 20.0).abs() < 1e-10);
                found_b = true;
            }
        }
        assert!(found_a, "expected key A inside accumulator");
        assert!(found_b, "expected key B inside accumulator");
    }

    #[test]
    fn test_late_data_drop() {
        let config = make_agg_config(
            4,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(4, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        let mut worker = Worker::new(
            0,
            rx,
            sink.clone(),
            make_hot_reload(agg_configs),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 0,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy: LateDataPolicy::Drop,
                wall_clock_idle_grace_period_ms: 0,
                wall_clock_max_open_grace_period_ms: 0,
            },
            Arc::new(AtomicUsize::new(0)),
            wm,
        );

        // Establish watermark at t=20000ms
        let pf = PolicyFingerprint(4);
        worker
            .process_group_samples(4, pf, "", group_samples("cpu", vec![(20_000, 1.0)]))
            .unwrap();
        let _ = sink.drain();

        // Send a late sample
        worker
            .process_group_samples(4, pf, "", group_samples("cpu", vec![(5_000, 99.0)]))
            .unwrap();

        assert_eq!(sink.len(), 0, "late sample should be dropped");
    }

    // -----------------------------------------------------------------------
    // Test: late data ForwardToStore
    // -----------------------------------------------------------------------

    #[test]
    fn test_late_data_forward_to_store() {
        let config = make_agg_config(
            5,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(5, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        let mut worker = Worker::new(
            0,
            rx,
            sink.clone(),
            make_hot_reload(agg_configs),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 15_000,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy: LateDataPolicy::ForwardToStore,
                wall_clock_idle_grace_period_ms: 0,
                wall_clock_max_open_grace_period_ms: 0,
            },
            Arc::new(AtomicUsize::new(0)),
            wm,
        );

        // Seed then advance max event time far enough that the 15s lateness
        // budget permits closing [0, 10s).
        let pf = PolicyFingerprint(5);
        worker
            .process_group_samples(5, pf, "", group_samples("cpu", vec![(500, 1.0)]))
            .unwrap();
        worker
            .process_group_samples(5, pf, "", group_samples("cpu", vec![(30_000, 0.0)]))
            .unwrap();
        let _ = sink.drain();

        // Send late sample for evicted pane
        worker
            .process_group_samples(5, pf, "", group_samples("cpu", vec![(8_000, 55.0)]))
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "ForwardToStore should emit");

        let (output, acc) = &captured[0];
        assert!(!output.policy_fp.is_unset());
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);

        let sum_acc = acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("should be SumAccumulator");
        assert!(
            (sum_acc.sum - 55.0).abs() < 1e-10,
            "late sample sum should be 55.0, got {}",
            sum_acc.sum
        );
    }

    // -----------------------------------------------------------------------
    // Test: worker from streaming_config YAML
    // -----------------------------------------------------------------------

    #[test]
    fn test_worker_from_streaming_config_yaml() {
        let yaml = r#"
aggregations:
- aggregationType: SingleSubpopulation
  aggregationSubType: Sum
  labels:
    grouping: []
    rollup: []
    aggregated: []
  metric: requests_total
  parameters: {}
  tumblingWindowSize: 10
  windowSize: 10
  windowType: tumbling
  slideInterval: 0
  spatialFilter: ''
"#;

        let data: serde_yaml::Value = serde_yaml::from_str(yaml).expect("valid YAML");
        let streaming_config =
            StreamingConfig::from_yaml_data(&data).expect("valid streaming config");

        // PR 5: the streaming-config key is the policy fingerprint.
        let agg_id = *streaming_config
            .get_all_aggregation_configs()
            .keys()
            .next()
            .expect("one agg");
        assert!(streaming_config.contains(agg_id));

        let agg_configs = streaming_config.get_all_aggregation_configs().clone();
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        let pf = PolicyFingerprint(agg_id);
        let sid = 1_u64;
        worker
            .process_group_samples(
                sid,
                pf,
                "",
                group_samples("requests_total", vec![(1_000, 3.0)]),
            )
            .unwrap();
        worker
            .process_group_samples(
                sid,
                pf,
                "",
                group_samples("requests_total", vec![(5_000, 4.0)]),
            )
            .unwrap();
        worker
            .process_group_samples(
                sid,
                pf,
                "",
                group_samples("requests_total", vec![(9_000, 5.0)]),
            )
            .unwrap();
        assert_eq!(sink.len(), 0);

        worker
            .process_group_samples(
                sid,
                pf,
                "",
                group_samples("requests_total", vec![(10_000, 0.0)]),
            )
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1);

        let (output, acc) = &captured[0];
        let _ = agg_id;
        assert!(!output.policy_fp.is_unset());
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);

        let sum_acc = acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("should be SumAccumulator");
        assert!(
            (sum_acc.sum - 12.0).abs() < 1e-10,
            "sum should be 3+4+5=12, got {}",
            sum_acc.sum
        );
    }

    #[test]
    fn test_extract_key_from_series() {
        let config = AggregationConfig::new(
            AggregationType::SingleSubpopulation,
            "Sum".to_string(),
            HashMap::new(),
            asap_types::KeyByLabelNames::new(vec!["method".to_string(), "status".to_string()]),
            asap_types::KeyByLabelNames::new(vec![]),
            asap_types::KeyByLabelNames::new(vec![]),
            String::new(),
            60,
            0,
            WindowKind::Tumbling,
            "http_requests_total".to_string(),
            "http_requests_total".to_string(),
            Some(60),
            None,
            None,
        );

        let key = extract_key_from_series(
            "http_requests_total{method=\"GET\",status=\"200\"}",
            &config,
        );
        assert_eq!(key.labels, vec!["GET".to_string(), "200".to_string()]);
    }

    #[test]
    fn test_build_group_key_label_values() {
        let key = build_group_key_label_values("constant");
        assert_eq!(key.labels, vec!["constant".to_string()]);

        let key = build_group_key_label_values("us-east;svc-a");
        assert_eq!(key.labels, vec!["us-east".to_string(), "svc-a".to_string()]);

        let key = build_group_key_label_values("");
        assert_eq!(key.labels, vec!["".to_string()]);
    }

    // -----------------------------------------------------------------------
    // Tests: cross-group watermark propagation
    // -----------------------------------------------------------------------

    #[test]
    fn test_group_watermarks_are_isolated() {
        // Two groups on the same worker. Group A advances to t=100s while
        // group B remains active at t=5s. Group A is not evidence that group
        // B's source has completed its earlier window.
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let agg_configs = HashMap::from([(1, config)]);
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Two distinct group_keys ("groupA" / "groupB") under the same
        // policy → two distinct sids (each sid is one bucket; the legacy
        // `(agg_id, group_key)` tuple folded into a single u64).
        let pf = PolicyFingerprint(1);
        let sid_a = 21_u64;
        let sid_b = 22_u64;
        // Group A: send sample at t=5s (within window [0, 10s))
        worker
            .process_group_samples(
                sid_a,
                pf,
                "groupA",
                group_samples("cpu", vec![(5_000, 1.0)]),
            )
            .unwrap();
        // Group B: send sample at t=5s (within window [0, 10s))
        worker
            .process_group_samples(
                sid_b,
                pf,
                "groupB",
                group_samples("cpu", vec![(5_000, 2.0)]),
            )
            .unwrap();
        let _ = sink.drain();

        // Advance group A's watermark to t=100s (closes many windows).
        worker
            .process_group_samples(
                sid_a,
                pf,
                "groupA",
                group_samples("cpu", vec![(100_000, 3.0)]),
            )
            .unwrap();
        let _ = sink.drain();

        // Group B has NOT received new data — its event-time watermark is
        // still at 5s. Flushing must not borrow group A's timestamp.
        worker.flush_all().unwrap();
        let flushed = sink.drain();

        let group_b_outputs: Vec<_> = flushed
            .iter()
            .filter(|(out, _)| {
                out.key
                    .as_ref()
                    .map(|k| k.labels == vec!["groupB".to_string()])
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            group_b_outputs.is_empty(),
            "one group must not force-close another group's event-time window"
        );

        worker
            .process_group_samples(
                sid_b,
                pf,
                "groupB",
                group_samples("cpu", vec![(5_000, 4.0)]),
            )
            .unwrap();
        worker.force_close_all().unwrap();
        let group_b = sink
            .drain()
            .into_iter()
            .find(|(out, _)| {
                out.key
                    .as_ref()
                    .map(|k| k.labels == vec!["groupB".to_string()])
                    .unwrap_or(false)
            })
            .expect("group B should emit on shutdown");
        let sum = group_b
            .1
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("must emit SumAccumulator");
        assert_eq!(sum.sum, 6.0, "group B's second sample must not be late");
    }

    #[test]
    fn repeated_flushes_do_not_make_fixed_timestamp_input_late() {
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            1,
            0,
            vec![],
        );
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker_with_lateness(
            HashMap::from([(1, config)]),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
            1,
        );
        let pf = PolicyFingerprint(1);

        worker
            .process_group_samples(1, pf, "", group_samples("cpu", vec![(0, 1.0)]))
            .unwrap();
        worker.flush_all().unwrap();
        worker.flush_all().unwrap();
        worker
            .process_group_samples(1, pf, "", group_samples("cpu", vec![(0, 2.0)]))
            .unwrap();
        worker.force_close_all().unwrap();

        let emitted = sink.drain();
        assert_eq!(emitted.len(), 1);
        let sum = emitted[0]
            .1
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("must emit SumAccumulator");
        assert_eq!(sum.sum, 3.0, "flush must not manufacture event time");
    }

    #[test]
    fn allowed_lateness_delays_event_time_window_close() {
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker_with_lateness(
            HashMap::from([(1, config)]),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
            5_000,
        );
        let pf = PolicyFingerprint(1);

        worker
            .process_group_samples(1, pf, "", group_samples("cpu", vec![(5_000, 1.0)]))
            .unwrap();
        worker
            .process_group_samples(1, pf, "", group_samples("cpu", vec![(10_000, 2.0)]))
            .unwrap();
        assert_eq!(
            sink.len(),
            0,
            "window [0, 10s) remains open until max event time reaches 15s"
        );

        worker
            .process_group_samples(1, pf, "", group_samples("cpu", vec![(15_000, 3.0)]))
            .unwrap();
        let emitted = sink.drain();
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].0.start_timestamp, 0);
        assert_eq!(emitted[0].0.end_timestamp, 10_000);
    }

    #[test]
    fn first_catch_up_batch_closes_every_complete_window() {
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            5,
            0,
            vec![],
        );
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker_with_lateness(
            HashMap::from([(1, config)]),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
            0,
        );

        worker
            .process_group_samples(
                1,
                PolicyFingerprint(1),
                "",
                group_samples(
                    "cpu",
                    vec![
                        (500, 1.0),
                        (4_200, 2.0),
                        (5_400, 3.0),
                        (9_400, 4.0),
                        (10_500, 5.0),
                    ],
                ),
            )
            .unwrap();

        let emitted = sink.drain();
        assert_eq!(emitted.len(), 2);
        assert_eq!(emitted[0].0.start_timestamp, 0);
        assert_eq!(emitted[0].0.end_timestamp, 5_000);
        assert_eq!(emitted[1].0.start_timestamp, 5_000);
        assert_eq!(emitted[1].0.end_timestamp, 10_000);
    }

    #[test]
    fn test_flush_publishes_worker_watermark() {
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let agg_configs = HashMap::from([(1, config)]);
        let sink = Arc::new(CapturingOutputSink::new());
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut worker = Worker::new(
            0,
            rx,
            sink,
            make_hot_reload(agg_configs),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 0,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy: LateDataPolicy::Drop,
                wall_clock_idle_grace_period_ms: 0,
                wall_clock_max_open_grace_period_ms: 0,
            },
            Arc::new(AtomicUsize::new(0)),
            wm.clone(),
        );

        assert_eq!(wm.load(Ordering::Acquire), i64::MIN);

        // Send data at t=50s
        let pf = PolicyFingerprint(1);
        worker
            .process_group_samples(1, pf, "", group_samples("cpu", vec![(50_000, 1.0)]))
            .unwrap();

        // Flush should publish worker watermark
        worker.flush_all().unwrap();
        assert_eq!(
            wm.load(Ordering::Acquire),
            50_000,
            "worker watermark should be published after flush"
        );
    }

    // -----------------------------------------------------------------------
    // Sweep blocker #2: ASAP-tier persistence path for sketch ingest.
    //
    // Pre-fix the OTLP sketch path produced `worker_process_accumulator`
    // log lines but never persisted into the per_key store, so PromQL
    // queries returned `Metric not found`. The tests below pin the
    // `process_accumulator_input` → window-close → emit_batch contract
    // so a future refactor can't regress it without tripping a unit
    // test. They are deliberately written in terms of the public worker
    // API + a real `DDSketchAccumulator`, exactly mirroring what the
    // OTLP ingest dispatch builds via `decode_modified_otlp_sketch_bytes`.
    // -----------------------------------------------------------------------

    use crate::precompute_engine::operators::DDSketchAccumulator;
    use asap_sketchlib::DdSketch;

    /// Build a fresh DDSketch holding `vals` so each test has a real,
    /// non-empty sketch to push through `process_accumulator_input`.
    fn make_ddsketch(alpha: f64, vals: &[f64]) -> DDSketchAccumulator {
        let mut s = DdSketch::new(alpha);
        for v in vals {
            // DDSketch only ingests positive values; the agent's
            // `_quantile` suffix metric carries latencies, so
            // positive-only is the realistic shape.
            s.update(*v);
        }
        DDSketchAccumulator {
            inner: s,
            sample_p: 1.0,
        }
    }

    /// Pinning test: a single-group, single-window sketch ingest must
    /// emit *exactly one* persisted output once the watermark advances
    /// past the window boundary. This is the unit-level reproducer of
    /// the sweep blocker — pre-fix the path between
    /// `worker_process_accumulator` and the per_key store insert was
    /// silent, so this test would never see an emit.
    #[test]
    fn test_process_accumulator_input_persists_after_window_close() {
        // 30s tumbling window — matches `backend-streaming.yaml`
        // for `http_requests_total_latency_ms_quantile`.
        let cfg = make_agg_config(
            1,
            "http_requests_total_latency_ms_quantile",
            AggregationType::DDSketch,
            "",
            30,
            0,
            vec!["zone"],
        );
        let agg_configs = HashMap::from([(1, cfg)]);
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // First batch: 10 sketches at t=60_000 ms, all under the same
        // bucket (group_key="us-east") — mirrors the agent emitting one
        // sketch per (zone,rack,node,pod) tuple while the backend rolls
        // them up by zone.
        let pf = PolicyFingerprint(1);
        let sid = 31_u64;
        for i in 0..10 {
            let s = make_ddsketch(0.01, &[1.0 + i as f64, 2.0, 3.0]);
            worker
                .process_accumulator_input(sid, pf, "us-east", 60_000, Box::new(s))
                .expect("first batch must process");
        }
        assert_eq!(
            sink.len(),
            0,
            "first batch alone does not close any window — watermark is still at first-sample time"
        );

        // Second batch at t=120_000 ms (60s later). Watermark advances
        // 60_000 → 120_000, and `closed_windows` must return [60_000, 90_000)
        // (a 30s window). The pane at 60_000 holds the merged sketch from
        // batch 1, so `merge_sketch_panes_for_window` returns Some(...)
        // and the output is emitted.
        let s2 = make_ddsketch(0.01, &[5.0, 6.0]);
        worker
            .process_accumulator_input(sid, pf, "us-east", 120_000, Box::new(s2))
            .expect("second batch must process");

        let captured = sink.drain();
        assert!(
            !captured.is_empty(),
            "ASAP-tier sketch persistence regressed: window close did not emit any output. \
             pre-fix this is exactly the symptom the sweep agent saw — \
             `worker_process_accumulator` fires but per_key store stays empty."
        );
        // The emitted output's window must be [60_000, 90_000) — the
        // 30s tumbling window that contained the first batch.
        let (output, acc) = &captured[0];
        // PR-6 follow-up: `aggregation_id` field is gone; the worker
        // now emits the config's policy fingerprint. Non-UNSET asserts
        // the emit path threaded the source config through.
        assert!(!output.policy_fp.is_unset());
        assert_eq!(output.start_timestamp, 60_000);
        assert_eq!(output.end_timestamp, 90_000);
        assert_eq!(
            acc.type_name(),
            "DDSketchAccumulator",
            "persisted accumulator must round-trip as DDSketchAccumulator (not silently demoted)"
        );

        // The merged sketch must contain all 10 first-batch sketches.
        // Each sketch added 3 values, so total count = 10 * 3 = 30.
        let dd = acc
            .as_any()
            .downcast_ref::<DDSketchAccumulator>()
            .expect("must downcast back to DDSketchAccumulator");
        assert_eq!(
            dd.inner.total_count(),
            30,
            "all 10 first-batch sketches must merge into the persisted output (3 values × 10)"
        );
    }

    // M2.3.6g — `test_sketch_ingest_persists_and_query_returns_non_empty`
    // deleted: it exercised the retired SketchStore + StoreOutputSink
    // pair end-to-end. The SketchStoreSink path (M2.3.4+) is covered
    // by its own dedicated tests in `output_sink::tests` and by
    // `engine::e2e_feedback_loop_tests`.

    /// Pin the agent-emit-shape vs. backend-grouping-config invariant from
    /// hypothesis (A) of the sweep diagnostic. The agent emits one sketch
    /// per `(zone, rack, node, pod)` tuple; backend's `grouping_labels =
    /// [zone]` rolls them up. This test asserts the rollup actually
    /// happens — sketches with different `(rack, node, pod)` but same
    /// `zone` must collapse into a single persisted output per zone.
    /// If a future change changes `grouping_labels` to include
    /// `rack/node/pod`, the agent's per-tuple emit shape would land 1000
    /// outputs in the store instead of `n_zones`, blowing up cardinality
    /// and breaking ASAP-tier reads.
    #[test]
    fn test_grouping_labels_roll_up_per_tuple_sketches() {
        let cfg = make_agg_config(
            1,
            "http_requests_total_latency_ms_quantile",
            AggregationType::DDSketch,
            "",
            30,
            0,
            vec!["zone"], // sole grouping dim — rack/node/pod are rolled up
        );
        let agg_configs = HashMap::from([(1, cfg)]);
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Three sketches in the SAME zone but different (rack,node,pod)
        // tuples — emulating what the agent ships. Group key the ingest
        // path computes is the zone value alone; distinct group_keys
        // (us-east vs us-west) get distinct sids.
        let pf = PolicyFingerprint(1);
        let sid_east = 41_u64;
        let sid_west = 42_u64;
        for i in 0..3 {
            let s = make_ddsketch(0.01, &[100.0 + i as f64]);
            worker
                .process_accumulator_input(sid_east, pf, "us-east", 60_000, Box::new(s))
                .unwrap();
        }
        // Two sketches in a different zone.
        for i in 0..2 {
            let s = make_ddsketch(0.01, &[200.0 + i as f64]);
            worker
                .process_accumulator_input(sid_west, pf, "us-west", 60_000, Box::new(s))
                .unwrap();
        }

        // Advance the watermark past 90_000 to close window [60_000, 90_000).
        let s = make_ddsketch(0.01, &[1.0]);
        worker
            .process_accumulator_input(sid_east, pf, "us-east", 120_000, Box::new(s))
            .unwrap();
        let s = make_ddsketch(0.01, &[1.0]);
        worker
            .process_accumulator_input(sid_west, pf, "us-west", 120_000, Box::new(s))
            .unwrap();

        let captured = sink.drain();
        // Exactly two emissions for the closed window: one per zone.
        // (us-east merges 3 per-tuple sketches; us-west merges 2.)
        let closed_window_outputs: Vec<_> = captured
            .iter()
            .filter(|(o, _)| o.start_timestamp == 60_000 && o.end_timestamp == 90_000)
            .collect();
        assert_eq!(
            closed_window_outputs.len(),
            2,
            "must emit exactly 2 outputs (one per zone) for the closed window — \
             rollup over rack/node/pod must collapse the 5 per-tuple sketches into 2 per-zone outputs"
        );

        // Verify the merged counts: us-east merges 3 sketches × 1 value each = 3.
        for (output, acc) in closed_window_outputs.iter() {
            let dd = acc
                .as_any()
                .downcast_ref::<DDSketchAccumulator>()
                .expect("must be DDSketchAccumulator");
            let zone = output
                .key
                .as_ref()
                .and_then(|k| k.labels.first().cloned())
                .unwrap_or_default();
            let expected_count: u64 = match zone.as_str() {
                "us-east" => 3,
                "us-west" => 2,
                other => panic!("unexpected zone {other}"),
            };
            assert_eq!(
                dd.inner.total_count(),
                expected_count,
                "zone {zone} must roll up exactly {expected_count} per-tuple sketches"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Sweep blocker #2 — root-cause fix: wall-clock watermark fallback.
    //
    // Pin the failure mode PR #82 escalated as "watermark-semantics design
    // change": event-time stagnates (agent stamps every sketch with the same
    // `time_unix_nano`, e.g. window-start instead of flush-time), so
    // `closed_windows(prev_wm, prev_wm + 1)` in `flush_all` returns empty
    // forever, the 30s window never closes, no output ever lands in the
    // per_key store, and ASAP-tier queries come back empty even though
    // `worker_process_accumulator` keeps logging.
    //
    // The fix tracks each pane's wall-clock last-touch time and force-closes
    // its window once `now - last_touch >= window_size + grace`. This test
    // injects a fake clock so it runs in milliseconds instead of needing
    // `std::thread::sleep(35s)`.
    // -----------------------------------------------------------------------

    /// Build a worker with explicit wall-clock closure grace values.
    fn make_worker_with_wall_clock_policy(
        agg_configs: HashMap<u64, AggregationConfig>,
        sink: Arc<CapturingOutputSink>,
        late_data_policy: LateDataPolicy,
        idle_grace_period_ms: i64,
        max_open_grace_period_ms: i64,
    ) -> Worker {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        Worker::new(
            0,
            rx,
            sink,
            make_hot_reload(agg_configs),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 0,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy,
                wall_clock_idle_grace_period_ms: idle_grace_period_ms,
                wall_clock_max_open_grace_period_ms: max_open_grace_period_ms,
            },
            Arc::new(AtomicUsize::new(0)),
            wm,
        )
    }

    /// Direct test of the wall-clock fallback in `flush_all`. Pins
    /// the fix for sweep blocker #2: even if event-time freezes (every
    /// sketch arrives with the same `time_unix_nano`), the pane must
    /// close once wall-clock time exceeds `window_size + grace`. Uses
    /// an injected fake clock to run in microseconds rather than
    /// sleeping 35 real seconds.
    #[test]
    fn wall_clock_fallback_closes_idle_window() {
        let cfg = make_agg_config(
            1,
            "http_requests_total_latency_ms_quantile",
            AggregationType::DDSketch,
            "",
            30, // 30-second tumbling window
            0,
            vec!["zone"],
        );
        let agg_configs = HashMap::from([(1, cfg)]);
        let sink = Arc::new(CapturingOutputSink::new());
        // 5s grace period — production default.
        let mut worker = make_worker_with_wall_clock_policy(
            agg_configs,
            sink.clone(),
            LateDataPolicy::Drop,
            5_000,
            0,
        );

        // Pin clock at t_wall = 1_000_000 ms during ingest. Every
        // sketch arrives stamped with the SAME event-time
        // (time_unix_nano collapsing to a constant 0 / window-start
        // is the production failure mode the live diagnostic showed:
        // 8000 worker_process_accumulator log lines, 0 store entries).
        let wall_clock = Arc::new(AtomicI64::new(1_000_000));
        let wc_clone = wall_clock.clone();
        worker.set_now_ms_fn(Box::new(move || wc_clone.load(Ordering::Relaxed)));

        // Ingest 10 sketches all stamped at frozen event-time t_event=0.
        let pf = PolicyFingerprint(1);
        let sid = 51_u64;
        for i in 0..10 {
            let s = make_ddsketch(0.01, &[1.0 + i as f64]);
            worker
                .process_accumulator_input(sid, pf, "us-east", 0, Box::new(s))
                .expect("ingest must accept frozen-event-time sketches");
        }
        assert_eq!(
            sink.len(),
            0,
            "no output should be emitted yet: event-time hasn't advanced past the window"
        );

        // Flush at the same wall-clock time (only ~0s elapsed since
        // pane creation). Wall-clock fallback must NOT trigger yet —
        // pane is younger than window_size + grace = 35s.
        worker.flush_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "flush at t_wall=last_touch must not close the window — fallback fires only after grace"
        );

        // Advance fake wall-clock by exactly window_size + grace = 35s.
        // Now the pane has been open long enough that the fallback
        // must close + emit + persist its window, even though
        // event-time is still pinned at 0.
        wall_clock.store(1_000_000 + 30_000 + 5_000, Ordering::Relaxed);
        worker.flush_all().unwrap();

        let captured = sink.drain();
        assert!(
            !captured.is_empty(),
            "wall-clock fallback failed to close the idle window — \
             this is the live sweep blocker #2 root cause: with frozen event-time, \
             the 30s window \
             never closes."
        );

        // The emitted output must cover the window [0, 30_000)
        // — the tumbling 30s window containing all the frozen-time
        // sketches.
        let (output, acc) = &captured[0];
        // PR-6 follow-up: `aggregation_id` field is gone; the worker
        // now emits the config's policy fingerprint. Non-UNSET asserts
        // the emit path threaded the source config through.
        assert!(!output.policy_fp.is_unset());
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 30_000);
        assert_eq!(
            acc.type_name(),
            "DDSketchAccumulator",
            "wall-clock-fallback-emitted accumulator must round-trip as DDSketchAccumulator"
        );
        let dd = acc
            .as_any()
            .downcast_ref::<DDSketchAccumulator>()
            .expect("must downcast back to DDSketchAccumulator");
        assert_eq!(
            dd.inner.total_count(),
            10,
            "all 10 frozen-time sketches must merge into the single emitted output"
        );

        // Calling flush_all again with no new data must NOT re-emit
        // the same window — once a window has been closed via the
        // fallback, its pane is drained from `sketch_panes` and from
        // `pane_wall_clock`, so there's nothing left to
        // close. This pins the monotonicity invariant: emitted
        // windows have monotonically non-decreasing close times and
        // each window is emitted at most once.
        worker.flush_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "wall-clock fallback must be idempotent — already-closed window must not re-emit"
        );
    }

    #[test]
    fn wall_clock_fallback_does_not_close_active_raw_ingest() {
        let cfg = make_agg_config(
            7,
            "netflow_bytes",
            AggregationType::SingleSubpopulation,
            "Sum",
            1,
            0,
            vec![],
        );
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker_with_wall_clock_policy(
            HashMap::from([(7, cfg)]),
            sink.clone(),
            LateDataPolicy::Drop,
            5_000,
            0,
        );
        let wall_clock = Arc::new(AtomicI64::new(1_000_000));
        let wc_clone = wall_clock.clone();
        worker.set_now_ms_fn(Box::new(move || wc_clone.load(Ordering::Relaxed)));

        let pf = PolicyFingerprint(7);
        let mut expected_sum = 0.0;
        for i in 0..7 {
            wall_clock.store(1_000_000 + i * 1_000, Ordering::Relaxed);
            let value = 1.0 + i as f64;
            expected_sum += value;
            worker
                .process_group_samples(7, pf, "", group_samples("netflow_bytes", vec![(0, value)]))
                .unwrap();
        }

        // The pane is older than the old creation-time deadline, but was
        // touched only 500ms ago. Active ingest must keep it open.
        wall_clock.store(1_006_500, Ordering::Relaxed);
        worker.flush_all().unwrap();
        assert_eq!(sink.len(), 0, "active raw pane was force-closed");

        wall_clock.store(1_007_000, Ordering::Relaxed);
        expected_sum += 8.0;
        worker
            .process_group_samples(7, pf, "", group_samples("netflow_bytes", vec![(0, 8.0)]))
            .unwrap();

        wall_clock.store(1_013_001, Ordering::Relaxed);
        worker.flush_all().unwrap();
        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "idle raw pane must close once");
        let sum = captured[0]
            .1
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("must emit SumAccumulator");
        assert!((sum.sum - expected_sum).abs() < 1e-9);
    }

    #[test]
    fn absolute_wall_clock_deadline_closes_active_raw_ingest() {
        let cfg = make_agg_config(
            9,
            "netflow_bytes",
            AggregationType::SingleSubpopulation,
            "Sum",
            1,
            0,
            vec![],
        );
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker_with_wall_clock_policy(
            HashMap::from([(9, cfg)]),
            sink.clone(),
            LateDataPolicy::ForwardToStore,
            5_000,
            5_000,
        );
        let wall_clock = Arc::new(AtomicI64::new(3_000_000));
        let wc_clone = wall_clock.clone();
        worker.set_now_ms_fn(Box::new(move || wc_clone.load(Ordering::Relaxed)));

        let pf = PolicyFingerprint(9);
        for i in 0..7 {
            wall_clock.store(3_000_000 + i * 1_000, Ordering::Relaxed);
            worker
                .process_group_samples(
                    9,
                    pf,
                    "",
                    group_samples("netflow_bytes", vec![(0, 1.0 + i as f64)]),
                )
                .unwrap();
        }

        // The last touch was only 500ms ago, but the pane has been open for
        // 6.5s: longer than window_size + max_open_grace = 6s.
        wall_clock.store(3_006_500, Ordering::Relaxed);
        worker.flush_all().unwrap();
        let mut captured = sink.drain();
        assert_eq!(captured.len(), 1, "absolute deadline must bound freshness");
        let initial = captured.pop().expect("deadline output").1;
        let sum = initial
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("must emit SumAccumulator");
        assert_eq!(sum.sum, 28.0);

        // Continuing input for the already-closed event-time window becomes a
        // mergeable correction instead of being silently dropped.
        wall_clock.store(3_007_000, Ordering::Relaxed);
        worker
            .process_group_samples(9, pf, "", group_samples("netflow_bytes", vec![(0, 8.0)]))
            .unwrap();
        let mut corrections = sink.drain();
        assert_eq!(corrections.len(), 1, "late input must emit a correction");
        let correction = corrections.pop().expect("correction output").1;
        let merged = initial
            .merge_with(correction.as_ref())
            .expect("deadline output and correction must merge");
        let merged_sum = merged
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("merged output must remain SumAccumulator");
        assert_eq!(merged_sum.sum, 36.0);
    }

    #[test]
    fn absolute_deadline_is_disabled_for_drop_policy() {
        let cfg = make_agg_config(
            11,
            "netflow_bytes",
            AggregationType::SingleSubpopulation,
            "Sum",
            1,
            0,
            vec![],
        );
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker_with_wall_clock_policy(
            HashMap::from([(11, cfg)]),
            sink.clone(),
            LateDataPolicy::Drop,
            0,
            5_000,
        );
        let wall_clock = Arc::new(AtomicI64::new(5_000_000));
        let wc_clone = wall_clock.clone();
        worker.set_now_ms_fn(Box::new(move || wc_clone.load(Ordering::Relaxed)));

        worker
            .process_group_samples(
                11,
                PolicyFingerprint(11),
                "",
                group_samples("netflow_bytes", vec![(0, 1.0)]),
            )
            .unwrap();
        wall_clock.store(5_006_500, Ordering::Relaxed);
        worker.flush_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "a deadline must not close a pane when later input would be dropped"
        );
    }

    #[test]
    fn wall_clock_fallback_does_not_close_active_sketch_ingest() {
        let cfg = make_agg_config(
            8,
            "latency",
            AggregationType::DDSketch,
            "",
            1,
            0,
            vec!["zone"],
        );
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker_with_wall_clock_policy(
            HashMap::from([(8, cfg)]),
            sink.clone(),
            LateDataPolicy::Drop,
            5_000,
            0,
        );
        let wall_clock = Arc::new(AtomicI64::new(2_000_000));
        let wc_clone = wall_clock.clone();
        worker.set_now_ms_fn(Box::new(move || wc_clone.load(Ordering::Relaxed)));

        let pf = PolicyFingerprint(8);
        for i in 0..7 {
            wall_clock.store(2_000_000 + i * 1_000, Ordering::Relaxed);
            worker
                .process_accumulator_input(
                    80,
                    pf,
                    "us-east",
                    0,
                    Box::new(make_ddsketch(0.01, &[1.0 + i as f64])),
                )
                .unwrap();
        }

        wall_clock.store(2_006_500, Ordering::Relaxed);
        worker.flush_all().unwrap();
        assert_eq!(sink.len(), 0, "active sketch pane was force-closed");

        wall_clock.store(2_007_000, Ordering::Relaxed);
        worker
            .process_accumulator_input(80, pf, "us-east", 0, Box::new(make_ddsketch(0.01, &[8.0])))
            .unwrap();

        wall_clock.store(2_013_001, Ordering::Relaxed);
        worker.flush_all().unwrap();
        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "idle sketch pane must close once");
        let dd = captured[0]
            .1
            .as_any()
            .downcast_ref::<DDSketchAccumulator>()
            .expect("must emit DDSketchAccumulator");
        assert_eq!(dd.inner.total_count(), 8);
    }

    #[test]
    fn absolute_deadline_forwards_late_sketch_correction() {
        let cfg = make_agg_config(
            10,
            "latency",
            AggregationType::DDSketch,
            "",
            1,
            0,
            vec!["zone"],
        );
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker_with_wall_clock_policy(
            HashMap::from([(10, cfg)]),
            sink.clone(),
            LateDataPolicy::ForwardToStore,
            5_000,
            5_000,
        );
        let wall_clock = Arc::new(AtomicI64::new(4_000_000));
        let wc_clone = wall_clock.clone();
        worker.set_now_ms_fn(Box::new(move || wc_clone.load(Ordering::Relaxed)));

        let pf = PolicyFingerprint(10);
        for i in 0..7 {
            wall_clock.store(4_000_000 + i * 1_000, Ordering::Relaxed);
            worker
                .process_accumulator_input(
                    100,
                    pf,
                    "us-east",
                    0,
                    Box::new(make_ddsketch(0.01, &[1.0 + i as f64])),
                )
                .unwrap();
        }

        wall_clock.store(4_006_500, Ordering::Relaxed);
        worker.flush_all().unwrap();
        let mut deadline_outputs = sink.drain();
        assert_eq!(deadline_outputs.len(), 1);
        let initial = deadline_outputs.pop().expect("deadline output").1;

        wall_clock.store(4_007_000, Ordering::Relaxed);
        worker
            .process_accumulator_input(100, pf, "us-east", 0, Box::new(make_ddsketch(0.01, &[8.0])))
            .unwrap();
        let mut corrections = sink.drain();
        assert_eq!(corrections.len(), 1, "late sketch must emit a correction");
        let correction = corrections.pop().expect("correction output").1;
        let merged = initial
            .merge_with(correction.as_ref())
            .expect("deadline output and sketch correction must merge");
        let dd = merged
            .as_any()
            .downcast_ref::<DDSketchAccumulator>()
            .expect("merged output must remain DDSketchAccumulator");
        assert_eq!(dd.inner.total_count(), 8);
    }

    /// Pin the wall-clock-fallback opt-out: setting
    /// Disabling both wall-clock grace values preserves event-time-only
    /// semantics, matching pre-fix behaviour. This keeps
    /// `flush_all`'s contract backward-compatible for callers that
    /// want strict event-time (e.g. deterministic replays).
    #[test]
    fn wall_clock_fallback_disabled_preserves_event_time_only_semantics() {
        let cfg = make_agg_config(
            1,
            "http_requests_total_latency_ms_quantile",
            AggregationType::DDSketch,
            "",
            30,
            0,
            vec!["zone"],
        );
        let agg_configs = HashMap::from([(1, cfg)]);
        let sink = Arc::new(CapturingOutputSink::new());
        // grace=0 disables the fallback entirely.
        let mut worker = make_worker_with_wall_clock_policy(
            agg_configs,
            sink.clone(),
            LateDataPolicy::Drop,
            0,
            0,
        );

        let wall_clock = Arc::new(AtomicI64::new(1_000_000));
        let wc_clone = wall_clock.clone();
        worker.set_now_ms_fn(Box::new(move || wc_clone.load(Ordering::Relaxed)));

        let s = make_ddsketch(0.01, &[42.0]);
        worker
            .process_accumulator_input(1, PolicyFingerprint(1), "us-east", 0, Box::new(s))
            .unwrap();

        // Even after a wall-clock eternity, no emit happens with
        // grace=0 — event-time hasn't advanced past the window.
        wall_clock.store(1_000_000 + 86_400_000, Ordering::Relaxed); // +24h
        worker.flush_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "grace=0 must disable the fallback — event-time-only semantics"
        );
    }

    // -----------------------------------------------------------------------
    // Test: shutdown force-close emits the trailing window
    //
    // The immediate-shutdown batch case: every record falls in one window and
    // no later timestamp ever advances the watermark, so flush_all (with the
    // wall-clock fallback disabled, grace=0) leaves the window open. On
    // shutdown, force_close_all must close and emit it so the data reaches the
    // store instead of being lost. Covers both the sample (`active_panes`) and
    // sketch (`sketch_panes`) paths.
    // -----------------------------------------------------------------------

    // This single-series updater cannot implement a grouped sum of counter increases.
    // The physical compiler rejects raw counter producers until series state is preserved.
    #[test]
    fn pooled_counter_samples_lose_independent_same_timestamp_reset() {
        use crate::precompute_engine::operators::IncreaseAccumulator;
        let config = make_agg_config(
            1,
            "requests_total",
            AggregationType::SingleSubpopulation,
            "Increase",
            10,
            0,
            vec![],
        );
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            HashMap::from([(1, config)]),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );
        worker
            .process_group_samples(
                1,
                PolicyFingerprint(1),
                "",
                vec![
                    ("requests_total{instance=\"a\"}".into(), 1000, 100.0),
                    ("requests_total{instance=\"b\"}".into(), 1000, 50.0),
                    ("requests_total{instance=\"a\"}".into(), 2000, 110.0),
                    ("requests_total{instance=\"b\"}".into(), 2000, 5.0),
                ],
            )
            .unwrap();
        worker.force_close_all().unwrap();
        let captured = sink.drain();
        let accumulator = captured[0]
            .1
            .as_any()
            .downcast_ref::<IncreaseAccumulator>()
            .unwrap();
        assert_eq!(accumulator.total_increase, 10.0);
        let independent_increases = (110.0 - 100.0) + 5.0;
        assert_eq!(independent_increases, 15.0);
        assert_ne!(accumulator.total_increase, independent_increases);
    }

    #[test]
    fn shutdown_force_close_emits_trailing_sample_window() {
        // 10s tumbling window; make_worker uses grace=0, isolating the
        // force-close from the wall-clock fallback.
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(1, config);
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // All samples land in window [0, 10_000); the watermark freezes below
        // the window end because no later timestamp ever arrives.
        let pf = PolicyFingerprint(1);
        for i in 0..5 {
            worker
                .process_group_samples(
                    1,
                    pf,
                    "",
                    group_samples("cpu", vec![(1_000 + i * 100, 1.0)]),
                )
                .unwrap();
        }

        worker.flush_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "trailing window must remain open after the final flush"
        );

        worker.force_close_all().unwrap();
        let captured = sink.drain();
        assert_eq!(
            captured.len(),
            1,
            "shutdown force-close must emit the trailing window"
        );
        let (output, acc) = &captured[0];
        assert!(!output.policy_fp.is_unset());
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);
        let sum_acc = acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("should be SumAccumulator");
        assert!(
            (sum_acc.sum - 5.0).abs() < 1e-10,
            "5 samples of 1.0 → sum 5, got {}",
            sum_acc.sum
        );

        // Idempotent: panes are drained, so a second force-close emits nothing.
        worker.force_close_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "force-close must be idempotent once panes are drained"
        );
    }

    #[test]
    fn shutdown_force_close_emits_trailing_sketch_window() {
        // 30s tumbling window; grace=0 isolates the force-close.
        let cfg = make_agg_config(
            1,
            "http_requests_total_latency_ms_quantile",
            AggregationType::DDSketch,
            "",
            30,
            0,
            vec!["zone"],
        );
        let agg_configs = HashMap::from([(1, cfg)]);
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker_with_wall_clock_policy(
            agg_configs,
            sink.clone(),
            LateDataPolicy::Drop,
            0,
            0,
        );

        // 10 sketches, all stamped at frozen event-time 0 → window [0, 30_000).
        let pf = PolicyFingerprint(1);
        for i in 0..10 {
            let s = make_ddsketch(0.01, &[1.0 + i as f64]);
            worker
                .process_accumulator_input(51, pf, "us-east", 0, Box::new(s))
                .unwrap();
        }

        worker.flush_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "trailing sketch window must remain open after flush (grace=0, event-time frozen)"
        );

        worker.force_close_all().unwrap();
        let captured = sink.drain();
        assert_eq!(
            captured.len(),
            1,
            "shutdown force-close must emit the trailing sketch window"
        );
        let (output, acc) = &captured[0];
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 30_000);
        assert_eq!(acc.type_name(), "DDSketchAccumulator");
        let dd = acc
            .as_any()
            .downcast_ref::<DDSketchAccumulator>()
            .expect("must downcast to DDSketchAccumulator");
        assert_eq!(
            dd.inner.total_count(),
            10,
            "all 10 frozen-time sketches must merge into the single emitted output"
        );

        worker.force_close_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "force-close must be idempotent once panes are drained"
        );
    }
}
