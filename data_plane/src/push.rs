//! Push precompute (docs/design/parallel-precompute.md): each ingest shard
//! folds its own series' samples into its own instance of each open window
//! of each refresh group built by push, as the samples arrive, instead of
//! one ingestion run per closed window over buffered raw samples.
//!
//! An instance is the group's compiled ingestion run over a [`PushStore`],
//! whose raw data slots are streams fed by the shard: each morsel (the
//! samples of one Remote Write request routed to the shard) is converted
//! once per slot and handed, inline on the shard thread, to every `Scan`
//! of the group that reads its metric, so one morsel reaches every view of
//! its source with no queue between views and no copy of the samples (D2).
//! The summary builds fold each batch as it comes. The run's future is not
//! `Send` (the executor's runs hold `Rc`s), so it lives in the shard's
//! `LocalPool` and never leaves the shard thread.
//!
//! At window close the coordinator's flush is followed by a seal: each
//! shard ends its instance's streams, the run finishes with its partial
//! rows (one per series it owns), and the partial goes to the precompute
//! committer, which concatenates the shards' partials in key order
//! ([`crate::ingestion::Precompute::merge`], the series-split combine of
//! the precompute task pool) and commits the window as the window path
//! does. Every shard seals every window, idle ones with an empty run.
//!
//! A group is built by push when a window's run may be split by series
//! (every node computes each series alone; [`crate::pipelines`] gives its
//! breakers the `Concatenate` rule), carries no state across windows,
//! reads no other group's views, and its `Scan`s read exactly the window
//! (checked once at install by a probe run). Other groups keep the window
//! path.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Instant;

use asap_executor::planner::ir::operator::Source;
use asap_executor::planner::plan::materialized_view::MaterializedView;
use asap_executor::runtime::{Limits, OutputStream, RunContext, Scope};
use asap_executor::sources::{DataStore, MaterializedViewRequest, RawDataRequest};
use asap_executor::values::{Batch, SchemaRef};
use asap_executor::Error;
use futures::channel::mpsc::{unbounded, UnboundedSender};
use futures::executor::LocalPool;
use futures::task::LocalSpawnExt;
use futures::StreamExt;

use crate::ingestion::{window_end, ClosedWindow, Computed, Precompute};
use crate::metrics::{Metrics, Timer};
use crate::store::{RawData, RawSample};

/// The samples of one Remote Write request routed to a shard, and their
/// batches converted once per metric and slot schema: every `Scan` of
/// every open window reading that metric in that schema gets the same
/// batches (the executor re-applies each slot's time range and
/// predicates, so the conversion ignores them).
pub struct Morsel {
    raw: RawData,
    converted: RefCell<HashMap<(String, String), Vec<Batch>>>,
}

impl Morsel {
    fn batches(
        &self,
        metric: &str,
        schema_key: &str,
        schema: &SchemaRef,
    ) -> Result<Vec<Batch>, Error> {
        let key = (metric.to_string(), schema_key.to_string());
        if let Some(batches) = self.converted.borrow().get(&key) {
            return Ok(batches.clone());
        }
        let request = RawDataRequest {
            time_range_ms: None,
            schema,
            predicates: &[],
        };
        let batches: Vec<Batch> = self
            .raw
            .read(metric, &request, None, None)?
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect();
        self.converted.borrow_mut().insert(key, batches.clone());
        Ok(batches)
    }
}

/// One `Scan`'s stream in an instance: what it reads and where its
/// batches go.
struct Reader {
    metric: String,
    schema: SchemaRef,
    /// The schema, printed: readers with equal schemas share conversions.
    schema_key: String,
    sender: Option<UnboundedSender<Batch>>,
}

impl Reader {
    /// `morsel`'s rows of this reader's metric, in its schema.
    fn feed(&self, morsel: &Morsel) -> Result<(), Error> {
        let Some(sender) = &self.sender else {
            return Ok(());
        };
        for batch in morsel.batches(&self.metric, &self.schema_key, &self.schema)? {
            let _ = sender.unbounded_send(batch);
        }
        Ok(())
    }
}

/// The raw data slots of one instance, fed by its shard.
#[derive(Default)]
pub struct Feed {
    readers: RefCell<Vec<Reader>>,
    /// `Scan`s per metric the run opens (from the install probe); while
    /// fewer are open, morsels are kept to replay to the late ones.
    expected: HashMap<String, usize>,
    /// Each `Scan`'s requested time range (for the install probe).
    ranges: RefCell<Vec<Option<(i64, i64)>>>,
    pending: RefCell<Vec<Rc<Morsel>>>,
}

impl Feed {
    fn complete(&self) -> bool {
        let readers = self.readers.borrow();
        self.expected
            .iter()
            .all(|(metric, count)| readers.iter().filter(|r| &r.metric == metric).count() >= *count)
    }

    fn feed(&self, morsel: &Rc<Morsel>) -> Result<(), Error> {
        for reader in self.readers.borrow().iter() {
            reader.feed(morsel)?;
        }
        if !self.complete() {
            self.pending.borrow_mut().push(morsel.clone());
        }
        Ok(())
    }

    /// Ends every stream: the window is sealed.
    fn seal(&self) {
        self.pending.borrow_mut().clear();
        for reader in self.readers.borrow_mut().iter_mut() {
            reader.sender = None;
        }
    }
}

/// The data store of a push instance: raw data slots are the shard's
/// streams; it reads no view (a group built by push reads none).
pub struct PushStore {
    feed: Rc<Feed>,
}

impl DataStore for PushStore {
    fn read_raw_data(
        &self,
        source: &Source,
        request: &RawDataRequest,
        _context: RunContext,
    ) -> Result<OutputStream<'_, Batch>, Error> {
        let Source::TimeSeries { metric } = source else {
            return Err(Error::Invalid(format!(
                "the data plane stores time series only, not {source:?}"
            )));
        };
        let (sender, receiver) = unbounded();
        let reader = Reader {
            metric: metric.clone(),
            schema: request.schema.clone(),
            schema_key: format!("{:?}", request.schema),
            sender: Some(sender),
        };
        self.feed.ranges.borrow_mut().push(request.time_range_ms);
        for morsel in self.feed.pending.borrow().iter() {
            reader.feed(morsel)?;
        }
        self.feed.readers.borrow_mut().push(reader);
        if self.feed.complete() {
            self.feed.pending.borrow_mut().clear();
        }
        Ok(receiver.map(Ok).boxed_local())
    }

    fn read_materialized_view(
        &self,
        view: &MaterializedView,
        _request: &MaterializedViewRequest,
        _context: RunContext,
    ) -> Result<OutputStream<'_, Batch>, Error> {
        Err(Error::Invalid(format!(
            "a group built by push reads no view, but read {}",
            view.node_id
        )))
    }
}

/// What a group built by push needs on every shard, decided at install.
#[derive(Debug, Clone)]
pub struct PushGroup {
    pub width_ms: i64,
    pub phase_ms: i64,
    pub metrics: BTreeSet<String>,
    /// `Scan`s per metric one run opens.
    pub reads: HashMap<String, usize>,
}

impl PushGroup {
    fn window_end(&self, timestamp_ms: i64) -> i64 {
        window_end(timestamp_ms - self.phase_ms, self.width_ms) + self.phase_ms
    }
}

/// A shard's partial of one sealed window, for the committer.
pub struct Partial {
    pub window: ClosedWindow,
    pub shard: u32,
    pub result: Result<Computed, String>,
    pub sealed_at: Instant,
    pub started: Instant,
}

/// Which groups are built by push: those `candidates` (split by series,
/// not serial, reading no view) whose probe run over the window
/// `(phase, phase + W]` opens only `Scan`s reading exactly that window and
/// finishes once they end. `None` for a group kept on the window path.
pub fn probe(
    precompute: &Arc<Precompute>,
    candidates: &[bool],
    windows: &[(i64, i64)],
    metrics: &[BTreeSet<String>],
) -> Vec<Option<PushGroup>> {
    let mut pool = LocalPool::new();
    candidates
        .iter()
        .enumerate()
        .map(|(group, &candidate)| {
            if !candidate {
                return None;
            }
            let (width_ms, phase_ms) = windows[group];
            let end = phase_ms + width_ms;
            let feed = Rc::new(Feed::default());
            let context = RunContext::new(
                Scope::Ingestion {
                    window_start_ms: end - width_ms,
                    window_end_ms: end,
                },
                precompute.limits().clone(),
            )
            .ok()?;
            // The executor takes its store as an `Arc`; this one never
            // leaves the shard thread (its run is not `Send` either).
            #[allow(clippy::arc_with_non_send_sync)]
            let store: Arc<dyn DataStore> = Arc::new(PushStore { feed: feed.clone() });
            let slot: Rc<RefCell<Option<Result<Computed, Error>>>> = Rc::default();
            let run = precompute.push_run(group, store, context);
            let out = slot.clone();
            pool.spawner()
                .spawn_local(async move {
                    *out.borrow_mut() = Some(run.await);
                })
                .ok()?;
            pool.run_until_stalled();
            let mut reads: HashMap<String, usize> = HashMap::new();
            let mut exact = true;
            for reader in feed.readers.borrow().iter() {
                *reads.entry(reader.metric.clone()).or_default() += 1;
            }
            for range in feed.ranges.borrow().iter() {
                exact &= *range == Some((end - width_ms, end));
            }
            feed.seal();
            pool.run_until_stalled();
            let finished = matches!(slot.borrow().as_ref(), Some(Ok(_)));
            let pushable = exact && finished && !reads.is_empty();
            tracing::info!(group, pushable, ?reads, "push probe");
            pushable.then(|| PushGroup {
                width_ms,
                phase_ms,
                metrics: metrics[group].clone(),
                reads,
            })
        })
        .collect()
}

/// One open window's run on a shard.
struct Instance {
    feed: Rc<Feed>,
    result: Rc<RefCell<Option<Result<Computed, Error>>>>,
    context: RunContext,
    started: Instant,
    /// Why the instance failed before its seal (a memory budget).
    failed: Option<String>,
}

/// A shard's push state: its open instances and how far each group is
/// sealed.
pub struct PushShard {
    shard: u32,
    precompute: Arc<Precompute>,
    groups: Vec<Option<PushGroup>>,
    /// The metrics any push group scans, and the raw-data pane width, for
    /// morsels.
    metrics: Vec<String>,
    pane_width_ms: i64,
    limits: Limits,
    budget_bytes: Option<u64>,
    instances: BTreeMap<(usize, i64), Instance>,
    sealed_through: Vec<Option<i64>>,
    pool: LocalPool,
    partials: Sender<Partial>,
    batching: MorselBatching,
    /// Accepted samples not folded yet, and when the oldest came.
    pending: Vec<RawSample>,
    pending_since: Option<Instant>,
}

/// How a shard coalesces its routed samples into morsels before folding:
/// each fold runs every view of the group, so small morsels (one 10 Hz
/// scrape's share of a shard) cost more per sample than large ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MorselBatching {
    /// Fold once this many samples are pending (0 or 1: fold every batch).
    pub max_samples: usize,
    /// ... or once the oldest pending sample came this long ago.
    pub max_age_ms: u64,
}

impl Default for MorselBatching {
    fn default() -> Self {
        Self {
            max_samples: 32_768,
            max_age_ms: 1_000,
        }
    }
}

impl PushShard {
    pub fn new(
        shard: usize,
        precompute: Arc<Precompute>,
        groups: Vec<Option<PushGroup>>,
        pane_width_ms: i64,
        budget_bytes: Option<u64>,
        partials: Sender<Partial>,
        batching: MorselBatching,
    ) -> Self {
        let metrics: BTreeSet<String> = groups
            .iter()
            .flatten()
            .flat_map(|g| g.metrics.iter().cloned())
            .collect();
        let limits = precompute.limits().clone();
        Self {
            shard: shard as u32,
            sealed_through: vec![None; groups.len()],
            precompute,
            groups,
            metrics: metrics.into_iter().collect(),
            pane_width_ms,
            limits,
            budget_bytes,
            instances: BTreeMap::new(),
            pool: LocalPool::new(),
            partials,
            batching,
            pending: Vec::new(),
            pending_since: None,
        }
    }

    /// Takes `samples` (accepted, of this shard's series): buffers them
    /// into the shard's pending morsel and folds it once it holds
    /// `max_samples` or its oldest sample is `max_age_ms` old. A seal
    /// folds what is pending first, so batching never delays a window.
    pub fn push(&mut self, samples: &[RawSample], metrics: &Metrics) {
        if self.metrics.is_empty() {
            return;
        }
        if self.batching.max_samples <= 1 {
            self.fold(samples, metrics);
            return;
        }
        self.pending.extend(
            samples
                .iter()
                .filter(|s| self.metrics.iter().any(|m| **m == *s.labels.metric))
                .cloned(),
        );
        if self.pending.is_empty() {
            return;
        }
        let since = *self.pending_since.get_or_insert_with(Instant::now);
        if self.pending.len() >= self.batching.max_samples
            || since.elapsed().as_millis() as u64 >= self.batching.max_age_ms
        {
            self.flush(metrics);
        }
    }

    /// Folds the pending morsel.
    fn flush(&mut self, metrics: &Metrics) {
        let pending = std::mem::take(&mut self.pending);
        self.pending_since = None;
        self.fold(&pending, metrics);
    }

    fn open(&mut self, group: usize, end_ms: i64) -> Result<Instance, Error> {
        let spec = self.groups[group].as_ref().expect("a push group");
        let feed = Rc::new(Feed {
            expected: spec.reads.clone(),
            ..Feed::default()
        });
        let context = RunContext::new(
            Scope::Ingestion {
                window_start_ms: end_ms - spec.width_ms,
                window_end_ms: end_ms,
            },
            self.limits.clone(),
        )?;
        // As in `probe`: an `Arc` the executor takes, kept on the shard thread.
        #[allow(clippy::arc_with_non_send_sync)]
        let store: Arc<dyn DataStore> = Arc::new(PushStore { feed: feed.clone() });
        let run = self.precompute.push_run(group, store, context.clone());
        let result: Rc<RefCell<Option<Result<Computed, Error>>>> = Rc::default();
        let out = result.clone();
        self.pool
            .spawner()
            .spawn_local(async move {
                *out.borrow_mut() = Some(run.await);
            })
            .map_err(|e| Error::Invalid(e.to_string()))?;
        self.pool.run_until_stalled();
        Ok(Instance {
            feed,
            result,
            context,
            started: Instant::now(),
            failed: None,
        })
    }

    /// Folds `samples` (accepted, of this shard's series) into the open
    /// windows they fall in; returns the samples late for every push
    /// group's views (their window already sealed), which are not folded.
    fn fold(&mut self, samples: &[RawSample], metrics: &Metrics) -> u64 {
        if samples.is_empty() || self.metrics.is_empty() {
            return 0;
        }
        // Each group's samples by the window they fall in: a window's
        // instance gets a morsel of its own samples only, so a large
        // window does not cost a small one's run.
        let mut late = 0;
        let mut by_window: Vec<BTreeMap<i64, Vec<RawSample>>> =
            vec![BTreeMap::new(); self.groups.len()];
        for sample in samples {
            let mut late_for_all = true;
            let mut any = false;
            for (group, spec) in self.groups.iter().enumerate() {
                let Some(spec) = spec else { continue };
                if !spec.metrics.contains(&*sample.labels.metric) {
                    continue;
                }
                any = true;
                let end = spec.window_end(sample.timestamp_ms);
                if self.sealed_through[group].is_some_and(|sealed| end <= sealed) {
                    continue;
                }
                late_for_all = false;
                by_window[group]
                    .entry(end)
                    .or_default()
                    .push(sample.clone());
            }
            if any && late_for_all {
                late += 1;
            }
        }
        for (group, windows) in by_window.into_iter().enumerate() {
            if windows.is_empty() {
                continue;
            }
            let timer = Timer::start();
            for (end, rows) in windows {
                if !self.instances.contains_key(&(group, end)) {
                    match self.open(group, end) {
                        Ok(instance) => {
                            self.instances.insert((group, end), instance);
                        }
                        Err(error) => {
                            tracing::error!(group, end, %error, "push instance did not open");
                            continue;
                        }
                    }
                }
                let instance = self.instances.get_mut(&(group, end)).unwrap();
                if instance.failed.is_some() {
                    continue;
                }
                let mut raw = RawData::for_metrics(self.metrics.iter(), self.pane_width_ms);
                raw.append(&rows);
                let morsel = Rc::new(Morsel {
                    raw,
                    converted: RefCell::default(),
                });
                if let Err(error) = instance.feed.feed(&morsel) {
                    instance.failed = Some(error.to_string());
                }
            }
            self.pool.run_until_stalled();
            if let Some(push) = &metrics.push {
                push.push[group][self.shard as usize].record(timer);
            }
        }
        self.account(metrics);
        late
    }

    /// Publishes this shard's open state and, over the memory budget,
    /// fails this shard's largest instance when every group has only one
    /// window open on every shard: while a group has an older window open,
    /// sealing it will free memory, so nothing fails.
    fn account(&mut self, metrics: &Metrics) {
        let Some(push) = &metrics.push else { return };
        let mut by_group = vec![0u64; self.groups.len()];
        let mut open_by_group = vec![0usize; self.groups.len()];
        for ((group, _), instance) in &self.instances {
            open_by_group[*group] += 1;
            if instance.failed.is_none() {
                by_group[*group] += instance.context.peak_bytes() as u64;
            }
        }
        for (group, bytes) in by_group.iter().enumerate() {
            push.state_bytes[group][self.shard as usize].store(*bytes, Ordering::Relaxed);
        }
        push.open_instances[self.shard as usize]
            .store(self.instances.len() as u64, Ordering::Relaxed);
        push.older_window_open[self.shard as usize]
            .store(open_by_group.iter().any(|&n| n > 1), Ordering::Relaxed);
        let Some(budget) = self.budget_bytes else {
            return;
        };
        let older_open = push
            .older_window_open
            .iter()
            .any(|open| open.load(Ordering::Relaxed));
        if push.total_state_bytes() <= budget || older_open {
            return;
        }
        let largest = self
            .instances
            .iter_mut()
            .filter(|(_, instance)| instance.failed.is_none())
            .max_by_key(|(_, instance)| instance.context.peak_bytes());
        if let Some(((group, _), instance)) = largest {
            instance.failed = Some(format!(
                "the precompute memory budget ({budget} bytes) is exceeded and no older window is open"
            ));
            instance.context.cancel();
            instance.feed.seal();
            // Its group has no other window open here.
            push.state_bytes[*group][self.shard as usize].store(0, Ordering::Relaxed);
        }
        self.pool.run_until_stalled();
    }

    /// Seals `windows` (every shard seals every closed window: an idle
    /// shard with an empty run) and sends their partials; drops instances
    /// of windows the coordinator skipped (`through`, by group).
    pub fn seal(&mut self, windows: &[ClosedWindow], through: &[Option<i64>], metrics: &Metrics) {
        if !self.pending.is_empty() {
            self.flush(metrics);
        }
        for window in windows {
            let Some(Some(_)) = self.groups.get(window.group) else {
                continue;
            };
            let timer = Timer::start();
            let key = (window.group, window.end_ms);
            let instance = match self.instances.remove(&key) {
                Some(instance) => Ok(instance),
                None => self.open(window.group, window.end_ms),
            };
            let result = match instance {
                Err(error) => Err(error.to_string()),
                Ok(instance) => {
                    instance.feed.seal();
                    self.pool.run_until_stalled();
                    match instance.failed {
                        Some(reason) => Err(reason),
                        None => match instance.result.borrow_mut().take() {
                            Some(result) => result.map_err(|e| e.to_string()),
                            None => Err("the push run did not finish at its seal".into()),
                        },
                    }
                    .map(|computed| (computed, instance.started))
                }
            };
            let (result, started) = match result {
                Ok((computed, started)) => (Ok(computed), started),
                Err(error) => (Err(error), Instant::now()),
            };
            if let Some(push) = &metrics.push {
                push.seal[window.group][self.shard as usize].record(timer);
            }
            let _ = self.partials.send(Partial {
                window: *window,
                shard: self.shard,
                result,
                sealed_at: Instant::now(),
                started,
            });
            let sealed = &mut self.sealed_through[window.group];
            *sealed = (*sealed).max(Some(window.end_ms));
        }
        for (group, through) in through.iter().enumerate() {
            if self.groups.get(group).is_none_or(Option::is_none) {
                continue;
            }
            let Some(through) = *through else { continue };
            let skipped: Vec<_> = self
                .instances
                .range((group, i64::MIN)..=(group, through))
                .map(|(key, _)| *key)
                .collect();
            for key in skipped {
                if let Some(instance) = self.instances.remove(&key) {
                    instance.context.cancel();
                    instance.feed.seal();
                }
            }
            let sealed = &mut self.sealed_through[group];
            *sealed = (*sealed).max(Some(through));
        }
        self.pool.run_until_stalled();
        self.account(metrics);
    }
}
