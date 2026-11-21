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
use asap_executor::planner::ir::ScalarExpr;
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

/// One `Scan`'s stream in an instance: what it asked for and where its
/// batches go.
struct Reader {
    metric: String,
    time_range_ms: Option<(i64, i64)>,
    schema: SchemaRef,
    predicates: Vec<ScalarExpr<()>>,
    sender: Option<UnboundedSender<Batch>>,
}

impl Reader {
    /// `morsel`'s rows of this reader's metric, in its schema.
    fn feed(&self, morsel: &RawData) -> Result<(), Error> {
        let Some(sender) = &self.sender else {
            return Ok(());
        };
        let request = RawDataRequest {
            time_range_ms: self.time_range_ms,
            schema: &self.schema,
            predicates: &self.predicates,
        };
        for batch in morsel.read(&self.metric, &request, None, None)? {
            if batch.num_rows() > 0 {
                let _ = sender.unbounded_send(batch);
            }
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
    pending: RefCell<Vec<Arc<RawData>>>,
}

impl Feed {
    fn complete(&self) -> bool {
        let readers = self.readers.borrow();
        self.expected
            .iter()
            .all(|(metric, count)| readers.iter().filter(|r| &r.metric == metric).count() >= *count)
    }

    fn feed(&self, morsel: &Arc<RawData>) -> Result<(), Error> {
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
            time_range_ms: request.time_range_ms,
            schema: request.schema.clone(),
            predicates: request.predicates.to_vec(),
            sender: Some(sender),
        };
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
                exact &= reader.time_range_ms == Some((end - width_ms, end));
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
}

impl PushShard {
    pub fn new(
        shard: usize,
        precompute: Arc<Precompute>,
        groups: Vec<Option<PushGroup>>,
        pane_width_ms: i64,
        budget_bytes: Option<u64>,
        partials: Sender<Partial>,
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
        }
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
    pub fn push(&mut self, samples: &[RawSample], metrics: &Metrics) -> u64 {
        if samples.is_empty() || self.metrics.is_empty() {
            return 0;
        }
        let mut morsel = RawData::for_metrics(self.metrics.iter(), self.pane_width_ms);
        morsel.append(samples);
        let morsel = Arc::new(morsel);
        let mut late = 0;
        let mut ends: Vec<BTreeSet<i64>> = vec![BTreeSet::new(); self.groups.len()];
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
                ends[group].insert(end);
            }
            if any && late_for_all {
                late += 1;
            }
        }
        for (group, ends) in ends.into_iter().enumerate() {
            if ends.is_empty() {
                continue;
            }
            let timer = Timer::start();
            for end in ends {
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

    /// Publishes this shard's open state and fails, under the memory
    /// budget, an instance that alone holds the shard over it: while older
    /// windows are open, sealing them will free memory, so nothing fails.
    fn account(&mut self, metrics: &Metrics) {
        let Some(push) = &metrics.push else { return };
        let mut by_group = vec![0u64; self.groups.len()];
        for ((group, _), instance) in &self.instances {
            if instance.failed.is_none() {
                by_group[*group] += instance.context.peak_bytes() as u64;
            }
        }
        for (group, bytes) in by_group.iter().enumerate() {
            push.state_bytes[group][self.shard as usize].store(*bytes, Ordering::Relaxed);
        }
        push.open_instances[self.shard as usize]
            .store(self.instances.len() as u64, Ordering::Relaxed);
        let Some(budget) = self.budget_bytes else {
            return;
        };
        // An older window open on any shard will be sealed and free its
        // state: wait for it (the reservation succeeds late) rather than
        // fail. Only a lone window over the budget fails.
        let older_open = push
            .open_instances
            .iter()
            .any(|open| open.load(Ordering::Relaxed) > 1);
        if push.total_state_bytes() <= budget || self.instances.len() != 1 || older_open {
            return;
        }
        if let Some(((group, _), instance)) = self.instances.iter_mut().next() {
            if instance.failed.is_none() {
                instance.failed = Some(format!(
                    "the precompute memory budget ({budget} bytes) is exceeded and no older window is open"
                ));
                instance.context.cancel();
                instance.feed.seal();
                push.state_bytes[*group][self.shard as usize].store(0, Ordering::Relaxed);
            }
        }
        self.pool.run_until_stalled();
    }

    /// Seals `windows` (every shard seals every closed window: an idle
    /// shard with an empty run) and sends their partials; drops instances
    /// of windows the coordinator skipped (`through`, by group).
    pub fn seal(&mut self, windows: &[ClosedWindow], through: &[Option<i64>], metrics: &Metrics) {
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
