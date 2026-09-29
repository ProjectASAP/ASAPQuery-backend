//! Own one native control for the lifetime of a request, including async inputs.
//!
//! Planner's native context is !Send. A blocking worker owns it while Tokio's
//! handle drives input I/O on that same thread. The thread-local is scoped to
//! this worker invocation; it is never installed on a Tokio scheduler thread.
use super::EngineError;
use asap_physical_operators::dag::{Error, Limits, Reservation, RunContext, Scope};
use std::{
    cell::RefCell,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Default)]
struct Cancellation {
    cancelled: AtomicBool,
    changed: tokio::sync::Notify,
}
struct CancelOnDrop(Arc<Cancellation>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancelled.store(true, Ordering::Release);
        self.0.changed.notify_one();
    }
}
struct ActiveRequest {
    context: RunContext,
    cancellation: Arc<Cancellation>,
}
thread_local! {
    static ACTIVE: RefCell<Option<ActiveRequest>> = const { RefCell::new(None) };
}
struct ClearRequest;
impl Drop for ClearRequest {
    fn drop(&mut self) {
        ACTIVE.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

/// Dropping the caller future cancels remote reads and cooperative native work.
pub(crate) async fn run<T, F>(limits: Limits, operation: F) -> Result<T, EngineError>
where
    T: Send + 'static,
    F: FnOnce(tokio::runtime::Handle) -> Result<T, EngineError> + Send + 'static,
{
    let cancellation = Arc::new(Cancellation::default());
    let _cancel = CancelOnDrop(cancellation.clone());
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let context = RunContext::new(
            Scope::Query {
                evaluation_time_ms: 0,
                revision: 0,
            },
            limits,
        )?;
        ACTIVE.with(|slot| {
            assert!(slot.borrow().is_none(), "nested request executor");
            *slot.borrow_mut() = Some(ActiveRequest {
                context,
                cancellation,
            });
        });
        let _clear = ClearRequest;
        check()?;
        operation(handle)
    })
    .await
    .map_err(|error| {
        EngineError::Physical(Error::Operator(format!("query worker failed: {error}")))
    })?
}

pub(crate) fn check() -> Result<(), Error> {
    ACTIVE.with(|slot| {
        if let Some(active) = slot.borrow().as_ref() {
            if active.cancellation.cancelled.load(Ordering::Acquire) {
                active.context.cancel();
            }
            if active.context.is_cancelled() {
                return Err(Error::Cancelled);
            }
        }
        Ok(())
    })
}

pub(crate) fn context(scope: Scope) -> Result<RunContext, Error> {
    context_with_limits(scope, Limits::default())
}

/// A request always shares its active control; standalone callers supply their limit.
pub(crate) fn context_with_limits(scope: Scope, limits: Limits) -> Result<RunContext, Error> {
    check()?;
    ACTIVE.with(|slot| match slot.borrow().as_ref() {
        Some(active) => {
            let mut context = active.context.clone();
            context.scope = scope;
            Ok(context)
        }
        None => RunContext::new(scope, limits),
    })
}

pub(crate) fn reserve(bytes: usize) -> Result<Reservation, Error> {
    context(Scope::Query {
        evaluation_time_ms: 0,
        revision: 0,
    })?
    .reserve(bytes)
}

/// Drive a local native future while bridging caller cancellation on each poll.
/// This must not enter another executor: native operators may synchronously run
/// a nested DAG while they are polled.
pub(crate) fn drive<T>(future: impl Future<Output = T>) -> Result<T, Error> {
    use futures::FutureExt;
    let mut future = Box::pin(future);
    loop {
        check()?;
        if let Some(result) = future.as_mut().now_or_never() {
            check()?;
            return Ok(result);
        }
        std::thread::yield_now();
    }
}

pub(crate) async fn wait<T>(future: impl Future<Output = T>) -> Result<T, Error> {
    check()?;
    let cancellation = ACTIVE.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|active| active.cancellation.clone())
    });
    if let Some(cancellation) = cancellation {
        tokio::select! {
            result = future => { check()?; Ok(result) },
            _ = cancellation.changed.notified() => Err(Error::Cancelled),
        }
    } else {
        Ok(future.await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Evaluation scopes share accounting; independent HTTP requests do not.
    #[tokio::test]
    async fn range_scopes_share_retained_input_and_result_budget() {
        let error = run(
            Limits {
                max_bytes: 1024,
                ..Limits::default()
            },
            |_| {
                let first = context(Scope::Query {
                    evaluation_time_ms: 1,
                    revision: 7,
                })?;
                let _input = first.reserve(600)?;
                let next = context(Scope::Query {
                    evaluation_time_ms: 2,
                    revision: 7,
                })?;
                let _result = next.reserve(500)?;
                Ok(())
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, EngineError::Physical(Error::MemoryLimit)));
        run(
            Limits {
                max_bytes: 1024,
                ..Limits::default()
            },
            |_| {
                let _all = reserve(1024)?;
                Ok(())
            },
        )
        .await
        .unwrap();
    }

    // Cancellation must also stop a nested, cooperatively yielding native run.
    #[tokio::test]
    async fn dropping_request_cancels_native_polling() {
        let (started, ready) = tokio::sync::oneshot::channel();
        let (finished, done) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(run(Limits::default(), move |_| {
            let context = context(Scope::Query {
                evaluation_time_ms: 1,
                revision: 1,
            })?;
            started.send(()).unwrap();
            let result = drive(std::future::pending::<()>());
            finished
                .send(matches!(result, Err(Error::Cancelled)) && context.is_cancelled())
                .unwrap();
            result.map_err(EngineError::from)
        }));
        ready.await.unwrap();
        task.abort();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), done)
                .await
                .unwrap()
                .unwrap()
        );
    }

    // Aborting the async caller wakes a remote read even when it never replies.
    #[tokio::test]
    async fn dropping_request_cancels_inflight_input_and_worker() {
        let (started, ready) = tokio::sync::oneshot::channel();
        let (finished, done) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(run(Limits::default(), move |handle| {
            handle.block_on(async {
                started.send(()).unwrap();
                let result = wait(std::future::pending::<()>()).await;
                let cancelled = matches!(result, Err(Error::Cancelled));
                finished.send(cancelled).unwrap();
                result.map_err(EngineError::from)
            })
        }));
        ready.await.unwrap();
        task.abort();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), done)
                .await
                .unwrap()
                .unwrap()
        );
    }
}
