//! The application-owned asynchronous runtime boundary.
//!
//! This module is deliberately small and runtime-neutral at its edges.  The
//! application owns a [`Runtime`] value, rather than relying on a process-wide
//! executor or on a library creating a hidden runtime.  Futures are scheduled
//! by [`async_executor`], timers by [`async_io`], blocking closures by a
//! bounded worker pool owned by this module, and the public future combinators
//! come from [`futures_lite`].
//!
//! A runtime task must never perform blocking work directly.  Use
//! [`Runtime::spawn_blocking`] for filesystem, process, or other synchronous
//! operations.  Dropping an async task cancels it; call [`Task::detach`] only
//! for work that is intentionally owned by the runtime until shutdown.
//! Dropping a blocking task cancels result delivery but does not cancel the
//! queued or running synchronous closure.  Shutdown stops accepting work,
//! drains accepted blocking jobs, joins both worker pools, and wakes
//! [`Shutdown::wait`] callers.  It cannot interrupt a closure that is already
//! running.

use async_channel::{Receiver, Sender, TrySendError};
use async_executor::{Executor, Task as AsyncTask};
use futures_lite::future;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// A task spawned on the application's asynchronous executor.
pub type Task<T> = AsyncTask<T>;

/// Descriptive alias for [`Task`].
pub type RuntimeTask<T> = Task<T>;

/// A task returned by [`Runtime::spawn_blocking`].
pub type BlockingTask<T> = RuntimeTask<Result<T, BlockingTaskError>>;

/// The error returned when a runtime or handle cannot accept work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpawnError {
    /// The runtime has stopped accepting work.
    Shutdown,
    /// The bounded blocking queue has reached its configured capacity.
    BlockingQueueFull,
}

impl fmt::Display for SpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shutdown => formatter.write_str("runtime has shut down"),
            Self::BlockingQueueFull => formatter.write_str("blocking queue is full"),
        }
    }
}

impl std::error::Error for SpawnError {}

/// The result error for a [`BlockingTask`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockingTaskError {
    /// The blocking pool closed before this job could produce a result.
    Shutdown,
    /// The submitted closure panicked; the worker remains available for later
    /// jobs.
    Panicked,
}

impl fmt::Display for BlockingTaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shutdown => formatter.write_str("blocking pool shut down"),
            Self::Panicked => formatter.write_str("blocking task panicked"),
        }
    }
}

impl std::error::Error for BlockingTaskError {}

/// The error returned when a timeout elapses before its future completes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("future timed out")
    }
}

impl std::error::Error for Elapsed {}

/// The error returned when a runtime worker cannot be joined cleanly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownError {
    /// A runtime worker thread panicked while it was being shut down.
    WorkerPanicked,
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerPanicked => formatter.write_str("runtime worker panicked"),
        }
    }
}

impl std::error::Error for ShutdownError {}

/// The error returned by [`Semaphore::acquire`] when a semaphore is closed.
///
/// `Semaphore` currently has no close operation, so an acquire on a live
/// semaphore always succeeds eventually.  Keeping the error in the future's
/// result matches the application boundary used by Tokio and leaves room for
/// a future explicit close without changing handler signatures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcquireError;

impl fmt::Display for AcquireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("semaphore is closed")
    }
}

impl std::error::Error for AcquireError {}

/// The error returned when [`Semaphore::try_acquire`] has no permit available.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TryAcquireError {
    /// All permits are currently held or reserved by waiters.
    NoPermits,
}

impl fmt::Display for TryAcquireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPermits => formatter.write_str("semaphore has no available permits"),
        }
    }
}

impl std::error::Error for TryAcquireError {}

/// A bounded, application-owned asynchronous semaphore.
///
/// Permits are handed out fairly to queued waiters and are returned when the
/// corresponding RAII permit is dropped.  The semaphore has no hidden
/// executor or blocking operation; waiting only registers a waker and yields
/// to the caller's executor.
pub struct Semaphore {
    state: Mutex<SemaphoreState>,
}

struct SemaphoreState {
    available: usize,
    next_waiter_id: u64,
    waiters: VecDeque<SemaphoreWaiter>,
}

struct SemaphoreWaiter {
    id: u64,
    waker: Waker,
    granted: bool,
}

impl Semaphore {
    /// Creates a semaphore with `permits` available slots.
    pub fn new(permits: usize) -> Self {
        Self {
            state: Mutex::new(SemaphoreState {
                available: permits,
                next_waiter_id: 0,
                waiters: VecDeque::new(),
            }),
        }
    }

    /// Attempts to take one permit without waiting.
    pub fn try_acquire(&self) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.available == 0 || !state.waiters.is_empty() {
            return Err(TryAcquireError::NoPermits);
        }
        state.available -= 1;
        Ok(SemaphorePermit { semaphore: self })
    }

    /// Waits asynchronously for one permit.
    pub fn acquire(&self) -> SemaphoreAcquire<'_> {
        SemaphoreAcquire {
            semaphore: self,
            waiter_id: None,
        }
    }

    /// Acquire one permit as an infallible mutex-style guard.
    ///
    /// Unlike a channel, this semaphore has no close operation, so callers
    /// that use a one-permit semaphore for mutual exclusion need not carry an
    /// unreachable error branch through their lifecycle transitions.
    pub async fn lock(&self) -> SemaphorePermit<'_> {
        self.acquire()
            .await
            .expect("an application semaphore cannot close")
    }

    /// Waits asynchronously for one permit while retaining an owned
    /// reference to the semaphore.
    pub fn acquire_owned(self: Arc<Self>) -> OwnedSemaphoreAcquire {
        OwnedSemaphoreAcquire {
            semaphore: self,
            waiter_id: None,
        }
    }

    /// Acquire an owned mutex-style guard that keeps the semaphore alive.
    pub async fn lock_owned(self: Arc<Self>) -> OwnedSemaphorePermit {
        self.acquire_owned()
            .await
            .expect("an application semaphore cannot close")
    }

    fn poll_acquire(
        &self,
        waiter_id: &mut Option<u64>,
        waker: &Waker,
    ) -> Poll<Result<(), AcquireError>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if let Some(id) = *waiter_id {
            let Some(waiter) = state.waiters.iter_mut().find(|waiter| waiter.id == id) else {
                // A granted waiter can only disappear when it is cancelled by
                // its own future, so a missing entry means the permit was
                // assigned and the waiter was woken before it was polled.
                *waiter_id = None;
                return Poll::Ready(Ok(()));
            };
            if waiter.granted {
                state
                    .waiters
                    .retain(|waiter| waiter.id != id);
                *waiter_id = None;
                return Poll::Ready(Ok(()));
            }
            if !waiter.waker.will_wake(waker) {
                waiter.waker = waker.clone();
            }
            return Poll::Pending;
        }

        if state.available > 0 && state.waiters.is_empty() {
            state.available -= 1;
            return Poll::Ready(Ok(()));
        }

        let id = state.next_waiter_id;
        state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
        state.waiters.push_back(SemaphoreWaiter {
            id,
            waker: waker.clone(),
            granted: false,
        });
        *waiter_id = Some(id);
        Poll::Pending
    }

    fn cancel_waiter(&self, waiter_id: Option<u64>) {
        let Some(waiter_id) = waiter_id else {
            return;
        };
        let wake = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(position) = state
                .waiters
                .iter()
                .position(|waiter| waiter.id == waiter_id)
            else {
                return;
            };
            let waiter = state
                .waiters
                .remove(position)
                .expect("semaphore waiter position must be valid");
            if !waiter.granted {
                None
            } else if let Some(next) = state.waiters.iter_mut().find(|waiter| !waiter.granted) {
                next.granted = true;
                Some(next.waker.clone())
            } else {
                state.available = state
                    .available
                    .checked_add(1)
                    .expect("semaphore permit count overflow");
                None
            }
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    fn release(&self) {
        let wake = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(waiter) = state.waiters.iter_mut().find(|waiter| !waiter.granted) {
                waiter.granted = true;
                Some(waiter.waker.clone())
            } else {
                state.available = state
                    .available
                    .checked_add(1)
                    .expect("semaphore permit count overflow");
                None
            }
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

/// A borrowed permit from a [`Semaphore`].
pub struct SemaphorePermit<'a> {
    semaphore: &'a Semaphore,
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        self.semaphore.release();
    }
}

/// A future that waits for a borrowed [`SemaphorePermit`].
pub struct SemaphoreAcquire<'a> {
    semaphore: &'a Semaphore,
    waiter_id: Option<u64>,
}

impl<'a> Future for SemaphoreAcquire<'a> {
    type Output = Result<SemaphorePermit<'a>, AcquireError>;

    fn poll(self: std::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this
            .semaphore
            .poll_acquire(&mut this.waiter_id, context.waker())
        {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(SemaphorePermit {
                semaphore: this.semaphore,
            })),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for SemaphoreAcquire<'_> {
    fn drop(&mut self) {
        self.semaphore.cancel_waiter(self.waiter_id.take());
    }
}

/// A future that waits for an owned [`OwnedSemaphorePermit`].
pub struct OwnedSemaphoreAcquire {
    semaphore: Arc<Semaphore>,
    waiter_id: Option<u64>,
}

impl Future for OwnedSemaphoreAcquire {
    type Output = Result<OwnedSemaphorePermit, AcquireError>;

    fn poll(self: std::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this
            .semaphore
            .poll_acquire(&mut this.waiter_id, context.waker())
        {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(OwnedSemaphorePermit {
                semaphore: this.semaphore.clone(),
            })),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for OwnedSemaphoreAcquire {
    fn drop(&mut self) {
        self.semaphore.cancel_waiter(self.waiter_id.take());
    }
}

/// A permit that keeps its [`Semaphore`] alive and returns capacity on drop.
pub struct OwnedSemaphorePermit {
    semaphore: Arc<Semaphore>,
}

impl Drop for OwnedSemaphorePermit {
    fn drop(&mut self) {
        self.semaphore.release();
    }
}

/// A cloneable, edge-triggered asynchronous notification.
///
/// `notify_one` retains one coalesced notification when no waiter is present,
/// while `notify_waiters` wakes every currently registered waiter without
/// retaining a notification.  A waiter that has already been selected by a
/// notification preserves that edge even if it is polled after another
/// notification arrives.
pub struct Notify {
    state: Mutex<NotifyState>,
}

struct NotifyState {
    permit: bool,
    next_waiter_id: u64,
    waiters: VecDeque<NotifyWaiter>,
}

struct NotifyWaiter {
    id: u64,
    waker: Waker,
}

impl Notify {
    /// Creates an empty notification signal.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(NotifyState {
                permit: false,
                next_waiter_id: 0,
                waiters: VecDeque::new(),
            }),
        }
    }

    /// Wakes one waiter, or retains one notification when no waiter exists.
    /// Repeated notifications coalesce into one retained edge.
    pub fn notify_one(&self) {
        let waker = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match state.waiters.pop_front() {
                Some(waiter) => Some(waiter.waker),
                None => {
                    state.permit = true;
                    None
                }
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Wakes all currently registered waiters without retaining a signal.
    pub fn notify_waiters(&self) {
        let wakers = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .waiters
                .drain(..)
                .map(|waiter| waiter.waker)
                .collect::<Vec<_>>()
        };
        for waker in wakers {
            waker.wake();
        }
    }

    /// Creates a future that completes on the next notification edge.
    pub fn notified(&self) -> Notified<'_> {
        Notified {
            notify: self,
            waiter_id: None,
        }
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

/// A future returned by [`Notify::notified`].
pub struct Notified<'a> {
    notify: &'a Notify,
    waiter_id: Option<u64>,
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this
            .notify
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if let Some(id) = this.waiter_id {
            if state.waiters.iter().all(|waiter| waiter.id != id) {
                // This future was selected by notify_one/notify_waiters.  Do
                // not consume a newer retained permit belonging to a later
                // notification.
                this.waiter_id = None;
                return Poll::Ready(());
            }
            let waiter = state
                .waiters
                .iter_mut()
                .find(|waiter| waiter.id == id)
                .expect("notification waiter must be present");
            if !waiter.waker.will_wake(context.waker()) {
                waiter.waker = context.waker().clone();
            }
            return Poll::Pending;
        }

        if state.permit {
            state.permit = false;
            return Poll::Ready(());
        }

        let id = state.next_waiter_id;
        state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
        state.waiters.push_back(NotifyWaiter {
            id,
            waker: context.waker().clone(),
        });
        this.waiter_id = Some(id);
        Poll::Pending
    }
}

impl Drop for Notified<'_> {
    fn drop(&mut self) {
        let Some(waiter_id) = self.waiter_id.take() else {
            return;
        };
        let mut state = self
            .notify
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(position) = state
            .waiters
            .iter()
            .position(|waiter| waiter.id == waiter_id)
        {
            state.waiters.remove(position);
        }
    }
}

/// A one-way, cloneable application shutdown signal.
///
/// The signal is edge-triggered and idempotent.  Calling
/// [`ShutdownTrigger::trigger`] closes the underlying channel, so every
/// current and future [`Shutdown::wait`] observes shutdown; unlike a message
/// channel, one waiter cannot consume the signal for all other waiters.
#[derive(Clone)]
pub struct Shutdown {
    receiver: Receiver<()>,
}

impl fmt::Debug for Shutdown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Shutdown")
            .field("triggered", &self.is_triggered())
            .finish()
    }
}

/// The sender half of a [`Shutdown`] signal.
#[derive(Clone)]
pub struct ShutdownTrigger {
    sender: Sender<()>,
}

impl fmt::Debug for ShutdownTrigger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShutdownTrigger")
            .field("triggered", &self.is_triggered())
            .finish()
    }
}

/// Creates a shutdown signal and its trigger.
pub fn shutdown_signal() -> (ShutdownTrigger, Shutdown) {
    let (sender, receiver) = async_channel::unbounded();
    (ShutdownTrigger { sender }, Shutdown { receiver })
}

impl Shutdown {
    /// Waits until shutdown has been triggered.
    ///
    /// All waiters are woken when the trigger closes the signal.  The future
    /// returns `()` even though the underlying channel uses a close error to
    /// wake all receivers; this keeps shutdown composition independent of a
    /// channel error type.
    pub async fn wait(&self) {
        let _ = self.receiver.clone().recv().await;
    }

    /// Returns whether the signal has been triggered.
    pub fn is_triggered(&self) -> bool {
        self.receiver.is_closed()
    }
}

impl ShutdownTrigger {
    /// Triggers shutdown.  Repeated calls are harmless.
    pub fn trigger(&self) {
        self.sender.close();
    }

    /// Returns whether shutdown has been triggered.
    pub fn is_triggered(&self) -> bool {
        self.sender.is_closed()
    }
}

/// A cloneable handle that can submit work to an existing [`Runtime`].
///
/// A handle does not keep the runtime alive.  Once its runtime is dropped or
/// shut down, submission returns [`SpawnError`] instead of creating work that
/// can no longer be driven.
#[derive(Clone)]
pub struct RuntimeHandle {
    state: Weak<RuntimeState>,
}

impl fmt::Debug for RuntimeHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeHandle")
            .field("shutdown", &self.is_shutdown())
            .finish()
    }
}

impl RuntimeHandle {
    fn state(&self) -> Result<Arc<RuntimeState>, SpawnError> {
        self.state.upgrade().ok_or(SpawnError::Shutdown)
    }

    /// Spawns a sendable future on the application executor.
    pub fn spawn<F, T>(&self, future: F) -> Result<RuntimeTask<T>, SpawnError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let state = self.state()?;
        state.try_spawn(future)
    }

    /// Runs a blocking closure on the owning runtime's bounded blocking pool.
    pub fn spawn_blocking<F, T>(&self, function: F) -> Result<BlockingTask<T>, SpawnError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let state = self.state()?;
        state.try_spawn_blocking(function)
    }

    /// Adapt this application runtime to h12tiny's Hyper task boundary.
    ///
    /// h12tiny connection drivers create background futures while accepting
    /// HTTP/1.1, HTTP/2, TLS, and upgrade connections. The returned executor
    /// submits each one to this owning runtime; it never creates a Tokio
    /// runtime or a process-global executor. A submission racing shutdown is
    /// intentionally dropped because the enclosing server lifecycle is already
    /// draining and will report its own shutdown result.
    pub fn h12_executor(&self) -> h12tiny::runtime::BoxExecutor {
        let runtime = self.clone();
        h12tiny::runtime::BoxExecutor::new(h12tiny::runtime::FnExecutor::new(
            move |future| {
                if let Ok(task) = runtime.spawn(future) {
                    task.detach();
                }
            },
        ))
    }

    /// Returns whether the owning runtime is no longer accepting work.
    pub fn is_shutdown(&self) -> bool {
        self.state
            .upgrade()
            .map_or(true, |state| state.is_shutdown())
    }
}

/// A futures-lite asynchronous reader backed by this runtime's blocking pool.
///
/// Filesystem reads cannot run on an executor worker. `AsyncFile` moves its
/// owned `std::fs::File` into one bounded blocking job at a time and moves it
/// back when that read completes, so response streaming stays bounded and
/// cancellation never exposes concurrent cursor operations on one file.
pub struct AsyncFile {
    runtime: RuntimeHandle,
    file: Option<std::fs::File>,
    pending: Option<Mutex<BlockingTask<AsyncFileRead>>>,
}

struct AsyncFileRead {
    file: std::fs::File,
    result: io::Result<Vec<u8>>,
}

impl AsyncFile {
    /// Wrap an already-open standard file. Callers that need metadata and
    /// streaming to describe the same descriptor should open/stat it in one
    /// blocking operation, then use this constructor.
    pub fn from_file(runtime: RuntimeHandle, file: std::fs::File) -> Self {
        Self {
            runtime,
            file: Some(file),
            pending: None,
        }
    }

    /// Opens `path` using the supplied application-owned blocking boundary.
    pub async fn open(
        runtime: RuntimeHandle,
        path: impl Into<std::path::PathBuf>,
    ) -> io::Result<Self> {
        let path = path.into();
        let task = runtime
            .spawn_blocking(move || std::fs::File::open(path))
            .map_err(runtime_io_error)?;
        let file = task.await.map_err(blocking_io_error)??;
        Ok(Self::from_file(runtime, file))
    }

    fn start_read(&mut self, length: usize) -> io::Result<()> {
        let file = self.file.take().ok_or_else(|| {
            io::Error::other("file read started while another operation is pending")
        })?;
        let task = self
            .runtime
            .spawn_blocking(move || {
                let mut file = file;
                let mut buffer = vec![0; length];
                let result = file.read(&mut buffer).map(|read| {
                    buffer.truncate(read);
                    buffer
                });
                AsyncFileRead { file, result }
            })
            .map_err(runtime_io_error)?;
        self.pending = Some(Mutex::new(task));
        Ok(())
    }
}

impl Unpin for AsyncFile {}

impl futures_lite::io::AsyncRead for AsyncFile {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.pending.is_none() {
            if let Err(error) = self.start_read(buffer.len()) {
                return Poll::Ready(Err(error));
            }
        }

        let result = {
            let task = self
                .pending
                .as_ref()
                .expect("a file read must be pending")
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut task = task;
            match std::pin::Pin::new(&mut *task).poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result,
            }
        };
        self.pending = None;
        let read = result.map_err(blocking_io_error)?;
        self.file = Some(read.file);
        let bytes = read.result?;
        buffer[..bytes.len()].copy_from_slice(&bytes);
        Poll::Ready(Ok(bytes.len()))
    }
}

fn runtime_io_error(error: SpawnError) -> io::Error {
    io::Error::other(error.to_string())
}

fn blocking_io_error(error: BlockingTaskError) -> io::Error {
    io::Error::other(error.to_string())
}

/// Configuration for an application [`Runtime`].
#[derive(Clone, Debug)]
pub struct RuntimeBuilder {
    worker_threads: usize,
    blocking_threads: usize,
    blocking_queue_capacity: usize,
    thread_name: String,
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeBuilder {
    /// Creates a builder using one worker per available logical CPU.
    pub fn new() -> Self {
        let worker_threads = thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(1);
        Self {
            worker_threads,
            blocking_threads: worker_threads.min(16),
            blocking_queue_capacity: 128,
            thread_name: "smolvm-runtime".to_owned(),
        }
    }

    /// Sets the number of executor worker threads.
    ///
    /// A value of zero is rejected by [`RuntimeBuilder::build`].  Keeping the
    /// invalid value in the builder makes configuration parsing straightforward
    /// while still returning a normal [`io::Error`] at the construction edge.
    pub fn worker_threads(mut self, worker_threads: usize) -> Self {
        self.worker_threads = worker_threads;
        self
    }

    /// Sets the number of runtime-owned blocking worker threads.
    pub fn blocking_threads(mut self, blocking_threads: usize) -> Self {
        self.blocking_threads = blocking_threads;
        self
    }

    /// Sets the maximum number of blocking jobs waiting to run.
    ///
    /// A value of zero is rejected by [`RuntimeBuilder::build`].  Once this
    /// queue is full, [`Runtime::try_spawn_blocking`] returns
    /// [`SpawnError::BlockingQueueFull`] so callers can apply their own
    /// backpressure policy.
    pub fn blocking_queue_capacity(mut self, capacity: usize) -> Self {
        self.blocking_queue_capacity = capacity;
        self
    }

    /// Sets the base name used for runtime worker threads.
    pub fn thread_name(mut self, thread_name: impl Into<String>) -> Self {
        self.thread_name = thread_name.into();
        self
    }

    /// Builds an application-owned runtime and starts its workers.
    pub fn build(self) -> io::Result<Runtime> {
        if self.worker_threads == 0 || self.blocking_threads == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime requires at least one async and blocking worker thread",
            ));
        }
        if self.blocking_queue_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "runtime requires a non-zero blocking queue capacity",
            ));
        }

        let executor = Arc::new(Executor::new());
        let (trigger, shutdown) = shutdown_signal();
        let (blocking_pool, mut blocking_workers) = BlockingPool::build(
            &self.thread_name,
            self.blocking_threads,
            self.blocking_queue_capacity,
        )?;
        let state = Arc::new(RuntimeState {
            executor: executor.clone(),
            trigger,
            shutdown: shutdown.clone(),
            accepting: AtomicBool::new(true),
            lifecycle: Mutex::new(()),
            blocking_pool: blocking_pool.clone(),
        });

        let mut workers = Vec::with_capacity(self.worker_threads);
        for index in 0..self.worker_threads {
            let executor = executor.clone();
            let shutdown = shutdown.clone();
            let thread_name = format!("{}-{index}", self.thread_name);
            let worker = thread::Builder::new()
                .name(thread_name)
                .spawn(move || {
                    // `Executor::run` drives all tasks scheduled on this
                    // application-owned executor while the shutdown future is
                    // pending.  A worker must not execute blocking work here;
                    // callers use `Runtime::spawn_blocking` for that boundary.
                    future::block_on(executor.run(shutdown.wait()));
                });

            match worker {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    state.stop_accepting();
                    state.trigger.trigger();
                    blocking_pool.close();
                    for worker in workers {
                        let _ = worker.join();
                    }
                    for worker in blocking_workers.drain(..) {
                        let _ = worker.join();
                    }
                    return Err(error);
                }
            }
        }

        Ok(Runtime {
            state,
            workers,
            blocking_workers,
        })
    }
}

/// The application-owned futures runtime.
///
/// `Runtime` starts fixed async and blocking worker pools when built and joins
/// them during [`Runtime::shutdown`] or drop.  It does not install a global
/// executor or blocking pool, and libraries should accept a
/// [`RuntimeHandle`] rather than constructing their own runtime.  The runtime
/// has no implicit cancellation boundary for detached async tasks; applications
/// should pass a [`Shutdown`] into long-lived tasks and select it with their
/// work.
pub struct Runtime {
    state: Arc<RuntimeState>,
    workers: Vec<JoinHandle<()>>,
    blocking_workers: Vec<JoinHandle<()>>,
}

impl fmt::Debug for Runtime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Runtime")
            .field("worker_count", &self.workers.len())
            .field("blocking_worker_count", &self.blocking_workers.len())
            .field("shutdown", &self.is_shutdown())
            .finish()
    }
}

impl Runtime {
    /// Builds a runtime using one worker per available logical CPU.
    pub fn new() -> io::Result<Self> {
        RuntimeBuilder::new().build()
    }

    /// Builds a runtime with an explicit number of worker threads.
    pub fn with_workers(worker_threads: usize) -> io::Result<Self> {
        RuntimeBuilder::new()
            .worker_threads(worker_threads)
            .build()
    }

    /// Returns a builder for configuring worker count and thread names.
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::new()
    }

    /// Returns a weak, cloneable handle for passing runtime ownership into
    /// application components.
    pub fn handle(&self) -> RuntimeHandle {
        RuntimeHandle {
            state: Arc::downgrade(&self.state),
        }
    }

    /// Returns a clone of this runtime's application shutdown signal.
    pub fn shutdown_signal(&self) -> Shutdown {
        self.state.shutdown.clone()
    }

    /// Returns a trigger for this runtime's application shutdown signal.
    pub fn shutdown_trigger(&self) -> ShutdownTrigger {
        self.state.trigger.clone()
    }

    /// Spawns a sendable future on this runtime's executor.
    ///
    /// This method is infallible while the runtime is live.  Use
    /// [`Runtime::try_spawn`] when the call may race with shutdown.
    pub fn spawn<F, T>(&self, future: F) -> RuntimeTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.try_spawn(future)
            .expect("cannot spawn task after runtime shutdown")
    }

    /// Attempts to spawn a sendable future, returning an error after shutdown.
    pub fn try_spawn<F, T>(&self, future: F) -> Result<RuntimeTask<T>, SpawnError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.state.try_spawn(future)
    }

    /// Runs a blocking closure on this runtime's bounded blocking pool.
    ///
    /// The blocking pool is separate from this runtime's async workers and is
    /// bounded by [`RuntimeBuilder::blocking_queue_capacity`].  Dropping the
    /// returned task stops awaiting its result but cannot interrupt a closure
    /// that has already started.  The result is `Err` if the closure panics or
    /// the pool closes before it can produce a result.  This convenience
    /// method panics when the runtime is shut down or the queue is full; use
    /// [`Runtime::try_spawn_blocking`] when backpressure must be explicit.
    pub fn spawn_blocking<F, T>(&self, function: F) -> BlockingTask<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.try_spawn_blocking(function)
            .expect("cannot spawn blocking task: runtime shut down or queue full")
    }

    /// Attempts to submit a blocking closure, returning an error after
    /// shutdown.
    pub fn try_spawn_blocking<F, T>(&self, function: F) -> Result<BlockingTask<T>, SpawnError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.state.try_spawn_blocking(function)
    }

    /// Drives a future to completion on this runtime's executor.
    ///
    /// This is intended for a synchronous application entry point.  Calling
    /// it from a runtime task is supported, but doing so should be reserved
    /// for short, bounded orchestration because it occupies the calling
    /// worker while the future is being driven.
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        future::block_on(self.state.executor.run(future))
    }

    /// Returns a timer future owned by the application's runtime boundary.
    pub fn sleep(&self, duration: Duration) -> impl Future<Output = ()> {
        sleep(duration)
    }

    /// Returns a deadline timer future owned by the application's runtime
    /// boundary.
    pub fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> {
        sleep_until(deadline)
    }

    /// Returns a future that applies a timeout to `future`.
    pub fn timeout<F, T>(
        &self,
        duration: Duration,
        future: F,
    ) -> impl Future<Output = Result<T, Elapsed>>
    where
        F: Future<Output = T>,
    {
        timeout(duration, future)
    }

    /// Returns a future that applies a deadline timeout to `future`.
    pub fn timeout_at<F, T>(
        &self,
        deadline: Instant,
        future: F,
    ) -> impl Future<Output = Result<T, Elapsed>>
    where
        F: Future<Output = T>,
    {
        timeout_at(deadline, future)
    }

    /// Returns whether this runtime has stopped accepting work.
    pub fn is_shutdown(&self) -> bool {
        self.state.is_shutdown()
    }

    /// Signals shutdown and joins all async and blocking workers.
    ///
    /// Shutdown is idempotent.  A task that is currently executing must yield
    /// before its worker can observe the signal, so blocking operations belong
    /// in [`Runtime::spawn_blocking`].
    pub fn shutdown(&mut self) -> Result<(), ShutdownError> {
        self.state.stop_accepting();
        self.state.trigger.trigger();
        self.state.blocking_pool.close();
        self.join_workers()
    }

    fn join_workers(&mut self) -> Result<(), ShutdownError> {
        let current_thread = thread::current().id();
        let mut panicked = false;
        for worker in self.workers.drain(..).chain(self.blocking_workers.drain(..)) {
            // A runtime can be moved into one of its own tasks.  Joining the
            // current worker would deadlock; dropping this handle detaches it
            // after the shutdown signal has been delivered.
            if worker.thread().id() == current_thread {
                continue;
            }
            if worker.join().is_err() {
                panicked = true;
            }
        }
        if panicked {
            Err(ShutdownError::WorkerPanicked)
        } else {
            Ok(())
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

struct RuntimeState {
    executor: Arc<Executor<'static>>,
    trigger: ShutdownTrigger,
    shutdown: Shutdown,
    accepting: AtomicBool,
    lifecycle: Mutex<()>,
    blocking_pool: Arc<BlockingPool>,
}

impl RuntimeState {
    fn is_shutdown(&self) -> bool {
        !self.accepting.load(Ordering::Acquire)
    }

    fn stop_accepting(&self) {
        let _guard = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.accepting.store(false, Ordering::Release);
    }

    fn try_spawn<F, T>(&self, future: F) -> Result<RuntimeTask<T>, SpawnError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let _guard = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.is_shutdown() {
            return Err(SpawnError::Shutdown);
        }
        Ok(self.executor.spawn(future))
    }

    fn try_spawn_blocking<F, T>(&self, function: F) -> Result<BlockingTask<T>, SpawnError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let _guard = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.is_shutdown() {
            return Err(SpawnError::Shutdown);
        }
        let receiver = self.blocking_pool.try_submit(function)?;
        Ok(self.executor.spawn(async move {
            receiver
                .recv()
                .await
                .unwrap_or(Err(BlockingTaskError::Shutdown))
        }))
    }
}

type BlockingJob = Box<dyn FnOnce() + Send + 'static>;

struct BlockingPool {
    sender: Sender<BlockingJob>,
}

impl BlockingPool {
    fn build(
        thread_name: &str,
        worker_count: usize,
        queue_capacity: usize,
    ) -> io::Result<(Arc<Self>, Vec<JoinHandle<()>>)> {
        let (sender, receiver) = async_channel::bounded(queue_capacity);
        let pool = Arc::new(Self { sender });
        let mut workers = Vec::with_capacity(worker_count);

        for index in 0..worker_count {
            let receiver = receiver.clone();
            let name = format!("{thread_name}-blocking-{index}");
            match thread::Builder::new().name(name).spawn(move || {
                while let Ok(job) = receiver.recv_blocking() {
                    job();
                }
            }) {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    pool.close();
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(error);
                }
            }
        }

        Ok((pool, workers))
    }

    fn close(&self) {
        self.sender.close();
    }

    fn try_submit<F, T>(
        &self,
        function: F,
    ) -> Result<Receiver<Result<T, BlockingTaskError>>, SpawnError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (result_sender, result_receiver) = async_channel::bounded(1);
        let job: BlockingJob = Box::new(move || {
            let result = catch_unwind(AssertUnwindSafe(function))
                .map_err(|_| BlockingTaskError::Panicked);
            let _ = result_sender.try_send(result);
        });

        match self.sender.try_send(job) {
            Ok(()) => Ok(result_receiver),
            Err(TrySendError::Full(_)) => Err(SpawnError::BlockingQueueFull),
            Err(TrySendError::Closed(_)) => Err(SpawnError::Shutdown),
        }
    }
}

/// Returns a future that completes with `future` or [`Elapsed`] if `duration`
/// passes first.
pub async fn timeout<F, T>(duration: Duration, future: F) -> Result<T, Elapsed>
where
    F: Future<Output = T>,
{
    future::or(
        async move { Ok(future.await) },
        async move {
            sleep(duration).await;
            Err(Elapsed)
        },
    )
    .await
}

/// Returns a future that completes with `future` or [`Elapsed`] at `deadline`.
pub async fn timeout_at<F, T>(deadline: Instant, future: F) -> Result<T, Elapsed>
where
    F: Future<Output = T>,
{
    future::or(
        async move { Ok(future.await) },
        async move {
            sleep_until(deadline).await;
            Err(Elapsed)
        },
    )
    .await
}

/// Returns a future that completes after `duration`.
pub async fn sleep(duration: Duration) {
    async_io::Timer::after(duration).await;
}

/// Returns a future that completes at `deadline`.
pub async fn sleep_until(deadline: Instant) {
    async_io::Timer::at(deadline).await;
}

#[cfg(test)]
mod tests {
    use super::{
        shutdown_signal, BlockingTaskError, Elapsed, Notify, Runtime, Semaphore, ShutdownError,
        SpawnError,
    };
    use futures_lite::future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    #[test]
    fn runtime_drives_spawned_tasks_and_blocking_work() {
        let runtime = Runtime::with_workers(2).expect("runtime");
        let value = runtime.block_on(async {
            let first = runtime.spawn(async { 40 });
            let second = runtime.spawn_blocking(|| 2);
            first.await + second.await.expect("blocking result")
        });
        assert_eq!(value, 42);
    }

    #[test]
    fn blocking_pool_is_bounded_and_runtime_owned() {
        let runtime = Runtime::builder()
            .worker_threads(1)
            .blocking_threads(1)
            .blocking_queue_capacity(1)
            .build()
            .expect("runtime");
        let started = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let first_started = started.clone();
        let first_release = release.clone();
        let first = runtime.spawn_blocking(move || {
            first_started.wait();
            first_release.wait();
            1
        });

        started.wait();
        let second = runtime
            .try_spawn_blocking(|| 2)
            .expect("one queued job fits");
        assert_eq!(
            runtime.try_spawn_blocking(|| 3).expect_err("queue is bounded"),
            SpawnError::BlockingQueueFull
        );

        release.wait();
        let values = runtime.block_on(async {
            (first.await.expect("first result"), second.await.expect("second result"))
        });
        assert_eq!(values, (1, 2));
    }

    #[test]
    fn blocking_panic_is_reported_and_does_not_kill_the_pool() {
        let runtime = Runtime::with_workers(1).expect("runtime");
        let panicked = runtime.spawn_blocking(|| panic!("test blocking panic"));
        assert_eq!(
            runtime.block_on(panicked),
            Err(BlockingTaskError::Panicked)
        );

        let healthy = runtime.spawn_blocking(|| 7);
        assert_eq!(runtime.block_on(healthy), Ok(7));
    }

    #[test]
    fn runtime_builder_rejects_zero_workers() {
        let error = Runtime::builder()
            .worker_threads(0)
            .build()
            .expect_err("zero workers must be invalid");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn timeout_reports_elapsed_without_tokio() {
        let runtime = Runtime::with_workers(1).expect("runtime");
        let result = runtime.block_on(runtime.timeout(
            Duration::from_millis(10),
            future::pending::<()>(),
        ));
        assert_eq!(result, Err(Elapsed));
    }

    #[test]
    fn semaphore_permit_returns_capacity_on_drop() {
        let semaphore = Semaphore::new(1);
        let permit = semaphore.try_acquire().expect("initial permit");
        assert!(semaphore.try_acquire().is_err());
        drop(permit);
        assert!(semaphore.try_acquire().is_ok());
    }

    #[test]
    fn semaphore_wakes_a_waiter_when_capacity_returns() {
        let runtime = Runtime::with_workers(1).expect("runtime");
        let semaphore = Semaphore::new(1);
        let held = semaphore.try_acquire().expect("initial permit");
        let mut waiter = semaphore.acquire();
        assert!(runtime.block_on(future::poll_once(&mut waiter)).is_none());

        drop(held);
        let permit = runtime
            .block_on(waiter)
            .expect("waiter receives returned permit");
        drop(permit);
    }

    #[test]
    fn notify_one_retains_one_coalesced_notification() {
        let runtime = Runtime::with_workers(1).expect("runtime");
        let notify = Notify::new();
        notify.notify_one();
        notify.notify_one();
        runtime.block_on(notify.notified());

        let mut next = notify.notified();
        assert!(runtime.block_on(future::poll_once(&mut next)).is_none());
    }

    #[test]
    fn notify_waiters_wakes_all_registered_waiters() {
        let runtime = Runtime::with_workers(1).expect("runtime");
        let notify = Notify::new();
        let mut first = notify.notified();
        let mut second = notify.notified();
        assert!(runtime.block_on(future::poll_once(&mut first)).is_none());
        assert!(runtime.block_on(future::poll_once(&mut second)).is_none());

        notify.notify_waiters();
        runtime.block_on(async {
            first.await;
            second.await;
        });
    }

    #[test]
    fn shutdown_wakes_all_waiters() {
        let runtime = Runtime::with_workers(2).expect("runtime");
        let (trigger, signal) = shutdown_signal();
        let observed = Arc::new(AtomicUsize::new(0));
        let first_observed = observed.clone();
        let second_observed = observed.clone();
        let first_signal = signal.clone();
        let first = runtime.spawn(async move {
            first_signal.wait().await;
            first_observed.fetch_add(1, Ordering::SeqCst);
        });
        let second_signal = signal;
        let second = runtime.spawn(async move {
            second_signal.wait().await;
            second_observed.fetch_add(1, Ordering::SeqCst);
        });

        trigger.trigger();
        runtime.block_on(async {
            first.await;
            second.await;
        });
        assert_eq!(observed.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn handle_rejects_submission_after_shutdown() {
        let mut runtime = Runtime::with_workers(1).expect("runtime");
        let handle = runtime.handle();
        runtime.shutdown().expect("workers shut down");
        assert!(handle.is_shutdown());
        assert_eq!(
            handle.spawn(async {}).expect_err("spawn rejected"),
            SpawnError::Shutdown
        );
        assert_eq!(
            handle
                .spawn_blocking(|| 1)
                .expect_err("blocking spawn rejected"),
            SpawnError::Shutdown
        );
    }

    #[test]
    fn runtime_shutdown_is_idempotent() {
        let mut runtime = Runtime::with_workers(1).expect("runtime");
        assert_eq!(runtime.shutdown(), Ok(()));
        assert_eq!(runtime.shutdown(), Ok(()));
    }

    #[test]
    fn shutdown_error_is_stable_for_matching() {
        assert_eq!(ShutdownError::WorkerPanicked, ShutdownError::WorkerPanicked);
    }

}
