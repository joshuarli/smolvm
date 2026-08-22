//! Futures-lite file I/O backed by the caller-owned blocking boundary.
//!
//! The registry cannot use `async-fs`: that crate submits work to a
//! process-global `blocking` pool. `BlockingFile` keeps the asynchronous API
//! needed by h12tiny bodies while sending each synchronous operation through
//! the [`crate::RegistryExecutor`] supplied by the application.

use crate::{
    BlockingSubmitError, BoxBlockingFuture, BoxBlockingValue, RegistryError,
    RegistryExecutor, Result,
};
use futures_lite::io::{AsyncRead, AsyncWrite};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

fn boundary_error(error: impl std::fmt::Display) -> RegistryError {
    RegistryError::Blocking(error.to_string())
}

fn submit_error(error: BlockingSubmitError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, error.to_string())
}

async fn run<T, F>(executor: &Arc<dyn RegistryExecutor>, operation: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    // The closure's concrete `io::Result<T>` is erased only at the trait
    // boundary. It is downcast immediately after awaiting, so I/O failures
    // remain `RegistryError::Io` and a contract violation is explicit rather
    // than silently becoming a transport error.
    let future = executor
        .submit_blocking(Box::new(move || {
            Box::new(operation()) as BoxBlockingValue
        }))
        .map_err(boundary_error)?;
    let value = future.await.map_err(boundary_error)?;
    let value = value.downcast::<io::Result<T>>().map_err(|_| {
        boundary_error("blocking executor returned an unexpected result type")
    })?;
    (*value).map_err(RegistryError::Io)
}

pub(crate) async fn copy(
    executor: &Arc<dyn RegistryExecutor>,
    from: &Path,
    to: &Path,
) -> Result<()> {
    let from = from.to_owned();
    let to = to.to_owned();
    run(executor, move || std::fs::copy(from, to).map(|_| ())).await
}

pub(crate) async fn remove_file(executor: &Arc<dyn RegistryExecutor>, path: &Path) -> Result<()> {
    let path = path.to_owned();
    run(executor, move || std::fs::remove_file(path)).await
}

pub(crate) async fn metadata(executor: &Arc<dyn RegistryExecutor>, path: &Path) -> Result<std::fs::Metadata> {
    let path = path.to_owned();
    run(executor, move || std::fs::metadata(path)).await
}

enum Operation {
    Read(usize),
    Write(Vec<u8>),
    Flush,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingKind {
    Read,
    Write,
    Flush,
}

enum Output {
    Read(Vec<u8>, usize),
    Write(usize),
    Flush,
}

struct OperationResult {
    file: File,
    output: io::Result<Output>,
}

/// A file whose synchronous operations run on the application's blocking pool.
pub(crate) struct BlockingFile {
    executor: Arc<dyn RegistryExecutor>,
    file: Option<File>,
    // Mutex makes the adapter `Sync`, which h12tiny requires for request body
    // values, without requiring every executor future itself to be `Sync`.
    pending: Option<std::sync::Mutex<BoxBlockingFuture>>,
    pending_kind: Option<PendingKind>,
}

impl BlockingFile {
    pub(crate) async fn open(
        executor: Arc<dyn RegistryExecutor>,
        path: impl Into<PathBuf>,
    ) -> Result<Self> {
        let path = path.into();
        let file = run(&executor, move || File::open(path)).await?;
        Ok(Self {
            executor,
            file: Some(file),
            pending: None,
            pending_kind: None,
        })
    }

    pub(crate) async fn create(
        executor: Arc<dyn RegistryExecutor>,
        path: impl Into<PathBuf>,
    ) -> Result<Self> {
        let path = path.into();
        let file = run(&executor, move || File::create(path)).await?;
        Ok(Self {
            executor,
            file: Some(file),
            pending: None,
            pending_kind: None,
        })
    }

    fn start(&mut self, operation: Operation) -> io::Result<()> {
        let kind = match &operation {
            Operation::Read(_) => PendingKind::Read,
            Operation::Write(_) => PendingKind::Write,
            Operation::Flush => PendingKind::Flush,
        };
        let file = self.file.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "registry file operation after close")
        })?;
        let future = self.executor.submit_blocking(Box::new(move || {
            let mut file = file;
            let output = match operation {
                Operation::Read(length) => {
                    let mut buffer = vec![0; length];
                    file.read(&mut buffer)
                        .map(|read| Output::Read(buffer, read))
                }
                Operation::Write(buffer) => file.write(&buffer).map(Output::Write),
                Operation::Flush => file.flush().map(|()| Output::Flush),
            };
            Box::new(OperationResult { file, output }) as BoxBlockingValue
        }))
        .map_err(submit_error)?;
        self.pending = Some(std::sync::Mutex::new(future));
        self.pending_kind = Some(kind);
        Ok(())
    }

    fn poll_operation(
        &mut self,
        cx: &mut Context<'_>,
        destination: Option<&mut [u8]>,
        expected: PendingKind,
    ) -> Poll<io::Result<usize>> {
        if self.pending_kind != Some(expected) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                "registry file operation changed while pending",
            )));
        }
        let poll = self
            .pending
            .as_mut()
            .expect("file operation must be pending")
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
            .poll(cx);
        let value = match poll {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => {
                self.pending = None;
                self.pending_kind = None;
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Other,
                    error.to_string(),
                )));
            }
            Poll::Ready(Ok(value)) => value,
        };
        self.pending = None;
        self.pending_kind = None;
        let operation = match value.downcast::<OperationResult>() {
            Ok(operation) => *operation,
            Err(_) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Other,
                    "blocking executor returned an unexpected file operation",
                )))
            }
        };
        self.file = Some(operation.file);
        match operation.output {
            Ok(Output::Read(buffer, read)) if expected == PendingKind::Read => {
                let destination = destination.expect("read operation needs a destination");
                destination[..read].copy_from_slice(&buffer[..read]);
                Poll::Ready(Ok(read))
            }
            Ok(Output::Write(written)) if expected == PendingKind::Write => {
                Poll::Ready(Ok(written))
            }
            Ok(Output::Flush) if expected == PendingKind::Flush => Poll::Ready(Ok(0)),
            Ok(_) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                "blocking executor returned the wrong file operation",
            ))),
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl Unpin for BlockingFile {}

impl AsyncRead for BlockingFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.pending.is_none() {
            if let Err(error) = self.start(Operation::Read(buffer.len())) {
                return Poll::Ready(Err(error));
            }
        }
        self.poll_operation(cx, Some(buffer), PendingKind::Read)
    }
}

impl AsyncWrite for BlockingFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.pending.is_none() {
            if let Err(error) = self.start(Operation::Write(buffer.to_owned())) {
                return Poll::Ready(Err(error));
            }
        }
        self.poll_operation(cx, None, PendingKind::Write)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.pending.is_none() {
            if let Err(error) = self.start(Operation::Flush) {
                return Poll::Ready(Err(error));
            }
        }
        match self.poll_operation(cx, None, PendingKind::Flush) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.as_mut().poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.file.take();
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BlockingTaskError;
    use futures_lite::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Clone, Copy)]
    struct ThreadExecutor;

    impl RegistryExecutor for ThreadExecutor {
        fn execute(&self, future: crate::BoxSendFuture) {
            std::thread::spawn(|| futures_lite::future::block_on(future));
        }

        fn submit_blocking(
            &self,
            job: crate::BoxBlockingJob,
        ) -> std::result::Result<crate::BoxBlockingFuture, BlockingSubmitError> {
            let worker = std::thread::spawn(job);
            Ok(Box::pin(async move {
                worker
                    .join()
                    .map_err(|_| BlockingTaskError::Panicked)
            }))
        }
    }

    struct RejectingExecutor;

    impl RegistryExecutor for RejectingExecutor {
        fn execute(&self, _future: crate::BoxSendFuture) {}

        fn submit_blocking(
            &self,
            _job: crate::BoxBlockingJob,
        ) -> std::result::Result<crate::BoxBlockingFuture, BlockingSubmitError> {
            Err(BlockingSubmitError::QueueFull)
        }
    }

    #[test]
    fn blocking_file_round_trip_uses_executor_and_preserves_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("round-trip");
        let executor: Arc<dyn RegistryExecutor> = Arc::new(ThreadExecutor);

        futures_lite::future::block_on(async {
            let mut writer = BlockingFile::create(executor.clone(), &path).await.unwrap();
            writer.write_all(b"registry file").await.unwrap();
            writer.flush().await.unwrap();

            let mut reader = BlockingFile::open(executor, &path).await.unwrap();
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, b"registry file");
        });
    }

    #[test]
    fn blocking_file_reports_submission_failure() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rejected");
        let executor: Arc<dyn RegistryExecutor> = Arc::new(RejectingExecutor);

        let error = match futures_lite::future::block_on(BlockingFile::create(executor, path)) {
            Ok(_) => panic!("queue rejection must reach the file caller"),
            Err(error) => error,
        };
        assert!(matches!(error, RegistryError::Blocking(message) if message.contains("full")));
    }
}
