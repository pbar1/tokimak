#![warn(clippy::pedantic)]

use std::future::Future;
use std::num::NonZero;

use futures::Stream;
use futures::StreamExt;
use tokio::task::JoinError;
use tokio_util::task::LocalPoolHandle;
use tracing::warn;

/// Adapter for spawing futures as Tokio tasks with concurrency control.
#[derive(Debug)]
pub struct TaskReactor {
    pool: LocalPoolHandle,
}

impl TaskReactor {
    pub fn new() -> Self {
        let pool_size = std::thread::available_parallelism()
            .unwrap_or_else(|error| {
                warn!(?error, "failed to get parallelism, defaulting to 1");
                unsafe { NonZero::new_unchecked(1) }
            })
            .get();
        let pool = LocalPoolHandle::new(pool_size);
        Self { pool }
    }

    /// Given a stream of futures, spawns them as Tokio tasks each pinned to
    /// their worker. Also enforces a global concurrency limit.
    pub fn buffer_spawned<TStream, TFuture, T>(
        n: usize,
        stream: TStream,
    ) -> impl Stream<Item = Result<T, JoinError>>
    where
        TStream: Stream<Item = TFuture>,
        TFuture: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let reactor = Self::new();
        stream
            .map(move |f| reactor.pool.spawn_pinned(|| f))
            .buffer_unordered(n)
    }
}
