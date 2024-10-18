#![warn(clippy::pedantic)]

use std::future::Future;
use std::sync::Arc;

use futures::prelude::*;
use tokio::task::JoinSet;

/// Adapter for spawing futures as Tokio tasks with concurrency control.
#[derive(Debug)]
pub struct TaskReactor<T> {
    tasks: JoinSet<T>,
    closed: bool,
}
