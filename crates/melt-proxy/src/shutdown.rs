//! Bridge between an externally-driven shutdown signal and
//! `axum_server`'s graceful-shutdown handle.
//!
//! The CLI owns the OS signal subscription (see
//! `crates/melt-cli/src/shutdown.rs`) so every long-running task —
//! the proxy listener, the metrics admin server, the sync loops —
//! observes a single shared event. This module just translates that
//! event into a `Handle::graceful_shutdown(deadline)` call.

use std::future::Future;
use std::time::Duration;

use axum_server::Handle;

/// Spawn a watcher that, on `shutdown`, calls
/// `handle.graceful_shutdown(Some(deadline))`. The proxy listener
/// then stops accepting new connections, in-flight statements get
/// `deadline` to drain, and the listener future resolves.
pub fn install_external<F>(handle: Handle, shutdown: F, deadline: Duration)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        shutdown.await;
        tracing::warn!(?deadline, "proxy: shutdown signal received — draining");
        handle.graceful_shutdown(Some(deadline));
    });
}
