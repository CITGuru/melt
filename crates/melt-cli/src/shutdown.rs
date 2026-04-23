//! Process-wide cooperative shutdown signal.
//!
//! Every long-running task spawned by the runtime — the proxy
//! listener, the metrics admin server, the sync loops, the policy
//! refresher — observes the same `Shutdown` handle and exits
//! cleanly when it fires. This is the only place SIGINT/SIGTERM are
//! turned into a tokio event; subsystems do not subscribe to OS
//! signals on their own.
//!
//! Two signals = abort. The first triggers cooperative shutdown; if
//! a second arrives before the runtime drains we exit hard with
//! status 130 so a wedged subsystem can't keep the process alive.

use std::sync::Arc;

use tokio::sync::Notify;

#[derive(Clone, Default)]
pub struct Shutdown(Arc<Notify>);

impl Shutdown {
    pub fn new() -> Self {
        Self(Arc::new(Notify::new()))
    }

    /// Wake every current and future waiter. Idempotent.
    pub fn trigger(&self) {
        self.0.notify_waiters();
    }

    /// Hand out a clone of the inner `Arc<Notify>` so a subsystem
    /// (e.g. a sync loop) can hold its own handle and `notified().await`
    /// inside a `tokio::select!` without borrowing this struct.
    pub fn notify(&self) -> Arc<Notify> {
        self.0.clone()
    }
}

/// Spawn a background task that translates SIGINT/SIGTERM (Unix) or
/// Ctrl-C (other platforms) into a single `Shutdown::trigger()` call.
/// A second signal short-circuits and exits the process with status
/// 130, the conventional "terminated by Ctrl-C" code.
pub fn install_signal_handler(shutdown: Shutdown) {
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::warn!("shutdown: signal received — propagating to subsystems");
        shutdown.trigger();
        wait_for_signal().await;
        tracing::error!("shutdown: second signal received — aborting");
        std::process::exit(130);
    });
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM");
    let mut int = signal(SignalKind::interrupt()).expect("install SIGINT");
    tokio::select! {
        _ = term.recv() => {},
        _ = int.recv() => {},
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
