use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::{MetricsError, Result};

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install a global Prometheus recorder. Idempotent; the second call
/// is a no-op so tests that call `init` multiple times don't panic.
pub fn install_prometheus() -> Result<()> {
    if HANDLE.get().is_some() {
        return Ok(());
    }
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| MetricsError::Init(format!("install_recorder: {e}")))?;
    let _ = HANDLE.set(handle);
    Ok(())
}

pub fn render() -> String {
    HANDLE
        .get()
        .map(|h| h.render())
        .unwrap_or_else(|| "# metrics recorder not installed\n".to_string())
}
