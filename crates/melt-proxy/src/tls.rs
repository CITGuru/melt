use std::path::Path;

use axum_server::tls_rustls::RustlsConfig;
use melt_core::{MeltError, Result};

/// Load a PEM cert + key pair into a rustls-backed axum-server config.
///
/// Production deployments should use a private CA whose SAN matches
/// `<account>.snowflakecomputing.com`, with DNS configured so clients
/// reach Melt at that hostname. The driver will validate the SAN and
/// refuse to connect otherwise.
pub async fn load(cert: &Path, key: &Path) -> Result<RustlsConfig> {
    RustlsConfig::from_pem_file(cert, key)
        .await
        .map_err(|e| MeltError::config(format!("TLS load: {e}")))
}
