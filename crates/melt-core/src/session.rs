use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// Opaque session identifier — the proxy generates one per Snowflake
/// login forwarded through Melt.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub uuid::Uuid);

impl SessionId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Authenticated session state cached by the proxy after forwarding
/// a login request to upstream Snowflake.
///
/// `concurrency` is per-session; the proxy also enforces a global
/// cap via `SessionStore::global` in `melt-proxy`.
#[derive(Clone, Debug)]
pub struct SessionInfo {
    pub id: SessionId,
    pub token: String,
    pub role: Option<String>,
    pub warehouse: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub expires_at: Instant,
    pub concurrency: Arc<tokio::sync::Semaphore>,
}

impl SessionInfo {
    pub fn new(token: impl Into<String>, max_concurrent_per_session: u32) -> Self {
        Self {
            id: SessionId::new(),
            token: token.into(),
            role: None,
            warehouse: None,
            database: None,
            schema: None,
            expires_at: Instant::now() + std::time::Duration::from_secs(3600),
            concurrency: Arc::new(tokio::sync::Semaphore::new(
                max_concurrent_per_session as usize,
            )),
        }
    }
}
