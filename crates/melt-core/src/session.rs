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

/// Pre-baked session claims used by `melt sessions seed`. Mirror the
/// fields a real Snowflake login response carries on `data.sessionInfo`,
/// but populated entirely from local config — no upstream call.
///
/// The proxy's `SessionStore::seed` writes one of these into the store
/// at startup so any login matching the canned demo creds resolves
/// instantly. Anything that isn't here (e.g. driver-supplied per-call
/// overrides like `database`) falls back to the seeded defaults — the
/// goal is "every demo query sees a populated session" rather than
/// production-realistic per-login state.
#[derive(Clone, Debug)]
pub struct SeedClaims {
    pub user: String,
    pub account: String,
    pub database: String,
    pub schema: String,
    pub warehouse: Option<String>,
    pub role: Option<String>,
}

impl SeedClaims {
    /// Default claims — built directly from the `SEED_*` constants in
    /// `melt-core::config`. Operators don't override these; the demo
    /// always uses one canonical set so the README quickstart and the
    /// integration test agree.
    pub fn demo_default() -> Self {
        use crate::config::{
            SEED_ACCOUNT, SEED_DATABASE, SEED_ROLE, SEED_SCHEMA, SEED_USER, SEED_WAREHOUSE,
        };
        Self {
            user: SEED_USER.to_string(),
            account: SEED_ACCOUNT.to_string(),
            database: SEED_DATABASE.to_string(),
            schema: SEED_SCHEMA.to_string(),
            warehouse: Some(SEED_WAREHOUSE.to_string()),
            role: Some(SEED_ROLE.to_string()),
        }
    }
}
