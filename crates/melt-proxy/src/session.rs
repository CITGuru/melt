use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use melt_core::{ProxyLimits, SeedClaims, SessionId, SessionInfo};
use tokio::sync::Semaphore;
use uuid::Uuid;

/// Default session lifetime for entries minted locally (seed mode and
/// the post-`forward_login` cache). Real Snowflake responses override
/// this via `validityInSeconds` parsed in `handlers::session`.
const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(60 * 60);

/// In-memory session store keyed by the Snowflake-issued bearer token.
/// Sessions expire after `expires_at`; the proxy checks this before
/// every statement.
pub struct SessionStore {
    inner: DashMap<String, Arc<SessionInfo>>,
    limits: ProxyLimits,
    pub global: Arc<Semaphore>,
}

impl SessionStore {
    pub fn new(limits: ProxyLimits) -> Self {
        let global = Arc::new(Semaphore::new(limits.max_concurrent_global as usize));
        Self {
            inner: DashMap::new(),
            limits,
            global,
        }
    }

    /// Register a new session for `token`. Used immediately after
    /// `forward_login` succeeds.
    pub fn register(&self, token: String) -> Arc<SessionInfo> {
        let info = Arc::new(SessionInfo {
            id: SessionId(Uuid::new_v4()),
            token: token.clone(),
            role: None,
            warehouse: None,
            database: None,
            schema: None,
            expires_at: Instant::now() + DEFAULT_SESSION_TTL,
            concurrency: Arc::new(Semaphore::new(
                self.limits.max_concurrent_per_session as usize,
            )),
        });
        self.inner.insert(token, info.clone());
        info
    }

    /// Seed-mode counterpart to [`Self::register`]. Inserts a session
    /// pre-populated from `claims` and keyed by `token` so the login
    /// handler can short-circuit `POST /session/v1/login-request`
    /// without an upstream call. Idempotent — calling twice with the
    /// same token replaces the entry.
    ///
    /// Unlike `register`, this does not assume the token came from
    /// upstream Snowflake; the caller (`server::serve` at startup)
    /// supplies a deterministic token (`SEED_TOKEN`) so the
    /// integration test and the example client can agree on it.
    pub fn seed(&self, token: impl Into<String>, claims: SeedClaims) -> Arc<SessionInfo> {
        let token = token.into();
        let info = Arc::new(SessionInfo {
            id: SessionId(Uuid::new_v4()),
            token: token.clone(),
            role: claims.role,
            warehouse: claims.warehouse,
            database: Some(claims.database),
            schema: Some(claims.schema),
            expires_at: Instant::now() + DEFAULT_SESSION_TTL,
            concurrency: Arc::new(Semaphore::new(
                self.limits.max_concurrent_per_session as usize,
            )),
        });
        self.inner.insert(token, info.clone());
        info
    }

    pub fn lookup(&self, token: &str) -> Option<Arc<SessionInfo>> {
        let entry = self.inner.get(token)?;
        if entry.expires_at <= Instant::now() {
            drop(entry);
            self.inner.remove(token);
            return None;
        }
        Some(entry.clone())
    }

    pub fn close(&self, token: &str) {
        self.inner.remove(token);
    }

    /// Apply an in-place update to the session for `token`. The
    /// existing `Arc<SessionInfo>` is rebuilt with the mutated state
    /// so other holders observe the change atomically on next lookup.
    /// No-op if the token isn't registered.
    pub fn update<F>(&self, token: &str, mutate: F)
    where
        F: FnOnce(&mut SessionInfo),
    {
        let Some(mut entry) = self.inner.get_mut(token) else {
            return;
        };
        let mut clone = (**entry).clone();
        mutate(&mut clone);
        *entry = Arc::new(clone);
    }

    pub fn active(&self) -> usize {
        self.inner.len()
    }
}
