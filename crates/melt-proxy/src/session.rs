use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use melt_core::{ProxyLimits, SessionId, SessionInfo};
use tokio::sync::Semaphore;
use uuid::Uuid;

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
            expires_at: Instant::now() + Duration::from_secs(60 * 60),
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
