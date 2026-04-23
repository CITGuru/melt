use std::ops::Deref;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Thin wrapper around a JWT/OAuth bearer token. `Deref<Target=str>`
/// so call sites can pass `&token` wherever `&str` is expected.
/// Carries its own expiry so callers can skip re-fetching in tight
/// loops.
#[derive(Clone, Debug)]
pub struct ServiceToken {
    token: String,
    pub expires_at: Instant,
}

impl ServiceToken {
    pub fn new(token: impl Into<String>, ttl: Duration) -> Self {
        Self {
            token: token.into(),
            expires_at: Instant::now() + ttl,
        }
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    pub fn nearly_expired(&self) -> bool {
        let now = Instant::now();
        now + Duration::from_secs(60) >= self.expires_at
    }

    pub fn as_str(&self) -> &str {
        &self.token
    }
}

impl Deref for ServiceToken {
    type Target = str;
    fn deref(&self) -> &str {
        &self.token
    }
}

/// Internal cache that auto-refreshes the service token when it nears
/// expiry. Sync subsystems call `client.service_token().await` each
/// iteration to pick up the current valid token rather than capturing
/// a `String` that would go stale on the first refresh.
pub(crate) struct ServiceTokenCache {
    inner: Mutex<Option<ServiceToken>>,
}

impl ServiceTokenCache {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }

    pub fn get(&self) -> Option<ServiceToken> {
        let guard = self.inner.lock();
        guard.as_ref().cloned()
    }

    pub fn put(&self, token: ServiceToken) {
        *self.inner.lock() = Some(token);
    }

    pub fn invalidate(&self) {
        *self.inner.lock() = None;
    }
}
