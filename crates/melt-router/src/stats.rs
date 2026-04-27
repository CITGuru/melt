use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lru::LruCache;
use melt_core::config::RouterConfig;
use melt_core::{RouterCache, StorageBackend, TableRef};
use parking_lot::Mutex;

const TABLES_CACHE_CAP: usize = 4096;
const POLICY_CACHE_CAP: usize = 4096;
const ESTIMATE_CACHE_CAP: usize = 1024;
const MISSING_TTL: Duration = Duration::from_secs(30);

/// Three TTL caches keep routing latency in the sub-millisecond
/// range. All entries time-out so newly-synced tables are picked up
/// without manual invalidation.
pub struct Cache {
    tables_ttl: Duration,
    estimate_ttl: Duration,
    policy_ttl: Duration,

    tables: Mutex<LruCache<TableRef, (bool, Instant)>>,
    policy: Mutex<LruCache<TableRef, (Option<String>, Instant)>>,
    estimates: Mutex<LruCache<Vec<TableRef>, (Vec<u64>, Instant)>>,
}

impl Cache {
    pub fn new(cfg: &RouterConfig) -> Self {
        Self {
            tables_ttl: cfg.table_exists_cache_ttl,
            estimate_ttl: cfg.estimate_bytes_cache_ttl,
            policy_ttl: cfg.table_exists_cache_ttl, // sync's refresh_interval is the real cap; we re-cap on the matcher path
            tables: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(TABLES_CACHE_CAP).unwrap(),
            )),
            policy: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(POLICY_CACHE_CAP).unwrap(),
            )),
            estimates: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(ESTIMATE_CACHE_CAP).unwrap(),
            )),
        }
    }

    pub async fn tables_exist(
        &self,
        backend: &dyn StorageBackend,
        tables: &[TableRef],
    ) -> Vec<bool> {
        let now = Instant::now();
        let mut out = vec![false; tables.len()];
        let mut to_fetch: Vec<usize> = Vec::new();
        {
            let cache = self.tables.lock();
            for (i, t) in tables.iter().enumerate() {
                if let Some((v, at)) = cache.peek(t) {
                    let ttl = if *v { self.tables_ttl } else { MISSING_TTL };
                    if now.duration_since(*at) < ttl {
                        out[i] = *v;
                        continue;
                    }
                }
                to_fetch.push(i);
            }
        }
        if !to_fetch.is_empty() {
            let subset: Vec<TableRef> = to_fetch.iter().map(|&i| tables[i].clone()).collect();
            match backend.tables_exist(&subset).await {
                Ok(results) => {
                    let mut cache = self.tables.lock();
                    for (j, &i) in to_fetch.iter().enumerate() {
                        let v = results.get(j).copied().unwrap_or(false);
                        out[i] = v;
                        cache.put(tables[i].clone(), (v, now));
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "tables_exist backend call failed");
                }
            }
        }
        out
    }

    /// Look up enforce-mode filtered view names for each input table.
    /// Backed by the same TTL cache as `policy_markers` so the
    /// invalidation hook clears both in one call.
    pub async fn policy_views(
        &self,
        backend: &dyn StorageBackend,
        tables: &[TableRef],
    ) -> Vec<Option<String>> {
        // No separate cache layer — view name changes are rare (only
        // when sync re-translates), and the hot path is the policy
        // marker check above. We hit the backend directly.
        backend.policy_views(tables).await.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "policy_views backend call failed");
            vec![None; tables.len()]
        })
    }

    pub async fn policy_markers(
        &self,
        backend: &dyn StorageBackend,
        tables: &[TableRef],
    ) -> Vec<Option<String>> {
        let now = Instant::now();
        let mut out: Vec<Option<String>> = vec![None; tables.len()];
        let mut to_fetch: Vec<usize> = Vec::new();
        {
            let cache = self.policy.lock();
            for (i, t) in tables.iter().enumerate() {
                if let Some((v, at)) = cache.peek(t) {
                    if now.duration_since(*at) < self.policy_ttl {
                        out[i] = v.clone();
                        continue;
                    }
                }
                to_fetch.push(i);
            }
        }
        if !to_fetch.is_empty() {
            let subset: Vec<TableRef> = to_fetch.iter().map(|&i| tables[i].clone()).collect();
            match backend.policy_markers(&subset).await {
                Ok(results) => {
                    let mut cache = self.policy.lock();
                    for (j, &i) in to_fetch.iter().enumerate() {
                        let v = results.get(j).cloned().unwrap_or(None);
                        out[i] = v.clone();
                        cache.put(tables[i].clone(), (v, now));
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "policy_markers backend call failed");
                }
            }
        }
        out
    }

    /// Per-table byte estimates, in the same order as `tables`. The
    /// dual-execution router needs per-table fidelity for the
    /// oversize trigger case and the per-Materialize-fragment cap.
    /// Returns `None` only when the backend errors.
    pub async fn estimate_bytes_per_table(
        &self,
        backend: &dyn StorageBackend,
        tables: &[TableRef],
    ) -> Option<Vec<u64>> {
        let now = Instant::now();
        let key: Vec<TableRef> = tables.to_vec();
        if let Some((v, at)) = self.estimates.lock().peek(&key).cloned() {
            if now.duration_since(at) < self.estimate_ttl {
                return Some(v);
            }
        }
        match backend.estimate_scan_bytes(tables).await {
            Ok(v) => {
                if v.len() != tables.len() {
                    tracing::warn!(
                        expected = tables.len(),
                        got = v.len(),
                        "estimate_scan_bytes returned wrong-length vec; ignoring",
                    );
                    return None;
                }
                self.estimates.lock().put(key, (v.clone(), now));
                Some(v)
            }
            Err(e) => {
                tracing::warn!(error = %e, "estimate_scan_bytes backend call failed");
                None
            }
        }
    }

    /// Sum of per-table estimates. Used by the existing
    /// `lake_max_scan_bytes` guardrail (one cap across the whole
    /// query). Built on top of [`Self::estimate_bytes_per_table`] so
    /// both APIs hit the same TTL cache entry.
    pub async fn estimate_bytes(
        &self,
        backend: &dyn StorageBackend,
        tables: &[TableRef],
    ) -> Option<u64> {
        self.estimate_bytes_per_table(backend, tables)
            .await
            .map(|v| v.iter().sum())
    }
}

/// Implementation of `melt-core::RouterCache` so sync subsystems can
/// invalidate router state via the trait without depending on this crate.
#[async_trait]
impl RouterCache for Cache {
    async fn invalidate_table(&self, table: &TableRef) {
        self.tables.lock().pop(table);
        self.policy.lock().pop(table);
        let mut estimates = self.estimates.lock();
        let to_drop: Vec<Vec<TableRef>> = estimates
            .iter()
            .filter(|(k, _)| k.iter().any(|t| t == table))
            .map(|(k, _)| k.clone())
            .collect();
        for k in to_drop {
            estimates.pop(&k);
        }
    }

    async fn invalidate_all(&self) {
        self.tables.lock().clear();
        self.policy.lock().clear();
        self.estimates.lock().clear();
    }
}

/// Convenience: build the trait-object alias the CLI hands to sync.
pub fn arc_cache(cfg: &RouterConfig) -> Arc<Cache> {
    Arc::new(Cache::new(cfg))
}
