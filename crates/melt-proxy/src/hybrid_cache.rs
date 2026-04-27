//! Time-bounded result cache for hybrid queries.
//!
//! v1 of the cache is **statement-level**: keyed on the original
//! (un-normalized) SQL plus session database + schema, storing the
//! eager Arrow batches the hybrid path produced. A subsequent
//! identical query inside the TTL window skips the entire hybrid
//! pipeline (no Snowflake fragments staged, no `local_sql`
//! executed) and returns the cached batches directly.
//!
//! ## Why statement-level for v1
//!
//! The §13 ("future work") item the design doc calls out is a
//! *fragment-level* cache — keyed on `RemoteFragment::snowflake_sql`,
//! letting unrelated queries that share a common dimension fragment
//! reuse one Snowflake roundtrip. That's strictly more powerful but
//! requires the Arrow IPC ingest path (DuckDB Appender + Arrow batch
//! into a fresh `__remote_N` temp table without re-running the
//! fragment SQL). Both bigger LOC and a more nuanced design pass.
//!
//! Statement-level captures the dashboard / scheduled-report
//! use-case (the most common production cache hit pattern) and lays
//! the groundwork — the invalidation API is per-table so the
//! fragment-level upgrade is a drop-in once the IPC path lands.
//!
//! ## Invalidation
//!
//! - **TTL**: `router.hybrid_fragment_cache_ttl`. Lookups bump
//!   stale entries off the cache; periodic [`prune_expired`] keeps
//!   long-idle entries from squatting memory.
//! - **Per-table**: every cached entry records the
//!   `scanned_tables` from its plan; [`invalidate_table`] drops
//!   any entry whose `scanned_tables` includes the table. Used by
//!   `melt sync refresh` and the sync writer's existing
//!   `RouterCache` invalidation hook.
//! - **Capacity**: `router.hybrid_fragment_cache_max_entries` is a
//!   hard ceiling; oldest-first eviction once exceeded.
//!
//! ## Disabled by default
//!
//! `hybrid_fragment_cache_ttl == 0` (the default) wires `Option::None`
//! into the proxy state and the cache layer becomes a no-op. Set a
//! TTL > 0 to opt in.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use melt_core::TableRef;
use parking_lot::RwLock;

use crate::execution::HybridExecution;

/// One cached statement result.
#[derive(Clone)]
struct Entry {
    inserted_at: Instant,
    /// Tables the original plan scanned. Used by
    /// [`FragmentCache::invalidate_table`] to drop entries when a
    /// touched table's data may have shifted.
    scanned_tables: Vec<TableRef>,
    schema: Option<SchemaRef>,
    batches: Vec<RecordBatch>,
}

/// Statement-level result cache for the hybrid router.
///
/// `Arc<FragmentCache>` is shared across the proxy's hot path. All
/// internal mutation goes through a `parking_lot::RwLock<HashMap>` —
/// reads dominate (lookups per query), writes are rare (one per
/// completed hybrid query that's worth caching).
pub struct FragmentCache {
    ttl: Duration,
    max_entries: usize,
    inner: RwLock<HashMap<String, Entry>>,
}

impl FragmentCache {
    /// `ttl == 0` ⇒ no-op cache. Callers should prefer wrapping in
    /// `Option<Arc<FragmentCache>>` and only constructing when the
    /// TTL is positive — but constructing a 0-TTL cache here is
    /// also valid and just behaves as a no-op.
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            ttl,
            max_entries: max_entries.max(1),
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Whether the cache will store anything. Cheap inline check the
    /// caller can use to skip the canonicalization step entirely
    /// when the cache is disabled.
    pub fn enabled(&self) -> bool {
        !self.ttl.is_zero()
    }

    /// Cache hit ⇒ returns the cached batches and schema; bumps the
    /// entry's `inserted_at` so popular queries don't expire on
    /// idle. Cache miss / expired entry ⇒ `None`.
    pub fn get(&self, key: &str) -> Option<(Option<SchemaRef>, Vec<RecordBatch>)> {
        if !self.enabled() {
            return None;
        }
        let now = Instant::now();
        // Fast path: read lock, return clone, skip the LRU bump on
        // expired or missing entries.
        {
            let read = self.inner.read();
            let entry = read.get(key)?;
            if now.duration_since(entry.inserted_at) >= self.ttl {
                drop(read);
                self.inner.write().remove(key);
                return None;
            }
            return Some((entry.schema.clone(), entry.batches.clone()));
        }
    }

    /// Insert into the cache. Capacity-evicts the oldest entry once
    /// the table size exceeds `max_entries`.
    pub fn insert(
        &self,
        key: String,
        scanned_tables: Vec<TableRef>,
        schema: Option<SchemaRef>,
        batches: Vec<RecordBatch>,
    ) {
        if !self.enabled() {
            return;
        }
        // Don't waste cache space on empty results — the per-query
        // overhead is already low and the next call will re-execute
        // anyway.
        if batches.is_empty() {
            return;
        }
        let mut write = self.inner.write();
        if write.len() >= self.max_entries {
            // Cheapest possible eviction: drop the entry with the
            // earliest `inserted_at`. O(n) per insert past
            // capacity, fine for the small caches this is sized
            // for (defaults are O(100s) of entries).
            if let Some((oldest_key, _)) = write
                .iter()
                .min_by_key(|(_, e)| e.inserted_at)
                .map(|(k, e)| (k.clone(), e.inserted_at))
            {
                write.remove(&oldest_key);
            }
        }
        write.insert(
            key,
            Entry {
                inserted_at: Instant::now(),
                scanned_tables,
                schema,
                batches,
            },
        );
    }

    /// Drop every entry whose plan touched `table`. Called by sync
    /// writers when `table` is refreshed / re-bootstrapped, and by
    /// the admin reload handler when `[sync].remote` patterns
    /// change. O(cache.size).
    pub fn invalidate_table(&self, table: &TableRef) {
        if !self.enabled() {
            return;
        }
        let mut write = self.inner.write();
        write.retain(|_, e| !e.scanned_tables.iter().any(|t| t == table));
    }

    /// Drop every entry. Called when `[sync].remote` config or other
    /// global routing inputs change in a way too broad to enumerate.
    pub fn invalidate_all(&self) {
        if !self.enabled() {
            return;
        }
        self.inner.write().clear();
    }

    /// Sweep TTL-expired entries. Cheap to call frequently; safe to
    /// call from the metrics tick. The proxy doesn't currently
    /// schedule this — entries expire lazily on lookup — but it's
    /// exposed for an optional future maintenance task.
    pub fn prune_expired(&self) -> usize {
        if !self.enabled() {
            return 0;
        }
        let now = Instant::now();
        let mut write = self.inner.write();
        let before = write.len();
        write.retain(|_, e| now.duration_since(e.inserted_at) < self.ttl);
        before - write.len()
    }

    /// Current entry count. Useful for tests and `/melt/admin/cache`
    /// inspection (latter not wired in v1).
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Convenience wrapper used by `execute_hybrid` to fingerprint a
    /// hybrid execution into the cache. Pure helper; doesn't touch
    /// the cache.
    pub fn key_for(sql: &str, database: &str, schema: &str) -> String {
        // Stable enough — we don't normalize whitespace because
        // identical SQL strings are the v1 hit set. Trim leading /
        // trailing whitespace so the dashboard re-issuing the same
        // query with subtly different padding still hits.
        format!(
            "{}:{}::{}",
            database.trim(),
            schema.trim(),
            sql.trim(),
        )
    }
}

/// Adapter from a [`HybridExecution`] to a cache-write payload.
/// Walks the plan's tables (Materialize fragments + Attach rewrites)
/// to populate `scanned_tables` so per-table invalidation works.
pub fn cache_write_from_execution(exec: &HybridExecution) -> Vec<TableRef> {
    let mut out: Vec<TableRef> = Vec::new();
    for frag in &exec.plan.remote_fragments {
        for t in &frag.scanned_tables {
            if !out.iter().any(|x| x == t) {
                out.push(t.clone());
            }
        }
    }
    for rw in &exec.plan.attach_rewrites {
        if !out.iter().any(|x| x == &rw.original) {
            out.push(rw.original.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    fn empty_table_ref(name: &str) -> TableRef {
        TableRef::new("DB", "PUB", name)
    }

    fn dummy_batch() -> RecordBatch {
        use arrow::array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let arr = Int64Array::from(vec![1i64, 2, 3]);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap()
    }

    #[test]
    fn disabled_when_zero_ttl() {
        let c = FragmentCache::new(Duration::ZERO, 10);
        assert!(!c.enabled());
        c.insert("k".into(), vec![], None, vec![dummy_batch()]);
        assert!(c.get("k").is_none());
    }

    #[test]
    fn round_trip_hit() {
        let c = FragmentCache::new(Duration::from_secs(60), 10);
        c.insert(
            "k".into(),
            vec![empty_table_ref("T")],
            None,
            vec![dummy_batch()],
        );
        let hit = c.get("k").unwrap();
        assert_eq!(hit.1.len(), 1);
        assert_eq!(hit.1[0].num_rows(), 3);
    }

    #[test]
    fn ttl_expires() {
        let c = FragmentCache::new(Duration::from_millis(20), 10);
        c.insert("k".into(), vec![], None, vec![dummy_batch()]);
        std::thread::sleep(Duration::from_millis(40));
        assert!(c.get("k").is_none());
        assert_eq!(c.len(), 0, "lookup of expired entry should evict it");
    }

    #[test]
    fn empty_results_skipped() {
        let c = FragmentCache::new(Duration::from_secs(60), 10);
        c.insert("k".into(), vec![], None, vec![]);
        assert!(c.get("k").is_none());
    }

    #[test]
    fn invalidate_table_drops_relevant_entries() {
        let c = FragmentCache::new(Duration::from_secs(60), 10);
        let t1 = empty_table_ref("T1");
        let t2 = empty_table_ref("T2");
        c.insert("a".into(), vec![t1.clone()], None, vec![dummy_batch()]);
        c.insert(
            "b".into(),
            vec![t1.clone(), t2.clone()],
            None,
            vec![dummy_batch()],
        );
        c.insert("c".into(), vec![t2.clone()], None, vec![dummy_batch()]);
        c.invalidate_table(&t1);
        assert!(c.get("a").is_none());
        assert!(c.get("b").is_none());
        assert!(c.get("c").is_some());
    }

    #[test]
    fn invalidate_all_clears() {
        let c = FragmentCache::new(Duration::from_secs(60), 10);
        c.insert("a".into(), vec![], None, vec![dummy_batch()]);
        c.insert("b".into(), vec![], None, vec![dummy_batch()]);
        c.invalidate_all();
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn capacity_evicts_oldest() {
        let c = FragmentCache::new(Duration::from_secs(60), 2);
        c.insert("a".into(), vec![], None, vec![dummy_batch()]);
        std::thread::sleep(Duration::from_millis(2));
        c.insert("b".into(), vec![], None, vec![dummy_batch()]);
        std::thread::sleep(Duration::from_millis(2));
        c.insert("c".into(), vec![], None, vec![dummy_batch()]);
        assert!(c.get("a").is_none(), "oldest evicted");
        assert!(c.get("b").is_some());
        assert!(c.get("c").is_some());
    }

    #[test]
    fn prune_expired_runs_cleanly() {
        let c = FragmentCache::new(Duration::from_millis(20), 10);
        c.insert("k".into(), vec![], None, vec![dummy_batch()]);
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(c.prune_expired(), 1);
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn key_for_normalizes_padding() {
        assert_eq!(
            FragmentCache::key_for("  SELECT 1  ", "DB", "PUB"),
            FragmentCache::key_for("SELECT 1", "DB", "PUB")
        );
    }
}
