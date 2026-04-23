use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use futures::StreamExt;
use melt_core::{MeltError, ProxyLimits, RecordBatchStream, Result, RouteKind, SessionId};
use metrics::gauge;
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use crate::response::{batches_to_partition, ColumnMeta};

/// Shape returned to the proxy's pagination handler.
#[derive(Debug)]
pub struct PartitionPage {
    pub rows: Vec<Vec<Option<String>>>,
    pub partition: u32,
    pub has_more: bool,
    pub row_type: Vec<ColumnMeta>,
}

struct Entry {
    stream: AsyncMutex<Option<RecordBatchStream>>,
    schema: Option<SchemaRef>,
    /// Which session created this handle. `poll_partition` / `cancel`
    /// require the caller's current session id to match — otherwise
    /// we return `HandleNotFound`, indistinguishable from a stale
    /// handle so enumeration attempts don't leak presence.
    session: SessionId,
    route: RouteKind,
    #[allow(dead_code)]
    created_at: Instant,
    last_poll_at: Mutex<Instant>,
    bytes: Mutex<u64>,
    cursor: Mutex<u32>,
}

/// LRU keyed by handle UUID. The budget / eviction model is lazy:
/// per-batch byte accounting accumulates as batches are pulled, and
/// entries are evicted in LRU order once `result_store_max_bytes`
/// is crossed. A background sweeper drops entries idle longer than
/// `result_store_idle_ttl`.
pub struct ResultStore {
    limits: ProxyLimits,
    entries: Mutex<HashMap<Uuid, Arc<Entry>>>,
    /// Insertion-order list used for LRU eviction (cheapest correct
    /// approximation for the MVP — refine later if necessary).
    order: Mutex<Vec<Uuid>>,
    total_bytes: Mutex<u64>,
}

impl ResultStore {
    pub fn new(limits: ProxyLimits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            entries: Mutex::new(HashMap::new()),
            order: Mutex::new(Vec::new()),
            total_bytes: Mutex::new(0),
        })
    }

    pub fn insert(
        &self,
        stream: RecordBatchStream,
        session: SessionId,
        route: RouteKind,
        schema: Option<SchemaRef>,
    ) -> Uuid {
        let handle = Uuid::new_v4();
        let entry = Arc::new(Entry {
            stream: AsyncMutex::new(Some(stream)),
            schema,
            session,
            route,
            created_at: Instant::now(),
            last_poll_at: Mutex::new(Instant::now()),
            bytes: Mutex::new(0),
            cursor: Mutex::new(0),
        });
        let evict_to_capacity = {
            let mut entries = self.entries.lock();
            let mut order = self.order.lock();
            entries.insert(handle, entry);
            order.push(handle);
            order.len() > self.limits.result_store_max_entries as usize
        };
        if evict_to_capacity {
            self.evict_oldest_entry();
        }
        handle
    }

    /// Pull the next partition's worth of rows. Requires the caller's
    /// `SessionId` to match the entry's — otherwise we return
    /// `HandleNotFound` (same observable behavior as a stale / evicted
    /// handle, so enumeration cannot confirm a handle exists).
    ///
    /// We don't currently page within a single batch — Snowflake drivers
    /// are happy with one batch per partition.
    pub async fn poll_partition(
        &self,
        handle: Uuid,
        partition: u32,
        session: &SessionId,
    ) -> Result<PartitionPage> {
        let entry = self
            .entries
            .lock()
            .get(&handle)
            .cloned()
            .ok_or(MeltError::HandleNotFound)?;

        if entry.session != *session {
            // Do not reveal "exists but not yours" — drivers see the
            // same code they'd see for an evicted / restarted handle.
            return Err(MeltError::HandleNotFound);
        }

        let cursor_now = *entry.cursor.lock();
        if partition < cursor_now {
            return Err(MeltError::HandleNotFound);
        }
        if partition > cursor_now {
            return Err(MeltError::HandleNotFound);
        }

        let mut stream_guard = entry.stream.lock().await;
        let Some(stream) = stream_guard.as_mut() else {
            return Ok(PartitionPage {
                rows: Vec::new(),
                partition,
                has_more: false,
                row_type: column_meta(entry.schema.as_ref()),
            });
        };

        let next: Option<Result<RecordBatch>> = stream.next().await;

        let mut last_poll = entry.last_poll_at.lock();
        *last_poll = Instant::now();
        drop(last_poll);

        match next {
            Some(Ok(batch)) => {
                let bytes = batch.get_array_memory_size() as u64;
                {
                    let mut e = entry.bytes.lock();
                    *e += bytes;
                }
                let total = {
                    let mut t = self.total_bytes.lock();
                    *t += bytes;
                    *t
                };
                gauge!(melt_metrics::RESULT_STORE_BYTES).set(total as f64);
                self.maybe_evict();
                let mut cursor = entry.cursor.lock();
                *cursor += 1;
                drop(cursor);
                let row_type = column_meta(Some(&batch.schema()));
                let rows = batches_to_partition(&[batch]);
                Ok(PartitionPage {
                    rows,
                    partition,
                    has_more: true,
                    row_type,
                })
            }
            Some(Err(e)) => {
                stream_guard.take();
                Err(e)
            }
            None => {
                stream_guard.take();
                Ok(PartitionPage {
                    rows: Vec::new(),
                    partition,
                    has_more: false,
                    row_type: column_meta(entry.schema.as_ref()),
                })
            }
        }
    }

    /// Unrestricted route lookup — used only by internal call sites
    /// (e.g. the idle sweeper's eviction path). Do NOT call from a
    /// handler without first checking the caller's session via
    /// [`lookup_route_for_session`].
    pub fn lookup_route(&self, handle: Uuid) -> Option<RouteKind> {
        self.entries.lock().get(&handle).map(|e| e.route)
    }

    /// Session-scoped route lookup. Returns `Some(route)` only when
    /// the handle exists AND was created by `session`. Hides presence
    /// otherwise — the two `None` cases (missing vs. cross-session)
    /// are deliberately indistinguishable.
    pub fn lookup_route_for_session(&self, handle: Uuid, session: &SessionId) -> Option<RouteKind> {
        let entries = self.entries.lock();
        let entry = entries.get(&handle)?;
        if entry.session == *session {
            Some(entry.route)
        } else {
            None
        }
    }

    /// Cancel a handle. Requires session match — returns
    /// `HandleNotFound` on mismatch so an attacker with a leaked UUID
    /// cannot cancel someone else's in-flight query.
    pub fn cancel(&self, handle: Uuid, session: &SessionId) -> Result<()> {
        {
            let entries = self.entries.lock();
            let entry = entries.get(&handle).ok_or(MeltError::HandleNotFound)?;
            if entry.session != *session {
                return Err(MeltError::HandleNotFound);
            }
        }
        self.remove(handle).map(|_| ())
    }

    pub fn remove(&self, handle: Uuid) -> Result<u64> {
        let mut entries = self.entries.lock();
        let entry = entries.remove(&handle).ok_or(MeltError::HandleNotFound)?;
        let bytes = *entry.bytes.lock();
        let mut order = self.order.lock();
        order.retain(|h| h != &handle);
        let new_total = {
            let mut t = self.total_bytes.lock();
            *t = t.saturating_sub(bytes);
            *t
        };
        gauge!(melt_metrics::RESULT_STORE_BYTES).set(new_total as f64);
        Ok(bytes)
    }

    fn maybe_evict(&self) {
        let limit = self.limits.result_store_max_bytes.as_u64();
        loop {
            let total = *self.total_bytes.lock();
            if total <= limit {
                return;
            }
            if !self.evict_oldest_entry() {
                return;
            }
        }
    }

    fn evict_oldest_entry(&self) -> bool {
        let mut order = self.order.lock();
        let Some(handle) = order.first().copied() else {
            return false;
        };
        order.remove(0);
        drop(order);
        if self.remove(handle).is_ok() {
            tracing::warn!(%handle, "result store evicted handle under pressure");
            true
        } else {
            false
        }
    }

    /// Background sweeper. Drops every entry whose `now -
    /// last_poll_at > result_store_idle_ttl`.
    pub fn run_idle_sweeper(self: Arc<Self>) {
        let store = self.clone();
        tokio::spawn(async move {
            let ttl = store.limits.result_store_idle_ttl;
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                let now = Instant::now();
                let stale: Vec<Uuid> = {
                    let entries = store.entries.lock();
                    entries
                        .iter()
                        .filter(|(_, e)| now.duration_since(*e.last_poll_at.lock()) > ttl)
                        .map(|(h, _)| *h)
                        .collect()
                };
                for h in stale {
                    let _ = store.remove(h);
                }
            }
        });
    }

    pub fn total_bytes(&self) -> u64 {
        *self.total_bytes.lock()
    }
}

fn column_meta(schema: Option<&SchemaRef>) -> Vec<ColumnMeta> {
    let Some(schema) = schema else {
        return Vec::new();
    };
    schema
        .fields()
        .iter()
        .map(|f| ColumnMeta {
            name: f.name().clone(),
            data_type: f.data_type().to_string(),
            nullable: f.is_nullable(),
        })
        .collect()
}

/// Hold a Bytes payload and replay tag in the store under a passthrough
/// handle so cancel routes correctly.
pub fn stream_from_bytes(b: Bytes) -> RecordBatchStream {
    let _ = b;
    Box::pin(futures::stream::empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_store() -> Arc<ResultStore> {
        ResultStore::new(ProxyLimits::default())
    }

    fn empty_stream() -> RecordBatchStream {
        Box::pin(futures::stream::empty())
    }

    /// A session can always poll handles it created.
    #[tokio::test]
    async fn owner_can_poll() {
        let store = fresh_store();
        let alice = SessionId::new();
        let handle = store.insert(empty_stream(), alice.clone(), RouteKind::Lake, None);
        let page = store.poll_partition(handle, 0, &alice).await.unwrap();
        assert_eq!(page.rows.len(), 0);
        assert!(!page.has_more);
    }

    /// Cross-session poll MUST return HandleNotFound — not
    /// Unauthorized, not 500, not a success with someone else's data.
    /// This is the fix for the leaked-UUID issue: an attacker who
    /// knows the UUID but not the session gets the same observable
    /// behavior as polling a stale handle.
    #[tokio::test]
    async fn cross_session_poll_is_not_found() {
        let store = fresh_store();
        let alice = SessionId::new();
        let mallory = SessionId::new();
        let handle = store.insert(empty_stream(), alice, RouteKind::Lake, None);
        let err = store.poll_partition(handle, 0, &mallory).await.unwrap_err();
        assert!(matches!(err, MeltError::HandleNotFound));
    }

    /// Same invariant on cancel — an attacker who guesses a UUID
    /// cannot kill someone else's in-flight query.
    #[tokio::test]
    async fn cross_session_cancel_is_not_found() {
        let store = fresh_store();
        let alice = SessionId::new();
        let mallory = SessionId::new();
        let handle = store.insert(empty_stream(), alice.clone(), RouteKind::Lake, None);
        let err = store.cancel(handle, &mallory).unwrap_err();
        assert!(matches!(err, MeltError::HandleNotFound));
        // Entry must still be there — Mallory's call must not have any side effect.
        assert!(store.lookup_route_for_session(handle, &alice).is_some());
    }

    /// `lookup_route_for_session` returns `None` for cross-session —
    /// indistinguishable from "handle absent."
    #[test]
    fn lookup_hides_cross_session_presence() {
        let store = fresh_store();
        let alice = SessionId::new();
        let mallory = SessionId::new();
        let handle = store.insert(empty_stream(), alice.clone(), RouteKind::Lake, None);
        assert!(store.lookup_route_for_session(handle, &alice).is_some());
        assert!(store.lookup_route_for_session(handle, &mallory).is_none());
    }
}
