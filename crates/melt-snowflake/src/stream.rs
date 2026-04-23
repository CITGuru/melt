use arrow::record_batch::RecordBatch;
use futures::stream::{self, BoxStream};
use melt_core::{Result, TableRef};
use serde::{Deserialize, Serialize};

/// Monotonic snapshot identifier within a single CDC stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct SnapshotId(pub i64);

/// Streaming CDC batches returned by `read_stream_since`.
pub struct ChangeStream {
    pub table: TableRef,
    pub start_snapshot: Option<SnapshotId>,
    pub end_snapshot: SnapshotId,
    pub rows: BoxStream<'static, Result<ChangeBatch>>,
}

impl ChangeStream {
    pub fn latest_snapshot(&self) -> SnapshotId {
        self.end_snapshot
    }

    pub fn empty(table: TableRef, start: Option<SnapshotId>, end: SnapshotId) -> Self {
        Self {
            table,
            start_snapshot: start,
            end_snapshot: end,
            rows: Box::pin(stream::empty()),
        }
    }
}

pub struct ChangeBatch {
    pub action: ChangeAction,
    pub batch: RecordBatch,
}

#[derive(Clone, Copy, Debug)]
pub enum ChangeAction {
    Insert,
    Update,
    Delete,
}

/// Inspect the leading `__action` column emitted by our standard
/// CDC stream consumer SQL and return the most common action in the
/// batch. Defaults to `Insert` when the column is absent or the
/// batch is empty — the apply path then treats it as a backfill.
pub fn infer_action(batch: &RecordBatch) -> ChangeAction {
    use arrow::array::{Array, StringArray};
    let Some(idx) = batch.schema().column_with_name("__action").map(|(i, _)| i) else {
        return ChangeAction::Insert;
    };
    let Some(arr) = batch.column(idx).as_any().downcast_ref::<StringArray>() else {
        return ChangeAction::Insert;
    };
    let mut inserts = 0u32;
    let mut updates = 0u32;
    let mut deletes = 0u32;
    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }
        match arr.value(i) {
            "INSERT" => inserts += 1,
            "UPDATE" | "UPDATE_AFTER" | "UPDATE_BEFORE" => updates += 1,
            "DELETE" => deletes += 1,
            _ => {}
        }
    }
    if deletes >= inserts && deletes >= updates {
        ChangeAction::Delete
    } else if updates > inserts {
        ChangeAction::Update
    } else {
        ChangeAction::Insert
    }
}
