//! CDC apply for Iceberg. Drains a `ChangeStream` through DuckDB into
//! the Iceberg catalog attached as `ice` (see `pool.rs`). INSERTs go
//! via `INSERT INTO ice.t`, UPDATEs as delete-then-insert on `id`,
//! DELETEs via `DELETE FROM ice.t`. duckdb-iceberg owns the Parquet
//! write and the manifest commit — no hand-rolled JSON here.
//!
//! Staging shape mirrors `melt-ducklake/src/sync/apply.rs`. The helpers
//! are duplicated rather than shared because (a) the two backends
//! should stay independently explorable and (b) sharing them would
//! require a third crate neither side would otherwise depend on.

use std::time::Instant;

use duckdb::Connection;
use futures::StreamExt;
use melt_core::{MeltError, Result, TableRef};
use melt_snowflake::{ChangeAction, ChangeStream};

use crate::config::{IcebergCatalogKind, IcebergConfig};

/// Apply a `ChangeStream` to the Iceberg catalog attached as `ice`.
/// Returns the total number of rows affected across INSERT / UPDATE /
/// DELETE dispatches. The entire batch lands inside one transaction
/// so a mid-stream error rolls back and the Snowflake snapshot
/// pointer is not advanced.
pub fn write_changes(
    cfg: &IcebergConfig,
    conn: &mut Connection,
    table: &TableRef,
    stream: ChangeStream,
    _started: Instant,
) -> Result<u64> {
    if !matches!(
        cfg.catalog,
        IcebergCatalogKind::Rest | IcebergCatalogKind::Polaris
    ) {
        return Err(MeltError::BackendUnavailable(
            "Iceberg sync writes require a REST-compatible catalog \
             (catalog = \"rest\" or \"polaris\"). For Glue, front it \
             with a REST shim."
                .into(),
        ));
    }

    conn.execute_batch("BEGIN")
        .map_err(|e| MeltError::backend(format!("BEGIN: {e}")))?;

    let qualified = format!("ice.\"{}\".\"{}\"", table.schema, table.name);
    let mut rows_affected = 0u64;

    let mut rows = stream.rows;
    loop {
        let next = futures::executor::block_on(StreamExt::next(&mut rows));
        let Some(item) = next else { break };
        let change = match item {
            Ok(c) => c,
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(e);
            }
        };

        let row_count = change.batch.num_rows() as u64;
        if row_count == 0 {
            continue;
        }

        let view_name = format!("__melt_ice_apply_{}", uuid_v4_short());
        if let Err(e) = stage_batch(conn, &view_name, &change.batch) {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }

        let dispatch = match change.action {
            ChangeAction::Insert => {
                format!("INSERT INTO {qualified} SELECT * EXCLUDE (__action) FROM {view_name}")
            }
            ChangeAction::Update => format!(
                "DELETE FROM {qualified} WHERE id IN (SELECT id FROM {view_name}); \
                 INSERT INTO {qualified} SELECT * EXCLUDE (__action) FROM {view_name}"
            ),
            ChangeAction::Delete => {
                format!("DELETE FROM {qualified} WHERE id IN (SELECT id FROM {view_name})")
            }
        };

        let exec = conn.execute_batch(&dispatch);
        let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {view_name}"));
        if let Err(e) = exec {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(MeltError::backend(format!("iceberg apply: {e}")));
        }

        rows_affected += row_count;
    }

    conn.execute_batch("COMMIT")
        .map_err(|e| MeltError::backend(format!("COMMIT: {e}")))?;

    Ok(rows_affected)
}

fn stage_batch(
    conn: &Connection,
    view_name: &str,
    batch: &arrow::record_batch::RecordBatch,
) -> Result<()> {
    let columns: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| format!("\"{}\" {}", f.name(), arrow_to_sql_type(f.data_type())))
        .collect();
    let create = format!("CREATE TEMP TABLE {view_name} ({})", columns.join(", "));
    conn.execute_batch(&create)
        .map_err(|e| MeltError::backend(format!("staging table create: {e}")))?;

    if batch.num_rows() == 0 {
        return Ok(());
    }

    let mut values_clauses = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut row_vals = Vec::with_capacity(batch.num_columns());
        for col in batch.columns() {
            row_vals.push(stringify(col.as_ref(), row));
        }
        values_clauses.push(format!("({})", row_vals.join(", ")));
    }
    let insert = format!(
        "INSERT INTO {view_name} VALUES {}",
        values_clauses.join(", ")
    );
    conn.execute_batch(&insert)
        .map_err(|e| MeltError::backend(format!("staging insert: {e}")))?;
    Ok(())
}

fn arrow_to_sql_type(dt: &arrow::datatypes::DataType) -> &'static str {
    use arrow::datatypes::DataType::*;
    match dt {
        Boolean => "BOOLEAN",
        Int8 | Int16 | Int32 => "INTEGER",
        Int64 => "BIGINT",
        UInt8 | UInt16 | UInt32 | UInt64 => "BIGINT",
        Float16 | Float32 => "REAL",
        Float64 => "DOUBLE",
        Utf8 | LargeUtf8 => "VARCHAR",
        Date32 | Date64 => "DATE",
        Timestamp(_, _) => "TIMESTAMP",
        _ => "VARCHAR",
    }
}

fn stringify(col: &dyn arrow::array::Array, idx: usize) -> String {
    use arrow::array::*;
    if col.is_null(idx) {
        return "NULL".to_string();
    }
    macro_rules! cast_num {
        ($t:ty) => {
            if let Some(arr) = col.as_any().downcast_ref::<$t>() {
                return arr.value(idx).to_string();
            }
        };
    }
    cast_num!(Int8Array);
    cast_num!(Int16Array);
    cast_num!(Int32Array);
    cast_num!(Int64Array);
    cast_num!(UInt8Array);
    cast_num!(UInt16Array);
    cast_num!(UInt32Array);
    cast_num!(UInt64Array);
    cast_num!(Float32Array);
    cast_num!(Float64Array);
    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
        return arr.value(idx).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return sql_quote(arr.value(idx));
    }
    if let Some(arr) = col.as_any().downcast_ref::<LargeStringArray>() {
        return sql_quote(arr.value(idx));
    }
    "NULL".to_string()
}

fn sql_quote(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
}

fn uuid_v4_short() -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    id[..12].to_string()
}
