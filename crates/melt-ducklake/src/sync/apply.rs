use std::time::Instant;

use duckdb::Connection;
use melt_core::{MeltError, Result, TableRef};
use melt_snowflake::ChangeStream;

use crate::schema::SyncReport;

/// Apply a `ChangeStream` to the lake transactionally.
///
/// Each Arrow batch is staged as a temp DuckDB view, then a 3-step
/// DML pair (delete-only, delete pre-images, insert post-images)
/// folds Snowflake's UPDATE-as-(DELETE,INSERT) pairing into a
/// single snapshot.
pub fn write_changes(
    conn: &mut Connection,
    table: &TableRef,
    stream: ChangeStream,
    started: Instant,
) -> Result<SyncReport> {
    conn.execute_batch("BEGIN")
        .map_err(|e| MeltError::backend(format!("BEGIN: {e}")))?;

    let snapshot = stream.end_snapshot;
    let mut rows_inserted = 0u64;
    let mut rows_updated = 0u64;
    let mut rows_deleted = 0u64;
    let mut bytes_written = 0u64;

    let qualified = format!("\"{}\".\"{}\"", table.schema, table.name);
    let qualified_schema = format!("\"{}\"", table.schema);

    let mut rows = stream.rows;
    loop {
        let next = futures::executor::block_on(futures::StreamExt::next(&mut rows));
        let Some(item) = next else { break };
        let change = match item {
            Ok(c) => c,
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(e);
            }
        };

        let row_count = change.batch.num_rows() as u64;
        bytes_written += change.batch.get_array_memory_size() as u64;
        if row_count == 0 {
            continue;
        }

        let view_name = format!("__melt_apply_{}", uuid_v4_short());
        if let Err(e) = stage_batch(conn, &view_name, &change.batch) {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }

        // Bootstrap: CREATE TABLE from staged view shape (zero rows);
        // keep `__row_id` (the merge key), drop `__action` (per-event label).
        let ddl = format!(
            "CREATE SCHEMA IF NOT EXISTS {qualified_schema}; \
             CREATE TABLE IF NOT EXISTS {qualified} AS \
                 SELECT * EXCLUDE (__action) FROM {view_name} WHERE 1=0;"
        );
        if let Err(e) = conn.execute_batch(&ddl) {
            let _ = conn.execute_batch(&format!("DROP VIEW IF EXISTS {view_name}"));
            let _ = conn.execute_batch("ROLLBACK");
            return Err(MeltError::backend(format!("ensure table exists: {e}")));
        }

        // ADD COLUMN for new source columns; drops/type changes/renames not auto-applied.
        if let Err(e) = reconcile_schema(conn, table, &change.batch) {
            let _ = conn.execute_batch(&format!("DROP VIEW IF EXISTS {view_name}"));
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }

        // 3-step DML: delete-only → delete pre-images of INSERTs →
        // insert post-images. Order avoids the old DELETE-sweep bug
        // where UPDATE pairs (shared `__row_id`) lost their fresh row.
        let upsert_sql = format!(
            "DELETE FROM {qualified} WHERE __row_id IN ( \
                SELECT __row_id FROM {view_name} WHERE __action = 'DELETE' \
                  AND __row_id NOT IN ( \
                    SELECT __row_id FROM {view_name} WHERE __action = 'INSERT' \
                  ) \
             ); \
             DELETE FROM {qualified} WHERE __row_id IN ( \
                SELECT __row_id FROM {view_name} WHERE __action = 'INSERT' \
             ); \
             INSERT INTO {qualified} SELECT * EXCLUDE (__action) FROM {view_name} \
                WHERE __action = 'INSERT'"
        );

        let exec = conn.execute_batch(&upsert_sql);
        let _ = conn.execute_batch(&format!("DROP VIEW IF EXISTS {view_name}"));
        if let Err(e) = exec {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(MeltError::backend(format!("apply: {e}")));
        }

        // Metrics from staged `__action` counts; UPDATE pair = +1 insert + 1 delete.
        let (inserts_in_batch, deletes_in_batch) = count_actions(&change.batch);
        rows_inserted += inserts_in_batch;
        rows_deleted += deletes_in_batch;
        if inserts_in_batch > 0 && inserts_in_batch == deletes_in_batch {
            rows_updated += inserts_in_batch;
        }
        let _ = change.action; // suppress unused-field warning
        let _ = row_count;
    }

    conn.execute_batch("COMMIT")
        .map_err(|e| MeltError::backend(format!("COMMIT: {e}")))?;

    Ok(SyncReport {
        table: table.clone(),
        snapshot,
        rows_inserted,
        rows_updated,
        rows_deleted,
        bytes_written,
        elapsed: started.elapsed(),
    })
}

/// `ALTER TABLE … ADD COLUMN` for every column in `batch` that the
/// lake table doesn't have yet. Caller has already ensured the lake
/// table exists.
///
/// Only handles additive changes. Drops, type changes, and renames
/// are intentionally left alone — drops would lose history, type
/// changes risk silent data corruption, renames are indistinguishable
/// from drop+add without source `columnId` tracking.
fn reconcile_schema(
    conn: &Connection,
    table: &TableRef,
    batch: &arrow::record_batch::RecordBatch,
) -> Result<()> {
    use std::collections::HashSet;

    // Inline literals are safe — `table.schema/name` come from our
    // own catalog, not user input.
    let probe_sql = format!(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = '{}' AND table_name = '{}'",
        table.schema.replace('\'', "''"),
        table.name.replace('\'', "''"),
    );
    let mut stmt = conn
        .prepare(&probe_sql)
        .map_err(|e| MeltError::backend(format!("schema probe prepare: {e}")))?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| MeltError::backend(format!("schema probe query: {e}")))?;
    let mut existing: HashSet<String> = HashSet::new();
    for r in rows {
        let name = r.map_err(|e| MeltError::backend(format!("schema probe row: {e}")))?;
        existing.insert(name);
    }

    let mut added: Vec<String> = Vec::new();
    let qualified = format!("\"{}\".\"{}\"", table.schema, table.name);
    for field in batch.schema().fields() {
        let name = field.name();
        // `__action` is staging metadata; never a lake column.
        // Case-insensitive because Snowflake uppercases to `__ACTION`.
        if name.eq_ignore_ascii_case("__action") {
            continue;
        }
        if existing.contains(name) {
            continue;
        }
        let sql_type = arrow_to_sql_type(field.data_type());
        let alter =
            format!("ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS \"{name}\" {sql_type}");
        conn.execute_batch(&alter).map_err(|e| {
            MeltError::backend(format!("schema evolve: ADD COLUMN {name} {sql_type}: {e}"))
        })?;
        added.push(format!("{name}:{sql_type}"));
    }

    if !added.is_empty() {
        tracing::info!(
            table = %table,
            columns_added = added.len(),
            columns = %added.join(","),
            "schema evolution: added columns to lake"
        );
    }
    Ok(())
}

/// Count INSERT / DELETE rows in the staged batch's `__action`
/// column. Used strictly for metrics; the SQL dispatched to DuckDB
/// pulls the same information via `WHERE __action = ...` predicates
/// so nothing about apply correctness depends on this being right.
fn count_actions(batch: &arrow::record_batch::RecordBatch) -> (u64, u64) {
    use arrow::array::{Array, StringArray};
    let Some(idx) = batch.schema().column_with_name("__action").map(|(i, _)| i) else {
        return (batch.num_rows() as u64, 0);
    };
    let Some(arr) = batch.column(idx).as_any().downcast_ref::<StringArray>() else {
        return (batch.num_rows() as u64, 0);
    };
    let mut inserts = 0u64;
    let mut deletes = 0u64;
    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }
        match arr.value(i) {
            "INSERT" => inserts += 1,
            "DELETE" => deletes += 1,
            _ => {}
        }
    }
    (inserts, deletes)
}

/// Register `batch` as a temporary view named `view_name`. We use
/// DuckDB's row-by-row APPENDER API rather than `register_arrow`
/// because that surface is feature-gated in the duckdb crate.
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

    // Single multi-row INSERT — sufficient for CDC batches in the
    // hundreds-to-low-thousands. For larger batches we'd switch to
    // DuckDB's Appender API.
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

fn arrow_to_sql_type(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::DataType::*;
    match dt {
        Boolean => "BOOLEAN".into(),
        Int8 | Int16 | Int32 => "INTEGER".into(),
        Int64 => "BIGINT".into(),
        UInt8 | UInt16 | UInt32 | UInt64 => "BIGINT".into(),
        Float16 | Float32 => "REAL".into(),
        Float64 => "DOUBLE".into(),
        Utf8 | LargeUtf8 => "VARCHAR".into(),
        Date32 | Date64 => "DATE".into(),
        // Naive vs zoned timestamps map distinctly; bootstrap pins this for the lake table.
        Timestamp(_, None) => "TIMESTAMP".into(),
        Timestamp(_, Some(_)) => "TIMESTAMPTZ".into(),
        // NUMBER(p,s) → DECIMAL(p,s); VARCHAR fallback would break math.
        Decimal128(p, s) => format!("DECIMAL({p}, {s})"),
        Decimal256(p, s) => format!("DECIMAL({p}, {s})"),
        Binary | LargeBinary | FixedSizeBinary(_) => "BLOB".into(),
        _ => "VARCHAR".into(),
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
    // Date32 → ISO literal (DuckDB rejects implicit `CAST(INT AS DATE)`).
    if let Some(arr) = col.as_any().downcast_ref::<Date32Array>() {
        let days = arr.value(idx);
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        let date = epoch + chrono::Duration::days(days as i64);
        return format!("DATE '{}'", date.format("%Y-%m-%d"));
    }
    // Timestamp(Nanos) → string literal (uniform across DuckDB versions).
    if let Some(arr) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        let total_ns = arr.value(idx);
        let secs = total_ns.div_euclid(1_000_000_000);
        let nanos = total_ns.rem_euclid(1_000_000_000) as u32;
        let dt = chrono::DateTime::from_timestamp(secs, nanos).unwrap_or_default();
        let tz_overlay = matches!(
            col.data_type(),
            arrow::datatypes::DataType::Timestamp(_, Some(_))
        );
        let body = dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string();
        return if tz_overlay {
            format!("TIMESTAMPTZ '{body}+00'")
        } else {
            format!("TIMESTAMP '{body}'")
        };
    }
    // Decimal128 → matching-scale literal so DuckDB re-parses as DECIMAL(p,s).
    if let Some(arr) = col.as_any().downcast_ref::<Decimal128Array>() {
        let value = arr.value(idx);
        let scale = match col.data_type() {
            arrow::datatypes::DataType::Decimal128(_, s) => *s,
            _ => 0,
        };
        return format_decimal128_literal(value, scale);
    }
    "NULL".to_string()
}

/// Format an Arrow Decimal128 unscaled i128 as a plain decimal SQL
/// literal. Examples:
///
/// * `(888000001, 0)` → `"888000001"`
/// * `(12345, 2)`     → `"123.45"`
/// * `(-425, 1)`      → `"-42.5"`
/// * `(5, 3)`         → `"0.005"` (leading zeros on the integer part)
///
/// No SQL quoting — DECIMAL literals are unquoted in DuckDB.
fn format_decimal128_literal(value: i128, scale: i8) -> String {
    if scale <= 0 {
        // Negative scale = value * 10^|scale|; emit as plain integer.
        let factor: i128 = 10i128.pow(scale.unsigned_abs() as u32);
        return value.saturating_mul(factor).to_string();
    }
    let scale = scale as usize;
    let neg = value < 0;
    let mag = if neg { (-value) as u128 } else { value as u128 };
    let digits = mag.to_string();
    let (int_part, frac_part) = if digits.len() <= scale {
        let pad = "0".repeat(scale - digits.len());
        ("0".to_string(), format!("{pad}{digits}"))
    } else {
        let split = digits.len() - scale;
        (digits[..split].to_string(), digits[split..].to_string())
    };
    let sign = if neg { "-" } else { "" };
    format!("{sign}{int_part}.{frac_part}")
}

fn sql_quote(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
}

fn uuid_v4_short() -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    id[..12].to_string()
}

#[cfg(test)]
mod decimal_tests {
    use super::format_decimal128_literal;

    #[test]
    fn scale_zero_big_integer() {
        assert_eq!(format_decimal128_literal(888_000_001, 0), "888000001");
    }

    #[test]
    fn scale_two_simple() {
        assert_eq!(format_decimal128_literal(12345, 2), "123.45");
    }

    #[test]
    fn scale_with_leading_zeros_in_frac() {
        assert_eq!(format_decimal128_literal(5, 3), "0.005");
    }

    #[test]
    fn negative_value() {
        assert_eq!(format_decimal128_literal(-425, 1), "-42.5");
    }

    #[test]
    fn scale_exceeds_digit_count() {
        assert_eq!(format_decimal128_literal(42, 5), "0.00042");
    }
}
