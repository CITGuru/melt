//! `melt debug {count,rows}` — inspection commands for verifying
//! sync correctness.
//!
//! The sync subsystem drives state through several stages (pending →
//! bootstrapping → active, or → quarantined on failure) and the
//! state machine alone doesn't prove the lake copy matches the
//! Snowflake source. These commands issue the same query against
//! both sides and print a diff, so "sync succeeded" becomes a
//! verifiable claim rather than trust in the catalog.
//!
//! Intentional non-features: no caching, no streaming across large
//! tables, no sampling heuristics. The point is an easy-to-read
//! comparison for small-to-medium tables. For anything >1M rows,
//! pick a filtered subset and compare that instead.

use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use clap::Subcommand;
use futures::StreamExt;
use melt_core::{QueryContext, SessionInfo, StorageBackend, TableRef};
use melt_snowflake::SnowflakeClient;

use crate::config::{ActiveBackend, MeltConfig};

#[derive(Subcommand, Debug)]
pub enum DebugAction {
    /// Compare `SELECT COUNT(*)` between Snowflake and the Lake
    /// backend for a single table. Exits non-zero if the counts
    /// differ so this can gate CI.
    Count {
        /// Fully qualified table name, e.g. `DB.SCHEMA.TABLE`.
        fqn: String,
    },

    /// Fetch `LIMIT N` rows from both sides with a stable ordering
    /// and print them side-by-side. Useful for catching schema
    /// mismatches and stale-data bugs the row count alone can't
    /// surface.
    Rows {
        fqn: String,
        /// How many rows to pull from each side. Both sides use the
        /// same limit so differences in physical ordering don't
        /// masquerade as divergence.
        #[arg(long, default_value_t = 10)]
        limit: u32,
        /// Column to `ORDER BY`. Required because physical row order
        /// isn't guaranteed in either Snowflake or DuckDB; picking a
        /// deterministic column makes the comparison meaningful.
        #[arg(long)]
        order_by: String,
    },
}

pub async fn run(cfg_path: &Path, action: DebugAction) -> Result<()> {
    let cfg = MeltConfig::load(cfg_path)
        .with_context(|| format!("reading config at {}", cfg_path.display()))?;
    match action {
        DebugAction::Count { fqn } => count(&cfg, &fqn).await,
        DebugAction::Rows {
            fqn,
            limit,
            order_by,
        } => rows(&cfg, &fqn, limit, &order_by).await,
    }
}

async fn count(cfg: &MeltConfig, fqn_str: &str) -> Result<()> {
    let fqn = parse_fqn(fqn_str)?;
    let sf_qualified = quote_fqn_snowflake(&fqn);
    let lake_qualified = quote_fqn_lake(&fqn);

    let (sf_count, lake_count) = tokio::try_join!(
        snowflake_count(cfg, &sf_qualified),
        lake_count(cfg, &lake_qualified),
    )?;

    println!("table:       {fqn}");
    println!("snowflake:   {sf_count}");
    println!("lake:        {lake_count}");
    match sf_count.cmp(&lake_count) {
        std::cmp::Ordering::Equal => {
            println!("diff:        0  ✓ match");
        }
        std::cmp::Ordering::Greater => {
            let diff = sf_count - lake_count;
            println!("diff:        {diff}  (lake is behind by {diff} rows)");
            std::process::exit(1);
        }
        std::cmp::Ordering::Less => {
            let diff = lake_count - sf_count;
            println!("diff:        -{diff}  (lake has {diff} rows Snowflake doesn't — likely stale sync or mid-transaction)");
            std::process::exit(1);
        }
    }
    Ok(())
}

async fn snowflake_count(cfg: &MeltConfig, qualified: &str) -> Result<u64> {
    let client = Arc::new(SnowflakeClient::new(cfg.snowflake.clone()));
    let token = client
        .service_token()
        .await
        .context("acquiring Snowflake service token")?;
    let sql = format!("SELECT COUNT(*) AS n FROM {qualified}");
    let stream = client
        .execute_arrow(&token, &sql)
        .await
        .context("Snowflake execute_arrow COUNT(*)")?;
    drain_first_cell_u64(stream)
        .await
        .context("Snowflake count")
}

async fn lake_count(cfg: &MeltConfig, qualified: &str) -> Result<u64> {
    let backend = build_backend(cfg).await?;
    let ctx = QueryContext::from_session(&dummy_session());
    let sql = format!("SELECT COUNT(*) AS n FROM {qualified}");
    let stream = backend
        .execute(&sql, &ctx)
        .await
        .context("Lake execute COUNT(*)")?;
    drain_first_cell_u64(stream).await.context("Lake count")
}

async fn rows(cfg: &MeltConfig, fqn_str: &str, limit: u32, order_by: &str) -> Result<()> {
    let fqn = parse_fqn(fqn_str)?;
    // Snowflake side uses the full 3-part FQN. The lake side uses only
    // `"SCHEMA"."TABLE"` because the DuckDB connection already does
    // `USE lake` on setup; sync writes tables into that attached
    // catalog without the source database prefix. Keeping two SQL
    // strings is the smallest way to honour both conventions.
    let sf_sql = format!(
        "SELECT * FROM {} ORDER BY {} LIMIT {}",
        quote_fqn_snowflake(&fqn),
        quote_ident(order_by),
        limit
    );
    // Strip `__row_id` (melt's internal merge key) — proxy queries
    // get this from the `hide_internal` translator pass; debug calls
    // DuckDB directly so we do it inline here.
    let lake_sql = format!(
        "SELECT * EXCLUDE (__row_id) FROM {} ORDER BY {} LIMIT {}",
        quote_fqn_lake(&fqn),
        quote_ident(order_by),
        limit
    );

    let (sf_rows, lake_rows) =
        tokio::try_join!(snowflake_rows(cfg, &sf_sql), lake_rows(cfg, &lake_sql),)?;

    println!("table:       {fqn}");
    println!("ordered by:  {order_by}");
    println!("limit:       {limit}");
    println!();
    print_side_by_side(&sf_rows, &lake_rows);

    let matches = sf_rows.schema == lake_rows.schema && sf_rows.cells == lake_rows.cells;
    if matches {
        println!(
            "\n✓ rows match ({} rows, {} columns)",
            sf_rows.cells.len(),
            sf_rows.schema.len()
        );
        Ok(())
    } else {
        println!("\n✗ rows differ");
        std::process::exit(1);
    }
}

async fn snowflake_rows(cfg: &MeltConfig, sql: &str) -> Result<SimpleRows> {
    let client = Arc::new(SnowflakeClient::new(cfg.snowflake.clone()));
    let token = client.service_token().await?;
    let stream = client.execute_arrow(&token, sql).await?;
    collect_simple_rows(stream).await.context("Snowflake rows")
}

async fn lake_rows(cfg: &MeltConfig, sql: &str) -> Result<SimpleRows> {
    let backend = build_backend(cfg).await?;
    let ctx = QueryContext::from_session(&dummy_session());
    let stream = backend.execute(sql, &ctx).await?;
    collect_simple_rows(stream).await.context("Lake rows")
}

async fn build_backend(cfg: &MeltConfig) -> Result<Arc<dyn StorageBackend>> {
    match cfg.active_backend()? {
        #[cfg(feature = "ducklake")]
        ActiveBackend::DuckLake(dl) => {
            use melt_ducklake::{CatalogClient, DuckLakeBackend, DuckLakePool};
            let catalog = Arc::new(
                CatalogClient::connect(&dl.catalog_url)
                    .await
                    .context("connecting to ducklake catalog")?,
            );
            let pool = Arc::new(
                DuckLakePool::new(dl)
                    .await
                    .context("opening DuckLake pool")?,
            );
            Ok(Arc::new(DuckLakeBackend::from_parts(catalog, pool)))
        }
        #[cfg(feature = "iceberg")]
        ActiveBackend::Iceberg(ib) => {
            use melt_iceberg::{IcebergBackend, IcebergCatalogClient, IcebergPool};
            let catalog = Arc::new(
                IcebergCatalogClient::connect(&ib)
                    .await
                    .context("connecting to iceberg catalog")?,
            );
            catalog.assert_supported()?;
            let pool = Arc::new(
                IcebergPool::new(&ib)
                    .await
                    .context("opening Iceberg pool")?,
            );
            Ok(Arc::new(IcebergBackend::from_parts(catalog, pool)))
        }
    }
}

async fn drain_first_cell_u64(mut stream: melt_core::RecordBatchStream) -> Result<u64> {
    while let Some(batch) = stream.as_mut().next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        let col = batch.column(0);
        return cell_to_u64(col.as_ref(), 0);
    }
    Err(anyhow!("query returned no rows"))
}

/// A minimal in-memory mirror of an Arrow result so `count` / `rows`
/// can compare across backends without pulling both sides through
/// Arrow-aware diffing. Cells are stringified in the dialects'
/// native forms; if that stops being good enough we'll add typed
/// comparison.
struct SimpleRows {
    schema: Vec<String>,
    cells: Vec<Vec<String>>,
}

async fn collect_simple_rows(mut stream: melt_core::RecordBatchStream) -> Result<SimpleRows> {
    let mut schema: Vec<String> = Vec::new();
    let mut cells: Vec<Vec<String>> = Vec::new();
    while let Some(batch) = stream.as_mut().next().await {
        let batch = batch?;
        if schema.is_empty() {
            schema = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect();
        }
        for row in 0..batch.num_rows() {
            let mut r = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                r.push(cell_to_string(batch.column(col_idx).as_ref(), row));
            }
            cells.push(r);
        }
    }
    Ok(SimpleRows { schema, cells })
}

fn cell_to_u64(col: &dyn Array, idx: usize) -> Result<u64> {
    macro_rules! cast_u {
        ($t:ty) => {
            if let Some(a) = col.as_any().downcast_ref::<$t>() {
                if a.is_null(idx) {
                    return Err(anyhow!("first cell is NULL"));
                }
                return Ok(a.value(idx) as u64);
            }
        };
    }
    cast_u!(Int8Array);
    cast_u!(Int16Array);
    cast_u!(Int32Array);
    cast_u!(Int64Array);
    cast_u!(UInt8Array);
    cast_u!(UInt16Array);
    cast_u!(UInt32Array);
    cast_u!(UInt64Array);
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        if a.is_null(idx) {
            return Err(anyhow!("first cell is NULL"));
        }
        return a
            .value(idx)
            .parse::<u64>()
            .map_err(|e| anyhow!("first cell {:?} not u64: {e}", a.value(idx)));
    }
    Err(anyhow!(
        "COUNT(*) returned unexpected type {:?}",
        col.data_type()
    ))
}

fn cell_to_string(col: &dyn Array, idx: usize) -> String {
    use arrow::datatypes::{DataType, TimeUnit};
    if col.is_null(idx) {
        return "<NULL>".to_string();
    }
    macro_rules! cast {
        ($t:ty) => {
            if let Some(a) = col.as_any().downcast_ref::<$t>() {
                return a.value(idx).to_string();
            }
        };
    }
    cast!(StringArray);
    cast!(LargeStringArray);
    cast!(Int8Array);
    cast!(Int16Array);
    cast!(Int32Array);
    cast!(Int64Array);
    cast!(UInt8Array);
    cast!(UInt16Array);
    cast!(UInt32Array);
    cast!(UInt64Array);
    cast!(Float32Array);
    cast!(Float64Array);
    cast!(BooleanArray);

    // Render Date/Timestamp/Decimal to normalised strings so SF
    // (ns precision) and Lake (µs precision) line up byte-for-byte
    // when the value matches.
    if let Some(a) = col.as_any().downcast_ref::<Date32Array>() {
        let days = a.value(idx);
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        return (epoch + chrono::Duration::days(days as i64))
            .format("%Y-%m-%d")
            .to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        let ns = a.value(idx);
        let secs = ns.div_euclid(1_000_000_000);
        let nanos = ns.rem_euclid(1_000_000_000) as u32;
        if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
            return trim_trailing_zeros(
                &dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
            );
        }
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let us = a.value(idx);
        let secs = us.div_euclid(1_000_000);
        let nanos = (us.rem_euclid(1_000_000) as u32) * 1_000;
        if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
            return trim_trailing_zeros(
                &dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
            );
        }
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
        let ms = a.value(idx);
        let secs = ms.div_euclid(1_000);
        let nanos = (ms.rem_euclid(1_000) as u32) * 1_000_000;
        if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
            return trim_trailing_zeros(
                &dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
            );
        }
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampSecondArray>() {
        if let Some(dt) = chrono::DateTime::from_timestamp(a.value(idx), 0) {
            return dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string();
        }
    }
    if let Some(a) = col.as_any().downcast_ref::<Decimal128Array>() {
        let scale = match col.data_type() {
            DataType::Decimal128(_, s) => *s,
            _ => 0,
        };
        return format_decimal128_human(a.value(idx), scale);
    }

    let _ = TimeUnit::Nanosecond;
    format!("<{}>", col.data_type())
}

/// Trim trailing zeros from the fractional-seconds suffix so SF
/// (ns) and Lake (µs) timestamps render identically when the
/// underlying value matches.
fn trim_trailing_zeros(s: &str) -> String {
    if !s.contains('.') {
        return s.to_string();
    }
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// Plain decimal literal, no SQL quoting.
fn format_decimal128_human(value: i128, scale: i8) -> String {
    if scale <= 0 {
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

fn print_side_by_side(sf: &SimpleRows, lake: &SimpleRows) {
    if sf.schema != lake.schema {
        println!("column names differ:");
        println!("  snowflake: {:?}", sf.schema);
        println!("  lake:      {:?}", lake.schema);
        println!();
    }
    let n = sf.cells.len().max(lake.cells.len());
    for i in 0..n {
        let sf_row = sf.cells.get(i).map(|r| r.join(" | ")).unwrap_or_default();
        let lake_row = lake.cells.get(i).map(|r| r.join(" | ")).unwrap_or_default();
        let marker = if sf_row == lake_row { ' ' } else { '!' };
        println!("{marker} sf  : {sf_row}");
        println!("{marker} lake: {lake_row}");
    }
}

fn parse_fqn(fqn: &str) -> Result<TableRef> {
    let parts: Vec<&str> = fqn.split('.').collect();
    if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
        return Err(anyhow!("FQN must be DB.SCHEMA.TABLE, got {fqn:?}"));
    }
    Ok(TableRef::new(
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
    ))
}

/// Snowflake form: `"DB"."SCHEMA"."TABLE"`. Three-part name required
/// since the sync session doesn't necessarily have a default database
/// context matching `t.database`.
fn quote_fqn_snowflake(t: &TableRef) -> String {
    format!(
        "{}.{}.{}",
        quote_ident(&t.database),
        quote_ident(&t.schema),
        quote_ident(&t.name),
    )
}

/// Lake form: `"SCHEMA"."TABLE"`. The DuckDB connection has already
/// executed `USE lake;` during pool setup, and sync writes tables
/// into the attached lake catalog without the source database
/// prefix. Using the 3-part name here makes DuckDB look up the
/// database component as a catalog — which doesn't exist — and fail
/// with "Catalog `<db>` does not exist".
fn quote_fqn_lake(t: &TableRef) -> String {
    format!("{}.{}", quote_ident(&t.schema), quote_ident(&t.name))
}

/// Quote an identifier for both Snowflake and DuckDB. Both dialects
/// accept double-quoted, case-sensitive identifiers. Any embedded `"`
/// is escaped by doubling, matching the SQL standard.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Synthetic session for the Lake `execute` call. The backend only
/// uses this for tracing context — there's no real proxy session in
/// the `melt debug` path.
fn dummy_session() -> Arc<SessionInfo> {
    Arc::new(SessionInfo::new("melt-debug", 1))
}
