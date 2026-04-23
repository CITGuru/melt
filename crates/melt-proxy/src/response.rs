use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub nullable: bool,
}

/// The Snowflake REST API returns rows as nested JSON arrays of
/// strings. Cell value is `null` for SQL NULL, otherwise the textual
/// representation. This is the well-documented "JSON row format" the
/// official drivers consume.
pub fn batches_to_partition(batches: &[RecordBatch]) -> Vec<Vec<Option<String>>> {
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    for batch in batches {
        let columns: Vec<&dyn Array> = batch.columns().iter().map(|c| c.as_ref()).collect();
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(columns.len());
            for col in &columns {
                if col.is_null(row_idx) {
                    row.push(None);
                } else {
                    row.push(Some(stringify_cell(*col, row_idx)));
                }
            }
            rows.push(row);
        }
    }
    rows
}

fn stringify_cell(col: &dyn Array, idx: usize) -> String {
    use arrow::array::*;
    macro_rules! cast {
        ($t:ty) => {
            if let Some(arr) = col.as_any().downcast_ref::<$t>() {
                return arr.value(idx).to_string();
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

    // Date32 → `YYYY-MM-DD` for the JSON rowset.
    if let Some(arr) = col.as_any().downcast_ref::<Date32Array>() {
        let days = arr.value(idx);
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        let date = epoch + chrono::Duration::days(days as i64);
        return date.format("%Y-%m-%d").to_string();
    }
    // Timestamp → human-readable ISO; drivers tolerate via DATE_FORMAT.
    if let Some(arr) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        let total_ns = arr.value(idx);
        let secs = total_ns.div_euclid(1_000_000_000);
        let nanos = total_ns.rem_euclid(1_000_000_000) as u32;
        let dt = chrono::DateTime::from_timestamp(secs, nanos).unwrap_or_default();
        return dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string();
    }
    // Decimal128 → string literal to preserve i128 precision.
    if let Some(arr) = col.as_any().downcast_ref::<Decimal128Array>() {
        let scale = match col.data_type() {
            arrow::datatypes::DataType::Decimal128(_, s) => *s,
            _ => 0,
        };
        return format_decimal128(arr.value(idx), scale);
    }

    format!("<unsupported {}>", col.data_type())
}

/// Same shape as `melt-ducklake/src/sync/apply.rs::format_decimal128_literal`
/// but unqualified — we emit a plain decimal string, not a DuckDB
/// literal with quotes. Duplicating the logic keeps the proxy free
/// of a reverse dependency on melt-ducklake.
fn format_decimal128(value: i128, scale: i8) -> String {
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

/// Snowflake-shaped statement response envelope. Mirrors the shape
/// the official REST API returns so drivers parse it without
/// modification.
#[derive(Clone, Debug, Serialize)]
pub struct StatementResponse {
    #[serde(rename = "resultSetMetaData")]
    pub metadata: ResultSetMetaData,
    pub data: Vec<Vec<Option<String>>>,
    #[serde(rename = "statementHandle")]
    pub statement_handle: String,
    #[serde(rename = "statementStatusUrl")]
    pub status_url: String,
    #[serde(rename = "createdOn")]
    pub created_on: i64,
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResultSetMetaData {
    #[serde(rename = "numRows")]
    pub num_rows: usize,
    pub format: String,
    #[serde(rename = "rowType")]
    pub row_type: Vec<ColumnMeta>,
    pub partition_info: Vec<PartitionInfo>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PartitionInfo {
    #[serde(rename = "rowCount")]
    pub row_count: usize,
    #[serde(rename = "uncompressedSize")]
    pub uncompressed_size: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub success: bool,
    #[serde(rename = "errorCode", skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(rename = "sqlState", skip_serializing_if = "Option::is_none")]
    pub sql_state: Option<String>,
}

impl ErrorResponse {
    pub fn snowflake(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            success: false,
            error_code: Some(code.into()),
            sql_state: None,
        }
    }
}
