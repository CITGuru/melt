//! CSV fixture loader for the snapshot acceptance test (and any
//! future `--fixture-csv` flag we add to the binary). Mirrors the
//! columns the live Snowflake pull will return.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};

use crate::model::QueryHistoryRow;

/// Required CSV header set. Order doesn't matter; missing columns
/// fail the load loudly so we can't ship a fixture that silently
/// drops `EXECUTION_TIME`.
const REQUIRED: &[&str] = &[
    "QUERY_ID",
    "QUERY_TEXT",
    "START_TIME",
    "EXECUTION_TIME",
    "WAREHOUSE_SIZE",
    "BYTES_SCANNED",
];

/// Load a CSV file shaped like a `QUERY_HISTORY` export.
pub fn load_query_history_csv(path: &Path) -> Result<Vec<QueryHistoryRow>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let header_line = lines
        .next()
        .ok_or_else(|| anyhow!("empty fixture {}", path.display()))?
        .with_context(|| format!("reading header of {}", path.display()))?;
    let header = parse_csv_line(&header_line);
    let cols: Vec<&str> = header.iter().map(|s| s.as_str()).collect();
    for req in REQUIRED {
        if !cols.iter().any(|c| c.eq_ignore_ascii_case(req)) {
            return Err(anyhow!(
                "fixture {} missing required column {}",
                path.display(),
                req
            ));
        }
    }
    let idx = |name: &str| -> usize {
        cols.iter()
            .position(|c| c.eq_ignore_ascii_case(name))
            .expect("checked above")
    };
    let i_query_id = idx("QUERY_ID");
    let i_query_text = idx("QUERY_TEXT");
    let i_start_time = idx("START_TIME");
    let i_exec = idx("EXECUTION_TIME");
    let i_size = idx("WAREHOUSE_SIZE");
    let i_bytes = idx("BYTES_SCANNED");

    let mut out = Vec::new();
    for (lineno, line) in lines.enumerate() {
        let line =
            line.with_context(|| format!("reading row {} of {}", lineno + 2, path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let row = parse_csv_line(&line);
        let cell = |i: usize| row.get(i).cloned().unwrap_or_default();
        let start_time_str = cell(i_start_time);
        let start_time: DateTime<Utc> = start_time_str.parse().with_context(|| {
            format!(
                "parsing START_TIME `{start_time_str}` on row {} of {}",
                lineno + 2,
                path.display()
            )
        })?;
        let exec_str = cell(i_exec);
        let execution_time_ms: u64 = exec_str.parse().unwrap_or(0);
        let size = cell(i_size);
        let warehouse_size = if size.trim().is_empty() {
            None
        } else {
            Some(size)
        };
        let bytes_scanned: u64 = cell(i_bytes).parse().unwrap_or(0);
        out.push(QueryHistoryRow {
            query_id: cell(i_query_id),
            query_text: cell(i_query_text),
            start_time,
            execution_time_ms,
            warehouse_size,
            bytes_scanned,
        });
    }
    Ok(out)
}

/// Minimal CSV parser — handles `"`-quoted fields with `""`-escaped
/// quotes, comma separators, and bare fields. Good enough for the
/// fixture; we don't ship a generic CSV crate to keep the audit's
/// build cheap.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            let mut s = String::new();
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        s.push('"');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                s.push(bytes[i] as char);
                i += 1;
            }
            fields.push(s);
            if i < bytes.len() && bytes[i] == b',' {
                i += 1;
            }
        } else {
            let start = i;
            while i < bytes.len() && bytes[i] != b',' {
                i += 1;
            }
            fields.push(line[start..i].to_string());
            if i < bytes.len() && bytes[i] == b',' {
                i += 1;
            }
        }
    }
    // Trailing empty field if the line ends in `,`.
    if line.ends_with(',') {
        fields.push(String::new());
    }
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_quoted_with_embedded_commas() {
        let line = r#"a,"b,c","d""e",f"#;
        let cells = parse_csv_line(line);
        assert_eq!(cells, vec!["a", "b,c", "d\"e", "f"]);
    }
}
