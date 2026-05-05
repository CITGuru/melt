//! `melt audit share` — opt-in upload path. Spec §3.3 / §5.
//!
//! Default-local stays default-local: the share subcommand is the
//! *only* code path in this crate that initiates network egress to a
//! Melt-owned endpoint, and it never runs without an explicit
//! `share` invocation. Even then, the operator sees the exact bytes
//! about to leave the box and confirms before the POST happens.
//!
//! Reduction pipeline (in order):
//!
//! 1. Load the prior `melt audit` JSON artifact from disk. Only this
//!    one file is read — the sibling `*.talkingpoints.md` is never
//!    opened, because operators sometimes paste real numbers into it.
//! 2. Apply the share-time redaction pass on top of the literal
//!    redaction `crate::redact` already did at audit time:
//!    - trim `top_patterns[].pattern_redacted` to a verb + table FQN
//!      shape (`SELECT … FROM A.B.C …`) so the predicate column
//!      list and join structure don't leave the box;
//!    - with `--anonymize` (default for `share`), strip the table
//!      identifier below the schema level
//!      (`ANALYTICS.PUBLIC.EVENTS` → `ANALYTICS.PUBLIC.<redacted>`).
//! 3. Print the redacted JSON and prompt `Upload? [y/N]` unless
//!    `--yes` is set.
//! 4. POST to the configured endpoint and print the short URL.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use clap::Args;
use serde::Deserialize;
use serde_json::Value;

/// Default upload endpoint. Overridable via `--endpoint` so the
/// integration test can target a local mock server.
pub const DEFAULT_SHARE_ENDPOINT: &str = "https://getmelt.com/audit/share";

/// `melt audit share` argument bag.
#[derive(Args, Debug, Clone)]
pub struct ShareArgs {
    /// Path to a prior `melt audit` JSON artifact, or a directory
    /// containing one. When omitted, the most recent
    /// `melt-audit-*.json` in `--out-dir` is used.
    pub json_path: Option<PathBuf>,

    /// Skip the `Upload? [y/N]` confirmation prompt. Required for
    /// CI; intentionally not the default.
    #[arg(long)]
    pub yes: bool,

    /// Strip the table identifier below the schema level from every
    /// `top_patterns[].table_fqn` and the trimmed `pattern_redacted`.
    /// Default for the share path; pass `--anonymize=false` to
    /// keep table names (e.g. when sharing internally).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub anonymize: bool,

    /// Override the upload endpoint. Defaults to
    /// `https://getmelt.com/audit/share`.
    #[arg(long, default_value = DEFAULT_SHARE_ENDPOINT)]
    pub endpoint: String,

    /// Directory used to (a) locate the most recent JSON when no
    /// path is supplied and (b) write the `*-shared.json` artifact
    /// recording exactly what was uploaded.
    #[arg(long, default_value = ".")]
    pub out_dir: PathBuf,
}

/// Wire shape returned by `getmelt.com/audit/share`. Field set kept
/// minimal so a future endpoint can extend without breaking us.
#[derive(Debug, Clone, Deserialize)]
struct ShareResponse {
    url: String,
}

/// Drive the share subcommand to an exit code.
pub fn run(args: ShareArgs) -> u8 {
    match run_inner(args) {
        Ok(_) => 0,
        Err(ShareError::Cancelled) => {
            eprintln!("upload cancelled.");
            // Cancellation is a normal CI/operator outcome, not a
            // failure — exit clean so a `--yes`-less prompt that the
            // user declines doesn't fail-stop the surrounding script.
            0
        }
        Err(ShareError::Usage(e)) => {
            eprintln!("error: {e}");
            2
        }
        Err(ShareError::Runtime(e)) => {
            eprintln!("melt audit share: {e:#}");
            1
        }
    }
}

enum ShareError {
    Cancelled,
    Usage(anyhow::Error),
    Runtime(anyhow::Error),
}

fn run_inner(args: ShareArgs) -> Result<(), ShareError> {
    let json_path = resolve_json_path(args.json_path.as_deref(), &args.out_dir)
        .map_err(ShareError::Usage)?;
    let raw = std::fs::read_to_string(&json_path).map_err(|e| {
        ShareError::Runtime(anyhow::anyhow!(
            "read audit JSON from {}: {e}",
            json_path.display()
        ))
    })?;
    let parsed: Value = serde_json::from_str(&raw).map_err(|e| {
        ShareError::Runtime(anyhow::anyhow!(
            "parse audit JSON at {} (was this produced by `melt audit`?): {e}",
            json_path.display()
        ))
    })?;
    let redacted = redact_for_share(parsed, args.anonymize);
    let payload = serde_json::to_string_pretty(&redacted)
        .expect("Value is always serializable to JSON");

    if !args.yes && !confirm_upload(&payload, &args.endpoint)? {
        return Err(ShareError::Cancelled);
    }

    let url = upload(&args.endpoint, &payload).map_err(ShareError::Runtime)?;

    // Receipt is written *after* a successful upload so the artifact
    // records exactly what left the box — a failed upload leaves no
    // misleading "shared" sidecar on disk.
    let shared_path = sibling_shared_path(&json_path, &args.out_dir);
    std::fs::create_dir_all(&args.out_dir).map_err(|e| {
        ShareError::Runtime(anyhow::anyhow!(
            "create --out-dir {}: {e}",
            args.out_dir.display()
        ))
    })?;
    std::fs::write(&shared_path, &payload).map_err(|e| {
        ShareError::Runtime(anyhow::anyhow!(
            "write share artifact to {}: {e}",
            shared_path.display()
        ))
    })?;

    println!("✓ Uploaded {} bytes", payload.len());
    println!("  Endpoint: {}", args.endpoint);
    println!("  Receipt:  {}", shared_path.display());
    println!();
    println!("{url}");
    Ok(())
}

/// Pick the JSON the operator wants uploaded. Three accepted shapes:
///
/// * explicit file path
/// * explicit directory → newest `melt-audit-*.json` inside it
/// * `None` → newest `melt-audit-*.json` in `out_dir`
pub fn resolve_json_path(
    explicit: Option<&Path>,
    out_dir: &Path,
) -> anyhow::Result<PathBuf> {
    if let Some(p) = explicit {
        if p.is_dir() {
            return newest_audit_json(p);
        }
        if !p.exists() {
            anyhow::bail!("audit JSON not found: {}", p.display());
        }
        return Ok(p.to_path_buf());
    }
    newest_audit_json(out_dir)
}

fn newest_audit_json(dir: &Path) -> anyhow::Result<PathBuf> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| anyhow::anyhow!("read --out-dir {}: {e}", dir.display()))?;
    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        if !is_share_candidate(&path) {
            continue;
        }
        let mtime = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        match &best {
            None => best = Some((mtime, path)),
            Some((cur, _)) if mtime > *cur => best = Some((mtime, path)),
            _ => {}
        }
    }
    best.map(|(_, p)| p).ok_or_else(|| {
        anyhow::anyhow!(
            "no melt-audit-*.json found in {} — pass an explicit <json-path> \
             or run `melt audit` first",
            dir.display()
        )
    })
}

fn is_share_candidate(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
        return false;
    };
    // The audit binary writes `melt-audit-<acct>-<date>.json` and a
    // sibling `melt-audit-<acct>-<date>.talkingpoints.md`. The share
    // path NEVER reads the talking-points file (operators sometimes
    // paste real numbers into it), and skips any prior `*-shared.json`
    // receipt to avoid re-sharing an already-redacted artifact.
    name.starts_with("melt-audit-")
        && name.ends_with(".json")
        && !name.ends_with("-shared.json")
        && !name.contains(".talkingpoints")
}

fn sibling_shared_path(source: &Path, out_dir: &Path) -> PathBuf {
    let stem = source
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("melt-audit-share");
    out_dir.join(format!("{stem}-shared.json"))
}

/// Apply the share-time redaction pass to a raw audit-output value.
/// Pure function over `serde_json::Value` so the unit tests can pin
/// the exact JSON shape that leaves the box.
pub fn redact_for_share(mut v: Value, anonymize: bool) -> Value {
    let Some(obj) = v.as_object_mut() else {
        return v;
    };

    // Operators sometimes inline talking-points in custom forks; if a
    // future caller stuffs a `talking_points` field into the JSON,
    // strip it here as a defense in depth. The default audit
    // pipeline never writes such a field — the test in `tests/share.rs`
    // pins this guarantee.
    obj.remove("talking_points");

    if let Some(patterns) = obj.get_mut("top_patterns").and_then(|p| p.as_array_mut()) {
        for entry in patterns.iter_mut() {
            redact_pattern_entry(entry, anonymize);
        }
    }
    v
}

fn redact_pattern_entry(entry: &mut Value, anonymize: bool) {
    let Some(obj) = entry.as_object_mut() else {
        return;
    };

    let original_pattern = obj
        .get("pattern_redacted")
        .and_then(|p| p.as_str())
        .unwrap_or("")
        .to_string();
    let original_table = obj
        .get("table_fqn")
        .and_then(|p| p.as_str())
        .unwrap_or("")
        .to_string();

    let table_out = if anonymize {
        anonymize_table(&original_table)
    } else {
        original_table.clone()
    };

    let trimmed = trim_pattern(&original_pattern, &original_table, &table_out);

    if let Some(slot) = obj.get_mut("pattern_redacted") {
        *slot = Value::String(trimmed);
    }
    if anonymize {
        if let Some(slot) = obj.get_mut("table_fqn") {
            *slot = Value::String(table_out);
        }
    }
}

/// Replace the table-level identifier (last `.`-segment) with
/// `<redacted>`. Schema-and-up are preserved so prospects can still
/// reason about which database/schema is in scope.
fn anonymize_table(fqn: &str) -> String {
    let segs: Vec<&str> = fqn.split('.').collect();
    if segs.len() <= 1 {
        return "<redacted>".to_string();
    }
    let mut out = segs[..segs.len() - 1].join(".");
    out.push_str(".<redacted>");
    out
}

/// Trim a fully-redacted pattern like
/// `SELECT col, count(*) FROM A.B.C WHERE x=? GROUP BY col`
/// down to `SELECT … FROM A.B.C …` (or `… FROM A.B.<redacted> …` when
/// anonymizing). The trailing `…` is omitted when the original
/// pattern has no clauses after the table reference.
fn trim_pattern(pattern: &str, original_table: &str, table_out: &str) -> String {
    let trimmed = pattern.trim();
    let verb = trimmed
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_string();
    if verb.is_empty() {
        return format!("… FROM {table_out}");
    }

    let has_tail = pattern_has_tail_after_table(pattern, original_table);
    if has_tail {
        format!("{verb} … FROM {table_out} …")
    } else {
        format!("{verb} … FROM {table_out}")
    }
}

fn pattern_has_tail_after_table(pattern: &str, original_table: &str) -> bool {
    if original_table.is_empty() {
        // Without a table to anchor on, prefer the safer "tail
        // exists" projection — the verb is still trimmed, and the
        // trailing `…` correctly suggests there's more SQL we're
        // not surfacing.
        return true;
    }
    let upper = pattern.to_ascii_uppercase();
    let needle = original_table.to_ascii_uppercase();
    let Some(idx) = upper.find(&needle) else {
        return true;
    };
    let after = pattern[idx + needle.len()..].trim();
    !after.is_empty()
}

fn confirm_upload(payload: &str, endpoint: &str) -> Result<bool, ShareError> {
    println!("---");
    println!("About to upload the following bytes to {endpoint}:");
    println!("---");
    println!("{payload}");
    println!("---");
    print!("Upload? [y/N] ");
    io::stdout().flush().ok();

    let mut answer = String::new();
    io::stdin().read_line(&mut answer).map_err(|e| {
        ShareError::Runtime(anyhow::anyhow!("read confirmation from stdin: {e}"))
    })?;
    let trimmed = answer.trim();
    Ok(matches!(trimmed, "y" | "Y" | "yes" | "YES" | "Yes"))
}

fn upload(endpoint: &str, body: &str) -> anyhow::Result<String> {
    // Build a single-thread runtime locally — the share path is a
    // one-shot CLI invocation, and standing up a multi-thread
    // runtime here would be wasteful (and would also clash with the
    // existing single-thread pattern `run_live` uses for the
    // Snowflake pull).
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("init tokio runtime for share upload: {e}"))?;
    runtime.block_on(upload_async(endpoint, body))
}

async fn upload_async(endpoint: &str, body: &str) -> anyhow::Result<String> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("melt-audit/", env!("CARGO_PKG_VERSION")))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| anyhow::anyhow!("build HTTP client: {e}"))?;
    let response = client
        .post(endpoint)
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("POST {endpoint}: {e}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|e| anyhow::anyhow!("read response body from {endpoint}: {e}"))?;
    if !status.is_success() {
        anyhow::bail!(
            "share endpoint {endpoint} returned HTTP {status}: {}",
            text.trim()
        );
    }
    let parsed: ShareResponse = serde_json::from_str(&text).map_err(|e| {
        anyhow::anyhow!(
            "parse share response from {endpoint}: {e} (body: {})",
            text.trim()
        )
    })?;
    Ok(parsed.url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_output() -> Value {
        json!({
            "schema_version": 1,
            "account": "ACME-DEMO",
            "window": {
                "start": "2026-04-04T00:00:00Z",
                "end": "2026-05-04T00:00:00Z",
                "days": 30
            },
            "total_queries": 12345,
            "total_spend_usd": 9876.54,
            "dollar_per_query_baseline": 0.8,
            "routable_static": {
                "count": 4000,
                "pct": 32.4,
                "dollar_per_query_post": 0.5,
                "dollars_saved": 3000.0,
                "annualized": 36500.0
            },
            "routable_conservative": {
                "count": 2000,
                "pct": 16.2,
                "dollar_per_query_post": 0.6,
                "dollars_saved": 2000.0,
                "annualized": 24000.0
            },
            "top_patterns": [
                {
                    "rank": 1,
                    "freq": 500,
                    "avg_ms": 1200,
                    "table_fqn": "ANALYTICS.PUBLIC.EVENTS",
                    "pattern_redacted": "SELECT user_id, count(*) FROM ANALYTICS.PUBLIC.EVENTS WHERE created_at > ? GROUP BY user_id",
                    "est_dollars_in_window": 1500.0
                },
                {
                    "rank": 2,
                    "freq": 200,
                    "avg_ms": 800,
                    "table_fqn": "ANALYTICS.PUBLIC.ORDERS",
                    "pattern_redacted": "SELECT * FROM ANALYTICS.PUBLIC.ORDERS",
                    "est_dollars_in_window": 600.0
                },
                {
                    "rank": 3,
                    "freq": 100,
                    "avg_ms": 400,
                    "table_fqn": "PUBLIC.ACME_PROD_USERS",
                    "pattern_redacted": "SELECT email FROM PUBLIC.ACME_PROD_USERS WHERE id = ?",
                    "est_dollars_in_window": 300.0
                }
            ],
            "passthrough_reasons_breakdown": {
                "writes": 10,
                "snowflake_features": 5,
                "parse_failed": 0,
                "no_tables": 1
            },
            "confidence_band_pct": 20,
            "disclaimers": ["confidence band ±20%"]
        })
    }

    #[test]
    fn anonymize_strips_third_segment_table_identifier() {
        let v = redact_for_share(sample_output(), true);
        let patterns = v["top_patterns"].as_array().expect("top_patterns array");
        assert!(!patterns.is_empty(), "fixture has top_patterns");
        for entry in patterns {
            let fqn = entry["table_fqn"].as_str().unwrap();
            let pattern = entry["pattern_redacted"].as_str().unwrap();
            assert!(
                fqn.ends_with(".<redacted>"),
                "table_fqn must end with .<redacted> after anonymize, got {fqn}"
            );
            assert!(
                !pattern.contains("EVENTS")
                    && !pattern.contains("ORDERS")
                    && !pattern.contains("ACME_PROD_USERS"),
                "pattern still contains table-level identifier: {pattern}",
            );
            assert!(
                pattern.contains("<redacted>"),
                "pattern must reference anonymized table: {pattern}",
            );
        }
        // Schema is preserved so prospects can still see which
        // database/schema is in scope.
        assert_eq!(
            patterns[0]["table_fqn"].as_str().unwrap(),
            "ANALYTICS.PUBLIC.<redacted>"
        );
    }

    #[test]
    fn redaction_drops_predicate_columns_and_clauses() {
        let v = redact_for_share(sample_output(), true);
        let pattern = v["top_patterns"][0]["pattern_redacted"]
            .as_str()
            .expect("pattern_redacted is a string");
        // The original pattern named `user_id`, `count(*)`, `created_at`,
        // `GROUP BY user_id`. None of those should survive.
        for forbidden in ["user_id", "count(*)", "created_at", "GROUP BY"] {
            assert!(
                !pattern.contains(forbidden),
                "pattern leaked `{forbidden}` after share-time redaction: {pattern}",
            );
        }
        // The trimmed shape should be a verb + " … FROM <table> …".
        assert!(pattern.starts_with("SELECT … FROM "), "shape: {pattern}");
        assert!(pattern.ends_with(" …"), "shape (has tail): {pattern}");
    }

    #[test]
    fn redaction_omits_trailing_ellipsis_when_no_tail() {
        let v = redact_for_share(sample_output(), false);
        let pattern = v["top_patterns"][1]["pattern_redacted"]
            .as_str()
            .expect("pattern_redacted is a string");
        // Original was `SELECT * FROM ANALYTICS.PUBLIC.ORDERS` — no
        // trailing clauses, so the trimmed pattern should not append
        // a trailing `…`.
        assert_eq!(pattern, "SELECT … FROM ANALYTICS.PUBLIC.ORDERS");
    }

    #[test]
    fn anonymize_false_preserves_table_fqn() {
        let v = redact_for_share(sample_output(), false);
        let fqn = v["top_patterns"][0]["table_fqn"].as_str().unwrap();
        assert_eq!(fqn, "ANALYTICS.PUBLIC.EVENTS");
    }

    #[test]
    fn talking_points_field_is_dropped_defense_in_depth() {
        let mut v = sample_output();
        v.as_object_mut().unwrap().insert(
            "talking_points".to_string(),
            json!("DO NOT UPLOAD - real numbers"),
        );
        let out = redact_for_share(v, true);
        assert!(
            out.get("talking_points").is_none(),
            "talking_points field must be stripped before upload"
        );
    }

    #[test]
    fn share_candidate_filter_excludes_talking_points_and_receipts() {
        assert!(is_share_candidate(Path::new(
            "melt-audit-ACME-DEMO-2026-05-04.json"
        )));
        assert!(!is_share_candidate(Path::new(
            "melt-audit-ACME-DEMO-2026-05-04.talkingpoints.md"
        )));
        assert!(!is_share_candidate(Path::new(
            "melt-audit-ACME-DEMO-2026-05-04-shared.json"
        )));
        assert!(!is_share_candidate(Path::new("README.md")));
    }

    #[test]
    fn anonymize_table_handles_short_fqns() {
        assert_eq!(anonymize_table("DB.SCHEMA.TBL"), "DB.SCHEMA.<redacted>");
        assert_eq!(anonymize_table("SCHEMA.TBL"), "SCHEMA.<redacted>");
        assert_eq!(anonymize_table("TBL"), "<redacted>");
        assert_eq!(anonymize_table(""), "<redacted>");
    }
}
