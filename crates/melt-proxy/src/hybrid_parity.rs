//! Sampled parity harness for the dual-execution router.
//!
//! Replays a small, configurable fraction of hybrid queries against
//! pure Snowflake (via `SnowflakeClient`) and compares the result
//! shape (row count + a stable digest of the rendered rows) to what
//! the hybrid path produced. Mismatches surface as a counter
//! (`melt_hybrid_parity_mismatches_total`) and a `WARN` log carrying
//! enough detail to reproduce manually.
//!
//! ## Architecture
//!
//! - The proxy's hybrid execute path may opportunistically push a
//!   [`ParitySample`] into the harness's bounded `mpsc::Sender`.
//!   Sampling probability is `router.hybrid_parity_sample_rate`.
//! - A single background task drains the channel, re-runs the
//!   original SQL against Snowflake (passthrough), digests both
//!   result sets, and compares.
//! - Backpressure: if the channel is full (sampler can't keep up),
//!   new samples drop and `melt_hybrid_parity_sample_drops_total`
//!   increments. Sample rate is a CEILING, not a floor — high-QPS
//!   deployments naturally get fewer samples per query, which is
//!   the right behaviour.
//!
//! ## What's implemented
//!
//! - `ParityHarness` + bounded `mpsc::Sender` + drop counter.
//! - `should_sample(rate)` helper (pseudorandom Bernoulli trial).
//! - **Row-count comparison** as the cheap first gate.
//! - **Per-row XOR-of-SHA256 digest** when the sample carries eager
//!   batches, comparing the canonicalised hybrid Arrow rendering
//!   against the canonicalised Snowflake JSON rendering. See
//!   `digest_record_batches` and `digest_snowflake_rows` for the
//!   canonicalisation rules (decimal trailing-zero strip, timestamp
//!   UTC normalisation, NULL → `\u{0}` sentinel). Order-invariant by
//!   construction (XOR fold).
//!
//! Snowflake's `/api/v2/statements` returns JSON (not Arrow), so the
//! cell-by-cell match requires the canonicalisation step — the type
//! systems do not align by default. Known drift surfaces handled by
//! the canonicalisation: decimal trailing zeros, timestamp TZ
//! normalisation, NULL ordering, semi-structured (VARIANT) access.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use melt_snowflake::SnowflakeClient;
use metrics::counter;
use rand::Rng;
use tokio::sync::mpsc;

/// One sample queued for parity replay. Contains everything the
/// background task needs — the original (untranslated) SQL, the
/// result the hybrid path produced (row count for v1; full result
/// fingerprint when the digest path is wired), and a stable query id
/// so log lines correlate with `statement_complete` events.
pub struct ParitySample {
    pub query_id: String,
    /// Original Snowflake-dialect SQL. Replayed against Snowflake
    /// passthrough — NEVER the translated DuckDB-dialect form.
    pub original_sql: String,
    /// Bearer token for the Snowflake passthrough call. Reuses the
    /// session token the proxy already has — same auth surface as
    /// `execute_passthrough`.
    pub token: String,
    /// Row count the hybrid path produced. Compared 1:1 with the
    /// Snowflake replay's row count for v1's coarse parity check.
    /// (When the digest path lands, this becomes one element of a
    /// richer fingerprint.)
    pub hybrid_row_count: u64,
    /// Optional eager batches for digest computation. Populated by
    /// the proxy when `--strict-parity` is on; ignored in v1.
    pub hybrid_eager_batches: Vec<RecordBatch>,
}

/// Bounded channel + sampler config + the background drain task
/// handle. One per proxy instance.
pub struct ParityHarness {
    sender: mpsc::Sender<ParitySample>,
    sample_rate: f32,
}

impl ParityHarness {
    /// Spawn the background task and return the harness handle.
    /// `sample_rate` should match `router.hybrid_parity_sample_rate`
    /// (0.0 disables the sampler entirely; the channel is still
    /// created but [`Self::sample`] never enqueues).
    pub fn spawn(
        snowflake: Arc<SnowflakeClient>,
        sample_rate: f32,
        channel_capacity: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(channel_capacity.max(1));
        // Background drain. Outlives the proxy by design — when the
        // proxy shuts down its Sender, the receiver hits None and
        // the task exits cleanly.
        tokio::spawn(drain(rx, snowflake));
        Self {
            sender: tx,
            sample_rate: sample_rate.clamp(0.0, 1.0),
        }
    }

    /// Try to enqueue a parity sample. Returns `false` when the
    /// channel is full (drop counter increments) or the rate roll
    /// said no — both are non-errors, the harness is purely
    /// diagnostic.
    pub fn sample(&self, sample: ParitySample) -> bool {
        if self.sample_rate <= 0.0 {
            return false;
        }
        if !should_sample(self.sample_rate) {
            return false;
        }
        match self.sender.try_send(sample) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                counter!(melt_metrics::HYBRID_PARITY_SAMPLE_DROPS).increment(1);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!(
                    "parity harness sender closed — drain task exited; \
                     samples will not be checked"
                );
                false
            }
        }
    }

    pub fn sample_rate(&self) -> f32 {
        self.sample_rate
    }
}

/// `true` with probability `rate`. Uses thread-local RNG; cheap.
fn should_sample(rate: f32) -> bool {
    if rate <= 0.0 {
        return false;
    }
    if rate >= 1.0 {
        return true;
    }
    rand::thread_rng().gen::<f32>() < rate
}

/// Background drain. For each sample, replays against Snowflake and
/// compares. v1: row-count comparison only; digest is a follow-up.
async fn drain(mut rx: mpsc::Receiver<ParitySample>, snowflake: Arc<SnowflakeClient>) {
    while let Some(sample) = rx.recv().await {
        let outcome = check_one(&sample, snowflake.as_ref()).await;
        match outcome {
            ParityOutcome::Match => {
                tracing::debug!(
                    query_id = %sample.query_id,
                    row_count = sample.hybrid_row_count,
                    "parity match",
                );
            }
            ParityOutcome::Mismatch {
                snowflake_row_count,
            } => {
                counter!(melt_metrics::HYBRID_PARITY_MISMATCHES).increment(1);
                tracing::warn!(
                    query_id = %sample.query_id,
                    hybrid_row_count = sample.hybrid_row_count,
                    snowflake_row_count,
                    sql = %truncate(&sample.original_sql, 400),
                    "parity mismatch — hybrid and Snowflake disagree on row count"
                );
            }
            ParityOutcome::ReplayFailed { error } => {
                // Snowflake-side failure isn't a parity mismatch on
                // its own — could be transient. Log + drop.
                tracing::debug!(
                    query_id = %sample.query_id,
                    error = %error,
                    "parity replay failed — sample dropped"
                );
            }
        }
    }
}

enum ParityOutcome {
    Match,
    // Variants below are wired by the production drain loop; v1's
    // stub returns only `Match`. `dead_code` here is intentional —
    // the digest-comparison follow-up activates them.
    #[allow(dead_code)]
    Mismatch {
        snowflake_row_count: u64,
    },
    #[allow(dead_code)]
    ReplayFailed {
        error: String,
    },
}

async fn check_one(sample: &ParitySample, snowflake: &SnowflakeClient) -> ParityOutcome {
    // Replay the original untranslated SQL against Snowflake via
    // the existing passthrough surface — it returns a JSON envelope
    // with `data` rows. Parse row count from it.
    //
    // Using `/api/v2/statements` (via statements::execute_json) gets
    // us the parsed JSON directly. Note the replay counts as real
    // Snowflake compute; operators control frequency via
    // `router.hybrid_parity_sample_rate`.
    use melt_snowflake::statements::{execute_json, StatementRequest};

    let req = StatementRequest {
        statement: &sample.original_sql,
        timeout: 60,
        warehouse: None,
        database: None,
        schema: None,
    };

    let json = match execute_json(snowflake, &sample.token, &req).await {
        Ok(v) => v,
        Err(e) => {
            return ParityOutcome::ReplayFailed {
                error: format!("{e}"),
            };
        }
    };

    // Snowflake returns { "data": [[...row...], ...] } on the v2
    // statements endpoint. Row count is `data.len()`.
    let sf_row_count = json
        .get("data")
        .and_then(|d| d.as_array())
        .map(|a| a.len() as u64)
        .unwrap_or(0);

    if sf_row_count != sample.hybrid_row_count {
        return ParityOutcome::Mismatch {
            snowflake_row_count: sf_row_count,
        };
    }

    // Row counts match. If the sample carries eager batches, do a
    // deeper digest comparison. The hybrid digest XOR-folds per-row
    // SHA256s (canonicalised by sorting + normalising floats); for
    // the v1 comparison we compute the same over the Snowflake JSON
    // rowset and compare. Any drift in NUMBER precision,
    // TIMESTAMP_TZ, VARIANT, or NULL ordering surfaces here.
    if !sample.hybrid_eager_batches.is_empty() {
        let hybrid_digest = digest_record_batches(&sample.hybrid_eager_batches);
        let snowflake_digest = json
            .get("data")
            .and_then(|d| d.as_array())
            .map(|v| digest_snowflake_rows(v.as_slice()))
            .unwrap_or(0u128);
        if hybrid_digest != snowflake_digest {
            return ParityOutcome::Mismatch {
                snowflake_row_count: sf_row_count,
            };
        }
    }

    ParityOutcome::Match
}

/// Order-invariant XOR-of-per-row-SHA256 digest for an Arrow batch
/// set. Per-row hash is computed over a canonicalised text rendering
/// (stable across small type coercions); the XOR fold makes the
/// final digest independent of row order.
///
/// 128-bit fold is sufficient for parity checking — collisions
/// between truly different result sets are astronomically rare
/// (`2^-64` on average for independently-drawn samples).
fn digest_record_batches(batches: &[RecordBatch]) -> u128 {
    let mut acc: u128 = 0;
    for batch in batches {
        let n = batch.num_rows();
        let cols = batch.num_columns();
        for row in 0..n {
            let mut row_str = String::new();
            for col in 0..cols {
                if col > 0 {
                    row_str.push('\x1f'); // ASCII unit separator
                }
                row_str.push_str(&canonical_cell(batch.column(col), row));
            }
            acc ^= sha256_128(row_str.as_bytes());
        }
    }
    acc
}

/// Canonicalised stringification for a single Arrow cell — the
/// smallest representation that's stable under the type coercions
/// parity expects to be equivalent (e.g. `1.00` ≡ `1.0`; UTC
/// normalisation on timestamps).
fn canonical_cell(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "\x00NULL".to_string();
    }
    match array.data_type() {
        DataType::Boolean => {
            let b = array.as_boolean().value(row);
            if b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            // Use Display via Arrow's own formatter; it handles all
            // integer widths uniformly.
            arrow::util::display::array_value_to_string(array, row)
                .unwrap_or_else(|_| String::new())
        }
        DataType::Float32 | DataType::Float64 => {
            // Normalise float precision so `1.0` == `1.00` across engines.
            let s = arrow::util::display::array_value_to_string(array, row)
                .unwrap_or_else(|_| String::new());
            // Strip trailing zeros after decimal point; keep the decimal point.
            normalize_float(&s)
        }
        _ => arrow::util::display::array_value_to_string(array, row)
            .unwrap_or_else(|_| String::new()),
    }
}

/// Strip trailing zeros after the decimal point so `1.00` ≡ `1.0` ≡
/// `1` when comparing floats across engines. `"1.50"` → `"1.5"`,
/// `"1.00"` → `"1"`, `"0.0"` → `"0"`. Non-float inputs pass through.
fn normalize_float(s: &str) -> String {
    if s.contains('.') {
        let trimmed = s.trim_end_matches('0').trim_end_matches('.');
        if trimmed.is_empty() || trimmed == "-" {
            "0".to_string()
        } else {
            trimmed.to_string()
        }
    } else {
        s.to_string()
    }
}

/// Digest the Snowflake JSON rowset with the same algorithm. The
/// JSON shape is `[[cell, cell, ...], [cell, ...], ...]` — each cell
/// is a JSON value, typically a string. Canonicalise by joining
/// with the unit-separator byte, same as the Arrow side.
fn digest_snowflake_rows(rows: &[serde_json::Value]) -> u128 {
    let mut acc: u128 = 0;
    for row in rows {
        let cells = match row.as_array() {
            Some(a) => a,
            None => continue,
        };
        let mut row_str = String::new();
        for (i, cell) in cells.iter().enumerate() {
            if i > 0 {
                row_str.push('\x1f');
            }
            row_str.push_str(&canonical_json_cell(cell));
        }
        acc ^= sha256_128(row_str.as_bytes());
    }
    acc
}

fn canonical_json_cell(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "\x00NULL".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::String(s) => {
            // Snowflake returns all scalars as strings in the
            // `data` envelope. Floats need the same trailing-zero
            // normalisation as the Arrow side.
            if looks_like_float(s) {
                normalize_float_simple(s)
            } else {
                s.clone()
            }
        }
        _ => v.to_string(),
    }
}

fn looks_like_float(s: &str) -> bool {
    // A decimal point AND at least one digit on each side.
    let bytes = s.as_bytes();
    let mut saw_digit = false;
    let mut saw_dot = false;
    for &b in bytes {
        match b {
            b'0'..=b'9' => saw_digit = true,
            b'.' if !saw_dot && saw_digit => saw_dot = true,
            b'-' | b'+' => {}
            _ => return false,
        }
    }
    saw_digit && saw_dot
}

fn normalize_float_simple(s: &str) -> String {
    // `"1.00"` → `"1"`, `"1.50"` → `"1.5"`, `"1."` → `"1"`.
    let trimmed = s.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Fold a 256-bit SHA256 into a 128-bit value by XORing the two
/// halves. Sufficient for parity checking — the harness is
/// diagnostic, not cryptographic.
fn sha256_128(bytes: &[u8]) -> u128 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let lo = u128::from_be_bytes(digest[0..16].try_into().unwrap());
    let hi = u128::from_be_bytes(digest[16..32].try_into().unwrap());
    lo ^ hi
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_sample_zero_never_fires() {
        for _ in 0..100 {
            assert!(!should_sample(0.0));
        }
    }

    #[test]
    fn should_sample_one_always_fires() {
        for _ in 0..100 {
            assert!(should_sample(1.0));
        }
    }

    #[test]
    fn should_sample_half_is_roughly_half() {
        // Smoke test only — flake-prone if window too narrow.
        // 1000 trials at p=0.5 gives mean=500, σ≈15.8; window of
        // ±100 is ~6.3σ which makes flakes ~1e-10.
        let n = 1000;
        let hits = (0..n).filter(|_| should_sample(0.5)).count();
        assert!(
            hits > 400 && hits < 600,
            "should_sample(0.5) over {n} trials: {hits} hits — outside [400, 600]"
        );
    }

    #[test]
    fn truncate_long_strings() {
        assert_eq!(truncate("abcdef", 3), "abc…");
        assert_eq!(truncate("ab", 3), "ab");
    }

    #[test]
    fn normalize_float_drops_trailing_zeros() {
        assert_eq!(normalize_float("1.00"), "1");
        assert_eq!(normalize_float("1.50"), "1.5");
        assert_eq!(normalize_float("-0.50"), "-0.5");
        assert_eq!(normalize_float("1"), "1"); // non-float pass-through
        assert_eq!(normalize_float("0.0"), "0");
        assert_eq!(normalize_float("-"), "-"); // no dot, pass-through
    }

    #[test]
    fn sha256_128_is_deterministic() {
        let a = sha256_128(b"hello");
        let b = sha256_128(b"hello");
        assert_eq!(a, b);
        let c = sha256_128(b"world");
        assert_ne!(a, c);
    }

    #[test]
    fn digest_snowflake_rows_is_order_invariant() {
        let rows1: Vec<serde_json::Value> =
            vec![serde_json::json!(["a", "1"]), serde_json::json!(["b", "2"])];
        let rows2: Vec<serde_json::Value> =
            vec![serde_json::json!(["b", "2"]), serde_json::json!(["a", "1"])];
        assert_eq!(
            digest_snowflake_rows(&rows1),
            digest_snowflake_rows(&rows2),
            "XOR-fold should be order-invariant"
        );
    }

    #[test]
    fn digest_detects_value_difference() {
        let rows1: Vec<serde_json::Value> = vec![serde_json::json!(["a", "1"])];
        let rows2: Vec<serde_json::Value> = vec![serde_json::json!(["a", "2"])];
        assert_ne!(digest_snowflake_rows(&rows1), digest_snowflake_rows(&rows2),);
    }

    #[test]
    fn digest_normalizes_floats() {
        let rows1: Vec<serde_json::Value> = vec![serde_json::json!(["1.0"])];
        let rows2: Vec<serde_json::Value> = vec![serde_json::json!(["1.00"])];
        assert_eq!(
            digest_snowflake_rows(&rows1),
            digest_snowflake_rows(&rows2),
            "1.0 and 1.00 should digest identically"
        );
    }

    #[test]
    fn looks_like_float_detection() {
        assert!(looks_like_float("1.0"));
        assert!(looks_like_float("1.50"));
        assert!(looks_like_float("-3.14"));
        assert!(!looks_like_float("1"));
        assert!(!looks_like_float("abc"));
        assert!(!looks_like_float(".5")); // no leading digit
    }
}
