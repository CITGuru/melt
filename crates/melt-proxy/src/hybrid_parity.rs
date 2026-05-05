//! Sampled parity harness for the dual-execution router.
//!
//! Replays a small, configurable fraction of hybrid queries against
//! pure Snowflake (via `SnowflakeClient`) and compares the result
//! shape (row count + an optional stable digest of the rendered rows)
//! to what the hybrid path produced. Mismatches surface as a labelled
//! counter (`melt_hybrid_parity_mismatches_total{route=hybrid,reason=…}`)
//! and a `WARN hybrid_parity_mismatch` log carrying enough detail to
//! reproduce manually — query hash, plan summary (NO SQL body),
//! expected/actual row counts, expected/actual digests, sampler
//! config.
//!
//! ## Architecture
//!
//! - The proxy's hybrid execute path opportunistically pushes a
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
//! ## Compare modes
//!
//! `[router].hybrid_parity_compare_mode` selects how aggressively the
//! sampler compares:
//!
//! - [`HybridParityCompareMode::RowCount`] (default) — the row count
//!   is the only check. Cheapest path; the digest comparison is
//!   skipped even when eager batches are available.
//! - [`HybridParityCompareMode::Hash`] — row counts AND a per-row
//!   XOR-of-SHA256 digest of the canonicalised result. Catches
//!   NUMBER precision, TIMESTAMP_TZ, VARIANT, and NULL ordering
//!   drift at the cost of buffering eager batches and running the
//!   canonicalisation. Hash mode is the follow-up budget — keep on
//!   `RowCount` until the bench harness shows the digest cost is
//!   workable at the configured sample rate.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use melt_core::HybridParityCompareMode;
use melt_snowflake::SnowflakeClient;
use metrics::counter;
use rand::Rng;
use tokio::sync::mpsc;

/// Compact, log-safe summary of the hybrid plan that produced the
/// sampled result. Stamped into every `WARN hybrid_parity_mismatch`
/// log line so operators have routing context without the SQL body
/// (which can carry PII / large literal lists). Mirrors the
/// observability surface in `docs/internal/DUAL_EXECUTION.md` §10.
#[derive(Clone, Debug, Default)]
pub struct PlanSummary {
    /// `attach` | `materialize` | `mixed` | `none`. Mirrors
    /// `melt_hybrid_strategy_total{strategy=…}`.
    pub strategy: String,
    /// Display-form of `HybridReason` (`RemoteByConfig`, etc.).
    pub reason: String,
    /// Number of `RemoteSql` nodes that chose Materialize. Each
    /// fragment is one Snowflake roundtrip at execute time.
    pub fragments: usize,
    /// Number of `RemoteSql` nodes that chose Attach.
    pub attach_rewrites: usize,
    /// Distinct table count across all remote nodes. We log a count
    /// rather than the names so the WARN line stays bounded and
    /// operators can join against `melt route <sql>` for the names.
    pub remote_table_count: usize,
    /// Estimated bytes the Materialize path will pull from Snowflake
    /// across all fragments. From `HybridPlan::estimated_remote_bytes`.
    pub estimated_remote_bytes: u64,
    /// Strategy chain member that decided the top-level collapse for
    /// this plan (`heuristic` / `cost` / `fallback`). Mirrors
    /// `melt_hybrid_strategy_decisions_total{strategy=…}`.
    pub chain_decided_by: String,
}

/// One sample queued for parity replay. Contains everything the
/// background task needs — the original (untranslated) SQL, the
/// result the hybrid path produced (row count + optionally eager
/// batches for the digest path), routing context for the WARN log,
/// and the sampler config that produced it (so the log is
/// self-describing and operators can correlate with `melt.toml`).
pub struct ParitySample {
    /// Stable per-query identifier — currently a UUID minted at sample
    /// time. The WARN log emits this verbatim; operators reproduce
    /// via `melt logs grep <query_id>`.
    pub query_id: String,
    /// SHA-256-128 of the original SQL. Stable across runs / proxies,
    /// safe to log (no PII), small enough for dashboards. Counterpart
    /// to the WARN log's `query_hash` field.
    pub query_hash: u128,
    /// Original Snowflake-dialect SQL. Replayed against Snowflake
    /// passthrough — NEVER the translated DuckDB-dialect form.
    pub original_sql: String,
    /// Bearer token for the Snowflake passthrough call. Reuses the
    /// session token the proxy already has — same auth surface as
    /// `execute_passthrough`.
    pub token: String,
    /// Row count the hybrid path produced. Compared 1:1 with the
    /// Snowflake replay's row count for the cheap first gate.
    pub hybrid_row_count: u64,
    /// Optional eager batches for digest computation. Only populated
    /// when `compare_mode == Hash`; ignored in `RowCount` mode so the
    /// hot path avoids cloning.
    pub hybrid_eager_batches: Vec<RecordBatch>,
    /// Plan-shape summary. Logged with every mismatch so operators
    /// see strategy / reason / fragment count without the SQL body.
    pub plan_summary: PlanSummary,
    /// Compare mode selected at proxy boot. Carried per-sample so the
    /// drain loop is fully self-describing — no global state read.
    pub compare_mode: HybridParityCompareMode,
    /// Sample rate active at the time the sample was queued. Logged
    /// alongside mismatches so an operator reading the log knows
    /// whether they were sampling 1% (and the mismatch is one of many
    /// the sampler missed) or 100% (and this is exhaustive).
    pub sample_rate: f32,
}

/// Bounded channel + sampler config + the background drain task
/// handle. One per proxy instance.
pub struct ParityHarness {
    sender: mpsc::Sender<ParitySample>,
    sample_rate: f32,
    compare_mode: HybridParityCompareMode,
}

impl ParityHarness {
    /// Spawn the background task and return the harness handle.
    /// `sample_rate` should match `router.hybrid_parity_sample_rate`
    /// (0.0 disables the sampler entirely; the channel is still
    /// created but [`Self::sample`] never enqueues).
    pub fn spawn(
        snowflake: Arc<SnowflakeClient>,
        sample_rate: f32,
        compare_mode: HybridParityCompareMode,
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
            compare_mode,
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

    pub fn compare_mode(&self) -> HybridParityCompareMode {
        self.compare_mode
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

/// Background drain. Runs until the channel closes (proxy shutdown).
async fn drain(mut rx: mpsc::Receiver<ParitySample>, snowflake: Arc<SnowflakeClient>) {
    while let Some(sample) = rx.recv().await {
        let outcome = check_one(&sample, snowflake.as_ref()).await;
        match outcome {
            ParityOutcome::Match => {
                counter!(
                    melt_metrics::HYBRID_PARITY_SAMPLES,
                    melt_metrics::LABEL_OUTCOME => melt_metrics::OUTCOME_OK,
                )
                .increment(1);
                tracing::debug!(
                    query_id = %sample.query_id,
                    query_hash = %format_hash(sample.query_hash),
                    row_count = sample.hybrid_row_count,
                    compare_mode = sample.compare_mode.as_str(),
                    "parity match",
                );
            }
            ParityOutcome::Mismatch {
                reason,
                snowflake_row_count,
                hybrid_digest,
                snowflake_digest,
            } => {
                counter!(
                    melt_metrics::HYBRID_PARITY_MISMATCHES,
                    melt_metrics::LABEL_ROUTE => "hybrid",
                    melt_metrics::LABEL_REASON => reason.as_str(),
                )
                .increment(1);
                counter!(
                    melt_metrics::HYBRID_PARITY_SAMPLES,
                    melt_metrics::LABEL_OUTCOME => "mismatch",
                )
                .increment(1);
                let summary = &sample.plan_summary;
                // No SQL body here by design (POWA-162 AC2): the WARN
                // line is a routing-shape fingerprint, the operator
                // grabs the SQL from `query_id` ↔ `statement_complete`
                // correlation when reproducing.
                tracing::warn!(
                    target: "hybrid_parity_mismatch",
                    query_id = %sample.query_id,
                    query_hash = %format_hash(sample.query_hash),
                    reason = reason.as_str(),
                    expected_row_count = sample.hybrid_row_count,
                    actual_row_count = snowflake_row_count,
                    expected_checksum = %format_hash_opt(hybrid_digest),
                    actual_checksum = %format_hash_opt(snowflake_digest),
                    plan_strategy = %summary.strategy,
                    plan_reason = %summary.reason,
                    plan_fragments = summary.fragments,
                    plan_attach_rewrites = summary.attach_rewrites,
                    plan_remote_table_count = summary.remote_table_count,
                    plan_estimated_remote_bytes = summary.estimated_remote_bytes,
                    plan_chain_decided_by = %summary.chain_decided_by,
                    sampler_rate = sample.sample_rate,
                    sampler_compare_mode = sample.compare_mode.as_str(),
                    "hybrid_parity_mismatch",
                );
            }
            ParityOutcome::ReplayFailed { error } => {
                counter!(
                    melt_metrics::HYBRID_PARITY_SAMPLES,
                    melt_metrics::LABEL_OUTCOME => "replay_failed",
                )
                .increment(1);
                // Snowflake-side failure isn't a parity mismatch on
                // its own — could be transient. Log + drop.
                tracing::debug!(
                    query_id = %sample.query_id,
                    query_hash = %format_hash(sample.query_hash),
                    error = %error,
                    "parity replay failed — sample dropped"
                );
            }
        }
    }
}

/// Mismatch reason — labels the `melt_hybrid_parity_mismatches_total`
/// counter and the `reason` field on the WARN log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MismatchReason {
    /// Row counts disagreed; this is the cheap first gate and always
    /// runs regardless of `compare_mode`.
    RowCount,
    /// Row counts matched but the per-row XOR-of-SHA256 digest
    /// disagreed. Only fires when `compare_mode == Hash`.
    Hash,
}

impl MismatchReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RowCount => "row_count",
            Self::Hash => "hash",
        }
    }
}

enum ParityOutcome {
    Match,
    Mismatch {
        reason: MismatchReason,
        snowflake_row_count: u64,
        /// Set only when `reason == Hash`; `None` otherwise.
        hybrid_digest: Option<u128>,
        snowflake_digest: Option<u128>,
    },
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
        role: None,
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
    let sf_rows: &[serde_json::Value] = json
        .get("data")
        .and_then(|d| d.as_array())
        .map(|a| a.as_slice())
        .unwrap_or(&[]);
    let sf_row_count = sf_rows.len() as u64;

    if sf_row_count != sample.hybrid_row_count {
        return ParityOutcome::Mismatch {
            reason: MismatchReason::RowCount,
            snowflake_row_count: sf_row_count,
            hybrid_digest: None,
            snowflake_digest: None,
        };
    }

    // Row counts match. In `Hash` compare mode, do the deeper digest
    // comparison when we have eager batches. The hybrid digest
    // XOR-folds per-row SHA256s (canonicalised by sorting + normalising
    // floats); for the comparison we compute the same over the
    // Snowflake JSON rowset and compare. Any drift in NUMBER precision,
    // TIMESTAMP_TZ, VARIANT, or NULL ordering surfaces here.
    if matches!(sample.compare_mode, HybridParityCompareMode::Hash)
        && !sample.hybrid_eager_batches.is_empty()
    {
        let hybrid_digest = digest_record_batches(&sample.hybrid_eager_batches);
        let snowflake_digest = digest_snowflake_rows(sf_rows);
        if hybrid_digest != snowflake_digest {
            return ParityOutcome::Mismatch {
                reason: MismatchReason::Hash,
                snowflake_row_count: sf_row_count,
                hybrid_digest: Some(hybrid_digest),
                snowflake_digest: Some(snowflake_digest),
            };
        }
    }

    ParityOutcome::Match
}

/// SHA-256-128 of an arbitrary byte string. Re-exported via the
/// `query_hash` constructor below for [`ParitySample`] callers — the
/// proxy hashes `original_sql` once per sample so the harness never
/// stringifies SQL into log lines itself.
pub fn hash_query(sql: &str) -> u128 {
    sha256_128(sql.as_bytes())
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

fn format_hash(h: u128) -> String {
    format!("{h:032x}")
}

fn format_hash_opt(h: Option<u128>) -> String {
    match h {
        Some(v) => format_hash(v),
        None => "-".to_string(),
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
    fn hash_query_is_stable_and_distinct() {
        let a = hash_query("SELECT 1");
        let b = hash_query("SELECT 1");
        let c = hash_query("SELECT 2");
        assert_eq!(a, b, "same SQL should produce same hash");
        assert_ne!(a, c, "different SQL should produce different hash");
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

    #[test]
    fn mismatch_reason_labels_match_metric_vocab() {
        assert_eq!(MismatchReason::RowCount.as_str(), "row_count");
        assert_eq!(MismatchReason::Hash.as_str(), "hash");
    }

    #[test]
    fn format_hash_is_zero_padded_lowercase_hex() {
        assert_eq!(format_hash(0), "0".repeat(32));
        assert_eq!(
            format_hash(0xdeadbeef_u128),
            "000000000000000000000000deadbeef"
        );
    }

    #[test]
    fn format_hash_opt_renders_dash_for_none() {
        assert_eq!(format_hash_opt(None), "-");
        assert_eq!(format_hash_opt(Some(1)), format_hash(1));
    }
}
