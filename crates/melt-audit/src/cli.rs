//! Shared clap arg surface for the `melt-audit` binary and the
//! `melt audit` subcommand on `melt-cli`. Three execution modes:
//!
//! - `--print-grants` — emit the role-creation snippet from spec §2
//!   and exit. No Snowflake connection.
//! - `--fixture <csv>` — local-only run against a `QUERY_HISTORY`
//!   CSV export. Drives the bundled `examples/audit/` fixture, the
//!   integration test, and the README quickstart.
//! - live mode (`--account` + `--token`/`--private-key`) — pulls
//!   `ACCOUNT_USAGE.QUERY_HISTORY` from Snowflake through
//!   [`crate::snowflake::run_pull`] and rolls the result through the
//!   same aggregator the fixture path uses.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use chrono::Utc;
use clap::Parser;

use crate::aggregate::{build_audit_output, AggregateConfig};
use crate::fixture::load_query_history_csv;
use crate::output::{output_stem, render_json, render_stdout_table, render_talkingpoints};
use crate::snowflake::{build_client, run_pull, AuditAuth, PullPlan, DEFAULT_LIMIT_ROWS};
use crate::{
    DEFAULT_CREDIT_PRICE_USD, DEFAULT_TOP_N, DEFAULT_WAREHOUSE, GRANTS_SQL, SUPPORTED_WINDOW_DAYS,
};

/// `melt audit` flags. Used both as the binary's top-level parser
/// (via `clap::Parser`) and as a flattened subcommand arg-bag inside
/// `melt-cli` (via `clap::Args`).
#[derive(Parser, Debug, Clone)]
#[command(
    name = "melt-audit",
    about = "Local-only $/savings projection from Snowflake QUERY_HISTORY",
    long_about = None,
)]
pub struct AuditArgs {
    /// Snowflake account locator — same identifier the driver uses.
    /// Required for live mode; in `--fixture` mode acts as the output
    /// filename suffix and the JSON `account` field. Optional when
    /// `--print-grants` is set.
    #[arg(long)]
    pub account: Option<String>,

    /// User for username/password or key-pair auth.
    #[arg(long)]
    pub user: Option<String>,

    /// Password for username/password auth. Use `--private-key` or
    /// `--token` for non-interactive runs.
    #[arg(long)]
    pub password: Option<String>,

    /// Path to PEM-encoded RSA private key for key-pair auth.
    #[arg(long)]
    pub private_key: Option<PathBuf>,

    /// OAuth/PAT bearer token. Mutually exclusive with `--user/...`.
    #[arg(long)]
    pub token: Option<String>,

    /// Window for the audit query. Only `30d`, `60d`, `90d` are
    /// accepted in v1 (spec §2).
    #[arg(long, default_value = "30d")]
    pub window: String,

    /// Warehouse to run the audit query under. Restored to its
    /// pre-run state on exit.
    #[arg(long, default_value = DEFAULT_WAREHOUSE)]
    pub warehouse: String,

    /// Output directory for `melt-audit-<account>-<date>.{json,
    /// talkingpoints.md}`. Defaults to the current directory.
    #[arg(long, default_value = ".")]
    pub out_dir: PathBuf,

    /// USD per Snowflake credit used for the cost math.
    #[arg(long, default_value_t = DEFAULT_CREDIT_PRICE_USD)]
    pub credit_price: f64,

    /// `N` for the conservative routable rate — restricts to the
    /// top-N hottest tables by in-window spend.
    #[arg(long, default_value_t = DEFAULT_TOP_N)]
    pub top_n: usize,

    /// Print the role-creation snippet from spec §2 and exit 0.
    /// No Snowflake connection is opened.
    #[arg(long)]
    pub print_grants: bool,

    /// Offline mode: read a `QUERY_HISTORY` export from the given CSV
    /// path and run the local-processing pipeline. No Snowflake
    /// connection is opened. Drives the bundled `examples/audit/`
    /// fixture and the snapshot acceptance test.
    #[arg(long)]
    pub fixture: Option<PathBuf>,

    /// Disable ANSI colors in the stdout summary. Useful for CI
    /// captures and `> file` redirects.
    #[arg(long)]
    pub no_color: bool,
}

/// Drive a parsed [`AuditArgs`] to an [`ExitCode`].
pub fn run(args: AuditArgs) -> ExitCode {
    ExitCode::from(run_status(args))
}

/// Same as [`run`] but returns the raw exit-code byte. Used by
/// `melt-cli`'s `audit` subcommand wrapper, which has to call
/// `std::process::exit` itself (the wrapper runs inside `tokio::main`,
/// where returning an `ExitCode` would be swallowed). Exposing the
/// raw byte keeps the distinction between usage errors (2), runtime
/// failures (1), and success (0) intact.
pub fn run_status(args: AuditArgs) -> u8 {
    if args.print_grants {
        println!("{GRANTS_SQL}");
        return 0;
    }

    let window_days = match parse_window(&args.window) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };

    if let Some(fixture_path) = args.fixture.clone() {
        return match run_fixture(&args, &fixture_path, window_days) {
            Ok(_) => 0,
            Err(e) => {
                eprintln!("melt audit (fixture mode) failed: {e:#}");
                1
            }
        };
    }

    match run_live(&args, window_days) {
        Ok(_) => 0,
        Err(LiveError::Usage(e)) => {
            eprintln!("error: {e}");
            2
        }
        Err(LiveError::Runtime(e)) => {
            // The remediation hint (role + grants + `--print-grants`
            // pointer) is baked into the error chain by
            // `snowflake::run_pull`. Print the full chain so a
            // Snowflake DBA sees the upstream error and the fix in
            // one block (acceptance #5).
            eprintln!("melt audit: {e:#}");
            1
        }
    }
}

/// Exit-code lane for the live mode. `Usage` collapses to exit 2
/// (matching clap-style usage errors); `Runtime` collapses to exit 1
/// (matching the fixture-mode failure path). Keeping them split lets
/// `melt-cli`'s wrapper preserve the same distinction operators see
/// from the standalone `melt-audit` binary.
enum LiveError {
    Usage(anyhow::Error),
    Runtime(anyhow::Error),
}

fn run_live(args: &AuditArgs, window_days: u32) -> Result<(), LiveError> {
    let Some(account) = args.account.clone() else {
        return Err(LiveError::Usage(anyhow::anyhow!(
            "--account is required in live mode. For an offline test pass \
             --fixture <csv-path>; for the role-creation snippet pass --print-grants."
        )));
    };
    if args.password.is_some() {
        return Err(LiveError::Usage(anyhow::anyhow!(
            "--password is not supported by `melt audit` (Snowflake's REST API \
             has no password flow). Use --token <PAT> or --private-key <pem> \
             with --user; run `melt audit --print-grants` to provision MELT_AUDIT \
             with the right grants."
        )));
    }

    let auth = resolve_auth(args).map_err(LiveError::Usage)?;
    let client = build_client(&account, auth).map_err(LiveError::Usage)?;
    let plan = PullPlan {
        account: account.clone(),
        window_days,
        warehouse: args.warehouse.clone(),
        credit_price_usd: args.credit_price,
        limit_rows: DEFAULT_LIMIT_ROWS,
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            LiveError::Runtime(anyhow::anyhow!("init tokio runtime for live audit pull: {e}"))
        })?;
    let pull = runtime
        .block_on(run_pull(&plan, &client))
        .map_err(LiveError::Runtime)?;

    let cfg = AggregateConfig {
        account: account.clone(),
        credit_price_usd: args.credit_price,
        top_n: args.top_n,
        window_days,
        explicit_window_bounds: pull.min_start_time.zip(pull.max_start_time),
    };
    let out = build_audit_output(&pull.rows, &cfg);
    write_artifacts(args, &account, &out).map_err(LiveError::Runtime)?;

    println!(
        "✓ Pulled {} queries from Snowflake in {:.1}s",
        pull.rows.len(),
        pull.total_pull_duration.as_secs_f64(),
    );
    println!(
        "✓ Window {} → {} ({}d)",
        out.window.start.format("%Y-%m-%d"),
        out.window.end.format("%Y-%m-%d"),
        out.window.days,
    );
    print!("{}", render_stdout_table(&out, !args.no_color));
    Ok(())
}

fn resolve_auth(args: &AuditArgs) -> anyhow::Result<AuditAuth> {
    match (&args.token, &args.private_key) {
        (Some(_), Some(_)) => {
            anyhow::bail!(
                "pass exactly one of --token or --private-key (with --user); \
                 received both"
            )
        }
        (Some(t), None) => Ok(AuditAuth::Pat(t.clone())),
        (None, Some(path)) => {
            let pem_bytes = std::fs::read(path).map_err(|e| {
                anyhow::anyhow!("read --private-key from {}: {e}", path.display())
            })?;
            let user = args
                .user
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--user is required when --private-key is set"))?;
            Ok(AuditAuth::KeyPair { pem_bytes, user })
        }
        (None, None) => Err(anyhow::anyhow!(
            "no Snowflake credentials supplied — pass --token <PAT> or \
             --private-key <pem> with --user. Run `melt audit --print-grants` \
             to print the role-creation snippet from spec §2."
        )),
    }
}

/// Local pipeline: CSV → aggregate → render → write three artifacts.
fn run_fixture(args: &AuditArgs, fixture_path: &Path, window_days: u32) -> anyhow::Result<()> {
    let rows = load_query_history_csv(fixture_path)?;

    let account = args
        .account
        .clone()
        .unwrap_or_else(|| "FIXTURE".to_string());

    let cfg = AggregateConfig {
        account: account.clone(),
        credit_price_usd: args.credit_price,
        top_n: args.top_n,
        window_days,
        explicit_window_bounds: None,
    };
    let out = build_audit_output(&rows, &cfg);

    write_artifacts(args, &account, &out)?;

    println!(
        "✓ Loaded fixture {} ({} queries)",
        fixture_path.display(),
        rows.len()
    );
    println!(
        "✓ Window {} → {} ({}d)",
        out.window.start.format("%Y-%m-%d"),
        out.window.end.format("%Y-%m-%d"),
        out.window.days,
    );
    print!("{}", render_stdout_table(&out, !args.no_color));
    println!();
    println!(
        "Run `melt audit share` to (opt-in) upload anonymized results to \
         getmelt.com/audit/share."
    );
    Ok(())
}

/// Render + write the JSON + talking-points artifacts. Shared between
/// the fixture path (CSV) and the live path (Snowflake REST). Prints
/// the resulting file paths to stdout so operators can copy them
/// straight into a share CLI / Slack message.
fn write_artifacts(
    args: &AuditArgs,
    account: &str,
    out: &crate::model::AuditOutput,
) -> anyhow::Result<()> {
    let json = render_json(out);
    let talking = render_talkingpoints(out);

    std::fs::create_dir_all(&args.out_dir)?;
    let stem = output_stem(account, Utc::now());
    let json_path = args.out_dir.join(format!("{stem}.json"));
    let tp_path = args.out_dir.join(format!("{stem}.talkingpoints.md"));
    std::fs::write(&json_path, &json)?;
    std::fs::write(&tp_path, &talking)?;

    println!("JSON written → {}", json_path.display());
    println!("Talking points → {}", tp_path.display());
    Ok(())
}

fn parse_window(s: &str) -> Result<u32, String> {
    let stripped = s.strip_suffix('d').ok_or_else(|| {
        format!(
            "--window must look like `30d`. supported: {}",
            supported_label()
        )
    })?;
    let days: u32 = stripped.parse().map_err(|_| {
        format!(
            "--window must be a whole number of days. supported: {}",
            supported_label()
        )
    })?;
    if !SUPPORTED_WINDOW_DAYS.contains(&days) {
        return Err(format!(
            "--window {days}d is not supported. supported: {}",
            supported_label()
        ));
    }
    Ok(days)
}

fn supported_label() -> String {
    SUPPORTED_WINDOW_DAYS
        .iter()
        .map(|d| format!("{d}d"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_7d() {
        let err = parse_window("7d").unwrap_err();
        assert!(err.contains("supported"));
        assert!(err.contains("30d"));
    }

    #[test]
    fn accepts_30_60_90() {
        for d in ["30d", "60d", "90d"] {
            let ok = parse_window(d).expect("supported window");
            assert!([30, 60, 90].contains(&ok));
        }
    }
}
