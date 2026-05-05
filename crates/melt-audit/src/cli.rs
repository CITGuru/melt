//! Shared clap arg surface for the `melt-audit` binary and the
//! `melt audit` subcommand on `melt-cli`. Three execution modes:
//!
//! - `--print-grants` — emit the role-creation snippet from spec §2
//!   and exit. No Snowflake connection.
//! - `--fixture <csv>` — local-only run against a `QUERY_HISTORY`
//!   CSV export. Drives the bundled `examples/audit/` fixture, the
//!   integration test, and the README quickstart.
//! - live mode (`--account` + auth) — pulls `ACCOUNT_USAGE.QUERY_HISTORY`
//!   from Snowflake. Wire-up of the HTTP path is the follow-up commit
//!   on this branch.

use std::path::PathBuf;
use std::process::ExitCode;

use chrono::Utc;
use clap::Parser;

use crate::aggregate::{build_audit_output, AggregateConfig};
use crate::fixture::load_query_history_csv;
use crate::output::{output_stem, render_json, render_stdout_table, render_talkingpoints};
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
    if args.print_grants {
        println!("{GRANTS_SQL}");
        return ExitCode::SUCCESS;
    }

    let window_days = match parse_window(&args.window) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };

    if let Some(fixture_path) = args.fixture.clone() {
        return match run_fixture(&args, &fixture_path, window_days) {
            Ok(code) => code,
            Err(e) => {
                eprintln!("melt audit (fixture mode) failed: {e:#}");
                ExitCode::from(1)
            }
        };
    }

    // Live Snowflake mode. The HTTP pull lives in `crate::snowflake`
    // and is still stubbed; until it's wired in, surface a remediation
    // hint that names the MELT_AUDIT role and the missing-grants
    // signal so the message reads correctly to a Snowflake DBA
    // (acceptance #5).
    let Some(account) = args.account.as_deref() else {
        eprintln!(
            "error: --account is required in live mode. \
             For an offline test pass --fixture <csv-path>; for the \
             role-creation snippet pass --print-grants."
        );
        return ExitCode::from(2);
    };

    eprintln!(
        "melt audit: live Snowflake pull is not yet wired in this \
         build (account={account}, window={window_days}d). Run with \
         `--fixture examples/audit/query-history-fixture.csv` to drive the \
         local pipeline today, or `--print-grants` to print the role \
         grants block. Live ACCOUNT_USAGE access requires the MELT_AUDIT \
         role; check the grants printed by --print-grants if the operator's \
         session lacks IMPORTED PRIVILEGES on \
         SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY / WAREHOUSE_METERING_HISTORY."
    );
    ExitCode::from(2)
}

/// Local pipeline: CSV → aggregate → render → write three artifacts.
fn run_fixture(
    args: &AuditArgs,
    fixture_path: &std::path::Path,
    window_days: u32,
) -> anyhow::Result<ExitCode> {
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

    let json = render_json(&out);
    let talking = render_talkingpoints(&out);
    let table = render_stdout_table(&out, !args.no_color);

    std::fs::create_dir_all(&args.out_dir)?;
    let stem = output_stem(&account, Utc::now());
    let json_path = args.out_dir.join(format!("{stem}.json"));
    let tp_path = args.out_dir.join(format!("{stem}.talkingpoints.md"));
    std::fs::write(&json_path, &json)?;
    std::fs::write(&tp_path, &talking)?;

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
    print!("{table}");
    println!("JSON written → {}", json_path.display());
    println!("Talking points → {}", tp_path.display());
    println!();
    println!(
        "Run `melt audit share` to (opt-in) upload anonymized results to \
         getmelt.com/audit/share."
    );
    Ok(ExitCode::SUCCESS)
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
