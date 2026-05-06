//! Demo: query a local Melt proxy using `snowflake-connector-rs`.
//!
//! The connector is the unmodified upstream crate; only the
//! `with_address(host, port, protocol)` builder call differs from a
//! normal Snowflake connection. Each query exercises a different
//! routing path so you can watch Melt's logs and confirm the proxy
//! is making the decisions you expect.
//!
//! Two modes:
//!
//! * `MELT_MODE=seed` — credential-free demo against a local TPC-H
//!   sf=0.01 fixture (POWA-92, KI-002). Defaults match what
//!   `melt sessions seed` writes into `melt.demo.toml`.
//! * default (real mode) — forwards login to upstream Snowflake; set
//!   `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`.
//!
//! Run (seed mode):
//!
//! ```bash
//! cargo run -p melt-cli -- sessions seed
//! cargo run -p melt-cli -- --config melt.demo.toml all  # in another terminal
//! MELT_MODE=seed cargo run --release
//! ```
//!
//! Run (real mode):
//!
//! ```bash
//! export SNOWFLAKE_ACCOUNT=...  SNOWFLAKE_USER=...  SNOWFLAKE_PASSWORD=...
//! cargo run --release
//! ```
//!
//! In another terminal:
//!
//! ```bash
//! docker compose logs -f melt | grep statement_complete
//! ```

use std::env;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use snowflake_connector_rs::{
    SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig, SnowflakeRow, SnowflakeSession,
};

struct Settings {
    melt_host: String,
    melt_port: u16,
    melt_protocol: String,
    account: String,
    user: String,
    password: String,
    database: String,
    schema: String,
}

impl Settings {
    fn from_env() -> Result<Self> {
        // Defaults match the local docker compose setup. Override any
        // of them with env vars. In seed mode (`MELT_MODE=seed`) every
        // SNOWFLAKE_* default comes from `melt-core::config::SEED_*`
        // — same canned creds the Python example uses.
        let seed = env::var("MELT_MODE")
            .map(|v| v.eq_ignore_ascii_case("seed"))
            .unwrap_or(false);
        let env_or = |k: &str, default: &str| env::var(k).unwrap_or_else(|_| default.to_string());
        let req = |k: &str, fallback: Option<&str>| -> Result<String> {
            match (env::var(k).ok(), fallback) {
                (Some(v), _) => Ok(v),
                (None, Some(d)) => Ok(d.to_string()),
                (None, None) => Err(anyhow!("missing required env var: {k}")),
            }
        };
        Ok(Self {
            melt_host: env::var("MELT_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            melt_port: env::var("MELT_PORT")
                .unwrap_or_else(|_| "8443".into())
                .parse()
                .context("MELT_PORT must be a u16")?,
            melt_protocol: env::var("MELT_PROTOCOL").unwrap_or_else(|_| "http".into()),
            account: req("SNOWFLAKE_ACCOUNT", seed.then_some("melt-demo"))?,
            user: req("SNOWFLAKE_USER", seed.then_some("demo"))?,
            password: req("SNOWFLAKE_PASSWORD", seed.then_some("demo"))?,
            database: env_or(
                "SNOWFLAKE_DATABASE",
                if seed { "TPCH" } else { "ANALYTICS" },
            ),
            schema: env_or("SNOWFLAKE_SCHEMA", if seed { "SF01" } else { "PUBLIC" }),
        })
    }
}

/// Each entry: (label, sql, expected_route, why). Real-mode workload.
const REAL_QUERIES: &[(&str, &str, &str, &str)] = &[
    (
        "pure expression",
        "SELECT 1 + 1 AS answer",
        "lake",
        "no tables → router routes to lake (DuckDB computes locally)",
    ),
    (
        "translated SELECT",
        "SELECT IFF(value > 0, 'p', 'n') AS sign, \
                DATEADD(day, 7, ts)        AS week_later \
         FROM analytics.public.events LIMIT 5",
        "lake (if table exists) | snowflake (otherwise)",
        "IFF → CASE WHEN, DATEADD(day, …) → DATEADD('day', …)",
    ),
    (
        "write statement",
        "INSERT INTO analytics.public.events (id, value, ts) \
         VALUES (DEFAULT, 1, CURRENT_TIMESTAMP())",
        "snowflake",
        "writes always passthrough (§4.7 is_write)",
    ),
    (
        "snowflake-only feature",
        "SELECT table_schema, table_name \
         FROM information_schema.tables LIMIT 5",
        "snowflake",
        "INFORMATION_SCHEMA → UsesSnowflakeFeature (§A.10)",
    ),
];

/// Seed-mode workload — runs against the canned TPC-H sf=0.01 fixture
/// provisioned by `melt sessions seed`. The acceptance criterion calls
/// for ≥3 lake-routed queries; we exceed that and add an explicit
/// boundary case so operators see what seed mode refuses.
const SEED_QUERIES: &[(&str, &str, &str, &str)] = &[
    (
        "row count",
        "SELECT COUNT(*) AS n FROM TPCH.SF01.lineitem",
        "lake",
        "fully-qualified Lake table; router strips DB prefix to local schema",
    ),
    (
        "small projection",
        "SELECT n_nationkey, n_name FROM TPCH.SF01.nation \
         ORDER BY n_nationkey LIMIT 5",
        "lake",
        "tiny TPC-H reference table — pulled from the local DuckDB fixture",
    ),
    (
        "aggregate",
        "SELECT o_orderstatus, COUNT(*) AS n FROM TPCH.SF01.orders GROUP BY 1",
        "lake",
        "GROUP BY against the orders fact table",
    ),
    (
        "seed boundary",
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES",
        "seed-mode-unsupported (HTTP 422)",
        "INFORMATION_SCHEMA would route to upstream — seed mode refuses cleanly",
    ),
];

fn header(s: &str) {
    let bar = "─".repeat(s.chars().count() + 2);
    println!("\n┌{bar}┐\n│ {s} │\n└{bar}┘");
}

fn render_row(row: &SnowflakeRow) -> String {
    // Decode every cell as a String for display; failures become
    // "<unrenderable>". This is a demo, not the place to be type-clever.
    let cells: Vec<String> = row
        .column_types()
        .iter()
        .map(|col| {
            row.get::<String>(col.name())
                .unwrap_or_else(|_| "<unrenderable>".into())
        })
        .collect();
    format!("[{}]", cells.join(", "))
}

async fn run_one(
    session: &SnowflakeSession,
    label: &str,
    sql: &str,
    expected: &str,
    why: &str,
) {
    println!("\n┄┄ {label} ({expected})");
    println!("   ─ {why}");
    for line in sql.lines() {
        println!("   > {}", line.trim());
    }
    match session.query(sql).await {
        Ok(rows) => {
            println!("   ← {} row(s)", rows.len());
            for row in rows.iter().take(5) {
                println!("     {}", render_row(row));
            }
        }
        Err(e) => {
            println!("   ✗ {e}");
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let s = Settings::from_env()?;

    header(&format!(
        "Connecting to Melt at {}://{}:{} as {}@{}",
        s.melt_protocol, s.melt_host, s.melt_port, s.user, s.account
    ));

    let config = SnowflakeClientConfig {
        account: s.account.clone(),
        warehouse: None,
        database: Some(s.database.clone()),
        schema: Some(s.schema.clone()),
        role: None,
        timeout: Some(Duration::from_secs(60)),
    };

    let client = SnowflakeClient::new(
        &s.user,
        SnowflakeAuthMethod::Password(s.password.clone()),
        config,
    )?
    // ── This is the only call that differs from a normal Snowflake
    //    connection: redirect every request to Melt's listener instead
    //    of <account>.snowflakecomputing.com. The `account` field is
    //    still the real Snowflake account locator — Melt forwards it
    //    inside the login body when it talks to upstream.
    .with_address(&s.melt_host, Some(s.melt_port), Some(s.melt_protocol.clone()))?;

    let session = match client.create_session().await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("\n✗ login failed: {e}");
            eprintln!(
                "\n  This usually means Melt forwarded the login to the configured\n\
                   Snowflake account and that account isn't reachable. Set\n\
                   SNOWFLAKE_ACCOUNT to a real account locator, or use\n\
                   `melt route '<sql>'` for offline routing tests.\n"
            );
            return Err(anyhow!("login failed"));
        }
    };

    let queries = if env::var("MELT_MODE")
        .map(|v| v.eq_ignore_ascii_case("seed"))
        .unwrap_or(false)
    {
        SEED_QUERIES
    } else {
        REAL_QUERIES
    };
    for (label, sql, expected, why) in queries {
        run_one(&session, label, sql, expected, why).await;
    }

    header("Done");
    println!("Tail Melt's logs to see the routing decisions:");
    println!("  docker compose logs -f melt | grep statement_complete");
    Ok(())
}
