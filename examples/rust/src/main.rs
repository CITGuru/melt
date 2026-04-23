//! Demo: query a local Melt proxy using `snowflake-connector-rs`.
//!
//! The connector is the unmodified upstream crate; only the
//! `with_address(host, port, protocol)` builder call differs from a
//! normal Snowflake connection. Each query exercises a different
//! routing path so you can watch Melt's logs and confirm the proxy
//! is making the decisions you expect.
//!
//! Run:
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
        // of them with env vars.
        let req = |k: &str| env::var(k).map_err(|_| anyhow!("missing required env var: {k}"));
        Ok(Self {
            melt_host: env::var("MELT_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            melt_port: env::var("MELT_PORT")
                .unwrap_or_else(|_| "8443".into())
                .parse()
                .context("MELT_PORT must be a u16")?,
            melt_protocol: env::var("MELT_PROTOCOL").unwrap_or_else(|_| "http".into()),
            account: req("SNOWFLAKE_ACCOUNT")?,
            user: req("SNOWFLAKE_USER")?,
            password: req("SNOWFLAKE_PASSWORD")?,
            database: env::var("SNOWFLAKE_DATABASE").unwrap_or_else(|_| "ANALYTICS".into()),
            schema: env::var("SNOWFLAKE_SCHEMA").unwrap_or_else(|_| "PUBLIC".into()),
        })
    }
}

/// Each entry: (label, sql, expected_route, why)
const QUERIES: &[(&str, &str, &str, &str)] = &[
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

    for (label, sql, expected, why) in QUERIES {
        run_one(&session, label, sql, expected, why).await;
    }

    header("Done");
    println!("Tail Melt's logs to see the routing decisions:");
    println!("  docker compose logs -f melt | grep statement_complete");
    Ok(())
}
