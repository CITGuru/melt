//! `melt-snowflake` — single Snowflake HTTP client used by every part
//! of Melt that talks to Snowflake (proxy passthrough, login forwarder,
//! sync CDC reader, policy refresher).

pub mod auth;
pub mod client;
pub mod config;
pub mod errors;
pub mod passthrough;
pub mod policy;
pub mod policy_dsl;
pub mod snowpipe;
pub mod statements;
pub mod stream;
pub mod views;

pub use auth::ServiceToken;
pub use client::{PassthroughResponse, SnowflakeClient, ViewDef};
pub use config::{sf_link_attach_sql, sf_link_refresh_sql, SnowflakeConfig};
pub use errors::{snowflake_code, SnowflakeApiError};
pub use melt_core::ObjectKind;
pub use policy::list_policy_protected_tables_query;
pub use stream::{ChangeAction, ChangeBatch, ChangeStream, SnapshotId};

/// True when a `read_stream_since` failure means the stream object
/// itself is unsalvageable and the caller must drop + re-bootstrap.
/// Matches three terminal Snowflake response shapes:
///
///   * `does not exist` / `002003` — object missing
///   * `stale` — STALE_AFTER elapsed (Snowflake 707/708)
///   * `offset` + `expired` — offset past source retention
///
/// Everything else (network, auth, perms) returns false so the
/// normal retry path handles transient hiccups instead of triggering
/// a destructive rebuild.
pub fn is_stream_unrecoverable(e: &melt_core::MeltError) -> bool {
    let msg = format!("{e}").to_ascii_lowercase();
    msg.contains("does not exist")
        || msg.contains("002003")
        || msg.contains("stale")
        || (msg.contains("offset") && msg.contains("expired"))
}
pub use views::{classify_view_body, translate_view_body, ViewBodyClassification, ViewBodyReason};
