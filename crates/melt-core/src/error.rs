use thiserror::Error;

use crate::table::TableRef;

/// The unified error type for every Melt crate. Designed so the
/// proxy can map any variant to a Snowflake-shaped error response
/// (see `melt_snowflake::errors`).
#[derive(Debug, Error)]
pub enum MeltError {
    #[error("config error: {0}")]
    Config(String),

    #[error("SQL parse failed: {0}")]
    Parse(String),

    #[error("dialect translation failed: {0}")]
    Translate(String),

    #[error("Snowflake HTTP error ({code}): {message}")]
    Snowflake { code: String, message: String },

    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("upstream HTTP error: {0}")]
    Http(String),

    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("request timed out")]
    Timeout,

    #[error("request cancelled")]
    Cancelled,

    /// Maps to Snowflake error code 391918.
    #[error("statement handle not found")]
    HandleNotFound,

    /// Maps to Snowflake error code 000625.
    #[error("too many concurrent statements")]
    TooManyStatements,

    #[error("backend unavailable: {0}")]
    BackendUnavailable(String),

    #[error("unauthorized")]
    Unauthorized,

    /// Driver supplied a Snowflake account name that doesn't match
    /// the one Melt is configured to forward to. We refuse to silently
    /// route a driver to the wrong upstream — operator misconfig
    /// should be loud, not mysterious. Maps to Snowflake error code
    /// 390201 ("incorrect account").
    #[error("ACCOUNT_NAME '{supplied}' does not match Melt's configured upstream '{configured}'")]
    AccountMismatch {
        configured: String,
        supplied: String,
    },

    /// Caller asked for an operation that requires upstream Snowflake
    /// while the proxy is running in seed mode. We refuse rather than
    /// silently failing — the demo path is read-only, lake-only, and
    /// only populated with the canned TPC-H fixture. Real workloads
    /// belong on `mode = "real"`. See `docs/SEED_MODE.md`.
    #[error("seed mode does not support this operation: {0} — switch to `mode = \"real\"` (see docs/SEED_MODE.md)")]
    SeedModeUnsupported(String),

    #[error("other: {0}")]
    Other(String),
}

impl MeltError {
    pub fn config(msg: impl Into<String>) -> Self {
        MeltError::Config(msg.into())
    }
    pub fn backend(msg: impl Into<String>) -> Self {
        MeltError::Backend(msg.into())
    }
    pub fn parse(msg: impl Into<String>) -> Self {
        MeltError::Parse(msg.into())
    }
    pub fn translate(msg: impl Into<String>) -> Self {
        MeltError::Translate(msg.into())
    }
    pub fn other(msg: impl Into<String>) -> Self {
        MeltError::Other(msg.into())
    }
    pub fn seed_unsupported(msg: impl Into<String>) -> Self {
        MeltError::SeedModeUnsupported(msg.into())
    }
}

pub type Result<T> = std::result::Result<T, MeltError>;

/// Catalog errors are kept in their own enum so DuckDB / Postgres
/// dependencies don't leak through `melt-core`.
#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("not found: {0}")]
    NotFound(TableRef),
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error("other: {0}")]
    Other(String),
}
