use melt_core::MeltError;

/// HTTP-shaped error coming back from Snowflake. We keep the raw code
/// so the proxy can re-emit it verbatim to drivers that depend on
/// specific Snowflake error codes (e.g. 391918 for "statement handle
/// not found").
#[derive(Debug, thiserror::Error)]
#[error("Snowflake API error ({code}): {message}")]
pub struct SnowflakeApiError {
    pub code: String,
    pub message: String,
    pub http_status: u16,
}

/// Map a `MeltError` to the Snowflake error code drivers expect.
pub fn snowflake_code(e: &MeltError) -> &'static str {
    match e {
        MeltError::Parse(_) | MeltError::Translate(_) => "1003",
        MeltError::HandleNotFound => "391918",
        MeltError::TooManyStatements => "000625",
        MeltError::Timeout | MeltError::Cancelled => "604",
        MeltError::BackendUnavailable(_) => "000629",
        MeltError::Unauthorized => "390104",
        MeltError::AccountMismatch { .. } => "390201",
        MeltError::Snowflake { code, .. } => Box::leak(code.clone().into_boxed_str()),
        _ => "999999",
    }
}

/// Map a `MeltError` to the appropriate HTTP status code.
pub fn http_status(e: &MeltError) -> u16 {
    match e {
        MeltError::Parse(_) | MeltError::Translate(_) => 400,
        MeltError::HandleNotFound => 404,
        MeltError::TooManyStatements => 429,
        MeltError::Timeout => 504,
        MeltError::Cancelled => 200, // cancel returns 200 with code 604
        MeltError::Unauthorized => 401,
        MeltError::AccountMismatch { .. } => 400,
        MeltError::BackendUnavailable(_) => 503,
        MeltError::Snowflake { .. } => 502,
        _ => 500,
    }
}
