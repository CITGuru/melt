//! Hot-reload response types shared between `melt-metrics` (the
//! endpoint) and `melt-cli` (the subcommand wrapper).

use serde::{Deserialize, Serialize};

/// Wire shape returned by `POST /admin/reload`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReloadResponse {
    /// True when every subsystem accepted the new config. `false`
    /// when validation failed (in which case no subsystem was
    /// mutated).
    pub ok: bool,

    /// Populated on validation failure. Each entry names the field
    /// and why it was rejected.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<ReloadError>,

    /// Per-field diff of what actually changed in memory.
    #[serde(default)]
    pub changes: serde_json::Value,

    /// Fields that changed in the file but weren't applied because
    /// the subsystem requires a restart.
    #[serde(default)]
    pub skipped: Vec<SkippedField>,

    /// How long the validate + apply cycle took.
    #[serde(default)]
    pub duration_ms: u64,
}

impl ReloadResponse {
    pub fn ok(changes: serde_json::Value, skipped: Vec<SkippedField>, duration_ms: u64) -> Self {
        Self {
            ok: true,
            errors: Vec::new(),
            changes,
            skipped,
            duration_ms,
        }
    }

    pub fn validation_failure(errors: Vec<ReloadError>, duration_ms: u64) -> Self {
        Self {
            ok: false,
            errors,
            changes: serde_json::Value::Null,
            skipped: Vec::new(),
            duration_ms,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReloadError {
    pub field: String,
    pub error: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SkippedField {
    pub field: String,
    pub reason: String,
}
