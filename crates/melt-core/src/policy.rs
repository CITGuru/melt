use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::table::TableRef;

/// How Melt honors Snowflake's row access / masking / column-level
/// policies.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum PolicyMode {
    /// Sync periodically marks every table whose Snowflake side has a
    /// row access policy, masking policy, or column-level masking. The
    /// router refuses to route queries touching marked tables to Lake.
    /// Safe default.
    #[default]
    Passthrough,

    /// Default-deny. Only the operator-curated list of tables is
    /// eligible for Lake routing. Most conservative posture.
    AllowList { tables: Vec<TableRef> },

    /// Aspirational. Project policies into Lake as parameterized views
    /// and rewrite queries. **Not yet implemented** — the CLI rejects
    /// this mode at startup.
    Enforce,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PolicyConfig {
    #[serde(flatten)]
    pub mode: PolicyMode,

    /// How often sync rescans Snowflake's policy references.
    #[serde(with = "humantime_serde", default = "default_refresh")]
    pub refresh_interval: Duration,
}

fn default_refresh() -> Duration {
    Duration::from_secs(60)
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            mode: PolicyMode::default(),
            refresh_interval: default_refresh(),
        }
    }
}

/// What kind of Snowflake policy is attached to a table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyKind {
    RowAccess,
    Masking,
    ColumnMasking,
}

impl PolicyKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            PolicyKind::RowAccess => "row_access",
            PolicyKind::Masking => "masking",
            PolicyKind::ColumnMasking => "column_masking",
        }
    }
}

/// A table that Snowflake reports as protected by some policy.
///
/// `policy_body` is `Some` only when sync was able to fetch the
/// underlying expression (`DESCRIBE ROW ACCESS POLICY ...`) for use
/// in `PolicyMode::Enforce`. The two passthrough modes don't need it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProtectedTable {
    pub table: TableRef,
    pub policy_name: String,
    pub policy_kind: PolicyKind,
    #[serde(default)]
    pub policy_body: Option<String>,
}
