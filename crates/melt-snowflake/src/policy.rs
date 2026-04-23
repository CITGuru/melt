use melt_core::{PolicyKind, ProtectedTable, TableRef};

/// Account-wide scan for tables carrying row-access, masking,
/// aggregation, or projection policies. Requires
/// `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE` (typically granted
/// once by ACCOUNTADMIN). Note `ACCOUNT_USAGE` has 45min–2h latency
/// — acceptable here because policy changes are human-scale.
///
/// We avoid `INFORMATION_SCHEMA.POLICY_REFERENCES(POLICY_KIND => …)`
/// because that signature only accepts network-policy kinds; using
/// it for table policies returned empty rows and silently defeated
/// passthrough-mode protection.
pub fn list_policy_protected_tables_query() -> &'static str {
    "SELECT
        ref_database_name AS database,
        ref_schema_name   AS schema,
        ref_entity_name   AS name,
        policy_name,
        policy_kind
     FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
     WHERE policy_kind IN (
               'ROW_ACCESS_POLICY',
               'MASKING_POLICY',
               'AGGREGATION_POLICY',
               'PROJECTION_POLICY'
           )
       AND ref_entity_domain IN ('TABLE', 'VIEW', 'MATERIALIZED_VIEW', 'EXTERNAL_TABLE')
       AND policy_status    = 'ACTIVE'"
}

/// Parse the JSON rowset shape Snowflake returns from
/// `/api/v2/statements`. Defensive — unknown shape returns an empty
/// vec rather than blowing up sync.
pub fn parse_policy_rowset(value: &serde_json::Value) -> Vec<ProtectedTable> {
    let Some(data) = value.get("data").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(data.len());
    for row in data {
        let Some(row) = row.as_array() else { continue };
        if row.len() < 5 {
            continue;
        }
        let database = row[0].as_str().unwrap_or_default().to_string();
        let schema = row[1].as_str().unwrap_or_default().to_string();
        let name = row[2].as_str().unwrap_or_default().to_string();
        let policy_name = row[3].as_str().unwrap_or_default().to_string();
        let kind = match row[4]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "row_access_policy" | "row_access" => PolicyKind::RowAccess,
            "column_masking" => PolicyKind::ColumnMasking,
            _ => PolicyKind::Masking,
        };
        if database.is_empty() || schema.is_empty() || name.is_empty() {
            continue;
        }
        out.push(ProtectedTable {
            table: TableRef::new(database, schema, name),
            policy_name,
            policy_kind: kind,
            policy_body: None,
        });
    }
    out
}

/// Snowflake SQL to read the body of a row-access policy. Only used
/// when `PolicyMode::Enforce` is active; the sync subsystem fetches
/// each marked policy's body and feeds it into
/// [`crate::policy_dsl::translate`].
pub fn describe_row_access_policy_sql(policy_fqn: &str) -> String {
    format!("DESCRIBE ROW ACCESS POLICY {policy_fqn}")
}
