use std::fs;
use std::time::Duration;

use melt_core::{MeltError, PolicyConfig, Result};
use serde::{Deserialize, Serialize};

/// Build the DuckDB SQL that installs the community Snowflake
/// extension and attaches it as `sf_link` for the dual-execution
/// router's Attach strategy.
///
/// Returned SQL is appended to the iceberg/ducklake pool's per-
/// connection setup. Failure modes are caught upstream by
/// `tracing::warn!` in the pool — if the extension isn't available
/// or the credentials are invalid, individual hybrid queries fall
/// back to passthrough via the first-batch-error path.
///
/// Returns `None` when the operator hasn't configured credentials
/// the extension can use (no PAT / private key — both required by
/// the community extension). The plan calls this out as the
/// `attach_loaded = false` case in §8.2.
pub fn sf_link_attach_sql(cfg: &SnowflakeConfig) -> Option<String> {
    // The community Snowflake extension supports PAT auth or RSA key
    // pair auth, mirroring SnowflakeClient. Fail to None if neither
    // is configured — the operator wants hybrid but doesn't have
    // creds to set up sf_link.
    let has_pat = !cfg.pat.is_empty() || !cfg.pat_file.is_empty();
    let has_key = !cfg.private_key_file.is_empty() || !cfg.private_key.is_empty();
    if !has_pat && !has_key {
        return None;
    }

    // The community Snowflake extension requires a DATABASE field
    // on the secret — it's the default DB for sessions opened
    // through the attach. Use the configured database, falling back
    // to the account literal so the secret still constructs even
    // when no explicit database is set (the extension still allows
    // qualified `<db>.<schema>.<table>` references regardless).
    let database = if cfg.database.is_empty() {
        // Snowflake always has SNOWFLAKE_SAMPLE_DATA / a default DB
        // for any service user; SNOWFLAKE itself is read-only and
        // present on every account, making it a safe fallback.
        "SNOWFLAKE"
    } else {
        cfg.database.as_str()
    };

    let mut secret = String::from(
        "CREATE OR REPLACE SECRET sf_link (\n\
             TYPE SNOWFLAKE,\n\
             ACCOUNT '",
    );
    secret.push_str(&escape_sql(&cfg.account));
    secret.push_str("',\n    DATABASE '");
    secret.push_str(&escape_sql(database));
    secret.push_str("'");
    if !cfg.user.is_empty() {
        secret.push_str(",\n    USER '");
        secret.push_str(&escape_sql(&cfg.user));
        secret.push_str("'");
    }
    if !cfg.role.is_empty() {
        secret.push_str(",\n    ROLE '");
        secret.push_str(&escape_sql(&cfg.role));
        secret.push_str("'");
    }
    if !cfg.warehouse.is_empty() {
        secret.push_str(",\n    WAREHOUSE '");
        secret.push_str(&escape_sql(&cfg.warehouse));
        secret.push_str("'");
    }
    if !cfg.schema.is_empty() {
        secret.push_str(",\n    SCHEMA '");
        secret.push_str(&escape_sql(&cfg.schema));
        secret.push_str("'");
    }
    if !cfg.private_key_file.is_empty() {
        secret.push_str(",\n    PRIVATE_KEY_PATH '");
        secret.push_str(&escape_sql(&cfg.private_key_file));
        secret.push_str("'");
    } else if !cfg.pat.is_empty() {
        // PAT goes in the password slot per the extension's auth model.
        secret.push_str(",\n    PASSWORD '");
        secret.push_str(&escape_sql(&cfg.pat));
        secret.push_str("'");
    }
    secret.push_str("\n);\n");

    // The community Snowflake extension only supports read-only
    // attaches today (writes go through the regular Snowflake
    // driver). Hybrid execution is read-only by design (writes are
    // passed through), so READ_ONLY is the right default — and
    // omitting it makes the ATTACH fail outright with "Snowflake
    // currently only supports read-only access".
    let attach = format!(
        "ATTACH '{database}' AS sf_link (TYPE SNOWFLAKE, SECRET sf_link, READ_ONLY);\n",
        database = escape_sql(database),
    );

    let mut out = String::from(
        "INSTALL snowflake FROM community;\n\
         LOAD snowflake;\n",
    );
    out.push_str(&secret);
    out.push_str(&attach);
    Some(out)
}

fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SnowflakeConfig {
    /// Snowflake account identifier — the value drivers send in the
    /// login body's `account` field. Snowflake exposes two formats:
    ///
    /// - **Org-account** (modern, recommended): `<orgname>-<accountname>`,
    ///   e.g. `ACMECORP-PROD123`. Region/cloud-abstracted, supports
    ///   failover. The default `host()` derivation works for this form.
    /// - **Account locator** (legacy): the bare locator, e.g. `xy12345`.
    ///   Always location-bound; the URL needs the region (and cloud
    ///   for non-AWS) appended. The default derivation **does not work**
    ///   for these — set `host` explicitly.
    pub account: String,

    /// Explicit upstream hostname. Set this when:
    ///
    /// - Your account is in the legacy locator form and lives outside
    ///   `us-west-2` (the only AWS region whose locator alone resolves):
    ///   `xy12345.us-east-1.snowflakecomputing.com`,
    ///   `abc123.east-us-2.azure.snowflakecomputing.com`, etc.
    /// - You're going through PrivateLink:
    ///   `<account>.privatelink.snowflakecomputing.com`.
    /// - You've put a custom CNAME in front of Snowflake.
    /// - You're doing the SAN-strict setup where the proxy itself
    ///   answers on `<account>.snowflakecomputing.com`.
    ///
    /// Empty = derive `<account>.snowflakecomputing.com`. Honest only
    /// for the org-account form.
    #[serde(default, alias = "host_override")]
    pub host: String,

    #[serde(with = "humantime_serde", default = "SnowflakeConfig::default_timeout")]
    pub request_timeout: Duration,

    #[serde(default = "SnowflakeConfig::default_retries")]
    pub max_retries: u8,

    // Service auth for sync (CDC + policy refresh): exactly one of
    // pat / pat_file / private_key / private_key_file. Optional
    // user/role/warehouse/database/schema override service user
    // DEFAULT_*. Ignored for proxy passthrough (drivers send their
    // own credentials).
    /// Programmatic Access Token — Snowflake-issued long-lived token
    /// bound to a user. Simplest path. Passed as a bearer on
    /// `/api/v2/statements` calls.
    #[serde(default)]
    pub pat: String,

    /// Path to a file whose contents is a PAT. File is read once at
    /// startup and again on token-cache refresh. Trailing whitespace
    /// is trimmed. Preferred for Kubernetes `Secret` volume mounts.
    #[serde(default)]
    pub pat_file: String,

    /// Inline RSA private key in PEM format (PKCS#8 or PKCS#1).
    /// Multi-line strings in TOML use triple quotes. Rarely used —
    /// prefer `private_key_file`. Requires `user` to be set.
    #[serde(default)]
    pub private_key: String,

    /// Path to a PEM-encoded RSA private key (PKCS#8 or PKCS#1). The
    /// matching public key must be registered on the service user
    /// via `ALTER USER X SET RSA_PUBLIC_KEY = '...'`. Each sync
    /// iteration may sign a fresh JWT from this key and exchange it
    /// for a session token. Production-grade; what Snowflake's docs
    /// lead with for service accounts.
    #[serde(default)]
    pub private_key_file: String,

    /// Snowflake login name of the service user. Required when using
    /// `private_key` / `private_key_file` (JWT signing needs to name
    /// the user). Ignored for PAT auth, since the PAT already
    /// identifies the user.
    #[serde(default)]
    pub user: String,

    /// Role sync's statements execute under. Optional — omit to use
    /// the service user's `DEFAULT_ROLE`. Explicit is safer: grant
    /// changes on the user's default role then take effect
    /// immediately on sync's next iteration.
    #[serde(default)]
    pub role: String,

    /// Warehouse sync's statements execute on. Optional — omit to
    /// use the service user's `DEFAULT_WAREHOUSE`. Set this so
    /// Snowflake compute billing for sync is isolated from user
    /// workloads (use a dedicated XSMALL warehouse with short
    /// `AUTO_SUSPEND`).
    #[serde(default)]
    pub warehouse: String,

    /// Default database for unqualified names in sync's statements.
    /// Sync's own SQL uses fully-qualified names, so this field
    /// rarely matters.
    #[serde(default)]
    pub database: String,

    /// Default schema likewise.
    #[serde(default)]
    pub schema: String,

    /// Drives the router and sync policy-refresh loops.
    #[serde(default)]
    pub policy: PolicyConfig,
}

impl SnowflakeConfig {
    fn default_timeout() -> Duration {
        Duration::from_secs(60)
    }
    fn default_retries() -> u8 {
        3
    }

    /// Resolve the upstream Snowflake hostname for the shared HTTP
    /// client. Returns `host` if set, otherwise derives
    /// `<account>.snowflakecomputing.com` — which is correct only for
    /// the org-account form. Operators using the legacy locator form,
    /// PrivateLink, or a custom CNAME must set `host` explicitly.
    pub fn host(&self) -> String {
        if self.host.is_empty() {
            format!("{}.snowflakecomputing.com", self.account)
        } else {
            self.host.clone()
        }
    }

    pub fn base_url(&self) -> String {
        format!("https://{}", self.host())
    }

    /// Resolve which service-auth path to use. Called by
    /// `SnowflakeClient::service_token()` on cache miss, and by the
    /// CLI at startup to surface config errors before any sync loop
    /// runs.
    ///
    /// Validation rules:
    /// - exactly one of `pat` / `pat_file` / `private_key` /
    ///   `private_key_file` must be non-empty
    /// - key-pair paths require `user` to be non-empty
    pub fn resolve_service_auth(&self) -> Result<ServiceAuth> {
        let set: Vec<&str> = [
            ("pat", !self.pat.is_empty()),
            ("pat_file", !self.pat_file.is_empty()),
            ("private_key", !self.private_key.is_empty()),
            ("private_key_file", !self.private_key_file.is_empty()),
        ]
        .iter()
        .filter(|(_, b)| *b)
        .map(|(n, _)| *n)
        .collect();

        match set.as_slice() {
            [] => Err(MeltError::config(
                "no service credentials: set one of pat / pat_file / \
                 private_key / private_key_file in [snowflake]",
            )),
            ["pat"] => Ok(ServiceAuth::Pat(self.pat.clone())),
            ["pat_file"] => {
                let pat = fs::read_to_string(&self.pat_file).map_err(|e| {
                    MeltError::config(format!("read pat_file '{}': {e}", self.pat_file))
                })?;
                Ok(ServiceAuth::Pat(pat.trim().to_string()))
            }
            [pk] if *pk == "private_key" || *pk == "private_key_file" => {
                if self.user.is_empty() {
                    return Err(MeltError::config(
                        "[snowflake].user is required when using \
                         private_key / private_key_file (JWT signing \
                         names the service user explicitly)",
                    ));
                }
                let pem_bytes = if *pk == "private_key" {
                    self.private_key.clone().into_bytes()
                } else {
                    fs::read(&self.private_key_file).map_err(|e| {
                        MeltError::config(format!(
                            "read private_key_file '{}': {e}",
                            self.private_key_file
                        ))
                    })?
                };
                Ok(ServiceAuth::KeyPair {
                    pem_bytes,
                    user: self.user.clone(),
                })
            }
            multiple => Err(MeltError::config(format!(
                "multiple service credentials set ({}); pick exactly one",
                multiple.join(", ")
            ))),
        }
    }
}

/// Resolved service-auth credential. Produced by
/// [`SnowflakeConfig::resolve_service_auth`] and consumed by
/// [`crate::client::SnowflakeClient::service_token`].
#[derive(Clone, Debug)]
pub enum ServiceAuth {
    /// Long-lived Programmatic Access Token. Used directly as the
    /// bearer on `/api/v2/statements`.
    Pat(String),

    /// RSA private key (PEM bytes) + Snowflake login name. The
    /// service-token path signs a fresh JWT and exchanges it for a
    /// 1-hour session token on cache miss.
    KeyPair { pem_bytes: Vec<u8>, user: String },
}

impl Default for SnowflakeConfig {
    fn default() -> Self {
        Self {
            account: String::new(),
            host: String::new(),
            request_timeout: Self::default_timeout(),
            max_retries: Self::default_retries(),
            pat: String::new(),
            pat_file: String::new(),
            private_key: String::new(),
            private_key_file: String::new(),
            user: String::new(),
            role: String::new(),
            warehouse: String::new(),
            database: String::new(),
            schema: String::new(),
            policy: PolicyConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn host_derives_from_account_when_unset() {
        let cfg = SnowflakeConfig {
            account: "ACMECORP-PROD123".into(),
            ..Default::default()
        };
        assert_eq!(cfg.host(), "ACMECORP-PROD123.snowflakecomputing.com");
    }

    #[test]
    fn explicit_host_wins_over_derivation() {
        let cfg = SnowflakeConfig {
            account: "xy12345".into(),
            host: "xy12345.us-east-1.snowflakecomputing.com".into(),
            ..Default::default()
        };
        assert_eq!(cfg.host(), "xy12345.us-east-1.snowflakecomputing.com");
    }

    #[test]
    fn legacy_host_override_alias_still_parses() {
        let raw = r#"
            account = "xy12345"
            host_override = "xy12345.privatelink.snowflakecomputing.com"
        "#;
        let cfg: SnowflakeConfig = toml::from_str(raw).expect("parses");
        assert_eq!(cfg.host(), "xy12345.privatelink.snowflakecomputing.com");
    }

    #[test]
    fn resolve_service_auth_empty_errors() {
        let cfg = SnowflakeConfig {
            account: "ACMECORP-PROD123".into(),
            ..Default::default()
        };
        let err = cfg.resolve_service_auth().unwrap_err();
        match err {
            MeltError::Config(msg) => assert!(msg.contains("no service credentials")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn resolve_service_auth_pat_inline() {
        let cfg = SnowflakeConfig {
            account: "ACMECORP-PROD123".into(),
            pat: "my-pat-value".into(),
            ..Default::default()
        };
        match cfg.resolve_service_auth().unwrap() {
            ServiceAuth::Pat(p) => assert_eq!(p, "my-pat-value"),
            other => panic!("expected Pat, got {other:?}"),
        }
    }

    #[test]
    fn resolve_service_auth_pat_file_trims() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "  pat-from-file  ").unwrap();
        let cfg = SnowflakeConfig {
            account: "ACMECORP-PROD123".into(),
            pat_file: f.path().to_string_lossy().into(),
            ..Default::default()
        };
        match cfg.resolve_service_auth().unwrap() {
            ServiceAuth::Pat(p) => assert_eq!(p, "pat-from-file"),
            other => panic!("expected Pat, got {other:?}"),
        }
    }

    #[test]
    fn resolve_service_auth_multiple_errors() {
        let cfg = SnowflakeConfig {
            account: "ACMECORP-PROD123".into(),
            pat: "a".into(),
            private_key: "b".into(),
            user: "u".into(),
            ..Default::default()
        };
        let err = cfg.resolve_service_auth().unwrap_err();
        match err {
            MeltError::Config(msg) => {
                assert!(msg.contains("multiple service credentials"));
                assert!(msg.contains("pat"));
                assert!(msg.contains("private_key"));
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn resolve_service_auth_keypair_requires_user() {
        let cfg = SnowflakeConfig {
            account: "ACMECORP-PROD123".into(),
            private_key: "-----BEGIN PRIVATE KEY-----\n...\n".into(),
            ..Default::default()
        };
        let err = cfg.resolve_service_auth().unwrap_err();
        match err {
            MeltError::Config(msg) => assert!(msg.contains("[snowflake].user is required")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn resolve_service_auth_keypair_inline_returns_bytes() {
        let cfg = SnowflakeConfig {
            account: "ACMECORP-PROD123".into(),
            private_key: "-----BEGIN PRIVATE KEY-----\nFAKE\n".into(),
            user: "MELT_SYNC_USER".into(),
            ..Default::default()
        };
        match cfg.resolve_service_auth().unwrap() {
            ServiceAuth::KeyPair { pem_bytes, user } => {
                assert_eq!(user, "MELT_SYNC_USER");
                assert!(pem_bytes.starts_with(b"-----BEGIN"));
            }
            other => panic!("expected KeyPair, got {other:?}"),
        }
    }
}
