use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use melt_core::config::{ProxyConfig, RouterConfig};
use melt_core::{PolicyConfig, SyncConfig};
use melt_snowflake::SnowflakeConfig;
use serde::Deserialize;

/// Top-level `melt.toml` shape. Backend selection happens via the
/// `[backend.ducklake]` / `[backend.iceberg]` sub-tables, captured
/// here as opaque tomls and resolved by `ActiveBackend::from_toml`.
#[derive(Debug, Deserialize)]
pub struct MeltConfig {
    pub proxy: ProxyConfig,
    pub snowflake: SnowflakeConfig,
    #[serde(default)]
    pub router: RouterConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub sync: SyncConfig,
    /// Parsed but read only via `MeltConfig::peek_max_blocking_threads`,
    /// which deserializes a smaller view before the runtime is built.
    /// Keeping the field on the full struct means an operator can set
    /// it once and either entry point (early peek or full load) sees
    /// the value without surprise warnings about unknown TOML keys.
    #[serde(default)]
    #[allow(dead_code)]
    pub runtime: RuntimeConfig,
    pub backend: BackendTable,
}

/// Tunables for the Tokio runtime built in `main`. Optional —
/// defaults are picked in `crate::runtime_init` so this block can be
/// omitted entirely from `melt.toml`.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct RuntimeConfig {
    /// Cap on Tokio's blocking thread pool. Default `64`. See
    /// `docs/internal/KNOWN_ISSUES.md` § KI-001 — timed-out DuckDB
    /// queries pin their `spawn_blocking` thread, so we cap below
    /// the Tokio default of `512` to bound the worst-case pin count.
    /// `MELT_MAX_BLOCKING_THREADS` overrides this for ad-hoc tuning.
    pub max_blocking_threads: Option<usize>,
}

/// Minimal view of `melt.toml` used by the early runtime builder
/// before `MeltConfig::load` runs. We only need the `[runtime]`
/// block; loading the full config requires backend selection, which
/// we don't want to force on operators just to set a thread cap.
#[derive(Debug, Deserialize, Default)]
struct RuntimeConfigPeek {
    #[serde(default)]
    runtime: RuntimeConfig,
}

#[derive(Debug, Deserialize, Default)]
pub struct MetricsConfig {
    pub listen: Option<std::net::SocketAddr>,
    #[serde(default)]
    pub log_format: melt_metrics::LogFormat,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub admin_token: String,
    #[serde(default)]
    pub admin_token_file: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Deserialize, Default)]
pub struct BackendTable {
    #[cfg(feature = "ducklake")]
    pub ducklake: Option<melt_ducklake::DuckLakeConfig>,
    #[cfg(feature = "iceberg")]
    pub iceberg: Option<melt_iceberg::IcebergConfig>,
}

#[derive(Debug)]
pub enum ActiveBackend {
    #[cfg(feature = "ducklake")]
    DuckLake(melt_ducklake::DuckLakeConfig),
    #[cfg(feature = "iceberg")]
    Iceberg(melt_iceberg::IcebergConfig),
}

impl MeltConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("reading melt config at {}", path.display()))?;
        let cfg: MeltConfig = toml::from_str(&raw).context("parsing melt.toml")?;
        Ok(cfg)
    }

    /// Resolve the path to a Melt config file without loading it.
    ///
    /// Precedence (first hit wins):
    ///
    /// 1. `cli` — the explicit `--config <path>` argument. If set, MUST
    ///    exist; errors otherwise. Never falls back.
    /// 2. `$MELT_CONFIG` — env var override. Same contract as (1);
    ///    errors if set but the file is missing, because a silent
    ///    fallback past an intentional override hides misconfiguration.
    /// 3. `./melt.local.toml` — per-operator local override, gitignored
    ///    by convention. Lets you run `melt all` in the repo root
    ///    without flags.
    /// 4. `./melt.toml` — project-root template.
    /// 5. `$XDG_CONFIG_HOME/melt/melt.toml` (or
    ///    `$HOME/.config/melt/melt.toml` when `XDG_CONFIG_HOME` is
    ///    unset).
    /// 6. `$HOME/.melt/melt.toml` — legacy dotfile location.
    ///
    /// Returns the first path that both exists and is readable. When
    /// no source is configured and none of the implicit paths exist,
    /// surfaces an error listing every location we checked so the
    /// operator can tell immediately which file they meant to create.
    pub fn resolve_path(cli: Option<&str>) -> Result<PathBuf> {
        if let Some(p) = cli {
            let p = PathBuf::from(p);
            if !p.exists() {
                return Err(anyhow!("--config {} does not exist", p.display()));
            }
            return Ok(p);
        }

        if let Ok(env_path) = std::env::var("MELT_CONFIG") {
            if env_path.is_empty() {
                // Empty MELT_CONFIG = unset; fall through to implicit paths.
            } else {
                let p = PathBuf::from(&env_path);
                if !p.exists() {
                    return Err(anyhow!("MELT_CONFIG={} does not exist", p.display()));
                }
                return Ok(p);
            }
        }

        let mut searched: Vec<PathBuf> = Vec::new();
        let try_path = |p: PathBuf, out: &mut Vec<PathBuf>| -> Option<PathBuf> {
            if p.is_file() {
                Some(p)
            } else {
                out.push(p);
                None
            }
        };

        if let Some(hit) = try_path(PathBuf::from("./melt.local.toml"), &mut searched) {
            return Ok(hit);
        }
        if let Some(hit) = try_path(PathBuf::from("./melt.toml"), &mut searched) {
            return Ok(hit);
        }

        // Resolve XDG_CONFIG_HOME or HOME (treat empty env values as unset).
        let non_empty = |v: std::ffi::OsString| {
            if v.is_empty() {
                None
            } else {
                Some(PathBuf::from(v))
            }
        };
        let home = std::env::var_os("HOME").and_then(non_empty);
        let xdg_base: Option<PathBuf> = std::env::var_os("XDG_CONFIG_HOME")
            .and_then(non_empty)
            .or_else(|| home.as_ref().map(|h| h.join(".config")));
        if let Some(base) = xdg_base {
            if let Some(hit) = try_path(base.join("melt/melt.toml"), &mut searched) {
                return Ok(hit);
            }
        }

        if let Some(home) = home {
            if let Some(hit) = try_path(home.join(".melt/melt.toml"), &mut searched) {
                return Ok(hit);
            }
        }

        let list = searched
            .iter()
            .map(|p| format!("  - {}", p.display()))
            .collect::<Vec<_>>()
            .join("\n");
        Err(anyhow!(
            "no melt config found. Pass `--config <path>`, set \
             MELT_CONFIG=<path>, or create one at any of:\n{list}"
        ))
    }

    /// Resolve which backend is active. The TOML file MUST contain
    /// exactly one of `[backend.ducklake]` or `[backend.iceberg]`.
    pub fn active_backend(&self) -> Result<ActiveBackend> {
        let mut chosen: Vec<&'static str> = Vec::new();
        #[cfg(feature = "ducklake")]
        if self.backend.ducklake.is_some() {
            chosen.push("ducklake");
        }
        #[cfg(feature = "iceberg")]
        if self.backend.iceberg.is_some() {
            chosen.push("iceberg");
        }

        match chosen.as_slice() {
            [] => Err(anyhow!(
                "no backend configured — set either [backend.ducklake] or [backend.iceberg] in melt.toml"
            )),
            [_, _, ..] => Err(anyhow!(
                "multiple backends configured ({chosen:?}); choose exactly one"
            )),
            _ => {
                #[cfg(feature = "ducklake")]
                if let Some(dl) = &self.backend.ducklake {
                    return Ok(ActiveBackend::DuckLake(dl.clone()));
                }
                #[cfg(feature = "iceberg")]
                if let Some(ib) = &self.backend.iceberg {
                    return Ok(ActiveBackend::Iceberg(ib.clone()));
                }
                Err(anyhow!("backend table present but feature disabled at build time"))
            }
        }
    }

    #[allow(dead_code)]
    pub fn policy(&self) -> &PolicyConfig {
        &self.snowflake.policy
    }

    /// Best-effort read of `[runtime].max_blocking_threads` from a
    /// `melt.toml` without loading the rest of the config.
    ///
    /// Returns `None` when the file is missing, can't be parsed, or
    /// doesn't set the field. Errors are swallowed deliberately:
    /// `bootstrap server` and `bootstrap client` run before any
    /// config exists, and the runtime builder must still come up
    /// with a sane default.
    pub fn peek_max_blocking_threads(path: &Path) -> Option<usize> {
        let raw = fs::read_to_string(path).ok()?;
        let peek: RuntimeConfigPeek = toml::from_str(&raw).ok()?;
        peek.runtime.max_blocking_threads
    }
}
