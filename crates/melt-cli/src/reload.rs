//! Hot-reload closure builder. Shared by the DuckLake and Iceberg
//! runtime arms so `POST /admin/reload` behaves identically on both
//! backends.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use melt_core::{ReloadError, ReloadResponse, SkippedField, SyncConfig, SyncTableMatcher};
use melt_proxy::SharedMatcher;
use serde_json::json;

use crate::config::MeltConfig;

/// Captures everything the reload closure needs to re-read + re-apply
/// config. Cloneable because `ReloadFn` requires a `Fn`, not `FnMut`.
#[derive(Clone)]
pub struct ReloadCtx {
    pub config_path: PathBuf,
    pub matcher: SharedMatcher,
    /// Snapshot of the config at startup, used to compute a diff
    /// against the reloaded file. `ArcSwap` so later reloads see
    /// the most recent state.
    pub last_loaded: Arc<ArcSwap<SyncConfig>>,
}

impl ReloadCtx {
    pub fn new(config_path: PathBuf, matcher: SharedMatcher, initial: SyncConfig) -> Self {
        Self {
            config_path,
            matcher,
            last_loaded: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    /// Runs the full validate-then-apply cycle. On validation error
    /// no subsystem is mutated. On success, the matcher ArcSwap is
    /// replaced atomically.
    pub async fn reload(&self) -> ReloadResponse {
        let started = Instant::now();

        let raw = match tokio::fs::read_to_string(&self.config_path).await {
            Ok(s) => s,
            Err(e) => {
                return ReloadResponse::validation_failure(
                    vec![ReloadError {
                        field: "<file>".to_string(),
                        error: format!("reading {}: {e}", self.config_path.display()),
                    }],
                    started.elapsed().as_millis() as u64,
                );
            }
        };
        let new_cfg: MeltConfig = match toml::from_str(&raw) {
            Ok(c) => c,
            Err(e) => {
                return ReloadResponse::validation_failure(
                    vec![ReloadError {
                        field: "<parse>".to_string(),
                        error: e.to_string(),
                    }],
                    started.elapsed().as_millis() as u64,
                );
            }
        };

        let new_matcher = match SyncTableMatcher::from_config(&new_cfg.sync) {
            Ok(m) => Some(Arc::new(m)),
            Err(e) => {
                return ReloadResponse::validation_failure(
                    vec![ReloadError {
                        field: "sync".to_string(),
                        error: e.to_string(),
                    }],
                    started.elapsed().as_millis() as u64,
                );
            }
        };

        let old = self.last_loaded.load_full();
        let changes = diff_sync(&old, &new_cfg.sync);
        let skipped = skipped_fields();

        // Atomic ArcSwap; every in-flight request observes the new matcher after this point.
        self.matcher.store(Arc::new(new_matcher));
        self.last_loaded.store(Arc::new(new_cfg.sync.clone()));

        metrics::counter!(
            melt_metrics::ADMIN_RELOADS,
            melt_metrics::LABEL_OUTCOME => "ok",
        )
        .increment(1);

        ReloadResponse::ok(changes, skipped, started.elapsed().as_millis() as u64)
    }
}

/// Fields whose reload semantics are "restart required". Reported
/// to the operator so they know what the endpoint did NOT touch.
fn skipped_fields() -> Vec<SkippedField> {
    [
        ("proxy.listen", "socket rebind required"),
        ("proxy.tls_cert", "cert swap needs restart"),
        ("proxy.tls_key", "key swap needs restart"),
        ("snowflake.account", "upstream change — restart proxy"),
        ("snowflake.host", "upstream change — restart proxy"),
        ("backend.*.catalog_url", "connection pools hold state"),
        ("backend.*.s3", "DuckDB secret is built at pool creation"),
        ("metrics.listen", "admin listener rebind requires restart"),
    ]
    .into_iter()
    .map(|(field, reason)| SkippedField {
        field: field.to_string(),
        reason: reason.to_string(),
    })
    .collect()
}

fn diff_sync(old: &SyncConfig, new: &SyncConfig) -> serde_json::Value {
    let added_include: Vec<_> = new
        .include
        .iter()
        .filter(|p| !old.include.contains(p))
        .collect();
    let removed_include: Vec<_> = old
        .include
        .iter()
        .filter(|p| !new.include.contains(p))
        .collect();
    let added_exclude: Vec<_> = new
        .exclude
        .iter()
        .filter(|p| !old.exclude.contains(p))
        .collect();
    let removed_exclude: Vec<_> = old
        .exclude
        .iter()
        .filter(|p| !new.exclude.contains(p))
        .collect();

    let mut out = serde_json::Map::new();
    if !added_include.is_empty() || !removed_include.is_empty() {
        out.insert(
            "sync.include".to_string(),
            json!({
                "added": added_include,
                "removed": removed_include,
            }),
        );
    }
    if !added_exclude.is_empty() || !removed_exclude.is_empty() {
        out.insert(
            "sync.exclude".to_string(),
            json!({
                "added": added_exclude,
                "removed": removed_exclude,
            }),
        );
    }
    if old.auto_discover != new.auto_discover {
        out.insert(
            "sync.auto_discover".to_string(),
            json!({ "from": old.auto_discover, "to": new.auto_discover }),
        );
    }
    if old.lazy.auto_enable_change_tracking != new.lazy.auto_enable_change_tracking {
        out.insert(
            "sync.lazy.auto_enable_change_tracking".to_string(),
            json!({
                "from": old.lazy.auto_enable_change_tracking,
                "to":   new.lazy.auto_enable_change_tracking,
            }),
        );
    }
    if old.views.auto_include_dependencies != new.views.auto_include_dependencies {
        out.insert(
            "sync.views.auto_include_dependencies".to_string(),
            json!({
                "from": old.views.auto_include_dependencies,
                "to":   new.views.auto_include_dependencies,
            }),
        );
    }
    if old.views.prefer_stream_on_view != new.views.prefer_stream_on_view {
        out.insert(
            "sync.views.prefer_stream_on_view".to_string(),
            json!({
                "from": old.views.prefer_stream_on_view,
                "to":   new.views.prefer_stream_on_view,
            }),
        );
    }
    if old.views.max_dependency_depth != new.views.max_dependency_depth {
        out.insert(
            "sync.views.max_dependency_depth".to_string(),
            json!({
                "from": old.views.max_dependency_depth,
                "to":   new.views.max_dependency_depth,
            }),
        );
    }
    serde_json::Value::Object(out)
}
