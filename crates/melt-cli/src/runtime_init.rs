//! Pre-runtime helpers — anything that has to settle before
//! `tokio::runtime::Builder::build()` lives here.
//!
//! Today this is just the blocking-thread cap (KI-001 #3 in
//! `docs/internal/KNOWN_ISSUES.md`). The resolver is split out from
//! `main.rs` so it's testable without `Cli::parse` or a tokio runtime.

use std::path::PathBuf;

use crate::config::MeltConfig;

/// Default cap on Tokio's blocking thread pool. Tokio's own default
/// is `512`, which is plenty under healthy traffic but lets a
/// thundering herd of timed-out-but-still-running DuckDB queries
/// pin the entire pool. `64` keeps the worst-case pin count well
/// inside what a single host can absorb without starving its
/// proxy/sync foreground tasks.
pub const DEFAULT_MAX_BLOCKING_THREADS: usize = 64;

/// Env var name. Operators reach for this when they need to dial
/// the cap up for a specific deployment without editing config.
pub const MAX_BLOCKING_THREADS_ENV: &str = "MELT_MAX_BLOCKING_THREADS";

/// Source of environment lookups. The `Process` variant reads the
/// real process env; tests substitute `Map` so they don't race each
/// other through `std::env::set_var` on a shared environment.
pub enum EnvSource<'a> {
    Process,
    #[cfg_attr(not(test), allow(dead_code))]
    Map(&'a dyn Fn(&str) -> Option<String>),
}

impl<'a> EnvSource<'a> {
    fn get(&self, key: &str) -> Option<String> {
        match self {
            EnvSource::Process => std::env::var(key).ok(),
            EnvSource::Map(f) => f(key),
        }
    }
}

/// Resolve the blocking-thread cap with this precedence:
///
/// 1. `MELT_MAX_BLOCKING_THREADS` env var (any non-zero, parseable usize).
/// 2. `[runtime].max_blocking_threads` in the resolved `melt.toml`.
/// 3. [`DEFAULT_MAX_BLOCKING_THREADS`].
///
/// The config peek is best-effort. Bootstrap subcommands run before
/// any config exists, and we still need a working runtime to print
/// errors and certs. Invalid env values fall through to (2)/(3) with
/// a `WARN` so misconfiguration isn't silently masked.
pub fn resolve_max_blocking_threads(cli_config: Option<&str>, env: EnvSource<'_>) -> usize {
    if let Some(raw) = env.get(MAX_BLOCKING_THREADS_ENV) {
        match raw.parse::<usize>() {
            Ok(n) if n > 0 => return n,
            Ok(_) => tracing::warn!(
                "{MAX_BLOCKING_THREADS_ENV}={raw} is zero; ignoring and falling back to config/default"
            ),
            Err(e) => tracing::warn!(
                "{MAX_BLOCKING_THREADS_ENV}={raw} is not a usize ({e}); ignoring"
            ),
        }
    }

    if let Some(path) = resolve_config_path_silently(cli_config) {
        if let Some(n) = MeltConfig::peek_max_blocking_threads(&path) {
            if n > 0 {
                return n;
            }
            tracing::warn!(
                config = %path.display(),
                "[runtime].max_blocking_threads = 0 is invalid; using default"
            );
        }
    }

    DEFAULT_MAX_BLOCKING_THREADS
}

/// Wrap `MeltConfig::resolve_path` and discard errors. Bootstrap
/// flows have no config; we don't want to fail the runtime build
/// just because nothing's on disk yet.
fn resolve_config_path_silently(cli_config: Option<&str>) -> Option<PathBuf> {
    MeltConfig::resolve_path(cli_config).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::Write;

    fn env_from(map: HashMap<&'static str, &'static str>) -> impl Fn(&str) -> Option<String> {
        move |k| map.get(k).map(|v| v.to_string())
    }

    #[test]
    fn defaults_to_64_with_no_env_no_config() {
        // CWD-independent: pass an explicit nonsense path so
        // resolve_path errors out fast, and use an empty env map.
        let env: HashMap<&str, &str> = HashMap::new();
        // None as cli_config falls back to implicit-path search; the
        // result depends on cwd. We can't fully eliminate that here
        // without changing dirs, so pin it instead by asserting that
        // *some* explicit-path-not-found case yields the default.
        let got = resolve_max_blocking_threads(
            Some("/tmp/melt-config-that-does-not-exist-xyz.toml"),
            EnvSource::Map(&env_from(env)),
        );
        assert_eq!(got, DEFAULT_MAX_BLOCKING_THREADS);
        assert_eq!(DEFAULT_MAX_BLOCKING_THREADS, 64);
    }

    #[test]
    fn env_override_wins() {
        let mut env = HashMap::new();
        env.insert(MAX_BLOCKING_THREADS_ENV, "128");
        let got = resolve_max_blocking_threads(None, EnvSource::Map(&env_from(env)));
        assert_eq!(got, 128);
    }

    #[test]
    fn env_zero_is_ignored() {
        let mut env = HashMap::new();
        env.insert(MAX_BLOCKING_THREADS_ENV, "0");
        // Falls through to the default since cli_config is bogus.
        let got = resolve_max_blocking_threads(
            Some("/tmp/melt-config-that-does-not-exist-xyz.toml"),
            EnvSource::Map(&env_from(env)),
        );
        assert_eq!(got, DEFAULT_MAX_BLOCKING_THREADS);
    }

    #[test]
    fn env_garbage_is_ignored() {
        let mut env = HashMap::new();
        env.insert(MAX_BLOCKING_THREADS_ENV, "not-a-number");
        let got = resolve_max_blocking_threads(
            Some("/tmp/melt-config-that-does-not-exist-xyz.toml"),
            EnvSource::Map(&env_from(env)),
        );
        assert_eq!(got, DEFAULT_MAX_BLOCKING_THREADS);
    }

    #[test]
    fn config_value_used_when_env_unset() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "[runtime]\nmax_blocking_threads = 32\n").unwrap();
        let env: HashMap<&str, &str> = HashMap::new();
        let got = resolve_max_blocking_threads(
            Some(tmp.path().to_str().unwrap()),
            EnvSource::Map(&env_from(env)),
        );
        assert_eq!(got, 32);
    }

    #[test]
    fn env_overrides_config() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "[runtime]\nmax_blocking_threads = 32\n").unwrap();
        let mut env = HashMap::new();
        env.insert(MAX_BLOCKING_THREADS_ENV, "200");
        let got = resolve_max_blocking_threads(
            Some(tmp.path().to_str().unwrap()),
            EnvSource::Map(&env_from(env)),
        );
        assert_eq!(got, 200);
    }

    /// Smoke test: a runtime built with a small cap can still drain
    /// many concurrent `spawn_blocking` tasks (they queue past the
    /// cap rather than deadlock). Stand-in for the "parallel-query
    /// benchmark still completes" acceptance criterion — DuckDB
    /// queries land on this same blocking pool.
    #[test]
    fn capped_runtime_drains_many_blocking_tasks() {
        let cap = 4usize;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .max_blocking_threads(cap)
            .build()
            .unwrap();
        runtime.block_on(async {
            let total = 32usize;
            let handles: Vec<_> = (0..total)
                .map(|i| {
                    tokio::task::spawn_blocking(move || {
                        std::thread::sleep(std::time::Duration::from_millis(20));
                        i
                    })
                })
                .collect();
            let mut sum = 0usize;
            for h in handles {
                sum += h.await.unwrap();
            }
            assert_eq!(sum, (0..total).sum::<usize>());
        });
    }
}
