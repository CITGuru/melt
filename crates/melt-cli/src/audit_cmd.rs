//! `melt audit` — thin wrapper that re-uses the
//! [`melt_audit::AuditArgs`] surface so the binary and the
//! subcommand can never drift.

use std::process::ExitCode;

pub use melt_audit::AuditArgs;

/// Run the `melt audit` subcommand, delegating to the same code path
/// as the standalone `melt-audit` binary.
#[allow(dead_code)]
pub fn run(args: AuditArgs) -> ExitCode {
    melt_audit::run_cli(args)
}

/// Same as [`run`] but returns the raw exit-code byte. The CLI entry
/// point uses this to call `std::process::exit` directly so usage
/// errors (2) and runtime failures (1) survive the trip back to the
/// shell — `tokio::main` swallows `ExitCode` returns from `main`.
pub fn run_status(args: AuditArgs) -> u8 {
    melt_audit::cli::run_status(args)
}
