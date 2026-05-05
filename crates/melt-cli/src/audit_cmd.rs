//! `melt audit` — thin wrapper that re-uses the
//! [`melt_audit::AuditArgs`] surface so the binary and the
//! subcommand can never drift.

use std::process::ExitCode;

pub use melt_audit::AuditArgs;

/// Run the `melt audit` subcommand, delegating to the same code path
/// as the standalone `melt-audit` binary.
pub fn run(args: AuditArgs) -> ExitCode {
    melt_audit::run_cli(args)
}
