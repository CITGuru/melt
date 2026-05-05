//! `melt-audit` binary entrypoint. The same logic runs under
//! `melt audit` via `melt-cli`'s subcommand wrapper; both routes
//! share [`melt_audit::AuditArgs`] / [`melt_audit::run_cli`].

use std::process::ExitCode;

use clap::Parser;

fn main() -> ExitCode {
    melt_audit::run_cli(melt_audit::AuditArgs::parse())
}
