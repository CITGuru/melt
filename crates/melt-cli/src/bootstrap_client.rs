//! `melt bootstrap client` — fetch the CA from a running Melt instance
//! and emit OS-specific commands for trusting it + routing the Snowflake
//! hostname. The command never executes anything itself; the operator
//! can pipe the output into `bash` after reviewing.

use anyhow::{Context, Result};
use clap::ValueEnum;

/// Auto-detect or operator-forced target platform.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum TargetOs {
    Macos,
    Linux,
    Windows,
    /// Print all platforms in one dump. Useful for shared runbooks.
    All,
}

pub struct SetupArgs {
    pub server: String,
    pub snowflake_account: String,
    pub os: Option<TargetOs>,
}

pub async fn run(args: SetupArgs) -> Result<()> {
    let ca_url = format!("{}/melt/ca.pem", args.server.trim_end_matches('/'));
    // Fetch over the network with cert verification DISABLED for this
    // one call — we're bootstrapping trust; the whole point is that
    // the client does not yet trust Melt's CA. Operators MUST verify
    // the fingerprint the tool prints before installing.
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .context("building http client")?;
    let ca_pem = client
        .get(&ca_url)
        .send()
        .await
        .with_context(|| format!("fetching CA from {ca_url}"))?
        .error_for_status()
        .with_context(|| format!("CA fetch from {ca_url}"))?
        .text()
        .await
        .context("reading CA body")?;

    let fingerprint = fingerprint_pem(&ca_pem);

    let os = args.os.unwrap_or_else(detect_os);
    let account = &args.snowflake_account;
    let host = format!("{account}.snowflakecomputing.com");

    println!("# Fetched {} bytes of CA PEM from {ca_url}", ca_pem.len());
    println!("# SHA-256 fingerprint: {fingerprint}");
    println!("# Reproduce locally:   sha256sum ca.pem");
    println!("# VERIFY this fingerprint matches what `melt bootstrap server`");
    println!("# printed BEFORE running any of the commands below.");
    println!();
    println!("# Save the CA locally:");
    println!("cat > ~/melt-ca.pem <<'PEM'");
    print!("{ca_pem}");
    if !ca_pem.ends_with('\n') {
        println!();
    }
    println!("PEM");
    println!();

    match os {
        TargetOs::Macos => emit_macos(&host),
        TargetOs::Linux => emit_linux(&host),
        TargetOs::Windows => emit_windows(&host),
        TargetOs::All => {
            emit_macos(&host);
            println!();
            emit_linux(&host);
            println!();
            emit_windows(&host);
        }
    }

    println!();
    emit_python_snippet(account, &host);

    Ok(())
}

fn detect_os() -> TargetOs {
    match std::env::consts::OS {
        "macos" => TargetOs::Macos,
        "linux" => TargetOs::Linux,
        "windows" => TargetOs::Windows,
        _ => TargetOs::All,
    }
}

fn emit_macos(host: &str) {
    println!("# ─── macOS ─────────────────────────────────────────────");
    println!("# 1. Trust the CA (system keychain):");
    println!(
        "sudo security add-trusted-cert -d -r trustRoot \\\n    \
         -k /Library/Keychains/System.keychain ~/melt-ca.pem"
    );
    println!();
    println!("# 2. Route the Snowflake hostname to Melt:");
    println!("echo \"127.0.0.1 {host}\" | sudo tee -a /etc/hosts");
}

fn emit_linux(host: &str) {
    println!("# ─── Linux (Debian/Ubuntu) ─────────────────────────────");
    println!("# 1. Trust the CA:");
    println!(
        "sudo cp ~/melt-ca.pem /usr/local/share/ca-certificates/melt.crt && \\\n\
         sudo update-ca-certificates"
    );
    println!();
    println!("# Python requests / urllib3 uses its own CA bundle; either:");
    println!("export REQUESTS_CA_BUNDLE=~/melt-ca.pem");
    println!("# or concatenate into the system bundle (see `certifi`).");
    println!();
    println!("# 2. Route the Snowflake hostname to Melt:");
    println!("echo \"127.0.0.1 {host}\" | sudo tee -a /etc/hosts");
}

fn emit_windows(host: &str) {
    println!("# ─── Windows (PowerShell, admin) ───────────────────────");
    println!("# 1. Trust the CA:");
    println!(
        "Import-Certificate -FilePath $env:USERPROFILE\\melt-ca.pem `\n    \
         -CertStoreLocation Cert:\\LocalMachine\\Root"
    );
    println!();
    println!("# 2. Route the Snowflake hostname to Melt:");
    println!(
        "Add-Content -Path C:\\Windows\\System32\\drivers\\etc\\hosts `\n    \
         -Value \"127.0.0.1 {host}\""
    );
}

fn emit_python_snippet(account: &str, host: &str) {
    println!("# ─── Verify from Python ────────────────────────────────");
    println!("python - <<'PY'");
    println!("import snowflake.connector as sf");
    println!("c = sf.connect(");
    println!("    account=\"{account}\",");
    println!("    host=\"{host}\",");
    println!("    user=\"YOUR_USER\",");
    println!("    password=\"YOUR_PASSWORD\",");
    println!(")");
    println!("print(c.cursor().execute(\"SELECT 1\").fetchone())");
    println!("PY");
}

/// SHA-256 fingerprint of the CA PEM, formatted as colon-separated
/// uppercase hex. SHA-256 (not a non-crypto hash) so a MITM during
/// bootstrap can't forge a colliding CA — the operator's manual
/// fingerprint compare is the only trust anchor here.
fn fingerprint_pem(pem: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(pem.as_bytes());
    let mut out = String::with_capacity(digest.len() * 3);
    for (i, byte) in digest.iter().enumerate() {
        if i > 0 {
            out.push(':');
        }
        out.push_str(&format!("{byte:02X}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the output format against an independently-verifiable
    /// hash. `echo -n '' | sha256sum` returns
    /// `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`,
    /// which colon-separated and uppercased is exactly the expected
    /// string below. This test catches accidental switches back to a
    /// non-cryptographic hasher (the whole point of the fingerprint).
    #[test]
    fn fingerprint_of_empty_matches_sha256sum() {
        assert_eq!(
            fingerprint_pem(""),
            "E3:B0:C4:42:98:FC:1C:14:9A:FB:F4:C8:99:6F:B9:24:\
             27:AE:41:E4:64:9B:93:4C:A4:95:99:1B:78:52:B8:55"
                .replace(' ', "")
        );
    }

    #[test]
    fn fingerprint_is_32_pairs() {
        let fp = fingerprint_pem("any content");
        // 32 bytes × 2 hex chars + 31 colons = 95 characters.
        assert_eq!(fp.len(), 95);
        assert_eq!(fp.matches(':').count(), 31);
    }
}
