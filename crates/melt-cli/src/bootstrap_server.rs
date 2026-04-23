//! `melt bootstrap server` — mint a private CA + server cert the
//! Snowflake drivers will accept, write a ready-to-go `melt.toml`
//! skeleton, and print the client-side setup commands.
//!
//! Intent: collapse the openssl/step-ca recipe from the TLS section
//! of the README into one command. The operator provides the Snowflake
//! account identifier; the tool does the rest. The companion
//! `melt bootstrap client` subcommand handles the client-host half.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, KeyUsagePurpose,
};
use time::{Duration, OffsetDateTime};

pub struct BootstrapArgs {
    pub snowflake_account: String,
    pub output: PathBuf,
    pub force: bool,
}

pub fn run(args: BootstrapArgs) -> Result<()> {
    if args.snowflake_account.is_empty() {
        return Err(anyhow!("--snowflake-account is required"));
    }

    let outdir = &args.output;
    fs::create_dir_all(outdir)
        .with_context(|| format!("creating output dir {}", outdir.display()))?;

    let ca_pem = outdir.join("ca.pem");
    let ca_key = outdir.join("ca.key");
    let server_pem = outdir.join("server.pem");
    let server_key = outdir.join("server.key");
    let melt_toml = outdir.join("melt.toml");

    // Refuse to clobber existing material unless the operator said so.
    // Rotating a server cert should be an explicit choice.
    for p in [&ca_pem, &ca_key, &server_pem, &server_key] {
        if p.exists() && !args.force {
            return Err(anyhow!(
                "{} already exists — pass `--force` to overwrite",
                p.display()
            ));
        }
    }

    // SANs: Snowflake account hostname (driver SNI) + localhost dev SANs.
    let sans: Vec<String> = vec![
        format!("{}.snowflakecomputing.com", args.snowflake_account),
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ];

    let MintedChain {
        ca_cert_pem,
        ca_key_pem,
        server_cert_pem,
        server_key_pem,
    } = mint_chain(&sans)?;

    write_secret(&ca_pem, &ca_cert_pem)?;
    write_secret(&ca_key, &ca_key_pem)?;
    write_secret(&server_pem, &server_cert_pem)?;
    write_secret(&server_key, &server_key_pem)?;

    write_melt_toml(&melt_toml, &args, &server_pem, &server_key)?;

    print_next_steps(&args, outdir);
    Ok(())
}

struct MintedChain {
    ca_cert_pem: String,
    ca_key_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
}

/// Mint the CA + server cert as one unit so we never have to round-trip
/// the CA through PEM to sign the server cert. rcgen's `Certificate`
/// holds internal state that can't be reconstructed from PEM alone.
fn mint_chain(sans: &[String]) -> Result<MintedChain> {
    // CA: self-signed, 10-year validity.
    let mut ca_params = CertificateParams::default();
    ca_params.distinguished_name = {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "Melt Local CA");
        dn.push(DnType::OrganizationName, "Melt");
        dn
    };
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    ca_params.not_before = OffsetDateTime::now_utc();
    ca_params.not_after = ca_params.not_before + Duration::days(365 * 10);

    let ca_key = KeyPair::generate().context("CA keypair")?;
    let ca_cert = ca_params.self_signed(&ca_key).context("CA self-sign")?;

    // Server cert: signed by the CA above, 1-year validity.
    let mut server_params = CertificateParams::new(sans.to_vec()).context("server SAN params")?;
    server_params.distinguished_name = {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, &sans[0]);
        dn
    };
    server_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    server_params.not_before = OffsetDateTime::now_utc();
    server_params.not_after = server_params.not_before + Duration::days(365);

    let server_key = KeyPair::generate().context("server keypair")?;
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .context("server sign")?;

    Ok(MintedChain {
        ca_cert_pem: ca_cert.pem(),
        ca_key_pem: ca_key.serialize_pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
    })
}

/// Writes a file and sets 0600 permissions on Unix so private keys
/// don't leak via inherited ACLs.
fn write_secret(path: &Path, contents: &str) -> Result<()> {
    fs::write(path, contents).with_context(|| format!("writing {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perm = fs::metadata(path)?.permissions();
        perm.set_mode(0o600);
        fs::set_permissions(path, perm)?;
    }
    Ok(())
}

fn write_melt_toml(
    path: &Path,
    args: &BootstrapArgs,
    server_pem: &Path,
    server_key: &Path,
) -> Result<()> {
    if path.exists() && !args.force {
        tracing::info!(
            "{} exists — leaving it alone (pass --force to rewrite)",
            path.display()
        );
        return Ok(());
    }
    let sample = sample_melt_toml(args, server_pem, server_key);
    fs::write(path, sample).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

fn sample_melt_toml(args: &BootstrapArgs, server_pem: &Path, server_key: &Path) -> String {
    format!(
        r#"# Generated by `melt bootstrap` — review and commit (minus keys!).
# See docs/ARCHITECTURE.md for the full reference.

[proxy]
listen   = "0.0.0.0:8443"
tls_cert = "{cert}"
tls_key  = "{key}"

[proxy.limits]
request_timeout            = "30s"
max_concurrent_per_session = 16
max_concurrent_global      = 256
result_store_max_bytes     = "1GB"
result_store_max_entries   = 1000
result_store_idle_ttl      = "5m"
shutdown_drain_timeout     = "10s"

[snowflake]
account         = "{account}"
host            = ""
request_timeout = "60s"
max_retries     = 3

[snowflake.policy]
mode             = "passthrough"
refresh_interval = "60s"

[router]
lake_max_scan_bytes      = "100GB"
table_exists_cache_ttl   = "5m"
estimate_bytes_cache_ttl = "1m"

[metrics]
listen     = "0.0.0.0:9090"
log_format = "pretty"
log_level  = "info"

# Pick ONE backend block. DuckLake is the simpler default.

[backend.ducklake]
catalog_url      = "postgres://melt:melt@localhost:5432/melt_catalog"
data_path        = "s3://melt/ducklake/"
reader_pool_size = 8
writer_pool_size = 1

[backend.ducklake.s3]
region            = "us-east-1"
endpoint          = "localhost:9000"      # MinIO in docker-compose
url_style         = "path"
use_ssl           = false
access_key_id     = "melt"
secret_access_key = "meltmelt"

# [backend.iceberg]
# catalog   = "rest"
# warehouse = "s3://melt/iceberg/"
# rest_uri  = "https://polaris.internal/api/catalog"
# region    = "us-east-1"
"#,
        cert = server_pem.display(),
        key = server_key.display(),
        account = args.snowflake_account,
    )
}

fn print_next_steps(args: &BootstrapArgs, outdir: &Path) {
    let account = &args.snowflake_account;
    let host = format!("{account}.snowflakecomputing.com");
    let ca_path = outdir.join("ca.pem");

    println!();
    println!("✔ Bootstrap complete.");
    println!();
    println!("  Output:          {}", outdir.display());
    println!("  CA:              {}", ca_path.display());
    println!("  Server cert:     {}", outdir.join("server.pem").display());
    println!(
        "  Server key:      {} (0600)",
        outdir.join("server.key").display()
    );
    println!("  Config:          {}", outdir.join("melt.toml").display());
    println!();
    println!("Next steps:");
    println!();
    println!("  1. Start Melt pointing at the generated config:");
    println!(
        "       melt --config {cfg} start",
        cfg = outdir.join("melt.toml").display()
    );
    println!();
    println!("  2. On every CLIENT host, trust the CA. Run `melt bootstrap client`");
    println!("     from the client box, or do it manually:");
    println!();
    println!("     macOS:");
    println!(
        "       sudo security add-trusted-cert -d -r trustRoot \\\n         \
           -k /Library/Keychains/System.keychain {}",
        ca_path.display()
    );
    println!();
    println!("     Linux (Debian/Ubuntu):");
    println!(
        "       sudo cp {ca} /usr/local/share/ca-certificates/melt.crt && \\\n       \
           sudo update-ca-certificates",
        ca = ca_path.display()
    );
    println!();
    println!("  3. Route the Snowflake hostname to Melt (one of):");
    println!();
    println!("     /etc/hosts:");
    println!("       echo \"127.0.0.1 {host}\" | sudo tee -a /etc/hosts");
    println!();
    println!("     or tell the Snowflake driver directly:");
    println!(
        "       snowflake.connector.connect(account=\"{account}\", host=\"melt.internal\", ...)"
    );
    println!();
    println!("  4. Verify from Python:");
    println!("       python -c 'import snowflake.connector as sf; \\");
    println!(
        "                  c = sf.connect(account=\"{account}\", host=\"{host}\", user=\"...\", password=\"...\"); \\"
    );
    println!("                  print(c.cursor().execute(\"SELECT 1\").fetchone())'");
    println!();
}
