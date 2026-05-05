//! End-to-end tests for `melt audit share`. The endpoint itself
//! lives in the website repo (filed under PM-Melt as a separate
//! ticket); these tests pin the *client* contract:
//!
//! 1. The CLI parser accepts `share <json-path>` with `--yes`,
//!    `--anonymize`, and `--endpoint`.
//! 2. The share path never reads the sibling `*.talkingpoints.md`
//!    file — operators sometimes paste real numbers into it.
//! 3. The share-time redaction strips table-level identifiers when
//!    `--anonymize` is set, on top of the literal redaction the
//!    audit pipeline already applied.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use clap::Parser;
use melt_audit::cli::AuditCommand;
use melt_audit::share::{redact_for_share, resolve_json_path};
use melt_audit::AuditArgs;

const TALKING_POINTS_SENTINEL: &str = "DO_NOT_UPLOAD_SENTINEL_44_2026";

fn fixture_payload() -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "account": "ACME-DEMO",
        "window": {
            "start": "2026-04-04T00:00:00Z",
            "end": "2026-05-04T00:00:00Z",
            "days": 30
        },
        "total_queries": 1234,
        "total_spend_usd": 9876.54,
        "dollar_per_query_baseline": 8.0,
        "routable_static": {
            "count": 400, "pct": 32.4,
            "dollar_per_query_post": 5.0, "dollars_saved": 3000.0,
            "annualized": 36500.0
        },
        "routable_conservative": {
            "count": 200, "pct": 16.2,
            "dollar_per_query_post": 6.0, "dollars_saved": 2000.0,
            "annualized": 24000.0
        },
        "top_patterns": [
            {
                "rank": 1, "freq": 500, "avg_ms": 1200,
                "table_fqn": "ANALYTICS.PUBLIC.EVENTS",
                "pattern_redacted": "SELECT user_id FROM ANALYTICS.PUBLIC.EVENTS WHERE created_at > ?",
                "est_dollars_in_window": 1500.0
            }
        ],
        "passthrough_reasons_breakdown": {
            "writes": 0, "snowflake_features": 0,
            "parse_failed": 0, "no_tables": 0
        },
        "confidence_band_pct": 20,
        "disclaimers": ["confidence band ±20%"]
    })
}

#[test]
fn cli_parses_share_subcommand_with_path_and_yes() {
    let args = AuditArgs::try_parse_from([
        "melt-audit",
        "share",
        "/tmp/melt-audit-ACME-DEMO-2026-05-04.json",
        "--yes",
    ])
    .expect("share subcommand parses");
    let Some(AuditCommand::Share(share)) = args.command else {
        panic!("expected Share subcommand, got {:?}", args.command);
    };
    assert!(share.yes);
    assert!(share.anonymize, "share defaults --anonymize=true");
    assert_eq!(share.endpoint, melt_audit::DEFAULT_SHARE_ENDPOINT);
    assert_eq!(
        share.json_path.as_deref(),
        Some(std::path::Path::new(
            "/tmp/melt-audit-ACME-DEMO-2026-05-04.json"
        ))
    );
}

#[test]
fn cli_parses_share_with_endpoint_and_no_anonymize() {
    let args = AuditArgs::try_parse_from([
        "melt-audit",
        "share",
        "audit.json",
        "--yes",
        "--anonymize",
        "false",
        "--endpoint",
        "http://127.0.0.1:9999/audit/share",
    ])
    .expect("share subcommand parses");
    let Some(AuditCommand::Share(share)) = args.command else {
        panic!("expected Share subcommand");
    };
    assert!(!share.anonymize);
    assert_eq!(share.endpoint, "http://127.0.0.1:9999/audit/share");
}

#[test]
fn cli_default_no_subcommand_runs_audit_pipeline() {
    let args = AuditArgs::try_parse_from([
        "melt-audit",
        "--account",
        "ACME-DEMO",
        "--fixture",
        "/tmp/x.csv",
    ])
    .expect("flat audit invocation parses");
    assert!(args.command.is_none(), "no subcommand selected");
    assert_eq!(args.account.as_deref(), Some("ACME-DEMO"));
}

/// The share path opens exactly the JSON it was pointed at — never a
/// sibling `*.talkingpoints.md`. We enforce that by setting the
/// talking-points file unreadable for the current user before
/// resolving the share input. If `resolve_json_path` (or the
/// downstream loader) ever attempted to read the talking-points file,
/// the test would fail because the unreadable file would also be
/// pickable. Belt-and-suspenders: we also pin that
/// `is_share_candidate` filtering excludes the markdown file.
#[test]
fn share_path_never_reads_talking_points_file() {
    let tmp = tempdir();
    let json_path = tmp.join("melt-audit-ACME-DEMO-2026-05-04.json");
    let tp_path = tmp.join("melt-audit-ACME-DEMO-2026-05-04.talkingpoints.md");

    fs::write(&json_path, fixture_payload().to_string()).unwrap();
    fs::write(
        &tp_path,
        format!(
            "# Talking points\n\nReal account spend: $4,200,000\n\
             {TALKING_POINTS_SENTINEL}\n"
        ),
    )
    .unwrap();
    // Make the talking-points file unreadable. If the share path ever
    // tries to open it, the OS will deny the read and either error
    // out or surface the `chmod 0` permission to a caller.
    let mut perms = fs::metadata(&tp_path).unwrap().permissions();
    perms.set_mode(0o000);
    fs::set_permissions(&tp_path, perms).unwrap();

    // Auto-resolve from the directory: must still pick the JSON, not
    // the talking-points file (which would also be a `melt-audit-*`
    // sibling but is filtered out by extension).
    let resolved = resolve_json_path(None, &tmp).expect("resolves the JSON");
    assert_eq!(resolved, json_path);

    // Read + redact the JSON we resolved. The redacted payload must
    // not contain the talking-points sentinel — proving the share
    // path didn't accidentally splice the sibling file in.
    let raw = fs::read_to_string(&resolved).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let redacted = redact_for_share(parsed, true);
    let payload = serde_json::to_string(&redacted).unwrap();
    assert!(
        !payload.contains(TALKING_POINTS_SENTINEL),
        "talking-points contents leaked into share payload: {payload}",
    );

    // Restore perms so the tempdir cleanup can remove the file.
    let mut perms = fs::metadata(&tp_path).unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&tp_path, perms).unwrap();
    fs::remove_dir_all(&tmp).ok();
}

/// End-to-end against a localhost TCP server that echoes a static
/// `{"url":"..."}` reply. Pins (a) the wire format the CLI sends
/// (Content-Type: application/json, the redacted body) and (b) the
/// response shape the CLI parses. Skipped by default in CI without
/// `MELT_AUDIT_SHARE_E2E=1` so a missing localhost listener never
/// fails an unrelated branch.
#[test]
fn share_uploads_against_local_mock_endpoint() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    if std::env::var("MELT_AUDIT_SHARE_E2E").ok().as_deref() != Some("1") {
        eprintln!("skipping (MELT_AUDIT_SHARE_E2E != 1)");
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().unwrap().port();
    let endpoint = format!("http://127.0.0.1:{port}/audit/share");

    let (tx, rx) = mpsc::channel::<String>();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut buf = vec![0u8; 65536];
        let n = stream.read(&mut buf).expect("read");
        let request = String::from_utf8_lossy(&buf[..n]).to_string();
        let body =
            r#"{"url":"https://getmelt.com/audit/abc123","id":"abc123"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\n\
             content-type: application/json\r\n\
             content-length: {}\r\n\
             \r\n{}",
            body.len(),
            body,
        );
        stream.write_all(response.as_bytes()).expect("write");
        tx.send(request).ok();
    });

    let tmp = tempdir();
    let json_path = tmp.join("melt-audit-ACME-DEMO-2026-05-04.json");
    fs::write(&json_path, fixture_payload().to_string()).unwrap();

    let status = std::process::Command::new(env!("CARGO_BIN_EXE_melt-audit"))
        .args([
            "share",
            json_path.to_str().unwrap(),
            "--yes",
            "--endpoint",
            &endpoint,
            "--out-dir",
            tmp.to_str().unwrap(),
        ])
        .status()
        .expect("spawn");
    assert!(status.success(), "share subcommand exit: {status}");

    let request = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    assert!(
        request.contains("content-type: application/json"),
        "request must declare JSON content-type: {request}",
    );
    assert!(
        !request.contains("EVENTS"),
        "uploaded body must not contain table-level identifier: {request}",
    );
    assert!(
        request.contains("ANALYTICS.PUBLIC.<redacted>"),
        "uploaded body must contain anonymized table FQN: {request}",
    );

    let receipt = tmp.join("melt-audit-ACME-DEMO-2026-05-04-shared.json");
    assert!(
        receipt.exists(),
        "share receipt must be written on success: {}",
        receipt.display(),
    );
    fs::remove_dir_all(&tmp).ok();
}

#[test]
fn explicit_directory_picks_newest_audit_json() {
    let tmp = tempdir();
    let older = tmp.join("melt-audit-ACME-DEMO-2026-04-01.json");
    let newer = tmp.join("melt-audit-ACME-DEMO-2026-05-04.json");
    fs::write(&older, "{}").unwrap();
    // Make sure the mtimes order even on fast filesystems.
    std::thread::sleep(std::time::Duration::from_millis(10));
    fs::write(&newer, fixture_payload().to_string()).unwrap();

    let resolved = resolve_json_path(Some(&tmp), &PathBuf::from(".")).unwrap();
    assert_eq!(resolved, newer);

    fs::remove_dir_all(&tmp).ok();
}

fn tempdir() -> PathBuf {
    let base = std::env::temp_dir().join(format!(
        "melt-audit-share-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    fs::create_dir_all(&base).unwrap();
    base
}
