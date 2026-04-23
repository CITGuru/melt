//! `GET /melt/ca.pem` — public endpoint that serves the CA used to
//! sign the proxy's server certificate. Consumed by `melt bootstrap client`
//! so drivers on remote machines can fetch and trust the CA without
//! the operator shipping it out of band.
//!
//! Derivation rule: the CA lives as `ca.pem` in the same directory as
//! `proxy.tls_cert`. This matches what `melt bootstrap server` writes,
//! so the common case is zero-config. If the file is missing — e.g.
//! the operator brought their own cert from a public CA — the endpoint
//! returns 404 with a clear explanation.

use std::fs;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::server::ProxyState;

pub async fn serve(State(state): State<ProxyState>) -> Response {
    let Some(tls_cert) = state.tls_cert.as_ref() else {
        return not_found(
            "Melt is running without TLS. There is no CA to distribute — \
             configure `[proxy].tls_cert` to enable this endpoint.",
        );
    };
    let Some(parent) = tls_cert.parent() else {
        return not_found("tls_cert has no parent directory");
    };
    let ca_path = parent.join("ca.pem");
    let bytes = match fs::read(&ca_path) {
        Ok(b) => b,
        Err(e) => {
            return not_found(&format!(
                "{} not readable ({e}). Only certs minted by `melt bootstrap server` \
                 expose a CA — operators using a public or shared private CA \
                 should distribute that trust bundle out of band.",
                ca_path.display()
            ))
        }
    };
    (
        [
            (header::CONTENT_TYPE, "application/x-pem-file"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"ca.pem\"",
            ),
        ],
        bytes,
    )
        .into_response()
}

fn not_found(msg: &str) -> Response {
    (StatusCode::NOT_FOUND, msg.to_string()).into_response()
}
