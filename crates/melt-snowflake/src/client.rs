use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::{HeaderMap, HeaderValue, Method};
use melt_core::{MeltError, ObjectKind, RecordBatchStream, Result, TableRef};
use reqwest::Client as Reqwest;

use crate::auth::{ServiceToken, ServiceTokenCache};
use crate::config::{ServiceAuth, SnowflakeConfig};
use crate::policy::list_policy_protected_tables_query;
use crate::stream::{ChangeStream, SnapshotId};

/// The Snowflake HTTP client used by both the proxy passthrough path
/// and the sync subsystems' CDC reader. Wraps a `reqwest::Client`
/// configured with retries, timeouts, rustls TLS, and a service-token
/// cache.
#[derive(Clone)]
pub struct SnowflakeClient {
    pub(crate) http: Reqwest,
    pub(crate) cfg: Arc<SnowflakeConfig>,
    pub(crate) tokens: Arc<ServiceTokenCache>,
}

impl SnowflakeClient {
    pub fn new(cfg: SnowflakeConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .pool_idle_timeout(Duration::from_secs(30))
            .user_agent(concat!("melt/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("valid reqwest client");
        Self {
            http,
            cfg: Arc::new(cfg),
            tokens: Arc::new(ServiceTokenCache::new()),
        }
    }

    pub fn config(&self) -> &SnowflakeConfig {
        &self.cfg
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.cfg.base_url(), path)
    }

    /// Convenience wrapper when the caller has no query string / no
    /// source headers — still used by tests. Production paths go
    /// through [`Self::forward_login_with_query`].
    pub async fn forward_login(&self, body: Bytes) -> Result<reqwest::Response> {
        self.forward_login_with_query(None, &HeaderMap::new(), body)
            .await
    }

    /// Forward `POST /session/v1/login-request?<query>` — body and
    /// query string verbatim, plus a curated subset of the caller's
    /// request headers.
    ///
    /// Before forwarding, validate the driver's claimed
    /// `data.ACCOUNT_NAME` against `[snowflake].account`:
    ///
    /// - **match (case-insensitive)** → forward unchanged
    /// - **absent** → inject `cfg.account` so the upstream body is
    ///   internally consistent with the URL we're POSTing to
    /// - **mismatch** → return `MeltError::AccountMismatch` so the
    ///   handler can emit a Snowflake-shaped 390201 error
    ///
    /// Header forwarding matters because snowflake-connector-python
    /// (and the JDBC driver in pre-3.17 versions) gzip request bodies
    /// and advertise it via `Content-Encoding: gzip`. Melt can't parse
    /// the gzipped JSON to inject `ACCOUNT_NAME` via `enforce_account`
    /// — that's fine, the path degrades gracefully — but if we then
    /// forward the gzipped bytes to upstream *without* the
    /// `Content-Encoding` header, Snowflake tries to parse binary as
    /// JSON, returns HTTP 400, and drivers bail with 290400. Same
    /// story for `Accept` (drivers sometimes ask for
    /// `application/snowflake`) and `User-Agent` (used by telemetry).
    ///
    /// We allowlist forwarded headers rather than copying everything
    /// so `Host`, `Authorization`, and hop-by-hop (`Connection`,
    /// `Transfer-Encoding`) headers don't leak upstream.
    pub async fn forward_login_with_query(
        &self,
        query: Option<&str>,
        source_headers: &HeaderMap,
        body: Bytes,
    ) -> Result<reqwest::Response> {
        let body = enforce_account(&self.cfg.account, body)?;

        let mut url = self.url("/session/v1/login-request");
        if let Some(q) = query.filter(|s| !s.is_empty()) {
            url.push('?');
            url.push_str(q);
        }

        let mut req = self.http.post(&url);
        req = apply_forwarded_headers(req, source_headers);
        let resp = req
            .body(body)
            .send()
            .await
            .map_err(|e| MeltError::Http(format!("login forward: {e}")))?;
        Ok(resp)
    }

    /// Stream a passthrough request body straight to Snowflake and
    /// stream the response body straight back. No buffering. This is
    /// what keeps the <100ms passthrough overhead claim honest.
    ///
    /// Convenience wrapper — discards the upstream status + headers
    /// and returns just the body stream. Use
    /// [`Self::passthrough_full`] when callers need to replay the
    /// upstream `Content-Encoding` / `content-type` back to the
    /// driver.
    pub async fn passthrough(
        &self,
        method: Method,
        path: &str,
        token: &str,
        body: Bytes,
    ) -> Result<impl Stream<Item = Result<Bytes>> + Send + Unpin + 'static> {
        let resp = self
            .passthrough_full(method, path, None, &HeaderMap::new(), token, body)
            .await?;
        Ok(resp.body)
    }

    /// Backwards-compat alias. Earlier iterations of the v1 handler
    /// called this name — keep it routing to the new full-fidelity
    /// API so no call site has to pick between "just the body" and
    /// "everything". New code should use [`Self::passthrough_full`]
    /// directly.
    pub async fn passthrough_with_headers(
        &self,
        method: Method,
        path: &str,
        query: Option<&str>,
        source_headers: &HeaderMap,
        token: &str,
        body: Bytes,
    ) -> Result<PassthroughResponse> {
        self.passthrough_full(method, path, query, source_headers, token, body)
            .await
    }

    /// Full-fidelity passthrough. Forwards `<path>?<query>` verbatim,
    /// replays a curated subset of the caller's headers (see
    /// [`apply_forwarded_headers`]), attaches the session bearer with
    /// Snowflake's legacy `Authorization: Snowflake Token="..."`
    /// scheme, and returns the upstream status code + response
    /// headers + streaming body so the HTTP handler can replay
    /// `Content-Encoding`, `content-type`, and friends untouched.
    ///
    /// Why every piece matters:
    ///
    /// * Query string — drivers pin session context (`databaseName`,
    ///   `warehouse`, `roleName`) and correlation (`request_id`,
    ///   `request_guid`) on the URL. Strip them and Snowflake returns
    ///   HTTP 400.
    /// * Request `Content-Encoding` — request bodies are gzipped by
    ///   snowflake-connector-python and older JDBC driver versions.
    ///   Forwarded via the allowlisted header set.
    /// * Response `Content-Encoding` — Snowflake compresses responses
    ///   when the driver advertised `Accept-Encoding: gzip`. If Melt
    ///   strips that reply header the driver can't decompress and
    ///   reports `Expecting value: line 1 column 1` on the JSON
    ///   parse.
    pub async fn passthrough_full(
        &self,
        method: Method,
        path: &str,
        query: Option<&str>,
        source_headers: &HeaderMap,
        token: &str,
        body: Bytes,
    ) -> Result<PassthroughResponse> {
        let mut url = self.url(path);
        if let Some(q) = query.filter(|s| !s.is_empty()) {
            // `path` may already carry a `?` (e.g. v2 poll's `?partition=N`).
            url.push(if url.contains('?') { '&' } else { '?' });
            url.push_str(q);
        }

        let mut req = self.http.request(method, &url);
        req = apply_forwarded_headers(req, source_headers);
        let auth = HeaderValue::from_str(&format!("Snowflake Token=\"{token}\""))
            .map_err(|e| MeltError::Http(format!("invalid token header: {e}")))?;
        req = req.header("authorization", auth);
        if !source_headers.contains_key("accept") {
            req = req.header("accept", "application/json");
        }

        let resp = req
            .body(body)
            .send()
            .await
            .map_err(|e| MeltError::Http(format!("passthrough send: {e}")))?;

        let status = resp.status();
        if !status.is_success() && status.as_u16() != 401 {
            tracing::warn!(%status, %url, "snowflake passthrough non-2xx");
        }
        let status_code = status.as_u16();
        let headers = resp.headers().clone();
        let body_stream = resp
            .bytes_stream()
            .map(|r| r.map_err(|e| MeltError::Http(format!("passthrough body: {e}"))));
        Ok(PassthroughResponse {
            status: status_code,
            headers,
            body: Box::pin(body_stream),
        })
    }

    /// Issue a query against Snowflake and stream Arrow batches back.
    ///
    /// Sets `resultFormat=arrow` on `POST /api/v2/statements`. The
    /// REST API returns one base64-encoded Arrow IPC blob in the
    /// initial response under `data.rowsetBase64` (and zero or more
    /// extra partition URLs the sync subsystem could fetch in a
    /// follow-up — for the MVP we drain just the inline partition).
    ///
    /// Snowflake versions vary on whether Arrow is offered for every
    /// result; if the upstream rejects the format we surface a
    /// `BackendUnavailable` error so callers can fall back to the
    /// JSON shape or skip the cycle.
    pub async fn execute_arrow(&self, token: &str, sql: &str) -> Result<RecordBatchStream> {
        use futures::stream;

        let url = format!(
            "{}/api/v2/statements?resultFormat=arrow",
            self.cfg.base_url()
        );
        let body = self.with_session_context(serde_json::json!({
            "statement": sql,
            "timeout":   60,
        }));
        let resp = self
            .http
            .post(&url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|e| MeltError::Http(format!("execute_arrow send: {e}")))?;
        if !resp.status().is_success() {
            return Err(MeltError::BackendUnavailable(format!(
                "execute_arrow upstream {}",
                resp.status()
            )));
        }
        let value: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| MeltError::Http(format!("execute_arrow parse: {e}")))?;

        // Tail partitions fetched via `GET …?partition=k`; without
        // walking them, results truncate silently.
        let statement_handle = value
            .get("statementHandle")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        let row_type: Option<Vec<serde_json::Value>> = value
            .pointer("/resultSetMetaData/rowType")
            .and_then(|v| v.as_array())
            .cloned();

        let partition_info: Vec<serde_json::Value> = value
            .pointer("/resultSetMetaData/partitionInfo")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let total_partitions = partition_info.len().max(1);

        let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
        extract_partition_batches(&value, row_type.as_deref(), &mut batches)?;

        if total_partitions > 1 {
            let Some(handle) = statement_handle else {
                return Err(MeltError::backend(
                    "execute_arrow: multi-partition response without statementHandle",
                ));
            };
            for p in 1..total_partitions {
                let part_url = format!(
                    "{}/api/v2/statements/{handle}?partition={p}",
                    self.cfg.base_url()
                );
                let part_resp = self
                    .http
                    .get(&part_url)
                    .bearer_auth(token)
                    // Force JSON so tail partitions match partition 0.
                    .header("Accept", "application/json")
                    .send()
                    .await
                    .map_err(|e| MeltError::Http(format!("partition {p} send: {e}")))?;
                if !part_resp.status().is_success() {
                    return Err(MeltError::BackendUnavailable(format!(
                        "execute_arrow partition {p} upstream {}",
                        part_resp.status()
                    )));
                }
                // Read bytes so parse failures can include a body preview.
                let body_bytes = part_resp
                    .bytes()
                    .await
                    .map_err(|e| MeltError::Http(format!("partition {p} read: {e}")))?;
                let part_value: serde_json::Value =
                    serde_json::from_slice(&body_bytes).map_err(|e| {
                        let preview =
                            String::from_utf8_lossy(&body_bytes[..body_bytes.len().min(200)]);
                        MeltError::Http(format!(
                            "partition {p} parse: {e}. body preview: {preview}"
                        ))
                    })?;
                extract_partition_batches(&part_value, row_type.as_deref(), &mut batches)?;
            }
            tracing::debug!(
                statement = %handle,
                partitions = total_partitions,
                batches = batches.len(),
                "execute_arrow: walked multi-partition response",
            );
        }

        let stream = stream::iter(
            batches
                .into_iter()
                .map(Ok::<arrow::record_batch::RecordBatch, MeltError>),
        );
        Ok(Box::pin(stream))
    }

    /// CDC stream reader. Drives a Snowflake `STREAM` consumer SQL of
    /// the form `SELECT METADATA$ACTION, * FROM <stream_name>` and
    /// yields one `ChangeBatch` per Arrow batch. Returns the empty
    /// stream when no stream is wired (account credentials missing
    /// or table not yet streamed) so the sync loop can heartbeat
    /// without erroring.
    pub async fn read_stream_since(
        &self,
        token: &str,
        table: &TableRef,
        last_snapshot: Option<SnapshotId>,
    ) -> Result<ChangeStream> {
        use futures::StreamExt;

        let next = last_snapshot
            .map(|s| SnapshotId(s.0 + 1))
            .unwrap_or(SnapshotId(0));
        // Quoted spelling MUST match `create_stream_if_not_exists`
        // (preserves the lowercase `__melt_stream` suffix; otherwise
        // Snowflake responds `002003: does not exist`, which an
        // earlier bug swallowed into silently-empty data).
        let stream_name = format!(
            "\"{}\".\"{}\".\"{}__melt_stream\"",
            table.database, table.schema, table.name
        );

        // Consume table = CDC checkpoint; stale rows mean a prior tick
        // crashed before commit — replay (idempotent on `__row_id`).
        let consume_name = Self::consume_table_name(table);

        let stale_count = self
            .consume_table_count(token, &consume_name)
            .await
            .map_err(|e| {
                tracing::warn!(
                    error = %e,
                    consume = %consume_name,
                    "read_stream_since: pre-CTAS checkpoint probe failed; \
                     refusing to advance stream until probe succeeds (avoids data loss)",
                );
                e
            })?;
        if stale_count > 0 {
            tracing::warn!(
                consume = %consume_name,
                stale_rows = stale_count,
                "read_stream_since: stale consume table from a prior crashed apply — \
                 replaying instead of advancing stream",
            );
        } else {
            // `__row_id = METADATA$ROW_ID` is the merge key.
            let ctas = format!(
                "CREATE OR REPLACE TRANSIENT TABLE {consume_name} AS \
                 SELECT METADATA$ACTION AS __action, \
                        METADATA$ROW_ID AS __row_id, \
                        * EXCLUDE (METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID) \
                 FROM {stream_name} \
                 WHERE METADATA$ROW_ID IS NOT NULL"
            );
            self.execute_ddl(token, &ctas).await.map_err(|e| {
                tracing::warn!(
                    error = %e,
                    stream = %stream_name,
                    consume = %consume_name,
                    "read_stream_since: stream→transient CTAS failed",
                );
                e
            })?;
        }

        let sql = format!("SELECT * FROM {consume_name}");
        let stream = self.execute_arrow(token, &sql).await.map_err(|e| {
            tracing::warn!(
                error = %e,
                table = %consume_name,
                "read_stream_since: read from consume table failed",
            );
            e
        })?;

        let rows = stream.map(|b: Result<arrow::record_batch::RecordBatch>| {
            b.map(|batch| crate::stream::ChangeBatch {
                action: crate::stream::infer_action(&batch),
                batch,
            })
        });
        Ok(ChangeStream {
            table: table.clone(),
            start_snapshot: last_snapshot,
            end_snapshot: next,
            rows: Box::pin(rows),
        })
    }

    /// Discover tables with row access policies, masking policies, or
    /// column-level masking. Powers `PolicyMode::Passthrough`.
    ///
    /// Enumerate every table / view carrying a row-access, masking,
    /// aggregation, or projection policy account-wide. Read from the
    /// `SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES` view — the only
    /// cross-edition, single-round-trip way to get this list. See
    /// [`list_policy_protected_tables_query`] for why we can't use
    /// `INFORMATION_SCHEMA.POLICY_REFERENCES` here.
    ///
    /// Requires `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE` on the
    /// sync role. Without that grant, Snowflake returns
    /// `002003 (42S02): Object 'SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES'
    /// does not exist or not authorized`; we pass that body through so
    /// the operator knows exactly what to grant (or to switch to
    /// `PolicyMode::AllowList` as an escape hatch).
    pub async fn list_policy_protected_tables(
        &self,
        token: &str,
    ) -> Result<Vec<melt_core::ProtectedTable>> {
        let sql = list_policy_protected_tables_query();
        let body = self.with_session_context(serde_json::json!({
            "statement": sql,
            "timeout": 60,
        }));
        let resp = self
            .http
            .post(self.url("/api/v2/statements"))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|e| MeltError::Http(format!("policy refresh send: {e}")))?;

        if !resp.status().is_success() {
            // Forward Snowflake's body verbatim so operators see the code/message.
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(MeltError::BackendUnavailable(format!(
                "policy refresh upstream returned {status}: {}",
                body.trim()
            )));
        }

        let value: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| MeltError::Http(format!("policy refresh parse: {e}")))?;
        Ok(crate::policy::parse_policy_rowset(&value))
    }

    /// Long-lived service token used by sync subsystems for CDC
    /// reads and policy-refresh queries. Cached; auto-refreshed when
    /// <60s remain so callers can call this on every iteration.
    ///
    /// Resolves the credential at every cache miss so operators who
    /// rotate secrets on disk (`pat_file`, `private_key_file`) pick
    /// up the new value on the next refresh without restart.
    pub async fn service_token(&self) -> Result<ServiceToken> {
        if let Some(t) = self.tokens.get() {
            if !t.nearly_expired() {
                return Ok(t);
            }
        }

        let token = match self.cfg.resolve_service_auth()? {
            ServiceAuth::Pat(pat) => {
                // 24h cache; invalidated on upstream 401.
                ServiceToken::new(pat, Duration::from_secs(60 * 60 * 24))
            }
            ServiceAuth::KeyPair { pem_bytes, user } => {
                // 50min cache leaves headroom before Snowflake's 1h cap.
                let jwt = sign_snowflake_jwt(&self.cfg.account, &user, &pem_bytes)?;
                let session_token = self.exchange_jwt(&user, &jwt).await?;
                ServiceToken::new(session_token, Duration::from_secs(50 * 60))
            }
        };

        self.tokens.put(token.clone());
        Ok(token)
    }

    pub fn invalidate_service_token(&self) {
        self.tokens.invalidate();
    }

    /// Run a non-result DDL statement. Used for `CREATE STREAM`,
    /// `DROP STREAM`, `ALTER TABLE ... SET CHANGE_TRACKING`, etc.
    pub async fn execute_ddl(&self, token: &str, sql: &str) -> Result<()> {
        let body = self.with_session_context(serde_json::json!({
            "statement": sql,
            "timeout":   60,
        }));
        let resp = self
            .http
            .post(self.url("/api/v2/statements"))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|e| MeltError::Http(format!("execute_ddl send: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(MeltError::backend(format!("execute_ddl {status}: {body}")));
        }
        Ok(())
    }

    /// `SELECT COUNT(*) FROM <table>` against Snowflake.
    ///
    /// Uses the JSON `/api/v2/statements` path (not `execute_arrow`)
    /// because trivial results come back as inline JSON, not Arrow.
    /// Used by the sync bootstrap to sanity-check the initial drain
    /// — if the source has rows but the stream returns zero,
    /// change-tracking was off when the stream was created and we
    /// quarantine instead of marching the table to `active` empty.
    pub async fn count_table(&self, token: &str, table: &TableRef) -> Result<u64> {
        let qualified = format!(
            "\"{}\".\"{}\".\"{}\"",
            table.database, table.schema, table.name
        );
        let sql = format!("SELECT COUNT(*) AS n FROM {qualified}");
        let body = self.with_session_context(serde_json::json!({
            "statement": sql,
            "timeout": 60,
        }));
        let resp = self
            .http
            .post(self.url("/api/v2/statements"))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|e| MeltError::Http(format!("count_table send: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let fqn = format!("{}.{}.{}", table.database, table.schema, table.name);
            return Err(MeltError::BackendUnavailable(format!(
                "count_table {status} ({fqn}): {}",
                body.trim()
            )));
        }
        let value: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| MeltError::Http(format!("count_table parse: {e}")))?;

        let cell = value
            .pointer("/data/0/0")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                MeltError::backend(format!(
                    "count_table: unexpected response shape: {}",
                    serde_json::to_string(&value).unwrap_or_default()
                ))
            })?;
        cell.parse::<u64>().map_err(|e| {
            MeltError::backend(format!(
                "count_table: COUNT(*) = {cell:?} is not a u64: {e}"
            ))
        })
    }

    /// Canonical fully-qualified consume-table name. Single source of
    /// truth so callers can't drift on the spelling.
    pub fn consume_table_name(table: &TableRef) -> String {
        format!(
            "\"{}\".\"{}\".\"{}__melt_consume\"",
            table.database, table.schema, table.name
        )
    }

    /// `SELECT COUNT(*) FROM <consume_qualified>`, with missing
    /// table mapped to `Ok(0)`.
    ///
    /// Used by `read_stream_since` to detect a stale checkpoint
    /// from a crashed prior tick. CALLER MUST NOT downgrade other
    /// errors to 0 — a transient glitch silently treated as "no
    /// checkpoint" lets the next CTAS overwrite pending events.
    pub async fn consume_table_count(&self, token: &str, consume_qualified: &str) -> Result<u64> {
        let sql = format!("SELECT COUNT(*) AS n FROM {consume_qualified}");
        let body = self.with_session_context(serde_json::json!({
            "statement": sql,
            "timeout": 30,
        }));
        let resp = self
            .http
            .post(self.url("/api/v2/statements"))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|e| MeltError::Http(format!("consume_table_count send: {e}")))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            // Snowflake 002003 / "does not exist" → no checkpoint.
            if text.contains("002003") || text.to_ascii_lowercase().contains("does not exist") {
                return Ok(0);
            }
            return Err(MeltError::BackendUnavailable(format!(
                "consume_table_count {status} ({consume_qualified}): {body}",
                body = text.trim()
            )));
        }
        let value: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| MeltError::Http(format!("consume_table_count parse: {e}")))?;
        let cell = value
            .pointer("/data/0/0")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                MeltError::backend(format!(
                    "consume_table_count: unexpected response shape for {consume_qualified}: {}",
                    serde_json::to_string(&value).unwrap_or_default()
                ))
            })?;
        cell.parse::<u64>().map_err(|e| {
            MeltError::backend(format!(
                "consume_table_count: COUNT(*) = {cell:?} is not a u64: {e}"
            ))
        })
    }

    /// `DROP TABLE IF EXISTS` on a consume table. Called by the sync
    /// loop after a successful apply + `record_sync_progress` —
    /// dropping is the commit signal that the next CTAS may advance
    /// the stream offset.
    pub async fn drop_consume_table(&self, token: &str, consume_qualified: &str) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {consume_qualified}");
        self.execute_ddl(token, &sql).await
    }

    /// `CREATE STREAM IF NOT EXISTS <table>__melt_stream ON TABLE
    /// <table> SHOW_INITIAL_ROWS = TRUE`. Idempotent. With
    /// `SHOW_INITIAL_ROWS` the first consumption returns every existing
    /// row as an `INSERT` event — that's the bootstrap snapshot.
    pub async fn create_stream_if_not_exists(&self, token: &str, table: &TableRef) -> Result<()> {
        let stream = format!(
            "\"{}\".\"{}\".\"{}__melt_stream\"",
            table.database, table.schema, table.name
        );
        let source = format!(
            "\"{}\".\"{}\".\"{}\"",
            table.database, table.schema, table.name
        );
        let sql = format!(
            "CREATE STREAM IF NOT EXISTS {stream} \
             ON TABLE {source} \
             APPEND_ONLY = FALSE \
             SHOW_INITIAL_ROWS = TRUE"
        );
        self.execute_ddl(token, &sql).await
    }

    /// Like [`Self::create_stream_if_not_exists`] but uses `ON VIEW`.
    /// Snowflake requires the DDL keyword to match the object kind —
    /// `ON TABLE` against a view is rejected at parse time, so we
    /// branch explicitly instead of letting sync find out at
    /// bootstrap time with an opaque error.
    pub async fn create_stream_on_view_if_not_exists(
        &self,
        token: &str,
        table: &TableRef,
    ) -> Result<()> {
        let stream = format!(
            "\"{}\".\"{}\".\"{}__melt_stream\"",
            table.database, table.schema, table.name
        );
        let source = format!(
            "\"{}\".\"{}\".\"{}\"",
            table.database, table.schema, table.name
        );
        let sql = format!(
            "CREATE STREAM IF NOT EXISTS {stream} \
             ON VIEW {source} \
             APPEND_ONLY = FALSE \
             SHOW_INITIAL_ROWS = TRUE"
        );
        self.execute_ddl(token, &sql).await
    }

    /// `DROP STREAM IF EXISTS`. Called on demotion + on stream
    /// staleness rebuild.
    pub async fn drop_stream(&self, token: &str, table: &TableRef) -> Result<()> {
        let stream = format!(
            "\"{}\".\"{}\".\"{}__melt_stream\"",
            table.database, table.schema, table.name
        );
        let sql = format!("DROP STREAM IF EXISTS {stream}");
        self.execute_ddl(token, &sql).await
    }

    /// `ALTER TABLE ... SET CHANGE_TRACKING = TRUE`. Only used when
    /// `[sync.lazy].auto_enable_change_tracking = true`. Requires
    /// `APPLY CHANGE TRACKING` privilege on the schema.
    pub async fn enable_change_tracking(&self, token: &str, table: &TableRef) -> Result<()> {
        let source = format!(
            "\"{}\".\"{}\".\"{}\"",
            table.database, table.schema, table.name
        );
        let sql = format!("ALTER TABLE {source} SET CHANGE_TRACKING = TRUE");
        self.execute_ddl(token, &sql).await
    }

    /// `ALTER VIEW ... SET CHANGE_TRACKING = TRUE`. Required on every
    /// view before a stream can be created on top of it, in addition
    /// to each base table the view reads from.
    pub async fn enable_change_tracking_on_view(
        &self,
        token: &str,
        table: &TableRef,
    ) -> Result<()> {
        let source = format!(
            "\"{}\".\"{}\".\"{}\"",
            table.database, table.schema, table.name
        );
        let sql = format!("ALTER VIEW {source} SET CHANGE_TRACKING = TRUE");
        self.execute_ddl(token, &sql).await
    }

    /// Probe `INFORMATION_SCHEMA.TABLES` + `INFORMATION_SCHEMA.VIEWS`
    /// to classify the object at `table`. Returns `ObjectKind::Unknown`
    /// if no row was found (so the caller can mark the row quarantined
    /// with an "object not found" reason rather than re-running the
    /// wrong bootstrap path).
    ///
    /// Uses the per-database `INFORMATION_SCHEMA` (fresh, no
    /// `ACCOUNT_USAGE` latency). `VIEWS.IS_SECURE` is joined so we
    /// can surface `SecureView` without a second round-trip.
    pub async fn describe_object_kind(&self, token: &str, table: &TableRef) -> Result<ObjectKind> {
        let sql = format!(
            "SELECT t.table_type, COALESCE(v.is_secure, 'NO') \
             FROM \"{db}\".INFORMATION_SCHEMA.TABLES t \
             LEFT JOIN \"{db}\".INFORMATION_SCHEMA.VIEWS v \
               ON v.table_catalog = t.table_catalog \
              AND v.table_schema  = t.table_schema \
              AND v.table_name    = t.table_name \
             WHERE t.table_schema = '{schema}' \
               AND t.table_name   = '{name}' \
             LIMIT 1",
            db = sql_ident_escape(&table.database),
            schema = sql_literal_escape(&table.schema),
            name = sql_literal_escape(&table.name),
        );
        let value = self.execute_json_rowset(token, &sql).await?;
        let row = value
            .pointer("/data")
            .and_then(|v| v.as_array())
            .and_then(|rows| rows.first())
            .and_then(|r| r.as_array());
        let Some(row) = row else {
            return Ok(ObjectKind::Unknown);
        };
        if row.len() < 2 {
            return Ok(ObjectKind::Unknown);
        }
        let table_type = row[0].as_str().unwrap_or_default();
        let is_secure = matches!(
            row[1].as_str().unwrap_or(""),
            "YES" | "yes" | "TRUE" | "true"
        );
        Ok(ObjectKind::from_snowflake(table_type, is_secure))
    }

    /// Fetch a view's body + base-table closure. Returns `None` when
    /// the view can't be read (usually because it's secure — Snowflake
    /// masks the body unless the caller owns it).
    ///
    /// Two round-trips today: `GET_DDL('VIEW', ...)` for the body and
    /// a join against `VIEW_TABLE_USAGE` for the immediate base-table
    /// deps. The caller walks view-on-view chains via its own BFS;
    /// we intentionally don't follow them here to keep the query
    /// flat.
    pub async fn fetch_view_definition(
        &self,
        token: &str,
        table: &TableRef,
    ) -> Result<Option<ViewDef>> {
        let fqn_literal = format!(
            "'\"{}\".\"{}\".\"{}\"'",
            sql_literal_escape(&table.database),
            sql_literal_escape(&table.schema),
            sql_literal_escape(&table.name),
        );
        let body_sql = format!("SELECT GET_DDL('VIEW', {fqn_literal})");
        let body_value = self.execute_json_rowset(token, &body_sql).await?;
        let body = body_value
            .pointer("/data")
            .and_then(|v| v.as_array())
            .and_then(|rows| rows.first())
            .and_then(|r| r.as_array())
            .and_then(|r| r.first())
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let Some(body) = body else {
            return Ok(None);
        };

        let deps_sql = format!(
            "SELECT t.table_catalog, t.table_schema, t.table_name, \
                    COALESCE(b.table_type, ''), COALESCE(v.is_secure, 'NO') \
             FROM \"{db}\".INFORMATION_SCHEMA.VIEW_TABLE_USAGE t \
             LEFT JOIN \"{db}\".INFORMATION_SCHEMA.TABLES b \
               ON b.table_catalog = t.table_catalog \
              AND b.table_schema  = t.table_schema \
              AND b.table_name    = t.table_name \
             LEFT JOIN \"{db}\".INFORMATION_SCHEMA.VIEWS v \
               ON v.table_catalog = t.table_catalog \
              AND v.table_schema  = t.table_schema \
              AND v.table_name    = t.table_name \
             WHERE t.view_schema = '{schema}' \
               AND t.view_name   = '{name}'",
            db = sql_ident_escape(&table.database),
            schema = sql_literal_escape(&table.schema),
            name = sql_literal_escape(&table.name),
        );
        let deps_value = self.execute_json_rowset(token, &deps_sql).await?;
        let mut base_tables: Vec<(TableRef, ObjectKind)> = Vec::new();
        if let Some(rows) = deps_value.pointer("/data").and_then(|v| v.as_array()) {
            for row in rows {
                let Some(cols) = row.as_array() else {
                    continue;
                };
                if cols.len() < 5 {
                    continue;
                }
                let db = cols[0].as_str().unwrap_or_default().to_string();
                let sc = cols[1].as_str().unwrap_or_default().to_string();
                let nm = cols[2].as_str().unwrap_or_default().to_string();
                if db.is_empty() || sc.is_empty() || nm.is_empty() {
                    continue;
                }
                let table_type = cols[3].as_str().unwrap_or_default();
                let is_secure = matches!(
                    cols[4].as_str().unwrap_or(""),
                    "YES" | "yes" | "TRUE" | "true"
                );
                let kind = ObjectKind::from_snowflake(table_type, is_secure);
                base_tables.push((TableRef::new(db, sc, nm), kind));
            }
        }

        // `IS_SECURE` for the view itself came through
        // `describe_object_kind`; we re-query here only if we need
        // the owner-view-specific masking signal. Keep the lookup
        // self-contained: the caller may not have probed the kind
        // before asking for the body.
        let kind = self.describe_object_kind(token, table).await?;
        let is_secure = matches!(kind, ObjectKind::SecureView);

        Ok(Some(ViewDef {
            body,
            base_tables,
            is_secure,
        }))
    }

    /// Shared `/api/v2/statements` helper — issues a SELECT and
    /// returns the parsed JSON response.
    ///
    /// Used by the object-kind + view-definition probes; we don't
    /// need Arrow here since the rowsets are tiny (<100 rows).
    async fn execute_json_rowset(&self, token: &str, sql: &str) -> Result<serde_json::Value> {
        let body = self.with_session_context(serde_json::json!({
            "statement": sql,
            "timeout":   30,
        }));
        let resp = self
            .http
            .post(self.url("/api/v2/statements"))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|e| MeltError::Http(format!("statements send: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(MeltError::backend(format!("statements {status}: {body}")));
        }
        resp.json::<serde_json::Value>()
            .await
            .map_err(|e| MeltError::Http(format!("statements parse: {e}")))
    }

    /// Merge any configured session-context fields (`warehouse`,
    /// `role`, `database`, `schema`) into a `/api/v2/statements`
    /// request body. Empty fields are omitted so Snowflake falls
    /// back to the service user's DEFAULT_* settings.
    fn with_session_context(&self, mut body: serde_json::Value) -> serde_json::Value {
        if let Some(obj) = body.as_object_mut() {
            for (k, v) in [
                ("warehouse", &self.cfg.warehouse),
                ("role", &self.cfg.role),
                ("database", &self.cfg.database),
                ("schema", &self.cfg.schema),
            ] {
                if !v.is_empty() {
                    obj.insert(k.to_string(), serde_json::Value::String(v.clone()));
                }
            }
        }
        body
    }

    /// Exchange a Snowflake-signed JWT for a 1-hour session token.
    /// Called from `service_token()` on the key-pair path.
    async fn exchange_jwt(&self, user: &str, jwt: &str) -> Result<String> {
        let body = serde_json::json!({
            "data": {
                "ACCOUNT_NAME":  self.cfg.account,
                "LOGIN_NAME":    user,
                "AUTHENTICATOR": "SNOWFLAKE_JWT",
                "TOKEN":         jwt,
                "CLIENT_APP_ID": "Melt",
                "CLIENT_APP_VERSION": env!("CARGO_PKG_VERSION"),
            }
        });
        let resp = self
            .http
            .post(self.url("/session/v1/login-request"))
            .json(&body)
            .send()
            .await
            .map_err(|e| MeltError::Http(format!("jwt exchange send: {e}")))?;
        let parsed: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| MeltError::Http(format!("jwt exchange parse: {e}")))?;

        if parsed.get("success") == Some(&serde_json::Value::Bool(false)) {
            let code = parsed
                .get("code")
                .and_then(|v| v.as_str())
                .unwrap_or("?")
                .to_string();
            let message = parsed
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("?")
                .to_string();
            return Err(MeltError::Snowflake { code, message });
        }

        parsed
            .pointer("/data/token")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| MeltError::backend("jwt exchange: no token in response"))
    }
}

/// Parsed output of [`SnowflakeClient::fetch_view_definition`].
///
/// `body` is the full `CREATE VIEW …` DDL text from `GET_DDL` — the
/// caller extracts the `SELECT` portion. `base_tables` are the
/// immediate relations the view reads; `is_secure` is `true` when the
/// view is defined as a secure view (body is still masked in that
/// case and bootstrap should reject before trying to decompose).
#[derive(Clone, Debug)]
pub struct ViewDef {
    pub body: String,
    pub base_tables: Vec<(TableRef, ObjectKind)>,
    pub is_secure: bool,
}

/// Quote an identifier for inlining inside `"<db>".INFORMATION_SCHEMA.X`.
/// Snowflake identifiers in this context are already case-sensitive
/// quoted — escape any embedded double quotes by doubling.
fn sql_ident_escape(s: &str) -> String {
    s.replace('"', "\"\"")
}

/// Escape a string for inlining inside single-quoted SQL literals.
/// `'O''Brien'` style escaping.
fn sql_literal_escape(s: &str) -> String {
    s.replace('\'', "''")
}

/// Sign a Snowflake-shaped JWT for key-pair authentication.
///
/// Snowflake's JWT spec (documented in their JDBC / Python SDKs;
/// absent from the public REST API docs) requires:
///
/// - `alg`  = `RS256`
/// - `iss`  = `<UPPER_ACCOUNT>.<UPPER_USER>.SHA256:<fingerprint>`
///   where `<fingerprint>` is the base64-encoded SHA-256 of the
///   DER-encoded `SubjectPublicKeyInfo` of the RSA public key.
/// - `sub`  = `<UPPER_ACCOUNT>.<UPPER_USER>`
/// - `iat`  = now (seconds since epoch)
/// - `exp`  = iat + 3600 (Snowflake caps at 1 hour)
///
/// Everything uppercase matters — Snowflake matches case-sensitively
/// on these claims.
pub(crate) fn sign_snowflake_jwt(account: &str, user: &str, pem: &[u8]) -> Result<String> {
    use base64::engine::general_purpose::STANDARD as b64;
    use base64::Engine as _;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use pkcs8::{DecodePrivateKey, EncodePublicKey};
    use sha2::{Digest, Sha256};

    let pem_str = std::str::from_utf8(pem)
        .map_err(|_| MeltError::config("service private key PEM is not valid UTF-8"))?;
    let key = rsa::RsaPrivateKey::from_pkcs8_pem(pem_str)
        .or_else(|_| {
            // Some operators export PKCS#1 ("BEGIN RSA PRIVATE KEY").
            use rsa::pkcs1::DecodeRsaPrivateKey;
            rsa::RsaPrivateKey::from_pkcs1_pem(pem_str)
        })
        .map_err(|e| {
            MeltError::config(format!(
                "service private key: invalid PKCS#8 or PKCS#1 PEM: {e}"
            ))
        })?;

    let public = rsa::RsaPublicKey::from(&key);
    let spki_doc = public
        .to_public_key_der()
        .map_err(|e| MeltError::backend(format!("SPKI encode: {e}")))?;
    let fingerprint = b64.encode(Sha256::digest(spki_doc.as_bytes()));

    let account_u = account.to_uppercase();
    let user_u = user.to_uppercase();
    let now = chrono::Utc::now().timestamp();
    let claims = serde_json::json!({
        "iss": format!("{account_u}.{user_u}.SHA256:{fingerprint}"),
        "sub": format!("{account_u}.{user_u}"),
        "iat": now,
        "exp": now + 3600,
    });

    let signing_key = EncodingKey::from_rsa_pem(pem)
        .map_err(|e| MeltError::backend(format!("jsonwebtoken: load RSA signing key: {e}")))?;
    encode(&Header::new(Algorithm::RS256), &claims, &signing_key)
        .map_err(|e| MeltError::backend(format!("jsonwebtoken: sign JWT: {e}")))
}

/// Return shape for [`SnowflakeClient::passthrough_full`] — everything
/// a Melt handler needs to replay the upstream reply to the driver.
/// `body` is pinned + boxed so it can be stored / moved across async
/// boundaries inside handlers without pulling the concrete stream
/// type into the public API.
pub struct PassthroughResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: std::pin::Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
}

/// Pull RecordBatch(es) out of one partition's response body.
/// Tail partitions don't carry their own `rowType`, so the caller
/// passes the partition-0 metadata via `row_type_hint`.
fn extract_partition_batches(
    value: &serde_json::Value,
    row_type_hint: Option<&[serde_json::Value]>,
    out: &mut Vec<arrow::record_batch::RecordBatch>,
) -> Result<()> {
    if let Some(b64) = value.pointer("/data/rowsetBase64").and_then(|v| v.as_str()) {
        decode_arrow_rowset_into(b64, out)?;
        return Ok(());
    }

    // JSON rowset: `rowType` from partition 0 or `row_type_hint` for tails.
    let rows: &[serde_json::Value] = value
        .pointer("/data")
        .and_then(|v| v.as_array())
        .map(|a| a.as_slice())
        .unwrap_or(&[]);
    let row_type_in_payload = value
        .pointer("/resultSetMetaData/rowType")
        .and_then(|v| v.as_array());
    let row_type: Option<&[serde_json::Value]> =
        row_type_in_payload.map(|a| a.as_slice()).or(row_type_hint);

    match row_type {
        Some(rt) if rt.is_empty() && rows.is_empty() => Ok(()), // empty partition, nothing to do
        Some(rt) => {
            let batch = json_rowset_to_batch(rt, rows)?;
            if batch.num_rows() > 0 {
                out.push(batch);
            }
            Ok(())
        }
        None => {
            let keys = value
                .as_object()
                .map(|o| o.keys().cloned().collect::<Vec<_>>().join(","))
                .unwrap_or_default();
            Err(MeltError::backend(format!(
                "execute_arrow: partition missing both rowsetBase64 and resultSetMetaData.rowType. keys=[{keys}]"
            )))
        }
    }
}

/// Decode one base64 Arrow IPC blob into RecordBatches.
fn decode_arrow_rowset_into(
    b64: &str,
    out: &mut Vec<arrow::record_batch::RecordBatch>,
) -> Result<()> {
    use arrow_ipc::reader::StreamReader;
    use base64::Engine as _;

    let ipc_bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| MeltError::backend(format!("base64 decode: {e}")))?;
    let cursor = std::io::Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| MeltError::backend(format!("arrow ipc: {e}")))?;
    for batch in reader {
        out.push(batch.map_err(|e| MeltError::backend(format!("arrow batch: {e}")))?);
    }
    Ok(())
}

/// Build a RecordBatch from Snowflake's inline JSON rowset shape:
///
/// ```json
/// "resultSetMetaData": { "rowType": [{"name":"id","type":"fixed",...}, ...] }
/// "data": [ ["1","alice",...], ["2","bob",...], ... ]
/// ```
///
/// Values are string-encoded on the wire regardless of the nominal
/// Snowflake type. This function reads the type from each
/// `rowType` entry and parses cells into the corresponding Arrow
/// array so downstream consumers (the router's DuckDB executor, the
/// sync apply path, `melt debug count/rows`) get semantically correct
/// columns — `ORDER BY CUSTOMER_NUMBER` sorts numerically,
/// `SUM(amount)` aggregates without a cast error, `DATEADD(DAY, …)`
/// works on dates, etc.
///
/// Type mapping (see also `snowflake_type_to_arrow` below):
///
/// | Snowflake `type`         | Arrow type                       | Cell parse |
/// |--------------------------|----------------------------------|------------|
/// | `text`                   | `Utf8`                           | as-is      |
/// | `fixed`, scale=0, p≤18   | `Int64`                          | `str::parse::<i64>` |
/// | `fixed`, scale=0, p>18   | `Utf8` (precision > i64 range)   | as-is      |
/// | `fixed`, scale>0         | `Float64`                        | `str::parse::<f64>` |
/// | `real`                   | `Float64`                        | `str::parse::<f64>` |
/// | `boolean`                | `Boolean`                        | `"true"/"false"` or `"1"/"0"` |
/// | `date`                   | `Date32`                         | integer days since epoch |
/// | `timestamp_ntz`          | `Timestamp(Nanosecond, None)`    | `"<secs>.<frac>"` → nanos |
/// | `timestamp_ltz`/`_tz`    | `Timestamp(Nanosecond, Some(UTC))` | same |
/// | anything else            | `Utf8`                           | as-is — operator sees raw Snowflake encoding |
///
/// Parse failures on individual cells become NULLs rather than
/// erroring the whole query: malformed values are less dangerous than
/// an opaque row-apply failure.
fn json_rowset_to_batch(
    row_type: &[serde_json::Value],
    rows: &[serde_json::Value],
) -> Result<arrow::record_batch::RecordBatch> {
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    let n_cols = row_type.len();
    let col_types: Vec<ColType> = row_type.iter().map(ColType::from_row_type).collect();

    let fields: Vec<Field> = row_type
        .iter()
        .zip(col_types.iter())
        .map(|(rt, ct)| {
            let name = rt
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let nullable = rt.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
            Field::new(name, ct.arrow_type(), nullable)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Transpose JSON rows to columnar `Vec`s for typed Arrow builders.
    let mut cols: Vec<Vec<Option<String>>> = vec![Vec::with_capacity(rows.len()); n_cols];
    for row in rows {
        let cells = row.as_array().ok_or_else(|| {
            MeltError::backend(format!("json_rowset_to_batch: row is not an array: {row}"))
        })?;
        for (i, col) in cols.iter_mut().enumerate().take(n_cols) {
            let value = match cells.get(i) {
                Some(serde_json::Value::String(s)) => Some(s.clone()),
                Some(serde_json::Value::Null) | None => None,
                Some(other) => Some(other.to_string()),
            };
            col.push(value);
        }
    }

    let arrays: Vec<arrow::array::ArrayRef> = col_types
        .into_iter()
        .zip(cols)
        .map(|(ct, vals)| ct.build_array(vals))
        .collect();

    arrow::record_batch::RecordBatch::try_new(schema, arrays)
        .map_err(|e| MeltError::backend(format!("json_rowset_to_batch: RecordBatch::try_new: {e}")))
}

/// Internal per-column type descriptor. Encodes just enough of the
/// Snowflake column metadata to pick an Arrow type and parse cells
/// correctly. Anything we don't recognise degrades to `Utf8` so we
/// never error out of a bootstrap on an unusual column.
#[derive(Clone, Copy, Debug)]
enum ColType {
    Utf8,
    Int64,
    Float64,
    Boolean,
    Date32,
    /// `tz=Some("UTC")` when the source was `timestamp_ltz` /
    /// `timestamp_tz`, `None` for `timestamp_ntz`.
    TimestampNanos {
        tz_utc: bool,
    },
    /// `NUMBER(precision, scale)` stored as Arrow `Decimal128`. We
    /// clamp precision to `arrow_schema::DECIMAL128_MAX_PRECISION`
    /// (38) in the constructor — Snowflake's max is 38 anyway.
    Decimal {
        precision: u8,
        scale: i8,
    },
}

impl ColType {
    fn from_row_type(rt: &serde_json::Value) -> Self {
        let kind = rt
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let scale = rt.get("scale").and_then(|v| v.as_i64()).unwrap_or(0);
        let precision = rt.get("precision").and_then(|v| v.as_i64()).unwrap_or(0);
        // Clamp to Arrow Decimal128's 38-digit cap (= Snowflake's NUMBER cap).
        let clamp = |p: i64| p.clamp(1, 38) as u8;
        match kind.as_str() {
            "text" => ColType::Utf8,
            // NUMBER(p, 0) with p ≤ 18 fits in Int64 exactly.
            "fixed" if scale == 0 && precision <= 18 => ColType::Int64,
            // Wider NUMBERs need Decimal128 (default NUMBER(38,0) overflows i64).
            "fixed" => ColType::Decimal {
                precision: clamp(precision.max(1)),
                scale: scale.clamp(-38, 38) as i8,
            },
            "real" | "double" | "float" => ColType::Float64,
            "boolean" => ColType::Boolean,
            "date" => ColType::Date32,
            "timestamp_ntz" => ColType::TimestampNanos { tz_utc: false },
            "timestamp_ltz" | "timestamp_tz" => ColType::TimestampNanos { tz_utc: true },
            // variant/object/array/binary/time/geography → Utf8 passthrough.
            _ => ColType::Utf8,
        }
    }

    fn arrow_type(&self) -> arrow_schema::DataType {
        use arrow_schema::{DataType, TimeUnit};
        match self {
            ColType::Utf8 => DataType::Utf8,
            ColType::Int64 => DataType::Int64,
            ColType::Float64 => DataType::Float64,
            ColType::Boolean => DataType::Boolean,
            ColType::Date32 => DataType::Date32,
            ColType::TimestampNanos { tz_utc } => DataType::Timestamp(
                TimeUnit::Nanosecond,
                if *tz_utc { Some("UTC".into()) } else { None },
            ),
            ColType::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale),
        }
    }

    fn build_array(self, vals: Vec<Option<String>>) -> arrow::array::ArrayRef {
        use arrow::array::{
            BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
            TimestampNanosecondArray,
        };
        use std::sync::Arc;
        match self {
            ColType::Utf8 => Arc::new(StringArray::from(vals)) as _,
            ColType::Int64 => {
                let parsed: Vec<Option<i64>> = vals
                    .into_iter()
                    .map(|o| o.and_then(|s| s.parse::<i64>().ok()))
                    .collect();
                Arc::new(Int64Array::from(parsed)) as _
            }
            ColType::Float64 => {
                let parsed: Vec<Option<f64>> = vals
                    .into_iter()
                    .map(|o| o.and_then(|s| s.parse::<f64>().ok()))
                    .collect();
                Arc::new(Float64Array::from(parsed)) as _
            }
            ColType::Boolean => {
                let parsed: Vec<Option<bool>> = vals
                    .into_iter()
                    .map(|o| o.as_deref().and_then(parse_bool))
                    .collect();
                Arc::new(BooleanArray::from(parsed)) as _
            }
            ColType::Date32 => {
                // Snowflake encodes DATE as days since epoch (= Arrow Date32).
                let parsed: Vec<Option<i32>> = vals
                    .into_iter()
                    .map(|o| o.and_then(|s| s.parse::<i32>().ok()))
                    .collect();
                Arc::new(Date32Array::from(parsed)) as _
            }
            ColType::TimestampNanos { tz_utc } => {
                // Snowflake encodes TIMESTAMP_* as `"<secs>.<9-digit-frac>"`.
                let parsed: Vec<Option<i64>> = vals
                    .into_iter()
                    .map(|o| o.as_deref().and_then(parse_snowflake_timestamp_nanos))
                    .collect();
                let arr = if tz_utc {
                    TimestampNanosecondArray::from(parsed).with_timezone("UTC".to_string())
                } else {
                    TimestampNanosecondArray::from(parsed)
                };
                Arc::new(arr) as _
            }
            ColType::Decimal { precision, scale } => {
                // Snowflake NUMBER → Arrow Decimal128 unscaled i128 (see parse_decimal_i128).
                let parsed: Vec<Option<i128>> = vals
                    .into_iter()
                    .map(|o| o.as_deref().and_then(|s| parse_decimal_i128(s, scale)))
                    .collect();
                let arr = arrow::array::Decimal128Array::from(parsed)
                    .with_precision_and_scale(precision, scale)
                    .expect("decimal128 precision/scale");
                Arc::new(arr) as _
            }
        }
    }
}

/// Parse a Snowflake-encoded NUMBER string into the unscaled i128
/// value expected by Arrow's `Decimal128Array`:
///
/// | Input       | Scale | Output     |
/// |-------------|-------|------------|
/// | `"888000001"` | 0   | `888000001` |
/// | `"123.45"`    | 2   | `12345`    |
/// | `"1.2"`       | 3   | `1200`     | (pad to target scale)
/// | `"1.234567"`  | 3   | `1234`     | (truncate toward zero)
/// | `"-42.5"`     | 1   | `-425`     |
///
/// Returns `None` on any parse failure — malformed cells become NULL
/// rather than erroring the whole batch.
fn parse_decimal_i128(s: &str, scale: i8) -> Option<i128> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let scale = scale.max(0) as usize;
    let (int_part, frac_part) = s.split_once('.').unwrap_or((s, ""));
    let mut frac = String::from(frac_part);
    if frac.len() < scale {
        frac.push_str(&"0".repeat(scale - frac.len()));
    } else if frac.len() > scale {
        frac.truncate(scale);
    }
    let combined: String = if scale == 0 {
        int_part.to_string()
    } else {
        format!("{int_part}{frac}")
    };
    combined.parse().ok()
}

fn parse_bool(s: &str) -> Option<bool> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "t" | "1" => Some(true),
        "false" | "f" | "0" => Some(false),
        _ => None,
    }
}

/// Parse a Snowflake-encoded timestamp string (`"<secs>.<frac9>"`)
/// into nanoseconds since epoch. Examples:
///
/// * `"1774677600.000000000"` → `1_774_677_600_000_000_000`
/// * `"-1000.500000000"`      → `-999_500_000_000`
/// * `"12345"`                → `12_345_000_000_000` (secs only, no frac)
///
/// Returns `None` on anything we can't read — malformed input lands
/// as a NULL cell rather than erroring the whole batch.
fn parse_snowflake_timestamp_nanos(s: &str) -> Option<i64> {
    let (secs_str, frac_str) = match s.split_once('.') {
        Some((a, b)) => (a, b),
        None => (s, "0"),
    };
    let secs: i64 = secs_str.parse().ok()?;
    // Right-pad/truncate frac digits to exactly 9 (= nanoseconds).
    let mut frac = String::from(frac_str);
    if frac.len() < 9 {
        frac.push_str(&"0".repeat(9 - frac.len()));
    } else {
        frac.truncate(9);
    }
    let nanos: i64 = frac.parse().ok()?;
    let sign = if secs_str.starts_with('-') { -1 } else { 1 };
    secs.checked_mul(1_000_000_000)?.checked_add(sign * nanos)
}

/// Replay the driver's request-framing headers (compression, content
/// negotiation, user-agent) onto the outbound `reqwest::RequestBuilder`.
///
/// Allowlist — NOT copy-everything — so we don't leak `Host`
/// (wrong hostname upstream), `Authorization` (handled separately by
/// downstream callers), or any hop-by-hop header (`Connection`,
/// `Transfer-Encoding`, `Upgrade`) to Snowflake.
///
/// Defaults `content-type: application/json` if the caller didn't set
/// one, preserving the behaviour older Melt builds had.
fn apply_forwarded_headers(
    mut req: reqwest::RequestBuilder,
    source: &HeaderMap,
) -> reqwest::RequestBuilder {
    // Allowlist for snowflake-connector-python + JDBC driver framing.
    const FORWARDED: &[&str] = &[
        "content-type",
        "content-encoding",
        "accept",
        "accept-encoding",
        "accept-language",
        "user-agent",
        "x-snowflake-service",
    ];

    let mut saw_content_type = false;
    for name in FORWARDED {
        if let Some(value) = source.get(*name) {
            if *name == "content-type" {
                saw_content_type = true;
            }
            if let Ok(v) = HeaderValue::from_bytes(value.as_bytes()) {
                req = req.header(*name, v);
            }
        }
    }
    if !saw_content_type {
        req = req.header("content-type", "application/json");
    }
    req
}

/// Validate / inject `data.ACCOUNT_NAME` in a Snowflake login body
/// against the configured upstream account.
///
/// Returns the (possibly rewritten) body bytes ready to forward, or
/// `MeltError::AccountMismatch` if the body explicitly named a
/// different account. Non-JSON / wrong-shape bodies pass through —
/// Snowflake rejects them itself.
pub(crate) fn enforce_account(configured: &str, body: Bytes) -> Result<Bytes> {
    use serde_json::Value;

    let mut value: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return Ok(body), // not JSON — let upstream reject
    };

    let Some(data) = value.get_mut("data").and_then(Value::as_object_mut) else {
        return Ok(body); // not the shape we're checking — pass through
    };

    match data.get("ACCOUNT_NAME").and_then(Value::as_str) {
        Some(supplied) if !accounts_equal(supplied, configured) => {
            Err(MeltError::AccountMismatch {
                configured: configured.to_string(),
                supplied: supplied.to_string(),
            })
        }
        Some(_) => Ok(body), // matches — forward unchanged
        None => {
            // Absent — inject so the upstream body is internally
            // consistent with the URL we're POSTing to.
            data.insert(
                "ACCOUNT_NAME".to_string(),
                Value::String(configured.to_string()),
            );
            let bytes = serde_json::to_vec(&value)
                .map_err(|e| MeltError::backend(format!("re-serialize login body: {e}")))?;
            Ok(Bytes::from(bytes))
        }
    }
}

/// Snowflake account locators are case-insensitive. Compare with
/// uppercase normalization so a driver that lowercases the locator
/// (`xy12345` vs `XY12345`, or `myorg-myaccount` vs the same in caps)
/// doesn't trigger a false mismatch error.
fn accounts_equal(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_account_passes_through_unchanged() {
        let body = Bytes::from(
            r#"{"data":{"LOGIN_NAME":"u","PASSWORD":"p","ACCOUNT_NAME":"ACMECORP-PROD123"}}"#,
        );
        let out = enforce_account("ACMECORP-PROD123", body.clone()).unwrap();
        assert_eq!(out, body);
    }

    #[test]
    fn case_difference_is_not_a_mismatch() {
        let body = Bytes::from(r#"{"data":{"LOGIN_NAME":"u","ACCOUNT_NAME":"acmecorp-prod123"}}"#);
        let out = enforce_account("ACMECORP-PROD123", body.clone()).unwrap();
        assert_eq!(out, body);
    }

    #[test]
    fn absent_account_gets_injected() {
        let body = Bytes::from(r#"{"data":{"LOGIN_NAME":"u","PASSWORD":"p"}}"#);
        let out = enforce_account("ACMECORP-PROD123", body).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["data"]["ACCOUNT_NAME"].as_str(), Some("ACMECORP-PROD123"));
        assert_eq!(v["data"]["LOGIN_NAME"].as_str(), Some("u"));
    }

    #[test]
    fn mismatched_account_returns_account_mismatch() {
        let body = Bytes::from(r#"{"data":{"LOGIN_NAME":"u","ACCOUNT_NAME":"WRONG-ACCT"}}"#);
        let err = enforce_account("ACMECORP-PROD123", body).unwrap_err();
        match err {
            MeltError::AccountMismatch {
                configured,
                supplied,
            } => {
                assert_eq!(configured, "ACMECORP-PROD123");
                assert_eq!(supplied, "WRONG-ACCT");
            }
            other => panic!("expected AccountMismatch, got {other:?}"),
        }
    }

    #[test]
    fn non_json_body_is_passed_through() {
        let body = Bytes::from_static(b"<not json at all>");
        let out = enforce_account("ACMECORP-PROD123", body.clone()).unwrap();
        assert_eq!(out, body);
    }

    #[test]
    fn missing_data_object_is_passed_through() {
        let body = Bytes::from(r#"{"foo":"bar"}"#);
        let out = enforce_account("ACMECORP-PROD123", body.clone()).unwrap();
        assert_eq!(out, body);
    }

    /// Generate an ephemeral PKCS#8 RSA-2048 PEM for one test.
    fn ephemeral_pkcs8_pem() -> String {
        use pkcs8::EncodePrivateKey;
        use rand::thread_rng;
        let priv_key = rsa::RsaPrivateKey::new(&mut thread_rng(), 2048).expect("generate RSA-2048");
        priv_key
            .to_pkcs8_pem(pkcs8::LineEnding::LF)
            .expect("PEM encode")
            .to_string()
    }

    #[test]
    fn jwt_claims_have_snowflake_shape() {
        use jsonwebtoken::{decode, DecodingKey, Validation};

        let pem = ephemeral_pkcs8_pem();
        let jwt = sign_snowflake_jwt("acmecorp-prod123", "melt_sync_user", pem.as_bytes())
            .expect("signs");

        let mut validation = Validation::new(jsonwebtoken::Algorithm::RS256);
        validation.insecure_disable_signature_validation();
        validation.validate_exp = false;

        let dk = DecodingKey::from_rsa_pem(pem.as_bytes()).unwrap();
        let decoded = decode::<serde_json::Value>(&jwt, &dk, &validation).expect("decode");
        let claims = decoded.claims;

        let iss = claims["iss"].as_str().unwrap();
        assert!(
            iss.starts_with("ACMECORP-PROD123.MELT_SYNC_USER.SHA256:"),
            "iss was {iss}"
        );
        let fp = iss
            .strip_prefix("ACMECORP-PROD123.MELT_SYNC_USER.SHA256:")
            .unwrap();
        assert_eq!(fp.len(), 44);
        assert!(fp.ends_with('='));

        assert_eq!(claims["sub"], "ACMECORP-PROD123.MELT_SYNC_USER");

        let iat = claims["iat"].as_i64().unwrap();
        let exp = claims["exp"].as_i64().unwrap();
        assert_eq!(exp - iat, 3600);
    }

    #[test]
    fn jwt_signing_rejects_non_pem_input() {
        let err = sign_snowflake_jwt("acct", "user", b"not a pem").unwrap_err();
        match err {
            MeltError::Config(msg) => assert!(msg.contains("private key")),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn jwt_fingerprint_is_deterministic_per_key() {
        let pem_a = ephemeral_pkcs8_pem();
        let pem_b = ephemeral_pkcs8_pem();

        let fp_a1 = extract_fingerprint(&pem_a);
        let fp_a2 = extract_fingerprint(&pem_a);
        let fp_b = extract_fingerprint(&pem_b);

        assert_eq!(fp_a1, fp_a2);
        assert_ne!(fp_a1, fp_b);
    }

    fn extract_fingerprint(pem: &str) -> String {
        use jsonwebtoken::{decode, DecodingKey, Validation};
        let jwt = sign_snowflake_jwt("a", "u", pem.as_bytes()).unwrap();
        let mut v = Validation::new(jsonwebtoken::Algorithm::RS256);
        v.insecure_disable_signature_validation();
        v.validate_exp = false;
        let dk = DecodingKey::from_rsa_pem(pem.as_bytes()).unwrap();
        let d = decode::<serde_json::Value>(&jwt, &dk, &v).unwrap();
        d.claims["iss"].as_str().unwrap().to_string()
    }

    #[test]
    fn with_session_context_injects_set_fields() {
        let cfg = SnowflakeConfig {
            account: "acct".into(),
            warehouse: "MELT_WH".into(),
            role: "MELT_SYNC_ROLE".into(),
            // database/schema intentionally empty
            ..Default::default()
        };
        let client = SnowflakeClient::new(cfg);
        let body = client.with_session_context(serde_json::json!({
            "statement": "SELECT 1",
            "timeout":   60,
        }));
        assert_eq!(body["warehouse"], "MELT_WH");
        assert_eq!(body["role"], "MELT_SYNC_ROLE");
        assert!(body.get("database").is_none());
        assert!(body.get("schema").is_none());
        assert_eq!(body["statement"], "SELECT 1");
    }

    #[test]
    fn with_session_context_omits_empty_fields() {
        let cfg = SnowflakeConfig {
            account: "acct".into(),
            ..Default::default()
        };
        let client = SnowflakeClient::new(cfg);
        let body = client.with_session_context(serde_json::json!({
            "statement": "SELECT 1",
        }));
        let obj = body.as_object().unwrap();
        assert!(!obj.contains_key("warehouse"));
        assert!(!obj.contains_key("role"));
        assert!(!obj.contains_key("database"));
        assert!(!obj.contains_key("schema"));
    }
}
