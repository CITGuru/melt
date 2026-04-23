//! Iceberg REST catalog client.
//!
//! Implements the subset of the [Iceberg REST spec][1] needed for
//! discovery and `iceberg_scan(...)` view registration:
//!
//! - `GET /v1/namespaces` (list namespaces, optionally with `parent`)
//! - `GET /v1/namespaces/{ns}/tables`
//! - `GET /v1/namespaces/{ns}/tables/{name}` (table metadata)
//!
//! Authentication is left as `Authorization: Bearer <token>` if the
//! configured `rest_uri` exposes one via env (`MELT_ICEBERG_TOKEN`),
//! otherwise unauthenticated.
//!
//! [1]: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
//!
//! Returns shapes are kept minimal — only the fields Melt consumes.

use melt_core::{MeltError, Result, TableRef};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct NamespaceList {
    pub namespaces: Vec<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TableIdent {
    pub namespace: Vec<String>,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TableList {
    pub identifiers: Vec<TableIdent>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TableMetadata {
    /// `s3://bucket/warehouse/db.db/orders/metadata/00001-...metadata.json`
    pub metadata_location: String,
    /// Total uncompressed bytes across all files in the current snapshot.
    /// Optional in the spec; defaults to 0 when absent.
    #[serde(default)]
    pub total_files_size: Option<u64>,
}

pub struct RestClient {
    base: String,
    token: Option<String>,
    http: reqwest::Client,
    /// Synthetic database used for table refs when the REST catalog
    /// uses a single-level namespace. Snowflake refs are
    /// `db.schema.name`; a single-level Iceberg namespace becomes
    /// `<warehouse_name>.<namespace>.<table>`.
    pub default_database: String,
}

impl RestClient {
    pub fn new(base: impl Into<String>, default_database: impl Into<String>) -> Self {
        Self {
            base: base.into().trim_end_matches('/').to_string(),
            token: std::env::var("MELT_ICEBERG_TOKEN").ok(),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("valid reqwest client"),
            default_database: default_database.into(),
        }
    }

    /// Replace the bearer token (used by Polaris which has its own
    /// service-principal token env var). Passing `None` clears it.
    pub fn with_token_override(mut self, token: Option<String>) -> Self {
        if token.is_some() {
            self.token = token;
        }
        self
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let mut b = self.http.request(method, self.url(path));
        if let Some(t) = &self.token {
            b = b.bearer_auth(t);
        }
        b
    }

    pub async fn list_namespaces(&self) -> Result<Vec<Vec<String>>> {
        let resp = self
            .req(reqwest::Method::GET, "/v1/namespaces")
            .send()
            .await
            .map_err(|e| MeltError::Http(format!("list_namespaces: {e}")))?;
        if !resp.status().is_success() {
            return Err(MeltError::BackendUnavailable(format!(
                "iceberg list_namespaces upstream {}",
                resp.status()
            )));
        }
        let parsed: NamespaceList = resp
            .json()
            .await
            .map_err(|e| MeltError::Http(format!("list_namespaces parse: {e}")))?;
        Ok(parsed.namespaces)
    }

    pub async fn list_tables(&self, namespace: &[String]) -> Result<Vec<TableIdent>> {
        let ns = namespace.join("%1F"); // unit-separator per spec
        let path = format!("/v1/namespaces/{ns}/tables");
        let resp = self
            .req(reqwest::Method::GET, &path)
            .send()
            .await
            .map_err(|e| MeltError::Http(format!("list_tables: {e}")))?;
        if !resp.status().is_success() {
            return Err(MeltError::BackendUnavailable(format!(
                "iceberg list_tables upstream {}",
                resp.status()
            )));
        }
        let parsed: TableList = resp
            .json()
            .await
            .map_err(|e| MeltError::Http(format!("list_tables parse: {e}")))?;
        Ok(parsed.identifiers)
    }

    pub async fn load_table(&self, ident: &TableIdent) -> Result<TableMetadata> {
        let ns = ident.namespace.join("%1F");
        let path = format!("/v1/namespaces/{ns}/tables/{}", ident.name);
        let resp = self
            .req(reqwest::Method::GET, &path)
            .send()
            .await
            .map_err(|e| MeltError::Http(format!("load_table: {e}")))?;
        if !resp.status().is_success() {
            return Err(MeltError::BackendUnavailable(format!(
                "iceberg load_table upstream {}",
                resp.status()
            )));
        }
        // Snake-case nested under either "metadata" or top-level
        // depending on REST server flavour.
        let value: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| MeltError::Http(format!("load_table parse: {e}")))?;

        let metadata_location = value
            .get("metadata-location")
            .or_else(|| value.get("metadata_location"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                MeltError::backend("iceberg REST: missing metadata-location in load_table")
            })?
            .to_string();

        // Crude size hint: `metadata.snapshots.last().summary.total-files-size`.
        let total_files_size = value
            .pointer("/metadata/snapshots")
            .and_then(|s| s.as_array())
            .and_then(|arr| arr.last())
            .and_then(|snap| snap.pointer("/summary/total-files-size"))
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok());

        Ok(TableMetadata {
            metadata_location,
            total_files_size,
        })
    }

    /// Map a REST-catalog `(namespace, name)` pair into Melt's
    /// `db.schema.name` shape so the router and proxy stay
    /// dialect-symmetric across DuckLake and Iceberg.
    pub fn to_table_ref(&self, ident: &TableIdent) -> TableRef {
        match ident.namespace.as_slice() {
            [] => TableRef::new(self.default_database.clone(), "public", ident.name.clone()),
            [schema] => TableRef::new(
                self.default_database.clone(),
                schema.clone(),
                ident.name.clone(),
            ),
            // Multi-level namespace — first part wins as database,
            // remaining join as a dotted schema.
            multi => {
                let database = multi[0].clone();
                let schema = multi[1..].join(".");
                TableRef::new(database, schema, ident.name.clone())
            }
        }
    }
}
