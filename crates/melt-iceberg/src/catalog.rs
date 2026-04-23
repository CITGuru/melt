use std::collections::{HashMap, HashSet};

use melt_core::{MeltError, ProtectedTable, Result, TableRef};
use parking_lot::RwLock;

use crate::config::{IcebergCatalogKind, IcebergConfig};
use crate::glue::GlueClient;
use crate::rest::RestClient;

/// Iceberg catalog adapter. The Iceberg side has no native `Postgres`
/// table for stats / policies, so we keep an in-memory mirror that
/// sync refreshes from the catalog API and from Snowflake's
/// `POLICY_REFERENCES`.
pub struct IcebergCatalogClient {
    cfg: IcebergConfig,
    state: RwLock<CatalogState>,
    rest: Option<RestClient>,
    glue: Option<GlueClient>,
}

#[derive(Default)]
struct CatalogState {
    tables: HashSet<TableRef>,
    estimates: HashMap<TableRef, u64>,
    markers: HashMap<TableRef, String>,
}

impl IcebergCatalogClient {
    pub async fn connect(cfg: &IcebergConfig) -> Result<Self> {
        let rest = match cfg.catalog {
            IcebergCatalogKind::Rest | IcebergCatalogKind::Polaris => {
                if cfg.rest_uri.is_empty() {
                    return Err(MeltError::config(format!(
                        "[backend.iceberg].rest_uri is required when catalog = \"{:?}\"",
                        cfg.catalog
                    )));
                }
                let warehouse_default = derive_default_db(&cfg.warehouse);
                let client = RestClient::new(cfg.rest_uri.clone(), warehouse_default);
                // Polaris-specific auth shape: callers pass a service
                // principal token via `MELT_POLARIS_TOKEN`; we fall
                // back to the generic `MELT_ICEBERG_TOKEN`. Without a
                // token, `RestClient` issues unauthenticated calls
                // which Polaris will 401 — fine for catalog discovery
                // probes during dev.
                let configured = matches!(cfg.catalog, IcebergCatalogKind::Polaris)
                    .then(|| std::env::var("MELT_POLARIS_TOKEN").ok())
                    .flatten();
                Some(client.with_token_override(configured))
            }
            // Glue ships via the AWS SDK; Hive is documented stub.
            IcebergCatalogKind::Glue | IcebergCatalogKind::Hive => None,
        };
        let glue = match cfg.catalog {
            IcebergCatalogKind::Glue => {
                Some(GlueClient::new(derive_default_db(&cfg.warehouse)).await?)
            }
            _ => None,
        };
        Ok(Self {
            cfg: cfg.clone(),
            state: RwLock::new(CatalogState::default()),
            rest,
            glue,
        })
    }

    pub async fn ping(&self) -> Result<()> {
        if let Some(rest) = &self.rest {
            // List namespaces is a cheap liveness probe.
            return rest.list_namespaces().await.map(|_| ());
        }
        if let Some(glue) = &self.glue {
            return glue.list_databases().await.map(|_| ());
        }
        Ok(())
    }

    pub async fn list_tables(&self) -> Result<Vec<TableRef>> {
        if let Some(rest) = &self.rest {
            // Refresh in-memory mirror; on errors return cached snapshot.
            let mut discovered: Vec<TableRef> = Vec::new();
            let mut estimates: HashMap<TableRef, u64> = HashMap::new();
            let namespaces = match rest.list_namespaces().await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(error = %e, "iceberg list_namespaces failed; serving cached set");
                    return Ok(self.state.read().tables.iter().cloned().collect());
                }
            };
            for ns in namespaces {
                let tables = match rest.list_tables(&ns).await {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::warn!(error = %e, namespace = ?ns, "iceberg list_tables failed");
                        continue;
                    }
                };
                for ident in tables {
                    let tref = rest.to_table_ref(&ident);
                    discovered.push(tref.clone());
                    if let Ok(meta) = rest.load_table(&ident).await {
                        if let Some(sz) = meta.total_files_size {
                            estimates.insert(tref.clone(), sz);
                        }
                    }
                }
            }
            let mut s = self.state.write();
            s.tables = discovered.iter().cloned().collect();
            for (k, v) in estimates {
                s.estimates.insert(k, v);
            }
            Ok(discovered)
        } else if let Some(glue) = &self.glue {
            let entries = match glue.list_all().await {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(error = %e, "glue list_all failed; serving cached set");
                    return Ok(self.state.read().tables.iter().cloned().collect());
                }
            };
            let mut s = self.state.write();
            s.tables = entries.iter().map(|e| e.r#ref.clone()).collect();
            for e in &entries {
                s.estimates.insert(e.r#ref.clone(), e.bytes);
            }
            Ok(entries.into_iter().map(|e| e.r#ref).collect())
        } else {
            Ok(self.state.read().tables.iter().cloned().collect())
        }
    }

    pub async fn tables_exist(&self, t: &[TableRef]) -> Result<Vec<bool>> {
        let s = self.state.read();
        Ok(t.iter().map(|x| s.tables.contains(x)).collect())
    }

    pub async fn estimate_scan_bytes(&self, t: &[TableRef]) -> Result<u64> {
        let s = self.state.read();
        Ok(t.iter().map(|x| *s.estimates.get(x).unwrap_or(&0)).sum())
    }

    pub async fn policy_markers(&self, t: &[TableRef]) -> Result<Vec<Option<String>>> {
        let s = self.state.read();
        Ok(t.iter().map(|x| s.markers.get(x).cloned()).collect())
    }

    pub fn write_policy_markers(&self, p: &[ProtectedTable]) -> Result<()> {
        let mut s = self.state.write();
        for marker in p {
            s.markers
                .insert(marker.table.clone(), marker.policy_name.clone());
            s.tables.insert(marker.table.clone());
        }
        Ok(())
    }

    pub fn retain_policy_markers(&self, keep: &[TableRef]) -> Result<()> {
        let mut s = self.state.write();
        let keep_set: HashSet<TableRef> = keep.iter().cloned().collect();
        s.markers.retain(|k, _| keep_set.contains(k));
        Ok(())
    }

    pub fn record_table(&self, table: TableRef, bytes: u64) {
        let mut s = self.state.write();
        s.tables.insert(table.clone());
        s.estimates.insert(table, bytes);
    }

    pub fn config(&self) -> &IcebergConfig {
        &self.cfg
    }

    /// Helper used by sync to confirm the chosen catalog flavour is
    /// supported in this build.
    pub fn assert_supported(&self) -> Result<()> {
        match self.cfg.catalog {
            IcebergCatalogKind::Rest | IcebergCatalogKind::Polaris => Ok(()),
            IcebergCatalogKind::Glue => Ok(()), // routed through `glue` module
            IcebergCatalogKind::Hive => Err(MeltError::config(
                "Iceberg Hive catalog is not implemented natively; \
                 stand up the Hive REST shim (`hive-metastore-rest`) and \
                 set `catalog = \"rest\"` with `rest_uri` pointing at it.",
            )),
        }
    }
}

/// Derive a synthetic "database" name from the warehouse URI. Used as
/// the first segment of `db.schema.name` refs when the REST catalog's
/// namespaces are single-level.
fn derive_default_db(warehouse: &str) -> String {
    warehouse
        .trim_end_matches('/')
        .rsplit('/')
        .find(|seg| !seg.is_empty())
        .unwrap_or("iceberg")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_derived_from_warehouse_uri() {
        assert_eq!(derive_default_db("s3://acme/iceberg/"), "iceberg");
        assert_eq!(derive_default_db("s3://acme/lake"), "lake");
        assert_eq!(derive_default_db(""), "iceberg");
    }
}
