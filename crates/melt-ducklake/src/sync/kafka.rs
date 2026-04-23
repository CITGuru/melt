//! Kafka-driven CDC ingestion.
//!
//! Some Snowflake → Lake setups don't go through Snowflake `STREAM`
//! objects directly — they go via Snowflake's Kafka Connector, or via
//! Debezium against the upstream OLTP source, or via a Snowpipe
//! Streaming Java sidecar that publishes change events to Kafka. In
//! all those cases, the wire format is JSON-shaped CDC envelopes
//! flowing through one Kafka topic per table.
//!
//! `KafkaCdcConsumer` reads from a configured topic, decodes a small
//! envelope shape (`{action, after, before}`), and feeds the result
//! into the same `apply::write_changes` machinery that
//! `read_stream_since` uses.
//!
//! Enabled with the `kafka` cargo feature. Brings in the `rdkafka`
//! crate (which builds librdkafka inline via `cmake-build`).

use std::sync::Arc;
use std::time::Duration;

use melt_core::{MeltError, Result, RouterCache, TableRef};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::Deserialize;

use crate::catalog::CatalogClient;
use crate::pool::DuckLakePool;

#[derive(Clone, Debug, Deserialize)]
pub struct KafkaCdcConfig {
    pub brokers: String,
    pub group_id: String,
    /// One topic per table, mapped onto `<db>.<schema>.<name>`.
    pub topics: Vec<TopicMapping>,
    #[serde(with = "humantime_serde", default = "KafkaCdcConfig::default_poll")]
    pub poll_interval: Duration,
}

impl KafkaCdcConfig {
    fn default_poll() -> Duration {
        Duration::from_millis(500)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct TopicMapping {
    pub topic: String,
    pub table: TableRef,
}

#[derive(Debug, Deserialize)]
struct CdcEnvelope {
    /// One of `INSERT`, `UPDATE`, `DELETE`.
    action: String,
    /// New row state on INSERT / UPDATE.
    #[serde(default)]
    after: Option<serde_json::Value>,
    /// Previous row state on UPDATE / DELETE.
    #[serde(default)]
    before: Option<serde_json::Value>,
}

pub struct KafkaCdcConsumer {
    pub catalog: Arc<CatalogClient>,
    pub pool: Arc<DuckLakePool>,
    pub router_cache: Arc<dyn RouterCache>,
    pub cfg: KafkaCdcConfig,
}

impl KafkaCdcConsumer {
    pub fn new(
        catalog: Arc<CatalogClient>,
        pool: Arc<DuckLakePool>,
        router_cache: Arc<dyn RouterCache>,
        cfg: KafkaCdcConfig,
    ) -> Self {
        Self {
            catalog,
            pool,
            router_cache,
            cfg,
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", &self.cfg.group_id)
            .set("bootstrap.servers", &self.cfg.brokers)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|e| MeltError::backend(format!("kafka client: {e}")))?;

        let topic_names: Vec<&str> = self.cfg.topics.iter().map(|t| t.topic.as_str()).collect();
        consumer
            .subscribe(&topic_names)
            .map_err(|e| MeltError::backend(format!("kafka subscribe: {e}")))?;
        tracing::info!(?topic_names, "kafka CDC consumer subscribed");

        loop {
            match tokio::time::timeout(self.cfg.poll_interval, consumer.recv()).await {
                Ok(Ok(msg)) => {
                    if let Err(e) = self.handle(&msg).await {
                        tracing::warn!(error = %e, "kafka CDC handler error");
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "kafka recv error");
                }
                Err(_) => {}
            }
        }
    }

    async fn handle(&self, msg: &impl Message) -> Result<()> {
        let topic = msg.topic();
        let Some(mapping) = self.cfg.topics.iter().find(|m| m.topic == topic) else {
            return Ok(());
        };
        let payload = match msg.payload() {
            Some(b) => b,
            None => return Ok(()),
        };
        let env: CdcEnvelope = match serde_json::from_slice(payload) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(error = %e, topic = %topic, "kafka CDC: bad envelope");
                return Ok(());
            }
        };

        // Translate envelope → single SQL stmt against lake.
        // Caller-managed table; schema evolution is a follow-up.
        let qualified = format!("\"{}\".\"{}\"", mapping.table.schema, mapping.table.name);
        let sql = match env.action.to_ascii_uppercase().as_str() {
            "INSERT" => insert_sql(&qualified, env.after.as_ref()),
            "UPDATE" => update_sql(&qualified, env.after.as_ref(), env.before.as_ref()),
            "DELETE" => delete_sql(&qualified, env.before.as_ref()),
            other => {
                tracing::warn!(action = %other, "kafka CDC: unknown action");
                return Ok(());
            }
        };
        let Some(sql) = sql else {
            return Ok(());
        };

        let writer = self.pool.write().await;
        writer
            .execute_batch(&sql)
            .map_err(|e| MeltError::backend(format!("kafka apply: {e}")))?;
        self.router_cache.invalidate_table(&mapping.table).await;
        Ok(())
    }
}

fn insert_sql(qualified: &str, after: Option<&serde_json::Value>) -> Option<String> {
    let obj = after?.as_object()?;
    let cols: Vec<String> = obj.keys().map(|k| format!("\"{k}\"")).collect();
    let vals: Vec<String> = obj.values().map(json_to_sql).collect();
    Some(format!(
        "INSERT INTO {qualified} ({}) VALUES ({})",
        cols.join(", "),
        vals.join(", ")
    ))
}

fn delete_sql(qualified: &str, before: Option<&serde_json::Value>) -> Option<String> {
    let obj = before?.as_object()?;
    let id = obj.get("id")?;
    Some(format!(
        "DELETE FROM {qualified} WHERE id = {}",
        json_to_sql(id)
    ))
}

fn update_sql(
    qualified: &str,
    after: Option<&serde_json::Value>,
    before: Option<&serde_json::Value>,
) -> Option<String> {
    // Delete-then-insert keyed on `id`. Same shape as
    // `apply::write_changes` UPDATE handling.
    let after = after?.as_object()?;
    let id = before
        .and_then(|b| b.get("id"))
        .or_else(|| after.get("id"))?;
    let cols: Vec<String> = after.keys().map(|k| format!("\"{k}\"")).collect();
    let vals: Vec<String> = after.values().map(json_to_sql).collect();
    Some(format!(
        "DELETE FROM {qualified} WHERE id = {}; \
         INSERT INTO {qualified} ({}) VALUES ({})",
        json_to_sql(id),
        cols.join(", "),
        vals.join(", ")
    ))
}

fn json_to_sql(v: &serde_json::Value) -> String {
    use serde_json::Value::*;
    match v {
        Null => "NULL".into(),
        Bool(b) => b.to_string(),
        Number(n) => n.to_string(),
        String(s) => format!("'{}'", s.replace('\'', "''")),
        Array(_) | Object(_) => {
            let s = serde_json::to_string(v).unwrap_or_default();
            format!("'{}'", s.replace('\'', "''"))
        }
    }
}
