use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::{LogFormat, MetricsConfig};

static TRACING_INIT: std::sync::Once = std::sync::Once::new();

pub fn init_tracing(cfg: &MetricsConfig) {
    TRACING_INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(cfg.log_level.clone()));

        match cfg.log_format {
            LogFormat::Json => {
                let layer = tracing_subscriber::fmt::layer().json();
                let _ = tracing_subscriber::registry()
                    .with(filter)
                    .with(layer)
                    .try_init();
            }
            LogFormat::Pretty => {
                let layer = tracing_subscriber::fmt::layer().pretty();
                let _ = tracing_subscriber::registry()
                    .with(filter)
                    .with(layer)
                    .try_init();
            }
        }
    });
}
