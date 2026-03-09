use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize JSON-formatted logging for production use.
///
/// This is an alternative to `init_tracing` that outputs structured JSON logs.
pub fn init_json_logging(default_level: &str) {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_level));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer().json())
        .init();
}
