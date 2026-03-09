use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize tracing with the given service name and log level.
///
/// Uses `RUST_LOG` env var if set, otherwise falls back to the provided level.
pub fn init_tracing(service_name: &str, default_level: &str) {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_level));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_ansi(true),
        )
        .init();

    tracing::info!(service = service_name, "tracing initialized");
}

/// Shutdown tracing (flush pending spans).
pub fn shutdown_tracing() {
    // tracing-subscriber doesn't need explicit shutdown in most cases,
    // but this provides a hook for future OpenTelemetry integration.
    tracing::info!("tracing shutdown");
}
