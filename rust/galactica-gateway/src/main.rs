use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use galactica_gateway::{GatewayConfig, GrpcControlPlaneClient, build_router_with_config};
use galactica_observability::init_tracing;
use serde::Deserialize;
use tokio::net::TcpListener;
use tracing::info;

/// Galactica Gateway — OpenAI-compatible HTTP API + gRPC server
#[derive(Parser, Debug)]
#[command(name = "galactica-gateway", version, about)]
struct Cli {
    /// Listen address for the HTTP server
    #[arg(long)]
    listen_addr: Option<String>,

    /// Control plane address to connect to
    #[arg(long)]
    control_plane_addr: Option<String>,

    /// Global rate limit applied before tenant-specific limits
    #[arg(long)]
    global_requests_per_minute: Option<u32>,

    /// Default per-tenant rate limit if the auth policy does not specify one
    #[arg(long)]
    default_tenant_requests_per_minute: Option<u32>,

    /// Maximum number of turns tracked per session
    #[arg(long)]
    session_history_limit: Option<usize>,

    /// Maximum number of audit rows returned by the admin API
    #[arg(long)]
    audit_limit: Option<u32>,

    /// Deployment mode label surfaced in health/settings responses
    #[arg(long)]
    deployment_mode: Option<String>,

    /// Path to configuration file
    #[arg(long, short)]
    config: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    listen_addr: Option<String>,
    control_plane_addr: Option<String>,
    global_requests_per_minute: Option<u32>,
    default_tenant_requests_per_minute: Option<u32>,
    session_history_limit: Option<usize>,
    audit_limit: Option<u32>,
    deployment_mode: Option<String>,
}

#[derive(Debug)]
struct EffectiveConfig {
    listen_addr: String,
    control_plane_addr: String,
    gateway: GatewayConfig,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let file_config = load_config(cli.config.as_deref())?;
    let config = EffectiveConfig::from_sources(cli, file_config);

    let _tracing = init_tracing("galactica-gateway");

    let addr: SocketAddr = config
        .listen_addr
        .parse()
        .with_context(|| format!("invalid listen address {}", config.listen_addr))?;
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind gateway listener {addr}"))?;
    let local_addr = listener
        .local_addr()
        .context("failed to resolve gateway listener address")?;
    let router = build_router_with_config(
        Arc::new(GrpcControlPlaneClient::new(
            config.control_plane_addr.clone(),
        )),
        config.gateway.clone(),
    );
    info!(
        listen_addr = %local_addr,
        control_plane_addr = %config.control_plane_addr,
        deployment_mode = %config.gateway.deployment_mode,
        "starting gateway"
    );
    axum::serve(listener, router)
        .await
        .context("gateway server failed")?;
    Ok(())
}

impl EffectiveConfig {
    fn from_sources(cli: Cli, file: FileConfig) -> Self {
        Self {
            listen_addr: cli
                .listen_addr
                .or(file.listen_addr)
                .unwrap_or_else(|| "0.0.0.0:8080".to_string()),
            control_plane_addr: cli
                .control_plane_addr
                .or(file.control_plane_addr)
                .unwrap_or_else(|| "http://127.0.0.1:9090".to_string()),
            gateway: GatewayConfig {
                global_requests_per_minute: cli
                    .global_requests_per_minute
                    .or(file.global_requests_per_minute)
                    .unwrap_or(600),
                default_tenant_requests_per_minute: cli
                    .default_tenant_requests_per_minute
                    .or(file.default_tenant_requests_per_minute)
                    .unwrap_or(120),
                session_history_limit: cli
                    .session_history_limit
                    .or(file.session_history_limit)
                    .unwrap_or(64),
                audit_limit: cli.audit_limit.or(file.audit_limit).unwrap_or(100),
                deployment_mode: cli
                    .deployment_mode
                    .or(file.deployment_mode)
                    .unwrap_or_else(|| "hybrid".to_string()),
            },
        }
    }
}

fn load_config(path: Option<&Path>) -> Result<FileConfig> {
    let Some(path) = path else {
        return Ok(FileConfig::default());
    };
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    toml::from_str(&contents)
        .with_context(|| format!("failed to parse config file {}", path.display()))
}
