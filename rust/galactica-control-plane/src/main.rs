use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use galactica_artifact::LocalModelRegistry;
use galactica_common::proto::common;
use galactica_control_plane::{
    ControlPlaneConfig, ControlPlaneServer, ControlPlaneService, GrpcNodeExecutor,
    SqliteStateStore, TenantRecord,
};
use galactica_networking::{
    detect_bind_endpoints, preferred_endpoint, spawn_control_plane_mdns_advertiser,
};
use galactica_observability::init_tracing;
use serde::Deserialize;
use tonic::transport::Server;
use tracing::{info, warn};

/// Galactica Control Plane — cluster state, scheduling, placement, artifact service
#[derive(Parser, Debug)]
#[command(name = "galactica-control-plane", version, about)]
struct Cli {
    /// Listen address for the gRPC server
    #[arg(long)]
    listen_addr: Option<String>,

    /// Path to the SQLite database file used for durable control-plane state
    #[arg(long)]
    state_db_path: Option<PathBuf>,

    /// Root directory containing local model manifests
    #[arg(long)]
    model_registry_root: Option<PathBuf>,

    /// Shared secret used to sign enrollment tokens
    #[arg(long)]
    enrollment_secret: Option<String>,

    /// Node heartbeat interval advertised to agents
    #[arg(long)]
    heartbeat_interval_seconds: Option<u32>,

    /// Bootstrap tenant ID seeded on startup for local development
    #[arg(long)]
    tenant_id: Option<String>,

    /// Bootstrap tenant API key seeded on startup for local development
    #[arg(long)]
    api_key: Option<String>,

    /// Allowlisted model IDs for the bootstrap tenant. Repeat to add multiple values.
    #[arg(long = "allowed-model")]
    allowed_models: Vec<String>,

    /// Allowlisted node pools for the bootstrap tenant. Repeat to add multiple values.
    #[arg(long = "allowed-node-pool")]
    allowed_node_pools: Vec<String>,

    /// Whether the bootstrap tenant requires mTLS for tenant traffic
    #[arg(long)]
    require_mtls: Option<bool>,

    /// Print a freshly minted worker enrollment token for the bootstrap tenant on startup
    #[arg(long)]
    print_enrollment_token: bool,

    /// TTL used when minting the bootstrap worker enrollment token
    #[arg(long)]
    enrollment_token_ttl_seconds: Option<u64>,

    /// Mint a bootstrap enrollment token and exit without serving
    #[arg(long)]
    mint_enrollment_token_only: bool,

    /// Path to configuration file
    #[arg(long, short)]
    config: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    listen_addr: Option<String>,
    state_db_path: Option<PathBuf>,
    model_registry_root: Option<PathBuf>,
    enrollment_secret: Option<String>,
    heartbeat_interval_seconds: Option<u32>,
    tenant_id: Option<String>,
    api_key: Option<String>,
    #[serde(default)]
    allowed_models: Vec<String>,
    #[serde(default)]
    allowed_node_pools: Vec<String>,
    require_mtls: Option<bool>,
    print_enrollment_token: Option<bool>,
    enrollment_token_ttl_seconds: Option<u64>,
    mint_enrollment_token_only: Option<bool>,
}

#[derive(Debug)]
struct EffectiveConfig {
    listen_addr: String,
    state_db_path: PathBuf,
    model_registry_root: PathBuf,
    enrollment_secret: String,
    heartbeat_interval_seconds: u32,
    tenant_id: String,
    api_key: String,
    allowed_models: Vec<String>,
    allowed_node_pools: Vec<String>,
    require_mtls: bool,
    print_enrollment_token: bool,
    enrollment_token_ttl_seconds: u64,
    mint_enrollment_token_only: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let file_config = load_config(cli.config.as_deref())?;
    let config = EffectiveConfig::from_sources(cli, file_config);

    let _tracing = if config.mint_enrollment_token_only {
        None
    } else {
        Some(init_tracing("galactica-control-plane"))
    };

    if let Some(parent) = config.state_db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create control-plane state directory {}",
                parent.display()
            )
        })?;
    }
    fs::create_dir_all(&config.model_registry_root).with_context(|| {
        format!(
            "failed to create model registry root {}",
            config.model_registry_root.display()
        )
    })?;

    let store = Arc::new(
        SqliteStateStore::new(&config.state_db_path)
            .await
            .with_context(|| {
                format!(
                    "failed to initialize sqlite state store {}",
                    config.state_db_path.display()
                )
            })?,
    );
    let service = ControlPlaneService::with_config(
        store,
        Arc::new(LocalModelRegistry::new(&config.model_registry_root)),
        Arc::new(GrpcNodeExecutor),
        ControlPlaneConfig {
            enrollment_secret: config.enrollment_secret.clone(),
            heartbeat_interval_seconds: config.heartbeat_interval_seconds,
        },
    );
    service
        .seed_tenant(TenantRecord {
            tenant_id: config.tenant_id.clone(),
            api_key: config.api_key.clone(),
            scopes: vec!["inference:read".to_string(), "inference:write".to_string()],
            require_mtls: config.require_mtls,
            max_requests_per_minute: 240,
            allowed_models: config.allowed_models.clone(),
            allowed_node_pools: config.allowed_node_pools.clone(),
            expires_at: None,
        })
        .await
        .context("failed to seed bootstrap tenant")?;
    if config.mint_enrollment_token_only {
        let token = service
            .mint_enrollment_token(
                &config.tenant_id,
                vec!["worker".to_string()],
                Duration::from_secs(config.enrollment_token_ttl_seconds),
            )
            .await
            .context("failed to mint bootstrap enrollment token")?;
        println!("{token}");
        return Ok(());
    }
    if config.print_enrollment_token {
        let token = service
            .mint_enrollment_token(
                &config.tenant_id,
                vec!["worker".to_string()],
                Duration::from_secs(config.enrollment_token_ttl_seconds),
            )
            .await
            .context("failed to mint bootstrap enrollment token")?;
        println!("{token}");
    }

    let addr: SocketAddr = config
        .listen_addr
        .parse()
        .with_context(|| format!("invalid listen address {}", config.listen_addr))?;
    info!(
        listen_addr = %addr,
        state_db_path = %config.state_db_path.display(),
        model_registry_root = %config.model_registry_root.display(),
        tenant_id = %config.tenant_id,
        "starting control plane"
    );
    let _mdns_advertiser = if let Some(endpoint) = select_lan_advertised_endpoint(addr.port()) {
        match spawn_control_plane_mdns_advertiser(control_plane_hostname(), endpoint.clone()).await
        {
            Ok(handle) => {
                info!(control_plane_endpoint = %endpoint, "started LAN discovery advertiser");
                Some(handle)
            }
            Err(error) => {
                warn!(error = %error, "failed to start LAN discovery advertiser");
                None
            }
        }
    } else {
        info!(
            "skipping LAN discovery advertiser because no non-loopback control-plane endpoint was detected"
        );
        None
    };
    Server::builder()
        .add_service(ControlPlaneServer::new(service))
        .serve(addr)
        .await
        .context("control plane server failed")?;
    Ok(())
}

impl EffectiveConfig {
    fn from_sources(cli: Cli, file: FileConfig) -> Self {
        Self {
            listen_addr: cli
                .listen_addr
                .or(file.listen_addr)
                .unwrap_or_else(|| "0.0.0.0:9090".to_string()),
            state_db_path: cli
                .state_db_path
                .or(file.state_db_path)
                .unwrap_or_else(|| PathBuf::from("./var/galactica/control-plane.db")),
            model_registry_root: cli
                .model_registry_root
                .or(file.model_registry_root)
                .unwrap_or_else(|| PathBuf::from("./models")),
            enrollment_secret: cli
                .enrollment_secret
                .or(file.enrollment_secret)
                .unwrap_or_else(|| "galactica-dev-secret".to_string()),
            heartbeat_interval_seconds: cli
                .heartbeat_interval_seconds
                .or(file.heartbeat_interval_seconds)
                .unwrap_or(15),
            tenant_id: cli
                .tenant_id
                .or(file.tenant_id)
                .unwrap_or_else(|| "tenant-dev".to_string()),
            api_key: cli
                .api_key
                .or(file.api_key)
                .unwrap_or_else(|| "galactica-dev-key".to_string()),
            allowed_models: if cli.allowed_models.is_empty() {
                file.allowed_models
            } else {
                cli.allowed_models
            },
            allowed_node_pools: if cli.allowed_node_pools.is_empty() {
                file.allowed_node_pools
            } else {
                cli.allowed_node_pools
            },
            require_mtls: cli.require_mtls.or(file.require_mtls).unwrap_or(false),
            print_enrollment_token: if cli.print_enrollment_token {
                true
            } else {
                file.print_enrollment_token.unwrap_or(false)
            },
            enrollment_token_ttl_seconds: cli
                .enrollment_token_ttl_seconds
                .or(file.enrollment_token_ttl_seconds)
                .unwrap_or(3600),
            mint_enrollment_token_only: if cli.mint_enrollment_token_only {
                true
            } else {
                file.mint_enrollment_token_only.unwrap_or(false)
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

fn select_lan_advertised_endpoint(port: u16) -> Option<String> {
    let endpoints = detect_bind_endpoints(port);
    endpoints
        .iter()
        .find(|endpoint| endpoint.kind == common::v1::EndpointKind::Lan as i32)
        .or_else(|| {
            endpoints.iter().find(|endpoint| {
                endpoint.kind == common::v1::EndpointKind::Wan as i32
                    || endpoint.kind == common::v1::EndpointKind::Tailscale as i32
            })
        })
        .map(|endpoint| endpoint.url.clone())
        .or_else(|| preferred_endpoint(&endpoints))
        .filter(|endpoint| !is_loopback_endpoint(endpoint))
}

fn is_loopback_endpoint(endpoint: &str) -> bool {
    endpoint.contains("127.0.0.1") || endpoint.contains("[::1]") || endpoint.contains("localhost")
}

fn control_plane_hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::env::var("COMPUTERNAME")
                .ok()
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| "control-plane".to_string())
}
