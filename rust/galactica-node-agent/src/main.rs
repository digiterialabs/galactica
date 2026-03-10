use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use futures::stream::{self, StreamExt};
use galactica_artifact::runtime_preference_score;
use galactica_common::proto::{common, control};
use galactica_networking::{
    detect_bind_endpoints, detect_lan_scan_control_plane_endpoints,
    detect_tailscale_peer_control_plane_endpoints, discover_mdns_control_plane_endpoints,
    endpoint_from_url, fingerprint_certificate, preferred_endpoint,
};
use galactica_node_agent::{
    DefaultHardwareDetector, LlamaCppBackend, LlamaCppBackendConfig, MlxBackend, MlxBackendConfig,
    NodeAgentServer, NodeAgentService, OnnxBackend, RuntimeBackend, VllmBackend,
};
use galactica_observability::{init_tracing, inject_trace_context_into_tonic_request};
use reqwest::Url;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tracing::{Instrument, info, info_span, warn};

/// Galactica Node Agent — per-machine hardware detection, process supervision, runtime management
#[derive(Parser, Debug)]
#[command(name = "galactica-node-agent", version, about)]
struct Cli {
    /// Listen address for the node agent gRPC server
    #[arg(long)]
    listen_addr: Option<String>,

    /// Control plane address to connect to
    #[arg(long)]
    control_plane_addr: Option<String>,

    /// Enrollment token minted by the control plane. If omitted, the agent runs standalone.
    #[arg(long)]
    enrollment_token: Option<String>,

    /// Override the detected hostname presented to the control plane
    #[arg(long)]
    hostname: Option<String>,

    /// Public agent endpoint advertised to the control plane
    #[arg(long)]
    agent_endpoint: Option<String>,

    /// Version string reported during registration
    #[arg(long = "node-version")]
    node_version: Option<String>,

    /// Runtime backend to use: auto, llama.cpp, mlx, vllm, or onnxruntime
    #[arg(long)]
    runtime_backend: Option<String>,

    /// Root directory containing local model manifests
    #[arg(long)]
    models_root: Option<PathBuf>,

    /// Default heartbeat interval used until the control plane replies with one
    #[arg(long)]
    heartbeat_interval_seconds: Option<u64>,

    /// Path to configuration file
    #[arg(long, short)]
    config: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    listen_addr: Option<String>,
    control_plane_addr: Option<String>,
    enrollment_token: Option<String>,
    hostname: Option<String>,
    agent_endpoint: Option<String>,
    version: Option<String>,
    runtime_backend: Option<String>,
    models_root: Option<PathBuf>,
    heartbeat_interval_seconds: Option<u64>,
}

#[derive(Debug)]
struct EffectiveConfig {
    listen_addr: String,
    control_plane_addr: String,
    enrollment_token: Option<String>,
    hostname: Option<String>,
    agent_endpoint: Option<String>,
    version: String,
    runtime_backend: String,
    models_root: PathBuf,
    heartbeat_interval_seconds: u64,
}

#[derive(Debug, Clone)]
struct EnrolledIdentity {
    node_id: String,
    fingerprint: String,
}

#[derive(Debug, Clone)]
struct RegistrationConfig {
    control_plane_addr: String,
    hostname: String,
    agent_endpoint: String,
    agent_endpoints: Vec<common::v1::NetworkEndpoint>,
    version: String,
    capabilities: common::v1::NodeCapabilities,
    system_memory: common::v1::Memory,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let file_config = load_config(cli.config.as_deref())?;
    let config = EffectiveConfig::from_sources(cli, file_config);

    let _tracing = init_tracing("galactica-node-agent");

    let mut detector = DefaultHardwareDetector::default();
    let snapshot = detector.detect_snapshot();
    let hostname = config.hostname.clone().unwrap_or(snapshot.hostname.clone());
    let capabilities = snapshot.capabilities.clone();
    let system_memory = snapshot.system_memory;
    let backend = select_runtime_backend(&config, &capabilities)?;
    let backend_name = backend.get_capabilities().await?.runtime_name;

    let service = NodeAgentService::new(backend, capabilities.clone(), system_memory);

    let addr: SocketAddr = config
        .listen_addr
        .parse()
        .with_context(|| format!("invalid listen address {}", config.listen_addr))?;
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind node agent listener {addr}"))?;
    let local_addr = listener
        .local_addr()
        .context("failed to resolve node agent listener address")?;
    let agent_endpoints = merge_agent_endpoints(local_addr, config.agent_endpoint.clone());
    let agent_endpoint =
        preferred_endpoint(&agent_endpoints).unwrap_or_else(|| format!("http://{local_addr}"));

    if let Some(enrollment_token) = config.enrollment_token.clone() {
        let configured_control_plane_addr = config.control_plane_addr.clone();
        let registration_hostname = hostname.clone();
        let registration_capabilities = capabilities.clone();
        let registration_memory = system_memory;
        let version = config.version.clone();
        let advertised_endpoint = agent_endpoint.clone();
        let advertised_endpoints = agent_endpoints.clone();
        tokio::spawn(async move {
            let mut enrolled: Option<EnrolledIdentity> = None;

            loop {
                let control_plane_addr =
                    match resolve_control_plane_addr(&configured_control_plane_addr).await {
                        Some(addr) => addr,
                        None => {
                            warn!(
                                configured_control_plane_addr = %configured_control_plane_addr,
                                "failed to discover a reachable control plane; retrying in 5s"
                            );
                            sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                    };

                if enrolled.is_none() {
                    match enroll_node(
                        &control_plane_addr,
                        &enrollment_token,
                        &registration_hostname,
                        registration_capabilities.clone(),
                    )
                    .await
                    {
                        Ok(identity) => {
                            enrolled = Some(identity);
                        }
                        Err(error) => {
                            warn!(
                                error = %error,
                                control_plane_addr = %control_plane_addr,
                                "node enrollment failed; retrying in 5s"
                            );
                            sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                    }
                }

                if let Some(identity) = enrolled.clone()
                    && let Err(error) = maintain_registration(
                        RegistrationConfig {
                            control_plane_addr: control_plane_addr.clone(),
                            hostname: registration_hostname.clone(),
                            agent_endpoint: advertised_endpoint.clone(),
                            agent_endpoints: advertised_endpoints.clone(),
                            version: version.clone(),
                            capabilities: registration_capabilities.clone(),
                            system_memory: registration_memory,
                        },
                        identity,
                    )
                    .await
                {
                    warn!(
                        error = %error,
                        control_plane_addr = %control_plane_addr,
                        "node registration loop exited; re-discovering control plane in 5s"
                    );
                    sleep(Duration::from_secs(5)).await;
                }
            }
        });
    } else {
        info!("starting node agent without control-plane enrollment token");
    }

    info!(
        listen_addr = %local_addr,
        control_plane_addr = %config.control_plane_addr,
        hostname = %hostname,
        agent_endpoint = %agent_endpoint,
        agent_endpoint_count = agent_endpoints.len(),
        runtime_backend = %backend_name,
        models_root = %config.models_root.display(),
        default_heartbeat_interval_seconds = config.heartbeat_interval_seconds,
        "starting node agent"
    );
    Server::builder()
        .add_service(NodeAgentServer::new(service))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .context("node agent server failed")?;
    Ok(())
}

impl EffectiveConfig {
    fn from_sources(cli: Cli, file: FileConfig) -> Self {
        Self {
            listen_addr: cli
                .listen_addr
                .or(file.listen_addr)
                .unwrap_or_else(|| "0.0.0.0:50061".to_string()),
            control_plane_addr: cli
                .control_plane_addr
                .or(file.control_plane_addr)
                .unwrap_or_else(|| "http://127.0.0.1:9090".to_string()),
            enrollment_token: cli.enrollment_token.or(file.enrollment_token),
            hostname: cli.hostname.or(file.hostname),
            agent_endpoint: cli.agent_endpoint.or(file.agent_endpoint),
            version: cli
                .node_version
                .or(file.version)
                .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()),
            runtime_backend: cli
                .runtime_backend
                .or(file.runtime_backend)
                .unwrap_or_else(|| "auto".to_string()),
            models_root: cli
                .models_root
                .or(file.models_root)
                .unwrap_or_else(|| PathBuf::from("./models")),
            heartbeat_interval_seconds: cli
                .heartbeat_interval_seconds
                .or(file.heartbeat_interval_seconds)
                .unwrap_or(15),
        }
    }
}

fn merge_agent_endpoints(
    local_addr: SocketAddr,
    configured_agent_endpoint: Option<String>,
) -> Vec<common::v1::NetworkEndpoint> {
    let mut endpoints = BTreeMap::new();

    for endpoint in detect_bind_endpoints(local_addr.port()) {
        endpoints.insert(endpoint.url.clone(), endpoint);
    }

    if let Some(agent_endpoint) = configured_agent_endpoint.filter(|value| !value.is_empty()) {
        endpoints.insert(
            agent_endpoint.clone(),
            endpoint_from_url(agent_endpoint, common::v1::EndpointKind::Unspecified, 0),
        );
    }

    if endpoints.is_empty() {
        let endpoint = endpoint_from_url(
            format!("http://{local_addr}"),
            common::v1::EndpointKind::Loopback,
            100,
        );
        endpoints.insert(endpoint.url.clone(), endpoint);
    }

    endpoints.into_values().collect()
}

async fn resolve_control_plane_addr(configured_control_plane_addr: &str) -> Option<String> {
    let port = control_plane_port(configured_control_plane_addr).unwrap_or(9090);
    let mut candidates = Vec::new();

    if !configured_control_plane_addr.is_empty() {
        candidates.push(configured_control_plane_addr.to_string());
    }

    if is_loopback_control_plane_addr(configured_control_plane_addr) {
        candidates.extend(
            detect_tailscale_peer_control_plane_endpoints(port)
                .into_iter()
                .map(|endpoint| endpoint.url),
        );
        match discover_mdns_control_plane_endpoints(Duration::from_millis(750)).await {
            Ok(endpoints) => {
                candidates.extend(endpoints.into_iter().map(|endpoint| endpoint.url));
            }
            Err(error) => {
                warn!(error = %error, "LAN discovery query failed");
            }
        }
        candidates.extend(
            detect_lan_scan_control_plane_endpoints(port)
                .into_iter()
                .map(|endpoint| endpoint.url),
        );
    }

    first_verified_control_plane(dedup_urls(candidates)).await
}

fn control_plane_port(control_plane_addr: &str) -> Option<u16> {
    Url::parse(control_plane_addr)
        .ok()
        .and_then(|url| url.port_or_known_default())
}

fn is_loopback_control_plane_addr(control_plane_addr: &str) -> bool {
    let Ok(url) = Url::parse(control_plane_addr) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

fn dedup_urls(urls: Vec<String>) -> Vec<String> {
    let mut deduped = BTreeMap::new();
    for url in urls {
        if !url.is_empty() {
            deduped.entry(url.clone()).or_insert(url);
        }
    }
    deduped.into_values().collect()
}

async fn first_verified_control_plane(candidates: Vec<String>) -> Option<String> {
    if candidates.is_empty() {
        return None;
    }

    let concurrency = candidates.len().min(64);
    let mut probes = stream::iter(candidates.into_iter().map(|candidate| async move {
        if verify_control_plane_endpoint(&candidate).await {
            Some(candidate)
        } else {
            None
        }
    }))
    .buffer_unordered(concurrency);

    while let Some(result) = probes.next().await {
        if result.is_some() {
            return result;
        }
    }
    None
}

async fn verify_control_plane_endpoint(endpoint: &str) -> bool {
    let Ok(Ok(mut client)) = timeout(
        Duration::from_millis(400),
        control::v1::control_plane_client::ControlPlaneClient::connect(endpoint.to_string()),
    )
    .await
    else {
        return false;
    };

    let request = tonic::Request::new(control::v1::AuthenticateRequest {
        credential: Some(control::v1::authenticate_request::Credential::ApiKey(
            "__galactica_probe__".to_string(),
        )),
    });
    match timeout(Duration::from_millis(400), client.authenticate(request)).await {
        Ok(Ok(_)) => true,
        Ok(Err(status)) => control_plane_probe_status_is_valid(status.code()),
        Err(_) => false,
    }
}

fn control_plane_probe_status_is_valid(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
    )
}

async fn enroll_node(
    control_plane_addr: &str,
    enrollment_token: &str,
    hostname: &str,
    capabilities: common::v1::NodeCapabilities,
) -> Result<EnrolledIdentity> {
    async move {
        let mut client = control::v1::control_plane_client::ControlPlaneClient::connect(
            control_plane_addr.to_string(),
        )
        .await
        .with_context(|| format!("failed to connect to control plane {control_plane_addr}"))?;
        let mut request = tonic::Request::new(control::v1::EnrollNodeRequest {
            enrollment_token: enrollment_token.to_string(),
            hostname: hostname.to_string(),
            capabilities: Some(capabilities),
        });
        inject_trace_context_into_tonic_request(&mut request);
        let response = client
            .enroll_node(request)
            .await
            .context("control plane rejected enrollment")?
            .into_inner();
        let identity = response
            .identity
            .context("control plane did not return node identity material")?;
        let certificate_pem = identity.certificate_pem;
        let node_id = identity
            .node_id
            .map(|node_id| node_id.value)
            .context("control plane did not return node_id")?;
        let fingerprint = fingerprint_certificate(&certificate_pem);
        info!(node_id = %node_id, fingerprint = %fingerprint, "node enrolled");
        Ok(EnrolledIdentity {
            node_id,
            fingerprint,
        })
    }
    .instrument(info_span!(
        "node_agent.enroll_node",
        otel.kind = "client",
        control_plane_addr = %control_plane_addr,
        hostname = %hostname
    ))
    .await
}

async fn maintain_registration(
    config: RegistrationConfig,
    enrolled: EnrolledIdentity,
) -> Result<()> {
    loop {
        match register_node(
            &config.control_plane_addr,
            &config.hostname,
            &config.agent_endpoint,
            &config.agent_endpoints,
            &config.version,
            config.capabilities.clone(),
            &enrolled,
        )
        .await
        {
            Ok(()) => {
                info!(
                    node_id = %enrolled.node_id,
                    agent_endpoint = %config.agent_endpoint,
                    "node registered"
                );
            }
            Err(error) => {
                warn!(error = %error, "node registration failed; retrying in 5s");
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        }

        if let Err(error) = report_capabilities(
            &config.control_plane_addr,
            config.capabilities.clone(),
            &enrolled,
        )
        .await
        {
            warn!(error = %error, "capability report failed");
        }

        loop {
            let heartbeat_interval_seconds = match send_heartbeat(
                &config.control_plane_addr,
                enrolled.node_id.clone(),
                config.system_memory,
                &enrolled,
            )
            .await
            {
                Ok(next_interval) => next_interval.max(1),
                Err(error) => {
                    warn!(error = %error, "heartbeat failed; re-registering in 5s");
                    break;
                }
            };
            sleep(Duration::from_secs(heartbeat_interval_seconds)).await;
        }
        sleep(Duration::from_secs(5)).await;
    }
}

async fn register_node(
    control_plane_addr: &str,
    hostname: &str,
    agent_endpoint: &str,
    agent_endpoints: &[common::v1::NetworkEndpoint],
    version: &str,
    capabilities: common::v1::NodeCapabilities,
    enrolled: &EnrolledIdentity,
) -> Result<()> {
    async move {
        let mut client = control::v1::control_plane_client::ControlPlaneClient::connect(
            control_plane_addr.to_string(),
        )
        .await
        .with_context(|| format!("failed to connect to control plane {control_plane_addr}"))?;
        let mut request = tonic::Request::new(control::v1::RegisterNodeRequest {
            hostname: hostname.to_string(),
            capabilities: Some(capabilities),
            version: version.to_string(),
            agent_endpoint: agent_endpoint.to_string(),
            agent_endpoints: agent_endpoints.to_vec(),
        });
        inject_node_fingerprint(&mut request, &enrolled.fingerprint)?;
        inject_trace_context_into_tonic_request(&mut request);
        client
            .register_node(request)
            .await
            .context("control plane rejected node registration")?;
        Ok(())
    }
    .instrument(info_span!(
        "node_agent.register_node",
        otel.kind = "client",
        control_plane_addr = %control_plane_addr,
        node_id = %enrolled.node_id
    ))
    .await
}

async fn report_capabilities(
    control_plane_addr: &str,
    capabilities: common::v1::NodeCapabilities,
    enrolled: &EnrolledIdentity,
) -> Result<()> {
    async move {
        let mut client = control::v1::control_plane_client::ControlPlaneClient::connect(
            control_plane_addr.to_string(),
        )
        .await
        .with_context(|| format!("failed to connect to control plane {control_plane_addr}"))?;
        let mut request = tonic::Request::new(control::v1::ReportCapabilitiesRequest {
            node_id: Some(common::v1::NodeId {
                value: enrolled.node_id.clone(),
            }),
            capabilities: Some(capabilities),
        });
        inject_node_fingerprint(&mut request, &enrolled.fingerprint)?;
        inject_trace_context_into_tonic_request(&mut request);
        client
            .report_capabilities(request)
            .await
            .context("control plane rejected capability report")?;
        Ok(())
    }
    .instrument(info_span!(
        "node_agent.report_capabilities",
        otel.kind = "client",
        control_plane_addr = %control_plane_addr,
        node_id = %enrolled.node_id
    ))
    .await
}

async fn send_heartbeat(
    control_plane_addr: &str,
    node_id: String,
    system_memory: common::v1::Memory,
    enrolled: &EnrolledIdentity,
) -> Result<u64> {
    async move {
        let mut client = control::v1::control_plane_client::ControlPlaneClient::connect(
            control_plane_addr.to_string(),
        )
        .await
        .with_context(|| format!("failed to connect to control plane {control_plane_addr}"))?;
        let mut request = tonic::Request::new(control::v1::HeartbeatRequest {
            node_id: Some(common::v1::NodeId { value: node_id }),
            system_memory: Some(system_memory),
            accelerator_utilization: Vec::new(),
            running_instances: Vec::new(),
        });
        inject_node_fingerprint(&mut request, &enrolled.fingerprint)?;
        inject_trace_context_into_tonic_request(&mut request);
        let response = client
            .heartbeat(request)
            .await
            .context("control plane rejected heartbeat")?
            .into_inner();
        Ok(u64::from(response.heartbeat_interval_seconds.max(1)))
    }
    .instrument(info_span!(
        "node_agent.heartbeat",
        otel.kind = "client",
        control_plane_addr = %control_plane_addr,
        node_id = %enrolled.node_id
    ))
    .await
}

fn inject_node_fingerprint<T>(request: &mut tonic::Request<T>, fingerprint: &str) -> Result<()> {
    let value = MetadataValue::try_from(fingerprint)
        .with_context(|| format!("invalid node fingerprint metadata {fingerprint}"))?;
    request
        .metadata_mut()
        .insert("x-galactica-node-fingerprint", value);
    Ok(())
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

fn select_runtime_backend(
    config: &EffectiveConfig,
    capabilities: &common::v1::NodeCapabilities,
) -> Result<Arc<dyn RuntimeBackend>> {
    let preferred = config.runtime_backend.trim().to_ascii_lowercase();
    let detected_backends = &capabilities.runtime_backends;
    let backend = match preferred.as_str() {
        "auto" => {
            let selected = detected_backends
                .iter()
                .max_by_key(|backend| runtime_preference_score(backend, capabilities))
                .ok_or_else(|| {
                    anyhow::anyhow!("no supported runtime backend detected on this node")
                })?
                .as_str();
            match selected {
                "llama.cpp" | "llamacpp" => Arc::new(LlamaCppBackend::with_model_registry(
                    LlamaCppBackendConfig::default(),
                    &config.models_root,
                )) as Arc<dyn RuntimeBackend>,
                "mlx" => Arc::new(MlxBackend::with_model_registry(
                    mlx_backend_config(config),
                    &config.models_root,
                )) as Arc<dyn RuntimeBackend>,
                "vllm" => Arc::new(VllmBackend::new()) as Arc<dyn RuntimeBackend>,
                "onnxruntime" | "onnx" => Arc::new(OnnxBackend::new()) as Arc<dyn RuntimeBackend>,
                other => {
                    return Err(anyhow::anyhow!(
                        "no supported runtime backend detected on this node: {other}"
                    ));
                }
            }
        }
        "llama.cpp" | "llamacpp" => Arc::new(LlamaCppBackend::with_model_registry(
            LlamaCppBackendConfig::default(),
            &config.models_root,
        )) as Arc<dyn RuntimeBackend>,
        "mlx" => Arc::new(MlxBackend::with_model_registry(
            mlx_backend_config(config),
            &config.models_root,
        )) as Arc<dyn RuntimeBackend>,
        "vllm" => Arc::new(VllmBackend::new()) as Arc<dyn RuntimeBackend>,
        "onnxruntime" | "onnx" => Arc::new(OnnxBackend::new()) as Arc<dyn RuntimeBackend>,
        other => {
            return Err(anyhow::anyhow!(
                "unsupported runtime backend selection: {other}"
            ));
        }
    };
    Ok(backend)
}

fn mlx_backend_config(config: &EffectiveConfig) -> MlxBackendConfig {
    let repo_root = config
        .models_root
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let python_program = [
        repo_root.join(".venv-mlx314/bin/python3.14"),
        repo_root.join(".venv-mlx314/bin/python3"),
        repo_root.join(".venv-mlx/bin/python3"),
    ]
    .into_iter()
    .find(|candidate| candidate.exists())
    .map(|candidate| {
        if candidate.is_absolute() {
            candidate
        } else {
            std::env::current_dir()
                .map(|cwd| cwd.join(&candidate))
                .unwrap_or(candidate)
        }
        .display()
        .to_string()
    })
    .unwrap_or_else(|| "python3".to_string());
    MlxBackendConfig {
        python_program,
        ..MlxBackendConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use galactica_common::proto::common::v1::{
        AcceleratorInfo, AcceleratorType, CpuArch, Memory, NetworkProfile, NodeCapabilities, OsType,
    };

    use super::{EffectiveConfig, control_plane_probe_status_is_valid, select_runtime_backend};

    fn test_config() -> EffectiveConfig {
        EffectiveConfig {
            listen_addr: "127.0.0.1:50061".to_string(),
            control_plane_addr: "http://127.0.0.1:9090".to_string(),
            enrollment_token: None,
            hostname: None,
            agent_endpoint: None,
            version: "test".to_string(),
            runtime_backend: "auto".to_string(),
            models_root: PathBuf::from("./models"),
            heartbeat_interval_seconds: 15,
        }
    }

    #[tokio::test]
    async fn auto_runtime_prefers_mlx_on_apple_silicon() {
        let capabilities = NodeCapabilities {
            os: OsType::Macos as i32,
            cpu_arch: CpuArch::Arm64 as i32,
            accelerators: vec![
                AcceleratorInfo {
                    r#type: AcceleratorType::Metal as i32,
                    name: "Apple GPU".to_string(),
                    vram: Some(Memory {
                        total_bytes: 16,
                        available_bytes: 16,
                    }),
                    compute_capability: "metal3".to_string(),
                },
                AcceleratorInfo {
                    r#type: AcceleratorType::Cpu as i32,
                    name: "CPU".to_string(),
                    vram: None,
                    compute_capability: String::new(),
                },
            ],
            system_memory: Some(Memory {
                total_bytes: 32,
                available_bytes: 24,
            }),
            network_profile: NetworkProfile::Lan as i32,
            runtime_backends: vec!["llama.cpp".to_string(), "mlx".to_string()],
            locality: HashMap::new(),
        };

        let backend = select_runtime_backend(&test_config(), &capabilities).unwrap();
        let runtime = backend.get_capabilities().await.unwrap();
        assert_eq!(runtime.runtime_name, "mlx");
    }

    #[tokio::test]
    async fn auto_runtime_prefers_llama_cpp_on_windows_cuda() {
        let capabilities = NodeCapabilities {
            os: OsType::Windows as i32,
            cpu_arch: CpuArch::X8664 as i32,
            accelerators: vec![
                AcceleratorInfo {
                    r#type: AcceleratorType::Cuda as i32,
                    name: "RTX".to_string(),
                    vram: Some(Memory {
                        total_bytes: 16,
                        available_bytes: 16,
                    }),
                    compute_capability: "sm89".to_string(),
                },
                AcceleratorInfo {
                    r#type: AcceleratorType::Cpu as i32,
                    name: "CPU".to_string(),
                    vram: None,
                    compute_capability: String::new(),
                },
            ],
            system_memory: Some(Memory {
                total_bytes: 64,
                available_bytes: 48,
            }),
            network_profile: NetworkProfile::Lan as i32,
            runtime_backends: vec!["llama.cpp".to_string(), "onnxruntime".to_string()],
            locality: HashMap::new(),
        };

        let backend = select_runtime_backend(&test_config(), &capabilities).unwrap();
        let runtime = backend.get_capabilities().await.unwrap();
        assert_eq!(runtime.runtime_name, "llama.cpp");
    }

    #[tokio::test]
    async fn auto_runtime_prefers_llama_cpp_on_intel_macos() {
        let capabilities = NodeCapabilities {
            os: OsType::Macos as i32,
            cpu_arch: CpuArch::X8664 as i32,
            accelerators: vec![AcceleratorInfo {
                r#type: AcceleratorType::Cpu as i32,
                name: "Intel CPU".to_string(),
                vram: None,
                compute_capability: String::new(),
            }],
            system_memory: Some(Memory {
                total_bytes: 16,
                available_bytes: 12,
            }),
            network_profile: NetworkProfile::Lan as i32,
            runtime_backends: vec!["llama.cpp".to_string()],
            locality: HashMap::new(),
        };

        let backend = select_runtime_backend(&test_config(), &capabilities).unwrap();
        let runtime = backend.get_capabilities().await.unwrap();
        assert_eq!(runtime.runtime_name, "llama.cpp");
    }

    #[test]
    fn authenticate_probe_accepts_expected_control_plane_statuses() {
        assert!(control_plane_probe_status_is_valid(
            tonic::Code::Unauthenticated
        ));
        assert!(control_plane_probe_status_is_valid(
            tonic::Code::PermissionDenied
        ));
        assert!(!control_plane_probe_status_is_valid(
            tonic::Code::Unimplemented
        ));
        assert!(!control_plane_probe_status_is_valid(
            tonic::Code::Unavailable
        ));
    }
}
