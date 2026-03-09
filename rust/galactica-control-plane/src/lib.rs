mod store;

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_stream::try_stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use galactica_artifact::{ModelRegistry, resolve_variant};
use galactica_common::inference::{
    InferenceChunk, InferenceRequest, InferenceUsage, NodeExecutionPayload, estimate_tokens,
};
use galactica_common::proto::{common, control, node};
use galactica_common::{GalacticaError, Result, chrono_to_timestamp, timestamp_to_chrono};
use galactica_networking::issue_tls_identity;
use galactica_observability::{
    inject_trace_context_into_tonic_request, set_parent_from_tonic_request,
};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock, broadcast};
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};
use tracing::{Instrument, info_span};

pub use control::v1::control_plane_server::{ControlPlane, ControlPlaneServer};
pub use store::{
    AuditRecord, AuthContext, ClusterState, ControlEvent, CredentialKind, DownloadRecord,
    EnrollmentTokenRecord, EventEnvelope, InMemoryStateStore, InstanceStatus, ModelInstanceRecord,
    NodeIdentityRecord, NodeRecord, SqliteStateStore, StateStore, TaskRecord, TenantRecord,
    event_apply,
};

type InferStream =
    Pin<Box<dyn Stream<Item = std::result::Result<control::v1::InferChunk, Status>> + Send>>;
type WatchEventsStream =
    Pin<Box<dyn Stream<Item = std::result::Result<common::v1::ClusterEvent, Status>> + Send>>;

const TENANT_ID_HEADER: &str = "x-galactica-tenant-id";
const SCOPES_HEADER: &str = "x-galactica-scopes";
const EXPIRES_AT_HEADER: &str = "x-galactica-expires-at";
const ACTOR_HEADER: &str = "x-galactica-actor";
const CREDENTIAL_KIND_HEADER: &str = "x-galactica-credential-kind";
const NODE_FINGERPRINT_HEADER: &str = "x-galactica-node-fingerprint";

fn rpc_server_span<T>(name: &'static str, request: &Request<T>) -> tracing::Span {
    let span = info_span!("rpc.server", rpc.method = %name, otel.kind = "server");
    set_parent_from_tonic_request(&span, request);
    span
}

#[derive(Debug, Clone, Default)]
pub struct TopologyGraph {
    edges: BTreeMap<String, BTreeSet<String>>,
}

impl TopologyGraph {
    pub fn add_node(&mut self, node_id: impl Into<String>) {
        self.edges.entry(node_id.into()).or_default();
    }

    pub fn remove_node(&mut self, node_id: &str) {
        self.edges.remove(node_id);
        for peers in self.edges.values_mut() {
            peers.remove(node_id);
        }
    }

    pub fn add_edge(&mut self, from: impl Into<String>, to: impl Into<String>) {
        self.edges.entry(from.into()).or_default().insert(to.into());
    }

    pub fn has_cycle(&self) -> bool {
        fn visit(
            node: &str,
            edges: &BTreeMap<String, BTreeSet<String>>,
            visiting: &mut BTreeSet<String>,
            visited: &mut BTreeSet<String>,
        ) -> bool {
            if visited.contains(node) {
                return false;
            }
            if !visiting.insert(node.to_string()) {
                return true;
            }
            if let Some(neighbors) = edges.get(node) {
                for neighbor in neighbors {
                    if visit(neighbor, edges, visiting, visited) {
                        return true;
                    }
                }
            }
            visiting.remove(node);
            visited.insert(node.to_string());
            false
        }

        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        self.edges
            .keys()
            .any(|node| visit(node, &self.edges, &mut visiting, &mut visited))
    }
}

pub fn pool_label(capabilities: &common::v1::NodeCapabilities) -> String {
    if let (Some(provider), Some(region)) = (
        capabilities.locality.get("cloud_provider"),
        capabilities.locality.get("region"),
    ) {
        return format!(
            "cloud-{provider}-{region}-{}",
            accelerator_label(capabilities)
        );
    }

    format!(
        "{}-{}-{}",
        os_label(capabilities.os),
        accelerator_label(capabilities),
        arch_label(capabilities.cpu_arch)
    )
}

pub fn compute_execution_pools(nodes: &[NodeRecord]) -> BTreeMap<String, Vec<NodeRecord>> {
    let mut pools = BTreeMap::new();
    for node in nodes
        .iter()
        .filter(|node| node.status == common::v1::NodeStatus::Online as i32)
    {
        pools
            .entry(pool_label(&node.capabilities))
            .or_insert_with(Vec::new)
            .push(node.clone());
    }
    pools
}

fn os_label(value: i32) -> &'static str {
    match common::v1::OsType::try_from(value).unwrap_or(common::v1::OsType::Unspecified) {
        common::v1::OsType::Macos => "macos",
        common::v1::OsType::Linux => "linux",
        common::v1::OsType::Windows => "windows",
        common::v1::OsType::Unspecified => "unknown",
    }
}

fn accelerator_label(capabilities: &common::v1::NodeCapabilities) -> &'static str {
    let accelerator = capabilities
        .accelerators
        .iter()
        .find(|accelerator| accelerator.r#type != common::v1::AcceleratorType::Cpu as i32)
        .or_else(|| capabilities.accelerators.first());
    match accelerator
        .map(|accelerator| {
            common::v1::AcceleratorType::try_from(accelerator.r#type).unwrap_or_default()
        })
        .unwrap_or(common::v1::AcceleratorType::Cpu)
    {
        common::v1::AcceleratorType::Metal => "metal",
        common::v1::AcceleratorType::Cuda => "cuda",
        common::v1::AcceleratorType::Rocm => "rocm",
        common::v1::AcceleratorType::Directml => "directml",
        common::v1::AcceleratorType::Cpu | common::v1::AcceleratorType::Unspecified => "cpu",
    }
}

fn arch_label(value: i32) -> &'static str {
    match common::v1::CpuArch::try_from(value).unwrap_or(common::v1::CpuArch::Unspecified) {
        common::v1::CpuArch::Arm64 => "arm64",
        common::v1::CpuArch::X8664 => "x86_64",
        common::v1::CpuArch::Unspecified => "unknown",
    }
}

fn is_remote_pool(capabilities: &common::v1::NodeCapabilities) -> bool {
    capabilities.locality.contains_key("cloud_provider")
        || matches!(
            common::v1::NetworkProfile::try_from(capabilities.network_profile)
                .unwrap_or(common::v1::NetworkProfile::Unspecified),
            common::v1::NetworkProfile::Wan | common::v1::NetworkProfile::Public
        )
}

#[derive(Default)]
pub struct Scheduler {
    cursors: Mutex<BTreeMap<String, usize>>,
}

#[derive(Debug, Clone)]
pub struct PlacementDecision {
    pub pool: String,
    pub node: NodeRecord,
    pub variant: common::v1::ModelVariant,
}

#[derive(Default)]
pub struct DefaultPlacementEngine {
    scheduler: Scheduler,
    pub topology: Arc<RwLock<TopologyGraph>>,
}

impl DefaultPlacementEngine {
    pub fn compatible_pools(
        &self,
        manifest: &common::v1::ModelManifest,
        nodes: &[NodeRecord],
        auth: &AuthContext,
    ) -> Vec<String> {
        let mut compatible = BTreeSet::new();
        for node in nodes {
            let pool = pool_label(&node.capabilities);
            if !auth.allowed_node_pools.is_empty() && !auth.allowed_node_pools.contains(&pool) {
                continue;
            }
            if resolve_variant(manifest, &node.capabilities).is_ok() {
                compatible.insert(pool);
            }
        }
        compatible.into_iter().collect()
    }

    pub async fn place_model(
        &self,
        manifest: &common::v1::ModelManifest,
        nodes: &[NodeRecord],
        auth: &AuthContext,
    ) -> Result<PlacementDecision> {
        let mut local_pools: BTreeMap<String, Vec<(NodeRecord, common::v1::ModelVariant)>> =
            BTreeMap::new();
        let mut remote_pools: BTreeMap<String, Vec<(NodeRecord, common::v1::ModelVariant)>> =
            BTreeMap::new();
        for node in nodes
            .iter()
            .filter(|node| node.status == common::v1::NodeStatus::Online as i32)
        {
            let pool = pool_label(&node.capabilities);
            if !auth.allowed_node_pools.is_empty() && !auth.allowed_node_pools.contains(&pool) {
                continue;
            }
            if let Ok(variant) = resolve_variant(manifest, &node.capabilities) {
                let entry = if is_remote_pool(&node.capabilities) {
                    &mut remote_pools
                } else {
                    &mut local_pools
                };
                entry.entry(pool).or_default().push((node.clone(), variant));
            }
        }

        if !local_pools.is_empty() {
            return self.scheduler.select_node(&local_pools).await;
        }
        self.scheduler.select_node(&remote_pools).await
    }
}

impl Scheduler {
    async fn select_node(
        &self,
        pools: &BTreeMap<String, Vec<(NodeRecord, common::v1::ModelVariant)>>,
    ) -> Result<PlacementDecision> {
        let selection_key = pools.keys().cloned().collect::<Vec<_>>().join("|");
        let entries = pools
            .iter()
            .flat_map(|(pool, entries)| {
                entries
                    .iter()
                    .cloned()
                    .map(|(node, variant)| (pool.clone(), node, variant))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if entries.is_empty() {
            return Err(GalacticaError::failed_precondition(
                "no execution pools available",
            ));
        }
        let mut cursors = self.cursors.lock().await;
        let cursor = cursors.entry(selection_key).or_insert(0);
        let selected = entries[*cursor % entries.len()].clone();
        *cursor = (*cursor + 1) % entries.len();
        Ok(PlacementDecision {
            pool: selected.0,
            node: selected.1,
            variant: selected.2,
        })
    }
}

#[derive(Clone)]
pub struct NodeRegistry {
    store: Arc<dyn StateStore>,
}

impl NodeRegistry {
    pub fn new<S>(store: Arc<S>) -> Self
    where
        S: StateStore + 'static,
    {
        Self { store }
    }

    pub async fn register(
        &self,
        hostname: String,
        agent_endpoint: String,
        capabilities: common::v1::NodeCapabilities,
        version: String,
    ) -> Result<NodeRecord> {
        self.register_with_id(
            format!("node-{}", uuid::Uuid::new_v4()),
            hostname,
            agent_endpoint,
            capabilities,
            version,
        )
        .await
    }

    pub async fn register_with_id(
        &self,
        node_id: String,
        hostname: String,
        agent_endpoint: String,
        capabilities: common::v1::NodeCapabilities,
        version: String,
    ) -> Result<NodeRecord> {
        let now = Utc::now();
        let node = NodeRecord {
            node_id,
            hostname,
            agent_endpoint,
            capabilities: capabilities.clone(),
            status: common::v1::NodeStatus::Online as i32,
            last_heartbeat: now,
            registered_at: now,
            version,
            system_memory: capabilities.system_memory,
        };
        self.store
            .apply_event(ControlEvent::NodeRegistered { node: node.clone() })
            .await?;
        Ok(node)
    }

    pub async fn heartbeat(
        &self,
        node_id: &str,
        system_memory: Option<common::v1::Memory>,
    ) -> Result<()> {
        if self.store.get_node(node_id).await?.is_none() {
            return Err(GalacticaError::not_found(format!(
                "unknown node: {node_id}"
            )));
        }
        self.store
            .apply_event(ControlEvent::NodeHeartbeat {
                node_id: node_id.to_string(),
                observed_at: Utc::now(),
                system_memory,
            })
            .await?;
        Ok(())
    }

    pub async fn check_timeouts(&self, timeout: Duration) -> Result<Vec<String>> {
        let now = Utc::now();
        let mut timed_out = Vec::new();
        for node in self.store.list_nodes().await? {
            let age = now
                .signed_duration_since(node.last_heartbeat)
                .to_std()
                .unwrap_or_default();
            if age > timeout && node.status != common::v1::NodeStatus::Offline as i32 {
                self.store
                    .apply_event(ControlEvent::NodeTimedOut {
                        node_id: node.node_id.clone(),
                    })
                    .await?;
                timed_out.push(node.node_id);
            }
        }
        Ok(timed_out)
    }

    pub async fn deregister(&self, node_id: &str, reason: impl Into<String>) -> Result<()> {
        if self.store.get_node(node_id).await?.is_none() {
            return Err(GalacticaError::not_found(format!(
                "unknown node: {node_id}"
            )));
        }
        self.store
            .apply_event(ControlEvent::NodeRemoved {
                node_id: node_id.to_string(),
                reason: reason.into(),
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
pub trait NodeExecutor: Send + Sync {
    async fn load_model(
        &self,
        endpoint: &str,
        request: node::v1::LoadModelRequest,
    ) -> Result<node::v1::LoadModelResponse>;
    async fn execute_task(
        &self,
        endpoint: &str,
        request: node::v1::ExecuteTaskRequest,
    ) -> Result<node::v1::ExecuteTaskResponse>;
}

#[derive(Clone, Default)]
pub struct GrpcNodeExecutor;

#[async_trait]
impl NodeExecutor for GrpcNodeExecutor {
    async fn load_model(
        &self,
        endpoint: &str,
        request: node::v1::LoadModelRequest,
    ) -> Result<node::v1::LoadModelResponse> {
        let mut client =
            node::v1::node_agent_client::NodeAgentClient::connect(endpoint.to_string())
                .await
                .map_err(|error| {
                    GalacticaError::unavailable(format!("failed to connect node agent: {error}"))
                })?;
        let mut request = tonic::Request::new(request);
        inject_trace_context_into_tonic_request(&mut request);
        client
            .load_model(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| GalacticaError::unavailable(format!("load_model failed: {error}")))
    }

    async fn execute_task(
        &self,
        endpoint: &str,
        request: node::v1::ExecuteTaskRequest,
    ) -> Result<node::v1::ExecuteTaskResponse> {
        let mut client =
            node::v1::node_agent_client::NodeAgentClient::connect(endpoint.to_string())
                .await
                .map_err(|error| {
                    GalacticaError::unavailable(format!("failed to connect node agent: {error}"))
                })?;
        let mut request = tonic::Request::new(request);
        inject_trace_context_into_tonic_request(&mut request);
        client
            .execute_task(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| GalacticaError::unavailable(format!("execute_task failed: {error}")))
    }
}

#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    pub enrollment_secret: String,
    pub heartbeat_interval_seconds: u32,
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self {
            enrollment_secret: "galactica-dev-secret".to_string(),
            heartbeat_interval_seconds: 15,
        }
    }
}

#[derive(Clone)]
pub struct ControlPlaneService<R, E> {
    store: Arc<dyn StateStore>,
    registry: NodeRegistry,
    artifacts: Arc<R>,
    placement: Arc<DefaultPlacementEngine>,
    executor: Arc<E>,
    config: ControlPlaneConfig,
}

impl<R, E> ControlPlaneService<R, E>
where
    R: ModelRegistry + Send + Sync + 'static,
    E: NodeExecutor + Send + Sync + 'static,
{
    pub fn new<S>(store: Arc<S>, artifacts: Arc<R>, executor: Arc<E>) -> Self
    where
        S: StateStore + 'static,
    {
        Self::with_config(store, artifacts, executor, ControlPlaneConfig::default())
    }

    pub fn with_config<S>(
        store: Arc<S>,
        artifacts: Arc<R>,
        executor: Arc<E>,
        config: ControlPlaneConfig,
    ) -> Self
    where
        S: StateStore + 'static,
    {
        let store: Arc<dyn StateStore> = store;
        Self {
            registry: NodeRegistry {
                store: store.clone(),
            },
            store,
            artifacts,
            placement: Arc::new(DefaultPlacementEngine::default()),
            executor,
            config,
        }
    }

    pub fn store(&self) -> Arc<dyn StateStore> {
        self.store.clone()
    }

    pub async fn seed_tenant(&self, tenant: TenantRecord) -> Result<()> {
        self.store.upsert_tenant(tenant).await
    }

    pub async fn list_audit_records(&self) -> Result<Vec<AuditRecord>> {
        self.store.list_audit_records().await
    }

    pub async fn mint_enrollment_token(
        &self,
        tenant_id: &str,
        allowed_roles: Vec<String>,
        ttl: Duration,
    ) -> Result<String> {
        if self.store.get_tenant(tenant_id).await?.is_none() {
            return Err(GalacticaError::not_found(format!(
                "unknown tenant: {tenant_id}"
            )));
        }
        let expires_at = Utc::now()
            + chrono::Duration::from_std(ttl)
                .map_err(|error| GalacticaError::internal(format!("invalid ttl: {error}")))?;
        let nonce = uuid::Uuid::new_v4().to_string();
        let roles = allowed_roles.join(",");
        let payload = format!("{tenant_id}:{}:{nonce}:{roles}", expires_at.timestamp());
        let signature = sha256_hex(&format!("{}:{payload}", self.config.enrollment_secret));
        let token = format!("{payload}:{signature}");
        self.store
            .save_enrollment_token(EnrollmentTokenRecord {
                token: token.clone(),
                tenant_id: tenant_id.to_string(),
                allowed_roles,
                expires_at,
                used_at: None,
            })
            .await?;
        Ok(token)
    }

    async fn resolve_tenant_context(
        &self,
        tenant_id: &str,
        scopes: &[String],
        expires_at: DateTime<Utc>,
        actor: String,
        credential_kind: CredentialKind,
    ) -> Result<AuthContext> {
        if expires_at <= Utc::now() {
            return Err(GalacticaError::unauthorized(
                "expired authentication context",
            ));
        }
        let tenant = self
            .store
            .get_tenant(tenant_id)
            .await?
            .ok_or_else(|| GalacticaError::unauthorized("unknown tenant"))?;
        if let Some(tenant_expiry) = tenant.expires_at
            && tenant_expiry <= Utc::now()
        {
            return Err(GalacticaError::unauthorized("tenant credentials expired"));
        }
        Ok(AuthContext {
            tenant_id: tenant.tenant_id,
            scopes: scopes.to_vec(),
            expires_at,
            require_mtls: tenant.require_mtls,
            max_requests_per_minute: tenant.max_requests_per_minute,
            allowed_models: tenant.allowed_models,
            allowed_node_pools: tenant.allowed_node_pools,
            actor,
            credential_kind,
        })
    }

    async fn auth_from_metadata<T>(&self, request: &Request<T>) -> Result<AuthContext> {
        let metadata = request.metadata();
        let tenant_id = metadata_str(metadata, TENANT_ID_HEADER)?;
        let scopes = metadata_str(metadata, SCOPES_HEADER)?
            .split(',')
            .filter(|scope| !scope.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        let expires_at = parse_rfc3339(&metadata_str(metadata, EXPIRES_AT_HEADER)?)?;
        let actor = metadata_str(metadata, ACTOR_HEADER)?;
        let credential_kind = match metadata_str(metadata, CREDENTIAL_KIND_HEADER)?.as_str() {
            "certificate" => CredentialKind::CertificateFingerprint,
            _ => CredentialKind::ApiKey,
        };
        self.resolve_tenant_context(&tenant_id, &scopes, expires_at, actor, credential_kind)
            .await
    }

    async fn require_node_identity<T>(&self, request: &Request<T>) -> Result<NodeIdentityRecord> {
        let fingerprint = metadata_str(request.metadata(), NODE_FINGERPRINT_HEADER)?;
        let identity = self
            .store
            .find_node_identity_by_fingerprint(&fingerprint)
            .await?
            .ok_or_else(|| GalacticaError::unauthorized("unknown node fingerprint"))?;
        if identity.expires_at <= Utc::now() {
            return Err(GalacticaError::unauthorized("node identity expired"));
        }
        Ok(identity)
    }

    async fn audit(
        &self,
        actor: impl Into<String>,
        action: impl Into<String>,
        resource: impl Into<String>,
        outcome: impl Into<String>,
        details: BTreeMap<String, String>,
    ) -> Result<()> {
        self.store
            .append_audit_record(AuditRecord {
                event_id: format!("audit-{}", uuid::Uuid::new_v4()),
                timestamp: Utc::now(),
                actor: actor.into(),
                action: action.into(),
                resource: resource.into(),
                outcome: outcome.into(),
                details,
            })
            .await
    }

    fn ensure_scope(auth: &AuthContext, scope: &str) -> Result<()> {
        if auth.scopes.iter().any(|candidate| candidate == scope) {
            return Ok(());
        }
        Err(GalacticaError::unauthorized(format!(
            "missing required scope: {scope}"
        )))
    }

    fn ensure_model_access(auth: &AuthContext, model_id: &str) -> Result<()> {
        if auth.allowed_models.is_empty()
            || auth
                .allowed_models
                .iter()
                .any(|allowed| allowed == model_id)
        {
            return Ok(());
        }
        Err(GalacticaError::unauthorized(format!(
            "tenant {} cannot access model {model_id}",
            auth.tenant_id
        )))
    }

    async fn authenticate_request(
        &self,
        api_key: Option<String>,
        certificate_fingerprint: Option<String>,
    ) -> Result<AuthContext> {
        if let Some(api_key) = api_key {
            let tenant = self
                .store
                .find_tenant_by_api_key(&api_key)
                .await?
                .ok_or_else(|| GalacticaError::unauthorized("invalid api key"))?;
            if let Some(expires_at) = tenant.expires_at
                && expires_at <= Utc::now()
            {
                return Err(GalacticaError::unauthorized("api key expired"));
            }
            let auth = AuthContext {
                actor: format!("tenant:{}", tenant.tenant_id),
                credential_kind: CredentialKind::ApiKey,
                expires_at: tenant
                    .expires_at
                    .unwrap_or_else(|| Utc::now() + chrono::Duration::hours(1)),
                tenant_id: tenant.tenant_id,
                scopes: tenant.scopes,
                require_mtls: tenant.require_mtls,
                max_requests_per_minute: tenant.max_requests_per_minute,
                allowed_models: tenant.allowed_models,
                allowed_node_pools: tenant.allowed_node_pools,
            };
            return Ok(auth);
        }

        if let Some(fingerprint) = certificate_fingerprint {
            let identity = self
                .store
                .find_node_identity_by_fingerprint(&fingerprint)
                .await?
                .ok_or_else(|| GalacticaError::unauthorized("invalid node certificate"))?;
            let tenant = self
                .store
                .get_tenant(&identity.tenant_id)
                .await?
                .ok_or_else(|| GalacticaError::unauthorized("node tenant missing"))?;
            return Ok(AuthContext {
                actor: format!("node:{}", identity.node_id),
                credential_kind: CredentialKind::CertificateFingerprint,
                expires_at: identity.expires_at,
                tenant_id: tenant.tenant_id,
                scopes: vec![
                    "node:register".to_string(),
                    "node:heartbeat".to_string(),
                    "node:report".to_string(),
                ],
                require_mtls: tenant.require_mtls,
                max_requests_per_minute: tenant.max_requests_per_minute,
                allowed_models: tenant.allowed_models,
                allowed_node_pools: tenant.allowed_node_pools,
            });
        }

        Err(GalacticaError::unauthorized("missing credentials"))
    }

    async fn infer_once(
        &self,
        request: InferenceRequest,
        auth: &AuthContext,
    ) -> Result<Vec<InferenceChunk>> {
        Self::ensure_scope(auth, "inference:write")?;
        Self::ensure_model_access(auth, &request.model)?;

        let mut audit_details = BTreeMap::new();
        audit_details.insert("model_id".to_string(), request.model.clone());
        audit_details.insert("request_id".to_string(), request.request_id.clone());
        self.audit(
            auth.actor.clone(),
            "inference.start",
            format!("model:{}", request.model),
            "attempt",
            audit_details.clone(),
        )
        .await?;

        let manifest = self.artifacts.get_model_manifest(&request.model).await?;
        let state = self.store.get_state().await?;
        let nodes = state.nodes.values().cloned().collect::<Vec<_>>();
        let placement = self.placement.place_model(&manifest, &nodes, auth).await?;

        let instance = if let Some(instance) = state.instances.values().find(|instance| {
            instance.node_id == placement.node.node_id
                && instance.model_id == request.model
                && instance.runtime == placement.variant.runtime
                && instance.status == InstanceStatus::Ready
        }) {
            instance.clone()
        } else {
            let loaded = self
                .executor
                .load_model(
                    &placement.node.agent_endpoint,
                    node::v1::LoadModelRequest {
                        model_id: Some(common::v1::ModelId {
                            value: request.model.clone(),
                        }),
                        variant_runtime: placement.variant.runtime.clone(),
                        variant_quantization: placement.variant.quantization.clone(),
                        max_memory_bytes: placement.variant.size_bytes,
                    },
                )
                .await?;
            let instance = ModelInstanceRecord {
                instance_id: loaded
                    .instance_id
                    .as_ref()
                    .map(|instance_id| instance_id.value.clone())
                    .ok_or_else(|| {
                        GalacticaError::internal("node agent did not return an instance_id")
                    })?,
                model_id: request.model.clone(),
                node_id: placement.node.node_id.clone(),
                runtime: placement.variant.runtime.clone(),
                quantization: placement.variant.quantization.clone(),
                memory_used_bytes: placement.variant.size_bytes,
                status: InstanceStatus::Loading,
                updated_at: Utc::now(),
            };
            self.store
                .apply_event(ControlEvent::InstanceCreated {
                    instance: instance.clone(),
                })
                .await?;
            self.store
                .apply_event(ControlEvent::InstanceStatusChanged {
                    instance_id: instance.instance_id.clone(),
                    status: InstanceStatus::Ready,
                })
                .await?;
            self.audit(
                "control-plane",
                "model.load",
                format!("instance:{}", instance.instance_id),
                "success",
                BTreeMap::from([
                    ("model_id".to_string(), instance.model_id.clone()),
                    ("node_id".to_string(), instance.node_id.clone()),
                ]),
            )
            .await?;
            instance
        };

        let prompt = request.prompt();
        let task_id = format!("task-{}", uuid::Uuid::new_v4());
        self.store
            .apply_event(ControlEvent::TaskCreated {
                task: TaskRecord {
                    task_id: task_id.clone(),
                    request_id: request.request_id.clone(),
                    model_id: request.model.clone(),
                    node_id: placement.node.node_id.clone(),
                    instance_id: instance.instance_id.clone(),
                    prompt: prompt.clone(),
                    status: common::v1::TaskStatus::Pending as i32,
                    result: None,
                    error_message: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
            })
            .await?;
        self.store
            .apply_event(ControlEvent::TaskStatusChanged {
                task_id: task_id.clone(),
                status: common::v1::TaskStatus::Running as i32,
                result: None,
                error_message: None,
            })
            .await?;

        let execution = self
            .executor
            .execute_task(
                &placement.node.agent_endpoint,
                node::v1::ExecuteTaskRequest {
                    task_id: Some(common::v1::TaskId {
                        value: task_id.clone(),
                    }),
                    instance_id: Some(common::v1::InstanceId {
                        value: instance.instance_id.clone(),
                    }),
                    payload: serde_json::to_vec(&NodeExecutionPayload {
                        task_id: task_id.clone(),
                        prompt,
                        params: request.params.clone(),
                    })
                    .map_err(|error| {
                        GalacticaError::internal(format!("failed to encode payload: {error}"))
                    })?,
                },
            )
            .await?;

        let output = String::from_utf8(execution.result.clone()).map_err(|error| {
            GalacticaError::internal(format!("node agent returned invalid utf8: {error}"))
        })?;
        let status = execution.status;
        let error_message =
            (!execution.error_message.is_empty()).then_some(execution.error_message);
        self.store
            .apply_event(ControlEvent::TaskStatusChanged {
                task_id: task_id.clone(),
                status,
                result: Some(output.clone()),
                error_message: error_message.clone(),
            })
            .await?;

        if status != common::v1::TaskStatus::Completed as i32 {
            self.audit(
                auth.actor.clone(),
                "inference.finish",
                format!("task:{task_id}"),
                "failed",
                BTreeMap::from([
                    ("request_id".to_string(), request.request_id.clone()),
                    (
                        "error".to_string(),
                        error_message
                            .clone()
                            .unwrap_or_else(|| "node execution failed".to_string()),
                    ),
                ]),
            )
            .await?;
            return Err(GalacticaError::unavailable(
                error_message.unwrap_or_else(|| "node execution failed".to_string()),
            ));
        }

        let prompt_tokens = request.prompt_token_estimate();
        let completion_tokens = estimate_tokens(&output);
        let usage = InferenceUsage {
            prompt_tokens,
            completion_tokens,
            total_tokens: prompt_tokens + completion_tokens,
        };
        let words = output.split_whitespace().collect::<Vec<_>>();
        let chunks = words
            .iter()
            .enumerate()
            .map(|(index, word)| InferenceChunk {
                request_id: request.request_id.clone(),
                model: request.model.clone(),
                choice_index: 0,
                delta_content: if index + 1 == words.len() {
                    (*word).to_string()
                } else {
                    format!("{word} ")
                },
                finish_reason: (index + 1 == words.len()).then_some("stop".to_string()),
                usage: (index + 1 == words.len()).then_some(usage.clone()),
            })
            .collect::<Vec<_>>();
        self.audit(
            auth.actor.clone(),
            "inference.finish",
            format!("task:{task_id}"),
            "success",
            BTreeMap::from([
                ("request_id".to_string(), request.request_id),
                ("model_id".to_string(), request.model),
                ("node_id".to_string(), placement.node.node_id),
                ("pool".to_string(), placement.pool),
            ]),
        )
        .await?;
        Ok(chunks)
    }
}

#[tonic::async_trait]
impl<R, E> ControlPlane for ControlPlaneService<R, E>
where
    R: ModelRegistry + Send + Sync + 'static,
    E: NodeExecutor + Send + Sync + 'static,
{
    type InferStream = InferStream;
    type WatchEventsStream = WatchEventsStream;

    async fn register_node(
        &self,
        request: Request<control::v1::RegisterNodeRequest>,
    ) -> std::result::Result<Response<control::v1::RegisterNodeResponse>, Status> {
        let span = rpc_server_span("control.register_node", &request);
        async move {
            let identity = self.require_node_identity(&request).await?;
            let auth = self
                .authenticate_request(None, Some(identity.fingerprint.clone()))
                .await?;
            Self::ensure_scope(&auth, "node:register")?;
            let request = request.into_inner();
            let capabilities = request
                .capabilities
                .ok_or_else(|| Status::invalid_argument("capabilities are required"))?;
            let node = self
                .registry
                .register_with_id(
                    identity.node_id.clone(),
                    request.hostname,
                    request.agent_endpoint,
                    capabilities,
                    request.version,
                )
                .await?;
            self.audit(
                auth.actor,
                "node.register",
                format!("node:{}", node.node_id),
                "success",
                BTreeMap::from([("hostname".to_string(), node.hostname.clone())]),
            )
            .await?;
            Ok(Response::new(control::v1::RegisterNodeResponse {
                node_id: Some(common::v1::NodeId {
                    value: node.node_id,
                }),
                registered_at: Some(chrono_to_timestamp(node.registered_at)),
            }))
        }
        .instrument(span)
        .await
    }

    async fn heartbeat(
        &self,
        request: Request<control::v1::HeartbeatRequest>,
    ) -> std::result::Result<Response<control::v1::HeartbeatResponse>, Status> {
        let span = rpc_server_span("control.heartbeat", &request);
        async move {
            let identity = self.require_node_identity(&request).await?;
            let auth = self
                .authenticate_request(None, Some(identity.fingerprint.clone()))
                .await?;
            Self::ensure_scope(&auth, "node:heartbeat")?;
            let request = request.into_inner();
            let node_id = request
                .node_id
                .as_ref()
                .map(|node_id| node_id.value.as_str())
                .ok_or_else(|| Status::invalid_argument("node_id is required"))?;
            if node_id != identity.node_id {
                return Err(GalacticaError::unauthorized(
                    "node fingerprint does not match node_id",
                )
                .into());
            }
            self.registry
                .heartbeat(node_id, request.system_memory)
                .await?;
            Ok(Response::new(control::v1::HeartbeatResponse {
                server_time: Some(chrono_to_timestamp(Utc::now())),
                heartbeat_interval_seconds: self.config.heartbeat_interval_seconds,
            }))
        }
        .instrument(span)
        .await
    }

    async fn report_capabilities(
        &self,
        request: Request<control::v1::ReportCapabilitiesRequest>,
    ) -> std::result::Result<Response<control::v1::ReportCapabilitiesResponse>, Status> {
        let span = rpc_server_span("control.report_capabilities", &request);
        async move {
            let identity = self.require_node_identity(&request).await?;
            let auth = self
                .authenticate_request(None, Some(identity.fingerprint.clone()))
                .await?;
            Self::ensure_scope(&auth, "node:report")?;
            let request = request.into_inner();
            let node_id = request
                .node_id
                .as_ref()
                .map(|node_id| node_id.value.as_str())
                .ok_or_else(|| Status::invalid_argument("node_id is required"))?;
            if node_id != identity.node_id {
                return Err(GalacticaError::unauthorized(
                    "node fingerprint does not match node_id",
                )
                .into());
            }
            let capabilities = request
                .capabilities
                .ok_or_else(|| Status::invalid_argument("capabilities are required"))?;
            self.store
                .update_node_capabilities(node_id, capabilities)
                .await?;
            Ok(Response::new(control::v1::ReportCapabilitiesResponse {}))
        }
        .instrument(span)
        .await
    }

    async fn get_cluster_state(
        &self,
        request: Request<control::v1::GetClusterStateRequest>,
    ) -> std::result::Result<Response<control::v1::GetClusterStateResponse>, Status> {
        let span = rpc_server_span("control.get_cluster_state", &request);
        async move {
            let state = self.store.get_state().await?;
            Ok(Response::new(control::v1::GetClusterStateResponse {
                nodes: state.nodes.values().map(NodeRecord::to_proto).collect(),
                loaded_models: state
                    .instances
                    .values()
                    .filter(|instance| instance.status == InstanceStatus::Ready)
                    .map(|instance| control::v1::LoadedModel {
                        instance_id: Some(common::v1::InstanceId {
                            value: instance.instance_id.clone(),
                        }),
                        model_id: Some(common::v1::ModelId {
                            value: instance.model_id.clone(),
                        }),
                        node_id: Some(common::v1::NodeId {
                            value: instance.node_id.clone(),
                        }),
                        runtime: instance.runtime.clone(),
                    })
                    .collect(),
            }))
        }
        .instrument(span)
        .await
    }

    async fn list_events(
        &self,
        request: Request<control::v1::ListEventsRequest>,
    ) -> std::result::Result<Response<control::v1::ListEventsResponse>, Status> {
        let span = rpc_server_span("control.list_events", &request);
        async move {
            let auth = self.auth_from_metadata(&request).await?;
            Self::ensure_scope(&auth, "inference:read")?;
            let request = request.into_inner();
            let events = self.store.get_events_since(request.since_sequence).await?;
            let last_sequence = self.store.get_state().await?.last_sequence;
            Ok(Response::new(control::v1::ListEventsResponse {
                events: events
                    .iter()
                    .map(|event| control::v1::SequencedClusterEvent {
                        sequence: event.sequence,
                        event: Some(event_to_proto(event)),
                    })
                    .collect(),
                last_sequence,
            }))
        }
        .instrument(span)
        .await
    }

    async fn list_audit_records(
        &self,
        request: Request<control::v1::ListAuditRecordsRequest>,
    ) -> std::result::Result<Response<control::v1::ListAuditRecordsResponse>, Status> {
        let span = rpc_server_span("control.list_audit_records", &request);
        async move {
            let auth = self.auth_from_metadata(&request).await?;
            Self::ensure_scope(&auth, "inference:read")?;
            let request = request.into_inner();
            let since = request.since.map(timestamp_to_chrono);
            let limit = if request.limit == 0 {
                usize::MAX
            } else {
                request.limit as usize
            };
            let records = ControlPlaneService::list_audit_records(self)
                .await?
                .into_iter()
                .filter(|record| since.map(|since| record.timestamp >= since).unwrap_or(true))
                .rev()
                .take(limit)
                .map(|record| control::v1::AuditRecord {
                    event_id: record.event_id,
                    timestamp: Some(chrono_to_timestamp(record.timestamp)),
                    actor: record.actor,
                    action: record.action,
                    resource: record.resource,
                    outcome: record.outcome,
                    details: record.details.into_iter().collect(),
                })
                .collect();
            Ok(Response::new(control::v1::ListAuditRecordsResponse {
                records,
            }))
        }
        .instrument(span)
        .await
    }

    async fn list_available_models(
        &self,
        request: Request<control::v1::ListAvailableModelsRequest>,
    ) -> std::result::Result<Response<control::v1::ListAvailableModelsResponse>, Status> {
        let span = rpc_server_span("control.list_available_models", &request);
        async move {
            let auth = self.auth_from_metadata(&request).await?;
            Self::ensure_scope(&auth, "inference:read")?;
            let request = request.into_inner();
            let models = self
                .artifacts
                .list_models(
                    (!request.filter_runtime.is_empty()).then_some(request.filter_runtime.as_str()),
                    None,
                )
                .await?
                .into_iter()
                .filter(|manifest| {
                    auth.allowed_models.is_empty()
                        || auth.allowed_models.iter().any(|allowed| {
                            manifest.model_id.as_ref().map(|model_id| &model_id.value)
                                == Some(allowed)
                        })
                })
                .collect();
            Ok(Response::new(control::v1::ListAvailableModelsResponse {
                models,
            }))
        }
        .instrument(span)
        .await
    }

    async fn infer(
        &self,
        request: Request<control::v1::InferRequest>,
    ) -> std::result::Result<Response<Self::InferStream>, Status> {
        let span = rpc_server_span("control.infer", &request);
        async move {
            let auth = self.auth_from_metadata(&request).await?;
            let chunks = self.infer_once(request.into_inner().into(), &auth).await?;
            let stream = try_stream! {
                for chunk in chunks {
                    yield control::v1::InferChunk::from(chunk);
                }
            };
            Ok(Response::new(Box::pin(stream) as Self::InferStream))
        }
        .instrument(span)
        .await
    }

    async fn watch_events(
        &self,
        request: Request<control::v1::WatchEventsRequest>,
    ) -> std::result::Result<Response<Self::WatchEventsStream>, Status> {
        let span = rpc_server_span("control.watch_events", &request);
        async move {
            let since = request.into_inner().since.map(timestamp_to_chrono);
            let historical = self.store.get_events_since(0).await?;
            let mut receiver = self.store.subscribe();
            let stream = try_stream! {
                for event in historical
                    .into_iter()
                    .filter(|event| since.map(|since| event.timestamp > since).unwrap_or(true))
                {
                    yield event_to_proto(&event);
                }
                loop {
                    match receiver.recv().await {
                        Ok(event) => {
                            if since.map(|since| event.timestamp > since).unwrap_or(true) {
                                yield event_to_proto(&event);
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    }
                }
            };
            Ok(Response::new(Box::pin(stream) as Self::WatchEventsStream))
        }
        .instrument(span)
        .await
    }

    async fn enroll_node(
        &self,
        request: Request<control::v1::EnrollNodeRequest>,
    ) -> std::result::Result<Response<control::v1::EnrollNodeResponse>, Status> {
        let span = rpc_server_span("control.enroll_node", &request);
        async move {
            let request = request.into_inner();
            let token_record = self
                .store
                .consume_enrollment_token(&request.enrollment_token)
                .await?
                .ok_or_else(|| Status::unauthenticated("invalid or expired enrollment token"))?;
            validate_signed_token(
                &self.config.enrollment_secret,
                &request.enrollment_token,
                &token_record.tenant_id,
                token_record.expires_at.timestamp(),
                &token_record.allowed_roles,
            )?;
            let node_id = format!("node-{}", uuid::Uuid::new_v4());
            let identity_material = issue_tls_identity(
                &node_id,
                &request.hostname,
                &self.config.enrollment_secret,
                chrono::Duration::days(30),
            )?;
            let identity = NodeIdentityRecord {
                node_id: node_id.clone(),
                tenant_id: token_record.tenant_id.clone(),
                certificate_pem: identity_material.certificate_pem.clone(),
                private_key_pem: identity_material.private_key_pem.clone(),
                fingerprint: identity_material.fingerprint.clone(),
                issued_at: identity_material.issued_at,
                expires_at: identity_material.expires_at,
                hostname: request.hostname.clone(),
            };
            self.store.save_node_identity(identity.clone()).await?;
            self.audit(
                format!("tenant:{}", token_record.tenant_id),
                "node.enroll",
                format!("node:{node_id}"),
                "success",
                BTreeMap::from([
                    ("hostname".to_string(), request.hostname),
                    (
                        "fingerprint".to_string(),
                        identity_material.fingerprint.clone(),
                    ),
                ]),
            )
            .await?;
            Ok(Response::new(control::v1::EnrollNodeResponse {
                identity: Some(control::v1::NodeIdentity {
                    node_id: Some(common::v1::NodeId { value: node_id }),
                    certificate_pem: identity_material.certificate_pem,
                    private_key_pem: identity_material.private_key_pem,
                    issued_at: Some(chrono_to_timestamp(identity_material.issued_at)),
                    expires_at: Some(chrono_to_timestamp(identity_material.expires_at)),
                }),
            }))
        }
        .instrument(span)
        .await
    }

    async fn authenticate(
        &self,
        request: Request<control::v1::AuthenticateRequest>,
    ) -> std::result::Result<Response<control::v1::AuthenticateResponse>, Status> {
        let span = rpc_server_span("control.authenticate", &request);
        async move {
            let request = request.into_inner();
            let auth = self
                .authenticate_request(
                    request
                        .credential
                        .as_ref()
                        .and_then(|credential| match credential {
                            control::v1::authenticate_request::Credential::ApiKey(value) => {
                                Some(value.clone())
                            }
                            _ => None,
                        }),
                    request
                        .credential
                        .as_ref()
                        .and_then(|credential| {
                            match credential {
                            control::v1::authenticate_request::Credential::CertificateFingerprint(
                                value,
                            ) => Some(value.clone()),
                            _ => None,
                        }
                        }),
                )
                .await?;
            self.audit(
                auth.actor.clone(),
                "auth.authenticate",
                format!("tenant:{}", auth.tenant_id),
                "success",
                BTreeMap::from([(
                    "credential_kind".to_string(),
                    credential_kind_label(&auth.credential_kind).to_string(),
                )]),
            )
            .await?;
            Ok(Response::new(control::v1::AuthenticateResponse {
                authenticated: true,
                tenant_id: Some(common::v1::TenantId {
                    value: auth.tenant_id.clone(),
                }),
                scopes: auth.scopes.clone(),
                expires_at: Some(chrono_to_timestamp(auth.expires_at)),
                policy: Some(control::v1::AuthPolicy {
                    tenant_id: Some(common::v1::TenantId {
                        value: auth.tenant_id,
                    }),
                    require_mtls: auth.require_mtls,
                    max_requests_per_minute: auth.max_requests_per_minute,
                    allowed_models: auth.allowed_models,
                    allowed_node_pools: auth.allowed_node_pools,
                }),
                actor: auth.actor,
                credential_kind: credential_kind_label(&auth.credential_kind).to_string(),
            }))
        }
        .instrument(span)
        .await
    }
}

fn event_to_proto(event: &EventEnvelope) -> common::v1::ClusterEvent {
    use common::v1::cluster_event::Event;
    let payload = match &event.event {
        ControlEvent::NodeRegistered { node } => Event::NodeJoined(common::v1::NodeJoined {
            node: Some(node.to_proto()),
        }),
        ControlEvent::NodeTimedOut { node_id } | ControlEvent::NodeRemoved { node_id, .. } => {
            Event::NodeLeft(common::v1::NodeLeft {
                node_id: Some(common::v1::NodeId {
                    value: node_id.clone(),
                }),
                reason: match &event.event {
                    ControlEvent::NodeTimedOut { .. } => "timeout".to_string(),
                    ControlEvent::NodeRemoved { reason, .. } => reason.clone(),
                    _ => String::new(),
                },
            })
        }
        ControlEvent::InstanceCreated { instance } => Event::ModelLoaded(common::v1::ModelLoaded {
            node_id: Some(common::v1::NodeId {
                value: instance.node_id.clone(),
            }),
            model_id: Some(common::v1::ModelId {
                value: instance.model_id.clone(),
            }),
            instance_id: Some(common::v1::InstanceId {
                value: instance.instance_id.clone(),
            }),
        }),
        ControlEvent::InstanceDeleted { instance_id } => {
            Event::ModelUnloaded(common::v1::ModelUnloaded {
                node_id: None,
                instance_id: Some(common::v1::InstanceId {
                    value: instance_id.clone(),
                }),
            })
        }
        ControlEvent::TaskStatusChanged {
            task_id, status, ..
        } => Event::TaskCompleted(common::v1::TaskCompleted {
            task_id: Some(common::v1::TaskId {
                value: task_id.clone(),
            }),
            status: *status,
        }),
        _ => Event::TaskCompleted(common::v1::TaskCompleted {
            task_id: Some(common::v1::TaskId {
                value: event.event_id.clone(),
            }),
            status: common::v1::TaskStatus::Pending as i32,
        }),
    };

    common::v1::ClusterEvent {
        event_id: event.event_id.clone(),
        timestamp: Some(chrono_to_timestamp(event.timestamp)),
        event: Some(payload),
    }
}

fn metadata_str(metadata: &tonic::metadata::MetadataMap, key: &str) -> Result<String> {
    metadata
        .get(key)
        .ok_or_else(|| GalacticaError::unauthorized(format!("missing metadata header: {key}")))?
        .to_str()
        .map(|value| value.to_string())
        .map_err(|error| GalacticaError::unauthorized(format!("invalid metadata {key}: {error}")))
}

fn parse_rfc3339(value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| {
            GalacticaError::unauthorized(format!("invalid timestamp {value}: {error}"))
        })
}

pub fn inject_auth_metadata<T>(request: &mut Request<T>, auth: &AuthContext) -> Result<()> {
    let metadata = request.metadata_mut();
    metadata.insert(TENANT_ID_HEADER, parse_metadata_value(&auth.tenant_id)?);
    metadata.insert(SCOPES_HEADER, parse_metadata_value(&auth.scopes.join(","))?);
    metadata.insert(
        EXPIRES_AT_HEADER,
        parse_metadata_value(&auth.expires_at.to_rfc3339())?,
    );
    metadata.insert(ACTOR_HEADER, parse_metadata_value(&auth.actor)?);
    metadata.insert(
        CREDENTIAL_KIND_HEADER,
        parse_metadata_value(credential_kind_label(&auth.credential_kind))?,
    );
    Ok(())
}

pub fn inject_node_fingerprint<T>(request: &mut Request<T>, fingerprint: &str) -> Result<()> {
    request
        .metadata_mut()
        .insert(NODE_FINGERPRINT_HEADER, parse_metadata_value(fingerprint)?);
    Ok(())
}

fn parse_metadata_value(value: &str) -> Result<MetadataValue<tonic::metadata::Ascii>> {
    MetadataValue::try_from(value).map_err(|error| {
        GalacticaError::invalid_argument(format!("invalid metadata value {value}: {error}"))
    })
}

pub fn sha256_hex(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn credential_kind_label(kind: &CredentialKind) -> &'static str {
    match kind {
        CredentialKind::ApiKey => "api_key",
        CredentialKind::CertificateFingerprint => "certificate",
    }
}

fn validate_signed_token(
    secret: &str,
    token: &str,
    tenant_id: &str,
    expires_at: i64,
    allowed_roles: &[String],
) -> Result<()> {
    let expected_payload = format!(
        "{tenant_id}:{expires_at}:{}:{}",
        token
            .split(':')
            .nth(2)
            .ok_or_else(|| GalacticaError::unauthorized("invalid enrollment token"))?,
        allowed_roles.join(",")
    );
    let expected_signature = sha256_hex(&format!("{secret}:{expected_payload}"));
    let actual_signature = token
        .rsplit(':')
        .next()
        .ok_or_else(|| GalacticaError::unauthorized("invalid enrollment token"))?;
    if expected_signature != actual_signature {
        return Err(GalacticaError::unauthorized(
            "invalid enrollment token signature",
        ));
    }
    if expires_at <= Utc::now().timestamp() {
        return Err(GalacticaError::unauthorized("enrollment token expired"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;

    use futures::StreamExt;
    use galactica_artifact::LocalModelRegistry;
    use galactica_common::proto::control::v1::control_plane_server::ControlPlane;
    use galactica_common::proto::gateway::v1::{ChatMessage, InferenceParams};
    use galactica_common::proto::node::v1::node_agent_server::NodeAgentServer;
    use galactica_node_agent::{
        MlxBackend, NodeAgentService, default_macos_capabilities, default_system_memory,
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;

    fn sample_tenant() -> TenantRecord {
        TenantRecord {
            tenant_id: "tenant-dev".to_string(),
            api_key: "galactica-dev-key".to_string(),
            scopes: vec!["inference:read".to_string(), "inference:write".to_string()],
            require_mtls: false,
            max_requests_per_minute: 120,
            allowed_models: vec!["mistral-small".to_string()],
            allowed_node_pools: vec!["macos-metal-arm64".to_string()],
            expires_at: None,
        }
    }

    fn sample_node(node_id: &str) -> NodeRecord {
        NodeRecord {
            node_id: node_id.to_string(),
            hostname: format!("{node_id}.local"),
            agent_endpoint: "http://127.0.0.1:50061".to_string(),
            capabilities: default_macos_capabilities(),
            status: common::v1::NodeStatus::Online as i32,
            last_heartbeat: Utc::now(),
            registered_at: Utc::now(),
            version: "0.1.0".to_string(),
            system_memory: Some(default_system_memory()),
        }
    }

    fn sample_windows_cuda_node(node_id: &str) -> NodeRecord {
        NodeRecord {
            node_id: node_id.to_string(),
            hostname: format!("{node_id}.local"),
            agent_endpoint: "http://127.0.0.1:50062".to_string(),
            capabilities: common::v1::NodeCapabilities {
                os: common::v1::OsType::Windows as i32,
                cpu_arch: common::v1::CpuArch::X8664 as i32,
                accelerators: vec![
                    common::v1::AcceleratorInfo {
                        r#type: common::v1::AcceleratorType::Cuda as i32,
                        name: "RTX".to_string(),
                        vram: Some(common::v1::Memory {
                            total_bytes: 12 * 1024 * 1024 * 1024,
                            available_bytes: 12 * 1024 * 1024 * 1024,
                        }),
                        compute_capability: "sm89".to_string(),
                    },
                    common::v1::AcceleratorInfo {
                        r#type: common::v1::AcceleratorType::Cpu as i32,
                        name: "CPU".to_string(),
                        vram: None,
                        compute_capability: String::new(),
                    },
                ],
                system_memory: Some(common::v1::Memory {
                    total_bytes: 32 * 1024 * 1024 * 1024,
                    available_bytes: 28 * 1024 * 1024 * 1024,
                }),
                network_profile: common::v1::NetworkProfile::Lan as i32,
                runtime_backends: vec!["llama.cpp".to_string(), "onnxruntime".to_string()],
                locality: HashMap::new(),
            },
            status: common::v1::NodeStatus::Online as i32,
            last_heartbeat: Utc::now(),
            registered_at: Utc::now(),
            version: "0.1.0".to_string(),
            system_memory: Some(common::v1::Memory {
                total_bytes: 32 * 1024 * 1024 * 1024,
                available_bytes: 28 * 1024 * 1024 * 1024,
            }),
        }
    }

    #[test]
    fn execution_pools_group_heterogeneous_nodes() {
        let pools = compute_execution_pools(&[
            sample_node("node-a"),
            NodeRecord {
                node_id: "node-b".to_string(),
                hostname: "node-b".to_string(),
                agent_endpoint: "http://127.0.0.1:50062".to_string(),
                capabilities: common::v1::NodeCapabilities {
                    os: common::v1::OsType::Linux as i32,
                    cpu_arch: common::v1::CpuArch::X8664 as i32,
                    accelerators: vec![common::v1::AcceleratorInfo {
                        r#type: common::v1::AcceleratorType::Cuda as i32,
                        name: "RTX".to_string(),
                        vram: Some(common::v1::Memory {
                            total_bytes: 24,
                            available_bytes: 24,
                        }),
                        compute_capability: "sm90".to_string(),
                    }],
                    system_memory: Some(default_system_memory()),
                    network_profile: common::v1::NetworkProfile::Lan as i32,
                    runtime_backends: vec!["vllm".to_string()],
                    locality: HashMap::new(),
                },
                status: common::v1::NodeStatus::Online as i32,
                last_heartbeat: Utc::now(),
                registered_at: Utc::now(),
                version: "0.1.0".to_string(),
                system_memory: Some(default_system_memory()),
            },
        ]);
        assert!(pools.contains_key("macos-metal-arm64"));
        assert!(pools.contains_key("linux-cuda-x86_64"));
    }

    #[tokio::test]
    async fn placement_engine_prefers_local_pools_over_remote() {
        let engine = DefaultPlacementEngine::default();
        let manifest = common::v1::ModelManifest {
            model_id: Some(common::v1::ModelId {
                value: "mistral-small".to_string(),
            }),
            name: "Mistral Small".to_string(),
            family: "chat".to_string(),
            variants: vec![common::v1::ModelVariant {
                runtime: "mlx".to_string(),
                quantization: "4bit".to_string(),
                format: "safetensors".to_string(),
                size_bytes: 1024,
                compatible_accelerators: vec![common::v1::AcceleratorType::Metal as i32],
            }],
            chat_template: "chatml".to_string(),
            metadata: HashMap::new(),
        };
        let local = sample_node("local-node");
        let mut remote = sample_node("remote-node");
        remote.capabilities.locality = HashMap::from([
            ("cloud_provider".to_string(), "aws".to_string()),
            ("region".to_string(), "us-east-1".to_string()),
        ]);
        remote.capabilities.network_profile = common::v1::NetworkProfile::Wan as i32;
        let auth = AuthContext {
            tenant_id: "tenant-dev".to_string(),
            scopes: vec!["inference:write".to_string()],
            expires_at: Utc::now() + chrono::Duration::minutes(5),
            require_mtls: false,
            max_requests_per_minute: 120,
            allowed_models: vec!["mistral-small".to_string()],
            allowed_node_pools: vec![
                "macos-metal-arm64".to_string(),
                "cloud-aws-us-east-1-metal".to_string(),
            ],
            actor: "tenant:tenant-dev".to_string(),
            credential_kind: CredentialKind::ApiKey,
        };
        let decision = engine
            .place_model(&manifest, &[remote, local.clone()], &auth)
            .await
            .unwrap();
        assert_eq!(decision.node.node_id, local.node_id);
    }

    #[tokio::test]
    async fn placement_engine_round_robins_across_local_macos_and_windows_pools() {
        let engine = DefaultPlacementEngine::default();
        let mac = sample_node("mac-node");
        let windows = sample_windows_cuda_node("windows-node");
        let manifest = common::v1::ModelManifest {
            model_id: Some(common::v1::ModelId {
                value: "qwen3.5-4b".to_string(),
            }),
            name: "Qwen3.5 4B".to_string(),
            family: "chat".to_string(),
            variants: vec![
                common::v1::ModelVariant {
                    runtime: "mlx".to_string(),
                    quantization: "4bit".to_string(),
                    format: "mlx".to_string(),
                    size_bytes: 2 * 1024 * 1024 * 1024,
                    compatible_accelerators: vec![common::v1::AcceleratorType::Metal as i32],
                },
                common::v1::ModelVariant {
                    runtime: "llama.cpp".to_string(),
                    quantization: "q4_k_m".to_string(),
                    format: "gguf".to_string(),
                    size_bytes: 2 * 1024 * 1024 * 1024,
                    compatible_accelerators: vec![
                        common::v1::AcceleratorType::Cpu as i32,
                        common::v1::AcceleratorType::Cuda as i32,
                    ],
                },
            ],
            chat_template: "chatml".to_string(),
            metadata: HashMap::new(),
        };
        let auth = AuthContext {
            tenant_id: "tenant-dev".to_string(),
            scopes: vec!["inference:write".to_string()],
            expires_at: Utc::now() + chrono::Duration::minutes(5),
            require_mtls: false,
            max_requests_per_minute: 120,
            allowed_models: vec!["qwen3.5-4b".to_string()],
            allowed_node_pools: vec![
                "macos-metal-arm64".to_string(),
                "windows-cuda-x86_64".to_string(),
            ],
            actor: "tenant:tenant-dev".to_string(),
            credential_kind: CredentialKind::ApiKey,
        };

        let first = engine
            .place_model(&manifest, &[mac.clone(), windows.clone()], &auth)
            .await
            .unwrap();
        let second = engine
            .place_model(&manifest, &[mac, windows], &auth)
            .await
            .unwrap();

        assert_ne!(first.node.node_id, second.node.node_id);
        assert!(matches!(
            (first.pool.as_str(), second.pool.as_str()),
            ("macos-metal-arm64", "windows-cuda-x86_64")
                | ("windows-cuda-x86_64", "macos-metal-arm64")
        ));
    }

    #[tokio::test]
    async fn registry_detects_timeout_after_stale_heartbeat_event() {
        let store = Arc::new(InMemoryStateStore::default());
        let registry = NodeRegistry::new(store.clone());
        let node = registry
            .register(
                "mac-mini".to_string(),
                "http://127.0.0.1:50061".to_string(),
                default_macos_capabilities(),
                "0.1.0".to_string(),
            )
            .await
            .unwrap();
        store
            .apply_event(ControlEvent::NodeHeartbeat {
                node_id: node.node_id.clone(),
                observed_at: Utc::now() - chrono::Duration::seconds(60),
                system_memory: Some(default_system_memory()),
            })
            .await
            .unwrap();
        let timed_out = registry
            .check_timeouts(Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(timed_out, vec![node.node_id.clone()]);
    }

    #[tokio::test]
    async fn control_plane_enrolls_registers_and_routes_inference() {
        let root =
            std::env::temp_dir().join(format!("galactica-control-plane-{}", uuid::Uuid::new_v4()));
        let manifest_dir = root.join("mistral-small");
        std::fs::create_dir_all(&manifest_dir).unwrap();
        std::fs::write(
            manifest_dir.join("manifest.json"),
            r#"{
  "model_id": "mistral-small",
  "name": "Mistral Small",
  "family": "chat",
  "variants": [
    {
      "runtime": "mlx",
      "quantization": "4bit",
      "format": "safetensors",
      "size_bytes": 1024,
      "compatible_accelerators": [2]
    }
  ],
  "chat_template": "chatml",
  "metadata": {}
}"#,
        )
        .unwrap();

        let backend = Arc::new(MlxBackend::new());
        let agent = NodeAgentService::new(
            backend,
            default_macos_capabilities(),
            default_system_memory(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let incoming = TcpListenerStream::new(listener);
        tokio::spawn(async move {
            Server::builder()
                .add_service(NodeAgentServer::new(agent))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        let store = Arc::new(InMemoryStateStore::default());
        let service = ControlPlaneService::new(
            store.clone(),
            Arc::new(LocalModelRegistry::new(&root)),
            Arc::new(GrpcNodeExecutor),
        );
        service.seed_tenant(sample_tenant()).await.unwrap();
        let enrollment_token = service
            .mint_enrollment_token(
                "tenant-dev",
                vec!["node".to_string()],
                Duration::from_secs(300),
            )
            .await
            .unwrap();
        let enrolled = service
            .enroll_node(Request::new(control::v1::EnrollNodeRequest {
                enrollment_token,
                hostname: "mac-mini".to_string(),
                capabilities: Some(default_macos_capabilities()),
            }))
            .await
            .unwrap()
            .into_inner();
        let identity = enrolled.identity.unwrap();
        let fingerprint = sha256_hex(&identity.certificate_pem);

        let mut register = Request::new(control::v1::RegisterNodeRequest {
            hostname: "mac-mini".to_string(),
            capabilities: Some(default_macos_capabilities()),
            version: "0.1.0".to_string(),
            agent_endpoint: format!("http://{addr}"),
        });
        inject_node_fingerprint(&mut register, &fingerprint).unwrap();
        service.register_node(register).await.unwrap();

        let auth = service
            .authenticate_request(Some("galactica-dev-key".to_string()), None)
            .await
            .unwrap();
        let mut infer = Request::new(control::v1::InferRequest {
            request_id: "req-1".to_string(),
            model: "mistral-small".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: "summarize node health".to_string(),
            }],
            params: Some(InferenceParams {
                temperature: 0.7,
                top_p: 1.0,
                max_tokens: 32,
                stop: Vec::new(),
                frequency_penalty: 0.0,
                presence_penalty: 0.0,
            }),
        });
        inject_auth_metadata(&mut infer, &auth).unwrap();
        let response = service.infer(infer).await.unwrap().into_inner();
        let chunks = response.collect::<Vec<_>>().await;
        assert!(!chunks.is_empty());
        assert!(chunks.last().unwrap().as_ref().unwrap().finish_reason == "stop");
        assert!(!service.list_audit_records().await.unwrap().is_empty());
        std::fs::remove_dir_all(root).unwrap();
    }
}
