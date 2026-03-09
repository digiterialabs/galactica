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
use tokio::sync::{Mutex, RwLock, broadcast};
use tonic::{Request, Response, Status};

pub use control::v1::control_plane_server::{ControlPlane, ControlPlaneServer};

type InferStream =
    Pin<Box<dyn Stream<Item = std::result::Result<control::v1::InferChunk, Status>> + Send>>;
type WatchEventsStream =
    Pin<Box<dyn Stream<Item = std::result::Result<common::v1::ClusterEvent, Status>> + Send>>;

#[derive(Debug, Clone, PartialEq)]
pub struct NodeRecord {
    pub node_id: String,
    pub hostname: String,
    pub agent_endpoint: String,
    pub capabilities: common::v1::NodeCapabilities,
    pub status: i32,
    pub last_heartbeat: DateTime<Utc>,
    pub registered_at: DateTime<Utc>,
    pub version: String,
    pub system_memory: Option<common::v1::Memory>,
}

impl NodeRecord {
    fn to_proto(&self) -> common::v1::NodeInfo {
        common::v1::NodeInfo {
            node_id: Some(common::v1::NodeId {
                value: self.node_id.clone(),
            }),
            hostname: self.hostname.clone(),
            capabilities: Some(self.capabilities.clone()),
            status: self.status,
            last_heartbeat: Some(chrono_to_timestamp(self.last_heartbeat)),
            version: self.version.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelInstanceRecord {
    pub instance_id: String,
    pub model_id: String,
    pub node_id: String,
    pub runtime: String,
    pub quantization: String,
    pub memory_used_bytes: u64,
    pub status: InstanceStatus,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskRecord {
    pub task_id: String,
    pub request_id: String,
    pub model_id: String,
    pub node_id: String,
    pub instance_id: String,
    pub prompt: String,
    pub status: i32,
    pub result: Option<String>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DownloadRecord {
    pub model_id: String,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClusterState {
    pub nodes: BTreeMap<String, NodeRecord>,
    pub instances: BTreeMap<String, ModelInstanceRecord>,
    pub tasks: BTreeMap<String, TaskRecord>,
    pub downloads: BTreeMap<String, DownloadRecord>,
    pub last_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceStatus {
    Loading,
    Ready,
    Running,
    Failed,
    Unloaded,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ControlEvent {
    NodeRegistered {
        node: NodeRecord,
    },
    NodeHeartbeat {
        node_id: String,
        observed_at: DateTime<Utc>,
        system_memory: Option<common::v1::Memory>,
    },
    NodeTimedOut {
        node_id: String,
    },
    NodeRemoved {
        node_id: String,
        reason: String,
    },
    InstanceCreated {
        instance: ModelInstanceRecord,
    },
    InstanceDeleted {
        instance_id: String,
    },
    InstanceStatusChanged {
        instance_id: String,
        status: InstanceStatus,
    },
    TaskCreated {
        task: TaskRecord,
    },
    TaskStatusChanged {
        task_id: String,
        status: i32,
        result: Option<String>,
        error_message: Option<String>,
    },
    DownloadProgress {
        download: DownloadRecord,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventEnvelope {
    pub sequence: u64,
    pub event_id: String,
    pub timestamp: DateTime<Utc>,
    pub event: ControlEvent,
}

pub fn event_apply(state: &ClusterState, event: &EventEnvelope) -> ClusterState {
    let mut next = state.clone();
    next.last_sequence = event.sequence;

    match &event.event {
        ControlEvent::NodeRegistered { node } => {
            next.nodes.insert(node.node_id.clone(), node.clone());
        }
        ControlEvent::NodeHeartbeat {
            node_id,
            observed_at,
            system_memory,
        } => {
            if let Some(node) = next.nodes.get_mut(node_id) {
                node.last_heartbeat = *observed_at;
                node.status = common::v1::NodeStatus::Online as i32;
                node.system_memory = system_memory.clone();
            }
        }
        ControlEvent::NodeTimedOut { node_id } => {
            if let Some(node) = next.nodes.get_mut(node_id) {
                node.status = common::v1::NodeStatus::Offline as i32;
            }
        }
        ControlEvent::NodeRemoved { node_id, .. } => {
            next.nodes.remove(node_id);
            next.instances
                .retain(|_, instance| instance.node_id != *node_id);
        }
        ControlEvent::InstanceCreated { instance } => {
            next.instances
                .insert(instance.instance_id.clone(), instance.clone());
        }
        ControlEvent::InstanceDeleted { instance_id } => {
            next.instances.remove(instance_id);
        }
        ControlEvent::InstanceStatusChanged {
            instance_id,
            status,
        } => {
            if let Some(instance) = next.instances.get_mut(instance_id) {
                instance.status = *status;
                instance.updated_at = event.timestamp;
            }
        }
        ControlEvent::TaskCreated { task } => {
            next.tasks.insert(task.task_id.clone(), task.clone());
        }
        ControlEvent::TaskStatusChanged {
            task_id,
            status,
            result,
            error_message,
        } => {
            if let Some(task) = next.tasks.get_mut(task_id) {
                task.status = *status;
                task.result = result.clone();
                task.error_message = error_message.clone();
                task.updated_at = event.timestamp;
            }
        }
        ControlEvent::DownloadProgress { download } => {
            next.downloads
                .insert(download.model_id.clone(), download.clone());
        }
    }

    next
}

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn get_state(&self) -> ClusterState;
    async fn apply_event(&self, event: ControlEvent) -> EventEnvelope;
    async fn get_events_since(&self, sequence: u64) -> Vec<EventEnvelope>;
    async fn get_node(&self, node_id: &str) -> Option<NodeRecord>;
    async fn list_nodes(&self) -> Vec<NodeRecord>;
}

#[derive(Clone)]
pub struct InMemoryStateStore {
    inner: Arc<RwLock<StoreInner>>,
    event_sender: broadcast::Sender<EventEnvelope>,
}

#[derive(Default)]
struct StoreInner {
    state: ClusterState,
    events: Vec<EventEnvelope>,
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        let (event_sender, _) = broadcast::channel(128);
        Self {
            inner: Arc::new(RwLock::new(StoreInner::default())),
            event_sender,
        }
    }
}

impl InMemoryStateStore {
    pub fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.event_sender.subscribe()
    }

    pub async fn update_node_capabilities(
        &self,
        node_id: &str,
        capabilities: common::v1::NodeCapabilities,
    ) -> Result<()> {
        let mut inner = self.inner.write().await;
        let node = inner
            .state
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| GalacticaError::not_found(format!("unknown node: {node_id}")))?;
        node.capabilities = capabilities;
        Ok(())
    }
}

#[async_trait]
impl StateStore for InMemoryStateStore {
    async fn get_state(&self) -> ClusterState {
        self.inner.read().await.state.clone()
    }

    async fn apply_event(&self, event: ControlEvent) -> EventEnvelope {
        let mut inner = self.inner.write().await;
        let envelope = EventEnvelope {
            sequence: inner.state.last_sequence + 1,
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            event,
        };
        inner.state = event_apply(&inner.state, &envelope);
        inner.events.push(envelope.clone());
        let _ = self.event_sender.send(envelope.clone());
        envelope
    }

    async fn get_events_since(&self, sequence: u64) -> Vec<EventEnvelope> {
        self.inner
            .read()
            .await
            .events
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect()
    }

    async fn get_node(&self, node_id: &str) -> Option<NodeRecord> {
        self.inner.read().await.state.nodes.get(node_id).cloned()
    }

    async fn list_nodes(&self) -> Vec<NodeRecord> {
        self.inner
            .read()
            .await
            .state
            .nodes
            .values()
            .cloned()
            .collect()
    }
}

#[derive(Clone)]
pub struct NodeRegistry {
    store: Arc<InMemoryStateStore>,
}

impl NodeRegistry {
    pub fn new(store: Arc<InMemoryStateStore>) -> Self {
        Self { store }
    }

    pub async fn register(
        &self,
        hostname: String,
        agent_endpoint: String,
        capabilities: common::v1::NodeCapabilities,
        version: String,
    ) -> Result<NodeRecord> {
        let now = Utc::now();
        let node = NodeRecord {
            node_id: format!("node-{}", uuid::Uuid::new_v4()),
            hostname,
            agent_endpoint,
            capabilities: capabilities.clone(),
            status: common::v1::NodeStatus::Online as i32,
            last_heartbeat: now,
            registered_at: now,
            version,
            system_memory: capabilities.system_memory.clone(),
        };
        self.store
            .apply_event(ControlEvent::NodeRegistered { node: node.clone() })
            .await;
        Ok(node)
    }

    pub async fn heartbeat(
        &self,
        node_id: &str,
        system_memory: Option<common::v1::Memory>,
    ) -> Result<()> {
        if self.store.get_node(node_id).await.is_none() {
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
            .await;
        Ok(())
    }

    pub async fn check_timeouts(&self, timeout: Duration) -> Result<Vec<String>> {
        let now = Utc::now();
        let mut timed_out = Vec::new();
        for node in self.store.list_nodes().await {
            let age = now
                .signed_duration_since(node.last_heartbeat)
                .to_std()
                .unwrap_or_default();
            if age > timeout && node.status != common::v1::NodeStatus::Offline as i32 {
                self.store
                    .apply_event(ControlEvent::NodeTimedOut {
                        node_id: node.node_id.clone(),
                    })
                    .await;
                timed_out.push(node.node_id);
            }
        }
        Ok(timed_out)
    }

    pub async fn deregister(&self, node_id: &str, reason: impl Into<String>) -> Result<()> {
        if self.store.get_node(node_id).await.is_none() {
            return Err(GalacticaError::not_found(format!(
                "unknown node: {node_id}"
            )));
        }
        self.store
            .apply_event(ControlEvent::NodeRemoved {
                node_id: node_id.to_string(),
                reason: reason.into(),
            })
            .await;
        Ok(())
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

impl Scheduler {
    async fn select_node(
        &self,
        pools: &BTreeMap<String, Vec<(NodeRecord, common::v1::ModelVariant)>>,
    ) -> Result<PlacementDecision> {
        let pool_name =
            pools.keys().next().cloned().ok_or_else(|| {
                GalacticaError::failed_precondition("no execution pools available")
            })?;
        let entries = pools
            .get(&pool_name)
            .ok_or_else(|| GalacticaError::failed_precondition("selected pool was empty"))?;
        let mut cursors = self.cursors.lock().await;
        let cursor = cursors.entry(pool_name.clone()).or_insert(0);
        let selected = entries[*cursor % entries.len()].clone();
        *cursor = (*cursor + 1) % entries.len();

        Ok(PlacementDecision {
            pool: pool_name,
            node: selected.0,
            variant: selected.1,
        })
    }
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

    pub fn remove_edge(&mut self, from: &str, to: &str) {
        if let Some(peers) = self.edges.get_mut(from) {
            peers.remove(to);
        }
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
            if let Some(neighbours) = edges.get(node) {
                for neighbour in neighbours {
                    if visit(neighbour, edges, visiting, visited) {
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
    ) -> Vec<String> {
        let mut compatible = BTreeSet::new();
        for node in nodes {
            if resolve_variant(manifest, &node.capabilities).is_ok() {
                compatible.insert(pool_label(&node.capabilities));
            }
        }
        compatible.into_iter().collect()
    }

    pub async fn place_model(
        &self,
        manifest: &common::v1::ModelManifest,
        nodes: &[NodeRecord],
    ) -> Result<PlacementDecision> {
        let mut pools: BTreeMap<String, Vec<(NodeRecord, common::v1::ModelVariant)>> =
            BTreeMap::new();
        for node in nodes
            .iter()
            .filter(|node| node.status == common::v1::NodeStatus::Online as i32)
        {
            if let Ok(variant) = resolve_variant(manifest, &node.capabilities) {
                pools
                    .entry(pool_label(&node.capabilities))
                    .or_default()
                    .push((node.clone(), variant));
            }
        }
        self.scheduler.select_node(&pools).await
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
        client
            .execute_task(request)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| GalacticaError::unavailable(format!("execute_task failed: {error}")))
    }
}

#[derive(Clone)]
pub struct ControlPlaneService<R, E> {
    store: Arc<InMemoryStateStore>,
    registry: NodeRegistry,
    artifacts: Arc<R>,
    placement: Arc<DefaultPlacementEngine>,
    executor: Arc<E>,
    heartbeat_interval_seconds: u32,
}

impl<R, E> ControlPlaneService<R, E>
where
    R: ModelRegistry + Send + Sync + 'static,
    E: NodeExecutor + Send + Sync + 'static,
{
    pub fn new(store: Arc<InMemoryStateStore>, artifacts: Arc<R>, executor: Arc<E>) -> Self {
        Self {
            registry: NodeRegistry::new(store.clone()),
            store,
            artifacts,
            placement: Arc::new(DefaultPlacementEngine::default()),
            executor,
            heartbeat_interval_seconds: 15,
        }
    }

    pub fn store(&self) -> Arc<InMemoryStateStore> {
        self.store.clone()
    }

    async fn infer_once(&self, request: InferenceRequest) -> Result<Vec<InferenceChunk>> {
        let manifest = self.artifacts.get_model_manifest(&request.model).await?;
        let state = self.store.get_state().await;
        let nodes = state.nodes.values().cloned().collect::<Vec<_>>();
        let placement = self.placement.place_model(&manifest, &nodes).await?;

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
                .await;
            self.store
                .apply_event(ControlEvent::InstanceStatusChanged {
                    instance_id: instance.instance_id.clone(),
                    status: InstanceStatus::Ready,
                })
                .await;
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
            .await;
        self.store
            .apply_event(ControlEvent::TaskStatusChanged {
                task_id: task_id.clone(),
                status: common::v1::TaskStatus::Running as i32,
                result: None,
                error_message: None,
            })
            .await;

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
            .await;

        if status != common::v1::TaskStatus::Completed as i32 {
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
            .collect();

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
        let request = request.into_inner();
        let capabilities = request
            .capabilities
            .ok_or_else(|| Status::invalid_argument("capabilities are required"))?;
        let node = self
            .registry
            .register(
                request.hostname,
                request.agent_endpoint,
                capabilities,
                request.version,
            )
            .await?;

        Ok(Response::new(control::v1::RegisterNodeResponse {
            node_id: Some(common::v1::NodeId {
                value: node.node_id,
            }),
            registered_at: Some(chrono_to_timestamp(node.registered_at)),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<control::v1::HeartbeatRequest>,
    ) -> std::result::Result<Response<control::v1::HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let node_id = request
            .node_id
            .as_ref()
            .map(|node_id| node_id.value.as_str())
            .ok_or_else(|| Status::invalid_argument("node_id is required"))?;
        self.registry
            .heartbeat(node_id, request.system_memory)
            .await?;

        Ok(Response::new(control::v1::HeartbeatResponse {
            server_time: Some(chrono_to_timestamp(Utc::now())),
            heartbeat_interval_seconds: self.heartbeat_interval_seconds,
        }))
    }

    async fn report_capabilities(
        &self,
        request: Request<control::v1::ReportCapabilitiesRequest>,
    ) -> std::result::Result<Response<control::v1::ReportCapabilitiesResponse>, Status> {
        let request = request.into_inner();
        let node_id = request
            .node_id
            .as_ref()
            .map(|node_id| node_id.value.as_str())
            .ok_or_else(|| Status::invalid_argument("node_id is required"))?;
        let capabilities = request
            .capabilities
            .ok_or_else(|| Status::invalid_argument("capabilities are required"))?;
        self.store
            .update_node_capabilities(node_id, capabilities)
            .await?;
        Ok(Response::new(control::v1::ReportCapabilitiesResponse {}))
    }

    async fn get_cluster_state(
        &self,
        _request: Request<control::v1::GetClusterStateRequest>,
    ) -> std::result::Result<Response<control::v1::GetClusterStateResponse>, Status> {
        let state = self.store.get_state().await;
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

    async fn list_available_models(
        &self,
        request: Request<control::v1::ListAvailableModelsRequest>,
    ) -> std::result::Result<Response<control::v1::ListAvailableModelsResponse>, Status> {
        let request = request.into_inner();
        let models = self
            .artifacts
            .list_models(
                (!request.filter_runtime.is_empty()).then_some(request.filter_runtime.as_str()),
                None,
            )
            .await?;

        Ok(Response::new(control::v1::ListAvailableModelsResponse {
            models,
        }))
    }

    async fn infer(
        &self,
        request: Request<control::v1::InferRequest>,
    ) -> std::result::Result<Response<Self::InferStream>, Status> {
        let chunks = self.infer_once(request.into_inner().into()).await?;
        let stream = try_stream! {
            for chunk in chunks {
                yield control::v1::InferChunk::from(chunk);
            }
        };

        Ok(Response::new(Box::pin(stream) as Self::InferStream))
    }

    async fn watch_events(
        &self,
        request: Request<control::v1::WatchEventsRequest>,
    ) -> std::result::Result<Response<Self::WatchEventsStream>, Status> {
        let since = request.into_inner().since.map(timestamp_to_chrono);
        let historical = self.store.get_events_since(0).await;
        let mut receiver = self.store.subscribe();
        let stream = try_stream! {
            for event in historical.into_iter().filter(|event| since.map(|since| event.timestamp > since).unwrap_or(true)) {
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

    async fn enroll_node(
        &self,
        request: Request<control::v1::EnrollNodeRequest>,
    ) -> std::result::Result<Response<control::v1::EnrollNodeResponse>, Status> {
        let request = request.into_inner();
        let node = self
            .registry
            .register(
                request.hostname,
                "http://127.0.0.1:50061".to_string(),
                request.capabilities.unwrap_or_default(),
                "enrolled".to_string(),
            )
            .await?;
        Ok(Response::new(control::v1::EnrollNodeResponse {
            identity: Some(control::v1::NodeIdentity {
                node_id: Some(common::v1::NodeId {
                    value: node.node_id,
                }),
                certificate_pem: "test-certificate".to_string(),
                private_key_pem: "test-private-key".to_string(),
                issued_at: Some(chrono_to_timestamp(Utc::now())),
                expires_at: Some(chrono_to_timestamp(Utc::now() + chrono::Duration::days(30))),
            }),
        }))
    }

    async fn authenticate(
        &self,
        _request: Request<control::v1::AuthenticateRequest>,
    ) -> std::result::Result<Response<control::v1::AuthenticateResponse>, Status> {
        Ok(Response::new(control::v1::AuthenticateResponse {
            authenticated: true,
            tenant_id: Some(common::v1::TenantId {
                value: "tenant-dev".to_string(),
            }),
            scopes: vec!["inference:read".to_string(), "inference:write".to_string()],
            expires_at: Some(chrono_to_timestamp(Utc::now() + chrono::Duration::hours(1))),
        }))
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

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

    #[tokio::test]
    async fn event_store_applies_every_event_type() {
        let store = InMemoryStateStore::default();
        let node = sample_node("node-a");
        store
            .apply_event(ControlEvent::NodeRegistered { node: node.clone() })
            .await;
        store
            .apply_event(ControlEvent::NodeHeartbeat {
                node_id: node.node_id.clone(),
                observed_at: Utc::now(),
                system_memory: node.system_memory.clone(),
            })
            .await;
        store
            .apply_event(ControlEvent::InstanceCreated {
                instance: ModelInstanceRecord {
                    instance_id: "instance-1".to_string(),
                    model_id: "mistral-small".to_string(),
                    node_id: node.node_id.clone(),
                    runtime: "mlx".to_string(),
                    quantization: "4bit".to_string(),
                    memory_used_bytes: 1024,
                    status: InstanceStatus::Loading,
                    updated_at: Utc::now(),
                },
            })
            .await;
        store
            .apply_event(ControlEvent::InstanceStatusChanged {
                instance_id: "instance-1".to_string(),
                status: InstanceStatus::Ready,
            })
            .await;
        store
            .apply_event(ControlEvent::TaskCreated {
                task: TaskRecord {
                    task_id: "task-1".to_string(),
                    request_id: "req-1".to_string(),
                    model_id: "mistral-small".to_string(),
                    node_id: node.node_id.clone(),
                    instance_id: "instance-1".to_string(),
                    prompt: "hello".to_string(),
                    status: common::v1::TaskStatus::Pending as i32,
                    result: None,
                    error_message: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
            })
            .await;
        store
            .apply_event(ControlEvent::TaskStatusChanged {
                task_id: "task-1".to_string(),
                status: common::v1::TaskStatus::Completed as i32,
                result: Some("done".to_string()),
                error_message: None,
            })
            .await;
        store
            .apply_event(ControlEvent::DownloadProgress {
                download: DownloadRecord {
                    model_id: "mistral-small".to_string(),
                    downloaded_bytes: 10,
                    total_bytes: 10,
                    updated_at: Utc::now(),
                },
            })
            .await;
        store
            .apply_event(ControlEvent::InstanceDeleted {
                instance_id: "instance-1".to_string(),
            })
            .await;
        store
            .apply_event(ControlEvent::NodeTimedOut {
                node_id: node.node_id.clone(),
            })
            .await;

        let state = store.get_state().await;
        assert_eq!(state.nodes.len(), 1);
        assert_eq!(state.tasks.len(), 1);
        assert_eq!(state.downloads.len(), 1);
        assert!(state.instances.is_empty());
    }

    #[tokio::test]
    async fn registry_detects_timeout_after_heartbeat() {
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
        registry
            .heartbeat(&node.node_id, Some(default_system_memory()))
            .await
            .unwrap();

        {
            let mut inner = store.inner.write().await;
            inner
                .state
                .nodes
                .get_mut(&node.node_id)
                .unwrap()
                .last_heartbeat = Utc::now() - chrono::Duration::seconds(60);
        }

        let timed_out = registry
            .check_timeouts(Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(timed_out, vec![node.node_id.clone()]);
        assert_eq!(
            store.get_node(&node.node_id).await.unwrap().status,
            common::v1::NodeStatus::Offline as i32
        );
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
    async fn placement_engine_filters_by_compatibility_and_topology_detects_cycles() {
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
        let pools = engine.compatible_pools(&manifest, &[sample_node("node-a")]);
        assert_eq!(pools, vec!["macos-metal-arm64".to_string()]);
        let decision = engine
            .place_model(&manifest, &[sample_node("node-a")])
            .await
            .unwrap();
        assert_eq!(decision.node.node_id, "node-a");

        let mut topology = TopologyGraph::default();
        topology.add_node("node-a");
        topology.add_node("node-b");
        topology.add_edge("node-a", "node-b");
        topology.add_edge("node-b", "node-a");
        assert!(topology.has_cycle());
    }

    #[tokio::test]
    async fn control_plane_routes_inference_to_node_agents() {
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
        let registry = NodeRegistry::new(store.clone());
        registry
            .register(
                "mac-mini".to_string(),
                format!("http://{addr}"),
                default_macos_capabilities(),
                "0.1.0".to_string(),
            )
            .await
            .unwrap();

        let service = ControlPlaneService::new(
            store,
            Arc::new(LocalModelRegistry::new(&root)),
            Arc::new(GrpcNodeExecutor),
        );
        let response = service
            .infer(Request::new(control::v1::InferRequest {
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
            }))
            .await
            .unwrap()
            .into_inner();
        let chunks = response.collect::<Vec<_>>().await;

        assert!(!chunks.is_empty());
        assert!(chunks.last().unwrap().as_ref().unwrap().finish_reason == "stop");
        std::fs::remove_dir_all(root).unwrap();
    }
}
