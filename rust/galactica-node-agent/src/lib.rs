pub mod hardware;
pub mod planner;
pub mod runtime_backends;
pub mod supervision;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use galactica_common::Result;
use galactica_common::inference::NodeExecutionPayload;
use galactica_common::proto::{common, node, runtime};
use galactica_observability::set_parent_from_tonic_request;
use tonic::{Request, Response, Status};
use tracing::{Instrument, info_span};

pub use hardware::{
    AcceleratorHint, DefaultHardwareDetector, HardwareMonitor, HardwareProbe, HardwareSample,
    HardwareSnapshot, SystemHardwareProbe,
};
pub use node::v1::node_agent_server::{NodeAgent, NodeAgentServer};
pub use planner::{
    DesiredModelState, DesiredNodeState, DesiredTaskAssignment, NodeAgentRunner, ObservedNodeState,
    PlanAction, PlanContext, download, execute, load, plan, shutdown_orphaned, start_needed,
};
pub use runtime::v1::runtime_backend_server::{
    RuntimeBackend as RuntimeBackendGrpc, RuntimeBackendServer,
};
pub use runtime_backends::{
    LlamaCppBackend, LlamaCppBackendConfig, MlxBackend, MlxBackendConfig, OnnxBackend,
    OnnxBackendConfig, VllmBackend, VllmBackendConfig,
};
pub use supervision::{
    DefaultProcessSupervisor, RuntimeHandle, RuntimeHealth, RuntimeLifecycle,
    RuntimeLifecycleState, RuntimeProcessConfig,
};

type GenerateStream =
    Pin<Box<dyn Stream<Item = std::result::Result<runtime::v1::GenerateResponse, Status>> + Send>>;
type EventStream =
    Pin<Box<dyn Stream<Item = std::result::Result<runtime::v1::RuntimeEvent, Status>> + Send>>;

fn rpc_server_span<T>(name: &'static str, request: &Request<T>) -> tracing::Span {
    let span = info_span!("rpc.server", rpc.method = %name, otel.kind = "server");
    set_parent_from_tonic_request(&span, request);
    span
}

#[async_trait]
pub trait RuntimeBackend: Send + Sync {
    async fn get_capabilities(&self) -> Result<runtime::v1::GetCapabilitiesResponse>;
    async fn list_models(&self) -> Result<Vec<runtime::v1::RuntimeModelInfo>>;
    async fn ensure_model(
        &self,
        manifest: common::v1::ModelManifest,
        variant_runtime: String,
        variant_quantization: String,
    ) -> Result<runtime::v1::EnsureModelResponse>;
    async fn load_model(
        &self,
        request: runtime::v1::LoadRuntimeModelRequest,
    ) -> Result<runtime::v1::LoadRuntimeModelResponse>;
    async fn unload_model(
        &self,
        request: runtime::v1::UnloadRuntimeModelRequest,
    ) -> Result<runtime::v1::UnloadRuntimeModelResponse>;
    async fn generate(&self, request: runtime::v1::GenerateRequest) -> Result<GenerateStream>;
    async fn embed(&self, request: runtime::v1::EmbedRequest)
    -> Result<runtime::v1::EmbedResponse>;
    async fn health(&self) -> Result<runtime::v1::HealthResponse>;
    async fn stream_events(&self) -> Result<EventStream>;
}

#[derive(Clone)]
pub struct RuntimeBackendService<B> {
    backend: Arc<B>,
}

impl<B> RuntimeBackendService<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

#[tonic::async_trait]
impl<B> RuntimeBackendGrpc for RuntimeBackendService<B>
where
    B: RuntimeBackend + 'static,
{
    type GenerateStream = GenerateStream;
    type StreamEventsStream = EventStream;

    async fn get_capabilities(
        &self,
        _request: Request<runtime::v1::GetCapabilitiesRequest>,
    ) -> std::result::Result<Response<runtime::v1::GetCapabilitiesResponse>, Status> {
        Ok(Response::new(self.backend.get_capabilities().await?))
    }

    async fn list_models(
        &self,
        _request: Request<runtime::v1::ListRuntimeModelsRequest>,
    ) -> std::result::Result<Response<runtime::v1::ListRuntimeModelsResponse>, Status> {
        Ok(Response::new(runtime::v1::ListRuntimeModelsResponse {
            models: self.backend.list_models().await?,
        }))
    }

    async fn ensure_model(
        &self,
        request: Request<runtime::v1::EnsureModelRequest>,
    ) -> std::result::Result<Response<runtime::v1::EnsureModelResponse>, Status> {
        let request = request.into_inner();
        let manifest = request
            .manifest
            .ok_or_else(|| Status::invalid_argument("manifest is required"))?;
        let response = self
            .backend
            .ensure_model(
                manifest,
                request.variant_runtime,
                request.variant_quantization,
            )
            .await?;
        Ok(Response::new(response))
    }

    async fn load_model(
        &self,
        request: Request<runtime::v1::LoadRuntimeModelRequest>,
    ) -> std::result::Result<Response<runtime::v1::LoadRuntimeModelResponse>, Status> {
        Ok(Response::new(
            self.backend.load_model(request.into_inner()).await?,
        ))
    }

    async fn unload_model(
        &self,
        request: Request<runtime::v1::UnloadRuntimeModelRequest>,
    ) -> std::result::Result<Response<runtime::v1::UnloadRuntimeModelResponse>, Status> {
        Ok(Response::new(
            self.backend.unload_model(request.into_inner()).await?,
        ))
    }

    async fn generate(
        &self,
        request: Request<runtime::v1::GenerateRequest>,
    ) -> std::result::Result<Response<Self::GenerateStream>, Status> {
        Ok(Response::new(
            self.backend.generate(request.into_inner()).await?,
        ))
    }

    async fn embed(
        &self,
        request: Request<runtime::v1::EmbedRequest>,
    ) -> std::result::Result<Response<runtime::v1::EmbedResponse>, Status> {
        Ok(Response::new(
            self.backend.embed(request.into_inner()).await?,
        ))
    }

    async fn health(
        &self,
        _request: Request<runtime::v1::HealthRequest>,
    ) -> std::result::Result<Response<runtime::v1::HealthResponse>, Status> {
        Ok(Response::new(self.backend.health().await?))
    }

    async fn stream_events(
        &self,
        _request: Request<runtime::v1::StreamEventsRequest>,
    ) -> std::result::Result<Response<Self::StreamEventsStream>, Status> {
        Ok(Response::new(self.backend.stream_events().await?))
    }
}

#[derive(Clone)]
pub struct NodeAgentService<B: ?Sized> {
    backend: Arc<B>,
    capabilities: common::v1::NodeCapabilities,
    system_memory: common::v1::Memory,
}

impl<B: ?Sized> NodeAgentService<B> {
    pub fn new(
        backend: Arc<B>,
        capabilities: common::v1::NodeCapabilities,
        system_memory: common::v1::Memory,
    ) -> Self {
        Self {
            backend,
            capabilities,
            system_memory,
        }
    }
}

#[tonic::async_trait]
impl<B> NodeAgent for NodeAgentService<B>
where
    B: RuntimeBackend + 'static + ?Sized,
{
    async fn execute_task(
        &self,
        request: Request<node::v1::ExecuteTaskRequest>,
    ) -> std::result::Result<Response<node::v1::ExecuteTaskResponse>, Status> {
        let span = rpc_server_span("node_agent.execute_task", &request);
        async move {
            let request = request.into_inner();
            let payload: NodeExecutionPayload =
                serde_json::from_slice(&request.payload).map_err(|error| {
                    Status::invalid_argument(format!("invalid task payload: {error}"))
                })?;
            let generate_request = runtime::v1::GenerateRequest {
                instance_id: request.instance_id.clone(),
                prompt: payload.prompt,
                params: Some(runtime::v1::GenerateParams {
                    temperature: payload.params.temperature,
                    top_p: payload.params.top_p,
                    max_tokens: payload.params.max_tokens,
                    stop: payload.params.stop,
                }),
            };
            let mut stream = self.backend.generate(generate_request).await?;
            let mut output = String::new();
            while let Some(chunk) = tokio_stream::StreamExt::next(&mut stream).await {
                let chunk = chunk?;
                output.push_str(&chunk.text);
            }

            Ok(Response::new(node::v1::ExecuteTaskResponse {
                task_id: request.task_id,
                status: common::v1::TaskStatus::Completed as i32,
                result: output.into_bytes(),
                error_message: String::new(),
            }))
        }
        .instrument(span)
        .await
    }

    async fn load_model(
        &self,
        request: Request<node::v1::LoadModelRequest>,
    ) -> std::result::Result<Response<node::v1::LoadModelResponse>, Status> {
        let span = rpc_server_span("node_agent.load_model", &request);
        async move {
            let request = request.into_inner();
            let response = self
                .backend
                .load_model(runtime::v1::LoadRuntimeModelRequest {
                    model_id: request.model_id,
                    quantization: request.variant_quantization,
                    max_memory_bytes: request.max_memory_bytes,
                    runtime_options: HashMap::from([(
                        "variant_runtime".to_string(),
                        request.variant_runtime,
                    )]),
                })
                .await?;

            Ok(Response::new(node::v1::LoadModelResponse {
                instance_id: response.instance_id,
                success: response.success,
                error_message: response.error_message,
            }))
        }
        .instrument(span)
        .await
    }

    async fn unload_model(
        &self,
        request: Request<node::v1::UnloadModelRequest>,
    ) -> std::result::Result<Response<node::v1::UnloadModelResponse>, Status> {
        let span = rpc_server_span("node_agent.unload_model", &request);
        async move {
            let response = self
                .backend
                .unload_model(runtime::v1::UnloadRuntimeModelRequest {
                    instance_id: request.into_inner().instance_id,
                })
                .await?;

            Ok(Response::new(node::v1::UnloadModelResponse {
                success: response.success,
                error_message: response.error_message,
            }))
        }
        .instrument(span)
        .await
    }

    async fn get_status(
        &self,
        request: Request<node::v1::GetStatusRequest>,
    ) -> std::result::Result<Response<node::v1::GetStatusResponse>, Status> {
        let span = rpc_server_span("node_agent.get_status", &request);
        async move {
            let runtime_name = self.backend.get_capabilities().await?.runtime_name;
            let loaded_models = self
                .backend
                .list_models()
                .await?
                .into_iter()
                .map(|model| node::v1::ModelInstance {
                    instance_id: model.instance_id,
                    model_id: model.model_id,
                    runtime: runtime_name.clone(),
                    memory_used_bytes: model.memory_used_bytes,
                })
                .collect();

            Ok(Response::new(node::v1::GetStatusResponse {
                status: common::v1::NodeStatus::Online as i32,
                capabilities: Some(self.capabilities.clone()),
                system_memory: Some(self.system_memory),
                loaded_models,
            }))
        }
        .instrument(span)
        .await
    }
}

pub fn default_macos_capabilities() -> common::v1::NodeCapabilities {
    common::v1::NodeCapabilities {
        os: common::v1::OsType::Macos as i32,
        cpu_arch: common::v1::CpuArch::Arm64 as i32,
        accelerators: vec![common::v1::AcceleratorInfo {
            r#type: common::v1::AcceleratorType::Metal as i32,
            name: "Apple GPU".to_string(),
            vram: Some(common::v1::Memory {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
            }),
            compute_capability: "metal3".to_string(),
        }],
        system_memory: Some(common::v1::Memory {
            total_bytes: 16 * 1024 * 1024 * 1024,
            available_bytes: 12 * 1024 * 1024 * 1024,
        }),
        network_profile: common::v1::NetworkProfile::Lan as i32,
        runtime_backends: vec!["mlx".to_string()],
        locality: HashMap::from([("machine".to_string(), "local".to_string())]),
    }
}

pub fn default_system_memory() -> common::v1::Memory {
    common::v1::Memory {
        total_bytes: 16 * 1024 * 1024 * 1024,
        available_bytes: 12 * 1024 * 1024 * 1024,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use galactica_common::proto::node::v1::node_agent_server::NodeAgent;
    use tonic::Request;

    use super::{
        MlxBackend, NodeAgentService, RuntimeBackend, default_macos_capabilities,
        default_system_memory,
    };

    #[tokio::test]
    async fn mlx_backend_loads_generates_and_embeds() {
        let backend = MlxBackend::new();
        let loaded = backend
            .load_model(super::runtime::v1::LoadRuntimeModelRequest {
                model_id: Some(super::common::v1::ModelId {
                    value: "mistral-small".to_string(),
                }),
                quantization: "4bit".to_string(),
                max_memory_bytes: 2 * 1024 * 1024 * 1024,
                runtime_options: HashMap::new(),
            })
            .await
            .unwrap();
        let instance_id = loaded.instance_id.unwrap();

        let mut stream = backend
            .generate(super::runtime::v1::GenerateRequest {
                instance_id: Some(instance_id.clone()),
                prompt: "user: hello".to_string(),
                params: Some(super::runtime::v1::GenerateParams {
                    temperature: 0.7,
                    top_p: 1.0,
                    max_tokens: 32,
                    stop: Vec::new(),
                }),
            })
            .await
            .unwrap();
        let mut output = String::new();
        while let Some(chunk) = tokio_stream::StreamExt::next(&mut stream).await {
            output.push_str(&chunk.unwrap().text);
        }
        assert!(output.contains("hello"));

        let embedded = backend
            .embed(super::runtime::v1::EmbedRequest {
                instance_id: Some(instance_id),
                inputs: vec!["hello".to_string()],
            })
            .await
            .unwrap();
        assert_eq!(embedded.embeddings.len(), 1);
        assert_eq!(embedded.total_tokens, 1);
    }

    #[tokio::test]
    async fn node_agent_executes_tasks() {
        let backend = Arc::new(MlxBackend::new());
        let agent = NodeAgentService::new(
            backend.clone(),
            default_macos_capabilities(),
            default_system_memory(),
        );
        let loaded = agent
            .load_model(Request::new(super::node::v1::LoadModelRequest {
                model_id: Some(super::common::v1::ModelId {
                    value: "mistral-small".to_string(),
                }),
                variant_runtime: "mlx".to_string(),
                variant_quantization: "4bit".to_string(),
                max_memory_bytes: 2 * 1024 * 1024 * 1024,
            }))
            .await
            .unwrap()
            .into_inner();
        let payload = serde_json::to_vec(&super::NodeExecutionPayload {
            task_id: "task-1".to_string(),
            prompt: "user: summarize cluster status".to_string(),
            params: Default::default(),
        })
        .unwrap();

        let response = agent
            .execute_task(Request::new(super::node::v1::ExecuteTaskRequest {
                task_id: Some(super::common::v1::TaskId {
                    value: "task-1".to_string(),
                }),
                instance_id: loaded.instance_id,
                payload,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            response.status,
            super::common::v1::TaskStatus::Completed as i32
        );
        assert!(
            String::from_utf8(response.result)
                .unwrap()
                .contains("summarize")
        );
    }
}
