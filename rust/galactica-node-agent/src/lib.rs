pub mod hardware;
pub mod planner;
pub mod runtime_backends;
pub mod supervision;

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use async_stream::try_stream;
use async_trait::async_trait;
use futures::Stream;
use galactica_common::inference::{NodeExecutionPayload, estimate_tokens};
use galactica_common::proto::{common, node, runtime};
use galactica_common::{GalacticaError, Result, chrono_to_timestamp};
use tokio::sync::{RwLock, broadcast};
use tonic::{Request, Response, Status};

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
    LlamaCppBackend, LlamaCppBackendConfig, OnnxBackend, OnnxBackendConfig, VllmBackend,
    VllmBackendConfig,
};
pub use supervision::{
    DefaultProcessSupervisor, RuntimeHandle, RuntimeHealth, RuntimeLifecycle,
    RuntimeLifecycleState, RuntimeProcessConfig,
};

type GenerateStream =
    Pin<Box<dyn Stream<Item = std::result::Result<runtime::v1::GenerateResponse, Status>> + Send>>;
type EventStream =
    Pin<Box<dyn Stream<Item = std::result::Result<runtime::v1::RuntimeEvent, Status>> + Send>>;

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

#[derive(Debug, Clone)]
struct LoadedInstance {
    instance_id: String,
    model_id: String,
    quantization: String,
    memory_used_bytes: u64,
    ready: bool,
}

#[derive(Clone)]
pub struct MlxBackend {
    started_at: Instant,
    loaded_models: Arc<RwLock<BTreeMap<String, LoadedInstance>>>,
    event_sender: broadcast::Sender<runtime::v1::RuntimeEvent>,
}

impl Default for MlxBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MlxBackend {
    pub fn new() -> Self {
        let (event_sender, _) = broadcast::channel(64);
        Self {
            started_at: Instant::now(),
            loaded_models: Arc::new(RwLock::new(BTreeMap::new())),
            event_sender,
        }
    }

    fn supports_variant(runtime: &str) -> bool {
        runtime.eq_ignore_ascii_case("mlx")
    }

    fn synthetic_completion(prompt: &str) -> String {
        let last_line = prompt.lines().last().unwrap_or(prompt).trim();
        format!("MLX synthesized response for {last_line}")
    }
}

#[async_trait]
impl RuntimeBackend for MlxBackend {
    async fn get_capabilities(&self) -> Result<runtime::v1::GetCapabilitiesResponse> {
        Ok(runtime::v1::GetCapabilitiesResponse {
            runtime_name: "mlx".to_string(),
            runtime_version: "simulated-1.0".to_string(),
            supported_accelerators: vec![common::v1::AcceleratorType::Metal as i32],
            supported_quantizations: vec![
                "4bit".to_string(),
                "8bit".to_string(),
                "fp16".to_string(),
            ],
            supports_embedding: true,
            supports_streaming: true,
            max_model_size_bytes: 32 * 1024 * 1024 * 1024,
        })
    }

    async fn list_models(&self) -> Result<Vec<runtime::v1::RuntimeModelInfo>> {
        let models = self.loaded_models.read().await;
        Ok(models
            .values()
            .map(|instance| runtime::v1::RuntimeModelInfo {
                instance_id: Some(common::v1::InstanceId {
                    value: instance.instance_id.clone(),
                }),
                model_id: Some(common::v1::ModelId {
                    value: instance.model_id.clone(),
                }),
                quantization: instance.quantization.clone(),
                memory_used_bytes: instance.memory_used_bytes,
                ready: instance.ready,
            })
            .collect())
    }

    async fn ensure_model(
        &self,
        manifest: common::v1::ModelManifest,
        variant_runtime: String,
        variant_quantization: String,
    ) -> Result<runtime::v1::EnsureModelResponse> {
        let variant = manifest
            .variants
            .iter()
            .find(|candidate| {
                candidate.runtime.eq_ignore_ascii_case(&variant_runtime)
                    && (variant_quantization.is_empty()
                        || candidate.quantization == variant_quantization)
            })
            .ok_or_else(|| {
                GalacticaError::failed_precondition(format!(
                    "no MLX variant available for {}",
                    manifest
                        .model_id
                        .as_ref()
                        .map(|model_id| model_id.value.as_str())
                        .unwrap_or("unknown")
                ))
            })?;
        if !Self::supports_variant(&variant.runtime) {
            return Err(GalacticaError::failed_precondition(format!(
                "runtime {} is not supported by the MLX backend",
                variant.runtime
            )));
        }

        Ok(runtime::v1::EnsureModelResponse {
            available: true,
            download_required: false,
            estimated_size_bytes: variant.size_bytes,
        })
    }

    async fn load_model(
        &self,
        request: runtime::v1::LoadRuntimeModelRequest,
    ) -> Result<runtime::v1::LoadRuntimeModelResponse> {
        let model_id = request
            .model_id
            .as_ref()
            .map(|model_id| model_id.value.clone())
            .ok_or_else(|| GalacticaError::invalid_argument("model_id is required"))?;
        let instance_id = format!("mlx-{}", uuid::Uuid::new_v4());
        let memory_used_bytes = request
            .max_memory_bytes
            .clamp(512 * 1024 * 1024, 6 * 1024 * 1024 * 1024);

        self.loaded_models.write().await.insert(
            instance_id.clone(),
            LoadedInstance {
                instance_id: instance_id.clone(),
                model_id: model_id.clone(),
                quantization: request.quantization.clone(),
                memory_used_bytes,
                ready: true,
            },
        );
        let _ = self.event_sender.send(runtime::v1::RuntimeEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Some(chrono_to_timestamp(chrono::Utc::now())),
            event: Some(runtime::v1::runtime_event::Event::ModelLoaded(
                runtime::v1::RuntimeModelLoadedEvent {
                    instance_id: Some(common::v1::InstanceId {
                        value: instance_id.clone(),
                    }),
                    model_id: Some(common::v1::ModelId { value: model_id }),
                    memory_used_bytes,
                },
            )),
        });

        Ok(runtime::v1::LoadRuntimeModelResponse {
            instance_id: Some(common::v1::InstanceId { value: instance_id }),
            success: true,
            error_message: String::new(),
            memory_used_bytes,
        })
    }

    async fn unload_model(
        &self,
        request: runtime::v1::UnloadRuntimeModelRequest,
    ) -> Result<runtime::v1::UnloadRuntimeModelResponse> {
        let instance_id = request
            .instance_id
            .as_ref()
            .map(|instance_id| instance_id.value.clone())
            .ok_or_else(|| GalacticaError::invalid_argument("instance_id is required"))?;
        let removed = self.loaded_models.write().await.remove(&instance_id);
        if removed.is_none() {
            return Err(GalacticaError::not_found(format!(
                "loaded model not found: {instance_id}"
            )));
        }
        let _ = self.event_sender.send(runtime::v1::RuntimeEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Some(chrono_to_timestamp(chrono::Utc::now())),
            event: Some(runtime::v1::runtime_event::Event::ModelUnloaded(
                runtime::v1::RuntimeModelUnloadedEvent {
                    instance_id: Some(common::v1::InstanceId { value: instance_id }),
                },
            )),
        });

        Ok(runtime::v1::UnloadRuntimeModelResponse {
            success: true,
            error_message: String::new(),
        })
    }

    async fn generate(&self, request: runtime::v1::GenerateRequest) -> Result<GenerateStream> {
        let instance_id = request
            .instance_id
            .as_ref()
            .map(|instance_id| instance_id.value.clone())
            .ok_or_else(|| GalacticaError::invalid_argument("instance_id is required"))?;
        if !self.loaded_models.read().await.contains_key(&instance_id) {
            return Err(GalacticaError::not_found(format!(
                "loaded model not found: {instance_id}"
            )));
        }

        let completion = Self::synthetic_completion(&request.prompt);
        let prompt_tokens = estimate_tokens(&request.prompt);
        let completion_tokens = estimate_tokens(&completion);

        let stream = try_stream! {
            let words: Vec<&str> = completion.split_whitespace().collect();
            for (index, word) in words.iter().enumerate() {
                let finished = index + 1 == words.len();
                yield runtime::v1::GenerateResponse {
                    text: if finished {
                        (*word).to_string()
                    } else {
                        format!("{word} ")
                    },
                    finished,
                    finish_reason: if finished { "stop".to_string() } else { String::new() },
                    usage: Some(runtime::v1::GenerateUsage {
                        prompt_tokens,
                        completion_tokens,
                        tokens_per_second: 32.0,
                    }),
                };
            }
        };

        Ok(Box::pin(stream) as GenerateStream)
    }

    async fn embed(
        &self,
        request: runtime::v1::EmbedRequest,
    ) -> Result<runtime::v1::EmbedResponse> {
        let _instance_id = request
            .instance_id
            .as_ref()
            .map(|instance_id| instance_id.value.clone())
            .ok_or_else(|| GalacticaError::invalid_argument("instance_id is required"))?;
        let embeddings = request
            .inputs
            .iter()
            .map(|input| runtime::v1::Embedding {
                values: vec![
                    input.len() as f32,
                    input.bytes().map(f32::from).sum::<f32>(),
                    estimate_tokens(input) as f32,
                ],
            })
            .collect::<Vec<_>>();

        Ok(runtime::v1::EmbedResponse {
            embeddings,
            total_tokens: request
                .inputs
                .iter()
                .map(|input| estimate_tokens(input))
                .sum(),
        })
    }

    async fn health(&self) -> Result<runtime::v1::HealthResponse> {
        let model_count = self.loaded_models.read().await.len() as u32;
        Ok(runtime::v1::HealthResponse {
            status: runtime::v1::RuntimeStatus::Healthy as i32,
            runtime_name: "mlx".to_string(),
            runtime_version: "simulated-1.0".to_string(),
            uptime_seconds: self.started_at.elapsed().as_secs(),
            loaded_model_count: model_count,
        })
    }

    async fn stream_events(&self) -> Result<EventStream> {
        let mut receiver = self.event_sender.subscribe();
        let stream = try_stream! {
            loop {
                match receiver.recv().await {
                    Ok(event) => yield event,
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        };

        Ok(Box::pin(stream) as EventStream)
    }
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
pub struct NodeAgentService<B> {
    backend: Arc<B>,
    capabilities: common::v1::NodeCapabilities,
    system_memory: common::v1::Memory,
}

impl<B> NodeAgentService<B> {
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
    B: RuntimeBackend + 'static,
{
    async fn execute_task(
        &self,
        request: Request<node::v1::ExecuteTaskRequest>,
    ) -> std::result::Result<Response<node::v1::ExecuteTaskResponse>, Status> {
        let request = request.into_inner();
        let payload: NodeExecutionPayload = serde_json::from_slice(&request.payload)
            .map_err(|error| Status::invalid_argument(format!("invalid task payload: {error}")))?;
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

    async fn load_model(
        &self,
        request: Request<node::v1::LoadModelRequest>,
    ) -> std::result::Result<Response<node::v1::LoadModelResponse>, Status> {
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

    async fn unload_model(
        &self,
        request: Request<node::v1::UnloadModelRequest>,
    ) -> std::result::Result<Response<node::v1::UnloadModelResponse>, Status> {
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

    async fn get_status(
        &self,
        _request: Request<node::v1::GetStatusRequest>,
    ) -> std::result::Result<Response<node::v1::GetStatusResponse>, Status> {
        let loaded_models = self
            .backend
            .list_models()
            .await?
            .into_iter()
            .map(|model| node::v1::ModelInstance {
                instance_id: model.instance_id,
                model_id: model.model_id,
                runtime: "mlx".to_string(),
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
