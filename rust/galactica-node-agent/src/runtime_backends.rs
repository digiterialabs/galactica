use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_stream::try_stream;
use async_trait::async_trait;
use galactica_artifact::{LocalModelRegistry, ModelRegistry};
use galactica_common::inference::estimate_tokens;
use galactica_common::proto::{common, runtime};
use galactica_common::{GalacticaError, Result, chrono_to_timestamp};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock, broadcast};

use crate::supervision::{
    DefaultProcessSupervisor, RuntimeHandle, RuntimeLifecycleState, RuntimeProcessConfig,
};
use crate::{EventStream, GenerateStream, RuntimeBackend};

#[derive(Debug, Clone)]
struct ManagedLoadedModel {
    instance_id: String,
    model_id: String,
    quantization: String,
    memory_used_bytes: u64,
    ready: bool,
}

#[derive(Debug, Clone)]
struct ManagedRuntimeSpec {
    runtime_name: String,
    runtime_version: String,
    runtime_aliases: Vec<String>,
    supported_accelerators: Vec<i32>,
    supported_quantizations: Vec<String>,
    supports_embedding: bool,
    supports_streaming: bool,
    max_model_size_bytes: u64,
    instance_prefix: String,
    synthetic_prefix: String,
    process_config: RuntimeProcessConfig,
    min_memory_bytes: u64,
    max_memory_bytes: u64,
    default_runtime_options: HashMap<String, String>,
}

impl ManagedRuntimeSpec {
    fn supports_runtime(&self, runtime_name: &str) -> bool {
        self.runtime_name.eq_ignore_ascii_case(runtime_name)
            || self
                .runtime_aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(runtime_name))
    }

    fn normalize_quantization(&self, quantization: &str) -> String {
        if quantization.is_empty() {
            return self
                .supported_quantizations
                .first()
                .cloned()
                .unwrap_or_else(|| "default".to_string());
        }
        quantization.to_string()
    }
}

#[derive(Clone)]
struct ManagedRuntimeBackend {
    spec: Arc<ManagedRuntimeSpec>,
    started_at: Instant,
    loaded_models: Arc<RwLock<BTreeMap<String, ManagedLoadedModel>>>,
    event_sender: broadcast::Sender<runtime::v1::RuntimeEvent>,
    supervisor: DefaultProcessSupervisor,
    process_handle: Arc<Mutex<Option<RuntimeHandle>>>,
}

impl ManagedRuntimeBackend {
    fn new(spec: ManagedRuntimeSpec) -> Self {
        let (event_sender, _) = broadcast::channel(64);
        Self {
            spec: Arc::new(spec),
            started_at: Instant::now(),
            loaded_models: Arc::new(RwLock::new(BTreeMap::new())),
            event_sender,
            supervisor: DefaultProcessSupervisor,
            process_handle: Arc::new(Mutex::new(None)),
        }
    }

    async fn ensure_process(&self) -> Result<()> {
        let mut handle = self.process_handle.lock().await;
        match handle.as_mut() {
            Some(existing) => {
                let health = self.supervisor.health(existing).await?;
                if !health.alive || health.lifecycle_state == RuntimeLifecycleState::Failed {
                    self.supervisor.restart(existing).await?;
                }
            }
            None => {
                *handle = Some(
                    self.supervisor
                        .spawn(self.spec.process_config.clone())
                        .await?,
                );
            }
        }
        tokio::time::sleep(self.spec.process_config.health_grace_period).await;
        if let Some(existing) = handle.as_mut() {
            let health = self.supervisor.health(existing).await?;
            if health.lifecycle_state == RuntimeLifecycleState::Failed {
                let message = format!(
                    "{} runtime failed to become healthy",
                    self.spec.runtime_name
                );
                self.emit_error(message.clone(), "error").await;
                return Err(GalacticaError::unavailable(message));
            }
        }
        Ok(())
    }

    async fn transition_process(&self, next: RuntimeLifecycleState) -> Result<()> {
        let mut handle = self.process_handle.lock().await;
        if let Some(handle) = handle.as_mut() {
            handle.transition(next)?;
        }
        Ok(())
    }

    async fn shutdown_process_if_idle(&self) -> Result<()> {
        if !self.loaded_models.read().await.is_empty() {
            return Ok(());
        }
        let mut handle = self.process_handle.lock().await;
        if let Some(runtime_handle) = handle.as_mut() {
            self.supervisor.shutdown(runtime_handle).await?;
        }
        *handle = None;
        Ok(())
    }

    fn synthetic_completion(&self, prompt: &str) -> String {
        let last_line = prompt.lines().last().unwrap_or(prompt).trim();
        format!(
            "{} synthesized response for {last_line}",
            self.spec.synthetic_prefix
        )
    }

    async fn emit_model_loaded(
        &self,
        instance_id: String,
        model_id: String,
        memory_used_bytes: u64,
    ) {
        let _ = self.event_sender.send(runtime::v1::RuntimeEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Some(chrono_to_timestamp(chrono::Utc::now())),
            event: Some(runtime::v1::runtime_event::Event::ModelLoaded(
                runtime::v1::RuntimeModelLoadedEvent {
                    instance_id: Some(common::v1::InstanceId { value: instance_id }),
                    model_id: Some(common::v1::ModelId { value: model_id }),
                    memory_used_bytes,
                },
            )),
        });
    }

    async fn emit_model_unloaded(&self, instance_id: String) {
        let _ = self.event_sender.send(runtime::v1::RuntimeEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Some(chrono_to_timestamp(chrono::Utc::now())),
            event: Some(runtime::v1::runtime_event::Event::ModelUnloaded(
                runtime::v1::RuntimeModelUnloadedEvent {
                    instance_id: Some(common::v1::InstanceId { value: instance_id }),
                },
            )),
        });
    }

    async fn emit_error(&self, message: String, severity: &str) {
        let _ = self.event_sender.send(runtime::v1::RuntimeEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Some(chrono_to_timestamp(chrono::Utc::now())),
            event: Some(runtime::v1::runtime_event::Event::Error(
                runtime::v1::RuntimeErrorEvent {
                    message,
                    severity: severity.to_string(),
                },
            )),
        });
    }

    fn memory_for_request(&self, request: &runtime::v1::LoadRuntimeModelRequest) -> u64 {
        let quantization = self.spec.normalize_quantization(&request.quantization);
        let base = request
            .max_memory_bytes
            .max(self.spec.min_memory_bytes)
            .min(self.spec.max_memory_bytes);
        match self.spec.runtime_name.as_str() {
            "vllm" => {
                let tensor_parallel_size = request
                    .runtime_options
                    .get("tensor_parallel_size")
                    .or_else(|| {
                        self.spec
                            .default_runtime_options
                            .get("tensor_parallel_size")
                    })
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(1)
                    .max(1);
                base.saturating_mul(tensor_parallel_size)
                    .min(self.spec.max_memory_bytes)
            }
            "llama.cpp" => {
                let gpu_layers = request
                    .runtime_options
                    .get("gpu_layers")
                    .or_else(|| self.spec.default_runtime_options.get("gpu_layers"))
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0);
                let quantization_bonus = if quantization.contains("q8") {
                    384 * 1024 * 1024
                } else if quantization.contains("q5") {
                    192 * 1024 * 1024
                } else {
                    64 * 1024 * 1024
                };
                base.saturating_add(gpu_layers * 8 * 1024 * 1024)
                    .saturating_add(quantization_bonus)
                    .min(self.spec.max_memory_bytes)
            }
            "onnxruntime" => {
                let provider = request
                    .runtime_options
                    .get("execution_provider")
                    .or_else(|| self.spec.default_runtime_options.get("execution_provider"))
                    .map(|value| value.to_ascii_lowercase())
                    .unwrap_or_else(|| "cpu".to_string());
                let provider_bonus = match provider.as_str() {
                    "cuda" => 512 * 1024 * 1024,
                    "directml" => 256 * 1024 * 1024,
                    _ => 64 * 1024 * 1024,
                };
                base.saturating_add(provider_bonus)
                    .min(self.spec.max_memory_bytes)
            }
            _ => base,
        }
    }

    async fn process_health(&self) -> Result<Option<crate::RuntimeHealth>> {
        let mut handle = self.process_handle.lock().await;
        if let Some(runtime_handle) = handle.as_mut() {
            let health = self.supervisor.health(runtime_handle).await?;
            if health.lifecycle_state == RuntimeLifecycleState::Failed {
                self.emit_error(
                    format!(
                        "{} runtime process failed with {:?}",
                        self.spec.runtime_name, health.last_exit_code
                    ),
                    "error",
                )
                .await;
            }
            if !health.alive && health.lifecycle_state == RuntimeLifecycleState::Idle {
                *handle = None;
            }
            return Ok(Some(health));
        }
        Ok(None)
    }

    async fn list_loaded_models(&self) -> Vec<runtime::v1::RuntimeModelInfo> {
        let models = self.loaded_models.read().await;
        models
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
            .collect()
    }
}

#[async_trait]
impl RuntimeBackend for ManagedRuntimeBackend {
    async fn get_capabilities(&self) -> Result<runtime::v1::GetCapabilitiesResponse> {
        Ok(runtime::v1::GetCapabilitiesResponse {
            runtime_name: self.spec.runtime_name.clone(),
            runtime_version: self.spec.runtime_version.clone(),
            supported_accelerators: self.spec.supported_accelerators.clone(),
            supported_quantizations: self.spec.supported_quantizations.clone(),
            supports_embedding: self.spec.supports_embedding,
            supports_streaming: self.spec.supports_streaming,
            max_model_size_bytes: self.spec.max_model_size_bytes,
        })
    }

    async fn list_models(&self) -> Result<Vec<runtime::v1::RuntimeModelInfo>> {
        Ok(self.list_loaded_models().await)
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
                self.spec.supports_runtime(&candidate.runtime)
                    && self.spec.supports_runtime(&variant_runtime)
                    && (variant_quantization.is_empty()
                        || candidate.quantization == variant_quantization)
            })
            .ok_or_else(|| {
                GalacticaError::failed_precondition(format!(
                    "no {} variant available for {}",
                    self.spec.runtime_name,
                    manifest
                        .model_id
                        .as_ref()
                        .map(|model_id| model_id.value.as_str())
                        .unwrap_or("unknown")
                ))
            })?;

        let download_required = manifest.metadata.contains_key("download_url")
            && !manifest.metadata.contains_key("local_path");

        Ok(runtime::v1::EnsureModelResponse {
            available: true,
            download_required,
            estimated_size_bytes: variant.size_bytes,
        })
    }

    async fn load_model(
        &self,
        request: runtime::v1::LoadRuntimeModelRequest,
    ) -> Result<runtime::v1::LoadRuntimeModelResponse> {
        self.ensure_process().await?;
        self.transition_process(RuntimeLifecycleState::Loading)
            .await?;
        let model_id = request
            .model_id
            .as_ref()
            .map(|model_id| model_id.value.clone())
            .ok_or_else(|| GalacticaError::invalid_argument("model_id is required"))?;
        let instance_id = format!("{}-{}", self.spec.instance_prefix, uuid::Uuid::new_v4());
        let memory_used_bytes = self.memory_for_request(&request);

        self.loaded_models.write().await.insert(
            instance_id.clone(),
            ManagedLoadedModel {
                instance_id: instance_id.clone(),
                model_id: model_id.clone(),
                quantization: self.spec.normalize_quantization(&request.quantization),
                memory_used_bytes,
                ready: true,
            },
        );
        self.transition_process(RuntimeLifecycleState::Ready)
            .await?;
        self.emit_model_loaded(instance_id.clone(), model_id, memory_used_bytes)
            .await;

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
        self.emit_model_unloaded(instance_id.clone()).await;
        self.shutdown_process_if_idle().await?;

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
        self.ensure_process().await?;
        self.transition_process(RuntimeLifecycleState::Running)
            .await?;

        let completion = self.synthetic_completion(&request.prompt);
        let prompt_tokens = estimate_tokens(&request.prompt);
        let completion_tokens = estimate_tokens(&completion);
        let process_handle = Arc::clone(&self.process_handle);
        let spec = Arc::clone(&self.spec);

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
                        tokens_per_second: if spec.runtime_name == "vllm" { 128.0 } else { 48.0 },
                    }),
                };
            }
            let mut handle = process_handle.lock().await;
            if let Some(handle) = handle.as_mut() {
                let _ = handle.transition(RuntimeLifecycleState::Ready);
            }
        };

        Ok(Box::pin(stream) as GenerateStream)
    }

    async fn embed(
        &self,
        request: runtime::v1::EmbedRequest,
    ) -> Result<runtime::v1::EmbedResponse> {
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
        self.ensure_process().await?;

        let embeddings = request
            .inputs
            .iter()
            .map(|input| runtime::v1::Embedding {
                values: vec![
                    input.len() as f32,
                    input.bytes().map(f32::from).sum::<f32>(),
                    estimate_tokens(input) as f32,
                    self.spec.runtime_name.len() as f32,
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
        let loaded_model_count = self.loaded_models.read().await.len() as u32;
        let process_health = self.process_health().await?;
        let status = match process_health.as_ref() {
            Some(health) if health.lifecycle_state == RuntimeLifecycleState::Failed => {
                runtime::v1::RuntimeStatus::Unhealthy
            }
            Some(health)
                if matches!(
                    health.lifecycle_state,
                    RuntimeLifecycleState::Starting
                        | RuntimeLifecycleState::Downloading
                        | RuntimeLifecycleState::Loading
                        | RuntimeLifecycleState::ShuttingDown
                ) =>
            {
                runtime::v1::RuntimeStatus::Degraded
            }
            Some(health) if !health.alive && loaded_model_count > 0 => {
                runtime::v1::RuntimeStatus::Unhealthy
            }
            _ => runtime::v1::RuntimeStatus::Healthy,
        };

        Ok(runtime::v1::HealthResponse {
            status: status as i32,
            runtime_name: self.spec.runtime_name.clone(),
            runtime_version: self.spec.runtime_version.clone(),
            uptime_seconds: self.started_at.elapsed().as_secs(),
            loaded_model_count,
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

#[derive(Debug, Clone)]
pub struct VllmBackendConfig {
    pub process: RuntimeProcessConfig,
    pub tensor_parallel_size: u32,
}

impl Default for VllmBackendConfig {
    fn default() -> Self {
        Self {
            process: simulated_process_config("vllm"),
            tensor_parallel_size: 1,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MlxBackendConfig {
    pub python_program: String,
    pub startup_timeout: Duration,
}

impl Default for MlxBackendConfig {
    fn default() -> Self {
        Self {
            python_program: "python3".to_string(),
            startup_timeout: Duration::from_secs(900),
        }
    }
}

#[derive(Debug, Clone)]
struct MlxLoadedModel {
    instance_id: String,
    model_id: String,
    quantization: String,
    memory_used_bytes: u64,
    ready: bool,
    api_model_name: String,
    endpoint: String,
    handle: Arc<Mutex<RuntimeHandle>>,
}

#[derive(Debug, Clone)]
enum MlxModelSource {
    Local(PathBuf),
    HuggingFace { repo: String },
}

#[derive(Clone)]
enum MlxBackendMode {
    Simulated(ManagedRuntimeBackend),
    Real(Box<RealMlxBackend>),
}

#[derive(Clone)]
pub struct MlxBackend {
    mode: MlxBackendMode,
}

#[derive(Clone)]
struct RealMlxBackend {
    config: MlxBackendConfig,
    started_at: Instant,
    loaded_models: Arc<RwLock<BTreeMap<String, MlxLoadedModel>>>,
    event_sender: broadcast::Sender<runtime::v1::RuntimeEvent>,
    supervisor: DefaultProcessSupervisor,
    client: reqwest::Client,
    registry: LocalModelRegistry,
    models_root: PathBuf,
}

impl Default for MlxBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MlxBackend {
    pub fn new() -> Self {
        Self::with_config(MlxBackendConfig::default())
    }

    pub fn with_config(_config: MlxBackendConfig) -> Self {
        Self {
            mode: MlxBackendMode::Simulated(ManagedRuntimeBackend::new(ManagedRuntimeSpec {
                runtime_name: "mlx".to_string(),
                runtime_version: "simulated-1.0".to_string(),
                runtime_aliases: vec!["mlx".to_string()],
                supported_accelerators: vec![common::v1::AcceleratorType::Metal as i32],
                supported_quantizations: vec![
                    "4bit".to_string(),
                    "8bit".to_string(),
                    "fp16".to_string(),
                ],
                supports_embedding: true,
                supports_streaming: true,
                max_model_size_bytes: 32 * 1024 * 1024 * 1024,
                instance_prefix: "mlx".to_string(),
                synthetic_prefix: "MLX".to_string(),
                process_config: simulated_process_config("mlx"),
                min_memory_bytes: 512 * 1024 * 1024,
                max_memory_bytes: 32 * 1024 * 1024 * 1024,
                default_runtime_options: HashMap::new(),
            })),
        }
    }

    pub fn with_model_registry(config: MlxBackendConfig, models_root: impl Into<PathBuf>) -> Self {
        Self {
            mode: MlxBackendMode::Real(Box::new(RealMlxBackend::new(config, models_root.into()))),
        }
    }
}

impl RealMlxBackend {
    fn new(config: MlxBackendConfig, models_root: PathBuf) -> Self {
        let (event_sender, _) = broadcast::channel(64);
        Self {
            config,
            started_at: Instant::now(),
            loaded_models: Arc::new(RwLock::new(BTreeMap::new())),
            event_sender,
            supervisor: DefaultProcessSupervisor,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(600))
                .build()
                .expect("reqwest client should initialize"),
            registry: LocalModelRegistry::new(&models_root),
            models_root,
        }
    }

    fn capabilities(&self) -> runtime::v1::GetCapabilitiesResponse {
        runtime::v1::GetCapabilitiesResponse {
            runtime_name: "mlx".to_string(),
            runtime_version: "external".to_string(),
            supported_accelerators: vec![common::v1::AcceleratorType::Metal as i32],
            supported_quantizations: vec![
                "4bit".to_string(),
                "8bit".to_string(),
                "fp16".to_string(),
            ],
            supports_embedding: false,
            supports_streaming: true,
            max_model_size_bytes: 32 * 1024 * 1024 * 1024,
        }
    }

    fn find_variant<'a>(
        &self,
        manifest: &'a common::v1::ModelManifest,
        variant_runtime: &str,
        quantization: &str,
    ) -> Result<&'a common::v1::ModelVariant> {
        manifest
            .variants
            .iter()
            .find(|candidate| {
                candidate.runtime.eq_ignore_ascii_case(variant_runtime)
                    && (quantization.is_empty() || candidate.quantization == quantization)
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
            })
    }

    fn resolve_model_source(&self, manifest: &common::v1::ModelManifest) -> Result<MlxModelSource> {
        if let Some(local_path) = manifest.metadata.get("mlx_local_path") {
            let path = PathBuf::from(local_path);
            let resolved = if path.is_absolute() {
                path
            } else {
                self.models_root
                    .parent()
                    .unwrap_or(&self.models_root)
                    .join(path)
            };
            if resolved.exists() {
                return Ok(MlxModelSource::Local(resolved));
            }
        }

        if let Some(repo) = manifest.metadata.get("mlx_hf_repo") {
            return Ok(MlxModelSource::HuggingFace { repo: repo.clone() });
        }

        Err(GalacticaError::failed_precondition(format!(
            "model {} does not define an mlx_local_path or mlx_hf_repo",
            manifest
                .model_id
                .as_ref()
                .map(|model_id| model_id.value.as_str())
                .unwrap_or("unknown")
        )))
    }

    fn build_process_config(
        &self,
        source: &MlxModelSource,
        port: u16,
        manifest: &common::v1::ModelManifest,
    ) -> RuntimeProcessConfig {
        let mut process = RuntimeProcessConfig::new("mlx", &self.config.python_program);
        process.shutdown_grace_period = Duration::from_secs(5);
        process.health_grace_period = Duration::from_millis(500);
        process.working_directory = Some(self.models_root.clone());
        process.args = vec![
            "-m".to_string(),
            "mlx_lm".to_string(),
            "server".to_string(),
            "--host".to_string(),
            "127.0.0.1".to_string(),
            "--port".to_string(),
            port.to_string(),
            "--model".to_string(),
        ];
        match source {
            MlxModelSource::Local(path) => process.args.push(path.display().to_string()),
            MlxModelSource::HuggingFace { repo } => process.args.push(repo.clone()),
        }
        if manifest
            .metadata
            .get("reasoning_budget")
            .is_some_and(|value| value.trim() == "0")
        {
            process.args.extend([
                "--chat-template-args".to_string(),
                "{\"enable_thinking\":false}".to_string(),
            ]);
        }
        process
    }

    async fn reserve_port(&self) -> Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|error| {
            GalacticaError::internal(format!("failed to reserve port: {error}"))
        })?;
        let port = listener
            .local_addr()
            .map_err(|error| {
                GalacticaError::internal(format!("failed to read reserved port: {error}"))
            })?
            .port();
        drop(listener);
        Ok(port)
    }

    async fn discover_api_model_name(&self, endpoint: &str) -> Result<String> {
        let response = self
            .client
            .get(format!("{endpoint}/v1/models"))
            .send()
            .await
            .map_err(|error| {
                GalacticaError::unavailable(format!("failed to query mlx runtime models: {error}"))
            })?
            .error_for_status()
            .map_err(|error| {
                GalacticaError::unavailable(format!(
                    "mlx runtime returned an error for /v1/models: {error}"
                ))
            })?;
        let payload: OpenAiModelsResponse = response.json().await.map_err(|error| {
            GalacticaError::invalid_argument(format!(
                "invalid mlx /v1/models response payload: {error}"
            ))
        })?;
        payload
            .data
            .into_iter()
            .find(|model| !model.id.trim().is_empty())
            .map(|model| model.id)
            .ok_or_else(|| GalacticaError::unavailable("mlx runtime did not expose any models"))
    }

    async fn wait_for_ready(
        &self,
        endpoint: &str,
        handle: &Arc<Mutex<RuntimeHandle>>,
    ) -> Result<String> {
        let started = Instant::now();
        loop {
            if started.elapsed() > self.config.startup_timeout {
                return Err(GalacticaError::unavailable(format!(
                    "timed out waiting for mlx runtime at {endpoint}"
                )));
            }

            {
                let mut guard = handle.lock().await;
                let health = self.supervisor.health(&mut guard).await?;
                if health.lifecycle_state == RuntimeLifecycleState::Failed {
                    return Err(GalacticaError::unavailable(format!(
                        "mlx runtime failed to start with exit code {:?}",
                        health.last_exit_code
                    )));
                }
            }

            match self.discover_api_model_name(endpoint).await {
                Ok(model_name) => {
                    let mut guard = handle.lock().await;
                    let _ = guard.transition(RuntimeLifecycleState::Ready);
                    return Ok(model_name);
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
    }

    async fn emit_model_loaded(
        &self,
        instance_id: String,
        model_id: String,
        memory_used_bytes: u64,
    ) {
        let _ = self.event_sender.send(runtime::v1::RuntimeEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Some(chrono_to_timestamp(chrono::Utc::now())),
            event: Some(runtime::v1::runtime_event::Event::ModelLoaded(
                runtime::v1::RuntimeModelLoadedEvent {
                    instance_id: Some(common::v1::InstanceId { value: instance_id }),
                    model_id: Some(common::v1::ModelId { value: model_id }),
                    memory_used_bytes,
                },
            )),
        });
    }

    async fn emit_model_unloaded(&self, instance_id: String) {
        let _ = self.event_sender.send(runtime::v1::RuntimeEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Some(chrono_to_timestamp(chrono::Utc::now())),
            event: Some(runtime::v1::runtime_event::Event::ModelUnloaded(
                runtime::v1::RuntimeModelUnloadedEvent {
                    instance_id: Some(common::v1::InstanceId { value: instance_id }),
                },
            )),
        });
    }

    async fn emit_error(&self, message: String, severity: &str) {
        let _ = self.event_sender.send(runtime::v1::RuntimeEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Some(chrono_to_timestamp(chrono::Utc::now())),
            event: Some(runtime::v1::runtime_event::Event::Error(
                runtime::v1::RuntimeErrorEvent {
                    message,
                    severity: severity.to_string(),
                },
            )),
        });
    }

    fn usage_from_response(
        &self,
        response: &OpenAiChatCompletionResponse,
        prompt: &str,
        completion: &str,
        elapsed: Duration,
    ) -> runtime::v1::GenerateUsage {
        let usage = response.usage.as_ref();
        let prompt_tokens = usage
            .and_then(|usage| usage.prompt_tokens)
            .unwrap_or_else(|| estimate_tokens(prompt));
        let completion_tokens = usage
            .and_then(|usage| usage.completion_tokens)
            .unwrap_or_else(|| estimate_tokens(completion));
        let tokens_per_second = completion_tokens as f32 / elapsed.as_secs_f32().max(0.001);
        runtime::v1::GenerateUsage {
            prompt_tokens,
            completion_tokens,
            tokens_per_second,
        }
    }
}

#[derive(Debug, Deserialize)]
struct OpenAiModelsResponse {
    data: Vec<OpenAiModelInfo>,
}

#[derive(Debug, Deserialize)]
struct OpenAiModelInfo {
    id: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiChatCompletionResponse {
    choices: Vec<OpenAiChatChoice>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
}

#[derive(Debug, Deserialize)]
struct OpenAiChatChoice {
    #[serde(default)]
    finish_reason: Option<String>,
    #[serde(default)]
    message: Option<OpenAiChatMessage>,
}

#[derive(Debug, Deserialize)]
struct OpenAiChatMessage {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    reasoning_content: Option<String>,
    #[serde(default)]
    reasoning: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenAiUsage {
    #[serde(default)]
    prompt_tokens: Option<u32>,
    #[serde(default)]
    completion_tokens: Option<u32>,
}

#[async_trait]
impl RuntimeBackend for MlxBackend {
    async fn get_capabilities(&self) -> Result<runtime::v1::GetCapabilitiesResponse> {
        match &self.mode {
            MlxBackendMode::Simulated(inner) => inner.get_capabilities().await,
            MlxBackendMode::Real(inner) => Ok(inner.capabilities()),
        }
    }

    async fn list_models(&self) -> Result<Vec<runtime::v1::RuntimeModelInfo>> {
        match &self.mode {
            MlxBackendMode::Simulated(inner) => inner.list_models().await,
            MlxBackendMode::Real(inner) => {
                let models = inner.loaded_models.read().await;
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
        }
    }

    async fn ensure_model(
        &self,
        manifest: common::v1::ModelManifest,
        variant_runtime: String,
        variant_quantization: String,
    ) -> Result<runtime::v1::EnsureModelResponse> {
        match &self.mode {
            MlxBackendMode::Simulated(inner) => {
                inner
                    .ensure_model(manifest, variant_runtime, variant_quantization)
                    .await
            }
            MlxBackendMode::Real(inner) => {
                let variant =
                    inner.find_variant(&manifest, &variant_runtime, &variant_quantization)?;
                let available = manifest
                    .metadata
                    .get("mlx_local_path")
                    .map(PathBuf::from)
                    .map(|path| {
                        if path.is_absolute() {
                            path.exists()
                        } else {
                            inner
                                .models_root
                                .parent()
                                .unwrap_or(&inner.models_root)
                                .join(path)
                                .exists()
                        }
                    })
                    .unwrap_or(false)
                    || manifest.metadata.contains_key("mlx_hf_repo");
                Ok(runtime::v1::EnsureModelResponse {
                    available,
                    download_required: !manifest.metadata.contains_key("mlx_local_path"),
                    estimated_size_bytes: variant.size_bytes,
                })
            }
        }
    }

    async fn load_model(
        &self,
        request: runtime::v1::LoadRuntimeModelRequest,
    ) -> Result<runtime::v1::LoadRuntimeModelResponse> {
        match &self.mode {
            MlxBackendMode::Simulated(inner) => inner.load_model(request).await,
            MlxBackendMode::Real(inner) => {
                let model_id = request
                    .model_id
                    .as_ref()
                    .map(|model_id| model_id.value.clone())
                    .ok_or_else(|| GalacticaError::invalid_argument("model_id is required"))?;
                let runtime_name = request
                    .runtime_options
                    .get("variant_runtime")
                    .cloned()
                    .unwrap_or_else(|| "mlx".to_string());
                let manifest = inner.registry.get_model_manifest(&model_id).await?;
                let variant =
                    inner.find_variant(&manifest, &runtime_name, &request.quantization)?;
                let source = inner.resolve_model_source(&manifest)?;
                let port = inner.reserve_port().await?;
                let endpoint = format!("http://127.0.0.1:{port}");
                let process_config = inner.build_process_config(&source, port, &manifest);
                let handle = Arc::new(Mutex::new(inner.supervisor.spawn(process_config).await?));
                let api_model_name = inner.wait_for_ready(&endpoint, &handle).await?;

                let instance_id = format!("mlx-{}", uuid::Uuid::new_v4());
                let memory_used_bytes = request
                    .max_memory_bytes
                    .max(variant.size_bytes)
                    .min(32 * 1024 * 1024 * 1024);
                inner.loaded_models.write().await.insert(
                    instance_id.clone(),
                    MlxLoadedModel {
                        instance_id: instance_id.clone(),
                        model_id: model_id.clone(),
                        quantization: variant.quantization.clone(),
                        memory_used_bytes,
                        ready: true,
                        api_model_name,
                        endpoint,
                        handle,
                    },
                );
                inner
                    .emit_model_loaded(instance_id.clone(), model_id, memory_used_bytes)
                    .await;

                Ok(runtime::v1::LoadRuntimeModelResponse {
                    instance_id: Some(common::v1::InstanceId { value: instance_id }),
                    success: true,
                    error_message: String::new(),
                    memory_used_bytes,
                })
            }
        }
    }

    async fn unload_model(
        &self,
        request: runtime::v1::UnloadRuntimeModelRequest,
    ) -> Result<runtime::v1::UnloadRuntimeModelResponse> {
        match &self.mode {
            MlxBackendMode::Simulated(inner) => inner.unload_model(request).await,
            MlxBackendMode::Real(inner) => {
                let instance_id = request
                    .instance_id
                    .as_ref()
                    .map(|instance_id| instance_id.value.clone())
                    .ok_or_else(|| GalacticaError::invalid_argument("instance_id is required"))?;
                let loaded = inner
                    .loaded_models
                    .write()
                    .await
                    .remove(&instance_id)
                    .ok_or_else(|| {
                        GalacticaError::not_found(format!("loaded model not found: {instance_id}"))
                    })?;
                let mut handle = loaded.handle.lock().await;
                inner.supervisor.shutdown(&mut handle).await?;
                drop(handle);
                inner.emit_model_unloaded(instance_id).await;
                Ok(runtime::v1::UnloadRuntimeModelResponse {
                    success: true,
                    error_message: String::new(),
                })
            }
        }
    }

    async fn generate(&self, request: runtime::v1::GenerateRequest) -> Result<GenerateStream> {
        match &self.mode {
            MlxBackendMode::Simulated(inner) => inner.generate(request).await,
            MlxBackendMode::Real(inner) => {
                let instance_id = request
                    .instance_id
                    .as_ref()
                    .map(|instance_id| instance_id.value.clone())
                    .ok_or_else(|| GalacticaError::invalid_argument("instance_id is required"))?;
                let loaded = inner
                    .loaded_models
                    .read()
                    .await
                    .get(&instance_id)
                    .cloned()
                    .ok_or_else(|| {
                        GalacticaError::not_found(format!("loaded model not found: {instance_id}"))
                    })?;
                {
                    let mut handle = loaded.handle.lock().await;
                    let health = inner.supervisor.health(&mut handle).await?;
                    if health.lifecycle_state == RuntimeLifecycleState::Failed {
                        let message = format!("mlx runtime for {} is unhealthy", loaded.model_id);
                        inner.emit_error(message.clone(), "error").await;
                        return Err(GalacticaError::unavailable(message));
                    }
                    let _ = handle.transition(RuntimeLifecycleState::Running);
                }

                let params = request.params.unwrap_or_default();
                let response_started = Instant::now();
                let response = inner
                    .client
                    .post(format!("{}/v1/chat/completions", loaded.endpoint))
                    .json(&serde_json::json!({
                        "model": loaded.api_model_name,
                        "messages": [
                            {
                                "role": "user",
                                "content": request.prompt,
                            }
                        ],
                        "stream": false,
                        "temperature": params.temperature,
                        "top_p": params.top_p,
                        "max_tokens": params.max_tokens,
                        "stop": params.stop,
                    }))
                    .send()
                    .await
                    .map_err(|error| {
                        GalacticaError::unavailable(format!("failed to call mlx runtime: {error}"))
                    })?;
                let response = response.error_for_status().map_err(|error| {
                    GalacticaError::unavailable(format!("mlx runtime returned an error: {error}"))
                })?;
                let completion: OpenAiChatCompletionResponse =
                    response.json().await.map_err(|error| {
                        GalacticaError::invalid_argument(format!(
                            "invalid mlx response payload: {error}"
                        ))
                    })?;
                let (content, finish_reason) = completion
                    .choices
                    .first()
                    .map(|choice| {
                        let content = choice
                            .message
                            .as_ref()
                            .and_then(|message| {
                                message
                                    .content
                                    .clone()
                                    .or_else(|| message.reasoning_content.clone())
                                    .or_else(|| message.reasoning.clone())
                            })
                            .unwrap_or_default();
                        (
                            content,
                            choice
                                .finish_reason
                                .clone()
                                .unwrap_or_else(|| "stop".to_string()),
                        )
                    })
                    .unwrap_or_else(|| (String::new(), "stop".to_string()));
                let usage = inner.usage_from_response(
                    &completion,
                    &request.prompt,
                    &content,
                    response_started.elapsed(),
                );
                let handle = Arc::clone(&loaded.handle);
                let stream = try_stream! {
                    if content.is_empty() {
                        yield runtime::v1::GenerateResponse {
                            text: String::new(),
                            finished: true,
                            finish_reason,
                            usage: Some(usage),
                        };
                    } else {
                        let words: Vec<&str> = content.split_whitespace().collect();
                        for (index, word) in words.iter().enumerate() {
                            let finished = index + 1 == words.len();
                            yield runtime::v1::GenerateResponse {
                                text: if finished {
                                    (*word).to_string()
                                } else {
                                    format!("{word} ")
                                },
                                finished,
                                finish_reason: if finished {
                                    finish_reason.clone()
                                } else {
                                    String::new()
                                },
                                usage: if finished {
                                    Some(usage)
                                } else {
                                    None
                                },
                            };
                        }
                    }
                    let mut guard = handle.lock().await;
                    let _ = guard.transition(RuntimeLifecycleState::Ready);
                };
                Ok(Box::pin(stream) as GenerateStream)
            }
        }
    }

    async fn embed(
        &self,
        request: runtime::v1::EmbedRequest,
    ) -> Result<runtime::v1::EmbedResponse> {
        match &self.mode {
            MlxBackendMode::Simulated(inner) => inner.embed(request).await,
            MlxBackendMode::Real(_) => {
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
        }
    }

    async fn health(&self) -> Result<runtime::v1::HealthResponse> {
        match &self.mode {
            MlxBackendMode::Simulated(inner) => inner.health().await,
            MlxBackendMode::Real(inner) => {
                let models = inner.loaded_models.read().await;
                let mut status = runtime::v1::RuntimeStatus::Healthy;
                for loaded in models.values() {
                    let mut handle = loaded.handle.lock().await;
                    let health = inner.supervisor.health(&mut handle).await?;
                    if health.lifecycle_state == RuntimeLifecycleState::Failed {
                        status = runtime::v1::RuntimeStatus::Unhealthy;
                        break;
                    }
                }
                Ok(runtime::v1::HealthResponse {
                    status: status as i32,
                    runtime_name: "mlx".to_string(),
                    runtime_version: "external".to_string(),
                    uptime_seconds: inner.started_at.elapsed().as_secs(),
                    loaded_model_count: models.len() as u32,
                })
            }
        }
    }

    async fn stream_events(&self) -> Result<EventStream> {
        match &self.mode {
            MlxBackendMode::Simulated(inner) => inner.stream_events().await,
            MlxBackendMode::Real(inner) => {
                let mut receiver = inner.event_sender.subscribe();
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
    }
}

#[derive(Clone)]
pub struct VllmBackend {
    inner: ManagedRuntimeBackend,
}

impl Default for VllmBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl VllmBackend {
    pub fn new() -> Self {
        Self::with_config(VllmBackendConfig::default())
    }

    pub fn with_config(config: VllmBackendConfig) -> Self {
        Self {
            inner: ManagedRuntimeBackend::new(ManagedRuntimeSpec {
                runtime_name: "vllm".to_string(),
                runtime_version: "simulated-0.7".to_string(),
                runtime_aliases: vec!["vllm".to_string()],
                supported_accelerators: vec![common::v1::AcceleratorType::Cuda as i32],
                supported_quantizations: vec![
                    "fp16".to_string(),
                    "bf16".to_string(),
                    "int8".to_string(),
                    "fp8".to_string(),
                ],
                supports_embedding: true,
                supports_streaming: true,
                max_model_size_bytes: 96 * 1024 * 1024 * 1024,
                instance_prefix: "vllm".to_string(),
                synthetic_prefix: "vLLM".to_string(),
                process_config: config.process,
                min_memory_bytes: 2 * 1024 * 1024 * 1024,
                max_memory_bytes: 96 * 1024 * 1024 * 1024,
                default_runtime_options: HashMap::from([(
                    "tensor_parallel_size".to_string(),
                    config.tensor_parallel_size.to_string(),
                )]),
            }),
        }
    }
}

#[async_trait]
impl RuntimeBackend for VllmBackend {
    async fn get_capabilities(&self) -> Result<runtime::v1::GetCapabilitiesResponse> {
        self.inner.get_capabilities().await
    }
    async fn list_models(&self) -> Result<Vec<runtime::v1::RuntimeModelInfo>> {
        self.inner.list_models().await
    }
    async fn ensure_model(
        &self,
        manifest: common::v1::ModelManifest,
        variant_runtime: String,
        variant_quantization: String,
    ) -> Result<runtime::v1::EnsureModelResponse> {
        self.inner
            .ensure_model(manifest, variant_runtime, variant_quantization)
            .await
    }
    async fn load_model(
        &self,
        request: runtime::v1::LoadRuntimeModelRequest,
    ) -> Result<runtime::v1::LoadRuntimeModelResponse> {
        self.inner.load_model(request).await
    }
    async fn unload_model(
        &self,
        request: runtime::v1::UnloadRuntimeModelRequest,
    ) -> Result<runtime::v1::UnloadRuntimeModelResponse> {
        self.inner.unload_model(request).await
    }
    async fn generate(&self, request: runtime::v1::GenerateRequest) -> Result<GenerateStream> {
        self.inner.generate(request).await
    }
    async fn embed(
        &self,
        request: runtime::v1::EmbedRequest,
    ) -> Result<runtime::v1::EmbedResponse> {
        self.inner.embed(request).await
    }
    async fn health(&self) -> Result<runtime::v1::HealthResponse> {
        self.inner.health().await
    }
    async fn stream_events(&self) -> Result<EventStream> {
        self.inner.stream_events().await
    }
}

#[derive(Debug, Clone)]
pub struct LlamaCppBackendConfig {
    pub process: RuntimeProcessConfig,
    pub gpu_layers: String,
    pub context_size: u32,
    pub startup_timeout: Duration,
}

impl Default for LlamaCppBackendConfig {
    fn default() -> Self {
        Self {
            process: simulated_process_config("llama.cpp"),
            gpu_layers: "all".to_string(),
            context_size: 8192,
            startup_timeout: Duration::from_secs(900),
        }
    }
}

#[derive(Debug, Clone)]
struct LlamaCppLoadedModel {
    instance_id: String,
    model_id: String,
    quantization: String,
    memory_used_bytes: u64,
    ready: bool,
    alias: String,
    endpoint: String,
    handle: Arc<Mutex<RuntimeHandle>>,
}

#[derive(Debug, Clone)]
enum LlamaCppModelSource {
    Local(PathBuf),
    HuggingFace { repo: String, file: Option<String> },
}

#[derive(Clone)]
enum LlamaCppBackendMode {
    Simulated(ManagedRuntimeBackend),
    Real(Box<RealLlamaCppBackend>),
}

#[derive(Clone)]
pub struct LlamaCppBackend {
    mode: LlamaCppBackendMode,
}

#[derive(Clone)]
struct RealLlamaCppBackend {
    config: LlamaCppBackendConfig,
    started_at: Instant,
    loaded_models: Arc<RwLock<BTreeMap<String, LlamaCppLoadedModel>>>,
    event_sender: broadcast::Sender<runtime::v1::RuntimeEvent>,
    supervisor: DefaultProcessSupervisor,
    client: reqwest::Client,
    registry: LocalModelRegistry,
    models_root: PathBuf,
}

impl Default for LlamaCppBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl LlamaCppBackend {
    pub fn new() -> Self {
        Self::with_config(LlamaCppBackendConfig::default())
    }

    pub fn with_config(config: LlamaCppBackendConfig) -> Self {
        Self {
            mode: LlamaCppBackendMode::Simulated(ManagedRuntimeBackend::new(ManagedRuntimeSpec {
                runtime_name: "llama.cpp".to_string(),
                runtime_version: "simulated-1.0".to_string(),
                runtime_aliases: vec!["llama.cpp".to_string(), "llamacpp".to_string()],
                supported_accelerators: vec![
                    common::v1::AcceleratorType::Cpu as i32,
                    common::v1::AcceleratorType::Cuda as i32,
                    common::v1::AcceleratorType::Metal as i32,
                ],
                supported_quantizations: vec![
                    "q4_k_m".to_string(),
                    "q5_k_m".to_string(),
                    "q8_0".to_string(),
                    "fp16".to_string(),
                ],
                supports_embedding: true,
                supports_streaming: true,
                max_model_size_bytes: 32 * 1024 * 1024 * 1024,
                instance_prefix: "llama".to_string(),
                synthetic_prefix: "llama.cpp".to_string(),
                process_config: config.process,
                min_memory_bytes: 512 * 1024 * 1024,
                max_memory_bytes: 32 * 1024 * 1024 * 1024,
                default_runtime_options: HashMap::from([(
                    "gpu_layers".to_string(),
                    config.gpu_layers.clone(),
                )]),
            })),
        }
    }

    pub fn with_model_registry(
        config: LlamaCppBackendConfig,
        models_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            mode: LlamaCppBackendMode::Real(Box::new(RealLlamaCppBackend::new(
                config,
                models_root.into(),
            ))),
        }
    }
}

impl RealLlamaCppBackend {
    fn new(config: LlamaCppBackendConfig, models_root: PathBuf) -> Self {
        let (event_sender, _) = broadcast::channel(64);
        Self {
            config,
            started_at: Instant::now(),
            loaded_models: Arc::new(RwLock::new(BTreeMap::new())),
            event_sender,
            supervisor: DefaultProcessSupervisor,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(600))
                .build()
                .expect("reqwest client should initialize"),
            registry: LocalModelRegistry::new(&models_root),
            models_root,
        }
    }

    fn capabilities(&self) -> runtime::v1::GetCapabilitiesResponse {
        runtime::v1::GetCapabilitiesResponse {
            runtime_name: "llama.cpp".to_string(),
            runtime_version: "external".to_string(),
            supported_accelerators: vec![
                common::v1::AcceleratorType::Cpu as i32,
                common::v1::AcceleratorType::Cuda as i32,
                common::v1::AcceleratorType::Metal as i32,
            ],
            supported_quantizations: vec![
                "q4_k_m".to_string(),
                "q5_k_m".to_string(),
                "q8_0".to_string(),
                "fp16".to_string(),
            ],
            supports_embedding: false,
            supports_streaming: true,
            max_model_size_bytes: 32 * 1024 * 1024 * 1024,
        }
    }

    fn find_variant<'a>(
        &self,
        manifest: &'a common::v1::ModelManifest,
        variant_runtime: &str,
        quantization: &str,
    ) -> Result<&'a common::v1::ModelVariant> {
        manifest
            .variants
            .iter()
            .find(|candidate| {
                candidate.runtime.eq_ignore_ascii_case(variant_runtime)
                    && (quantization.is_empty() || candidate.quantization == quantization)
            })
            .ok_or_else(|| {
                GalacticaError::failed_precondition(format!(
                    "no llama.cpp variant available for {}",
                    manifest
                        .model_id
                        .as_ref()
                        .map(|model_id| model_id.value.as_str())
                        .unwrap_or("unknown")
                ))
            })
    }

    fn resolve_model_source(
        &self,
        manifest: &common::v1::ModelManifest,
        quantization: &str,
    ) -> Result<LlamaCppModelSource> {
        if let Some(local_path) = manifest.metadata.get("local_path") {
            let path = PathBuf::from(local_path);
            let resolved = if path.is_absolute() {
                path
            } else {
                self.models_root
                    .parent()
                    .unwrap_or(&self.models_root)
                    .join(path)
            };
            if resolved.exists() {
                return Ok(LlamaCppModelSource::Local(resolved));
            }
        }

        if let Some(repo) = manifest.metadata.get("llama_cpp_hf_repo") {
            let repo = if repo.contains(':') || quantization.is_empty() {
                repo.clone()
            } else {
                format!("{repo}:{quantization}")
            };
            return Ok(LlamaCppModelSource::HuggingFace {
                repo,
                file: manifest.metadata.get("llama_cpp_hf_file").cloned(),
            });
        }

        Err(GalacticaError::failed_precondition(format!(
            "model {} does not define a local_path or llama_cpp_hf_repo",
            manifest
                .model_id
                .as_ref()
                .map(|model_id| model_id.value.as_str())
                .unwrap_or("unknown")
        )))
    }

    fn build_process_config(
        &self,
        source: &LlamaCppModelSource,
        alias: &str,
        port: u16,
        manifest: &common::v1::ModelManifest,
    ) -> RuntimeProcessConfig {
        let mut process = RuntimeProcessConfig::new("llama.cpp", "llama-server");
        process.shutdown_grace_period = Duration::from_secs(5);
        process.health_grace_period = Duration::from_millis(500);
        process.working_directory = Some(self.models_root.clone());
        process.args = vec![
            "--host".to_string(),
            "127.0.0.1".to_string(),
            "--port".to_string(),
            port.to_string(),
            "--alias".to_string(),
            alias.to_string(),
            "--ctx-size".to_string(),
            manifest
                .metadata
                .get("context_size")
                .cloned()
                .unwrap_or_else(|| self.config.context_size.to_string()),
            "--parallel".to_string(),
            "1".to_string(),
            "--reasoning-format".to_string(),
            "none".to_string(),
            "--reasoning-budget".to_string(),
            manifest
                .metadata
                .get("reasoning_budget")
                .cloned()
                .unwrap_or_else(|| "0".to_string()),
            "--no-webui".to_string(),
            "--jinja".to_string(),
        ];
        if !self.config.gpu_layers.trim().is_empty() {
            process
                .args
                .extend(["--gpu-layers".to_string(), self.config.gpu_layers.clone()]);
        }
        match source {
            LlamaCppModelSource::Local(path) => process
                .args
                .extend(["--model".to_string(), path.display().to_string()]),
            LlamaCppModelSource::HuggingFace { repo, file } => {
                process.args.extend(["--hf-repo".to_string(), repo.clone()]);
                if let Some(file) = file {
                    process.args.extend(["--hf-file".to_string(), file.clone()]);
                }
            }
        }
        process
    }

    async fn reserve_port(&self) -> Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|error| {
            GalacticaError::internal(format!("failed to reserve port: {error}"))
        })?;
        let port = listener
            .local_addr()
            .map_err(|error| {
                GalacticaError::internal(format!("failed to read reserved port: {error}"))
            })?
            .port();
        drop(listener);
        Ok(port)
    }

    async fn wait_for_ready(
        &self,
        endpoint: &str,
        handle: &Arc<Mutex<RuntimeHandle>>,
    ) -> Result<()> {
        let started = Instant::now();
        let health_url = format!("{endpoint}/health");
        loop {
            if started.elapsed() > self.config.startup_timeout {
                return Err(GalacticaError::unavailable(format!(
                    "timed out waiting for llama.cpp runtime at {endpoint}"
                )));
            }

            {
                let mut guard = handle.lock().await;
                let health = self.supervisor.health(&mut guard).await?;
                if health.lifecycle_state == RuntimeLifecycleState::Failed {
                    return Err(GalacticaError::unavailable(format!(
                        "llama.cpp runtime failed to start with exit code {:?}",
                        health.last_exit_code
                    )));
                }
            }

            match self.client.get(&health_url).send().await {
                Ok(response) if response.status().is_success() => {
                    let mut guard = handle.lock().await;
                    let _ = guard.transition(RuntimeLifecycleState::Ready);
                    return Ok(());
                }
                Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
    }

    async fn emit_model_loaded(
        &self,
        instance_id: String,
        model_id: String,
        memory_used_bytes: u64,
    ) {
        let _ = self.event_sender.send(runtime::v1::RuntimeEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Some(chrono_to_timestamp(chrono::Utc::now())),
            event: Some(runtime::v1::runtime_event::Event::ModelLoaded(
                runtime::v1::RuntimeModelLoadedEvent {
                    instance_id: Some(common::v1::InstanceId { value: instance_id }),
                    model_id: Some(common::v1::ModelId { value: model_id }),
                    memory_used_bytes,
                },
            )),
        });
    }

    async fn emit_model_unloaded(&self, instance_id: String) {
        let _ = self.event_sender.send(runtime::v1::RuntimeEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Some(chrono_to_timestamp(chrono::Utc::now())),
            event: Some(runtime::v1::runtime_event::Event::ModelUnloaded(
                runtime::v1::RuntimeModelUnloadedEvent {
                    instance_id: Some(common::v1::InstanceId { value: instance_id }),
                },
            )),
        });
    }

    async fn emit_error(&self, message: String, severity: &str) {
        let _ = self.event_sender.send(runtime::v1::RuntimeEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            timestamp: Some(chrono_to_timestamp(chrono::Utc::now())),
            event: Some(runtime::v1::runtime_event::Event::Error(
                runtime::v1::RuntimeErrorEvent {
                    message,
                    severity: severity.to_string(),
                },
            )),
        });
    }

    fn usage_from_response(
        &self,
        response: &LlamaCppChatCompletionResponse,
        prompt: &str,
        completion: &str,
        elapsed: Duration,
    ) -> runtime::v1::GenerateUsage {
        let usage = response.usage.as_ref();
        let prompt_tokens = usage
            .and_then(|usage| usage.prompt_tokens)
            .unwrap_or_else(|| estimate_tokens(prompt));
        let completion_tokens = usage
            .and_then(|usage| usage.completion_tokens)
            .unwrap_or_else(|| estimate_tokens(completion));
        let tokens_per_second = completion_tokens as f32 / elapsed.as_secs_f32().max(0.001);
        runtime::v1::GenerateUsage {
            prompt_tokens,
            completion_tokens,
            tokens_per_second,
        }
    }
}

#[derive(Debug, Deserialize)]
struct LlamaCppChatCompletionResponse {
    choices: Vec<LlamaCppChatChoice>,
    #[serde(default)]
    usage: Option<LlamaCppUsage>,
}

#[derive(Debug, Deserialize)]
struct LlamaCppChatChoice {
    #[serde(default)]
    finish_reason: Option<String>,
    #[serde(default)]
    message: Option<LlamaCppChatMessage>,
}

#[derive(Debug, Deserialize)]
struct LlamaCppChatMessage {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    reasoning_content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LlamaCppUsage {
    #[serde(default)]
    prompt_tokens: Option<u32>,
    #[serde(default)]
    completion_tokens: Option<u32>,
}

fn sanitize_alias(model_id: &str) -> String {
    let alias = model_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    alias.trim_matches('-').to_string()
}

#[async_trait]
impl RuntimeBackend for LlamaCppBackend {
    async fn get_capabilities(&self) -> Result<runtime::v1::GetCapabilitiesResponse> {
        match &self.mode {
            LlamaCppBackendMode::Simulated(inner) => inner.get_capabilities().await,
            LlamaCppBackendMode::Real(inner) => Ok(inner.capabilities()),
        }
    }
    async fn list_models(&self) -> Result<Vec<runtime::v1::RuntimeModelInfo>> {
        match &self.mode {
            LlamaCppBackendMode::Simulated(inner) => inner.list_models().await,
            LlamaCppBackendMode::Real(inner) => {
                let models = inner.loaded_models.read().await;
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
        }
    }
    async fn ensure_model(
        &self,
        manifest: common::v1::ModelManifest,
        variant_runtime: String,
        variant_quantization: String,
    ) -> Result<runtime::v1::EnsureModelResponse> {
        match &self.mode {
            LlamaCppBackendMode::Simulated(inner) => {
                inner
                    .ensure_model(manifest, variant_runtime, variant_quantization)
                    .await
            }
            LlamaCppBackendMode::Real(inner) => {
                let variant =
                    inner.find_variant(&manifest, &variant_runtime, &variant_quantization)?;
                let available = manifest
                    .metadata
                    .get("local_path")
                    .map(PathBuf::from)
                    .map(|path| {
                        if path.is_absolute() {
                            path.exists()
                        } else {
                            inner
                                .models_root
                                .parent()
                                .unwrap_or(&inner.models_root)
                                .join(path)
                                .exists()
                        }
                    })
                    .unwrap_or(false)
                    || manifest.metadata.contains_key("llama_cpp_hf_repo");
                Ok(runtime::v1::EnsureModelResponse {
                    available,
                    download_required: !manifest.metadata.contains_key("local_path"),
                    estimated_size_bytes: variant.size_bytes,
                })
            }
        }
    }
    async fn load_model(
        &self,
        request: runtime::v1::LoadRuntimeModelRequest,
    ) -> Result<runtime::v1::LoadRuntimeModelResponse> {
        match &self.mode {
            LlamaCppBackendMode::Simulated(inner) => inner.load_model(request).await,
            LlamaCppBackendMode::Real(inner) => {
                let model_id = request
                    .model_id
                    .as_ref()
                    .map(|model_id| model_id.value.clone())
                    .ok_or_else(|| GalacticaError::invalid_argument("model_id is required"))?;
                let runtime_name = request
                    .runtime_options
                    .get("variant_runtime")
                    .cloned()
                    .unwrap_or_else(|| "llama.cpp".to_string());
                let manifest = inner.registry.get_model_manifest(&model_id).await?;
                let variant =
                    inner.find_variant(&manifest, &runtime_name, &request.quantization)?;
                let source = inner.resolve_model_source(&manifest, &variant.quantization)?;
                let port = inner.reserve_port().await?;
                let endpoint = format!("http://127.0.0.1:{port}");
                let alias = sanitize_alias(&model_id);
                let process_config = inner.build_process_config(&source, &alias, port, &manifest);
                let handle = Arc::new(Mutex::new(inner.supervisor.spawn(process_config).await?));
                inner.wait_for_ready(&endpoint, &handle).await?;

                let instance_id = format!("llama-{}", uuid::Uuid::new_v4());
                let memory_used_bytes = request
                    .max_memory_bytes
                    .max(variant.size_bytes)
                    .min(32 * 1024 * 1024 * 1024);
                inner.loaded_models.write().await.insert(
                    instance_id.clone(),
                    LlamaCppLoadedModel {
                        instance_id: instance_id.clone(),
                        model_id: model_id.clone(),
                        quantization: variant.quantization.clone(),
                        memory_used_bytes,
                        ready: true,
                        alias,
                        endpoint,
                        handle,
                    },
                );
                inner
                    .emit_model_loaded(instance_id.clone(), model_id, memory_used_bytes)
                    .await;

                Ok(runtime::v1::LoadRuntimeModelResponse {
                    instance_id: Some(common::v1::InstanceId { value: instance_id }),
                    success: true,
                    error_message: String::new(),
                    memory_used_bytes,
                })
            }
        }
    }
    async fn unload_model(
        &self,
        request: runtime::v1::UnloadRuntimeModelRequest,
    ) -> Result<runtime::v1::UnloadRuntimeModelResponse> {
        match &self.mode {
            LlamaCppBackendMode::Simulated(inner) => inner.unload_model(request).await,
            LlamaCppBackendMode::Real(inner) => {
                let instance_id = request
                    .instance_id
                    .as_ref()
                    .map(|instance_id| instance_id.value.clone())
                    .ok_or_else(|| GalacticaError::invalid_argument("instance_id is required"))?;
                let loaded = inner
                    .loaded_models
                    .write()
                    .await
                    .remove(&instance_id)
                    .ok_or_else(|| {
                        GalacticaError::not_found(format!("loaded model not found: {instance_id}"))
                    })?;
                let mut handle = loaded.handle.lock().await;
                inner.supervisor.shutdown(&mut handle).await?;
                drop(handle);
                inner.emit_model_unloaded(instance_id).await;
                Ok(runtime::v1::UnloadRuntimeModelResponse {
                    success: true,
                    error_message: String::new(),
                })
            }
        }
    }
    async fn generate(&self, request: runtime::v1::GenerateRequest) -> Result<GenerateStream> {
        match &self.mode {
            LlamaCppBackendMode::Simulated(inner) => inner.generate(request).await,
            LlamaCppBackendMode::Real(inner) => {
                let instance_id = request
                    .instance_id
                    .as_ref()
                    .map(|instance_id| instance_id.value.clone())
                    .ok_or_else(|| GalacticaError::invalid_argument("instance_id is required"))?;
                let loaded = inner
                    .loaded_models
                    .read()
                    .await
                    .get(&instance_id)
                    .cloned()
                    .ok_or_else(|| {
                        GalacticaError::not_found(format!("loaded model not found: {instance_id}"))
                    })?;
                {
                    let mut handle = loaded.handle.lock().await;
                    let health = inner.supervisor.health(&mut handle).await?;
                    if health.lifecycle_state == RuntimeLifecycleState::Failed {
                        let message =
                            format!("llama.cpp runtime for {} is unhealthy", loaded.model_id);
                        inner.emit_error(message.clone(), "error").await;
                        return Err(GalacticaError::unavailable(message));
                    }
                    let _ = handle.transition(RuntimeLifecycleState::Running);
                }

                let params = request.params.unwrap_or_default();
                let response_started = Instant::now();
                let response = inner
                    .client
                    .post(format!("{}/v1/chat/completions", loaded.endpoint))
                    .json(&serde_json::json!({
                        "model": loaded.alias,
                        "messages": [
                            {
                                "role": "user",
                                "content": request.prompt,
                            }
                        ],
                        "stream": false,
                        "temperature": params.temperature,
                        "top_p": params.top_p,
                        "max_tokens": params.max_tokens,
                        "stop": params.stop,
                    }))
                    .send()
                    .await
                    .map_err(|error| {
                        GalacticaError::unavailable(format!(
                            "failed to call llama.cpp runtime: {error}"
                        ))
                    })?;
                let response = response.error_for_status().map_err(|error| {
                    GalacticaError::unavailable(format!(
                        "llama.cpp runtime returned an error: {error}"
                    ))
                })?;
                let completion: LlamaCppChatCompletionResponse =
                    response.json().await.map_err(|error| {
                        GalacticaError::invalid_argument(format!(
                            "invalid llama.cpp response payload: {error}"
                        ))
                    })?;
                let (content, finish_reason) = completion
                    .choices
                    .first()
                    .map(|choice| {
                        let content = choice
                            .message
                            .as_ref()
                            .and_then(|message| {
                                message
                                    .content
                                    .clone()
                                    .or_else(|| message.reasoning_content.clone())
                            })
                            .unwrap_or_default();
                        (
                            content,
                            choice
                                .finish_reason
                                .clone()
                                .unwrap_or_else(|| "stop".to_string()),
                        )
                    })
                    .unwrap_or_else(|| (String::new(), "stop".to_string()));
                let usage = inner.usage_from_response(
                    &completion,
                    &request.prompt,
                    &content,
                    response_started.elapsed(),
                );
                let handle = Arc::clone(&loaded.handle);
                let stream = try_stream! {
                    if content.is_empty() {
                        yield runtime::v1::GenerateResponse {
                            text: String::new(),
                            finished: true,
                            finish_reason,
                            usage: Some(usage),
                        };
                    } else {
                        let words: Vec<&str> = content.split_whitespace().collect();
                        for (index, word) in words.iter().enumerate() {
                            let finished = index + 1 == words.len();
                            yield runtime::v1::GenerateResponse {
                                text: if finished {
                                    (*word).to_string()
                                } else {
                                    format!("{word} ")
                                },
                                finished,
                                finish_reason: if finished {
                                    finish_reason.clone()
                                } else {
                                    String::new()
                                },
                                usage: if finished {
                                    Some(usage)
                                } else {
                                    None
                                },
                            };
                        }
                    }
                    let mut guard = handle.lock().await;
                    let _ = guard.transition(RuntimeLifecycleState::Ready);
                };
                Ok(Box::pin(stream) as GenerateStream)
            }
        }
    }
    async fn embed(
        &self,
        request: runtime::v1::EmbedRequest,
    ) -> Result<runtime::v1::EmbedResponse> {
        match &self.mode {
            LlamaCppBackendMode::Simulated(inner) => inner.embed(request).await,
            LlamaCppBackendMode::Real(_) => {
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
        }
    }
    async fn health(&self) -> Result<runtime::v1::HealthResponse> {
        match &self.mode {
            LlamaCppBackendMode::Simulated(inner) => inner.health().await,
            LlamaCppBackendMode::Real(inner) => {
                let models = inner.loaded_models.read().await;
                let mut status = runtime::v1::RuntimeStatus::Healthy;
                for loaded in models.values() {
                    let mut handle = loaded.handle.lock().await;
                    let health = inner.supervisor.health(&mut handle).await?;
                    if health.lifecycle_state == RuntimeLifecycleState::Failed {
                        status = runtime::v1::RuntimeStatus::Unhealthy;
                        break;
                    }
                }
                Ok(runtime::v1::HealthResponse {
                    status: status as i32,
                    runtime_name: "llama.cpp".to_string(),
                    runtime_version: "external".to_string(),
                    uptime_seconds: inner.started_at.elapsed().as_secs(),
                    loaded_model_count: models.len() as u32,
                })
            }
        }
    }
    async fn stream_events(&self) -> Result<EventStream> {
        match &self.mode {
            LlamaCppBackendMode::Simulated(inner) => inner.stream_events().await,
            LlamaCppBackendMode::Real(inner) => {
                let mut receiver = inner.event_sender.subscribe();
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
    }
}

#[derive(Debug, Clone)]
pub struct OnnxBackendConfig {
    pub process: RuntimeProcessConfig,
    pub execution_provider: String,
}

impl Default for OnnxBackendConfig {
    fn default() -> Self {
        Self {
            process: simulated_process_config("onnxruntime"),
            execution_provider: "cpu".to_string(),
        }
    }
}

#[derive(Clone)]
pub struct OnnxBackend {
    inner: ManagedRuntimeBackend,
}

impl Default for OnnxBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl OnnxBackend {
    pub fn new() -> Self {
        Self::with_config(OnnxBackendConfig::default())
    }

    pub fn with_config(config: OnnxBackendConfig) -> Self {
        Self {
            inner: ManagedRuntimeBackend::new(ManagedRuntimeSpec {
                runtime_name: "onnxruntime".to_string(),
                runtime_version: "simulated-1.18".to_string(),
                runtime_aliases: vec!["onnxruntime".to_string(), "onnx".to_string()],
                supported_accelerators: vec![
                    common::v1::AcceleratorType::Cpu as i32,
                    common::v1::AcceleratorType::Cuda as i32,
                    common::v1::AcceleratorType::Directml as i32,
                ],
                supported_quantizations: vec![
                    "fp32".to_string(),
                    "fp16".to_string(),
                    "int8".to_string(),
                ],
                supports_embedding: true,
                supports_streaming: true,
                max_model_size_bytes: 48 * 1024 * 1024 * 1024,
                instance_prefix: "onnx".to_string(),
                synthetic_prefix: "ONNX Runtime".to_string(),
                process_config: config.process,
                min_memory_bytes: 1024 * 1024 * 1024,
                max_memory_bytes: 48 * 1024 * 1024 * 1024,
                default_runtime_options: HashMap::from([(
                    "execution_provider".to_string(),
                    config.execution_provider,
                )]),
            }),
        }
    }
}

#[async_trait]
impl RuntimeBackend for OnnxBackend {
    async fn get_capabilities(&self) -> Result<runtime::v1::GetCapabilitiesResponse> {
        self.inner.get_capabilities().await
    }
    async fn list_models(&self) -> Result<Vec<runtime::v1::RuntimeModelInfo>> {
        self.inner.list_models().await
    }
    async fn ensure_model(
        &self,
        manifest: common::v1::ModelManifest,
        variant_runtime: String,
        variant_quantization: String,
    ) -> Result<runtime::v1::EnsureModelResponse> {
        self.inner
            .ensure_model(manifest, variant_runtime, variant_quantization)
            .await
    }
    async fn load_model(
        &self,
        request: runtime::v1::LoadRuntimeModelRequest,
    ) -> Result<runtime::v1::LoadRuntimeModelResponse> {
        self.inner.load_model(request).await
    }
    async fn unload_model(
        &self,
        request: runtime::v1::UnloadRuntimeModelRequest,
    ) -> Result<runtime::v1::UnloadRuntimeModelResponse> {
        self.inner.unload_model(request).await
    }
    async fn generate(&self, request: runtime::v1::GenerateRequest) -> Result<GenerateStream> {
        self.inner.generate(request).await
    }
    async fn embed(
        &self,
        request: runtime::v1::EmbedRequest,
    ) -> Result<runtime::v1::EmbedResponse> {
        self.inner.embed(request).await
    }
    async fn health(&self) -> Result<runtime::v1::HealthResponse> {
        self.inner.health().await
    }
    async fn stream_events(&self) -> Result<EventStream> {
        self.inner.stream_events().await
    }
}

fn simulated_process_config(runtime_name: &str) -> RuntimeProcessConfig {
    let mut process = RuntimeProcessConfig::new(runtime_name, shell_program());
    process.args = shell_args(shell_sleep_command());
    process.shutdown_grace_period = Duration::from_millis(100);
    process.health_grace_period = Duration::from_millis(10);
    process.env.insert(
        "GALACTICA_RUNTIME_NAME".to_string(),
        runtime_name.to_string(),
    );
    process
}

#[cfg(unix)]
fn shell_program() -> String {
    "sh".to_string()
}

#[cfg(unix)]
fn shell_args(script: &str) -> Vec<String> {
    vec!["-c".to_string(), script.to_string()]
}

#[cfg(unix)]
fn shell_sleep_command() -> &'static str {
    "sleep 60"
}

#[cfg(windows)]
fn shell_program() -> String {
    "cmd".to_string()
}

#[cfg(windows)]
fn shell_args(script: &str) -> Vec<String> {
    vec!["/C".to_string(), script.to_string()]
}

#[cfg(windows)]
fn shell_sleep_command() -> &'static str {
    "ping -n 60 127.0.0.1 >NUL"
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio_stream::StreamExt;

    use super::*;

    fn sample_manifest(runtime_name: &str, quantization: &str) -> common::v1::ModelManifest {
        common::v1::ModelManifest {
            model_id: Some(common::v1::ModelId {
                value: format!("{runtime_name}-model"),
            }),
            name: format!("{runtime_name} model"),
            family: "chat".to_string(),
            variants: vec![common::v1::ModelVariant {
                runtime: runtime_name.to_string(),
                quantization: quantization.to_string(),
                format: "safetensors".to_string(),
                size_bytes: 4 * 1024 * 1024 * 1024,
                compatible_accelerators: vec![],
            }],
            chat_template: "default".to_string(),
            metadata: HashMap::from([(
                "download_url".to_string(),
                format!("https://example.test/{runtime_name}"),
            )]),
        }
    }

    #[tokio::test]
    async fn vllm_backend_reports_cuda_and_process_health() {
        let backend = VllmBackend::new();
        let capabilities = backend.get_capabilities().await.unwrap();
        assert_eq!(capabilities.runtime_name, "vllm");
        assert_eq!(
            capabilities.supported_accelerators,
            vec![common::v1::AcceleratorType::Cuda as i32]
        );
        let ensure = backend
            .ensure_model(
                sample_manifest("vllm", "fp16"),
                "vllm".to_string(),
                "fp16".to_string(),
            )
            .await
            .unwrap();
        assert!(ensure.download_required);

        let loaded = backend
            .load_model(runtime::v1::LoadRuntimeModelRequest {
                model_id: Some(common::v1::ModelId {
                    value: "vllm-model".to_string(),
                }),
                quantization: "fp16".to_string(),
                max_memory_bytes: 8 * 1024 * 1024 * 1024,
                runtime_options: HashMap::from([(
                    "tensor_parallel_size".to_string(),
                    "2".to_string(),
                )]),
            })
            .await
            .unwrap();
        assert!(loaded.memory_used_bytes >= 16 * 1024 * 1024 * 1024);

        let health = backend.health().await.unwrap();
        assert_eq!(health.status, runtime::v1::RuntimeStatus::Healthy as i32);
        assert_eq!(health.loaded_model_count, 1);

        backend
            .unload_model(runtime::v1::UnloadRuntimeModelRequest {
                instance_id: loaded.instance_id,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn llama_cpp_backend_generates_streaming_tokens() {
        let backend = LlamaCppBackend::new();
        let loaded = backend
            .load_model(runtime::v1::LoadRuntimeModelRequest {
                model_id: Some(common::v1::ModelId {
                    value: "llama-model".to_string(),
                }),
                quantization: "q4_k_m".to_string(),
                max_memory_bytes: 2 * 1024 * 1024 * 1024,
                runtime_options: HashMap::new(),
            })
            .await
            .unwrap();
        let instance_id = loaded.instance_id.unwrap();

        let mut stream = backend
            .generate(runtime::v1::GenerateRequest {
                instance_id: Some(instance_id.clone()),
                prompt: "user: explain cache locality".to_string(),
                params: Some(runtime::v1::GenerateParams {
                    temperature: 0.2,
                    top_p: 1.0,
                    max_tokens: 32,
                    stop: Vec::new(),
                }),
            })
            .await
            .unwrap();
        let mut output = String::new();
        while let Some(chunk) = stream.next().await {
            output.push_str(&chunk.unwrap().text);
        }
        assert!(output.contains("cache locality"));
        assert!(output.contains("llama.cpp"));

        backend
            .unload_model(runtime::v1::UnloadRuntimeModelRequest {
                instance_id: Some(instance_id),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn onnx_backend_embeds_and_accepts_aliases() {
        let backend = OnnxBackend::with_config(OnnxBackendConfig {
            process: simulated_process_config("onnxruntime"),
            execution_provider: "directml".to_string(),
        });
        let ensure = backend
            .ensure_model(
                sample_manifest("onnxruntime", "int8"),
                "onnx".to_string(),
                "int8".to_string(),
            )
            .await
            .unwrap();
        assert!(ensure.available);

        let loaded = backend
            .load_model(runtime::v1::LoadRuntimeModelRequest {
                model_id: Some(common::v1::ModelId {
                    value: "vision-model".to_string(),
                }),
                quantization: "int8".to_string(),
                max_memory_bytes: 4 * 1024 * 1024 * 1024,
                runtime_options: HashMap::from([(
                    "execution_provider".to_string(),
                    "directml".to_string(),
                )]),
            })
            .await
            .unwrap();
        let embedded = backend
            .embed(runtime::v1::EmbedRequest {
                instance_id: loaded.instance_id.clone(),
                inputs: vec!["hello".to_string(), "world".to_string()],
            })
            .await
            .unwrap();
        assert_eq!(embedded.embeddings.len(), 2);

        let health = backend.health().await.unwrap();
        assert_eq!(health.runtime_name, "onnxruntime");
        assert_eq!(health.status, runtime::v1::RuntimeStatus::Healthy as i32);
    }
}
