pub mod download;

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use galactica_common::proto::{artifact, common};
use galactica_common::{GalacticaError, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, broadcast};
use tonic::{Request, Response, Status};

pub use artifact::v1::artifact_service_server::{ArtifactService, ArtifactServiceServer};
pub use download::{
    ArtifactDownloadSource, ArtifactFetcher, CacheEntry, CacheKey, DownloadManager,
    DownloadRequest, LocalCache, sha256_digest, verify_content_hash,
};

type DownloadStream = Pin<
    Box<
        dyn futures::Stream<Item = std::result::Result<artifact::v1::DownloadProgress, Status>>
            + Send,
    >,
>;

#[async_trait]
pub trait ModelRegistry: Send + Sync {
    async fn get_model_manifest(&self, model_id: &str) -> Result<common::v1::ModelManifest>;

    async fn list_models(
        &self,
        filter_runtime: Option<&str>,
        filter_family: Option<&str>,
    ) -> Result<Vec<common::v1::ModelManifest>>;
}

#[derive(Debug, Clone)]
pub struct LocalModelRegistry {
    root: PathBuf,
}

impl LocalModelRegistry {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn manifest_paths(root: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
        if !root.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(root).map_err(|error| {
            GalacticaError::internal(format!("failed to read cache root: {error}"))
        })? {
            let entry = entry.map_err(|error| {
                GalacticaError::internal(format!("failed to read cache entry: {error}"))
            })?;
            let path = entry.path();
            if path.is_dir() {
                Self::manifest_paths(&path, output)?;
            } else if path.file_name().and_then(|name| name.to_str()) == Some("manifest.json") {
                output.push(path);
            }
        }

        Ok(())
    }

    fn read_manifest(path: &Path) -> Result<common::v1::ModelManifest> {
        let contents = std::fs::read(path).map_err(|error| {
            GalacticaError::internal(format!(
                "failed to read manifest {}: {error}",
                path.display()
            ))
        })?;
        let manifest: StoredManifest = serde_json::from_slice(&contents).map_err(|error| {
            GalacticaError::invalid_argument(format!(
                "invalid manifest {}: {error}",
                path.display()
            ))
        })?;
        validate_manifest(manifest.into_manifest())
    }
}

#[async_trait]
impl ModelRegistry for LocalModelRegistry {
    async fn get_model_manifest(&self, model_id: &str) -> Result<common::v1::ModelManifest> {
        let manifests = self.list_models(None, None).await?;
        manifests
            .into_iter()
            .find(|manifest| manifest_id(manifest) == model_id)
            .ok_or_else(|| {
                GalacticaError::not_found(format!("model manifest not found: {model_id}"))
            })
    }

    async fn list_models(
        &self,
        filter_runtime: Option<&str>,
        filter_family: Option<&str>,
    ) -> Result<Vec<common::v1::ModelManifest>> {
        let mut paths = Vec::new();
        Self::manifest_paths(&self.root, &mut paths)?;

        let mut manifests = Vec::new();
        for path in paths {
            let manifest = Self::read_manifest(&path)?;
            if !matches_runtime(&manifest, filter_runtime)
                || !matches_family(&manifest, filter_family)
            {
                continue;
            }
            manifests.push(manifest);
        }

        manifests.sort_by_key(manifest_id);
        Ok(manifests)
    }
}

#[derive(Debug, Clone)]
pub struct HuggingFaceRegistry {
    base_url: String,
    client: reqwest::Client,
}

impl Default for HuggingFaceRegistry {
    fn default() -> Self {
        Self::new("https://huggingface.co")
    }
}

impl HuggingFaceRegistry {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct HuggingFaceModelResponse {
    id: String,
    #[serde(default)]
    pipeline_tag: Option<String>,
    #[serde(default)]
    sha: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredManifest {
    model_id: String,
    name: String,
    family: String,
    variants: Vec<StoredVariant>,
    chat_template: String,
    #[serde(default)]
    metadata: HashMap<String, String>,
}

impl StoredManifest {
    fn into_manifest(self) -> common::v1::ModelManifest {
        common::v1::ModelManifest {
            model_id: Some(common::v1::ModelId {
                value: self.model_id,
            }),
            name: self.name,
            family: self.family,
            variants: self
                .variants
                .into_iter()
                .map(StoredVariant::into_variant)
                .collect(),
            chat_template: self.chat_template,
            metadata: self.metadata,
        }
    }
}

impl From<&common::v1::ModelManifest> for StoredManifest {
    fn from(value: &common::v1::ModelManifest) -> Self {
        Self {
            model_id: manifest_id(value),
            name: value.name.clone(),
            family: value.family.clone(),
            variants: value.variants.iter().map(StoredVariant::from).collect(),
            chat_template: value.chat_template.clone(),
            metadata: value.metadata.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredVariant {
    runtime: String,
    quantization: String,
    format: String,
    size_bytes: u64,
    compatible_accelerators: Vec<i32>,
    #[serde(default)]
    distributed: Option<StoredDistributedExecutionPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredDistributedExecutionPolicy {
    backend_family: String,
    min_shards: u32,
    max_shards: u32,
    preferred_shards: u32,
    per_shard_overhead_bytes: u64,
    requires_homogeneous_backend: bool,
    supports_generation: bool,
    supports_embedding: bool,
}

impl StoredVariant {
    fn into_variant(self) -> common::v1::ModelVariant {
        common::v1::ModelVariant {
            runtime: self.runtime,
            quantization: self.quantization,
            format: self.format,
            size_bytes: self.size_bytes,
            compatible_accelerators: self.compatible_accelerators,
            distributed: self.distributed.map(|distributed| {
                common::v1::DistributedExecutionPolicy {
                    backend_family: distributed.backend_family,
                    min_shards: distributed.min_shards,
                    max_shards: distributed.max_shards,
                    preferred_shards: distributed.preferred_shards,
                    per_shard_overhead_bytes: distributed.per_shard_overhead_bytes,
                    requires_homogeneous_backend: distributed.requires_homogeneous_backend,
                    supports_generation: distributed.supports_generation,
                    supports_embedding: distributed.supports_embedding,
                }
            }),
        }
    }
}

impl From<&common::v1::ModelVariant> for StoredVariant {
    fn from(value: &common::v1::ModelVariant) -> Self {
        Self {
            runtime: value.runtime.clone(),
            quantization: value.quantization.clone(),
            format: value.format.clone(),
            size_bytes: value.size_bytes,
            compatible_accelerators: value.compatible_accelerators.clone(),
            distributed: value.distributed.as_ref().map(|distributed| {
                StoredDistributedExecutionPolicy {
                    backend_family: distributed.backend_family.clone(),
                    min_shards: distributed.min_shards,
                    max_shards: distributed.max_shards,
                    preferred_shards: distributed.preferred_shards,
                    per_shard_overhead_bytes: distributed.per_shard_overhead_bytes,
                    requires_homogeneous_backend: distributed.requires_homogeneous_backend,
                    supports_generation: distributed.supports_generation,
                    supports_embedding: distributed.supports_embedding,
                }
            }),
        }
    }
}

fn validate_manifest(manifest: common::v1::ModelManifest) -> Result<common::v1::ModelManifest> {
    let model_id = manifest_id(&manifest);
    if model_id.is_empty() {
        return Err(GalacticaError::invalid_argument(
            "model manifest must define model_id",
        ));
    }
    if manifest.variants.is_empty() {
        return Err(GalacticaError::invalid_argument(format!(
            "model manifest {model_id} must define at least one variant"
        )));
    }
    for variant in &manifest.variants {
        if variant.runtime.trim().is_empty() {
            return Err(GalacticaError::invalid_argument(format!(
                "model manifest {model_id} contains a variant without runtime"
            )));
        }
        if variant.quantization.trim().is_empty() {
            return Err(GalacticaError::invalid_argument(format!(
                "model manifest {model_id} contains a variant without quantization"
            )));
        }
        if let Some(distributed) = &variant.distributed {
            if distributed.backend_family.trim().is_empty() {
                return Err(GalacticaError::invalid_argument(format!(
                    "distributed variant {}:{} in {model_id} must define backend_family",
                    variant.runtime, variant.quantization
                )));
            }
            if distributed.min_shards == 0 {
                return Err(GalacticaError::invalid_argument(format!(
                    "distributed variant {}:{} in {model_id} must use min_shards >= 1",
                    variant.runtime, variant.quantization
                )));
            }
            if distributed.max_shards < distributed.min_shards {
                return Err(GalacticaError::invalid_argument(format!(
                    "distributed variant {}:{} in {model_id} must use max_shards >= min_shards",
                    variant.runtime, variant.quantization
                )));
            }
            if distributed.preferred_shards < distributed.min_shards
                || distributed.preferred_shards > distributed.max_shards
            {
                return Err(GalacticaError::invalid_argument(format!(
                    "distributed variant {}:{} in {model_id} must keep preferred_shards within min/max shard bounds",
                    variant.runtime, variant.quantization
                )));
            }
        }
    }
    Ok(manifest)
}

#[async_trait]
impl ModelRegistry for HuggingFaceRegistry {
    async fn get_model_manifest(&self, model_id: &str) -> Result<common::v1::ModelManifest> {
        let url = format!("{}/api/models/{}", self.base_url, model_id);
        let response = self.client.get(url).send().await.map_err(|error| {
            GalacticaError::unavailable(format!("failed to reach HuggingFace: {error}"))
        })?;
        if response.status().as_u16() == 404 {
            return Err(GalacticaError::not_found(format!(
                "model manifest not found on HuggingFace: {model_id}"
            )));
        }
        let response = response.error_for_status().map_err(|error| {
            GalacticaError::unavailable(format!("HuggingFace request failed: {error}"))
        })?;
        let payload: HuggingFaceModelResponse = response.json().await.map_err(|error| {
            GalacticaError::invalid_argument(format!("invalid HuggingFace payload: {error}"))
        })?;

        let mut metadata = HashMap::new();
        metadata.insert("source".to_string(), "huggingface".to_string());
        if let Some(sha) = payload.sha {
            metadata.insert("sha".to_string(), sha);
        }
        metadata.insert(
            "download_url".to_string(),
            format!("{}/{}", self.base_url, payload.id),
        );
        let family = payload.pipeline_tag.unwrap_or_else(|| {
            payload
                .tags
                .first()
                .cloned()
                .unwrap_or_else(|| "text-generation".to_string())
        });

        validate_manifest(common::v1::ModelManifest {
            model_id: Some(common::v1::ModelId {
                value: payload.id.clone(),
            }),
            name: payload.id.clone(),
            family,
            variants: vec![
                common::v1::ModelVariant {
                    runtime: "mlx".to_string(),
                    quantization: "4bit".to_string(),
                    format: "safetensors".to_string(),
                    size_bytes: 4 * 1024 * 1024 * 1024,
                    compatible_accelerators: vec![common::v1::AcceleratorType::Metal as i32],
                    distributed: None,
                },
                common::v1::ModelVariant {
                    runtime: "llama.cpp".to_string(),
                    quantization: "q4_k_m".to_string(),
                    format: "gguf".to_string(),
                    size_bytes: 4 * 1024 * 1024 * 1024,
                    compatible_accelerators: vec![common::v1::AcceleratorType::Cpu as i32],
                    distributed: None,
                },
            ],
            chat_template: "default".to_string(),
            metadata,
        })
    }

    async fn list_models(
        &self,
        _filter_runtime: Option<&str>,
        _filter_family: Option<&str>,
    ) -> Result<Vec<common::v1::ModelManifest>> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Default)]
pub struct CompositeModelRegistry {
    registries: Vec<Arc<dyn ModelRegistry>>,
}

impl CompositeModelRegistry {
    pub fn new(registries: Vec<Arc<dyn ModelRegistry>>) -> Self {
        Self { registries }
    }
}

#[async_trait]
impl ModelRegistry for CompositeModelRegistry {
    async fn get_model_manifest(&self, model_id: &str) -> Result<common::v1::ModelManifest> {
        for registry in &self.registries {
            match registry.get_model_manifest(model_id).await {
                Ok(manifest) => return Ok(manifest),
                Err(GalacticaError::NotFound(_)) => continue,
                Err(error) => return Err(error),
            }
        }

        Err(GalacticaError::not_found(format!(
            "model manifest not found in any registry: {model_id}"
        )))
    }

    async fn list_models(
        &self,
        filter_runtime: Option<&str>,
        filter_family: Option<&str>,
    ) -> Result<Vec<common::v1::ModelManifest>> {
        let mut manifests = BTreeMap::new();
        for registry in &self.registries {
            for manifest in registry.list_models(filter_runtime, filter_family).await? {
                manifests.insert(manifest_id(&manifest), manifest);
            }
        }

        Ok(manifests.into_values().collect())
    }
}

pub fn resolve_variant(
    manifest: &common::v1::ModelManifest,
    capabilities: &common::v1::NodeCapabilities,
) -> Result<common::v1::ModelVariant> {
    let available_memory = node_available_memory_bytes(capabilities);
    let supported_accelerators: Vec<i32> = supported_accelerators(capabilities);

    manifest
        .variants
        .iter()
        .filter(|variant| variant_min_shards(variant) <= 1)
        .filter(|variant| {
            variant_supports_node_runtime_and_accelerator(
                variant,
                capabilities,
                &supported_accelerators,
            )
        })
        .filter(|variant| {
            available_memory == 0 || variant_memory_budget_bytes(variant, 1) <= available_memory
        })
        .max_by_key(|variant| variant_score(variant, &supported_accelerators, capabilities))
        .cloned()
        .ok_or_else(|| {
            GalacticaError::failed_precondition(format!(
                "no compatible variants available for {}",
                manifest_id(manifest)
            ))
        })
}

pub fn node_available_memory_bytes(capabilities: &common::v1::NodeCapabilities) -> u64 {
    let supported = supported_accelerators(capabilities);
    let system_budget = capabilities
        .system_memory
        .as_ref()
        .map(|memory| {
            if memory.available_bytes == 0 {
                memory.total_bytes
            } else {
                memory.available_bytes
            }
        })
        .unwrap_or(0);
    let accelerator_budget = capabilities
        .accelerators
        .iter()
        .filter(|accelerator| {
            supported.contains(&accelerator.r#type)
                && accelerator
                    .vram
                    .as_ref()
                    .is_some_and(|memory| memory.total_bytes > 0 || memory.available_bytes > 0)
        })
        .filter_map(|accelerator| accelerator.vram.as_ref())
        .map(|memory| {
            if memory.available_bytes == 0 {
                memory.total_bytes
            } else {
                memory.available_bytes
            }
        })
        .max()
        .unwrap_or(0);
    if common::v1::OsType::try_from(capabilities.os).unwrap_or_default()
        == common::v1::OsType::Macos
        && has_accelerator(capabilities, common::v1::AcceleratorType::Metal)
    {
        return accelerator_budget.max(system_budget);
    }
    if accelerator_budget > 0 {
        return accelerator_budget;
    }
    system_budget
}

pub fn supported_accelerators(capabilities: &common::v1::NodeCapabilities) -> Vec<i32> {
    capabilities
        .accelerators
        .iter()
        .map(|accelerator| accelerator.r#type)
        .collect()
}

pub fn variant_min_shards(variant: &common::v1::ModelVariant) -> u32 {
    variant
        .distributed
        .as_ref()
        .map(|distributed| distributed.min_shards.max(1))
        .unwrap_or(1)
}

pub fn variant_memory_budget_bytes(variant: &common::v1::ModelVariant, shard_count: u32) -> u64 {
    let shard_count = shard_count.max(1) as u64;
    if let Some(distributed) = &variant.distributed {
        let per_shard = variant.size_bytes.saturating_add(shard_count - 1) / shard_count;
        per_shard.saturating_add(distributed.per_shard_overhead_bytes)
    } else {
        variant.size_bytes
    }
}

pub fn variant_supports_node_runtime_and_accelerator(
    variant: &common::v1::ModelVariant,
    capabilities: &common::v1::NodeCapabilities,
    supported_accelerators: &[i32],
) -> bool {
    (capabilities.runtime_backends.is_empty()
        || capabilities
            .runtime_backends
            .iter()
            .any(|runtime| runtime.eq_ignore_ascii_case(&variant.runtime)))
        && (variant.compatible_accelerators.is_empty()
            || variant
                .compatible_accelerators
                .iter()
                .any(|accelerator| supported_accelerators.contains(accelerator)))
}

fn variant_score(
    variant: &common::v1::ModelVariant,
    supported_accelerators: &[i32],
    capabilities: &common::v1::NodeCapabilities,
) -> i64 {
    let accelerator_bonus = if variant
        .compatible_accelerators
        .iter()
        .any(|accelerator| supported_accelerators.contains(accelerator))
    {
        10
    } else {
        0
    };
    let runtime_bonus = if capabilities
        .runtime_backends
        .iter()
        .any(|runtime| runtime == &variant.runtime)
    {
        5
    } else {
        0
    };
    accelerator_bonus + runtime_bonus + runtime_preference_score(&variant.runtime, capabilities)
        - (variant.size_bytes / (1024 * 1024 * 1024)) as i64
}

pub fn runtime_preference_score(runtime: &str, capabilities: &common::v1::NodeCapabilities) -> i64 {
    let runtime = runtime.trim().to_ascii_lowercase();
    let os = common::v1::OsType::try_from(capabilities.os).unwrap_or_default();
    let cpu_arch = common::v1::CpuArch::try_from(capabilities.cpu_arch).unwrap_or_default();
    let has_cuda = has_accelerator(capabilities, common::v1::AcceleratorType::Cuda);
    let has_metal = has_accelerator(capabilities, common::v1::AcceleratorType::Metal);
    let has_directml = has_accelerator(capabilities, common::v1::AcceleratorType::Directml);

    match runtime.as_str() {
        "mlx" if os == common::v1::OsType::Macos && cpu_arch == common::v1::CpuArch::Arm64 => 8,
        "vllm" if has_cuda && os == common::v1::OsType::Linux => 7,
        "llama.cpp" | "llamacpp" if has_cuda && os == common::v1::OsType::Windows => 6,
        "onnxruntime" | "onnx" if has_directml && os == common::v1::OsType::Windows => 5,
        "llama.cpp" | "llamacpp" if has_metal => 4,
        "llama.cpp" | "llamacpp" if has_cuda => 4,
        "onnxruntime" | "onnx" if has_cuda => 3,
        "onnxruntime" | "onnx" if os == common::v1::OsType::Windows => 2,
        "llama.cpp" | "llamacpp" => 2,
        _ => 0,
    }
}

fn has_accelerator(
    capabilities: &common::v1::NodeCapabilities,
    accelerator: common::v1::AcceleratorType,
) -> bool {
    capabilities
        .accelerators
        .iter()
        .any(|candidate| candidate.r#type == accelerator as i32)
}

fn matches_runtime(manifest: &common::v1::ModelManifest, filter: Option<&str>) -> bool {
    match filter {
        Some(runtime) => manifest
            .variants
            .iter()
            .any(|variant| variant.runtime == runtime),
        None => true,
    }
}

fn matches_family(manifest: &common::v1::ModelManifest, filter: Option<&str>) -> bool {
    match filter {
        Some(family) => manifest.family == family,
        None => true,
    }
}

pub fn manifest_id(manifest: &common::v1::ModelManifest) -> String {
    manifest
        .model_id
        .as_ref()
        .map(|model_id| model_id.value.clone())
        .unwrap_or_default()
}

#[derive(Clone, Default)]
pub struct DownloadTracker {
    channels: Arc<RwLock<BTreeMap<String, broadcast::Sender<artifact::v1::DownloadProgress>>>>,
}

impl DownloadTracker {
    pub async fn record(&self, progress: artifact::v1::DownloadProgress) {
        let model_id = progress
            .model_id
            .as_ref()
            .map(|model_id| model_id.value.clone())
            .unwrap_or_default();
        self.record_scoped(&model_id, None, None, progress).await;
    }

    pub async fn record_scoped(
        &self,
        model_id: &str,
        runtime: Option<&str>,
        quantization: Option<&str>,
        progress: artifact::v1::DownloadProgress,
    ) {
        let key = download_key(model_id, runtime, quantization);
        let sender = self.sender(key).await;
        let _ = sender.send(progress);
    }

    async fn sender(&self, key: String) -> broadcast::Sender<artifact::v1::DownloadProgress> {
        let mut channels = self.channels.write().await;
        channels
            .entry(key)
            .or_insert_with(|| {
                let (sender, _) = broadcast::channel(32);
                sender
            })
            .clone()
    }

    pub async fn subscribe(
        &self,
        model_id: &str,
        runtime: Option<&str>,
        quantization: Option<&str>,
    ) -> broadcast::Receiver<artifact::v1::DownloadProgress> {
        self.sender(download_key(model_id, runtime, quantization))
            .await
            .subscribe()
    }
}

fn download_key(model_id: &str, runtime: Option<&str>, quantization: Option<&str>) -> String {
    format!(
        "{model_id}:{runtime}:{quantization}",
        runtime = runtime.unwrap_or_default(),
        quantization = quantization.unwrap_or_default()
    )
}

#[derive(Clone)]
pub struct ArtifactServiceImpl<R> {
    registry: R,
    downloads: DownloadTracker,
}

impl<R> ArtifactServiceImpl<R> {
    pub fn new(registry: R) -> Self {
        Self {
            registry,
            downloads: DownloadTracker::default(),
        }
    }

    pub fn with_downloads(registry: R, downloads: DownloadTracker) -> Self {
        Self {
            registry,
            downloads,
        }
    }

    pub fn downloads(&self) -> &DownloadTracker {
        &self.downloads
    }
}

#[tonic::async_trait]
impl<R> ArtifactService for ArtifactServiceImpl<R>
where
    R: ModelRegistry + Clone + Send + Sync + 'static,
{
    type WatchDownloadStream = DownloadStream;

    async fn get_model_manifest(
        &self,
        request: Request<artifact::v1::GetModelManifestRequest>,
    ) -> std::result::Result<Response<artifact::v1::GetModelManifestResponse>, Status> {
        let model_id = request
            .into_inner()
            .model_id
            .and_then(|model_id| {
                if model_id.value.is_empty() {
                    None
                } else {
                    Some(model_id.value)
                }
            })
            .ok_or_else(|| Status::invalid_argument("model_id is required"))?;
        let manifest = self.registry.get_model_manifest(&model_id).await?;

        Ok(Response::new(artifact::v1::GetModelManifestResponse {
            manifest: Some(manifest),
        }))
    }

    async fn list_models(
        &self,
        request: Request<artifact::v1::ListArtifactModelsRequest>,
    ) -> std::result::Result<Response<artifact::v1::ListArtifactModelsResponse>, Status> {
        let request = request.into_inner();
        let page_size = if request.page_size == 0 {
            50
        } else {
            request.page_size as usize
        };
        let start = request.page_token.parse::<usize>().unwrap_or(0);
        let manifests = self
            .registry
            .list_models(
                (!request.filter_runtime.is_empty()).then_some(request.filter_runtime.as_str()),
                (!request.filter_family.is_empty()).then_some(request.filter_family.as_str()),
            )
            .await?;
        let page = manifests
            .into_iter()
            .skip(start)
            .take(page_size)
            .collect::<Vec<_>>();
        let next_page_token = if page.len() == page_size {
            (start + page_size).to_string()
        } else {
            String::new()
        };

        Ok(Response::new(artifact::v1::ListArtifactModelsResponse {
            models: page,
            next_page_token,
        }))
    }

    async fn watch_download(
        &self,
        request: Request<artifact::v1::WatchDownloadRequest>,
    ) -> std::result::Result<Response<Self::WatchDownloadStream>, Status> {
        let request = request.into_inner();
        let model_id = request
            .model_id
            .as_ref()
            .map(|model_id| model_id.value.as_str())
            .unwrap_or_default()
            .to_string();
        let runtime =
            (!request.variant_runtime.is_empty()).then_some(request.variant_runtime.clone());
        let quantization = (!request.variant_quantization.is_empty())
            .then_some(request.variant_quantization.clone());
        let mut receiver = self
            .downloads
            .subscribe(&model_id, runtime.as_deref(), quantization.as_deref())
            .await;

        let bootstrap = artifact::v1::DownloadProgress {
            model_id: Some(common::v1::ModelId {
                value: model_id.clone(),
            }),
            total_bytes: 0,
            downloaded_bytes: 0,
            progress_percent: 0.0,
            status: artifact::v1::DownloadStatus::Queued as i32,
            error_message: String::new(),
        };

        let stream = try_stream! {
            yield bootstrap;
            loop {
                match receiver.recv().await {
                    Ok(progress) => yield progress,
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        };

        Ok(Response::new(Box::pin(stream) as Self::WatchDownloadStream))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use galactica_common::proto::common::v1::{
        AcceleratorInfo, AcceleratorType, CpuArch, Memory, ModelId, ModelManifest, ModelVariant,
        NetworkProfile, NodeCapabilities, OsType,
    };

    use super::{
        LocalModelRegistry, ModelRegistry, StoredManifest, node_available_memory_bytes,
        resolve_variant, variant_memory_budget_bytes,
    };

    fn sample_manifest() -> ModelManifest {
        ModelManifest {
            model_id: Some(ModelId {
                value: "mistral-small".to_string(),
            }),
            name: "Mistral Small".to_string(),
            family: "chat".to_string(),
            variants: vec![
                ModelVariant {
                    runtime: "mlx".to_string(),
                    quantization: "4bit".to_string(),
                    format: "safetensors".to_string(),
                    size_bytes: 2 * 1024 * 1024 * 1024,
                    compatible_accelerators: vec![AcceleratorType::Metal as i32],
                    distributed: None,
                },
                ModelVariant {
                    runtime: "llama.cpp".to_string(),
                    quantization: "q4_k_m".to_string(),
                    format: "gguf".to_string(),
                    size_bytes: 3 * 1024 * 1024 * 1024,
                    compatible_accelerators: vec![AcceleratorType::Cpu as i32],
                    distributed: None,
                },
            ],
            chat_template: "chatml".to_string(),
            metadata: HashMap::new(),
        }
    }

    fn sample_capabilities() -> NodeCapabilities {
        NodeCapabilities {
            os: OsType::Macos as i32,
            cpu_arch: CpuArch::Arm64 as i32,
            accelerators: vec![AcceleratorInfo {
                r#type: AcceleratorType::Metal as i32,
                name: "Apple GPU".to_string(),
                vram: Some(Memory {
                    total_bytes: 8 * 1024 * 1024 * 1024,
                    available_bytes: 8 * 1024 * 1024 * 1024,
                }),
                compute_capability: "metal3".to_string(),
            }],
            system_memory: Some(Memory {
                total_bytes: 16 * 1024 * 1024 * 1024,
                available_bytes: 12 * 1024 * 1024 * 1024,
            }),
            network_profile: NetworkProfile::Lan as i32,
            runtime_backends: vec!["mlx".to_string()],
            locality: HashMap::new(),
        }
    }

    fn mixed_macos_capabilities() -> NodeCapabilities {
        let mut capabilities = sample_capabilities();
        capabilities.accelerators.push(AcceleratorInfo {
            r#type: AcceleratorType::Cpu as i32,
            name: "CPU".to_string(),
            vram: None,
            compute_capability: String::new(),
        });
        capabilities.runtime_backends = vec!["llama.cpp".to_string(), "mlx".to_string()];
        capabilities
    }

    fn windows_cuda_capabilities() -> NodeCapabilities {
        NodeCapabilities {
            os: OsType::Windows as i32,
            cpu_arch: CpuArch::X8664 as i32,
            accelerators: vec![
                AcceleratorInfo {
                    r#type: AcceleratorType::Cuda as i32,
                    name: "RTX".to_string(),
                    vram: Some(Memory {
                        total_bytes: 12 * 1024 * 1024 * 1024,
                        available_bytes: 12 * 1024 * 1024 * 1024,
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
                total_bytes: 32 * 1024 * 1024 * 1024,
                available_bytes: 28 * 1024 * 1024 * 1024,
            }),
            network_profile: NetworkProfile::Lan as i32,
            runtime_backends: vec!["llama.cpp".to_string(), "onnxruntime".to_string()],
            locality: HashMap::new(),
        }
    }

    fn distributed_manifest() -> ModelManifest {
        ModelManifest {
            model_id: Some(ModelId {
                value: "qwen3.5-27b".to_string(),
            }),
            name: "Qwen3.5 27B".to_string(),
            family: "chat".to_string(),
            variants: vec![ModelVariant {
                runtime: "llama.cpp".to_string(),
                quantization: "q4_k_m".to_string(),
                format: "gguf".to_string(),
                size_bytes: 18 * 1024 * 1024 * 1024,
                compatible_accelerators: vec![
                    AcceleratorType::Cuda as i32,
                    AcceleratorType::Metal as i32,
                ],
                distributed: Some(
                    galactica_common::proto::common::v1::DistributedExecutionPolicy {
                        backend_family: "llama.cpp".to_string(),
                        min_shards: 2,
                        max_shards: 4,
                        preferred_shards: 2,
                        per_shard_overhead_bytes: 512 * 1024 * 1024,
                        requires_homogeneous_backend: true,
                        supports_generation: true,
                        supports_embedding: true,
                    },
                ),
            }],
            chat_template: "chatml".to_string(),
            metadata: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn local_registry_scans_manifests() {
        let root =
            std::env::temp_dir().join(format!("galactica-artifact-{}", uuid::Uuid::new_v4()));
        let manifest_dir = root.join("mistral-small");
        fs::create_dir_all(&manifest_dir).unwrap();
        fs::write(
            manifest_dir.join("manifest.json"),
            serde_json::to_vec(&StoredManifest::from(&sample_manifest())).unwrap(),
        )
        .unwrap();

        let registry = LocalModelRegistry::new(&root);
        let manifests = registry
            .list_models(Some("mlx"), Some("chat"))
            .await
            .unwrap();

        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].name, "Mistral Small");

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn variant_resolution_prefers_compatible_runtime_and_accelerator() {
        let selected = resolve_variant(&sample_manifest(), &sample_capabilities()).unwrap();
        assert_eq!(selected.runtime, "mlx");
        assert_eq!(selected.quantization, "4bit");
    }

    #[test]
    fn variant_resolution_prefers_mlx_over_llama_cpp_on_apple_silicon() {
        let manifest = ModelManifest {
            variants: vec![
                ModelVariant {
                    runtime: "mlx".to_string(),
                    quantization: "4bit".to_string(),
                    format: "mlx".to_string(),
                    size_bytes: 2 * 1024 * 1024 * 1024,
                    compatible_accelerators: vec![AcceleratorType::Metal as i32],
                    distributed: None,
                },
                ModelVariant {
                    runtime: "llama.cpp".to_string(),
                    quantization: "q4_k_m".to_string(),
                    format: "gguf".to_string(),
                    size_bytes: 2 * 1024 * 1024 * 1024,
                    compatible_accelerators: vec![
                        AcceleratorType::Cpu as i32,
                        AcceleratorType::Metal as i32,
                    ],
                    distributed: None,
                },
            ],
            ..sample_manifest()
        };

        let selected = resolve_variant(&manifest, &mixed_macos_capabilities()).unwrap();
        assert_eq!(selected.runtime, "mlx");
    }

    #[test]
    fn variant_resolution_prefers_llama_cpp_over_onnxruntime_on_windows_cuda() {
        let manifest = ModelManifest {
            variants: vec![
                ModelVariant {
                    runtime: "onnxruntime".to_string(),
                    quantization: "int4".to_string(),
                    format: "onnx".to_string(),
                    size_bytes: 2 * 1024 * 1024 * 1024,
                    compatible_accelerators: vec![AcceleratorType::Cuda as i32],
                    distributed: None,
                },
                ModelVariant {
                    runtime: "llama.cpp".to_string(),
                    quantization: "q4_k_m".to_string(),
                    format: "gguf".to_string(),
                    size_bytes: 2 * 1024 * 1024 * 1024,
                    compatible_accelerators: vec![
                        AcceleratorType::Cpu as i32,
                        AcceleratorType::Cuda as i32,
                    ],
                    distributed: None,
                },
            ],
            ..sample_manifest()
        };

        let selected = resolve_variant(&manifest, &windows_cuda_capabilities()).unwrap();
        assert_eq!(selected.runtime, "llama.cpp");
    }

    #[test]
    fn variant_resolution_ignores_multi_shard_variants_for_single_node_selection() {
        let selected = resolve_variant(&distributed_manifest(), &windows_cuda_capabilities());
        assert!(selected.is_err());
    }

    #[test]
    fn distributed_memory_budget_splits_model_size_and_uses_vram() {
        let manifest = distributed_manifest();
        let variant = &manifest.variants[0];

        assert_eq!(variant_memory_budget_bytes(variant, 2), 10_200_547_328);
        assert_eq!(
            node_available_memory_bytes(&windows_cuda_capabilities()),
            12 * 1024 * 1024 * 1024
        );
        assert_eq!(
            node_available_memory_bytes(&sample_capabilities()),
            12 * 1024 * 1024 * 1024
        );
    }
}
