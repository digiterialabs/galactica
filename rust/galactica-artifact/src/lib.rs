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
        Ok(manifest.into_manifest())
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

        manifests.sort_by(|left, right| manifest_id(left).cmp(&manifest_id(right)));
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
}

impl StoredVariant {
    fn into_variant(self) -> common::v1::ModelVariant {
        common::v1::ModelVariant {
            runtime: self.runtime,
            quantization: self.quantization,
            format: self.format,
            size_bytes: self.size_bytes,
            compatible_accelerators: self.compatible_accelerators,
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
        }
    }
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

        Ok(common::v1::ModelManifest {
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
                },
                common::v1::ModelVariant {
                    runtime: "llama.cpp".to_string(),
                    quantization: "q4_k_m".to_string(),
                    format: "gguf".to_string(),
                    size_bytes: 4 * 1024 * 1024 * 1024,
                    compatible_accelerators: vec![common::v1::AcceleratorType::Cpu as i32],
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
    let available_memory = capabilities
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
    let supported_accelerators: Vec<i32> = capabilities
        .accelerators
        .iter()
        .map(|accelerator| accelerator.r#type)
        .collect();

    manifest
        .variants
        .iter()
        .filter(|variant| {
            capabilities.runtime_backends.is_empty()
                || capabilities
                    .runtime_backends
                    .iter()
                    .any(|runtime| runtime == &variant.runtime)
        })
        .filter(|variant| {
            variant.compatible_accelerators.is_empty()
                || variant
                    .compatible_accelerators
                    .iter()
                    .any(|accelerator| supported_accelerators.contains(accelerator))
        })
        .filter(|variant| available_memory == 0 || variant.size_bytes <= available_memory)
        .max_by_key(|variant| variant_score(variant, &supported_accelerators, capabilities))
        .cloned()
        .ok_or_else(|| {
            GalacticaError::failed_precondition(format!(
                "no compatible variants available for {}",
                manifest_id(manifest)
            ))
        })
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
    accelerator_bonus + runtime_bonus - (variant.size_bytes / (1024 * 1024 * 1024)) as i64
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

    use super::{LocalModelRegistry, ModelRegistry, StoredManifest, resolve_variant};

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
                },
                ModelVariant {
                    runtime: "llama.cpp".to_string(),
                    quantization: "q4_k_m".to_string(),
                    format: "gguf".to_string(),
                    size_bytes: 3 * 1024 * 1024 * 1024,
                    compatible_accelerators: vec![AcceleratorType::Cpu as i32],
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
}
