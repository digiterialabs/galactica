use async_trait::async_trait;

use galactica_common::types::model::{ModelId, ModelManifest, ModelVariant};

/// Trait for model registries that can list, get, and resolve model variants.
#[async_trait]
pub trait ModelRegistry: Send + Sync {
    /// List all available models.
    async fn list_models(&self) -> galactica_common::Result<Vec<ModelManifest>>;

    /// Get a specific model by ID.
    async fn get_model(&self, model_id: &ModelId) -> galactica_common::Result<ModelManifest>;

    /// Resolve the best variant for a given runtime and constraints.
    async fn resolve_variant(
        &self,
        model_id: &ModelId,
        runtime: &str,
    ) -> galactica_common::Result<ModelVariant>;
}

/// Local filesystem-based model registry.
pub struct LocalModelRegistry {
    registry_path: std::path::PathBuf,
}

impl LocalModelRegistry {
    pub fn new(registry_path: std::path::PathBuf) -> Self {
        Self { registry_path }
    }
}

#[async_trait]
impl ModelRegistry for LocalModelRegistry {
    async fn list_models(&self) -> galactica_common::Result<Vec<ModelManifest>> {
        tracing::info!(path = ?self.registry_path, "listing models from local registry");
        todo!("implement local model listing")
    }

    async fn get_model(&self, model_id: &ModelId) -> galactica_common::Result<ModelManifest> {
        tracing::info!(?model_id, "getting model from local registry");
        todo!("implement local model get")
    }

    async fn resolve_variant(
        &self,
        model_id: &ModelId,
        runtime: &str,
    ) -> galactica_common::Result<ModelVariant> {
        tracing::info!(?model_id, %runtime, "resolving variant from local registry");
        todo!("implement local variant resolution")
    }
}

/// HuggingFace Hub-based model registry.
pub struct HuggingFaceRegistry {
    api_base: String,
}

impl HuggingFaceRegistry {
    pub fn new() -> Self {
        Self {
            api_base: "https://huggingface.co/api".to_string(),
        }
    }
}

impl Default for HuggingFaceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ModelRegistry for HuggingFaceRegistry {
    async fn list_models(&self) -> galactica_common::Result<Vec<ModelManifest>> {
        tracing::info!(api = %self.api_base, "listing models from HuggingFace");
        todo!("implement HuggingFace model listing")
    }

    async fn get_model(&self, model_id: &ModelId) -> galactica_common::Result<ModelManifest> {
        tracing::info!(?model_id, "getting model from HuggingFace");
        todo!("implement HuggingFace model get")
    }

    async fn resolve_variant(
        &self,
        model_id: &ModelId,
        runtime: &str,
    ) -> galactica_common::Result<ModelVariant> {
        tracing::info!(?model_id, %runtime, "resolving variant from HuggingFace");
        todo!("implement HuggingFace variant resolution")
    }
}
