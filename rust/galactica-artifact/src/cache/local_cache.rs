use std::path::{Path, PathBuf};

use galactica_common::types::model::ModelId;

/// Local disk cache for model artifacts.
pub struct LocalCache {
    cache_dir: PathBuf,
    max_bytes: u64,
}

impl LocalCache {
    pub fn new(cache_dir: PathBuf, max_bytes: u64) -> Self {
        Self {
            cache_dir,
            max_bytes,
        }
    }

    /// Get the path to a cached model artifact, if it exists.
    pub fn get(&self, model_id: &ModelId, variant: &str) -> Option<PathBuf> {
        let path = self.artifact_path(model_id, variant);
        if path.exists() {
            Some(path)
        } else {
            None
        }
    }

    /// Store a model artifact in the cache.
    pub async fn put(
        &self,
        model_id: &ModelId,
        variant: &str,
        source: &Path,
    ) -> galactica_common::Result<PathBuf> {
        let dest = self.artifact_path(model_id, variant);
        tracing::info!(?model_id, %variant, ?dest, "caching artifact");
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(galactica_common::GalacticaError::Io)?;
        }
        tokio::fs::copy(source, &dest)
            .await
            .map_err(galactica_common::GalacticaError::Io)?;
        Ok(dest)
    }

    /// Evict least-recently-used artifacts until disk usage is below the limit.
    pub async fn evict_lru(&self) -> galactica_common::Result<u64> {
        tracing::info!(dir = ?self.cache_dir, max = self.max_bytes, "running LRU eviction");
        todo!("implement LRU eviction")
    }

    /// Get total disk usage of the cache.
    pub async fn disk_usage(&self) -> galactica_common::Result<u64> {
        tracing::info!(dir = ?self.cache_dir, "calculating disk usage");
        todo!("implement disk usage calculation")
    }

    fn artifact_path(&self, model_id: &ModelId, variant: &str) -> PathBuf {
        self.cache_dir.join(&model_id.0).join(variant)
    }
}
