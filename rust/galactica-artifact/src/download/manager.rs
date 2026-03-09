use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use galactica_common::types::model::ModelId;

/// Progress of a model download.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadProgress {
    pub model_id: ModelId,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub progress_percent: f32,
    pub status: DownloadStatus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DownloadStatus {
    Queued,
    Downloading,
    Verifying,
    Complete,
    Failed,
}

/// Trait for download backends.
#[async_trait]
pub trait Downloader: Send + Sync {
    /// Download a model artifact to the given path.
    async fn download(
        &self,
        url: &str,
        dest: &std::path::Path,
        progress_callback: Option<Box<dyn Fn(DownloadProgress) + Send>>,
    ) -> galactica_common::Result<()>;
}

/// Manages model downloads with resume support and progress tracking.
pub struct DownloadManager {
    download_dir: std::path::PathBuf,
    max_concurrent: usize,
}

impl DownloadManager {
    pub fn new(download_dir: std::path::PathBuf, max_concurrent: usize) -> Self {
        Self {
            download_dir,
            max_concurrent,
        }
    }

    /// Start downloading a model.
    pub async fn start_download(
        &self,
        model_id: &ModelId,
        url: &str,
    ) -> galactica_common::Result<()> {
        tracing::info!(?model_id, %url, dir = ?self.download_dir, max = self.max_concurrent, "starting download");
        todo!("implement download with resume support")
    }

    /// Get the progress of an active download.
    pub fn get_progress(&self, model_id: &ModelId) -> Option<DownloadProgress> {
        tracing::debug!(?model_id, "checking download progress");
        None
    }
}
