// Galactica artifact: model registry, downloads, and caching.

pub mod cache;
pub mod download;
pub mod registry;

pub use cache::local_cache::LocalCache;
pub use download::integrity::verify_content_hash;
pub use download::manager::{DownloadManager, DownloadProgress, DownloadStatus, Downloader};
pub use registry::model_registry::{HuggingFaceRegistry, LocalModelRegistry, ModelRegistry};
