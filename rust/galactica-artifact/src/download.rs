use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use galactica_common::proto::{artifact, common};
use galactica_common::{GalacticaError, Result};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Semaphore};

use crate::DownloadTracker;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CacheKey {
    pub model_id: String,
    pub variant_runtime: String,
    pub variant_quantization: String,
}

impl CacheKey {
    pub fn new(
        model_id: impl Into<String>,
        variant_runtime: impl Into<String>,
        variant_quantization: impl Into<String>,
    ) -> Self {
        Self {
            model_id: model_id.into(),
            variant_runtime: variant_runtime.into(),
            variant_quantization: variant_quantization.into(),
        }
    }

    pub fn cache_id(&self) -> String {
        sanitize_path_component(&format!(
            "{}-{}-{}",
            self.model_id, self.variant_runtime, self.variant_quantization
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    pub key: CacheKey,
    pub path: PathBuf,
    pub size_bytes: u64,
    pub content_sha256: String,
    pub last_accessed: DateTime<Utc>,
}

#[derive(Clone)]
pub struct LocalCache {
    root: PathBuf,
    max_bytes: u64,
    entries: Arc<Mutex<BTreeMap<String, CacheEntry>>>,
}

impl LocalCache {
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("artifacts")).map_err(|error| {
            GalacticaError::internal(format!("failed to create cache root: {error}"))
        })?;
        fs::create_dir_all(root.join("partials")).map_err(|error| {
            GalacticaError::internal(format!("failed to create partial root: {error}"))
        })?;
        Ok(Self {
            root,
            max_bytes,
            entries: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn artifact_path(&self, key: &CacheKey) -> PathBuf {
        self.root
            .join("artifacts")
            .join(format!("{}.bin", key.cache_id()))
    }

    pub fn partial_path(&self, key: &CacheKey) -> PathBuf {
        self.root
            .join("partials")
            .join(format!("{}.part", key.cache_id()))
    }

    pub async fn get(&self, key: &CacheKey) -> Result<Option<CacheEntry>> {
        let mut entries = self.entries.lock().await;
        let cache_id = key.cache_id();
        if let Some(entry) = entries.get_mut(&cache_id) {
            if !entry.path.exists() {
                entries.remove(&cache_id);
                return Ok(None);
            }
            entry.last_accessed = Utc::now();
            return Ok(Some(entry.clone()));
        }
        Ok(None)
    }

    pub async fn contains(&self, key: &CacheKey) -> Result<bool> {
        Ok(self.get(key).await?.is_some())
    }

    pub async fn entries(&self) -> Vec<CacheEntry> {
        self.entries.lock().await.values().cloned().collect()
    }

    pub async fn total_size_bytes(&self) -> u64 {
        self.entries
            .lock()
            .await
            .values()
            .map(|entry| entry.size_bytes)
            .sum()
    }

    pub async fn put_bytes(
        &self,
        key: CacheKey,
        bytes: &[u8],
        expected_sha256: Option<&str>,
    ) -> Result<CacheEntry> {
        let path = self.artifact_path(&key);
        fs::write(&path, bytes).map_err(|error| {
            GalacticaError::internal(format!(
                "failed to write cache entry {}: {error}",
                path.display()
            ))
        })?;
        if let Some(expected_sha256) = expected_sha256 {
            verify_content_hash(&path, expected_sha256)?;
        }
        let content_sha256 = sha256_digest(bytes);
        self.register_entry(key, path, bytes.len() as u64, content_sha256)
            .await
    }

    pub async fn evict(&self, key: &CacheKey) -> Result<Option<CacheEntry>> {
        let mut entries = self.entries.lock().await;
        let removed = entries.remove(&key.cache_id());
        drop(entries);
        if let Some(removed) = removed.clone()
            && removed.path.exists()
        {
            fs::remove_file(&removed.path).map_err(|error| {
                GalacticaError::internal(format!(
                    "failed to remove cache entry {}: {error}",
                    removed.path.display()
                ))
            })?;
        }
        Ok(removed)
    }

    async fn register_downloaded_file(
        &self,
        key: CacheKey,
        path: PathBuf,
        size_bytes: u64,
        content_sha256: String,
    ) -> Result<CacheEntry> {
        self.register_entry(key, path, size_bytes, content_sha256)
            .await
    }

    async fn register_entry(
        &self,
        key: CacheKey,
        path: PathBuf,
        size_bytes: u64,
        content_sha256: String,
    ) -> Result<CacheEntry> {
        let entry = CacheEntry {
            key: key.clone(),
            path,
            size_bytes,
            content_sha256,
            last_accessed: Utc::now(),
        };
        let cache_id = key.cache_id();
        {
            let mut entries = self.entries.lock().await;
            entries.insert(cache_id.clone(), entry.clone());
        }
        self.evict_until_within_limit(Some(cache_id)).await?;
        Ok(entry)
    }

    async fn evict_until_within_limit(&self, protected: Option<String>) -> Result<()> {
        loop {
            let (total_size, candidate) = {
                let entries = self.entries.lock().await;
                let total_size = entries.values().map(|entry| entry.size_bytes).sum::<u64>();
                let candidate = entries
                    .iter()
                    .filter(|(cache_id, _)| protected.as_ref() != Some(cache_id))
                    .min_by_key(|(_, entry)| entry.last_accessed)
                    .map(|(_, entry)| entry.key.clone());
                (total_size, candidate)
            };

            if total_size <= self.max_bytes {
                return Ok(());
            }
            match candidate {
                Some(candidate) => {
                    self.evict(&candidate).await?;
                }
                None => return Ok(()),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDownloadSource {
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadRequest {
    pub key: CacheKey,
    pub source: ArtifactDownloadSource,
    pub expected_size_bytes: Option<u64>,
    pub expected_sha256: Option<String>,
    pub chunk_size_bytes: usize,
}

#[async_trait]
pub trait ArtifactFetcher: Send + Sync {
    async fn total_size(&self, source: &ArtifactDownloadSource) -> Result<u64>;
    async fn fetch_chunk(
        &self,
        source: &ArtifactDownloadSource,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>>;
}

pub struct DownloadManager<F> {
    cache: LocalCache,
    tracker: DownloadTracker,
    fetcher: Arc<F>,
    locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    semaphore: Arc<Semaphore>,
}

impl<F> DownloadManager<F> {
    pub fn new(cache: LocalCache, fetcher: Arc<F>) -> Self {
        Self::with_tracker(cache, fetcher, DownloadTracker::default(), 4)
    }

    pub fn with_tracker(
        cache: LocalCache,
        fetcher: Arc<F>,
        tracker: DownloadTracker,
        concurrency_limit: usize,
    ) -> Self {
        Self {
            cache,
            tracker,
            fetcher,
            locks: Arc::new(Mutex::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(concurrency_limit.max(1))),
        }
    }

    pub fn cache(&self) -> &LocalCache {
        &self.cache
    }

    pub fn tracker(&self) -> &DownloadTracker {
        &self.tracker
    }
}

impl<F> DownloadManager<F>
where
    F: ArtifactFetcher + 'static,
{
    pub async fn download(&self, request: DownloadRequest) -> Result<CacheEntry> {
        if let Some(entry) = self.cache.get(&request.key).await? {
            self.emit_progress(
                &request,
                entry.size_bytes,
                entry.size_bytes,
                artifact::v1::DownloadStatus::Complete,
                "",
            )
            .await;
            return Ok(entry);
        }

        let key_string = request.key.cache_id();
        let lock = {
            let mut locks = self.locks.lock().await;
            locks
                .entry(key_string)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = lock.lock().await;
        if let Some(entry) = self.cache.get(&request.key).await? {
            return Ok(entry);
        }

        let _permit = self.semaphore.acquire().await.map_err(|error| {
            GalacticaError::unavailable(format!("download concurrency limiter closed: {error}"))
        })?;

        let total_bytes = request
            .expected_size_bytes
            .unwrap_or(self.fetcher.total_size(&request.source).await?);
        let partial_path = self.cache.partial_path(&request.key);
        let final_path = self.cache.artifact_path(&request.key);
        if let Some(parent) = partial_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                GalacticaError::internal(format!("failed to create partial directory: {error}"))
            })?;
        }

        self.emit_progress(
            &request,
            total_bytes,
            0,
            artifact::v1::DownloadStatus::Queued,
            "",
        )
        .await;

        let mut downloaded_bytes = existing_file_size(&partial_path)?;
        let result = async {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&partial_path)
                .map_err(|error| {
                    GalacticaError::internal(format!(
                        "failed to open partial artifact {}: {error}",
                        partial_path.display()
                    ))
                })?;

            while downloaded_bytes < total_bytes {
                let chunk = self
                    .fetcher
                    .fetch_chunk(
                        &request.source,
                        downloaded_bytes,
                        request.chunk_size_bytes.max(1),
                    )
                    .await?;
                if chunk.is_empty() {
                    return Err(GalacticaError::unavailable(format!(
                        "incomplete download for {}",
                        request.key.model_id
                    )));
                }
                file.write_all(&chunk).map_err(|error| {
                    GalacticaError::internal(format!(
                        "failed to write partial artifact {}: {error}",
                        partial_path.display()
                    ))
                })?;
                downloaded_bytes = downloaded_bytes.saturating_add(chunk.len() as u64);
                self.emit_progress(
                    &request,
                    total_bytes,
                    downloaded_bytes.min(total_bytes),
                    artifact::v1::DownloadStatus::Downloading,
                    "",
                )
                .await;
            }
            file.flush().map_err(|error| {
                GalacticaError::internal(format!(
                    "failed to flush partial artifact {}: {error}",
                    partial_path.display()
                ))
            })?;

            self.emit_progress(
                &request,
                total_bytes,
                total_bytes,
                artifact::v1::DownloadStatus::Verifying,
                "",
            )
            .await;
            let content_sha256 = hash_file(&partial_path)?;
            if let Some(expected_sha256) = &request.expected_sha256 {
                verify_content_hash(&partial_path, expected_sha256)?;
            }
            if let Some(parent) = final_path.parent() {
                fs::create_dir_all(parent).map_err(|error| {
                    GalacticaError::internal(format!(
                        "failed to create artifact directory: {error}"
                    ))
                })?;
            }
            fs::rename(&partial_path, &final_path).map_err(|error| {
                GalacticaError::internal(format!(
                    "failed to finalize artifact {}: {error}",
                    final_path.display()
                ))
            })?;
            let entry = self
                .cache
                .register_downloaded_file(
                    request.key.clone(),
                    final_path.clone(),
                    total_bytes,
                    content_sha256,
                )
                .await?;
            self.emit_progress(
                &request,
                total_bytes,
                total_bytes,
                artifact::v1::DownloadStatus::Complete,
                "",
            )
            .await;
            Ok(entry)
        }
        .await;

        if let Err(error) = &result {
            self.emit_progress(
                &request,
                total_bytes,
                downloaded_bytes,
                artifact::v1::DownloadStatus::Failed,
                &error.to_string(),
            )
            .await;
        }

        result
    }

    async fn emit_progress(
        &self,
        request: &DownloadRequest,
        total_bytes: u64,
        downloaded_bytes: u64,
        status: artifact::v1::DownloadStatus,
        error_message: &str,
    ) {
        self.tracker
            .record_scoped(
                &request.key.model_id,
                Some(&request.key.variant_runtime),
                Some(&request.key.variant_quantization),
                artifact::v1::DownloadProgress {
                    model_id: Some(common::v1::ModelId {
                        value: request.key.model_id.clone(),
                    }),
                    total_bytes,
                    downloaded_bytes,
                    progress_percent: progress_percent(downloaded_bytes, total_bytes),
                    status: status as i32,
                    error_message: error_message.to_string(),
                },
            )
            .await;
    }
}

pub fn verify_content_hash(path: impl AsRef<Path>, expected_sha256: &str) -> Result<()> {
    let actual_sha256 = hash_file(path)?;
    let expected_sha256 = expected_sha256.trim().to_ascii_lowercase();
    if actual_sha256 != expected_sha256 {
        return Err(GalacticaError::failed_precondition(format!(
            "artifact integrity verification failed: expected {expected_sha256}, got {actual_sha256}"
        )));
    }
    Ok(())
}

fn progress_percent(downloaded_bytes: u64, total_bytes: u64) -> f32 {
    if total_bytes == 0 {
        0.0
    } else {
        (downloaded_bytes as f64 / total_bytes as f64 * 100.0) as f32
    }
}

fn existing_file_size(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    Ok(fs::metadata(path)
        .map_err(|error| {
            GalacticaError::internal(format!(
                "failed to stat partial artifact {}: {error}",
                path.display()
            ))
        })?
        .len())
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => character,
            _ => '_',
        })
        .collect()
}

pub fn sha256_digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn hash_file(path: impl AsRef<Path>) -> Result<String> {
    let path = path.as_ref();
    let mut file = fs::File::open(path).map_err(|error| {
        GalacticaError::internal(format!(
            "failed to open {} for hashing: {error}",
            path.display()
        ))
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            GalacticaError::internal(format!(
                "failed to read {} for hashing: {error}",
                path.display()
            ))
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::collections::VecDeque;

    use super::*;

    #[derive(Default)]
    struct FakeFetcher {
        blobs: Mutex<HashMap<String, Vec<u8>>>,
        fail_once_at: Mutex<HashMap<String, u64>>,
        failed: Mutex<HashSet<String>>,
        fetch_calls: Mutex<HashMap<String, usize>>,
        requested_offsets: Mutex<HashMap<String, VecDeque<u64>>>,
    }

    #[async_trait]
    impl ArtifactFetcher for FakeFetcher {
        async fn total_size(&self, source: &ArtifactDownloadSource) -> Result<u64> {
            Ok(self
                .blobs
                .lock()
                .await
                .get(&source.uri)
                .map(|blob| blob.len() as u64)
                .unwrap_or(0))
        }

        async fn fetch_chunk(
            &self,
            source: &ArtifactDownloadSource,
            offset: u64,
            max_bytes: usize,
        ) -> Result<Vec<u8>> {
            *self
                .fetch_calls
                .lock()
                .await
                .entry(source.uri.clone())
                .or_default() += 1;
            self.requested_offsets
                .lock()
                .await
                .entry(source.uri.clone())
                .or_default()
                .push_back(offset);

            if let Some(fail_at) = self.fail_once_at.lock().await.get(&source.uri).copied() {
                let mut failed = self.failed.lock().await;
                if offset >= fail_at && !failed.contains(&source.uri) {
                    failed.insert(source.uri.clone());
                    return Err(GalacticaError::unavailable("simulated fetch failure"));
                }
            }

            let blobs = self.blobs.lock().await;
            let blob = blobs
                .get(&source.uri)
                .ok_or_else(|| GalacticaError::not_found("blob not found"))?;
            let start = offset as usize;
            let end = (start + max_bytes).min(blob.len());
            Ok(blob[start..end].to_vec())
        }
    }

    #[tokio::test]
    async fn download_manager_resumes_from_partial_file() {
        let root =
            std::env::temp_dir().join(format!("galactica-download-{}", uuid::Uuid::new_v4()));
        let cache = LocalCache::new(&root, 128 * 1024 * 1024).unwrap();
        let fetcher = Arc::new(FakeFetcher::default());
        let payload = b"0123456789abcdef".to_vec();
        fetcher
            .blobs
            .lock()
            .await
            .insert("memory://model".to_string(), payload.clone());
        fetcher
            .fail_once_at
            .lock()
            .await
            .insert("memory://model".to_string(), 8);
        let manager = DownloadManager::new(cache.clone(), fetcher.clone());
        let request = DownloadRequest {
            key: CacheKey::new("model", "llama.cpp", "q4_k_m"),
            source: ArtifactDownloadSource {
                uri: "memory://model".to_string(),
            },
            expected_size_bytes: Some(payload.len() as u64),
            expected_sha256: Some(sha256_digest(&payload)),
            chunk_size_bytes: 4,
        };

        assert!(manager.download(request.clone()).await.is_err());
        assert_eq!(
            existing_file_size(&cache.partial_path(&request.key)).unwrap(),
            8
        );

        let entry = manager.download(request.clone()).await.unwrap();
        assert_eq!(entry.size_bytes, payload.len() as u64);
        assert!(entry.path.exists());
        assert_eq!(
            fetcher
                .requested_offsets
                .lock()
                .await
                .get("memory://model")
                .unwrap()
                .back()
                .copied(),
            Some(12)
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn integrity_verification_rejects_corrupted_artifacts() {
        let root =
            std::env::temp_dir().join(format!("galactica-download-{}", uuid::Uuid::new_v4()));
        let cache = LocalCache::new(&root, 128 * 1024 * 1024).unwrap();
        let fetcher = Arc::new(FakeFetcher::default());
        let payload = b"corrupted-data".to_vec();
        fetcher
            .blobs
            .lock()
            .await
            .insert("memory://bad".to_string(), payload);
        let manager = DownloadManager::new(cache.clone(), fetcher.clone());
        let request = DownloadRequest {
            key: CacheKey::new("bad-model", "onnxruntime", "int8"),
            source: ArtifactDownloadSource {
                uri: "memory://bad".to_string(),
            },
            expected_size_bytes: Some(14),
            expected_sha256: Some(sha256_digest(b"expected-data")),
            chunk_size_bytes: 8,
        };

        let error = manager.download(request.clone()).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("artifact integrity verification failed")
        );
        assert!(cache.partial_path(&request.key).exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn local_cache_evicts_least_recently_used_entries() {
        let root = std::env::temp_dir().join(format!("galactica-cache-{}", uuid::Uuid::new_v4()));
        let cache = LocalCache::new(&root, 10).unwrap();
        let first = CacheKey::new("first", "llama.cpp", "q4");
        let second = CacheKey::new("second", "llama.cpp", "q4");

        cache
            .put_bytes(first.clone(), b"123456", None)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        cache
            .put_bytes(second.clone(), b"abcdef", None)
            .await
            .unwrap();

        assert!(cache.get(&first).await.unwrap().is_none());
        assert!(cache.get(&second).await.unwrap().is_some());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn concurrent_downloads_are_deduplicated() {
        let root =
            std::env::temp_dir().join(format!("galactica-download-{}", uuid::Uuid::new_v4()));
        let cache = LocalCache::new(&root, 128 * 1024 * 1024).unwrap();
        let fetcher = Arc::new(FakeFetcher::default());
        let payload = vec![42u8; 32];
        fetcher
            .blobs
            .lock()
            .await
            .insert("memory://shared".to_string(), payload.clone());
        let manager = Arc::new(DownloadManager::new(cache.clone(), fetcher.clone()));
        let request = DownloadRequest {
            key: CacheKey::new("shared-model", "vllm", "fp16"),
            source: ArtifactDownloadSource {
                uri: "memory://shared".to_string(),
            },
            expected_size_bytes: Some(payload.len() as u64),
            expected_sha256: Some(sha256_digest(&payload)),
            chunk_size_bytes: 8,
        };

        let (left, right) =
            tokio::join!(manager.download(request.clone()), manager.download(request));
        assert!(left.is_ok());
        assert!(right.is_ok());
        assert_eq!(
            *fetcher
                .fetch_calls
                .lock()
                .await
                .get("memory://shared")
                .unwrap(),
            4
        );

        fs::remove_dir_all(root).unwrap();
    }
}
