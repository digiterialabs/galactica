use sha2::{Digest, Sha256};
use std::path::Path;

/// Verify the SHA-256 content hash of a file.
pub async fn verify_content_hash(
    path: &Path,
    expected_hash: &str,
) -> galactica_common::Result<bool> {
    let data = tokio::fs::read(path).await.map_err(galactica_common::GalacticaError::Io)?;

    let mut hasher = Sha256::new();
    hasher.update(&data);
    let result = hasher.finalize();
    let hex_hash = format!("{result:x}");

    Ok(hex_hash == expected_hash)
}
