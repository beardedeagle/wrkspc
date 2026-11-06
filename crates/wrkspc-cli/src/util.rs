//! Shared CLI helpers: path canonicalization and stable token files.

use std::path::{Path, PathBuf};

/// Canonicalizes the database path (absolute).
///
/// # Arguments
///
/// * `path` — the database path as given.
///
/// # Returns
///
/// The absolute path.
///
/// # Errors
///
/// Returns an `io::Error` when the path cannot be made absolute.
pub fn canonical_db_path(path: &Path) -> Result<PathBuf, std::io::Error> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir().map(|cwd| cwd.join(path))
    }
}

/// Reads (or creates with fresh randomness, mode 0600) a stable token at
/// `<db>.<tag>`. Used for loopback API gates whose clients persist the
/// token across restarts.
///
/// # Arguments
///
/// * `db` — the database path (token file derives from it).
/// * `tag` — the suffix after the database path.
///
/// # Returns
///
/// The token string.
///
/// # Errors
///
/// Returns an error string when the file cannot be written or its
/// permissions cannot be restricted.
pub async fn stable_token(db: &Path, tag: &str) -> Result<String, String> {
    let path = PathBuf::from(format!("{}.{tag}", db.display()));
    match tokio::fs::read_to_string(&path).await {
        Ok(token) if !token.trim().is_empty() => Ok(token.trim().to_string()),
        _ => {
            let token = uuid::Uuid::new_v4().to_string();
            // Mode 0600 AT OPEN: the credential must never exist in a
            // world-readable state (the write-then-chmod pattern left a
            // copyable window, and a crash before the chmod left the
            // file exposed indefinitely).
            #[cfg(unix)]
            let mut file = {
                tokio::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .mode(0o600)
                    .open(&path)
                    .await
                    .map_err(|e| format!("cannot open token file {}: {e}", path.display()))?
            };
            #[cfg(not(unix))]
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .await
                .map_err(|e| format!("cannot open token file {}: {e}", path.display()))?;
            {
                use tokio::io::AsyncWriteExt;
                file.write_all(token.as_bytes())
                    .await
                    .map_err(|e| format!("cannot write token to {}: {e}", path.display()))?;
            }
            Ok(token)
        }
    }
}
