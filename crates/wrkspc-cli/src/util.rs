//! Shared CLI helpers: path canonicalization and stable token files.

use std::path::{Path, PathBuf};

/// Canonicalizes the database path (CU-10).
///
/// The nearest EXISTING ancestor is fully resolved — symlinks
/// included — and the remaining tail is re-appended. The same
/// database reached through a symlink alias therefore maps to the
/// same canonical path, so sidecar token keys and the node instance
/// id do not change across restarts that spell the path differently.
///
/// # Arguments
///
/// * `path` — the database path as given.
///
/// # Returns
///
/// The canonical path.
///
/// # Errors
///
/// Returns an `io::Error` when the path has no existing ancestor or
/// cannot be resolved.
pub fn canonical_db_path(path: &Path) -> Result<PathBuf, std::io::Error> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    // Walk up to the nearest existing ancestor; every component above
    // it resolves (a symlinked parent like /tmp → /private/tmp lands
    // on the real location), and the not-yet-created tail stays
    // verbatim.
    let mut existing = absolute.as_path();
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    loop {
        match std::fs::symlink_metadata(existing) {
            Ok(_) => break,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tail.push(
                    existing
                        .file_name()
                        .ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                format!("invalid path: {}", absolute.display()),
                            )
                        })?
                        .to_os_string(),
                );
                existing = existing.parent().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("no existing ancestor for {}", absolute.display()),
                    )
                })?;
            }
            Err(e) => return Err(e),
        }
    }
    let mut resolved = std::fs::canonicalize(existing)?;
    for part in tail.into_iter().rev() {
        resolved.push(part);
    }
    Ok(resolved)
}

/// Loads and validates an EXISTING sidecar token file (CU-02).
///
/// The policy, shared by [`stable_token`] and the platform file
/// store: a symlink is refused (never followed), a non-regular file
/// is refused, a file owned by another user is refused, group/world
/// permission bits are tightened to 0600 before the value is
/// returned, and every read error other than a missing file is
/// REPORTED — never treated as permission to rotate identity.
///
/// # Arguments
///
/// * `path` — the sidecar path.
///
/// # Returns
///
/// `Ok(Some(token))` for a validated non-empty file, `Ok(None)` when
/// the file does not exist.
///
/// # Errors
///
/// Returns the reason the file must not be read.
pub async fn load_existing_token(path: &Path) -> Result<Option<String>, String> {
    load_existing_token_sync(path)
}

/// The synchronous core of [`load_existing_token`] — the platform
/// file store (a synchronous trait) uses this directly.
pub fn load_existing_token_sync(path: &Path) -> Result<Option<String>, String> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(format!(
                    "token file {} is a symlink — refusing to read or follow it",
                    path.display()
                ));
            }
            if !meta.is_file() {
                return Err(format!(
                    "token path {} is not a regular file",
                    path.display()
                ));
            }
            #[cfg(unix)]
            validate_token_ownership(&meta, path)?;
            let contents = std::fs::read_to_string(path)
                .map_err(|e| format!("cannot read token file {}: {e}", path.display()))?;
            let token = contents.trim().to_string();
            if token.is_empty() {
                return Err(format!(
                    "token file {} exists but is empty — remove it to generate a token",
                    path.display()
                ));
            }
            Ok(Some(token))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("cannot stat token file {}: {e}", path.display())),
    }
}

/// Unix-only ownership and mode validation for an existing token file
/// (CU-02): another owner is refused, and a permissive mode is
/// tightened before the value is read.
#[cfg(unix)]
fn validate_token_ownership(meta: &std::fs::Metadata, path: &Path) -> Result<(), String> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let euid = unsafe { libc::geteuid() };
    if meta.uid() != euid {
        return Err(format!(
            "token file {} is owned by uid {} — refusing to read it",
            path.display(),
            meta.uid()
        ));
    }
    if meta.mode() & 0o077 != 0 {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|e| {
            format!(
                "cannot tighten permissions on token file {}: {e}",
                path.display()
            )
        })?;
    }
    Ok(())
}

/// Reads (or creates with fresh randomness, mode 0600) a stable token at
/// `<db>.<tag>`. Used for loopback API gates whose clients persist the
/// token across restarts.
///
/// Creation is exclusive: concurrent first-callers contend on
/// `create_new`, the loser re-reads the winner's file, and every
/// caller returns the SAME value. The write is synced to disk before
/// the token is handed out. An existing empty file, a symlink, a
/// foreign owner, or a read failure is an ERROR — the credential is
/// never silently rotated (CU-02).
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
/// Returns the validation or I/O failure.
pub async fn stable_token(db: &Path, tag: &str) -> Result<String, String> {
    stable_token_sync(db, tag)
}

/// The synchronous core of [`stable_token`].
pub fn stable_token_sync(db: &Path, tag: &str) -> Result<String, String> {
    let path = PathBuf::from(format!("{}.{tag}", db.display()));
    if let Some(token) = load_existing_token_sync(&path)? {
        return Ok(token);
    }
    let token = uuid::Uuid::new_v4().to_string();
    // create_new + O_NOFOLLOW: an atomic exclusive create that can
    // never follow a link swapped into place by another process.
    let mut options = std::fs::OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.write(true).create_new(true).mode(0o600);
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(not(unix))]
    options.write(true).create_new(true);
    match options.open(&path) {
        Ok(mut file) => {
            use std::io::Write;
            file.write_all(token.as_bytes())
                .map_err(|e| format!("cannot write token to {}: {e}", path.display()))?;
            file.sync_all()
                .map_err(|e| format!("cannot sync token file {}: {e}", path.display()))?;
            Ok(token)
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            // A concurrent creator won: the stable value is the one on
            // disk, so every caller agrees.
            match load_existing_token_sync(&path)? {
                Some(token) => Ok(token),
                None => Err(format!(
                    "token file {} appeared during creation — refusing to rotate it",
                    path.display()
                )),
            }
        }
        Err(e) => Err(format!("cannot create token file {}: {e}", path.display())),
    }
}
