//! Platform secret storage for the operator UI token.
//!
//! The operator UI token is a bearer credential: it must not sit in
//! plaintext on disk. On macOS it lives in the user's login Keychain
//! (encrypted at rest, OS-managed ACLs). Other platforms fall back to
//! the legacy 0600 file next to the database until a platform store
//! lands (Windows: Credential Manager; Linux: secret service) — the
//! trait seam exists so those arrive as new implementations.

use std::path::Path;

/// Storage for one operator-facing token, keyed by account.
///
/// `Send + Sync` so callers can run blocking platform operations off
/// the async runtime with a bounded wait.
pub trait TokenStore: Send + Sync {
    /// Loads the token for `account`, or `None` when absent.
    ///
    /// # Errors
    ///
    /// Returns the platform store's failure message.
    fn load(&self, account: &str) -> Result<Option<String>, String>;

    /// Stores (or replaces) the token for `account`.
    ///
    /// # Errors
    ///
    /// Returns the platform store's failure message.
    fn store(&self, account: &str, token: &str) -> Result<(), String>;

    /// Removes the token for `account`; a missing entry is not an error.
    ///
    /// # Arguments
    ///
    /// * `account` — the account whose token is removed.
    ///
    /// # Errors
    ///
    /// Returns the store failure.
    fn delete(&self, account: &str) -> Result<(), String>;
}

/// Builds the platform store for this host.
pub fn platform_store() -> Box<dyn TokenStore> {
    #[cfg(target_os = "macos")]
    {
        Box::new(KeychainStore { service: "wrkspc" })
    }
    #[cfg(not(target_os = "macos"))]
    {
        Box::new(FileStore)
    }
}

/// Runs a blocking platform-store operation with a bounded wait. A
/// pending macOS keychain approval dialog blocks the underlying call
/// indefinitely; that must surface as a fast, loud failure — never a
/// silently hung node startup or CLI.
///
/// # Arguments
///
/// * `label` — what was being attempted (for the timeout message).
/// * `operation` — the blocking store call to bound.
///
/// # Errors
///
/// Returns the store's own error, or the timeout message.
async fn bounded_store_op<T, F>(label: &str, operation: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::task::spawn_blocking(operation),
    )
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(join_error)) => Err(format!("{label} failed: {join_error}")),
        Err(_) => Err(format!(
            "{label} timed out — a macOS keychain approval dialog may be pending; answer it or set WRKSPC_UI_TOKEN"
        )),
    }
}

/// The operator UI token: the environment override wins, otherwise the
/// platform store keyed by the node's `identity` (macOS: a Keychain
/// entry named for the node, e.g. `wrkspc.<host>`; elsewhere: a 0600
/// file next to `db`), migrating any legacy plaintext file or
/// database-path-keyed Keychain entry into it, otherwise a freshly
/// generated token.
///
/// # Arguments
///
/// * `identity` — the node's stable name (its host id, or `default`).
/// * `db` — the canonical database path (legacy-file migration key).
///
/// # Errors
///
/// Returns the store or filesystem failure.
pub async fn resolve_ui_token(identity: &str, db: &Path) -> Result<String, String> {
    if let Some(token) = std::env::var("WRKSPC_UI_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
    {
        return Ok(token);
    }
    let account = ui_token_account(identity);
    if let Some(token) =
        bounded_store_op("keychain read", move || platform_store().load(&account)).await?
    {
        return Ok(token);
    }
    // Migrate a legacy plaintext file into the platform store, then
    // remove it so the token never lingers on disk in the clear.
    let legacy = std::path::PathBuf::from(format!("{}.ui-token", db.display()));
    if let Ok(contents) = tokio::fs::read_to_string(&legacy).await {
        let token = contents.trim().to_string();
        if !token.is_empty() {
            let stored = token.clone();
            let account = ui_token_account(identity);
            bounded_store_op("keychain write", move || {
                platform_store().store(&account, &stored)
            })
            .await?;
            let _ = tokio::fs::remove_file(&legacy).await;
            return Ok(token);
        }
    }
    // Migrate a database-path-keyed Keychain entry (pre-host-identity
    // builds) into the node-named entry, then drop the old one.
    #[cfg(target_os = "macos")]
    {
        let legacy_account = format!("{}.ui-token", db.display());
        let legacy_for_load = legacy_account.clone();
        let account = ui_token_account(identity);
        if let Some(token) = bounded_store_op("keychain read", move || {
            platform_store().load(&legacy_for_load)
        })
        .await?
        {
            let stored = token.clone();
            bounded_store_op("keychain write", move || {
                platform_store().store(&account, &stored)
            })
            .await?;
            // Best-effort cleanup of the migrated entry.
            match bounded_store_op("keychain delete", move || {
                platform_store().delete(&legacy_account)
            })
            .await
            {
                Ok(()) => {}
                Err(error) => tracing::warn!(%error, "legacy keychain entry cleanup failed"),
            }
            return Ok(token);
        }
    }
    let token = uuid::Uuid::new_v4().to_string();
    let stored = token.clone();
    let account = ui_token_account(identity);
    bounded_store_op("keychain write", move || {
        platform_store().store(&account, &stored)
    })
    .await?;
    Ok(token)
}

/// The platform-store account key for a node's UI token. macOS: the
/// Keychain account name (`wrkspc.<node>`). Non-macOS: the 0600 token
/// file path next to the database.
pub fn ui_token_account(identity: &str) -> String {
    #[cfg(target_os = "macos")]
    {
        format!("wrkspc.{identity}")
    }
    #[cfg(not(target_os = "macos"))]
    {
        format!("{identity}.ui-token")
    }
}

/// `wrkspc ui-token`: copies the operator UI token to the clipboard from
/// the platform secret store. macOS: reads the Keychain and pipes the
/// token into `pbcopy` via stdin (never a process argument), then clears
/// the clipboard after 30 seconds — and only when it still holds the
/// token. Runs with NO arguments normally (the stored token is found in
/// the keychain); `--node <name>` picks one explicitly when several
/// nodes have tokens. Other platforms: prints the 0600 token file path
/// (selectable with `--db`).
///
/// # Arguments
///
/// * `node` — the optional node name (macOS Keychain selector).
/// * `db` — the optional database path (non-macOS file selector).
///
/// # Errors
///
/// Returns the store failure or the missing-token message.
pub async fn run_ui_token(
    node: Option<String>,
    db: Option<std::path::PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    let account =
        resolve_ui_token_account(node, db).map_err(Into::<Box<dyn std::error::Error>>::into)?;
    #[cfg(target_os = "macos")]
    {
        let account_for_load = account.clone();
        let token = bounded_store_op("keychain read", move || {
            platform_store().load(&account_for_load)
        })
        .await?
        .ok_or_else(|| format!("no UI token stored for {} — start the node first", account))?;
        use std::io::Write;
        let mut child = std::process::Command::new("pbcopy")
            .stdin(std::process::Stdio::piped())
            .spawn()
            .map_err(|error| format!("cannot spawn pbcopy: {error}"))?;
        child
            .stdin
            .take()
            .expect("piped stdin was requested")
            .write_all(token.as_bytes())?;
        child.wait()?;
        // The clearer only wipes the clipboard when it still holds OUR
        // token — anything the operator copied after the token stays
        // untouched. The token rides the pipe (never an argv, never a
        // temp file): the child sleeps 30s, then compares the current
        // pasteboard against the pipe and clears only on a match.
        let mut clearer = std::process::Command::new("sh")
            .args([
                "-c",
                "sleep 30; [ \"$(pbpaste)\" = \"$(cat)\" ] && pbcopy < /dev/null",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .ok();
        if let Some(mut stdin) = clearer.as_mut().and_then(|child| child.stdin.take()) {
            let _ = stdin.write_all(token.as_bytes());
        }
        println!(
            "token copied to the clipboard — it clears itself in 30s (only if it's still the token); paste it into the wrkspc login screen"
        );
    }
    #[cfg(not(target_os = "macos"))]
    {
        let account_for_load = account.clone();
        if bounded_store_op("token read", move || {
            platform_store().load(&account_for_load)
        })
        .await?
        .is_none()
        {
            return Err(
                format!("no UI token stored for {} — start the node first", account).into(),
            );
        }
        println!("token is stored at {} (mode 0600)", account);
    }
    Ok(())
}

/// Resolves which stored token `wrkspc ui-token` should copy. With an
/// explicit `--node`, the account is that node's Keychain name. Without
/// one, the keychain is enumerated: a single stored token is used
/// directly; none or several produce a clear error naming `--node`.
///
/// # Arguments
///
/// * `node` — the optional `--node` name.
/// * `db` — ignored on macOS (kept for signature symmetry).
///
/// # Errors
///
/// Returns the missing/ambiguous-token message.
#[cfg(target_os = "macos")]
fn resolve_ui_token_account(
    node: Option<String>,
    db: Option<std::path::PathBuf>,
) -> Result<String, String> {
    let _ = db;
    match node {
        Some(node) => Ok(ui_token_account(&node)),
        None => match keychain_accounts("wrkspc")?.as_slice() {
            [] => Err("no wrkspc UI token stored — start the node first".to_string()),
            [single] => Ok(single.clone()),
            many => {
                let mut message =
                    "multiple wrkspc nodes have tokens; name one with --node:\n".to_string();
                for account in many {
                    message.push_str("  ");
                    message.push_str(account.strip_prefix("wrkspc.").unwrap_or(account));
                    message.push('\n');
                }
                Err(message)
            }
        },
    }
}

/// Non-macOS: the account is the 0600 token file next to the database
/// (`node.db` in the current directory by default).
#[cfg(not(target_os = "macos"))]
fn resolve_ui_token_account(
    node: Option<String>,
    db: Option<std::path::PathBuf>,
) -> Result<String, String> {
    let _ = node;
    let db =
        crate::util::canonical_db_path(db.as_deref().unwrap_or(std::path::Path::new("node.db")))
            .map_err(|error| error.to_string())?;
    Ok(ui_token_account(&db.display().to_string()))
}

#[cfg(target_os = "macos")]
struct KeychainStore {
    service: &'static str,
}

#[cfg(target_os = "macos")]
fn keychain_accounts(service: &str) -> Result<Vec<String>, String> {
    use core_foundation::array::{CFArray, CFArrayRef};
    use core_foundation::base::{CFType, TCFType};
    use core_foundation::boolean::CFBoolean;
    use core_foundation::dictionary::CFDictionary;
    use core_foundation::string::CFString;
    use security_framework_sys::base::errSecItemNotFound;
    use security_framework_sys::item::{
        kSecAttrAccount, kSecAttrService, kSecClass, kSecClassGenericPassword, kSecMatchLimit,
        kSecMatchLimitAll, kSecReturnAttributes,
    };
    use security_framework_sys::keychain_item::SecItemCopyMatching;

    let query = unsafe {
        CFDictionary::from_CFType_pairs(&[
            (
                CFType::wrap_under_get_rule(kSecClass as _),
                CFType::wrap_under_get_rule(kSecClassGenericPassword as _),
            ),
            (
                CFType::wrap_under_get_rule(kSecAttrService as _),
                CFString::new(service).into_CFType(),
            ),
            (
                CFType::wrap_under_get_rule(kSecMatchLimit as _),
                CFType::wrap_under_get_rule(kSecMatchLimitAll as _),
            ),
            (
                CFType::wrap_under_get_rule(kSecReturnAttributes as _),
                CFBoolean::true_value().into_CFType(),
            ),
        ])
    };
    let mut result: core_foundation::base::CFTypeRef = std::ptr::null();
    let status = unsafe { SecItemCopyMatching(query.as_concrete_TypeRef(), &mut result) };
    if status == errSecItemNotFound {
        return Ok(Vec::new());
    }
    if status != 0 {
        return Err(format!("keychain search failed: {status}"));
    }
    let array = unsafe { CFArray::<CFDictionary>::wrap_under_create_rule(result as CFArrayRef) };
    let mut accounts = Vec::new();
    for entry in array.iter() {
        if let Some(value) = entry.find(unsafe { kSecAttrAccount } as *const _) {
            let account = unsafe { CFString::wrap_under_get_rule(*value as _) }.to_string();
            accounts.push(account);
        }
    }
    Ok(accounts)
}

#[cfg(target_os = "macos")]
impl TokenStore for KeychainStore {
    fn load(&self, account: &str) -> Result<Option<String>, String> {
        match security_framework::passwords::get_generic_password(self.service, account) {
            Ok(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into_owned())),
            Err(error) if error.code() == security_framework_sys::base::errSecItemNotFound => {
                Ok(None)
            }
            Err(error) => Err(format!("keychain read failed: {error}")),
        }
    }

    fn store(&self, account: &str, token: &str) -> Result<(), String> {
        security_framework::passwords::set_generic_password(self.service, account, token.as_bytes())
            .map_err(|error| format!("keychain write failed: {error}"))
    }

    fn delete(&self, account: &str) -> Result<(), String> {
        match security_framework::passwords::delete_generic_password(self.service, account) {
            Ok(()) => Ok(()),
            Err(error) if error.code() == security_framework_sys::base::errSecItemNotFound => {
                Ok(())
            }
            Err(error) => Err(format!("keychain delete failed: {error}")),
        }
    }
}

#[cfg(not(target_os = "macos"))]
struct FileStore;

#[cfg(not(target_os = "macos"))]
impl TokenStore for FileStore {
    fn load(&self, account: &str) -> Result<Option<String>, String> {
        let path = std::path::PathBuf::from(account);
        match std::fs::read_to_string(&path) {
            Ok(token) if !token.trim().is_empty() => Ok(Some(token.trim().to_string())),
            Ok(_) => Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(format!("cannot read {path:?}: {error}")),
        }
    }

    fn store(&self, account: &str, token: &str) -> Result<(), String> {
        let path = std::path::PathBuf::from(account);
        std::fs::write(&path, token).map_err(|error| format!("cannot write {path:?}: {error}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .map_err(|error| format!("cannot restrict {path:?}: {error}"))?;
        }
        Ok(())
    }

    fn delete(&self, account: &str) -> Result<(), String> {
        let path = std::path::PathBuf::from(account);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("cannot remove {path:?}: {error}")),
        }
    }
}
