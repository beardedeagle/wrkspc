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
/// `label` is what was being attempted (for the timeout message);
/// `operation` is the blocking store call to bound.
///
/// # Errors
///
/// Returns the store's own error. A TIMEOUT ENDS THE PROCESS (CU-01):
/// a started blocking task cannot be aborted and the runtime would
/// otherwise wait on it indefinitely at shutdown, defeating the
/// service's retry loop. The error is reported and the process exits
/// immediately — the service manager restarts it, and the next start
/// re-uses the now-answered dialog.
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
        Err(_) => {
            tracing::error!(
                "{label} timed out — a macOS keychain approval dialog may be pending; answer it or set WRKSPC_UI_TOKEN; exiting so the service can restart"
            );
            // Flush the tracing layer so the message survives the exit.
            std::thread::sleep(std::time::Duration::from_millis(200));
            std::process::exit(1);
        }
    }
}

/// Loads a legacy `<db>.ui-token` file for migration. A symlink or a
/// non-regular file is refused, never followed (CU-05: only the
/// intended plaintext file may be consumed).
async fn load_legacy_token_file(path: &std::path::Path) -> Result<Option<String>, String> {
    let meta = match tokio::fs::symlink_metadata(path).await {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(format!(
                "cannot stat legacy token file {}: {e}",
                path.display()
            ));
        }
    };
    if meta.file_type().is_symlink() || !meta.is_file() {
        tracing::warn!(
            "legacy token path {} is not a regular file — leaving it alone",
            path.display()
        );
        return Ok(None);
    }
    let contents = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| format!("cannot read legacy token file {}: {e}", path.display()))?;
    let token = contents.trim().to_string();
    Ok((!token.is_empty()).then_some(token))
}

/// Removes the legacy plaintext token file, but ONLY when it still
/// holds the value now stored elsewhere (CU-05): a file that changed
/// since migration is left alone with a warning, and every failure —
/// stat, read, mismatch, removal — is logged, never silent.
async fn cleanup_legacy_token_file(path: &std::path::Path, stored: &str) {
    let Some(current) = (match load_legacy_token_file(path).await {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(%error, "legacy token cleanup failed");
            return;
        }
    }) else {
        // Already gone (or refused as a link) — nothing to do.
        return;
    };
    if current != stored {
        tracing::warn!(
            "legacy token file {} holds a different value than the store — leaving it for the operator to inspect",
            path.display()
        );
        return;
    }
    if let Err(error) = tokio::fs::remove_file(path).await {
        tracing::warn!(
            %error,
            "legacy token file cleanup failed — the credential remains on disk; it will be retried on the next start"
        );
    }
}

/// Reads the operator token from an explicit file (the deploy wrapper's
/// tmpfs handoff). Read-only and fail-closed: a missing file, an empty
/// file, a symlink, or a non-regular file is an error — never a silent
/// token generation, so a restart cannot invent a credential the
/// wrapper did not supply.
///
/// `path` is the explicit token file.
///
/// # Errors
///
/// Returns the stat/read failure or the refusal message.
pub async fn resolve_ui_token_from_file(path: &Path) -> Result<String, String> {
    let meta = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|e| format!("cannot stat token file {}: {e}", path.display()))?;
    if meta.file_type().is_symlink() || !meta.is_file() {
        return Err(format!(
            "token file {} is not a regular file — refusing to read it",
            path.display()
        ));
    }
    let token = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| format!("cannot read token file {}: {e}", path.display()))?;
    let token = token.trim().to_string();
    if token.is_empty() {
        return Err(format!(
            "token file {} is empty — refusing to generate a replacement",
            path.display()
        ));
    }
    Ok(token)
}

/// The per-peer MCP bearer token for the node's loopback HTTP listener.
/// macOS: the Keychain (service `wrkspc.mcp`, account = peer id); the
/// legacy `<db>.<peer>.mcp-token` sidecar is migrated into the Keychain
/// and removed only after a readback proves the stored value matches.
/// Other platforms: the audited 0600 sidecar. `WRKSPC_MCP_TOKEN` (a
/// test/emergency pin, same role as the UI-token override) wins on
/// every platform.
///
/// `db` is the node database (legacy sidecar location); `peer` is the
/// binding's peer id.
///
/// # Errors
///
/// Returns the store or filesystem failure.
pub async fn mcp_token_for_peer(db: &Path, peer: &str) -> Result<String, String> {
    if let Some(token) = std::env::var("WRKSPC_MCP_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
    {
        return Ok(token);
    }
    #[cfg(target_os = "macos")]
    {
        let account = format!("wrkspc.mcp.{peer}");
        let legacy = std::path::PathBuf::from(format!("{}.{peer}.mcp-token", db.display()));
        let read_account = account.clone();
        if let Some(token) = bounded_store_op("keychain read", move || {
            mcp_platform_store().load(&read_account)
        })
        .await?
        {
            cleanup_legacy_token_file(&legacy, &token).await;
            return Ok(token);
        }
        if let Some(token) = load_legacy_token_file(&legacy).await? {
            let stored = token.clone();
            let store_account = account.clone();
            bounded_store_op("keychain write", move || {
                mcp_platform_store().store(&store_account, &stored)
            })
            .await?;
            // Readback proves the migration before the sidecar dies.
            let readback_account = account.clone();
            let readback = bounded_store_op("keychain read", move || {
                mcp_platform_store().load(&readback_account)
            })
            .await?;
            return match readback {
                Some(value) if value == token => {
                    cleanup_legacy_token_file(&legacy, &token).await;
                    Ok(token)
                }
                _ => Err(
                    "mcp token migration failed readback — leaving the sidecar in place"
                        .to_string(),
                ),
            };
        }
        let token = uuid::Uuid::new_v4().to_string();
        let stored = token.clone();
        let store_account = account.clone();
        bounded_store_op("keychain write", move || {
            mcp_platform_store().store(&store_account, &stored)
        })
        .await?;
        Ok(token)
    }
    #[cfg(not(target_os = "macos"))]
    {
        crate::util::stable_token(db, &format!("{peer}.mcp-token")).await
    }
}

/// The macOS Keychain store for per-peer MCP bearer tokens: a separate
/// service so `wrkspc ui-token` enumeration (service `wrkspc`) never
/// surfaces MCP tokens.
#[cfg(target_os = "macos")]
pub fn mcp_platform_store() -> Box<dyn TokenStore> {
    Box::new(KeychainStore {
        service: "wrkspc.mcp",
    })
}

/// The operator UI token: an explicit token file wins (the deploy
/// wrapper's tmpfs handoff), otherwise the environment override,
/// otherwise the platform store keyed by the node's `identity` (macOS:
/// a Keychain entry named for the node, e.g. `wrkspc.<host>`; elsewhere:
/// a 0600 file next to `db`), migrating any legacy plaintext file or
/// database-path-keyed Keychain entry into it, otherwise a freshly
/// generated token.
///
/// `identity` is the node's stable name (its host id, or `default`);
/// `db` is the canonical database path (legacy-file migration key);
/// `file` is the optional explicit token file (`--ui-token-file`).
///
/// # Errors
///
/// Returns the store or filesystem failure.
pub async fn resolve_ui_token(
    identity: &str,
    db: &Path,
    file: Option<&Path>,
) -> Result<String, String> {
    if let Some(path) = file {
        return resolve_ui_token_from_file(path).await;
    }
    if let Some(token) = std::env::var("WRKSPC_UI_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
    {
        return Ok(token);
    }
    let account = token_account(identity, db);
    let legacy = std::path::PathBuf::from(format!("{}.ui-token", db.display()));
    if let Some(token) =
        bounded_store_op("keychain read", move || platform_store().load(&account)).await?
    {
        // Retry the legacy-file cleanup on EVERY start (CU-05): a
        // previous migration whose deletion failed (read-only
        // directory) must not leave the live credential on disk
        // forever — the Keychain-hit return used to precede all
        // cleanup, so the failure was never revisited.
        // macOS only: on file-store platforms the legacy path IS the
        // platform store's own file, and "cleaning" it deletes the
        // live credential — the next resolve_ui_token call generates
        // a fresh token, and the hub's two listeners (UI and A2A)
        // diverge, each accepting a different token.
        #[cfg(target_os = "macos")]
        cleanup_legacy_token_file(&legacy, &token).await;
        return Ok(token);
    }
    // Migrate a legacy plaintext file into the platform store, then
    // remove it so the token never lingers on disk in the clear.
    if let Some(token) = load_legacy_token_file(&legacy).await? {
        let stored = token.clone();
        let account = token_account(identity, db);
        bounded_store_op("keychain write", move || {
            platform_store().store(&account, &stored)
        })
        .await?;
        cleanup_legacy_token_file(&legacy, &token).await;
        return Ok(token);
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
    let account = token_account(identity, db);
    bounded_store_op("keychain write", move || {
        platform_store().store(&account, &stored)
    })
    .await?;
    // A concurrent first-start may have won the create race: the
    // STORED value is the credential the UI will accept, so it is
    // what this caller reports (CU-02 — every caller agrees on one
    // stable value).
    let account = token_account(identity, db);
    if let Some(existing) =
        bounded_store_op("keychain read", move || platform_store().load(&account)).await?
    {
        return Ok(existing);
    }
    Ok(token)
}

/// The account `resolve_ui_token` reads and writes.
///
/// macOS: the Keychain account named for the node (`wrkspc.<node>`).
/// Non-macOS: the 0600 token file next to the database, keyed by its
/// CANONICAL path — the same key `wrkspc ui-token` reads. Keying the
/// node side by another string (the node identity) split the two, and
/// `wrkspc ui-token` reported no token for a node that just stored one.
///
/// `identity` is the node's stable name (macOS account); `db` is the
/// canonical database path (non-macOS file key).
fn token_account(identity: &str, db: &Path) -> String {
    #[cfg(target_os = "macos")]
    {
        let _ = db;
        ui_token_account(identity)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = identity;
        let canonical = crate::util::canonical_db_path(db).unwrap_or_else(|_| db.to_path_buf());
        ui_token_account(&canonical.display().to_string())
    }
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
/// nodes have tokens. `--file <path>` reads an explicit token file (the
/// deploy wrapper's tmpfs handoff) and delivers it the same way; on
/// non-macOS the explicit token is printed (an explicit operator ask,
/// never a default). Other platforms without `--file`: reports the 0600
/// token file path (selectable with `--db`), and names the deploy
/// wrapper's retrieval path when no store file exists.
///
/// `node` is the optional node name (macOS Keychain selector); `db` is
/// the optional database path (non-macOS file selector); `file` is the
/// optional explicit token file.
///
/// # Errors
///
/// Returns the store failure or the missing-token message.
pub async fn run_ui_token(
    node: Option<String>,
    db: Option<std::path::PathBuf>,
    file: Option<std::path::PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    // The explicit token file mirrors the runtime precedence: it wins
    // over the environment override and the platform store.
    if let Some(path) = &file {
        let token = resolve_ui_token_from_file(path).await?;
        #[cfg(target_os = "macos")]
        {
            let scheduled = copy_to_clipboard(&token)?;
            println!(
                "token from {} copied to the clipboard — {}; paste it into the wrkspc login screen",
                path.display(),
                clear_promise(scheduled)
            );
        }
        #[cfg(not(target_os = "macos"))]
        {
            // An explicit --file request is an explicit ask for the
            // value; the operator pastes it into the login card.
            println!("{token}");
        }
        return Ok(());
    }
    // The environment override has the SAME precedence here as at
    // runtime (CU-06): with WRKSPC_UI_TOKEN set, that token is the
    // active credential and is copied/reported AS SUCH — the stored
    // entry is never presented as authoritative, and the override is
    // never written into persistent storage as a side effect.
    if let Some(override_token) = std::env::var("WRKSPC_UI_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
    {
        #[cfg(target_os = "macos")]
        {
            let scheduled = copy_to_clipboard(&override_token)?;
            println!(
                "the active token (from WRKSPC_UI_TOKEN) was copied to the clipboard — {}",
                clear_promise(scheduled)
            );
        }
        #[cfg(not(target_os = "macos"))]
        {
            // The token value itself is never printed: it is a
            // credential, and the message reports only its SOURCE.
            let _ = override_token;
            println!(
                "the active token comes from the WRKSPC_UI_TOKEN environment override (no stored file is authoritative while it is set)"
            );
        }
        return Ok(());
    }
    let account = resolve_ui_token_account(node, db)
        .await
        .map_err(Into::<Box<dyn std::error::Error>>::into)?;
    #[cfg(target_os = "macos")]
    {
        let account_for_load = account.clone();
        let token = bounded_store_op("keychain read", move || {
            platform_store().load(&account_for_load)
        })
        .await?
        .ok_or_else(|| format!("no UI token stored for {} — start the node first", account))?;
        let scheduled = copy_to_clipboard(&token)?;
        println!(
            "token copied to the clipboard — {}; paste it into the wrkspc login screen",
            clear_promise(scheduled)
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
            return Err(format!(
                "no UI token stored at {account} — a wrapper-managed deployment seals the credential on the host (tpm2 unseal to a tmpfs mount); otherwise start the node first, or read an explicit token file with `wrkspc ui-token --file <path>`"
            )
            .into());
        }
        println!("token is stored at {} (mode 0600)", account);
    }
    Ok(())
}

/// The self-clear promise, phrased only when it was actually
/// scheduled (CU-07): a copy whose clearer failed must not advertise
/// an automatic clear that will never run.
#[cfg(target_os = "macos")]
fn clear_promise(scheduled: bool) -> &'static str {
    if scheduled {
        "it clears itself in 30s (only if it's still the token)"
    } else {
        "NOTE: the automatic 30s clear could not be scheduled — clear the clipboard manually after pasting"
    }
}

/// Copies a token into the pasteboard through `pbcopy` and schedules
/// the 30-second self-clear — reporting the ACTUAL outcomes (CU-07):
/// a failing copy is an error, and the caller learns whether the
/// self-clear was actually scheduled.
#[cfg(target_os = "macos")]
fn copy_to_clipboard(token: &str) -> Result<bool, Box<dyn std::error::Error>> {
    use std::io::Write;
    let mut child = std::process::Command::new("pbcopy")
        .stdin(std::process::Stdio::piped())
        .spawn()
        .map_err(|error| format!("cannot spawn pbcopy: {error}"))?;
    // Stdio::piped() was requested above, so the handle is present by
    // construction; a missing handle is reported as an error, never a
    // panic.
    let mut stdin = child
        .stdin
        .take()
        .ok_or("piped stdin was requested but not provided")?;
    stdin.write_all(token.as_bytes())?;
    // Close the pipe BEFORE waiting: pbcopy drains stdin to EOF, and
    // an open handle here deadlocks the wait forever.
    drop(stdin);
    let status = child.wait()?;
    if !status.success() {
        return Err(format!("pbcopy failed with {status} — the token was NOT copied").into());
    }
    // The clearer only wipes the clipboard when it still holds OUR
    // token — anything the operator copied after the token stays
    // untouched. The token rides the pipe (never an argv, never a
    // temp file): the child sleeps 30s, then compares the current
    // pasteboard against the pipe and clears only on a match.
    let clearer = std::process::Command::new("sh")
        .args([
            "-c",
            "sleep 30; [ \"$(pbpaste)\" = \"$(cat)\" ] && pbcopy < /dev/null",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
    match clearer {
        Ok(mut clearer) => {
            if let Some(mut stdin) = clearer.stdin.take()
                && stdin.write_all(token.as_bytes()).is_err()
            {
                tracing::warn!(
                    "could not hand the token to the clipboard clearer — the clipboard will not self-clear"
                );
                return Ok(false);
            }
            Ok(true)
        }
        Err(error) => {
            tracing::warn!(%error, "could not schedule the clipboard clearer — the clipboard will not self-clear");
            Ok(false)
        }
    }
}

/// Resolves which stored token `wrkspc ui-token` should copy. With an
/// explicit `--node`, the account is that node's Keychain name. Without
/// one, the keychain is enumerated: a single stored token is used
/// directly; none or several produce a clear error naming `--node`.
///
/// `node` is the optional `--node` name; `db` is ignored on macOS (kept
/// for signature symmetry).
///
/// # Errors
///
/// Returns the missing/ambiguous-token message.
#[cfg(target_os = "macos")]
async fn resolve_ui_token_account(
    node: Option<String>,
    db: Option<std::path::PathBuf>,
) -> Result<String, String> {
    let _ = db;
    match node {
        Some(node) => Ok(ui_token_account(&node)),
        // Enumeration is a Keychain operation and gets the SAME
        // process-bounding timeout as every other store call (CU-01).
        None => match bounded_store_op("keychain search", move || keychain_accounts("wrkspc"))
            .await?
            .as_slice()
        {
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
async fn resolve_ui_token_account(
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
        // Test seam (CU-01): debug builds honor
        // WRKSPC_TEST_KEYCHAIN_BLOCK_MS so the process-exit bound is
        // testable without a real approval dialog. Release binaries
        // never carry the branch.
        #[cfg(debug_assertions)]
        if let Some(millis) = std::env::var("WRKSPC_TEST_KEYCHAIN_BLOCK_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
        {
            std::thread::sleep(std::time::Duration::from_millis(millis));
        }
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
        // The audited helper (CU-02): symlinks refused, foreign
        // owners refused, permissive modes tightened, read errors
        // reported — never folded into "absent, generate".
        crate::util::load_existing_token_sync(&path)
    }

    fn store(&self, account: &str, token: &str) -> Result<(), String> {
        let path = std::path::PathBuf::from(account);
        // Reuse the audited create path (CU-02): O_NOFOLLOW, 0600 at
        // open, synced before returning, symlink/owner/mode policy on
        // the pre-existing file; an existing stable value WINS — the
        // store never rotates a token another caller already wrote.
        // The settling read tolerates a concurrent creator's
        // publication window (file exists, bytes not yet written).
        if crate::util::load_existing_token_settling(&path)?.is_some() {
            return Ok(());
        }
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
                // Windows: the owner-only, inheritance-free DACL lands
                // BEFORE the first secret byte (WRKSPC-011); Unix gets
                // 0600 from the open above.
                #[cfg(windows)]
                crate::util::win_acl::set_owner_only_acl(&file)?;
                use std::io::Write;
                file.write_all(token.as_bytes())
                    .map_err(|e| format!("cannot write {path:?}: {e}"))?;
                file.sync_all()
                    .map_err(|e| format!("cannot sync {path:?}: {e}"))?;
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // A concurrent creator won: its value stands. The
                // settling read waits out the winner's write window.
                if crate::util::load_existing_token_settling(&path)?.is_some() {
                    Ok(())
                } else {
                    Err(format!(
                        "token file {path:?} appeared during creation — refusing to rotate it"
                    ))
                }
            }
            Err(e) => Err(format!("cannot create {path:?}: {e}")),
        }
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

#[cfg(all(test, not(target_os = "macos")))]
mod tests {
    use super::resolve_ui_token;
    use std::path::PathBuf;

    /// Two resolves against the same database must return the SAME
    /// token and leave the 0600 store file in place (CU-02): the hub
    /// calls resolve_ui_token once per listener, and a token that
    /// rotates between the calls splits the UI and A2A surfaces.
    #[tokio::test]
    async fn repeated_resolve_keeps_one_token() {
        let dir = std::env::temp_dir().join(format!(
            "wrkspc-ui-token-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let db = dir.join("hub.db");
        let first = resolve_ui_token("hub", &db, None)
            .await
            .expect("first resolve");
        let second = resolve_ui_token("hub", &db, None)
            .await
            .expect("second resolve");
        assert_eq!(first, second, "the store never rotates a settled token");
        let account = super::ui_token_account(&db.display().to_string());
        let path = PathBuf::from(account);
        assert!(
            path.exists(),
            "the live credential file must survive the second resolve"
        );
        assert_eq!(
            std::fs::read_to_string(&path)
                .expect("token readable")
                .trim(),
            first,
            "the file keeps holding the accepted token"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An explicit token file outranks the platform store — the
    /// wrapper's tmpfs handoff is the deployment's chosen credential,
    /// never a fallback behind a stored one.
    #[tokio::test]
    async fn explicit_file_outranks_the_platform_store() {
        let dir = std::env::temp_dir().join(format!(
            "wrkspc-ui-token-file-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let db = dir.join("hub.db");
        let file = dir.join("token.file");
        std::fs::write(&file, "file-token\n").expect("write token file");
        // A stored token exists (generated on the store path) and must
        // lose to the explicit file.
        let store_token = resolve_ui_token("hub", &db, None)
            .await
            .expect("store resolve");
        assert_ne!(store_token, "file-token", "test setup: distinct values");
        let resolved = resolve_ui_token("hub", &db, Some(&file))
            .await
            .expect("file resolve");
        assert_eq!(resolved, "file-token", "the explicit file must win");
        let sidecar = PathBuf::from(format!("{}.ui-token", db.display()));
        assert_eq!(
            std::fs::read_to_string(&sidecar)
                .expect("sidecar readable")
                .trim(),
            store_token,
            "the store token is left untouched by the file resolve"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A missing token file is a loud failure, never a silent
    /// generation: a restart must not invent a credential the wrapper
    /// did not supply.
    #[tokio::test]
    async fn missing_file_fails_closed() {
        let dir = std::env::temp_dir().join(format!(
            "wrkspc-ui-token-missing-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let db = dir.join("hub.db");
        let file = dir.join("absent.token");
        let error = resolve_ui_token("hub", &db, Some(&file))
            .await
            .expect_err("a missing token file must fail");
        assert!(
            error.contains("cannot stat token file"),
            "unexpected error: {error}"
        );
        assert!(
            !PathBuf::from(format!("{}.ui-token", db.display())).exists(),
            "a failed file resolve must not create a store sidecar"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An empty token file is a loud failure, never a silent
    /// generation (the audited file-store rule applied to the explicit
    /// source).
    #[tokio::test]
    async fn empty_file_fails_closed() {
        let dir = std::env::temp_dir().join(format!(
            "wrkspc-ui-token-empty-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let db = dir.join("hub.db");
        let file = dir.join("empty.token");
        std::fs::write(&file, "   \n").expect("write empty file");
        let error = resolve_ui_token("hub", &db, Some(&file))
            .await
            .expect_err("an empty token file must fail");
        assert!(error.contains("empty"), "unexpected error: {error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A symlinked token file is refused, never followed.
    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_file_is_refused() {
        let dir = std::env::temp_dir().join(format!(
            "wrkspc-ui-token-link-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let db = dir.join("hub.db");
        let target = dir.join("real.token");
        std::fs::write(&target, "real-token\n").expect("write target");
        let link = dir.join("link.token");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");
        let error = resolve_ui_token("hub", &db, Some(&link))
            .await
            .expect_err("a symlinked token file must be refused");
        assert!(
            error.contains("not a regular file"),
            "unexpected error: {error}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Resolving from an explicit file never writes the platform
    /// store: the wrapper owns this credential end to end.
    #[tokio::test]
    async fn file_resolve_never_writes_the_sidecar() {
        let dir = std::env::temp_dir().join(format!(
            "wrkspc-ui-token-nowrite-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let db = dir.join("hub.db");
        let file = dir.join("token.file");
        std::fs::write(&file, "file-token\n").expect("write token file");
        let resolved = resolve_ui_token("hub", &db, Some(&file))
            .await
            .expect("file resolve");
        assert_eq!(resolved, "file-token");
        let sidecar = PathBuf::from(format!("{}.ui-token", db.display()));
        assert!(
            !sidecar.exists(),
            "an explicit token file must never spawn a store sidecar"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
