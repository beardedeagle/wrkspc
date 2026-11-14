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
/// `path` is the database path as given.
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
/// `path` is the sidecar path.
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
    match read_token_file(path)? {
        TokenFileRead::Missing => Ok(None),
        TokenFileRead::Empty => Err(empty_token_error(path)),
        TokenFileRead::Value(token) => Ok(Some(token)),
    }
}

/// The read state of one token file, split so create-contention flows
/// can distinguish an empty file caught mid-publication from a settled
/// empty credential without weakening the strict loader.
enum TokenFileRead {
    /// The file does not exist.
    Missing,
    /// The file exists but holds no token bytes.
    Empty,
    /// The file holds a token.
    Value(String),
}

/// Reads and validates an existing token file. Every refusal rule
/// (symlink, non-regular, foreign owner, permissive mode, Windows
/// inherited ACL) is applied here so all readers share one policy.
fn read_token_file(path: &Path) -> Result<TokenFileRead, String> {
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
            #[cfg(windows)]
            win_acl::validate_and_tighten_existing(path)?;
            let contents = std::fs::read_to_string(path)
                .map_err(|e| format!("cannot read token file {}: {e}", path.display()))?;
            let token = contents.trim().to_string();
            if token.is_empty() {
                Ok(TokenFileRead::Empty)
            } else {
                Ok(TokenFileRead::Value(token))
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(TokenFileRead::Missing),
        Err(e) => Err(format!("cannot stat token file {}: {e}", path.display())),
    }
}

fn empty_token_error(path: &Path) -> String {
    format!(
        "token file {} exists but is empty — remove it to generate a token",
        path.display()
    )
}

/// How many times a create-contention reader re-checks an EMPTY token
/// file before treating it as a settled empty credential. Between a
/// concurrent creator's exclusive open and its write, the file exists
/// but holds no bytes — that window is publication, not policy input.
/// A file that is STILL empty once the window passes fails closed,
/// exactly like the strict loader.
const PUBLICATION_SETTLE_ATTEMPTS: u32 = 20;
const PUBLICATION_SETTLE_SLEEP: std::time::Duration = std::time::Duration::from_millis(10);

/// Reads an existing token, waiting out a concurrent creator's
/// publication window. Used by the exclusive-create flows where
/// another caller may be between `create_new` and its first write.
pub(crate) fn load_existing_token_settling(path: &Path) -> Result<Option<String>, String> {
    let mut attempt: u32 = 0;
    loop {
        match read_token_file(path)? {
            TokenFileRead::Empty if attempt < PUBLICATION_SETTLE_ATTEMPTS => {
                attempt += 1;
                std::thread::sleep(PUBLICATION_SETTLE_SLEEP);
            }
            TokenFileRead::Empty => return Err(empty_token_error(path)),
            TokenFileRead::Missing => return Ok(None),
            TokenFileRead::Value(token) => return Ok(Some(token)),
        }
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
/// `db` is the database path (the token file derives from it); `tag` is
/// the suffix after the database path.
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
    // The settling read tolerates the publication window of a
    // concurrent creator (file exists, bytes not yet written); a
    // settled empty file still fails closed (CU-02).
    if let Some(token) = load_existing_token_settling(&path)? {
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
            // Windows: lock the ACL down to the owner BEFORE the first
            // byte lands (WRKSPC-011) — an inherited ACL could
            // otherwise expose the credential between create and
            // write. (Unix gets 0600 from the open above.)
            #[cfg(windows)]
            win_acl::set_owner_only_acl(&file)?;
            use std::io::Write;
            file.write_all(token.as_bytes())
                .map_err(|e| format!("cannot write token to {}: {e}", path.display()))?;
            file.sync_all()
                .map_err(|e| format!("cannot sync token file {}: {e}", path.display()))?;
            Ok(token)
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            // A concurrent creator won: the stable value is the one on
            // disk, so every caller agrees. The settling read waits
            // out the winner's write window instead of mistaking it
            // for a settled empty file.
            match load_existing_token_settling(&path)? {
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

/// Windows owner-only ACL enforcement for token files (WRKSPC-011).
///
/// Windows has no umask: a file created through [`std::fs::OpenOptions`]
/// carries the directory's INHERITED ACL, which can grant other local
/// users read access to a cleartext bearer credential. Every token
/// file therefore gets a DACL granting the current user alone full
/// access, with inheritance blocked (PROTECTED_DACL_SECURITY_INFORMATION),
/// applied BEFORE the secret bytes are written; pre-existing files get
/// the same tightening (plus a foreign-owner refusal) before their
/// value is read. This mirrors the Unix 0600 policy and the
/// install.ps1 private-key ACL handling in the same threat model.
/// The Windows ACL test hook: reachable from the integration tests
/// (a `pub(crate)` module would hide it from the external test crate).
#[cfg(windows)]
pub use win_acl::token_acl_is_owner_only;

#[cfg(windows)]
pub(crate) mod win_acl {
    use std::path::Path;
    use std::ptr;
    use windows_sys::Win32::Foundation::{CloseHandle, GENERIC_ALL, GetLastError, HANDLE};
    use windows_sys::Win32::Security::Authorization::{SE_FILE_OBJECT, SetSecurityInfo};
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACL, ACL_REVISION, AclSizeInformation, AddAccessAllowedAce,
        DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation, GetFileSecurityW,
        GetLengthSid, GetSecurityDescriptorDacl, GetSecurityDescriptorOwner, GetTokenInformation,
        INHERITED_ACE, InitializeAcl, InitializeSecurityDescriptor, OWNER_SECURITY_INFORMATION,
        PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SECURITY_DESCRIPTOR,
        SetFileSecurityW, SetSecurityDescriptorDacl, TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    /// `SECURITY_DESCRIPTOR_REVISION` (1) as a literal: windows-sys
    /// does not export the constant.
    const SECURITY_DESCRIPTOR_REVISION_1: u32 = 1;
    /// `ACCESS_ALLOWED_ACE_TYPE` (0) as a literal; the exported
    /// constant lives in the SystemServices feature, unused here.
    const ACCESS_ALLOWED_ACE_TYPE_0: u8 = 0;

    /// The current process token user's SID, copied out of the token
    /// so the token handle can be closed immediately.
    fn process_user_sid() -> Result<Vec<u8>, String> {
        // SAFETY: GetCurrentProcess returns the process pseudohandle,
        // which is always valid and never needs closing.
        let process = unsafe { GetCurrentProcess() };
        let mut token: HANDLE = ptr::null_mut();
        // SAFETY: `token` is a valid out-pointer and the pseudohandle
        // is valid for the process lifetime.
        let opened = unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token) };
        if opened == 0 {
            return Err(format!("cannot open the process token: {}", last_error()));
        }
        let sid = token_user_sid(token);
        // SAFETY: `token` was opened by OpenProcessToken above and is
        // not used afterwards.
        unsafe {
            CloseHandle(token);
        }
        sid
    }

    /// Reads the SID out of an opened token's `TokenUser` information.
    fn token_user_sid(token: HANDLE) -> Result<Vec<u8>, String> {
        let mut needed: u32 = 0;
        // SAFETY: the documented sizing call — a null buffer with a
        // zero length reports the required size in `needed`.
        let _ = unsafe { GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut needed) };
        if needed == 0 {
            return Err(format!(
                "cannot size the process token user: {}",
                last_error()
            ));
        }
        // u64 slots keep the buffer aligned for the pointer inside
        // TOKEN_USER on every Windows architecture.
        let mut buf = vec![0u64; needed.div_ceil(8) as usize];
        // SAFETY: `buf` is `needed` bytes, aligned to 8, and writable.
        let ok = unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                buf.as_mut_ptr() as *mut core::ffi::c_void,
                needed,
                &mut needed,
            )
        };
        if ok == 0 {
            return Err(format!(
                "cannot read the process token user: {}",
                last_error()
            ));
        }
        // SAFETY: GetTokenInformation succeeded, so `buf` holds a valid
        // TOKEN_USER within `needed` bytes.
        let user: &TOKEN_USER = unsafe { &*(buf.as_ptr() as *const TOKEN_USER) };
        let sid: PSID = user.User.Sid;
        if sid.is_null() {
            return Err("the process token user SID is null".to_string());
        }
        // SAFETY: `sid` points into `buf`, which holds the complete
        // TOKEN_USER returned above.
        let sid_len = unsafe { GetLengthSid(sid) } as usize;
        if sid_len == 0 || sid_len > needed as usize {
            return Err("the process token user SID has an invalid length".to_string());
        }
        let mut bytes = vec![0u8; sid_len];
        // SAFETY: both pointers are valid for `sid_len` bytes and the
        // source is a valid SID structure.
        unsafe { ptr::copy_nonoverlapping(sid as *const u8, bytes.as_mut_ptr(), sid_len) };
        Ok(bytes)
    }

    /// Builds an absolute security descriptor whose DACL grants
    /// GENERIC_ALL to `sid` alone. The returned buffer owns the ACL
    /// the descriptor points into and must outlive the security call.
    /// (u64 slots, not u8: the ACL pointer must stay aligned.)
    fn owner_only_security_descriptor(
        sid: &[u8],
    ) -> Result<(SECURITY_DESCRIPTOR, Vec<u64>), String> {
        // SAFETY: zeroed memory is a valid starting state for a
        // security descriptor before initialization.
        let mut sd: SECURITY_DESCRIPTOR = unsafe { core::mem::zeroed() };
        // SAFETY: `sd` points to a writable SECURITY_DESCRIPTOR.
        let init = unsafe {
            InitializeSecurityDescriptor(
                &mut sd as *mut SECURITY_DESCRIPTOR as PSECURITY_DESCRIPTOR,
                SECURITY_DESCRIPTOR_REVISION_1,
            )
        };
        if init == 0 {
            return Err(format!(
                "cannot initialize a security descriptor: {}",
                last_error()
            ));
        }
        let ace_size =
            core::mem::size_of::<ACCESS_ALLOWED_ACE>() - core::mem::size_of::<u32>() + sid.len();
        let acl_size = core::mem::size_of::<ACL>() + ace_size;
        // u64 slots keep the ACL buffer aligned.
        let mut acl_buf = vec![0u64; acl_size.div_ceil(8)];
        let acl = acl_buf.as_mut_ptr() as *mut ACL;
        // SAFETY: `acl` points to `acl_size` zeroed, aligned bytes.
        let init_acl = unsafe { InitializeAcl(acl, acl_size as u32, ACL_REVISION) };
        if init_acl == 0 {
            return Err(format!(
                "cannot initialize the owner-only ACL: {}",
                last_error()
            ));
        }
        // SAFETY: `sid` is the current process user SID; the ACL was
        // sized above for exactly this one ACE.
        let added =
            unsafe { AddAccessAllowedAce(acl, ACL_REVISION, GENERIC_ALL, sid.as_ptr() as PSID) };
        if added == 0 {
            return Err(format!("cannot add the owner ACE: {}", last_error()));
        }
        // SAFETY: the descriptor is initialized; `acl` stays alive
        // through the returned buffer for the caller's security call.
        let set = unsafe {
            SetSecurityDescriptorDacl(
                &mut sd as *mut SECURITY_DESCRIPTOR as PSECURITY_DESCRIPTOR,
                1, // bdaclpresent
                acl,
                0, // bdacldefaulted
            )
        };
        if set == 0 {
            return Err(format!(
                "cannot attach the owner-only DACL: {}",
                last_error()
            ));
        }
        Ok((sd, acl_buf))
    }

    /// Replaces an OPEN token file's DACL with the owner-only,
    /// inheritance-free one. Called on the winning `create_new` handle
    /// BEFORE the secret bytes are written, so no instant of the file
    /// ever carries a broader ACL; the handle closes the path-TOCTOU
    /// window.
    pub(crate) fn set_owner_only_acl(file: &std::fs::File) -> Result<(), String> {
        use std::os::windows::io::AsRawHandle;
        let sid = process_user_sid()?;
        // `_sd` is not passed (the ACL pointer is); the buffer it owns
        // is what must stay alive across the call.
        let (_sd, acl_buf) = owner_only_security_descriptor(&sid)?;
        // SAFETY: the handle belongs to the open file (kept alive by
        // `file`); the descriptor and its DACL buffer are valid for
        // the call; null owner/group/SACL leave those unchanged.
        let result = unsafe {
            SetSecurityInfo(
                file.as_raw_handle() as HANDLE,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                ptr::null_mut(),
                ptr::null_mut(),
                acl_buf.as_ptr() as *const ACL,
                ptr::null(),
            )
        };
        if result != 0 {
            return Err(format!(
                "cannot set the owner-only ACL: {}",
                std::io::Error::from_raw_os_error(result as i32)
            ));
        }
        Ok(())
    }

    /// Load-time policy for an EXISTING token file: a foreign owner is
    /// refused (the Windows form of the Unix uid check), then the DACL
    /// is replaced with the owner-only one so inherited ACEs can never
    /// widen read access to the credential.
    pub(crate) fn validate_and_tighten_existing(path: &Path) -> Result<(), String> {
        let wide = to_wide(path);
        let mut needed: u32 = 0;
        // SAFETY: sizing call for the owner information.
        let _ = unsafe {
            GetFileSecurityW(
                wide.as_ptr(),
                OWNER_SECURITY_INFORMATION,
                ptr::null_mut(),
                0,
                &mut needed,
            )
        };
        if needed == 0 {
            return Err(format!(
                "cannot size the security descriptor of {}: {}",
                path.display(),
                last_error()
            ));
        }
        let mut buf = vec![0u64; needed.div_ceil(8) as usize];
        // SAFETY: `buf` is `needed` bytes, aligned, and writable.
        let ok = unsafe {
            GetFileSecurityW(
                wide.as_ptr(),
                OWNER_SECURITY_INFORMATION,
                buf.as_mut_ptr() as PSECURITY_DESCRIPTOR,
                needed,
                &mut needed,
            )
        };
        if ok == 0 {
            return Err(format!(
                "cannot read the security descriptor of {}: {}",
                path.display(),
                last_error()
            ));
        }
        let mut owner: PSID = ptr::null_mut();
        let mut defaulted: i32 = 0;
        // SAFETY: `buf` holds a valid security descriptor returned by
        // GetFileSecurityW.
        let ok = unsafe {
            GetSecurityDescriptorOwner(
                buf.as_ptr() as PSECURITY_DESCRIPTOR,
                &mut owner,
                &mut defaulted,
            )
        };
        if ok == 0 || owner.is_null() {
            return Err(format!(
                "cannot read the owner of {}: {}",
                path.display(),
                last_error()
            ));
        }
        let me = process_user_sid()?;
        // SAFETY: `owner` points into the descriptor buffer and `me`
        // holds a SID copied from the process token; both are valid.
        let same = unsafe { EqualSid(owner, me.as_ptr() as PSID) };
        if same == 0 {
            return Err(format!(
                "token file {} is owned by another user — refusing to read it",
                path.display()
            ));
        }
        let (sd, acl_buf) = owner_only_security_descriptor(&me)?;
        // SAFETY: the descriptor and its DACL buffer are valid; only
        // the DACL information is replaced.
        let ok = unsafe {
            SetFileSecurityW(
                wide.as_ptr(),
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                &sd as *const SECURITY_DESCRIPTOR as PSECURITY_DESCRIPTOR,
            )
        };
        if ok == 0 {
            return Err(format!(
                "cannot tighten the ACL of {}: {}",
                path.display(),
                last_error()
            ));
        }
        let _ = acl_buf;
        Ok(())
    }

    /// Reports whether `path`'s DACL grants access to the current user
    /// alone: exactly one access-allowed ACE, not inherited, whose SID
    /// is the process token user. Exposed for the Windows ACL
    /// regression test.
    pub fn token_acl_is_owner_only(path: &Path) -> Result<bool, String> {
        let wide = to_wide(path);
        let mut needed: u32 = 0;
        // SAFETY: sizing call for the DACL information.
        let _ = unsafe {
            GetFileSecurityW(
                wide.as_ptr(),
                DACL_SECURITY_INFORMATION,
                ptr::null_mut(),
                0,
                &mut needed,
            )
        };
        if needed == 0 {
            return Err(format!(
                "cannot size the DACL of {}: {}",
                path.display(),
                last_error()
            ));
        }
        let mut buf = vec![0u64; needed.div_ceil(8) as usize];
        // SAFETY: `buf` is `needed` bytes, aligned, and writable.
        let ok = unsafe {
            GetFileSecurityW(
                wide.as_ptr(),
                DACL_SECURITY_INFORMATION,
                buf.as_mut_ptr() as PSECURITY_DESCRIPTOR,
                needed,
                &mut needed,
            )
        };
        if ok == 0 {
            return Err(format!(
                "cannot read the DACL of {}: {}",
                path.display(),
                last_error()
            ));
        }
        let mut present: i32 = 0;
        let mut dacl: *mut ACL = ptr::null_mut();
        let mut defaulted: i32 = 0;
        // SAFETY: `buf` holds a valid descriptor from GetFileSecurityW.
        let ok = unsafe {
            GetSecurityDescriptorDacl(
                buf.as_ptr() as PSECURITY_DESCRIPTOR,
                &mut present,
                &mut dacl,
                &mut defaulted,
            )
        };
        if ok == 0 || present == 0 || dacl.is_null() {
            return Ok(false);
        }
        let mut info: windows_sys::Win32::Security::ACL_SIZE_INFORMATION =
            unsafe { core::mem::zeroed() };
        // SAFETY: `info` is a writable ACL_SIZE_INFORMATION buffer of
        // the size GetAclInformation expects for AclSizeInformation.
        let ok = unsafe {
            GetAclInformation(
                dacl,
                &mut info as *mut _ as *mut core::ffi::c_void,
                core::mem::size_of_val(&info) as u32,
                AclSizeInformation,
            )
        };
        if ok == 0 || info.AceCount != 1 {
            return Ok(false);
        }
        let mut ace_ptr: *mut core::ffi::c_void = ptr::null_mut();
        // SAFETY: the ACE index is within the count reported above.
        let ok = unsafe { GetAce(dacl, 0, &mut ace_ptr) };
        if ok == 0 || ace_ptr.is_null() {
            return Ok(false);
        }
        // SAFETY: GetAce returned a pointer to the ACE inside the
        // descriptor buffer, valid for the buffer's lifetime.
        let ace: &ACCESS_ALLOWED_ACE = unsafe { &*(ace_ptr as *const ACCESS_ALLOWED_ACE) };
        if ace.Header.AceType != ACCESS_ALLOWED_ACE_TYPE_0 {
            return Ok(false);
        }
        if ace.Header.AceFlags & (INHERITED_ACE as u8) != 0 {
            return Ok(false);
        }
        let me = process_user_sid()?;
        // SAFETY: the SID starts at the ACE's SidStart field and stays
        // within the descriptor buffer; `me` is a valid SID.
        let ace_sid = (&ace.SidStart as *const u32) as PSID;
        Ok(unsafe { EqualSid(ace_sid, me.as_ptr() as PSID) } != 0)
    }

    /// The path as a null-terminated UTF-16 buffer for the W APIs.
    fn to_wide(path: &Path) -> Vec<u16> {
        use std::os::windows::ffi::OsStrExt;
        path.as_os_str()
            .encode_wide()
            .chain(core::iter::once(0))
            .collect()
    }

    fn last_error() -> std::io::Error {
        // SAFETY: GetLastError reads the calling thread's error code.
        std::io::Error::from_raw_os_error(unsafe { GetLastError() } as i32)
    }
}
