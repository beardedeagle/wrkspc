//! Process hardening installed before anything sensitive is read.
//!
//! A TLS private key is parsed into process memory at startup — `hub run`
//! through `rustls_pemfile::private_key`, the node by reading its client key
//! into a buffer for `reqwest` — and a panic after that point would otherwise
//! let the kernel write a core image containing it. The guard below runs before
//! either parse, so no crash of this process can produce a dump with a key in
//! it.

/// Disables core dumps for this process, for the rest of its life.
///
/// `RLIMIT_CORE` is set to zero, which stops the kernel writing a core image
/// when the process crashes or aborts. On Linux the process is also marked
/// non-dumpable, which additionally closes `/proc/<pid>/mem` and `ptrace` to
/// unprivileged readers.
///
/// The limit is inherited across `exec`, so a harness process this binary
/// spawns cannot raise it back.
///
/// # Returns
///
/// `Ok(())` once the guard is in place.
///
/// # Errors
///
/// Returns the operating system's error when the limit cannot be lowered. The
/// caller warns and continues rather than failing closed: refusing to start
/// would turn a defence-in-depth measure into an outage.
#[cfg(unix)]
pub fn forbid_core_dumps() -> std::io::Result<()> {
    let limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `setrlimit` reads one `libc::rlimit` through a valid pointer to a
    // value owned by this frame for the duration of the call. Lowering the
    // process's own RLIMIT_CORE needs no capability.
    if unsafe { libc::setrlimit(libc::RLIMIT_CORE, &limit) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    #[cfg(target_os = "linux")]
    {
        // SAFETY: `prctl` with PR_SET_DUMPABLE takes an integer argument and a
        // process may always change its own dumpable flag. A refusal here is
        // not fatal: RLIMIT_CORE above already covers the core-file path.
        unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) };
    }
    Ok(())
}

/// Core-dump guard for platforms without rlimits.
///
/// # Returns
///
/// `Ok(())`; these platforms have no `RLIMIT_CORE` to lower.
#[cfg(not(unix))]
pub fn forbid_core_dumps() -> std::io::Result<()> {
    Ok(())
}
