//! The core-dump guard.
//!
//! A TLS private key lives in this process's memory for its whole life, so the
//! only thing a crash may not do is hand the kernel an image of it.

#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use wrkspc_cli::harden::forbid_core_dumps;

/// The guard zeroes the core-dump limit, and re-running it is a no-op.
#[test]
fn forbid_core_dumps_zeroes_the_limit() {
    forbid_core_dumps().expect("the guard installs");

    let mut limit = libc::rlimit {
        rlim_cur: u64::MAX,
        rlim_max: u64::MAX,
    };
    // SAFETY: `getrlimit` writes into a `libc::rlimit` owned by this frame.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_CORE, &mut limit) };
    assert_eq!(rc, 0, "getrlimit failed");
    assert_eq!(limit.rlim_cur, 0, "a core dump could still be written");
    assert_eq!(limit.rlim_max, 0, "the limit could be raised again");

    // Calling it twice must not fail: every role installs it on start.
    forbid_core_dumps().expect("the guard is idempotent");
}
