//! The core-dump guard.
//!
//! A TLS private key lives in this process's memory for its whole life, so the
//! only thing a crash may not do is hand the kernel an image of it.
//!
//! The guard LOWERS `RLIMIT_CORE` irreversibly (a zeroed `rlim_max`
//! cannot be raised back), so the parent test process never installs
//! it: the check runs in a re-exec'd child of this test binary instead,
//! and every other test in this binary — plus any child process they
//! spawn — keeps its normal core limit.

#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    // The child probe reads RLIMIT_CORE back through `libc::getrlimit`;
    // the parent installs nothing.
    unsafe_code
)]

use wrkspc_cli::harden::forbid_core_dumps;

/// Set in the re-exec'd child's environment: only the child probe
/// installs the guard.
const CHILD_PROBE_ENV: &str = "WRKSPC_HARDEN_CHILD_PROBE";

// The guard zeroes the core-dump limit, and re-running it is a no-op —
// verified in a CHILD process, because the parent (this test binary)
// must not permanently lose its own core dumps.
#[test]
fn forbid_core_dumps_zeroes_the_limit() {
    let output = std::process::Command::new(std::env::current_exe().expect("test binary path"))
        .arg("--exact")
        .arg("harden_child_probe")
        .env(CHILD_PROBE_ENV, "1")
        .output()
        .expect("child probe spawn");
    assert!(
        output.status.success(),
        "the child probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// Runs ONLY as the re-exec'd child (selected with `--exact`): installs
// the guard and asserts the limits, so the irreversible mutation is
// confined to a process that exits immediately after. A plain
// `cargo test` run reaches this test without the marker env and skips
// it — the probe is meaningless in the parent process.
#[test]
fn harden_child_probe() {
    if std::env::var_os(CHILD_PROBE_ENV).is_none() {
        return;
    }
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
