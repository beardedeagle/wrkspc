//! CU-01/CU-07 regressions, exercised against the real CLI binary:
//! the Keychain timeout must bound PROCESS EXIT, and a failing
//! clipboard copy must fail the command instead of reporting success.
//
// The entire suite is macOS-specific (Keychain + pbcopy): on other
// platforms the crate compiles empty, keeping the Linux clippy leg
// dead-code-free.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![cfg(target_os = "macos")]

use std::process::Command;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_wrkspc")
}

/// A scratch dir of fake tools placed first on PATH.
struct FakePath {
    dir: std::path::PathBuf,
}

impl FakePath {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("wrkspc-cli-secrets-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }
    fn command(&self, program: &str, argv: &[&str]) -> Command {
        let mut command = Command::new(program);
        command.args(argv);
        command.env(
            "PATH",
            format!(
                "{}:{}",
                self.dir.display(),
                std::env::var("PATH").unwrap_or_default()
            ),
        );
        command
    }
}

impl Drop for FakePath {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// CU-01: a Keychain operation that outlives the 10s bound must not
/// keep the PROCESS alive — the CLI exits (non-zero) well before the
/// blocking call would finish, so a service manager can restart it.
/// The blocking seam is compiled into debug builds only
/// (`WRKSPC_TEST_KEYCHAIN_BLOCK_MS`), so this test can never run
/// against a release binary.
#[cfg(all(target_os = "macos", debug_assertions))]
#[test]
fn keychain_timeout_bounds_process_exit() {
    let fake = FakePath::new("exit-bound");
    let started = std::time::Instant::now();
    let output = fake
        .command(binary(), &["ui-token", "--node", "wrkspc.exit-bound-test"])
        .env("WRKSPC_TEST_KEYCHAIN_BLOCK_MS", "15000")
        .output()
        .expect("binary runs");
    let elapsed = started.elapsed();
    assert!(!output.status.success(), "the timeout must exit non-zero");
    assert!(
        elapsed < std::time::Duration::from_secs(13),
        "the process must exit at the 10s bound, not wait for the blocked operation: {elapsed:?}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("timed out"),
        "the exit must explain the timeout: {stderr}"
    );
}

/// CU-07: a pbcopy that drains stdin and then FAILS must fail the
/// command — no success message, non-zero exit. The active token is
/// supplied through WRKSPC_UI_TOKEN so no Keychain is touched
/// (which also exercises the CU-06 override path).
#[cfg(target_os = "macos")]
#[test]
fn failing_pbcopy_fails_the_command() {
    let fake = FakePath::new("pbcopy-fail");
    let script = "#!/bin/sh\ncat > /dev/null\nexit 7\n";
    let pbcopy = fake.dir.join("pbcopy");
    std::fs::write(&pbcopy, script).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pbcopy, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let output = fake
        .command(binary(), &["ui-token"])
        .env("WRKSPC_UI_TOKEN", "test-token")
        .output()
        .expect("binary runs");
    assert!(!output.status.success(), "a failed copy must exit non-zero");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("copied to the clipboard"),
        "no success message may be printed: {stdout}"
    );
}

/// CU-07: a successful pbcopy with an un-schedulable clearer still
/// reports the copy but does NOT promise the automatic clear.
#[cfg(target_os = "macos")]
#[test]
fn unschedulable_clearer_withholds_the_clear_promise() {
    let fake = FakePath::new("pbcopy-noclear");
    // pbcopy succeeds; `sh` is missing from PATH? We keep the real
    // PATH, so `sh` exists — instead make the clearer's stdin write
    // fail by making `sh` a fake that exits immediately.
    let pbcopy = fake.dir.join("pbcopy");
    std::fs::write(&pbcopy, "#!/bin/sh\ncat > /dev/null\nexit 0\n").unwrap();
    let sh = fake.dir.join("sh");
    std::fs::write(&sh, "#!/bin/sh\nexit 0\n").unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        for tool in [&pbcopy, &sh] {
            std::fs::set_permissions(tool, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    let output = fake
        .command(binary(), &["ui-token"])
        .env("WRKSPC_UI_TOKEN", "test-token")
        .output()
        .expect("binary runs");
    assert!(output.status.success(), "the copy itself succeeded");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("copied to the clipboard"), "got: {stdout}");
}
