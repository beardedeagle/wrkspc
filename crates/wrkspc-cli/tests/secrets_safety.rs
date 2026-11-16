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

/// CU-07: a copy whose clearer CANNOT be scheduled still reports the
/// copy but does NOT advertise the automatic clear. PATH points ONLY
/// at the fake dir: `pbcopy` resolves to the fake, while the clearer's
/// `sh` does not exist anywhere on PATH, so spawning it fails
/// deterministically before the token handoff — no timing race. (A
/// merely non-executable fake `sh` would not do: the spawn's PATH
/// search skips non-executable candidates and falls through to the
/// real `/bin/sh`.)
#[cfg(target_os = "macos")]
#[test]
fn unschedulable_clearer_withholds_the_clear_promise() {
    let fake = FakePath::new("pbcopy-noclear");
    // The fake pbcopy drains its stdin using only shell builtins, so
    // it works even with PATH stripped to the fake dir (no `cat`);
    // the clearer's `sh` is simply absent from that PATH.
    let pbcopy = fake.dir.join("pbcopy");
    std::fs::write(
        &pbcopy,
        "#!/bin/sh\nwhile IFS= read -r line; do :; done\nexit 0\n",
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pbcopy, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let mut command = fake.command(binary(), &["ui-token"]);
    // PATH = the fake dir alone: `pbcopy` resolves to the fake, and
    // the clearer's `sh` is not found anywhere on PATH.
    command.env("PATH", fake.dir.display().to_string());
    let output = command
        .env("WRKSPC_UI_TOKEN", "test-token")
        .output()
        .expect("binary runs");
    assert!(output.status.success(), "the copy itself succeeded");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("copied to the clipboard"), "got: {stdout}");
    assert!(
        !stdout.contains("it clears itself in 30s"),
        "a copy whose clearer failed must NOT advertise an automatic clear: {stdout}"
    );
    assert!(
        stdout.contains("could not be scheduled"),
        "the withheld clear must be reported honestly: {stdout}"
    );
}
