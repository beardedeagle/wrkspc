//! Smoke tests for the wrkspc binary: the subcommand surface parses and
//! help text renders. Component behavior is covered by the crate-level
//! integration tests; these prove the CLI shell is intact.

use assert_cmd::Command;

/// The binary renders --version and --help without a config, database, or network.
#[test]
fn version_and_help_render() {
    let version = Command::cargo_bin("wrkspc")
        .unwrap()
        .arg("--version")
        .output()
        .unwrap();
    assert!(version.status.success());
    let stdout = String::from_utf8_lossy(&version.stdout);
    assert!(stdout.contains("wrkspc"), "got: {stdout}");

    let help = Command::cargo_bin("wrkspc")
        .unwrap()
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    let stdout = String::from_utf8_lossy(&help.stdout);
    assert!(stdout.contains("mcp-bridge"), "got: {stdout}");
    assert!(stdout.contains("node"), "got: {stdout}");

    let node_help = Command::cargo_bin("wrkspc")
        .unwrap()
        .args(["node", "run", "--help"])
        .output()
        .unwrap();
    assert!(node_help.status.success());
    let stdout = String::from_utf8_lossy(&node_help.stdout);
    assert!(stdout.contains("--db"), "got: {stdout}");
    assert!(stdout.contains("--bindings"), "got: {stdout}");
    assert!(stdout.contains("--ui"), "got: {stdout}");
    assert!(stdout.contains("--a2a"), "got: {stdout}");

    let hub_help = Command::cargo_bin("wrkspc")
        .unwrap()
        .args(["hub", "run", "--help"])
        .output()
        .unwrap();
    assert!(hub_help.status.success());
    let stdout = String::from_utf8_lossy(&hub_help.stdout);
    assert!(stdout.contains("--tls-ca"), "got: {stdout}");
    assert!(stdout.contains("--revoke-list"), "got: {stdout}");
    assert!(stdout.contains("--a2a-addr-file"), "got: {stdout}");
}
