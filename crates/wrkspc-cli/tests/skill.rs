//! Installer integration tests against a synthetic `$HOME`: install,
//! uninstall, and status must be idempotent, wrkspc-marked, and never
//! disturb user content outside the marked sections.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::Path;

use wrkspc_cli::skill::{
    self, InstallOutcome, SECTION_END, SECTION_START, SkillStatus, UninstallOutcome,
};

/// A throwaway home directory with a unique suffix so parallel tests
/// never collide.
fn temp_home(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("wrkspc-skill-test-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp home");
    dir
}

fn entry(id: &str) -> &'static wrkspc_cli::skill::HarnessEntry {
    skill::parse(id).expect("known harness")
}

/// Installing codex creates the skills file and inserts the pointer section while preserving user content.
#[test]
fn codex_install_creates_skill_and_pointer_section() {
    let home = temp_home("codex-install");
    let codex = home.join(".codex");
    std::fs::create_dir_all(&codex).expect("codex dir");
    std::fs::write(codex.join("AGENTS.md"), "# My notes\n\nkeep this\n").expect("seed file");

    let outcome = skill::install(&home, entry("codex")).expect("install");
    assert_eq!(outcome, InstallOutcome::Installed);

    let skill_file = codex.join("skills/wrkspc/SKILL.md");
    assert_eq!(
        std::fs::read_to_string(&skill_file).expect("skill file"),
        skill::SKILL_MD
    );
    let instructions = std::fs::read_to_string(codex.join("AGENTS.md")).expect("AGENTS.md");
    assert!(
        instructions.starts_with("# My notes\n\nkeep this\n"),
        "user content preserved"
    );
    assert_eq!(
        instructions.matches(SECTION_START).count(),
        1,
        "one section"
    );
    assert!(instructions.contains("A wrkspc skill is installed"));
    assert!(instructions.contains(SECTION_END));
}

/// A second install is a no-op; refresh updates a stale skill copy.
#[test]
fn install_is_idempotent_and_refresh_updates_stale_skill() {
    let home = temp_home("codex-idempotent");
    std::fs::create_dir_all(home.join(".codex")).expect("codex dir");

    assert_eq!(
        skill::install(&home, entry("codex")).expect("first install"),
        InstallOutcome::Installed
    );
    assert_eq!(
        skill::install(&home, entry("codex")).expect("second install"),
        InstallOutcome::UpToDate
    );

    // Tamper with the skill; the next install refreshes it.
    let skill_file = home.join(".codex/skills/wrkspc/SKILL.md");
    std::fs::write(&skill_file, "stale").expect("tamper");
    assert_eq!(
        skill::status(&home, entry("codex")).unwrap(),
        SkillStatus::Stale
    );
    assert_eq!(
        skill::install(&home, entry("codex")).expect("refresh"),
        InstallOutcome::Refreshed
    );
    assert_eq!(
        std::fs::read_to_string(&skill_file).expect("skill file"),
        skill::SKILL_MD
    );
    assert_eq!(
        skill::status(&home, entry("codex")).unwrap(),
        SkillStatus::Installed
    );
}

/// An unreadable pointer file is an error, never an empty file: the
/// installer must not truncate a user-owned AGENTS.md it could not
/// read (e.g. invalid UTF-8).
#[test]
fn install_refuses_to_truncate_an_unreadable_pointer() {
    let home = temp_home("codex-unreadable");
    std::fs::create_dir_all(home.join(".codex")).expect("codex dir");
    let pointer = home.join(".codex/AGENTS.md");
    let original = b"# My notes\n\nkeep this\xff\xfe\n";
    std::fs::write(&pointer, original).expect("seed file");

    let error = skill::install(&home, entry("codex")).expect_err("must refuse");
    assert!(error.to_string().contains("cannot read"), "got: {error}");

    // The user's file is untouched, byte for byte.
    assert_eq!(std::fs::read(&pointer).expect("read back"), original);
    // And nothing else was written.
    assert!(!home.join(".codex/skills/wrkspc/SKILL.md").exists());
}

/// Uninstall removes the skill and pointer section and restores the original file contents.
#[test]
fn uninstall_removes_exactly_what_install_added() {
    let home = temp_home("codex-uninstall");
    std::fs::create_dir_all(home.join(".codex")).expect("codex dir");
    std::fs::write(home.join(".codex/AGENTS.md"), "# My notes\n").expect("seed file");

    skill::install(&home, entry("codex")).expect("install");
    assert_eq!(
        skill::uninstall(&home, entry("codex")).expect("uninstall"),
        UninstallOutcome::Removed
    );

    assert!(
        !home.join(".codex/skills/wrkspc/SKILL.md").exists(),
        "skill file removed"
    );
    assert_eq!(
        std::fs::read_to_string(home.join(".codex/AGENTS.md")).expect("AGENTS.md"),
        "# My notes\n",
        "user content restored untouched"
    );
    assert_eq!(
        skill::status(&home, entry("codex")).unwrap(),
        SkillStatus::Missing
    );
    assert_eq!(
        skill::uninstall(&home, entry("codex")).expect("second uninstall"),
        UninstallOutcome::NothingToRemove
    );
}

/// gemini-cli installs into its registry paths (pointer section + skill dir) from a seeded GEMINI.md.
#[test]
fn gemini_install_adds_pointer_section_and_skill_dir() {
    let home = temp_home("gemini");
    std::fs::create_dir_all(home.join(".gemini")).expect("gemini dir");
    std::fs::write(home.join(".gemini/GEMINI.md"), "prior content\n").expect("seed file");

    assert_eq!(
        skill::install(&home, entry("gemini-cli")).expect("install"),
        InstallOutcome::Installed
    );
    let content = std::fs::read_to_string(home.join(".gemini/GEMINI.md")).expect("GEMINI.md");
    assert!(content.starts_with("prior content\n"));
    assert!(content.contains("Wrkspc is available through MCP tools"));
    assert_eq!(
        std::fs::read_to_string(home.join(".gemini/skills/wrkspc/SKILL.md")).expect("skill file"),
        skill::SKILL_MD,
        "gemini has a skills directory in the ecosystem registry"
    );

    skill::uninstall(&home, entry("gemini-cli")).expect("uninstall");
    assert_eq!(
        std::fs::read_to_string(home.join(".gemini/GEMINI.md")).expect("GEMINI.md"),
        "prior content\n"
    );
    assert!(!home.join(".gemini/skills/wrkspc").exists());
}

/// Registry entries with non-standard paths (OpenCode under ~/.config, Pi under ~/.agents/skills) install where the registry says.
#[test]
fn exotic_entries_install_to_their_registry_paths() {
    let home = temp_home("exotic");
    // OpenCode lives under ~/.config; Pi is detected by ~/.pi/agent but
    // installs into the shared ~/.agents/skills location.
    std::fs::create_dir_all(home.join(".config/opencode")).expect("opencode dir");
    std::fs::create_dir_all(home.join(".pi/agent")).expect("pi dir");

    let detected = skill::detect(&home);
    let ids = detected.iter().map(|e| e.id).collect::<Vec<_>>();
    assert!(
        ids.contains(&"opencode"),
        "detected via ~/.config/opencode: {ids:?}"
    );
    assert!(ids.contains(&"pi"), "detected via ~/.pi/agent: {ids:?}");

    assert_eq!(
        skill::install(&home, entry("opencode")).expect("opencode install"),
        InstallOutcome::Installed
    );
    assert_eq!(
        std::fs::read_to_string(home.join(".config/opencode/skills/wrkspc/SKILL.md"))
            .expect("opencode skill"),
        skill::SKILL_MD
    );

    assert_eq!(
        skill::install(&home, entry("pi")).expect("pi install"),
        InstallOutcome::Installed
    );
    assert_eq!(
        std::fs::read_to_string(home.join(".agents/skills/wrkspc/SKILL.md"))
            .expect("shared skills dir"),
        skill::SKILL_MD
    );
}

/// Detection reports only harnesses whose machine-level directories exist.
#[test]
fn detect_finds_only_harnesses_that_exist() {
    let home = temp_home("detect");
    std::fs::create_dir_all(home.join(".codex")).expect("codex dir");
    std::fs::create_dir_all(home.join(".claude")).expect("claude dir");

    let ids = skill::detect(&home)
        .iter()
        .map(|e| e.id)
        .collect::<Vec<_>>();
    assert!(ids.contains(&"codex"));
    assert!(ids.contains(&"claude-code"));
    assert!(!ids.contains(&"gemini-cli"));
    assert!(!ids.contains(&"cursor"));
}

/// Status is Missing when the harness directory exists but the skill/pointer file does not.
#[test]
fn status_reports_missing_for_absent_harness_files() {
    let home = temp_home("status");
    std::fs::create_dir_all(home.join(".claude")).expect("claude dir");
    assert_eq!(
        skill::status(&home, entry("claude-code")).unwrap(),
        SkillStatus::Missing
    );
    assert_eq!(
        skill::status(Path::new("/nonexistent-wrkspc-home"), entry("gemini-cli")).unwrap(),
        SkillStatus::Missing
    );
}

/// CU-09: a read failure (directory in place of the skill file) is
/// EXPOSED, never collapsed into Missing.
#[test]
fn status_reports_unreadable_instead_of_missing() {
    let home = temp_home("unreadable");
    let skill_file = home.join(".claude/skills/wrkspc/SKILL.md");
    std::fs::create_dir_all(skill_file.parent().unwrap()).unwrap();
    std::fs::create_dir_all(&skill_file).unwrap(); // a DIRECTORY where the file goes
    let error = skill::status(&home, entry("claude-code")).unwrap_err();
    assert!(!error.is_empty());
}

/// CU-04: installing through a symlinked SKILL leaf is refused and
/// the link's target is untouched.
#[test]
fn install_refuses_a_symlinked_skill_leaf() {
    let home = temp_home("symlink-leaf");
    let skill_file = home.join(".claude/skills/wrkspc/SKILL.md");
    std::fs::create_dir_all(skill_file.parent().unwrap()).unwrap();
    let victim = home.join("victim.md");
    std::fs::write(&victim, "unrelated user text").unwrap();
    std::os::unix::fs::symlink(&victim, &skill_file).unwrap();

    let error = skill::install(&home, entry("claude-code")).unwrap_err();
    assert!(error.to_string().contains("symlink"), "got: {error}");
    assert_eq!(
        std::fs::read_to_string(&victim).unwrap(),
        "unrelated user text",
        "the link target must not be replaced"
    );
}

/// CU-04: uninstalling through a symlinked parent directory is
/// refused, never followed.
#[test]
fn uninstall_refuses_a_symlinked_skill_directory() {
    let home = temp_home("symlink-dir");
    std::fs::create_dir_all(home.join(".claude/skills")).unwrap();
    let outside = temp_home("outside-dir");
    std::fs::create_dir_all(outside.join("skills/wrkspc")).unwrap();
    std::fs::write(outside.join("skills/wrkspc/SKILL.md"), skill::SKILL_MD).unwrap();
    std::os::unix::fs::symlink(
        outside.join("skills/wrkspc"),
        home.join(".claude/skills/wrkspc"),
    )
    .unwrap();

    let error = skill::uninstall(&home, entry("claude-code")).unwrap_err();
    assert!(
        error.to_string().contains("resolves outside"),
        "got: {error}"
    );
    assert!(
        outside.join("skills/wrkspc/SKILL.md").exists(),
        "the external target must survive"
    );
}

/// The physical skill target two entries share (reconstructed from
/// the public registry fields — the same formula the installer uses).
fn physical_target(home: &Path, entry: &wrkspc_cli::skill::HarnessEntry) -> std::path::PathBuf {
    home.join(entry.skills_dir).join("wrkspc").join("SKILL.md")
}

/// CU-08: uninstalling one harness whose skill path is shared is
/// refused with the affected harnesses named.
#[tokio::test]
async fn shared_skill_uninstall_is_refused_when_partial() {
    let home = temp_home("shared-uninstall");
    // Find two registry entries sharing one skill path.
    let sharers: Vec<&wrkspc_cli::skill::HarnessEntry> = wrkspc_cli::skill::HARNESSES
        .iter()
        .filter(|entry| {
            wrkspc_cli::skill::HARNESSES.iter().any(|other| {
                other.id != entry.id
                    && physical_target(&home, other) == physical_target(&home, entry)
            })
        })
        .collect();
    let first = sharers[0];
    let sharer_ids: Vec<&str> = wrkspc_cli::skill::HARNESSES
        .iter()
        .filter(|other| {
            other.id != first.id && physical_target(&home, other) == physical_target(&home, first)
        })
        .map(|other| other.id)
        .collect();
    assert!(
        !sharer_ids.is_empty(),
        "the registry must contain shared paths"
    );

    // Install the shared copy, then attempt a partial uninstall: it
    // must be refused with the affected harnesses named.
    skill::install(&home, first).unwrap();
    let error = wrkspc_cli::skill::run_skill(wrkspc_cli::SkillCommand::Uninstall {
        harness: Some(vec![first.id.to_string()]),
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("shared"), "got: {error}");
    for id in &sharer_ids {
        assert!(
            error.to_string().contains(*id),
            "the refusal must name {}: {error}",
            id
        );
    }
}

/// A pointer write that fails AFTER the skill copy succeeded rolls the
/// copy back: an install must never persist a half-install (new skill,
/// missing pointer). The prior skill content is restored byte-for-byte.
#[test]
fn pointer_failure_rolls_the_skill_copy_back() {
    let home = temp_home("rollback");
    std::fs::create_dir_all(home.join(".codex/skills/wrkspc")).unwrap();
    let skill_file = home.join(".codex/skills/wrkspc/SKILL.md");
    std::fs::write(&skill_file, "user's own skill content").unwrap();
    // The pointer's parent becomes read-only: the skill copy writes
    // fine (its own directory stays writable), the AGENTS.md write
    // fails.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(home.join(".codex"), std::fs::Permissions::from_mode(0o555)).unwrap();

    let error = skill::install(&home, entry("codex")).unwrap_err();
    assert!(!error.to_string().is_empty());
    assert_eq!(
        std::fs::read_to_string(&skill_file).unwrap(),
        "user's own skill content",
        "the failed install must restore the prior skill copy"
    );

    // Restore permissions so cleanup can remove the scratch dir.
    std::fs::set_permissions(home.join(".codex"), std::fs::Permissions::from_mode(0o755)).unwrap();
}
