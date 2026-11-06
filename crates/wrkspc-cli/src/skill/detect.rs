//! Harness lookup: id parsing, installation detection, and path
//! resolution.

use super::HarnessEntry;
use super::registry::HARNESSES;
use std::path::{Path, PathBuf};

pub fn parse(id: &str) -> Option<&'static HarnessEntry> {
    HARNESSES
        .iter()
        .find(|entry| entry.id.eq_ignore_ascii_case(id))
}

/// The registry entries whose detection paths exist under `home`.
///
/// # Arguments
///
/// * `home` — the home directory to inspect.
///
/// # Returns
///
/// The detected entries, in registry order.
pub fn detect(home: &Path) -> Vec<&'static HarnessEntry> {
    HARNESSES
        .iter()
        .filter(|entry| {
            entry.detect.iter().any(|candidate| {
                let path = Path::new(candidate);
                if path.is_absolute() {
                    path.exists()
                } else {
                    home.join(path).exists()
                }
            })
        })
        .collect()
}

/// The skill file location for a harness.
pub(crate) fn skill_path(home: &Path, entry: &HarnessEntry) -> PathBuf {
    home.join(entry.skills_dir).join("wrkspc").join("SKILL.md")
}

/// The standing-instructions file that receives the pointer section,
/// for the binding cohort only.
pub(crate) fn pointer_file(home: &Path, entry: &HarnessEntry) -> Option<PathBuf> {
    match entry.id {
        "codex" => Some(home.join(".codex/AGENTS.md")),
        "claude-code" => Some(home.join(".claude/CLAUDE.md")),
        "gemini-cli" => Some(home.join(".gemini/GEMINI.md")),
        _ => None,
    }
}
