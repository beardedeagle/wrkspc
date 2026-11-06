//! Install, uninstall, and status application against one harness.

use super::detect::{pointer_file, skill_path};
use super::{HarnessEntry, InstallOutcome, SkillStatus, UninstallOutcome};
use super::{SKILL_MD, pointer_section, with_section, without_section};
use std::path::Path;

pub fn install(
    home: &Path,
    entry: &HarnessEntry,
) -> Result<InstallOutcome, Box<dyn std::error::Error>> {
    let mut any_new = false;
    let mut any_refreshed = false;

    // Read first, write second — on either file. A pointer section
    // (AGENTS.md/CLAUDE.md/GEMINI.md) that cannot be read must abort
    // the install before anything is touched.
    let pointer_plan =
        if let Some(instructions) = entry.pointer.then(|| pointer_file(home, entry)).flatten() {
            let current = read_or_default(&instructions).map_err(|error| {
                format!(
                    "cannot read {} before install: {error}",
                    instructions.display()
                )
            })?;
            let (_, section_existed) = without_section(&current);
            let (updated, changed) = with_section(&current, &pointer_section(entry));
            Some((instructions, updated, changed, section_existed))
        } else {
            None
        };

    // The skills directory copy.
    let path = skill_path(home, entry);
    let current = read_or_default(&path)
        .map_err(|error| format!("cannot read {} before install: {error}", path.display()))?;
    if current != SKILL_MD {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, SKILL_MD)?;
        if current.is_empty() {
            any_new = true;
        } else {
            any_refreshed = true;
        }
    }

    // The standing-instructions pointer (binding cohort only).
    if let Some((instructions, updated, true, section_existed)) = pointer_plan {
        if let Some(parent) = instructions.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&instructions, updated)?;
        if section_existed {
            any_refreshed = true;
        } else {
            any_new = true;
        }
    }

    Ok(if any_new {
        InstallOutcome::Installed
    } else if any_refreshed {
        InstallOutcome::Refreshed
    } else {
        InstallOutcome::UpToDate
    })
}

/// Removes the skill from one harness's directories.
///
/// # Arguments
///
/// * `home` — the home directory.
/// * `entry` — the target harness.
///
/// # Returns
///
/// The uninstall outcome.
///
/// # Errors
///
/// Returns a boxed error when a file cannot be written.
pub fn uninstall(
    home: &Path,
    entry: &HarnessEntry,
) -> Result<UninstallOutcome, Box<dyn std::error::Error>> {
    let mut removed_anything = false;

    let path = skill_path(home, entry);
    if path.exists() {
        std::fs::remove_file(&path)?;
        removed_anything = true;
        // Tidy the now-empty skill directories; ignoring failures
        // keeps uninstall best-effort like the rest of the tooling.
        let _ = std::fs::remove_dir(path.parent().unwrap_or_else(|| Path::new(".")));
        let _ = std::fs::remove_dir(
            path.parent()
                .and_then(Path::parent)
                .unwrap_or_else(|| Path::new(".")),
        );
    }

    if let Some(instructions) = pointer_file(home, entry).filter(|path| path.exists()) {
        let current = read_or_default(&instructions).map_err(|error| {
            format!(
                "cannot read {} before uninstall: {error}",
                instructions.display()
            )
        })?;
        let (updated, removed) = without_section(&current);
        if removed {
            std::fs::write(&instructions, updated)?;
            removed_anything = true;
        }
    }

    Ok(if removed_anything {
        UninstallOutcome::Removed
    } else {
        UninstallOutcome::NothingToRemove
    })
}

/// Reads a file to a string, treating ONLY a missing file as empty.
///
/// Install/uninstall must never truncate an existing user-owned file
/// (an AGENTS.md with invalid UTF-8, a permission error, or a dangling
/// link): anything but a missing file surfaces as an error, or the next
/// `write` would silently destroy content the unreadable read just hid.
fn read_or_default(path: &Path) -> std::io::Result<String> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(contents),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(error),
    }
}

/// Reports whether one harness's installed skill matches the shipped
/// content.
///
/// # Arguments
///
/// * `home` — the home directory.
/// * `entry` — the target harness.
///
/// # Returns
///
/// The skill status.
pub fn status(home: &Path, entry: &HarnessEntry) -> SkillStatus {
    let path = skill_path(home, entry);
    let Ok(current) = std::fs::read(&path) else {
        return SkillStatus::Missing;
    };
    if current == SKILL_MD.as_bytes() {
        SkillStatus::Installed
    } else {
        SkillStatus::Stale
    }
}
