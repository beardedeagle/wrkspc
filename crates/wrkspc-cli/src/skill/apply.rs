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
            let (_, section_existed) = without_section(&current)?;
            let (updated, changed) = with_section(&current, &pointer_section(entry))?;
            Some((instructions, updated, changed, section_existed))
        } else {
            None
        };

    // The skills directory copy. The target is validated first (CU-04):
    // never a symlinked leaf, and the resolved parent must stay under
    // the resolved home — a dangling link is refused, not written
    // through.
    let path = skill_path(home, entry);
    let current = read_or_default(&path)
        .map_err(|error| format!("cannot read {} before install: {error}", path.display()))?;
    // The pre-install skill state, remembered so a failure in a LATER
    // step (the pointer write) rolls the copy back — a half-install
    // (new skill, missing pointer) must not persist.
    let skill_existed = matches!(std::fs::symlink_metadata(&path), Ok(meta) if meta.is_file());
    let mut skill_changed = false;
    if current != SKILL_MD {
        ensure_safe_target(home, &path)?;
        std::fs::write(&path, SKILL_MD)?;
        skill_changed = true;
        if current.is_empty() {
            any_new = true;
        } else {
            any_refreshed = true;
        }
    }

    // The standing-instructions pointer (binding cohort only). A
    // failure here rolls the skill copy back to its prior state.
    if let Some((instructions, updated, true, section_existed)) = pointer_plan {
        if let Err(error) = (|| -> Result<(), Box<dyn std::error::Error>> {
            ensure_safe_target(home, &instructions)?;
            std::fs::write(&instructions, updated)?;
            Ok(())
        })() {
            if skill_changed {
                if skill_existed {
                    std::fs::write(&path, &current)?;
                } else {
                    let _ = std::fs::remove_file(&path);
                }
            }
            return Err(error);
        }
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
        // Deleting goes through the same link/root policy (CU-04): a
        // symlinked leaf or an escaping parent is refused, never
        // followed.
        ensure_safe_target(home, &path)?;
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
        let (updated, removed) = without_section(&current)?;
        if removed {
            ensure_safe_target(home, &instructions)?;
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

/// Validates a write/delete target (CU-04): the leaf must be a real
/// file or absent — a symlink, dangling or live, is refused, never
/// written or deleted through — and the resolved parent directory
/// must live under the resolved home root, closing every path escape
/// (symlinked intermediate directories included).
///
/// # Arguments
///
/// * `home` — the (already validated) home root.
/// * `target` — the leaf being written or deleted.
///
/// # Errors
///
/// Returns the refusal reason.
fn ensure_safe_target(home: &Path, target: &Path) -> Result<(), Box<dyn std::error::Error>> {
    match std::fs::symlink_metadata(target) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(format!(
                "{} is a symlink — refusing to write through it",
                target.display()
            )
            .into());
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)?;
        let resolved_home = std::fs::canonicalize(home)?;
        let resolved_parent = std::fs::canonicalize(parent)?;
        if !resolved_parent.starts_with(&resolved_home) {
            return Err(format!(
                "{} resolves outside {} — refusing",
                parent.display(),
                resolved_home.display()
            )
            .into());
        }
    }
    Ok(())
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
/// content (CU-09).
///
/// Read failures are EXPOSED, not collapsed into `Missing`: a
/// permissions error, a directory in place of the file, or a
/// symlinked leaf each report their own problem, so remediation is
/// distinguishable from "not installed yet".
///
/// # Arguments
///
/// * `home` — the home directory.
/// * `entry` — the target harness.
///
/// # Returns
///
/// The skill status.
///
/// # Errors
///
/// Returns the read-failure reason.
pub fn status(home: &Path, entry: &HarnessEntry) -> Result<SkillStatus, String> {
    let path = skill_path(home, entry);
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(format!(
                "{} is a symlink — the installer will not read through it",
                path.display()
            ));
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(SkillStatus::Missing),
        Err(e) => return Err(format!("cannot inspect {}: {e}", path.display())),
    }
    let current =
        std::fs::read(&path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    if current == SKILL_MD.as_bytes() {
        Ok(SkillStatus::Installed)
    } else {
        Ok(SkillStatus::Stale)
    }
}

/// The standing-instructions pointer component, reported separately
/// from copy freshness (CU-09): `absent`, `current`, or `stale` — a
/// harness whose pointer file is missing or drifted is NOT a fully
/// healthy install even when its skill copy matches.
///
/// # Arguments
///
/// * `home` — the home directory.
/// * `entry` — the target harness.
pub fn pointer_state(home: &Path, entry: &HarnessEntry) -> String {
    let Some(instructions) = entry.pointer.then(|| pointer_file(home, entry)).flatten() else {
        return "n/a".to_string();
    };
    let current = match read_or_default(&instructions) {
        Ok(current) => current,
        Err(error) => return format!("unreadable: {error}"),
    };
    match with_section(&current, &pointer_section(entry)) {
        Ok((updated, changed)) => {
            if changed {
                "stale or missing".to_string()
            } else {
                let _ = updated;
                "current".to_string()
            }
        }
        Err(error) => format!("malformed: {error}"),
    }
}
