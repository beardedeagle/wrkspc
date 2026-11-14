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
            replace_file_atomically(&instructions, &updated)?;
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
/// `home` is the home directory; `entry` is the target harness.
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
            replace_file_atomically(&instructions, &updated)?;
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
/// The containment check runs on the nearest EXISTING ancestor BEFORE
/// any directory is created (WRKSPC-022): an escaping symlink in a
/// not-yet-created tail is refused without `create_dir_all` ever
/// following it, so a rejected install cannot create anything outside
/// home. The completed parent is re-validated afterwards in case the
/// directory tree changed while it was being made.
///
/// `home` is the (already validated) home root; `target` is the leaf
/// being written or deleted.
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
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let resolved_home = std::fs::canonicalize(home)?;
    // Walk up to the nearest existing ancestor of the parent; every
    // component above it resolves, exactly like the database-path
    // canonicalization. A dangling symlink in the path is treated as
    // existing and fails canonicalization — refused, never written
    // through.
    let mut existing = parent;
    loop {
        match std::fs::symlink_metadata(existing) {
            Ok(_) => break,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                existing = existing.parent().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("no existing ancestor for {}", parent.display()),
                    )
                })?;
            }
            Err(e) => return Err(e.into()),
        }
    }
    let resolved_existing = std::fs::canonicalize(existing)?;
    if !resolved_existing.starts_with(&resolved_home) {
        return Err(format!(
            "{} resolves outside {} — refusing",
            parent.display(),
            resolved_home.display()
        )
        .into());
    }
    // Only now create the missing tail.
    std::fs::create_dir_all(parent)?;
    let resolved_parent = std::fs::canonicalize(parent)?;
    if !resolved_parent.starts_with(&resolved_home) {
        return Err(format!(
            "{} resolves outside {} — refusing",
            parent.display(),
            resolved_home.display()
        )
        .into());
    }
    Ok(())
}

/// Replaces `path` with `contents` atomically (WRKSPC-021): the full
/// text is written to a uniquely-named temporary sibling (created
/// exclusively, synced to disk), given the destination's existing
/// permissions, and only then renamed over the destination. A failure
/// at ANY step — including the rename itself — removes the temporary
/// and leaves the destination byte-for-byte untouched, so an I/O
/// error or crash can never destroy user content outside the wrkspc
/// markers the way a truncate-in-place write could.
fn replace_file_atomically(path: &Path, contents: &str) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("no parent directory for {}", path.display()),
            )
        })?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("no file name for {}", path.display()),
            )
        })?;
    // The destination's permissions (mode bits / Windows attributes)
    // survive the replacement; a brand-new destination keeps the
    // defaults a plain create would have produced.
    let permissions = std::fs::metadata(path).ok().map(|meta| meta.permissions());
    let mut attempt: u32 = 0;
    let (tmp_path, mut tmp) = loop {
        let candidate = parent.join(format!(
            ".{name}.wrkspc-tmp-{}-{attempt}",
            std::process::id()
        ));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => break (candidate, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                attempt += 1;
                if attempt >= 16 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!("cannot create a temporary sibling for {}", path.display()),
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    };
    // A failure below must never leave the temporary behind; the
    // destination is only ever touched by the final rename.
    let write_result = (|| {
        use std::io::Write;
        tmp.write_all(contents.as_bytes())?;
        tmp.sync_all()?;
        if let Some(ref permissions) = permissions {
            std::fs::set_permissions(&tmp_path, permissions.clone())?;
        }
        std::fs::rename(&tmp_path, path)
    })();
    drop(tmp);
    if let Err(error) = write_result {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(error);
    }
    // Best-effort directory sync so the rename itself is durable
    // across a crash; filesystems that cannot sync a directory
    // handle simply skip it.
    #[cfg(unix)]
    if let Ok(dir) = std::fs::File::open(parent) {
        let _ = dir.sync_all();
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
/// `home` is the home directory; `entry` is the target harness.
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
/// from copy freshness (CU-09): `n/a` for harnesses without a pointer
/// file, `current` when the section matches, `stale or missing` when
/// it drifted or is absent, and `unreadable: …`/`malformed: …` when
/// the file cannot be used — a harness whose pointer file is missing
/// or drifted is NOT a fully healthy install even when its skill copy
/// matches.
///
/// `home` is the home directory; `entry` is the target harness.
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    /// WRKSPC-021: a failed replacement leaves the destination
    /// byte-for-byte untouched and no temporary sibling behind. A
    /// non-empty directory stands in for any rename-stage failure
    /// (quota, I/O error, permission change): the complete text was
    /// written and synced to the temporary, yet the destination is
    /// still only ever touched by the final rename.
    #[test]
    fn failed_replacement_preserves_the_destination() {
        let dir = std::env::temp_dir().join(format!(
            "wrkspc-apply-atomic-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let dest = dir.join("AGENTS.md");
        std::fs::create_dir_all(&dest).expect("directory in place of the file");
        std::fs::write(dest.join("user-note.txt"), "precious").expect("user content");

        let error = match replace_file_atomically(&dest, "# replaced") {
            Ok(()) => panic!("replacing onto a directory must fail"),
            Err(error) => error,
        };
        assert!(!error.to_string().is_empty());
        assert!(dest.is_dir(), "the destination must be untouched");
        assert_eq!(
            std::fs::read_to_string(dest.join("user-note.txt")).expect("read"),
            "precious",
            "the destination's content must survive the failed replace"
        );
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().contains("wrkspc-tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temporary siblings must be cleaned up: {leftovers:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// WRKSPC-021: a successful replacement produces the full new
    /// content and preserves the destination's existing permissions.
    #[cfg(unix)]
    #[test]
    fn successful_replacement_preserves_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!(
            "wrkspc-apply-perms-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let dest = dir.join("AGENTS.md");
        std::fs::write(&dest, "old content").expect("seed");
        std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o600)).expect("mode");

        replace_file_atomically(&dest, "new content").expect("replace");

        assert_eq!(
            std::fs::read_to_string(&dest).expect("read"),
            "new content",
            "the full updated content must be in place"
        );
        let mode = std::fs::symlink_metadata(&dest)
            .expect("stat")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "the original permissions must survive");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
