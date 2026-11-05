//! The wrkspc skill installer.
//!
//! Ships the harness-facing skill (see `skills/wrkspc/SKILL.md` in the
//! repository) into the harnesses installed on a machine, so an agent
//! knows wrkspc exists and how to use its tool surface before sessions
//! start. The skill is compiled into the binary; no network access and
//! no external files are needed.
//!
//! Detection and install targets mirror the open agent-skills ecosystem
//! registry (the `skills` CLI, `vercel-labs/skills` `src/agents.ts`, MIT
//! licensed): one table entry per harness with its detection paths and
//! its global skills directory. The table covers every registry entry
//! with a machine-level install location; project-only entries (Eve,
//! PromptScript, Replit) and the registry's `universal` alias are
//! intentionally absent because they have no per-machine home.
//!
//! The three harnesses wrkspc can actually bind (codex, claude, gemini)
//! additionally receive a one-line pointer section in their
//! standing-instructions file, delimited by `<!-- WRKSPC_START -->` /
//! `<!-- WRKSPC_END -->`: installation is idempotent, uninstallation
//! removes exactly what installation added, and surrounding user content
//! is never touched.

use std::io::IsTerminal;
use std::path::PathBuf;

/// The shipped skill, embedded at compile time.
pub const SKILL_MD: &str = include_str!("../../../../skills/wrkspc/SKILL.md");

/// Section markers used to bound every wrkspc edit inside a
/// harness-owned standing-instructions file.
pub const SECTION_START: &str = "<!-- WRKSPC_START -->";
pub const SECTION_END: &str = "<!-- WRKSPC_END -->";

/// One harness the installer knows how to target.
///
/// Paths are relative to `$HOME` unless absolute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HarnessEntry {
    /// The `--harness` id.
    pub id: &'static str,
    /// The display name.
    pub name: &'static str,
    /// Candidate paths (relative to `$HOME` unless absolute) whose
    /// existence marks the harness installed. Any one suffices.
    pub detect: &'static [&'static str],
    /// The global skills directory (relative to `$HOME`).
    pub skills_dir: &'static str,
    /// Whether the harness gets the standing-instructions pointer
    /// section (the wrkspc binding cohort).
    pub pointer: bool,
}

const fn harness(
    id: &'static str,
    name: &'static str,
    detect: &'static [&'static str],
    skills_dir: &'static str,
) -> HarnessEntry {
    HarnessEntry {
        id,
        name,
        detect,
        skills_dir,
        pointer: false,
    }
}

const fn harness_with_pointer(
    id: &'static str,
    name: &'static str,
    detect: &'static [&'static str],
    skills_dir: &'static str,
) -> HarnessEntry {
    HarnessEntry {
        id,
        name,
        detect,
        skills_dir,
        pointer: true,
    }
}

/// Whether a harness's skill copy matches the shipped content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillStatus {
    /// The skill is installed and matches the shipped content.
    Installed,
    /// The skill is installed but differs from the shipped content.
    Stale,
    /// The skill is not installed.
    Missing,
}

/// The outcome of installing into one harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstallOutcome {
    /// Fresh installation (nothing was there before).
    Installed,
    /// The skill already matched the shipped content; nothing changed.
    UpToDate,
    /// The skill was present but stale and has been refreshed.
    Refreshed,
}

/// The outcome of uninstalling from one harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UninstallOutcome {
    /// The skill and any wrkspc-marked content were removed.
    Removed,
    /// Nothing wrkspc-owned was present.
    NothingToRemove,
}

/// Resolves a `--harness` value.
///
/// # Arguments
///
/// * `id` — the harness id (case-insensitive).
///
/// # Returns
///
/// The registry entry, or `None` for an unknown id.
pub mod apply;
pub mod detect;
pub mod registry;
pub mod section;
pub mod selection;
#[cfg(test)]
mod tests;

pub use apply::{install, status, uninstall};
#[cfg(test)]
pub(crate) use detect::pointer_file;
pub use detect::{detect, parse};
pub use registry::HARNESSES;
pub(crate) use section::{pointer_section, with_section, without_section};
pub use selection::parse_selection;
pub(crate) use selection::select_install_targets;

/// Runs the `wrkspc skill` subcommand.
///
/// # Arguments
///
/// * `args` — the skill command and its options.
///
/// # Errors
///
/// Returns a boxed error when `$HOME` is unset, a harness name is
/// unknown, or a file operation fails.
pub async fn run_skill(args: crate::SkillCommand) -> Result<(), Box<dyn std::error::Error>> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or("HOME is not set; cannot locate harness directories")?;
    let selected =
        |names: &Option<Vec<String>>| -> Result<Vec<&'static HarnessEntry>, Box<dyn std::error::Error>> {
            match names {
                Some(names) => names
                    .iter()
                    .map(|name| {
                        parse(name).ok_or_else(|| {
                            format!("unknown harness `{name}` (see `wrkspc skill status --help` for ids)")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(Into::into),
                None => Ok(detect(&home)),
            }
        };
    match args {
        crate::SkillCommand::Install { harness, all } => {
            let entries = select_install_targets(
                &home,
                &harness,
                all,
                std::io::stdin().is_terminal(),
                &mut std::io::stdin().lock(),
                &mut std::io::stdout(),
            )?;
            if entries.is_empty() {
                println!("nothing selected; no changes");
                return Ok(());
            }
            for entry in entries {
                let outcome = install(&home, entry)?;
                println!("{}: skill {outcome:?}", entry.name);
            }
        }
        crate::SkillCommand::Uninstall { harness } => {
            for entry in selected(&harness)? {
                let outcome = uninstall(&home, entry)?;
                println!("{}: {outcome:?}", entry.name);
            }
        }
        crate::SkillCommand::Status => {
            for entry in selected(&None)? {
                println!("{}: {:?}", entry.name, status(&home, entry));
            }
        }
    }
    Ok(())
}
