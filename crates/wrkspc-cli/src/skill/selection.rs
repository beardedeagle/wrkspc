//! Selection semantics: number/id parsing and the interactive
//! (or flag-driven) install-target picker.

use super::HarnessEntry;
use super::SkillStatus;
use super::apply::status;
use super::detect::{detect, parse};
use std::path::Path;

pub fn parse_selection(
    input: &str,
    detected: &[&'static HarnessEntry],
) -> Result<Vec<&'static HarnessEntry>, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if trimmed.eq_ignore_ascii_case("all") {
        return Ok(detected.to_vec());
    }
    let mut selected: Vec<&'static HarnessEntry> = Vec::new();
    for token in trimmed
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|token| !token.is_empty())
    {
        let entry = if let Ok(index) = token.parse::<usize>() {
            detected
                .get(index.saturating_sub(1))
                .copied()
                .ok_or_else(|| format!("selection {index} is out of range"))?
        } else {
            parse(token).ok_or_else(|| format!("unknown harness `{token}`"))?
        };
        if !selected.contains(&entry) {
            selected.push(entry);
        }
    }
    Ok(selected)
}

/// Selects the install targets for `wrkspc skill install`.
///
/// `--harness` and `--all` bypass the prompt; with neither, a terminal
/// gets the numbered list and a selection line, and a non-terminal
/// input errors with a usage hint instead of guessing.
///
/// # Arguments
///
/// * `home` — the home directory to detect under.
/// * `harness` — the explicit `--harness` values, when given.
/// * `all` — whether `--all` was passed.
/// * `interactive` — whether stdin is a terminal.
/// * `input` — the selection reader.
/// * `output` — the prompt writer.
///
/// # Returns
///
/// The entries to install into.
///
/// # Errors
///
/// Returns a boxed error for an unknown harness id, an unreadable or
/// invalid selection, an I/O failure on the prompt, or a non-interactive
/// run without `--harness`/`--all`.
pub(crate) fn select_install_targets(
    home: &Path,
    harness: &Option<Vec<String>>,
    all: bool,
    interactive: bool,
    input: &mut dyn std::io::BufRead,
    output: &mut dyn std::io::Write,
) -> Result<Vec<&'static HarnessEntry>, Box<dyn std::error::Error>> {
    if let Some(names) = harness {
        return names
            .iter()
            .map(|name| {
                parse(name).ok_or_else(|| {
                    format!("unknown harness `{name}` (see `wrkspc skill status --help` for ids)")
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into);
    }
    if all {
        return Ok(detect(home));
    }
    if !interactive {
        return Err(
            "install with no flags needs a terminal for selection; rerun with `--all` or `--harness <id>`"
                .into(),
        );
    }
    let detected = detect(home);
    for (index, entry) in detected.iter().enumerate() {
        let marker = match status(home, entry) {
            SkillStatus::Installed => " [installed]",
            SkillStatus::Stale => " [stale]",
            SkillStatus::Missing => "",
        };
        writeln!(output, "{:>2}. {}{}", index + 1, entry.name, marker)?;
    }
    write!(
        output,
        "select harnesses (numbers or ids, comma-separated, `all`, or empty to cancel): "
    )?;
    output.flush()?;
    let mut line = String::new();
    input.read_line(&mut line)?;
    parse_selection(&line, &detected).map_err(Into::into)
}
