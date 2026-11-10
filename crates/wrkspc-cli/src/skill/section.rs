//! Pointer-section text operations: insert, replace, and remove the
//! wrkspc marker section in a harness standing-instructions file.

use super::HarnessEntry;
use super::detect::skill_path;
use super::{SECTION_END, SECTION_START};
use std::path::Path;

pub(crate) fn pointer_section(entry: &HarnessEntry) -> String {
    match entry.id {
        "gemini-cli" => concat!(
            "Wrkspc is available through MCP tools (list-peers, ",
            "post-message, read-conversation, report-work). Every message ",
            "you receive carries a `[wrkspc] conversation: <id>` and ",
            "`[wrkspc] you are: <peer id>` prefix; use the conversation id ",
            "when posting replies. Quote the exact return text of every ",
            "tool call and never claim a send unless the tool returned ",
            "`accepted` or `replay`."
        )
        .to_string(),
        _ => {
            let path = skill_path(Path::new("$HOME"), entry);
            format!(
                "A wrkspc skill is installed at `{}`. Load it when working \
                 with wrkspc conversations, peers, or tools.",
                path.display()
            )
        }
    }
}

/// Byte offsets of every line whose trimmed content is exactly `marker`
/// — marker text embedded in unrelated prose is never a delimiter.
fn marker_lines(content: &str, marker: &str) -> Vec<usize> {
    content
        .match_indices(marker)
        .filter(|&(offset, _)| {
            let line_start = content[..offset].rfind('\n').map(|p| p + 1).unwrap_or(0);
            let line_end = content[offset..]
                .find('\n')
                .map(|p| offset + p)
                .unwrap_or(content.len());
            content[line_start..line_end].trim_end() == marker
        })
        .map(|(offset, _)| offset)
        .collect()
}

/// Locates the wrkspc section as a strictly ordered, unique marker
/// pair on whole lines (CU-03).
///
/// Orphaned, duplicate, nested, or reversed markers are REFUSED
/// before either file is modified — ambiguous user content is
/// preserved and the operator is told to pair or remove the markers
/// by hand.
///
/// # Returns
///
/// `Ok(Some((start, end)))` with the span from the START line's first
/// byte through the END line's trailing newline (inclusive, clamped);
/// `Ok(None)` when no markers exist.
///
/// # Errors
///
/// Returns the malformed-marker message.
fn section_span(content: &str) -> Result<Option<(usize, usize)>, String> {
    let starts = marker_lines(content, SECTION_START);
    let ends = marker_lines(content, SECTION_END);
    match (starts.len(), ends.len()) {
        (0, 0) => Ok(None),
        (1, 1) => {
            let start = starts[0];
            let end_line_start = ends[0];
            if start > end_line_start {
                return Err(
                    "wrkspc markers are reversed (end before start) — pair or remove them manually before reinstalling"
                        .to_string(),
                );
            }
            let end = content[end_line_start..]
                .find('\n')
                .map(|p| end_line_start + p + 1)
                .unwrap_or(content.len());
            Ok(Some((start, end)))
        }
        _ => Err(
            "orphaned, duplicate, or nested wrkspc markers — pair or remove them manually before reinstalling"
                .to_string(),
        ),
    }
}

/// Replaces the wrkspc-marked section of a file's content with `body`
/// (inserting it when absent) and returns the new content plus whether
/// anything changed.
///
/// # Arguments
///
/// * `content` — the current file content (or empty when the file does
///   not exist).
/// * `body` — the section body (without the markers).
///
/// # Returns
///
/// The updated content and a `changed` flag.
///
/// # Errors
///
/// Returns the malformed-marker message when the file carries markers
/// this code cannot safely own.
pub(crate) fn with_section(content: &str, body: &str) -> Result<(String, bool), String> {
    let wanted_block = format!("{SECTION_START}\n{body}\n{SECTION_END}\n");
    match section_span(content)? {
        Some((start, end)) => {
            if content[start..end] == wanted_block {
                return Ok((content.to_string(), false));
            }
            let mut replaced = content.to_string();
            replaced.replace_range(start..end, &wanted_block);
            Ok((replaced, true))
        }
        None => {
            // Exactly one separator newline for non-empty content: the
            // uninstall removes exactly one newline before the START
            // marker, so the round trip is byte-exact (CU-03's
            // trailing-newline defect).
            let separator = if content.is_empty() { "" } else { "\n" };
            Ok((format!("{content}{separator}{wanted_block}"), true))
        }
    }
}

/// Removes the wrkspc-marked section from a file's content and returns
/// the new content plus whether a section was found.
///
/// # Arguments
///
/// * `content` — the current file content.
///
/// # Returns
///
/// The updated content and a `removed` flag.
///
/// # Errors
///
/// Returns the malformed-marker message when the file carries markers
/// this code cannot safely own.
pub(crate) fn without_section(content: &str) -> Result<(String, bool), String> {
    let Some((start, end)) = section_span(content)? else {
        return Ok((content.to_string(), false));
    };
    // Mirror the install-time separator: exactly one newline before
    // the START marker is wrkspc-owned.
    let mut head = start;
    if head > 0 && content[..head].ends_with('\n') {
        head -= 1;
    }
    let mut removed = content.to_string();
    removed.replace_range(head..end, "");
    // The span consumed the END line's trailing newline — when content
    // follows the block (the user appended instructions later), that
    // newline was the ONLY separator between the prefix and the
    // appended text. Restore exactly one so an appended heading does
    // not glue itself to the preceding paragraph. A suffix that
    // already starts with a newline (blank lines after the block)
    // needs nothing.
    if head > 0
        && end < content.len()
        && !removed.is_empty()
        && !removed[..head].ends_with('\n')
        && !removed[head..].starts_with('\n')
    {
        removed.insert(head, '\n');
    }
    Ok((removed, true))
}
