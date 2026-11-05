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
pub(crate) fn with_section(content: &str, body: &str) -> (String, bool) {
    let start = content.find(SECTION_START);
    let end = content.find(SECTION_END);
    match (start, end) {
        (Some(start), Some(end)) if start < end => {
            let wanted = format!("{SECTION_START}\n{body}\n{SECTION_END}");
            let current = &content[start..end + SECTION_END.len()];
            if current == wanted {
                (content.to_string(), false)
            } else {
                let mut replaced = content.to_string();
                replaced.replace_range(start..end + SECTION_END.len(), &wanted);
                (replaced, true)
            }
        }
        _ => {
            // Append only: a blank line separates the section from the
            // user's content (uninstall removes exactly this boundary).
            let separator = if content.is_empty() {
                ""
            } else if content.ends_with('\n') {
                "\n"
            } else {
                "\n\n"
            };
            (
                format!("{content}{separator}{SECTION_START}\n{body}\n{SECTION_END}\n"),
                true,
            )
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
pub(crate) fn without_section(content: &str) -> (String, bool) {
    let start = content.find(SECTION_START);
    let end = content.find(SECTION_END);
    match (start, end) {
        (Some(start), Some(end)) if start < end => {
            let mut tail = end + SECTION_END.len();
            if content[tail..].starts_with('\n') {
                tail += 1;
            }
            // Mirror the install-time separator: exactly one newline of
            // the blank line before the section is wrkspc-owned.
            let mut head = start;
            if head > 0 && content[..head].ends_with('\n') {
                head -= 1;
            }
            let mut removed = content.to_string();
            removed.replace_range(head..tail, "");
            (removed, true)
        }
        _ => (content.to_string(), false),
    }
}
