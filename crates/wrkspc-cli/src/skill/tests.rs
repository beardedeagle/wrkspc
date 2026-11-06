use super::*;
use std::path::Path;

#[test]
fn registry_entries_are_well_formed() {
    assert!(HARNESSES.len() >= 70, "registry covers the ecosystem table");
    let mut ids = HARNESSES.iter().map(|entry| entry.id).collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), HARNESSES.len(), "ids are unique");
    for entry in HARNESSES {
        assert!(!entry.id.is_empty());
        assert!(!entry.skills_dir.is_empty());
        assert!(
            !entry.detect.is_empty(),
            "{} needs detection paths",
            entry.id
        );
        assert!(
            !entry.skills_dir.starts_with('/'),
            "{} skills dir must be home-relative",
            entry.id
        );
    }
}

#[test]
fn binding_cohort_has_pointer_files() {
    for entry in HARNESSES {
        if entry.pointer {
            assert!(
                matches!(entry.id, "codex" | "claude-code" | "gemini-cli"),
                "unexpected pointer member {}",
                entry.id
            );
            assert!(
                pointer_file(Path::new("/tmp"), entry).is_some(),
                "{} needs a pointer file",
                entry.id
            );
        }
    }
}

#[test]
fn section_insert_and_replace_are_idempotent() {
    let original = "# User notes\n\nkeep this\n";
    let body = "wrkspc pointer";
    let (first, changed) = with_section(original, body);
    assert!(changed);
    assert!(first.starts_with("# User notes\n\nkeep this\n\n"));
    assert!(first.contains(SECTION_START) && first.contains(SECTION_END));

    let (second, changed) = with_section(&first, body);
    assert!(!changed, "second write must be a no-op");
    assert_eq!(first, second);

    let (updated, changed) = with_section(&second, "new body");
    assert!(changed);
    assert!(updated.contains("new body"));
    assert!(!updated.contains("wrkspc pointer"));
    assert_eq!(updated.matches(SECTION_START).count(), 1);
}

#[test]
fn section_removal_preserves_user_content() {
    let original = "# User notes\n\nkeep this\n";
    let (with, _) = with_section(original, "wrkspc pointer");
    let (removed, found) = without_section(&with);
    assert!(found);
    assert_eq!(removed, original);
}

#[test]
fn section_removal_when_absent_is_a_no_op() {
    let (removed, found) = without_section("plain content\n");
    assert!(!found);
    assert_eq!(removed, "plain content\n");
}

#[test]
fn parse_is_case_insensitive_and_rejects_unknown() {
    assert_eq!(parse("codex").map(|e| e.name), Some("Codex"));
    assert_eq!(parse("Claude-Code").map(|e| e.id), Some("claude-code"));
    assert!(parse("nope").is_none());
}

/// Three detected entries for the selection tests.
fn detected_fixture() -> Vec<&'static HarnessEntry> {
    vec![
        parse("codex").unwrap(),
        parse("claude-code").unwrap(),
        parse("gemini-cli").unwrap(),
    ]
}

#[test]
fn parse_selection_handles_numbers_ids_all_and_empty() {
    let detected = detected_fixture();

    assert_eq!(
        parse_selection("", &detected).unwrap(),
        Vec::<&'static HarnessEntry>::new()
    );
    assert_eq!(
        parse_selection("all", &detected).unwrap(),
        detected,
        "`all` selects everything"
    );
    assert_eq!(
        parse_selection("2, gemini-cli", &detected).unwrap(),
        vec![parse("claude-code").unwrap(), parse("gemini-cli").unwrap()],
        "numbers index the detected list; ids resolve case-insensitively"
    );
    assert_eq!(
        parse_selection("1, codex, 1", &detected).unwrap(),
        vec![parse("codex").unwrap()],
        "duplicates collapse"
    );
}

#[test]
fn parse_selection_rejects_unknown_and_out_of_range() {
    let detected = detected_fixture();
    assert!(
        parse_selection("9", &detected)
            .unwrap_err()
            .contains("out of range")
    );
    // Zero is out of range: choices number from 1, and an invalid
    // selection must not silently install into the first harness.
    assert!(
        parse_selection("0", &detected)
            .unwrap_err()
            .contains("out of range")
    );
    assert!(
        parse_selection("codex, nope", &detected)
            .unwrap_err()
            .contains("unknown harness")
    );
}

#[test]
fn select_install_targets_requires_flags_when_not_interactive() {
    let home = std::env::temp_dir();
    let error = select_install_targets(
        &home,
        &None,
        false,
        false,
        &mut std::io::Cursor::new(Vec::new()),
        &mut std::io::sink(),
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("--all") && error.contains("--harness"),
        "the hint must name both escape hatches: {error}"
    );
}

#[test]
fn select_install_targets_interactive_prompts_and_parses() {
    let home = std::env::temp_dir();
    let detected = detect(&home);
    let mut input = std::io::Cursor::new(b"all\n".to_vec());
    let mut output = Vec::new();
    let selected =
        select_install_targets(&home, &None, false, true, &mut input, &mut output).unwrap();
    assert_eq!(selected, detected, "`all` selects every detected entry");
    let prompt = String::from_utf8(output).unwrap();
    assert!(
        prompt.contains("select harnesses"),
        "the prompt must ask: {prompt}"
    );

    let mut input = std::io::Cursor::new(b"\n".to_vec());
    let selected =
        select_install_targets(&home, &None, false, true, &mut input, &mut std::io::sink())
            .unwrap();
    assert!(selected.is_empty(), "empty input cancels the install");
}

#[test]
fn select_install_targets_honors_harness_and_all_flags() {
    let home = std::env::temp_dir();
    let empty: Option<Vec<String>> = None;
    let mut sink_input = std::io::Cursor::new(Vec::new());
    let mut sink_output = std::io::sink();

    let named = select_install_targets(
        &home,
        &Some(vec!["codex".to_string()]),
        false,
        false,
        &mut sink_input,
        &mut sink_output,
    )
    .unwrap();
    assert_eq!(named, vec![parse("codex").unwrap()]);

    let all = select_install_targets(
        &home,
        &empty,
        true,
        false,
        &mut sink_input,
        &mut sink_output,
    )
    .unwrap();
    assert_eq!(all, detect(&home));
}
