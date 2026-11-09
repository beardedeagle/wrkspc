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
    let (first, changed) = with_section(original, body).unwrap();
    assert!(changed);
    assert!(first.starts_with("# User notes\n\nkeep this\n\n"));
    assert!(first.contains(SECTION_START) && first.contains(SECTION_END));

    let (second, changed) = with_section(&first, body).unwrap();
    assert!(!changed, "second write must be a no-op");
    assert_eq!(first, second);

    let (updated, changed) = with_section(&second, "new body").unwrap();
    assert!(changed);
    assert!(updated.contains("new body"));
    assert!(!updated.contains("wrkspc pointer"));
    assert_eq!(updated.matches(SECTION_START).count(), 1);
}

#[test]
fn section_removal_preserves_user_content() {
    let original = "# User notes\n\nkeep this\n";
    let (with, _) = with_section(original, "wrkspc pointer").unwrap();
    let (removed, found) = without_section(&with).unwrap();
    assert!(found);
    assert_eq!(removed, original);
}

#[test]
fn section_removal_when_absent_is_a_no_op() {
    let (removed, found) = without_section("plain content\n").unwrap();
    assert!(!found);
    assert_eq!(removed, "plain content\n");
}

/// CU-03: a file WITHOUT a final newline round-trips byte-exactly
/// (install then uninstall restores the original text).
#[test]
fn no_final_newline_round_trips_exactly() {
    let original = "keep user text";
    let (with, _) = with_section(original, "wrkspc pointer").unwrap();
    let (removed, found) = without_section(&with).unwrap();
    assert!(found);
    assert_eq!(removed, original);
}

/// CU-03: an orphaned START marker must never be "completed" by a
/// later install deleting user text — it is refused outright.
#[test]
fn orphan_start_marker_is_refused_not_repaired() {
    let content = format!("# Notes\n\nkeep me\n\n{SECTION_START}\nstale body\n");
    assert!(with_section(&content, "wrkspc pointer").is_err());
    assert!(without_section(&content).is_err());
    // The refusal leaves the ambiguous content intact for the operator.
}

/// CU-03: an orphaned END marker refuses both install and uninstall
/// instead of appending a second section.
#[test]
fn orphan_end_marker_is_refused_not_appended() {
    let content = format!("# Notes\n\n{SECTION_END}\n");
    assert!(with_section(&content, "wrkspc pointer").is_err());
    assert!(without_section(&content).is_err());
}

/// CU-03: duplicate start markers (an earlier interrupted install)
/// are refused, never silently re-paired.
#[test]
fn duplicate_start_markers_are_refused() {
    let content = format!("# Notes\n\n{SECTION_START}\nold\n{SECTION_START}\nnew\n{SECTION_END}\n");
    assert!(with_section(&content, "wrkspc pointer").is_err());
}

/// CU-03: reversed markers are refused, not treated as an empty
/// section that swallows the content between them.
#[test]
fn reversed_markers_are_refused() {
    let content = format!("keep me\n\n{SECTION_END}\nbody\n{SECTION_START}\n");
    assert!(with_section(&content, "wrkspc pointer").is_err());
    assert!(without_section(&content).is_err());
}

/// CU-03: marker TEXT inside prose is not a delimiter — install
/// appends one section and uninstall removes only it.
#[test]
fn marker_text_in_prose_is_not_a_delimiter() {
    let content = format!("The docs mention {SECTION_START} in prose.\n\nkeep\n");
    let (with, changed) = with_section(&content, "wrkspc pointer").unwrap();
    assert!(changed);
    assert_eq!(
        with.matches(SECTION_START).count(),
        2,
        "prose copy + one real marker"
    );
    let (removed, found) = without_section(&with).unwrap();
    assert!(found);
    assert_eq!(removed, content);
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

/// CU-11: unset, empty, and relative HOME values are refused by the
/// pure validation core; a valid absolute (even nonexistent) root
/// passes — the supported synthetic-home case.
#[test]
fn home_validation_rejects_unset_empty_and_relative() {
    assert!(
        super::validated_home_from(None)
            .unwrap_err()
            .to_string()
            .contains("not set")
    );
    assert!(
        super::validated_home_from(Some("".into()))
            .unwrap_err()
            .to_string()
            .contains("empty")
    );
    assert!(
        super::validated_home_from(Some("relative/home".into()))
            .unwrap_err()
            .to_string()
            .contains("absolute")
    );
    assert!(super::validated_home_from(Some("/nonexistent/absolute".into())).is_ok());
}
