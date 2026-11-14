//! CU-02/CU-10 regressions: stable token files are an audited
//! read/create transaction, and database-path canonicalization is
//! identity-stable through symlink aliases.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::Path;
use wrkspc_cli::util::{canonical_db_path, stable_token, stable_token_sync};

/// A scratch directory per test, removed on drop.
struct Scratch {
    dir: std::path::PathBuf,
}

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("wrkspc-util-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }
    fn join(&self, name: &str) -> std::path::PathBuf {
        self.dir.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// CU-02: an existing EMPTY file with permissive mode is an ERROR, not
/// permission to generate — the credential is never silently rotated.
#[tokio::test]
async fn empty_existing_token_file_is_an_error() {
    let scratch = Scratch::new("empty");
    let db = scratch.join("node.db");
    let sidecar = scratch.join("node.db.ui-token");
    std::fs::write(&sidecar, "").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o644)).unwrap();
    }
    let error = stable_token(&db, "ui-token").await.unwrap_err();
    assert!(error.contains("empty"), "got: {error}");
    assert_eq!(
        std::fs::read_to_string(&sidecar).unwrap(),
        "",
        "the file must not have been written"
    );
}

/// CU-02: a symlinked sidecar is refused, and its target is untouched.
/// Unix-only: symlink creation on Windows needs different std APIs and
/// privileges (WRKSPC-031).
#[cfg(unix)]
#[tokio::test]
async fn symlinked_token_file_is_refused_and_its_target_untouched() {
    let scratch = Scratch::new("symlink");
    let db = scratch.join("node.db");
    let sidecar = scratch.join("node.db.ui-token");
    let victim = scratch.join("victim.txt");
    std::fs::write(&victim, "unrelated user text").unwrap();
    std::os::unix::fs::symlink(&victim, &sidecar).unwrap();

    let error = stable_token(&db, "ui-token").await.unwrap_err();
    assert!(error.contains("symlink"), "got: {error}");
    assert_eq!(
        std::fs::read_to_string(&victim).unwrap(),
        "unrelated user text",
        "the link target must not be replaced"
    );
}

/// CU-02: creation is exclusive and stable under REAL concurrent
/// publication — first-callers on separate OS threads all receive the
/// SAME token value even while the winner is mid-write (WRKSPC-027).
/// The previous version spawned onto the single-threaded test runtime
/// and, with no await point inside the token path, ran every caller
/// sequentially — the create→write publication window was never
/// exercised. Each round uses a fresh scratch dir so the create race
/// repeats.
#[test]
fn concurrent_first_callers_agree_on_one_token() {
    for round in 0..8 {
        let scratch = Scratch::new(&format!("concurrent-{round}"));
        let db = scratch.join("node.db");
        let mut tokens = Vec::new();
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..16 {
                let db = db.clone();
                handles.push(scope.spawn(move || stable_token_sync(&db, "ui-token")));
            }
            for handle in handles {
                tokens.push(handle.join().expect("token thread panicked"));
            }
        });
        let mut tokens: Vec<String> = tokens
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|error| panic!("every caller must succeed: {error}"))
            })
            .collect();
        tokens.sort();
        tokens.dedup();
        assert_eq!(
            tokens.len(),
            1,
            "every caller must agree on one value: {tokens:?}"
        );
    }
}

/// CU-02: a permissive existing token file is tightened to 0600
/// before its value is returned (not rotated).
#[tokio::test]
async fn permissive_existing_token_is_tightened_not_rotated() {
    let scratch = Scratch::new("tighten");
    let db = scratch.join("node.db");
    let sidecar = scratch.join("node.db.ui-token");
    std::fs::write(&sidecar, "existing-value").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o644)).unwrap();
    }
    let token = stable_token(&db, "ui-token").await.unwrap();
    assert_eq!(token, "existing-value", "the value must not change");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::symlink_metadata(&sidecar)
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "group/world bits must be gone");
    }
}

/// CU-10: the same database reached through a symlink alias maps to
/// the SAME canonical path, so sidecar keys and instance identity do
/// not change across restarts. Unix-only: symlink creation on Windows
/// needs different std APIs and privileges (WRKSPC-031).
#[cfg(unix)]
#[test]
fn canonical_path_resolves_final_component_aliases() {
    let scratch = Scratch::new("canonical");
    let real = scratch.join("real.db");
    std::fs::write(&real, "placeholder").unwrap();
    let alias = scratch.join("alias.db");
    std::os::unix::fs::symlink(&real, &alias).unwrap();

    assert_eq!(
        canonical_db_path(&alias).unwrap(),
        canonical_db_path(&real).unwrap(),
        "the alias and the real path must canonicalize identically"
    );
}

/// CU-10: a not-yet-existing database canonicalizes its parent and
/// keeps the unresolved tail.
#[test]
fn canonical_path_resolves_the_parent_of_a_new_database() {
    let scratch = Scratch::new("new-db");
    let parent = scratch.join("sub");
    std::fs::create_dir_all(&parent).unwrap();
    let db = parent.join("fresh.db");
    let resolved = canonical_db_path(&db).unwrap();
    assert_eq!(
        resolved.file_name().and_then(|n| n.to_str()),
        Some("fresh.db"),
        "the tail stays verbatim"
    );
    assert!(
        resolved.starts_with(std::fs::canonicalize(&parent).unwrap()),
        "the parent must be resolved: {resolved:?}"
    );
    let _ = db;
    let _ = Path::new("");
}

/// WRKSPC-011: on Windows, a created token file must carry an
/// owner-only, non-inherited DACL, and a pre-existing token file is
/// tightened to the same before its value is used — the Windows
/// equivalent of the Unix 0600 assertion above (which does not
/// exercise DACLs).
#[cfg(windows)]
#[test]
fn windows_token_files_carry_owner_only_acls() {
    let scratch = Scratch::new("acl-create");
    let db = scratch.join("node.db");
    let token = stable_token_sync(&db, "ui-token").unwrap();
    assert!(!token.is_empty());
    assert!(
        wrkspc_cli::util::token_acl_is_owner_only(&scratch.join("node.db.ui-token")).unwrap(),
        "a created token file must grant access to the owner alone"
    );

    let scratch = Scratch::new("acl-tighten");
    let db = scratch.join("node.db");
    std::fs::write(scratch.join("node.db.ui-token"), "existing-value").unwrap();
    let read_back = stable_token_sync(&db, "ui-token").unwrap();
    assert_eq!(read_back, "existing-value", "the value must not change");
    assert!(
        wrkspc_cli::util::token_acl_is_owner_only(&scratch.join("node.db.ui-token")).unwrap(),
        "a pre-existing token file must be tightened to owner-only before use"
    );
}
