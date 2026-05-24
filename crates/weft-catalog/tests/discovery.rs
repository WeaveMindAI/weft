//! Discovery over a synthetic `nodes/` tree. Proves the two unit
//! forms (bare node, package), arbitrary nesting depth, auto-detected
//! package members, and the collision-is-a-hard-error rule.

use std::fs;
use std::path::Path;

use weft_catalog::{CatalogError, DiscoverPolicy, FsCatalog};

/// Write a minimal valid `metadata.json` declaring `node_type`.
fn write_node(dir: &Path, node_type: &str) {
    fs::create_dir_all(dir).unwrap();
    fs::write(
        dir.join("metadata.json"),
        format!(
            r#"{{ "type": "{node_type}", "label": "{node_type}", "description": "", "category": "Test", "inputs": [], "outputs": [] }}"#
        ),
    )
    .unwrap();
    fs::write(dir.join("mod.rs"), "// node impl\n").unwrap();
}

/// Write a `package.toml` naming the package (members auto-detected).
fn write_package_toml(dir: &Path, name: &str) {
    fs::create_dir_all(dir).unwrap();
    fs::write(
        dir.join("package.toml"),
        format!("[package]\nname = \"{name}\"\n\n[dependencies]\n"),
    )
    .unwrap();
}

/// A bare node sitting directly under `nodes/` is discovered, and is
/// its own degenerate one-member package.
#[test]
fn bare_node_at_depth_one() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");
    write_node(&nodes.join("debug"), "Debug");

    let cat = FsCatalog::discover(&nodes).unwrap();
    assert!(cat.entry("Debug").is_some());
    let pkg = cat.package_of("Debug").expect("bare node is its own package");
    assert_eq!(pkg.node_types, vec!["Debug".to_string()]);
    assert!(pkg.shared_rs.is_empty());
    assert!(pkg.package_deps.is_none());
}

/// Position and depth are irrelevant: a unit ten levels deep is found
/// just like one at the top.
#[test]
fn units_found_at_arbitrary_depth() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");
    write_node(&nodes.join("text"), "Text");
    let deep = nodes.join("a/b/c/d/e/f/g/h/i/j");
    write_node(&deep.join("buried"), "Buried");

    let cat = FsCatalog::discover(&nodes).unwrap();
    assert!(cat.entry("Text").is_some(), "shallow node missing");
    assert!(cat.entry("Buried").is_some(), "deep node missing");
}

/// A package root auto-detects its member nodes (every immediate
/// subdir with a `metadata.json`); no hand-maintained node list. Shared
/// `.rs` files at the root are collected. Members share the package_key.
#[test]
fn package_members_auto_detected() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");
    let pkg_root = nodes.join("nested/human");
    write_package_toml(&pkg_root, "human");
    write_node(&pkg_root.join("query"), "HumanQuery");
    write_node(&pkg_root.join("trigger"), "HumanTrigger");
    fs::write(pkg_root.join("form_helpers.rs"), "// shared\n").unwrap();

    let cat = FsCatalog::discover(&nodes).unwrap();
    let pkg = cat.package_of("HumanQuery").expect("package missing");
    assert_eq!(pkg.name, "human");
    assert_eq!(pkg.node_types.len(), 2, "both members auto-detected");
    assert!(pkg.node_types.iter().any(|t| t == "HumanQuery"));
    assert!(pkg.node_types.iter().any(|t| t == "HumanTrigger"));
    assert_eq!(pkg.shared_rs.len(), 1, "form_helpers.rs collected");
    assert!(pkg.package_deps.is_some(), "package deps present");
    let q = cat.entry("HumanQuery").unwrap();
    let t = cat.entry("HumanTrigger").unwrap();
    assert_eq!(q.package_key, t.package_key, "members share package_key");
}

/// A package root does not descend into its members looking for
/// sub-packages: the unit stops at the package boundary.
#[test]
fn package_does_not_nest() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");
    let pkg_root = nodes.join("pkg");
    write_package_toml(&pkg_root, "pkg");
    write_node(&pkg_root.join("member"), "Member");
    // A `metadata.json` two levels below the package root must NOT be
    // picked up as a separate unit: discovery stops at the package.
    write_node(&pkg_root.join("member/inner"), "InnerShouldBeIgnored");

    let cat = FsCatalog::discover(&nodes).unwrap();
    assert!(cat.entry("Member").is_some());
    assert!(
        cat.entry("InnerShouldBeIgnored").is_none(),
        "discovery must not descend past a unit boundary",
    );
}

/// Two units declaring the same `node_type` is ambiguous, not a
/// shadow: it fails loudly with both paths.
#[test]
fn duplicate_node_type_is_hard_error() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");
    write_node(&nodes.join("first"), "Dup");
    write_node(&nodes.join("second"), "Dup");

    let err = FsCatalog::discover(&nodes).expect_err("collision must error");
    match err {
        CatalogError::Collision { node_type, .. } => assert_eq!(node_type, "Dup"),
        other => panic!("expected Collision, got {other:?}"),
    }
}

/// A package with no member nodes (no subdir with metadata.json) is an
/// error: an empty package is almost certainly a mistake.
#[test]
fn empty_package_is_error() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");
    write_package_toml(&nodes.join("empty"), "empty");

    let err = FsCatalog::discover(&nodes).expect_err("empty package must error");
    assert!(matches!(err, CatalogError::Parse { .. }));
}

/// `package_roots_for` (the build-staging source) returns the deduped
/// package root per referenced node: members of one package collapse
/// to a single root, a bare node yields its own dir, unknown types are
/// skipped. This is what staging copies instead of re-walking nodes/.
#[test]
fn package_roots_for_dedupes_by_package() {
    use std::collections::BTreeSet;
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");
    // A package with two members + a bare node elsewhere.
    let pkg = nodes.join("deep/pkg");
    write_package_toml(&pkg, "pkg");
    write_node(&pkg.join("a"), "AA");
    write_node(&pkg.join("b"), "BB");
    write_node(&nodes.join("solo"), "Solo");

    let cat = FsCatalog::discover(&nodes).unwrap();
    let referenced: BTreeSet<String> =
        ["AA", "BB", "Solo"].iter().map(|s| s.to_string()).collect();
    let roots = cat.package_roots_for(&referenced);
    // AA + BB collapse to the one package root; Solo is its own. Two.
    assert_eq!(roots.len(), 2, "got {roots:?}");
    assert!(roots.iter().any(|r| r.ends_with("deep/pkg")));
    assert!(roots.iter().any(|r| r.ends_with("solo")));

    // An unknown type is skipped, not an error.
    let unknown: BTreeSet<String> = ["Nope".to_string()].into_iter().collect();
    assert!(cat.package_roots_for(&unknown).is_empty());
}

/// Under Lenient, a collision is a warning, not an error: the first
/// entry is kept, and the losing package must NOT list the colliding
/// type (its `node_types` and `entries` stay consistent).
#[test]
fn lenient_collision_warns_and_keeps_first() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");
    write_node(&nodes.join("first"), "Dup");
    write_node(&nodes.join("second"), "Dup");

    let cat = FsCatalog::discover_with_policy(&nodes, DiscoverPolicy::Lenient)
        .expect("lenient never hard-errors on a collision");
    assert!(cat.entry("Dup").is_some(), "the first Dup is kept");
    assert!(!cat.warnings().is_empty(), "collision recorded as a warning");
    // Exactly one package should claim `Dup`; the loser dropped it.
    let claimers = cat
        .packages()
        .filter(|p| p.node_types.iter().any(|t| t == "Dup"))
        .count();
    assert_eq!(claimers, 1, "only the winning package lists Dup");
}

/// Discovery must not follow symlinks anywhere in a node tree, so it
/// agrees with the (no-follow) build-staging copy and source-hash
/// walk. A symlinked package member discovered here but dropped by
/// staging/hash would serve a stale worker image. Covers both the
/// recursive descent and the package member-detection loop.
#[test]
#[cfg(unix)]
fn discovery_does_not_follow_symlinked_members() {
    use std::os::unix::fs::symlink;
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");

    // A real package with one real member.
    let pkg = nodes.join("pkg");
    write_package_toml(&pkg, "pkg");
    write_node(&pkg.join("real"), "Real");

    // A second real node elsewhere, symlinked INTO the package as a
    // member. Following the link would register it as a pkg member.
    let external = nodes.join("external");
    write_node(&external, "Linked");
    symlink(&external, pkg.join("linked")).unwrap();

    let cat = FsCatalog::discover(&nodes).unwrap();
    assert!(cat.entry("Real").is_some(), "real member discovered");
    // `external` itself is a real bare node directly under nodes/, so
    // `Linked` IS discovered via that path. What must NOT happen is the
    // symlinked `pkg/linked` being walked as a pkg member: the pkg must
    // own exactly its one real member.
    let pkg_desc = cat.package_of("Real").expect("pkg");
    assert_eq!(
        pkg_desc.node_types,
        vec!["Real".to_string()],
        "package must not pick up a symlinked member",
    );
}

/// A unit defined by a SYMLINKED marker file must not be discovered:
/// the no-follow staging copy and hash walk would drop the symlinked
/// `metadata.json`, so a discovered-but-unstaged unit would serve a
/// broken/stale worker image. Unit detection uses the same no-follow
/// view as the walks, so the unit simply isn't detected (consistent).
#[test]
#[cfg(unix)]
fn symlinked_marker_does_not_define_a_unit() {
    use std::os::unix::fs::symlink;
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");

    // A shared metadata.json elsewhere, symlinked in as a node's marker.
    let shared_meta = tmp.path().join("shared_metadata.json");
    fs::write(
        &shared_meta,
        r#"{ "type": "Sneaky", "label": "Sneaky", "description": "", "category": "Test", "inputs": [], "outputs": [] }"#,
    )
    .unwrap();
    let node = nodes.join("sneaky");
    fs::create_dir_all(&node).unwrap();
    fs::write(node.join("mod.rs"), "// impl\n").unwrap();
    symlink(&shared_meta, node.join("metadata.json")).unwrap();

    let cat = FsCatalog::discover(&nodes).unwrap();
    assert!(
        cat.entry("Sneaky").is_none(),
        "a node whose metadata.json is a symlink must not be discovered \
         (staging/hash would drop the symlinked marker)",
    );
}
