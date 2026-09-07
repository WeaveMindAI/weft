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
            r#"{{ "type": "{node_type}", "label": "{node_type}", "description": "", "inputs": [], "outputs": [] }}"#
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

/// Two nodes claiming the same SERVICE is ambiguous the same way two
/// claiming one type is: everything that stores or publishes a
/// connection finds the service by name alone, so a second claimant
/// would make that a coin toss.
///
/// The names in the message are pinned too. The walk decides who is
/// the original by arriving there first, and the filesystem's own
/// order is not the tree's, so an unsorted walk would blame a
/// different node per machine.
#[test]
fn duplicate_service_is_hard_error() {
    fn write_service_node(dir: &Path, node_type: &str, service: &str) {
        fs::create_dir_all(dir).unwrap();
        fs::write(
            dir.join("metadata.json"),
            format!(
                r#"{{ "type": "{node_type}", "label": "{node_type}", "description": "",
                      "inputs": [{{ "name": "account", "type": "Access",
                                    "widget": {{ "kind": "access" }} }}],
                      "outputs": [],
                      "service": {{ "service": "{service}",
                                    "acquisition": {{ "kind": "static",
                                                      "fields": [{{ "name": "host" }}] }} }} }}"#
            ),
        )
        .unwrap();
        fs::write(dir.join("mod.rs"), "// node impl\n").unwrap();
    }

    for _ in 0..8 {
        let tmp = tempfile::tempdir().unwrap();
        let nodes = tmp.path().join("nodes");
        write_service_node(&nodes.join("bbb"), "Second", "shared");
        write_service_node(&nodes.join("aaa"), "First", "shared");

        let err = FsCatalog::discover(&nodes).expect_err("service collision must error");
        match err {
            CatalogError::ServiceCollision { service, first, second } => {
                assert_eq!(service, "shared");
                assert_eq!(first, "First", "the node the walk reaches first, every time");
                assert_eq!(second, "Second");
            }
            other => panic!("expected ServiceCollision, got {other:?}"),
        }
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

/// Discovery follows symlinks anywhere in a node tree, so a project's
/// `nodes/base_catalog` can be one symlink into a weft checkout's
/// `catalog/` (this repo's `examples/` do exactly that). The staging
/// copy, the pack, and the source-hash walk follow the same way, so
/// what is discovered and what reaches the build context agree.
/// Covers both the recursive descent and the package member loop.
#[test]
#[cfg(unix)]
fn discovery_follows_symlinked_members() {
    use std::os::unix::fs::symlink;
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");

    // A real package with one real member.
    let pkg = nodes.join("pkg");
    write_package_toml(&pkg, "pkg");
    write_node(&pkg.join("real"), "Real");

    // A node living OUTSIDE nodes/, symlinked into the package as a
    // member.
    let external = tmp.path().join("external");
    write_node(&external, "Linked");
    symlink(&external, pkg.join("linked")).unwrap();

    let cat = FsCatalog::discover(&nodes).unwrap();
    assert!(cat.entry("Real").is_some(), "real member discovered");
    assert!(cat.entry("Linked").is_some(), "symlinked member discovered");
    let mut members = cat.package_of("Real").expect("pkg").node_types.clone();
    members.sort();
    assert_eq!(members, vec!["Linked".to_string(), "Real".to_string()]);
}

/// A unit defined by a SYMLINKED marker file is a real unit: the
/// symlink-following staging copy and hash walk carry the target's
/// bytes, so what is discovered is exactly what builds.
#[test]
#[cfg(unix)]
fn symlinked_marker_defines_a_unit() {
    use std::os::unix::fs::symlink;
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");

    // A shared metadata.json elsewhere, symlinked in as a node's marker.
    let shared_meta = tmp.path().join("shared_metadata.json");
    fs::write(
        &shared_meta,
        r#"{ "type": "Sneaky", "label": "Sneaky", "description": "", "inputs": [], "outputs": [] }"#,
    )
    .unwrap();
    let node = nodes.join("sneaky");
    fs::create_dir_all(&node).unwrap();
    fs::write(node.join("mod.rs"), "// impl\n").unwrap();
    symlink(&shared_meta, node.join("metadata.json")).unwrap();

    let cat = FsCatalog::discover(&nodes).unwrap();
    assert!(cat.entry("Sneaky").is_some(), "symlink-marked node discovered");
}

/// A symlink cycle in a node tree fails loudly instead of walking
/// forever.
#[test]
#[cfg(unix)]
fn symlink_cycle_errors_loudly() {
    use std::os::unix::fs::symlink;
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");
    let a = nodes.join("a");
    fs::create_dir_all(&a).unwrap();
    symlink(&nodes, a.join("back")).unwrap();

    let err = FsCatalog::discover(&nodes).expect_err("cycle must error");
    assert!(
        err.to_string().contains("symlink cycle"),
        "error names the cycle: {err}",
    );
}

/// Two symlinks to the SAME shared directory are a DAG, not a cycle:
/// the guard watches the descent chain, so reaching one directory
/// twice by different paths walks fine. (The two copies then collide
/// as duplicate node types, which is the honest error for that shape;
/// here the shared target holds no node so discovery just succeeds.)
#[test]
#[cfg(unix)]
fn two_symlinks_to_one_target_are_not_a_cycle() {
    use std::os::unix::fs::symlink;
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");
    fs::create_dir_all(&nodes).unwrap();
    let shared = tmp.path().join("shared");
    fs::create_dir_all(&shared).unwrap();
    symlink(&shared, nodes.join("one")).unwrap();
    symlink(&shared, nodes.join("two")).unwrap();

    FsCatalog::discover(&nodes).expect("a DAG of symlinks walks clean");
}

/// A `portsFromConfig` metadata value with a single spec whose `kind`
/// is `tag` (so a test can tell WHICH definition site won the merge).
fn specs_json(tag: &str) -> String {
    format!(
        r#"{{ "field": "fields", "specs": [{{ "kind": "{tag}", "label": "{tag}",
              "render": {{ "component": "text" }},
              "addsOutputs": [{{ "nameTemplate": "{{key}}", "portType": "String" }}] }}] }}"#
    )
}

/// A node that declares the `fields` config input its ports come from,
/// with `body` folded in (its own `portsFromConfig`, or nothing when the
/// package root supplies it).
fn write_derived_ports_node(dir: &Path, node_type: &str, body: &str) {
    fs::create_dir_all(dir).unwrap();
    fs::write(
        dir.join("metadata.json"),
        format!(
            r#"{{ "type": "{node_type}", "label": "{node_type}", "description": "",
                  "inputs": [{{ "name": "fields", "type": "List[JsonDict]" }}],
                  "outputs": []{body} }}"#
        ),
    )
    .unwrap();
    fs::write(dir.join("mod.rs"), "// node impl\n").unwrap();
}

/// Package-level metadata defaults: a PARTIAL `metadata.json` at the package
/// root is merged into every member key-by-key, the member's own key winning
/// wholesale. Exercised on the two shared keys that exist today
/// (`portsFromConfig`, `provider`); the mechanism is key-agnostic.
#[test]
fn package_metadata_defaults_merge_key_by_key_member_wins() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = tmp.path().join("nodes");

    // Package with root defaults and two members: one plain (inherits both
    // keys), one that carries its own `portsFromConfig` (overrides that key,
    // still inherits `provider`).
    let pkg = nodes.join("pack");
    write_package_toml(&pkg, "pack");
    fs::write(
        pkg.join("metadata.json"),
        format!(r#"{{ "portsFromConfig": {} }}"#, specs_json("root_spec")),
    )
    .unwrap();
    write_derived_ports_node(&pkg.join("inherits"), "Inherits", "");
    write_derived_ports_node(
        &pkg.join("overrides"),
        "Overrides",
        &format!(r#", "portsFromConfig": {}"#, specs_json("member_spec")),
    );

    // A bare node with its own key beside nothing: no defaults in play.
    write_derived_ports_node(
        &nodes.join("bare"),
        "Bare",
        &format!(r#", "portsFromConfig": {}"#, specs_json("bare_spec")),
    );

    let cat = FsCatalog::discover(&nodes).unwrap();

    let kind = |t: &str| {
        cat.entry(t)
            .unwrap()
            .metadata
            .ports_from_config
            .as_ref()
            .and_then(|p| p.specs.first())
            .map(|s| s.kind.clone())
    };
    assert_eq!(kind("Inherits"), Some("root_spec".into()), "member inherits package defaults");
    assert_eq!(kind("Overrides"), Some("member_spec".into()), "member's own key wins wholesale");
    assert_eq!(kind("Bare"), Some("bare_spec".into()), "bare node reads its own metadata");
}

/// A package-level `metadata.json` that sets an IDENTITY key (`type`,
/// `label`, or `description`, the things that name one node) is a
/// contradiction, never a shared default, and must fail discovery loudly
/// under Strict, never be merged. The refusal lives in
/// `weft_core::node::merge_package_defaults`, so the catalog and the
/// `#[derive(NodeManifest)]` runtime path enforce the identical rule.
#[test]
fn package_metadata_defaults_refuse_identity_keys() {
    for key in ["type", "label", "description"] {
        let tmp = tempfile::tempdir().unwrap();
        let nodes = tmp.path().join("nodes");
        let pkg = nodes.join("pack");
        write_package_toml(&pkg, "pack");
        fs::write(pkg.join("metadata.json"), format!(r#"{{ "{key}": "shared" }}"#)).unwrap();
        write_node(&pkg.join("member"), "Member");

        let err = FsCatalog::discover(&nodes).unwrap_err().to_string();
        assert!(err.contains(key), "the error names the offending key `{key}`: {err}");
    }
}

/// The `types` metadata key: declared at a package root, harvested
/// into the catalog's registry BEFORE metadata parses, so member port
/// types may use the declared names; a project-level declaration with
/// a conflicting body fails loudly under Strict and warns under
/// Lenient (builtin-only fallback).
#[test]
fn types_declarations_resolve_member_port_types() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("nodes");
    let pkg = root.join("chat");
    fs::create_dir_all(pkg.join("ask")).unwrap();
    fs::write(pkg.join("package.toml"), "[package]\nname = \"chat\"\n").unwrap();
    fs::write(
        pkg.join("metadata.json"),
        serde_json::json!({
            "types": {
                "ChatHistory": "List[ChatMessage]",
                "ChatMessage": "{ role: String, content: String }",
            }
        })
        .to_string(),
    )
    .unwrap();
    fs::write(
        pkg.join("ask").join("metadata.json"),
        serde_json::json!({
            "type": "Ask", "label": "Ask", "description": "d",
            "inputs": [{ "name": "history", "type": "ChatHistory" }],
            "outputs": [{ "name": "history", "type": "ChatHistory" }],
        })
        .to_string(),
    )
    .unwrap();
    fs::write(pkg.join("ask").join("mod.rs"), "// impl\n").unwrap();

    let cat = FsCatalog::discover(&root).expect("declared types must resolve member metadata");
    let ask = cat.entry("Ask").unwrap();
    let input = &ask.metadata.inputs[0];
    assert!(
        matches!(&input.input_type, weft_core::weft_type::WeftType::Named { name, .. } if name == "ChatHistory"),
        "port resolved to the declared nominal type, got {}",
        input.input_type
    );
    // The registry is exposed for the compile pipeline.
    assert_eq!(cat.type_registry().nominal_entries().len(), 2);

    // A second declaration of the same name with a DIFFERENT body:
    // Strict fails loudly, Lenient warns and falls back to builtin.
    let clash = root.join("other");
    fs::create_dir_all(&clash).unwrap();
    fs::write(
        clash.join("metadata.json"),
        serde_json::json!({
            "type": "Other", "label": "O", "description": "d",
            "types": { "ChatMessage": "{ role: Number, content: String }" },
        })
        .to_string(),
    )
    .unwrap();
    fs::write(clash.join("mod.rs"), "// impl\n").unwrap();
    let err = FsCatalog::discover(&root).unwrap_err();
    assert!(err.to_string().contains("declared twice"), "{err}");
    let lenient = FsCatalog::discover_with_policy(&root, DiscoverPolicy::Lenient).unwrap();
    assert!(lenient.warnings().iter().any(|w| w.contains("declared twice")), "{:?}", lenient.warnings());
}
