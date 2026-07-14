//! SECURITY: a node's provider declaration lives in metadata inside its
//! package root (the node's own `metadata.json`, or the package root's
//! partial defaults file), and the package root is hashed WHOLESALE by
//! `compute_node_package_hash` (the fingerprint the build records per node
//! and a deployment's key policy compares against its reviewed set). Editing
//! the declaration MUST flip the fingerprint: that is what makes a tampered
//! declaration (say, re-aiming a provider's base URL) read as a different,
//! unreviewed node instead of the reviewed one. If this test fails, the
//! declaration has escaped the fingerprint and the whole trust chain around
//! it is broken.

use std::path::Path;

const NODE_META: &str = r#"{ "type": "T", "label": "t", "description": "", "category": "AI" }"#;

fn provider_defaults(url: &str) -> String {
    format!(r#"{{ "provider": {{ "name": "openrouter", "base_url": "{url}" }} }}"#)
}

/// A one-member package: `package.toml` + optional package-level metadata
/// defaults + the member node. The shape the openrouter catalog package uses.
fn write_package(root: &Path, name: &str, defaults: Option<&str>) -> std::path::PathBuf {
    let dir = root.join(name);
    let member = dir.join("node");
    std::fs::create_dir_all(&member).unwrap();
    std::fs::write(dir.join("package.toml"), "[package]\nname = \"p\"\n").unwrap();
    if let Some(json) = defaults {
        std::fs::write(dir.join("metadata.json"), json).unwrap();
    }
    std::fs::write(member.join("mod.rs"), "// node body\n").unwrap();
    std::fs::write(member.join("metadata.json"), NODE_META).unwrap();
    dir
}

#[test]
fn editing_the_provider_declaration_flips_the_node_package_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    let undeclared = write_package(base, "undeclared", None);
    let declared =
        write_package(base, "declared", Some(&provider_defaults("https://openrouter.ai/api/v1")));
    let reaimed =
        write_package(base, "reaimed", Some(&provider_defaults("https://evil.example")));

    let hash = |root: &Path| {
        // The package dirs differ by NAME, which is folded into the digest via
        // the base-relative label; hash each against ITS OWN dir as the base so
        // only the CONTENT differs between the three.
        weft_compiler::hash::compute_node_package_hash(root, &[root]).unwrap()
    };
    let h_undeclared = hash(&undeclared);
    let h_declared = hash(&declared);
    let h_reaimed = hash(&reaimed);

    assert_ne!(h_undeclared, h_declared, "adding a declaration must flip the fingerprint");
    assert_ne!(
        h_declared, h_reaimed,
        "re-aiming a declaration's base_url must flip the fingerprint"
    );

    // The member's OWN metadata overriding the declaration flips it too: the
    // merge happens at load, but both definition sites are inside the hash.
    let member_override = write_package(base, "member_override", Some(&provider_defaults("https://openrouter.ai/api/v1")));
    std::fs::write(
        member_override.join("node").join("metadata.json"),
        r#"{ "type": "T", "label": "t", "description": "", "category": "AI",
             "provider": { "name": "openrouter", "base_url": "https://elsewhere.example" } }"#,
    )
    .unwrap();
    assert_ne!(hash(&declared), hash(&member_override), "a member-level override flips it too");

    // Determinism counterpoint: identical content hashes identically, so an
    // unmodified reviewed node keeps its fingerprint across machines/builds.
    let declared_again =
        write_package(base, "declared_again", Some(&provider_defaults("https://openrouter.ai/api/v1")));
    assert_eq!(hash(&declared), hash(&declared_again));
}
