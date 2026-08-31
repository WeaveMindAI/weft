//! Shape of the emitted per-package node-test crate
//! (`codegen::emit_test_crate`) and the worker-side guarantee that
//! tests never compile into a worker binary.

use weft_catalog::FsCatalog;
use weft_compiler::codegen::{emit_test_crate, EmitPaths};

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).expect("mkdir");
    for entry in std::fs::read_dir(src).expect("read dir") {
        let entry = entry.expect("dir entry");
        let to = dst.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).expect("copy file");
        }
    }
}

fn stdlib() -> FsCatalog {
    FsCatalog::discover(&weft_catalog::stdlib_root().expect("stdlib root"))
        .expect("stdlib discovers")
}

/// The emitted test crate: runner main + registry + the one package
/// crate with the `node-tests` feature ENABLED, locked to the
/// workspace's versions, on the pinned toolchain.
#[test]
fn emitted_test_crate_has_the_expected_shape() {
    let catalog = stdlib();
    let weft_root = weft_catalog::weft_repo_root().expect("weft root");
    let dir = tempfile::tempdir().expect("temp dir");
    let test_crate = emit_test_crate(
        dir.path(),
        &catalog,
        "slack",
        &EmitPaths::Local { weft_root: weft_root.clone() },
    )
    .expect("emit");

    assert_eq!(test_crate.binary_name, "slack_tests");
    assert!(test_crate.node_types.iter().any(|t| t == "SlackSendMessage"));

    let root_toml = std::fs::read_to_string(dir.path().join("Cargo.toml")).unwrap();
    assert!(
        root_toml.contains(r#"features = ["node-tests"]"#),
        "the test crate enables the package's node-tests feature:\n{root_toml}"
    );
    // The ENGINE dep specifically must carry the feature (the rig +
    // runner live behind it); a substring hit on the package dep's
    // feature line must not satisfy this.
    let engine_dep = root_toml
        .lines()
        .find(|l| l.trim_start().starts_with("weft-engine"))
        .expect("an engine dep line exists");
    assert!(
        engine_dep.contains("node-tests"),
        "weft-engine's dep entry enables node-tests:\n{root_toml}"
    );

    let pkg_toml = std::fs::read_to_string(dir.path().join("pkg_slack/Cargo.toml")).unwrap();
    assert!(
        pkg_toml.contains("node-tests = []"),
        "the package crate declares the feature:\n{pkg_toml}"
    );

    let main_rs = std::fs::read_to_string(dir.path().join("src/main.rs")).unwrap();
    assert!(main_rs.contains("weft_engine::test_runner::main"), "{main_rs}");
    assert!(main_rs.contains("TypeRegistry::install"), "baked types installed first");

    let registry = std::fs::read_to_string(dir.path().join("src/registry.rs")).unwrap();
    assert!(registry.contains("\"SlackSendMessage\""), "{registry}");

    assert!(dir.path().join("Cargo.lock").is_file(), "workspace lock seeded");
    assert!(dir.path().join("rust-toolchain.toml").is_file(), "toolchain pinned");

    let lib_rs = std::fs::read_to_string(dir.path().join("pkg_slack/src/lib.rs")).unwrap();
    assert!(
        lib_rs.contains("/catalog/slack/send_message/mod.rs"),
        "local mode includes real absolute node paths:\n{lib_rs}"
    );
}

/// Stage a minimal project holding just the slack package, so
/// container-mode emission and the hash have a real project root.
fn staged_slack_project() -> tempfile::TempDir {
    let project_dir = tempfile::tempdir().expect("temp project");
    copy_dir(
        &weft_catalog::stdlib_root().expect("stdlib root").join("slack"),
        &project_dir.path().join("nodes").join("slack"),
    );
    std::fs::write(
        project_dir.path().join("weft.toml"),
        "[package]\nid = \"00000000-0000-0000-0000-000000000001\"\nname = \"probe\"\n",
    )
    .expect("weft.toml");
    project_dir
}

/// Container mode roots every include at the image's nodes mount; the
/// arm that ships in images gets its own pin, not just Local's.
#[test]
fn container_mode_includes_are_mount_rooted() {
    let project_dir = staged_slack_project();
    let catalog = FsCatalog::discover(&project_dir.path().join("nodes")).expect("discover");
    let dir = tempfile::tempdir().expect("temp dir");
    emit_test_crate(
        dir.path(),
        &catalog,
        "slack",
        &EmitPaths::Container { nodes_root: project_dir.path().join("nodes") },
    )
    .expect("emit");
    let lib_rs = std::fs::read_to_string(dir.path().join("pkg_slack/src/lib.rs")).unwrap();
    assert!(
        lib_rs.contains("/weft/project-nodes/slack/send_message/mod.rs"),
        "container includes are mount-rooted:\n{lib_rs}"
    );
}

/// Re-emitting into a dirty directory yields a clean tree: the crate
/// is a pure function of its inputs, never an accumulation.
#[test]
fn re_emission_wipes_stale_files() {
    let catalog = stdlib();
    let weft_root = weft_catalog::weft_repo_root().expect("weft root");
    let dir = tempfile::tempdir().expect("temp dir");
    let stale = dir.path().join("pkg_oldname");
    std::fs::create_dir_all(&stale).expect("stale dir");
    std::fs::write(stale.join("Cargo.toml"), "[package]\nname = \"old\"").expect("stale file");
    emit_test_crate(
        dir.path(),
        &catalog,
        "slack",
        &EmitPaths::Local { weft_root: weft_root.clone() },
    )
    .expect("emit over dirty dir");
    assert!(!stale.exists(), "stale package dirs are wiped on re-emission");
    assert!(dir.path().join("pkg_slack").is_dir());
}

/// The test hash is the image's staleness rule: stable on unchanged
/// inputs, flipped by a package source edit AND by an image-recipe
/// edit (weft.toml's build choices shape the image bytes).
#[test]
fn node_test_hash_tracks_sources_and_recipe() {
    let project_dir = staged_slack_project();
    let weft_root = weft_catalog::weft_repo_root().expect("weft root");
    let project =
        weft_compiler::project::Project::load(project_dir.path()).expect("project loads");
    let package_root = project_dir.path().join("nodes").join("slack");
    // A SIBLING package, for the type-registry flip below.
    copy_dir(
        &weft_catalog::stdlib_root().expect("stdlib root").join("basic"),
        &project_dir.path().join("nodes").join("basic"),
    );
    let catalog = || {
        FsCatalog::discover(&project_dir.path().join("nodes")).expect("project catalog")
    };

    let h1 =
        weft_compiler::hash::compute_node_test_hash(&package_root, &project, &weft_root, &catalog())
            .expect("hash");
    let h2 =
        weft_compiler::hash::compute_node_test_hash(&package_root, &project, &weft_root, &catalog())
            .expect("hash again");
    assert_eq!(h1, h2, "unchanged inputs = a cache hit");

    // A package source edit flips it.
    let probe = package_root.join("api.rs");
    let original = std::fs::read_to_string(&probe).expect("read api.rs");
    std::fs::write(&probe, format!("{original}\n// probe edit\n")).expect("edit");
    let h3 =
        weft_compiler::hash::compute_node_test_hash(&package_root, &project, &weft_root, &catalog())
            .expect("hash after source edit");
    assert_ne!(h1, h3, "a node source edit must flip the test hash");

    // A SIBLING package's type declaration flips it too: the test
    // crate's main.rs bakes the FULL catalog type registry, so a type
    // edit anywhere changes this package's test binary bytes.
    let sibling_meta =
        project_dir.path().join("nodes").join("basic").join("debug").join("metadata.json");
    let meta = std::fs::read_to_string(&sibling_meta).expect("read sibling metadata");
    let mut parsed: serde_json::Value = serde_json::from_str(&meta).expect("metadata parses");
    parsed["types"]["ProbeType"] = serde_json::json!("{ probe: String }");
    std::fs::write(&sibling_meta, serde_json::to_string_pretty(&parsed).unwrap())
        .expect("edit sibling metadata");
    let h3b =
        weft_compiler::hash::compute_node_test_hash(&package_root, &project, &weft_root, &catalog())
            .expect("hash after sibling type edit");
    assert_ne!(h3, h3b, "a sibling package's type declaration must flip the test hash");

    // An image-recipe edit (weft.toml) flips it too: the base image
    // shapes the image the tests run in.
    std::fs::write(
        project_dir.path().join("weft.toml"),
        "[package]\nid = \"00000000-0000-0000-0000-000000000001\"\nname = \"probe\"\n\n\
         [build.worker]\nbase_image = \"debian:trixie-slim\"\n",
    )
    .expect("edit weft.toml");
    let project =
        weft_compiler::project::Project::load(project_dir.path()).expect("project reloads");
    let h4 =
        weft_compiler::hash::compute_node_test_hash(&package_root, &project, &weft_root, &catalog())
            .expect("hash after recipe edit");
    assert_ne!(h3b, h4, "a build-recipe edit must flip the test hash");
}

/// An unknown package is a loud error naming the known set.
#[test]
fn unknown_package_is_refused_with_the_known_set() {
    let catalog = stdlib();
    let weft_root = weft_catalog::weft_repo_root().expect("weft root");
    let dir = tempfile::tempdir().expect("temp dir");
    let err = emit_test_crate(
        dir.path(),
        &catalog,
        "nope",
        &EmitPaths::Local { weft_root },
    )
    .expect_err("unknown package refused")
    .to_string();
    assert!(err.contains("no package named 'nope'") && err.contains("slack"), "{err}");
}

/// The worker binary NEVER contains tests: every generated package
/// crate declares `node-tests` (so mod.rs's cfg gate is a known
/// feature name) but the WORKER root crate never enables it. Pinned
/// against the real worker emission on a minimal project.
#[test]
fn worker_emission_declares_but_never_enables_node_tests() {
    use weft_core::project::{NodeDefinition, Position, ProjectDefinition};
    let node = NodeDefinition {
        id: "send".into(),
        node_type: "SlackSendMessage".into(),
        label: None,
        config: serde_json::Value::Object(Default::default()),
        position: Position { x: 0.0, y: 0.0 },
        inputs: Vec::new(),
        outputs: Vec::new(),
        features: weft_core::NodeFeatures::default(),
        scope: Vec::new(),
        group_boundary: None,
        requires_infra: false,
        images: Vec::new(),
        published_service: None,
        span: None,
        header_span: None,
        config_spans: Default::default(),
        optional_ports: Default::default(),
        port_literals: Default::default(),
        port_literal_spans: Default::default(),
        file_refs: Default::default(),
        include_path: None,
    };
    let project = ProjectDefinition {
        id: uuid::Uuid::nil(),
        nodes: vec![node],
        edges: Vec::new(),
        groups: Vec::new(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    // Worker (container-mode) emission maps includes relative to
    // `<project_root>/nodes`, so stage a minimal project holding just
    // the slack package.
    let project_dir = tempfile::tempdir().expect("temp project");
    copy_dir(
        &weft_catalog::stdlib_root().expect("stdlib root").join("slack"),
        &project_dir.path().join("nodes").join("slack"),
    );
    let catalog = FsCatalog::discover(&project_dir.path().join("nodes"))
        .expect("staged catalog discovers");
    let dir = tempfile::tempdir().expect("temp dir");
    weft_compiler::codegen::emit(&project, project_dir.path(), dir.path(), &catalog, "probe")
        .expect("worker emission");

    let worker_toml = std::fs::read_to_string(dir.path().join("Cargo.toml")).unwrap();
    assert!(
        !worker_toml.contains("node-tests"),
        "the worker root must never enable node-tests:\n{worker_toml}"
    );
    let pkg_toml = std::fs::read_to_string(dir.path().join("pkg_slack/Cargo.toml")).unwrap();
    assert!(
        pkg_toml.contains("node-tests = []"),
        "the package crate declares the feature so the cfg name is known:\n{pkg_toml}"
    );
    assert!(
        !pkg_toml.contains(r#"features = ["node-tests"]"#),
        "and nothing enables it in a worker build"
    );
}
