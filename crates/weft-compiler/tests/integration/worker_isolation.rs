//! The worker crate's shape for leaving out a package that does not
//! compile (`codegen::emit`, `worker_isolate`): one optional dependency and
//! feature per package, a registry gated on them, each package's build
//! script on its own package crate, the
//! `weft-isolate` package beside it, and an image plan that says where the
//! built image keeps its record.

use weft_catalog::FsCatalog;
use weft_compiler::build_plan::{plan_build_from, ImageKind, TagPolicy};

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

fn empty_definition() -> weft_core::project::ProjectDefinition {
    weft_core::project::ProjectDefinition {
        id: uuid::Uuid::nil(),
        nodes: Vec::new(),
        edges: Vec::new(),
        groups: Vec::new(),
        given: Default::default(),
        broken: Vec::new(),
        metadata_wires: Vec::new(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        defaults: Default::default(),
    }
}

/// A full-library worker over a project holding two packages: slack
/// (shipping a build script) and format (one node, its own package).
fn emit_two_packages() -> (tempfile::TempDir, tempfile::TempDir) {
    let project = tempfile::tempdir().expect("temp project");
    let stdlib = weft_catalog::stdlib_root().expect("stdlib root");
    copy_dir(&stdlib.join("slack"), &project.path().join("nodes/slack"));
    copy_dir(&stdlib.join("basic/format"), &project.path().join("nodes/format"));
    std::fs::write(project.path().join("nodes/slack/build.rs"), "pub fn main() {}\n").expect("build.rs");
    let catalog = FsCatalog::discover(&project.path().join("nodes")).expect("discover");
    let out = tempfile::tempdir().expect("temp crate");
    weft_compiler::codegen::emit(
        &empty_definition(),
        out.path(),
        &catalog,
        "probe",
        weft_core::builds::NodeSet::Full,
        &weft_compiler::codegen::EmitPaths::Container { project_root: project.path().to_path_buf() },
    )
    .expect("worker emission");
    (project, out)
}

fn read(root: &std::path::Path, rel: &str) -> String {
    std::fs::read_to_string(root.join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
}

/// Each package is an optional dependency behind a feature named like it,
/// and the default feature set is every package, so a plain `cargo build`
/// builds the whole worker.
#[test]
fn every_package_is_an_optional_dependency_on_by_default() {
    let (_project, out) = emit_two_packages();
    let toml = read(out.path(), "Cargo.toml");
    let manifest: toml::Value = toml::from_str(&toml).expect("the manifest parses");
    for ident in ["pkg_format", "pkg_slack"] {
        let dep = &manifest["dependencies"][ident];
        assert_eq!(dep["optional"].as_bool(), Some(true), "{toml}");
        assert!(dep["path"].as_str().unwrap().starts_with(&format!("./{ident}-")), "{toml}");
        assert_eq!(manifest["features"][ident].as_array().unwrap(), &vec![toml::Value::String(format!("dep:{ident}"))]);
    }
    let default: Vec<&str> = manifest["features"]["default"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
    assert_eq!(default, vec!["pkg_format", "pkg_slack"]);
}

/// The registry compiles a package's arms and `all()` entries only with its
/// feature, and answers `left_out` from the record the build embedded;
/// the worker main hands that answer on.
#[test]
fn the_registry_gates_each_package_and_reads_the_embedded_record() {
    let (_project, out) = emit_two_packages();
    let registry = read(out.path(), "src/registry.rs");
    assert!(
        registry.contains("            #[cfg(feature = \"pkg_slack\")]\n            \"SlackSendMessage\" => Some(&pkg_slack::"),
        "{registry}"
    );
    assert!(registry.contains("        #[cfg(feature = \"pkg_format\")]\n        all.extend([\"Format\"]);"), "{registry}");
    assert!(!registry.contains("vec!["), "no ungated list of types: {registry}");
    assert!(registry.contains("fn left_out(&self) -> &'static weft_core::builds::LeftOut"), "{registry}");
    assert!(registry.contains("LeftOut::parse(include_bytes!(\"../left-out.json\"))"), "{registry}");
    assert!(registry.contains("panic!"), "a record that does not read stops the worker: {registry}");
    let main = read(out.path(), "src/main.rs");
    assert!(main.contains("registry::PROJECT_CATALOG.left_out()"), "{main}");
}

/// A package's build script is its own package crate's, so one that
/// fails (or whose build-dependency fails) leaves only that package
/// unbuilt; the worker crate has none.
#[test]
fn a_package_build_script_belongs_to_its_package_crate() {
    let (project, out) = emit_two_packages();
    assert!(!out.path().join("build.rs").exists(), "the worker crate has no build script");
    let worker: toml::Value = toml::from_str(&read(out.path(), "Cargo.toml")).expect("the manifest parses");
    assert!(worker["package"].get("build").is_none());
    assert!(worker.get("build-dependencies").is_none());
    let crate_dir = |ident: &str| {
        std::fs::read_dir(out.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .find(|name| name.starts_with(&format!("{ident}-")))
            .unwrap_or_else(|| panic!("no {ident} crate"))
    };
    let slack: toml::Value = toml::from_str(&read(&out.path().join(crate_dir("pkg_slack")), "Cargo.toml")).expect("slack's manifest parses");
    let script = slack["package"]["build"].as_str().expect("slack's crate builds its script");
    let staged = project.path().join("nodes/slack/build.rs");
    let in_container = format!("{}/{}", weft_compiler::worker_image::NODES_MOUNT, "nodes/slack/build.rs");
    assert!(script == in_container || std::path::Path::new(script) == staged, "{script}");
    let format: toml::Value = toml::from_str(&read(&out.path().join(crate_dir("pkg_format")), "Cargo.toml")).expect("format's manifest parses");
    assert!(format["package"].get("build").is_none(), "no build.rs, no build script");
}

/// Beside the crate: the candidates (one per package crate, naming a
/// directory that exists), the empty record dated like the package
/// sources, and the `weft-isolate` package on serde alone, in its own
/// workspace, its source the helper's.
#[test]
fn the_isolation_files_sit_beside_the_crate() {
    let (_project, out) = emit_two_packages();
    let candidates: serde_json::Value = serde_json::from_str(&read(out.path(), "isolate.json")).expect("candidates parse");
    let packages = candidates["packages"].as_array().unwrap();
    assert_eq!(packages.len(), 2);
    let slack = packages.iter().find(|p| p["package"] == "slack").expect("slack is a candidate");
    assert_eq!(slack["feature"], "pkg_slack");
    assert!(out.path().join(slack["crateDir"].as_str().unwrap()).join("Cargo.toml").is_file());
    assert!(slack["nodeTypes"].as_array().unwrap().iter().any(|t| t == "SlackSendMessage"));

    assert_eq!(read(out.path(), "left-out.json"), r#"{"packages":[]}"#);
    // Dated by its content, in 2020 to 2024, like every staged package file:
    // never the wall clock, which would recompile the worker crate each build.
    let dated = std::fs::metadata(out.path().join("left-out.json")).unwrap().modified().unwrap();
    let since_epoch = dated.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    assert!((1_577_836_800..1_577_836_800 + 5 * 365 * 24 * 3600).contains(&since_epoch), "{dated:?}");

    let helper = out.path().join(weft_compiler::codegen::isolate_dir_name());
    let manifest: toml::Value = toml::from_str(&read(&helper, "Cargo.toml")).expect("helper manifest parses");
    assert!(manifest.get("workspace").is_some(), "its own workspace");
    assert_eq!(
        manifest["package"]["name"].as_str(),
        Some(weft_compiler::codegen::isolate_dir_name().as_str()),
        "named by its content, so a kept compile cache never runs an older helper"
    );
    assert_eq!(manifest["bin"][0]["name"].as_str(), Some("weft-isolate"));
    let deps: Vec<&String> = manifest["dependencies"].as_table().unwrap().keys().collect();
    assert_eq!(deps, vec!["serde", "serde_json"]);
    assert_eq!(read(&helper, "src/main.rs"), include_str!("../../src/worker_isolate.rs"));
}

struct BareTags;

impl TagPolicy for BareTags {
    fn worker_ref(&self, binary_hash: &str) -> String {
        format!("weft-worker:{binary_hash}")
    }
    fn infra_ref(&self, image_name: &str, content_hash: &str) -> String {
        format!("{image_name}:{content_hash}")
    }
}

fn image() -> weft_core::builds::WorkerPackaging {
    weft_core::builds::WorkerPackaging::Image { builder: "weft-builder-base:test".into() }
}

fn plan(project: &weft_compiler::project::Project, stock: &weft_compiler::build::StockProject, packaging: &weft_core::builds::WorkerPackaging) -> weft_compiler::build_plan::BuildPlan {
    plan_build_from(
        project,
        &stock.definition,
        &stock.catalog,
        weft_compiler::CompileFs::disk(&stock.project.root),
        packaging,
        &BareTags,
        weft_core::builds::NodeSet::Full,
        &weft_compiler::hash::DISK,
    )
    .expect("plan")
}

fn worker_of(plan: weft_compiler::build_plan::BuildPlan) -> weft_compiler::build_plan::PlannedImage {
    assert!(plan
        .images
        .iter()
        .filter(|i| i.kind == ImageKind::Infra)
        .all(|i| i.recipe == weft_core::builds::BuildRecipe::Dockerfile { result_file: None }));
    plan.images.into_iter().find(|i| i.kind == ImageKind::Worker).expect("a worker image")
}

/// A worker built from a built-in template keeps its record where the
/// install reads it, and the plan says so; a custom template writes none.
#[test]
fn the_plan_names_the_record_only_when_the_dockerfile_writes_one() {
    let stock = weft_compiler::build::StockProject::materialize().expect("stock project");
    let built_in = worker_of(plan(&stock.project, &stock, &image()));
    assert_eq!(
        built_in.recipe,
        weft_core::builds::BuildRecipe::Dockerfile { result_file: Some(weft_core::builds::LEFT_OUT_RECORD_PATH.into()) }
    );
    let dockerfile = std::fs::read_to_string(built_in.context_dir.join("Dockerfile")).unwrap();
    assert!(
        dockerfile.contains(&format!("COPY --from=builder /work/left-out.json {}", weft_core::builds::LEFT_OUT_RECORD_PATH)),
        "{dockerfile}"
    );
    assert!(dockerfile.contains("FROM weft-builder-base:test AS builder"), "{dockerfile}");

    std::fs::write(stock.project.root.join("Dockerfile.tpl"), "FROM {{base_image}}\n").unwrap();
    let mut custom = stock.project.clone();
    custom.manifest.build.worker.dockerfile_template = Some("Dockerfile.tpl".into());
    assert_eq!(worker_of(plan(&custom, &stock, &image())).recipe, weft_core::builds::BuildRecipe::Dockerfile { result_file: None });
}

/// A worker compiled on the machine is a host build whose shared crates go
/// where the packaging says, and its name ignores what only an image is
/// built with (`[build.worker]`), which still names an image apart.
#[test]
fn a_host_worker_is_staged_in_the_packagings_folder_and_ignores_the_image_recipe() {
    let stock = weft_compiler::build::StockProject::materialize().expect("stock project");
    let shared = tempfile::tempdir().unwrap();
    let host = weft_core::builds::WorkerPackaging::Host { shared_crates: shared.path().to_path_buf() };
    let plain = plan(&stock.project, &stock, &host);
    assert_eq!(plain.binary_hash, weft_compiler::build::stock_binary_hash(&host).unwrap(), "an untouched project is the stock worker");
    let worker = worker_of(plain.clone());
    assert_eq!(worker.recipe, weft_core::builds::BuildRecipe::Host);
    assert!(worker.context_dir.join(weft_core::builds::HOST_RECIPE_FILE).is_file());
    assert!(std::fs::read_dir(shared.path()).unwrap().any(|e| e.unwrap().file_name().to_string_lossy().starts_with("weft-")), "the workspace slice is shared there");

    let mut rebased = stock.project.clone();
    rebased.manifest.build.worker.base_image = Some("debian:trixie-slim".into());
    assert_eq!(plan(&rebased, &stock, &host).binary_hash, plain.binary_hash, "a host build reads no [build.worker]");
    assert_ne!(plan(&rebased, &stock, &image()).binary_hash, plan(&stock.project, &stock, &image()).binary_hash, "an image does");
    assert_ne!(plain.binary_hash, plan(&stock.project, &stock, &image()).binary_hash, "an image and a program never share a name");
}
