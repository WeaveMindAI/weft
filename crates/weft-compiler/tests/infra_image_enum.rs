//! A build must enumerate a `requires_infra` node's declared images. This
//! replicates the compile (seed base catalog -> load + enrich ->
//! `image_set::infra_images`) on the `infra_min` fixture, and asserts
//! the mini_service image is found. An infra project that produces ZERO infra
//! images never becomes runnable, so this pins the enumeration at L3.

use std::path::Path;

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}

#[test]
fn cloud_compile_enumerates_the_infra_nodes_image() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../weft-e2e/fixtures/infra_min");
    assert!(fixture.is_dir(), "fixture missing at {}", fixture.display());
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&fixture, tmp.path());

    // Precondition, checked BEFORE any assertion: the stdlib catalog must be
    // present on disk (a stripped checkout genuinely can't run this test). Only
    // catalog ABSENCE is a skip; once it is present, a seed failure is a real
    // failure of the machinery under test and must fail the test, never turn it
    // green.
    let stdlib = weft_catalog::stdlib_root();
    if !stdlib.is_dir() {
        eprintln!("skipping: stdlib catalog not present at {}", stdlib.display());
        return;
    }
    weft_compiler::project::seed_base_catalog(tmp.path()).expect("seed base catalog");

    let project = weft_compiler::project::Project::load(tmp.path()).expect("load project");
    let (definition, catalog) =
        weft_compiler::hash::load_enriched_project(&project).expect("compile + enrich");

    let svc = definition
        .nodes
        .iter()
        .find(|n| n.node_type == "MiniService")
        .expect("MiniService node present in the enriched definition");
    assert!(svc.requires_infra, "MiniService must be requires_infra");

    let images = weft_compiler::image_set::infra_images(&definition, &catalog)
        .expect("enumerate infra images");
    assert_eq!(images.len(), 1, "expected exactly one infra image, got {images:?}");
    assert_eq!(images[0].image_name, "mini_service");
    assert_eq!(images[0].node_id, svc.id);
    assert!(images[0].source_dir.join("Dockerfile").is_file());
}
