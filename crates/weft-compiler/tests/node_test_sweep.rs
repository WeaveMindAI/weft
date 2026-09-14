//! The node-self-test SWEEP: runs every stdlib node's basic + fake
//! tests as part of the ordinary workspace test suite. Node tests are
//! written once, in each node's `tests.rs`; this test builds the
//! per-package test binary for every stdlib package that declares any
//! (through the same emit + cargo-build helper `weft test-node` uses)
//! and runs its `run-all`. Also the end-to-end proof of the whole
//! emit -> build -> list -> run mechanism on real catalog packages.
//!
//! Live tests never run here (they need credentials and can spend
//! money; `weft test-node --live` is their door).

use std::path::Path;
use std::process::Command;

use weft_catalog::FsCatalog;

fn stdlib() -> FsCatalog {
    FsCatalog::discover(&weft_catalog::stdlib_root().expect("stdlib root"))
        .expect("stdlib discovers")
}

fn run_package(binary: &Path, package: &str) {

    let out = Command::new(binary)
        .arg("run-all")
        .output()
        .unwrap_or_else(|e| panic!("run {}: {e}", binary.display()));
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    // Exit code 1 is "a test failed" WITH a report; anything else
    // without parseable JSON is the binary crashing, and the report
    // parse below surfaces it with both streams attached.
    let report: serde_json::Value =
        serde_json::from_str(weft_core::node_test::report_line(&stdout).unwrap_or(""))
            .unwrap_or_else(|e| {
                panic!(
                    "parse '{package}' run-all report (exit {:?}): {e}\nstdout:\n{stdout}\nstderr:\n{stderr}",
                    out.status.code(),
                )
            });
    assert!(
        report["passed"].as_bool() == Some(true),
        "package '{package}' node tests failed:\n{report:#}"
    );
    let ran = report["tests"].as_array().map(Vec::len).unwrap_or(0);
    assert!(ran > 0, "package '{package}' has a tests.rs but ran zero tests");
    eprintln!("package '{package}': {ran} node tests passed");
}

#[test]
fn every_stdlib_node_test_passes() {
    let catalog = stdlib();
    // Packages whose nodes declare any `tests.rs`: the catalog's own
    // predicate, so this sweep and `weft test-node` select identically.
    let mut packages: Vec<String> = catalog
        .packages()
        .filter(|pkg| catalog.package_declares_tests(pkg))
        .map(|pkg| pkg.name.clone())
        .collect();
    packages.sort();
    assert!(
        !packages.is_empty(),
        "no stdlib package declares node tests; the sweep should never be vacuous"
    );

    // The whole build (emitted crates AND the cargo cache) lives in
    // cargo's per-crate integration-test scratch dir: outside the
    // workspace artifact dir (so nothing pollutes target/debug), but
    // persistent across runs, so the engine compiles once and every
    // later sweep reuses the cache.
    let work = Path::new(env!("CARGO_TARGET_TMPDIR")).join("node-test-sweep");
    let dirs = weft_compiler::build::TestBuildDirs::under(&work);
    // Every package emitted before any builds, so cargo unifies
    // features across them: the same shape `weft test-node` uses.
    let workspace = weft_compiler::build::prepare_test_workspace(&catalog, &packages, &dirs)
        .unwrap_or_else(|e| panic!("prepare the test workspace: {e}"));
    let binaries = weft_compiler::build::build_node_test_binaries(&workspace)
        .unwrap_or_else(|e| panic!("build the node-test binaries: {e}"));
    for package in &packages {
        let binary = binaries
            .get(package)
            .unwrap_or_else(|| panic!("no binary built for '{package}'"));
        run_package(binary, package);
        // Same as `weft test-node`: the binary is an output, not a
        // cache, and keeping 29 of them left 3.2GB in every dev's
        // target/tmp for nothing. Relinking costs no measurable time.
        weft_compiler::build::drop_built_binary(binary);
    }
}
