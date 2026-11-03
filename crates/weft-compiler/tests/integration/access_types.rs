//! A connection carries its service in its type (`Access[fal]`): the
//! ordinary fit check refuses a wire from one service into a node taking
//! another, directly, through a join's type variable, and through an
//! included file's port. Real compiler, real stdlib, plus a few nodes of
//! the test's own that make and take connections.

use weft_catalog::{stdlib_root, FsCatalog};
use weft_compiler::enrich::{enrich_collecting, EnrichPolicy};
use weft_compiler::validate::validate;
use weft_compiler::weft_compiler::IncludeMode;
use weft_compiler::{CompileFs, Diagnostic};

use crate::rig::compile_with_mode;

/// The test's own nodes: `MakeA` / `MakeB` hand out a connection to `a`
/// / `b`, `MakeEither` one typed with both, and `UseA`, `UseAny`,
/// `UseEither` take one to `a`, to any service, and to `a` or `b`.
const NODES: &[(&str, &str)] = &[
    ("MakeA", r#""outputs": [{ "name": "conn", "type": "Access[a]" }]"#),
    ("MakeB", r#""outputs": [{ "name": "conn", "type": "Access[b]" }]"#),
    ("MakeEither", r#""outputs": [{ "name": "conn", "type": "Access[a] | Access[b]" }]"#),
    ("UseA", r#""inputs": [{ "name": "account", "type": "Access[a]", "required": true }]"#),
    ("UseAny", r#""inputs": [{ "name": "account", "type": "Access", "required": true }]"#),
    ("UseEither", r#""inputs": [{ "name": "account", "type": "Access[a] | Access[b]", "required": true }]"#),
];

/// The included file: one port typed `Access[a]`, passed on.
const PASS_A: &str = r#"
Group(x: Access[a]) -> (y: Access[a]) {
  self.y = self.x
}
"#;

/// Compile, enrich and validate `source` against the stdlib and the
/// test's nodes: the enrich failures, and every error code validate gave
/// with its message.
fn check(source: &str) -> (Vec<String>, Vec<Diagnostic>) {
    let nodes = tempfile::tempdir().unwrap();
    for (node_type, ports) in NODES {
        let dir = nodes.path().join(node_type.to_lowercase());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("mod.rs"), "// node impl\n").unwrap();
        std::fs::write(
            dir.join("metadata.json"),
            format!(r#"{{ "type": "{node_type}", "label": "{node_type}", "description": "a test node", {ports} }}"#),
        )
        .unwrap();
    }
    let stdlib = stdlib_root().expect("stdlib root");
    let catalog = FsCatalog::discover_roots(&[stdlib.as_path(), nodes.path()]).expect("catalog");
    for (node_type, _) in NODES {
        assert!(weft_core::node::MetadataCatalog::lookup(&catalog, node_type).is_some(), "{node_type} loads: {:?}", catalog.problems());
    }
    let files = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(files.path().join("lib")).unwrap();
    std::fs::write(files.path().join("lib/pass_a.weft"), PASS_A).unwrap();
    let mut project = compile_with_mode(source, uuid::Uuid::new_v4(), CompileFs::disk(files.path()), IncludeMode::Full, None)
        .unwrap_or_else(|errors| panic!("compile failed: {errors:?}"));
    let enrich_errors = catalog
        .type_registry()
        .scoped(|| enrich_collecting(&mut project, &catalog, EnrichPolicy::Strict))
        .into_iter()
        .map(|e| e.message)
        .collect();
    let diagnostics = validate(&project, &catalog)
        .into_iter()
        .filter(|d| d.severity == weft_compiler::Severity::Error)
        .collect();
    (enrich_errors, diagnostics)
}

/// The `type-mismatch` messages of `source`, after an enrich that failed
/// nothing.
fn mismatches(source: &str) -> Vec<String> {
    let (enrich_errors, diagnostics) = check(source);
    assert!(enrich_errors.is_empty(), "{enrich_errors:?}");
    diagnostics.into_iter().filter(|d| d.code.as_deref() == Some("type-mismatch")).map(|d| d.message).collect()
}

/// A connection to `a` fits a node taking `a`, any service, or `a` or
/// `b`; one to `b` does not fit `a`; one typed `a` or `b` fits only a
/// node taking both, never one taking `a` alone.
#[test]
fn a_connection_fits_only_a_port_naming_its_service() {
    let wire = |make: &str, take: &str| mismatches(&format!("m = {make}\nu = {take}\nu.account = m.conn\n"));
    for (make, take) in [("MakeA", "UseA"), ("MakeA", "UseAny"), ("MakeA", "UseEither"), ("MakeB", "UseEither"), ("MakeEither", "UseEither"), ("MakeEither", "UseAny")] {
        assert!(wire(make, take).is_empty(), "{make} into {take}: {:?}", wire(make, take));
    }
    for (make, take) in [("MakeB", "UseA"), ("MakeEither", "UseA")] {
        let refused = wire(make, take);
        assert!(refused.iter().any(|m| m.contains("Access[a]")), "{make} into {take}: {refused:?}");
    }
}

/// A join's type variable carries the service: two branches of one
/// service meet and reach a node taking it; a branch of another service
/// is refused (the join binds to the first), and so is a join of `a`
/// reaching a node taking `b` alone.
#[test]
fn a_join_carries_the_service_and_refuses_a_mix() {
    let joined = |second: &str, take: &str| {
        mismatches(&format!(
            "one = MakeA\ntwo = {second}\npick = FirstInOrder {{\n  first: one.conn\n  second: two.conn\n}}\nu = {take}\nu.account = pick.value\n"
        ))
    };
    assert!(joined("MakeA", "UseA").is_empty(), "{:?}", joined("MakeA", "UseA"));
    let mixed = joined("MakeB", "UseA");
    assert!(mixed.iter().any(|m| m.contains("pick.second")), "the branch to `b` is refused: {mixed:?}");
    let reaching = mismatches("one = MakeA\npick = FirstInOrder {\n  first: one.conn\n}\nu = UseEither\nu.account = pick.value\n");
    assert!(reaching.is_empty(), "{reaching:?}");
}

/// An included file's port typed `Access[a]` holds what crosses it the
/// same way a node's input does.
#[test]
fn an_included_files_port_holds_the_service() {
    let through = |make: &str| {
        mismatches(&format!("m = {make}\none = @include(\"lib/pass_a.weft\")\none.x = m.conn\nu = UseA\nu.account = one.y\n"))
    };
    assert!(through("MakeA").is_empty(), "{:?}", through("MakeA"));
    assert!(!through("MakeB").is_empty(), "a connection to `b` does not cross a port typed `Access[a]`");
}
