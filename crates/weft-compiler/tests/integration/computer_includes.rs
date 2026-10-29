//! Layer-3: the computer package's premade include files, as a program
//! reaches them through `__catalog/`, compile against the standard
//! library and pass the full check `weft validate` runs.

use weft_catalog::{stdlib_root, FsCatalog};
use weft_compiler::validate::ValidationMode;
use weft_compiler::{compile_strict, CompileFs};

/// A project holding the stdlib's computer package where a seeded
/// project holds it, `nodes/base_catalog/computer/`: the files the
/// includes read, copied (a link out of `nodes/` is refused).
fn project_with_computer_files() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("temp project");
    let from = stdlib_root().expect("stdlib root").join("computer");
    let to = dir.path().join("nodes/base_catalog/computer");
    std::fs::create_dir_all(&to).expect("mkdir");
    for name in ["perform.weft", "agent.weft", "agent_prompt.md"] {
        std::fs::copy(from.join(name), to.join(name)).unwrap_or_else(|e| panic!("copy {name}: {e}"));
    }
    dir
}

const MAIN: &str = r#"
box = Computer
model = CustomProvider { baseUrl: "http://127.0.0.1:9/v1", model: "m" }

agent = @include("__catalog/base_catalog/computer/agent.weft")
agent.task = "Print hi in the terminal"
agent.computer = box.computer
agent.model = model.provider

one = @include("__catalog/base_catalog/computer/perform.weft")
one.computer = box.computer
one.call = { "id": "c1", "name": "screenshot", "arguments": "" }

answer = Debug { data: agent.answer }
said = Debug { data: one.result }
"#;

#[test]
fn the_agent_and_perform_compile_and_validate_through_catalog_paths() {
    let dir = project_with_computer_files();
    let catalog = FsCatalog::discover(&stdlib_root().expect("stdlib root")).expect("stdlib catalog");
    // The pipeline `weft validate` runs, runtime rules included.
    let (project, diagnostics) =
        compile_strict(MAIN, uuid::Uuid::new_v4(), CompileFs::disk(dir.path()), &catalog, ValidationMode::Runtime, None);
    // Clean: no error, and no warning either (a level grown past what
    // the compiler accepts quietly, a name spelled off the weft way).
    assert!(diagnostics.is_empty(), "{diagnostics:#?}");
    // The agent's prompt is the stdlib file's own `__catalog/` marker,
    // spelled from the project root like any `@file`.
    let prompt_refs: Vec<_> = project.nodes.iter().flat_map(|node| node.file_refs.values()).map(|r| r.path.as_str()).collect();
    assert!(prompt_refs.contains(&"nodes/base_catalog/computer/agent_prompt.md"), "{prompt_refs:?}");
}
