//! Layer-3: a node's rule asking which install the program is built for
//! (`local_install`), through the real compiler. Answered when the
//! compile is for an install (the install's version build), which writes
//! the install on the program; a rule that fires there breaks its node,
//! and a compile for no install (the CLI's, `weft validate`, the editor)
//! leaves it unanswered. The rules left to the run are folded with the
//! install, so one the install settles never reaches the run.
use weft_compiler::CompileFs;
use weft_core::install::InstallKind;

const FOLDER: &str = "'{id}' reads `folder` on the machine weft runs on, which a cloud install does not have";
const REFUSAL: &str = "'{id}' runs on the machine weft runs on, which a cloud install does not have; wire a Computer into `computer`";

/// A catalog of two nodes: `Agent`, whose optional `computer` must be
/// wired on a cloud install, and `Machine`, which gives one.
fn catalog() -> (tempfile::TempDir, weft_catalog::FsCatalog) {
    let dir = tempfile::tempdir().unwrap();
    let node = |name: &str, metadata: String| {
        let at = dir.path().join(name);
        std::fs::create_dir_all(&at).unwrap();
        // The description alone leaves a node out of the catalog.
        std::fs::write(at.join("mod.rs"), "// node impl\n").unwrap();
        std::fs::write(at.join("metadata.json"), metadata).unwrap();
    };
    node(
        "agent",
        serde_json::json!({
            "type": "Agent", "label": "Agent", "description": "drives a coding agent",
            "inputs": [
                { "name": "computer", "type": "String", "required": false },
                { "name": "folder", "type": "String", "required": false }
            ],
            "outputs": [{ "name": "answer", "type": "String" }],
            "validate": [{
                "when": { "kind": "all", "of": [
                    { "kind": "not", "of": { "kind": "local_install" } },
                    { "kind": "not", "of": { "kind": "input_wired", "port": "computer" } }
                ] },
                "then": { "message": REFUSAL, "port": "computer" }
            }, {
                "when": { "kind": "all", "of": [
                    { "kind": "not", "of": { "kind": "local_install" } },
                    { "kind": "value", "field": "folder" }
                ] },
                "then": { "message": FOLDER, "port": "folder" }
            }]
        })
        .to_string(),
    );
    node(
        "machine",
        serde_json::json!({
            "type": "Machine", "label": "Machine", "description": "a computer",
            "outputs": [{ "name": "computer", "type": "String" }]
        })
        .to_string(),
    );
    let catalog = weft_catalog::FsCatalog::discover(dir.path()).expect("fixture catalog");
    (dir, catalog)
}

/// `main` compiled for `install`.
fn compiled(main: &str, install: Option<InstallKind>) -> weft_core::ProjectDefinition {
    let (_nodes, catalog) = catalog();
    let project = tempfile::tempdir().unwrap();
    let program = weft_compiler::compile_program_with_contents(main, uuid::Uuid::nil(), CompileFs::disk(project.path()), &catalog, install)
        .expect("compiles")
        .definition;
    assert_eq!(program.install, install, "the program says which install it was compiled for");
    program
}

/// What does not compile in `main` built for `install`.
fn broken(main: &str, install: Option<InstallKind>) -> Vec<String> {
    compiled(main, install).broken.iter().map(|part| part.why.clone()).collect()
}

#[test]
fn a_cloud_install_refuses_an_agent_with_no_computer() {
    let alone = "agent = Agent\n";
    let refused = broken(alone, Some(InstallKind::Cloud));
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert!(
        refused[0].ends_with(
            "'agent' runs on the machine weft runs on, which a cloud install does not have; wire a Computer into `computer`"
        ),
        "{refused:?}"
    );
    assert!(broken(alone, Some(InstallKind::Local)).is_empty(), "a local install has the machine");
    assert!(broken(alone, None).is_empty(), "with no install the rule has no answer");
    let wired = "box = Machine\nagent = Agent\nagent.computer = box.computer\n";
    assert!(broken(wired, Some(InstallKind::Cloud)).is_empty(), "a wired computer is all a cloud needs");
}

/// The build's own gate asks with the install the program was compiled
/// for, read off the compiled program, never handed in again: the
/// program the cloud compiled carries the refusal as a broken part, so
/// the gate finds nothing new, and with that part taken out the gate
/// still asks as the cloud and finds it. Compiled for no install, the
/// rule has no answer at the gate either.
#[test]
fn the_build_gate_asks_for_the_install_it_was_compiled_for() {
    let (_nodes, catalog) = catalog();
    let project = tempfile::tempdir().unwrap();
    let fs = CompileFs::disk(project.path());
    let compile = |install| weft_compiler::compile_program_with_contents("agent = Agent\n", uuid::Uuid::nil(), fs, &catalog, install).expect("compiles");
    let cloud = compile(Some(InstallKind::Cloud));
    assert_eq!(cloud.definition.install, Some(InstallKind::Cloud));
    assert!(weft_compiler::unsettled(&cloud, &catalog, fs).is_empty());
    let mut unrecorded = cloud.clone();
    unrecorded.definition.broken.clear();
    let left = weft_compiler::unsettled(&unrecorded, &catalog, fs);
    assert_eq!(left.len(), 1, "a refusal nobody recorded stops the build: {left:?}");
    let nowhere = compile(None);
    assert!(nowhere.definition.broken.is_empty());
    assert!(weft_compiler::unsettled(&nowhere, &catalog, fs).is_empty());
}

/// A rule pairing the install with a value a wire brings (`folder`) is
/// folded with the install the program is compiled for: on a cloud
/// install the folder is left to the run, which refuses the one that
/// arrives; on a local one the install settles the rule and nothing is
/// recorded, so the two programs hash apart.
#[test]
fn a_rule_mixing_the_install_and_a_wired_value_is_asked_when_the_node_fires() {
    let main = "box = Machine\nagent = Agent\nagent.computer = box.computer\nagent.folder = box.computer\n";
    let (local, cloud) = (compiled(main, Some(InstallKind::Local)), compiled(main, Some(InstallKind::Cloud)));
    assert!(local.broken.is_empty() && cloud.broken.is_empty(), "nothing is known to be wrong before the folder arrives");
    let agent = |program: &weft_core::ProjectDefinition| program.nodes.iter().find(|n| n.id == "agent").expect("agent").clone();
    let left = serde_json::to_value(&agent(&cloud).recheck).unwrap();
    assert_eq!(left[0]["when"], serde_json::json!({ "kind": "value", "field": "folder" }), "{left}");
    assert!(agent(&local).recheck.is_empty(), "settled on a local install: {:?}", agent(&local).recheck);
    let hash = |program: &weft_core::ProjectDefinition| weft_core::project::hash::compute_definition_hash(program).unwrap();
    assert_ne!(hash(&local), hash(&cloud), "what each records for the run tells them apart");
    assert_eq!(hash(&cloud), hash(&compiled(main, Some(InstallKind::Cloud))), "the same kind, the same program");
    let arrived = serde_json::json!({ "folder": "/data" }).as_object().cloned().unwrap();
    match weft_core::rules::recheck(&agent(&cloud), &cloud, &arrived) {
        Err(weft_core::error::WeftError::Input(m)) => {
            assert_eq!(m, "'agent' reads `folder` on the machine weft runs on, which a cloud install does not have")
        }
        other => panic!("a cloud install refuses the folder that arrived, got {other:?}"),
    }
    assert!(weft_core::rules::recheck(&agent(&cloud), &cloud, &serde_json::Map::new()).is_ok(), "no folder, nothing to refuse");
}
