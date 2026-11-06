//! What a program's nodes need from the machine its workers run on
//! (`weft_core::needs`), asked before the program starts: an activate
//! turning triggers on, a resync putting them back, a run started by hand.
//! A program that uses a node whose need the machine does not meet is
//! refused, naming each node and the platform's remedy; a node it does not
//! use is never asked about, and neither is a need of its package's
//! compile (`NeedStage::Build`), which a registered build already met. The
//! need's own node, finding what is missing anyway (it was uninstalled
//! since), fails naming the same remedy (`weft_core::needs::missing_at_run`).

use weft_core::needs::{NeedStage, NodeNeeds};
use weft_platform_traits::MachineNeeds;

use crate::api::project::Unrunnable;
use crate::state::DispatcherState;

/// Refuse `program` (project `id`'s running build, or the cut of it a run
/// runs) when the machine lacks what its nodes need, from what each node
/// type of the registered worker needs.
pub(crate) async fn refuse_unmet(
    state: &DispatcherState,
    id: uuid::Uuid,
    program: &weft_core::ProjectDefinition,
) -> Result<(), Unrunnable> {
    let running = state
        .projects
        .running_program(id)
        .await
        .map_err(|e| Unrunnable::Unchecked(crate::api::unavailable_or_internal("read the running build", e)))?;
    let Some(running) = running else { return Ok(()) };
    unmet_refusal(&*state.needs, program, &running.node_needs).await
}

/// [`refuse_unmet`]'s decision, given what each node type needs: only the
/// run-stage needs of the node types `program` uses are asked of `machine`.
async fn unmet_refusal(
    machine: &dyn MachineNeeds,
    program: &weft_core::ProjectDefinition,
    node_needs: &NodeNeeds,
) -> Result<(), Unrunnable> {
    let needs: Vec<_> = weft_core::needs::program_needs(program, node_needs)
        .into_iter()
        .filter(|row| row.need.stage == NeedStage::Run)
        .collect();
    if needs.is_empty() {
        return Ok(());
    }
    let missing = machine.missing(&needs).await.map_err(|e| {
        Unrunnable::Unchecked(crate::api::unavailable_or_internal("check what the program's nodes need from the machine", e))
    })?;
    if missing.is_empty() {
        return Ok(());
    }
    Err(Unrunnable::Lacks(weft_core::needs::refusal(&missing, machine.needs_from())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use weft_core::needs::Need;
    use weft_platform_traits::FakeMachineNeeds;

    fn program(nodes: &[(&str, &str)]) -> weft_core::ProjectDefinition {
        let nodes: Vec<_> = nodes
            .iter()
            .map(|(id, node_type)| {
                serde_json::json!({ "id": id, "nodeType": node_type, "inputs": [], "outputs": [], "position": { "x": 0, "y": 0 } })
            })
            .collect();
        serde_json::from_value(serde_json::json!({ "id": "00000000-0000-0000-0000-000000000001", "nodes": nodes, "edges": [] })).unwrap()
    }

    fn need(name: &str, stage: NeedStage) -> Need {
        Need { name: name.into(), stage, check: vec![name.into()], install: Default::default(), how: None, recipe: None }
    }

    fn refused(result: Result<(), Unrunnable>) -> Option<(StatusCode, String)> {
        result.err().map(<(StatusCode, String)>::from)
    }

    #[tokio::test]
    async fn a_program_whose_node_needs_what_the_machine_lacks_is_refused_naming_the_remedy() {
        let machine = FakeMachineNeeds::new();
        machine.lacks("python3");
        let node_needs: NodeNeeds = [("ExecPython".to_string(), vec![need("python3", NeedStage::Run)])].into();
        let (status, message) =
            refused(unmet_refusal(&machine, &program(&[("greet", "ExecPython")]), &node_needs).await).expect("refused");
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert!(message.contains("ExecPython (node 'greet') needs python3"), "{message}");
        assert!(message.contains("weft deps-install ExecPython"), "{message}");
    }

    #[tokio::test]
    async fn a_node_type_the_program_does_not_use_and_a_build_need_never_refuse() {
        let machine = FakeMachineNeeds::new();
        machine.lacks("ffmpeg");
        machine.lacks("libvips");
        let node_needs: NodeNeeds = [
            ("Studio".to_string(), vec![need("ffmpeg", NeedStage::Run)]),
            ("Thumbnail".to_string(), vec![need("libvips", NeedStage::Build)]),
        ]
        .into();
        let used = program(&[("greet", "Text"), ("thumb", "Thumbnail")]);
        assert!(refused(unmet_refusal(&machine, &used, &node_needs).await).is_none(), "Studio is not used, libvips is a build need");
    }
}
