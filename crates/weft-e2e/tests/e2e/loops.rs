//! Loop: part of a loop's body run on its own, through the CLI, the
//! install and a real worker.
#![cfg(feature = "e2e")]

use crate::common;

use serde_json::json;
use weft_e2e::{ensure, project::Project};

/// Part of a loop's body runs on its own: from a group in the body to the
/// end of a stretch after it, with what the loop would hand the group
/// handed in. Only that part runs, once, inside the loop's iteration 0,
/// and nothing of the loop or of the program around it does. A run that
/// starts in the body and ends past the loop holds the whole loop, which
/// it cannot start inside: the CLI refuses it before anything is built
/// (the install's own refusal says "this build").
#[tokio::test]
async fn part_of_a_loop_body_runs_on_its_own_inside_iteration_zero() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("loop_map", disp).await?;
    project.set_main(
        "src = ExecPython() -> (items: List[String]) {\n  code: \"return {'items': ['a', 'b']}\"\n}\n\
         clips = Loop(items: List[String]) -> (results: List[String | Null]) {\n  over: [\"items\"]\n\
           edit = Group(clip: String, at: Number) -> (cut: String) {\n\
             trim = ExecPython(clip: String, at: Number) -> (out: String) {\n      code: \"return {'out': f'{clip}@{at}'}\"\n    }\n\
             trim.clip = self.clip\n    trim.at = self.at\n    self.cut = trim.out\n  }\n\
           edit.clip = self.items\n  edit.at = self.index\n\
           caption = ExecPython(text: String) -> (out: String) {\n    code: \"return {'out': text + '!'}\"\n  }\n\
           caption.text = edit.cut\n\
           upper = ExecPython(text: String) -> (out: String) {\n    code: \"return {'out': text.upper()}\"\n  }\n\
           upper.text = caption.out\n  self.results = upper.out\n}\n\
         clips.items = src.items\nout = Debug\nout.data = clips.results\n",
    )?;
    let refused = project.weft_refused(&["run", "--from", r#"clips.edit={"clip":"intro","at":3}"#, "--target", "out"]).await?;
    anyhow::ensure!(
        refused.contains("the run cannot start") && refused.contains("'clips.edit' is inside loop 'clips'") && !refused.contains("this build"),
        "refused before building: {refused}"
    );
    let stdout = project.weft(&["run", "--json", "--from", r#"clips.edit={"clip":"intro","at":3}"#, "--target", "clips.upper"]).await?;
    let settled = project.settled(common::run_id_of(&stdout)?).await?;
    settled.completed()?
        .assert_output("clips.upper", &json!({ "out": "INTRO@3!" }))?
        .assert_untouched("src")?
        .assert_untouched("out")?;
    for node in ["clips.edit.trim", "clips.caption", "clips.upper"] {
        let frames: Vec<serde_json::Value> = settled.events_of(node).filter(|e| e.kind() == "node_completed").map(|e| e.frames().clone()).collect();
        anyhow::ensure!(frames == vec![json!([{ "index": 0 }])], "{node} ran once, inside iteration 0: {frames:?}");
    }
    settled.assert_loop_iterations("clips", 0)?;
    project.finish().await
}
