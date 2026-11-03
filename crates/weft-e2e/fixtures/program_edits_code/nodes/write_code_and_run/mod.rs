//! WriteCodeAndRun: a project-local custom node that edits another
//! project's code and runs it, through `ctx.project(id).code()`: the file
//! lands in that project's working copy (what it replaces kept beside it,
//! by its hash, with nothing read or stored again), and the run builds
//! that code first, the way `weft run` does. The edit is a step of its
//! own in the node's state: following the build pauses the run, and the
//! body run again after the pause comes straight back to the build
//! without editing again.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use weft::node::NodeOutput;
use weft::{Node, NodeManifest, RunContext, WeftError, WeftResult};

#[derive(NodeManifest)]
pub struct WriteCodeAndRunNode;

/// Whether the edit landed. A `bool` and not a `()`: a kept `Some(())`
/// reads back as `None`, since both are JSON `null`.
#[derive(Default, Serialize, Deserialize)]
struct Edited {
    edited: Option<bool>,
}

#[async_trait]
impl Node for WriteCodeAndRunNode {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let project: String = ctx.inputs.get("project")?;
        let path: String = ctx.inputs.get("path")?;
        let text: String = ctx.inputs.get("text")?;
        let kept_as: String = ctx.inputs.get("keptAs")?;
        let id = uuid::Uuid::parse_str(&project)
            .map_err(|e| WeftError::Input(format!("'{project}' is not a project id: {e}")))?;
        let mut state = ctx.keep(Edited::default()).await?;
        {
            let ctx = &ctx;
            state
                .once(|s| &mut s.edited, async move |_| {
                    let code = ctx.project(id).code();
                    let copy = code.state().await?;
                    let before = copy.working.get(&path).ok_or_else(|| WeftError::Input(format!("the project holds no {path}")))?;
                    match code.edit().text(path, text).blob(kept_as, before)?.apply().await? {
                        weft::working_copy::Applied::Applied { .. } => Ok(true),
                        weft::working_copy::Applied::Stale { paths } => Err(WeftError::Input(format!("changed underneath: {paths:?}"))),
                    }
                })
                .await?;
        }
        let started = ctx.project(id).code().run().target("out").start(&mut state).await?;
        ctx.pulse_downstream(NodeOutput::new().set("runId", started.run_id.to_string())).await
    }
}
