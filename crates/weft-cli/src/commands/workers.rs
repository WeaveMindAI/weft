//! `weft workers [set|reset]`: the project's own worker levers.
//!
//! Every lever the install sets for workers the project can set for
//! itself; what it leaves unset follows the install. A change reaches the
//! calls that follow it: they go to workers started with the new levers.

use anyhow::Context;
use weft_platform_traits::{WorkerOverrides, WorkersResponse};

use super::Ctx;

pub enum WorkersAction {
    Show,
    /// Set these levers, keeping the ones the project already sets.
    Set(WorkerOverrides),
    /// Put these levers back on the install's (every lever when empty).
    Reset(Vec<String>),
}

pub async fn run(ctx: Ctx, action: WorkersAction) -> anyhow::Result<()> {
    let (client, id, _) = super::resolve_project(&ctx)?;
    let path = format!("/projects/{id}/workers");
    let read = |v: serde_json::Value| -> anyhow::Result<WorkersResponse> {
        serde_json::from_value(v).context("read the project's worker levers")
    };
    let current = read(client.get_json(&path).await?)?;
    let project = match action {
        WorkersAction::Show => None,
        WorkersAction::Set(levers) => Some(current.project.merged(&levers)),
        WorkersAction::Reset(names) => {
            let mut project = current.project.clone();
            if names.is_empty() {
                project = WorkerOverrides::default();
            }
            for name in &names {
                project.unset(name).map_err(anyhow::Error::msg)?;
            }
            Some(project)
        }
    };
    let answer = match project {
        None => current,
        Some(project) => read(client.put_json(&path, &serde_json::to_value(&project)?).await?)?,
    };
    if ctx.json_out(&answer)? {
        return Ok(());
    }
    let effective = serde_json::to_value(&answer.effective)?;
    for lever in WorkerOverrides::LEVERS {
        let from = if answer.project.sets(lever) { "this project" } else { "the install" };
        let shown = match &effective[lever] {
            serde_json::Value::Null => "unset".to_string(),
            value => value.to_string().trim_matches('"').to_string(),
        };
        println!("{lever:<22} {shown:<8} ({from})");
    }
    Ok(())
}
