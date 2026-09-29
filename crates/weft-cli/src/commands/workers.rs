//! `weft workers [set|reset]`: the project's own worker levers.
//!
//! Every lever the install sets for workers the project can set for
//! itself; what it leaves unset follows the install. A change applies to
//! the running workers at once.

use super::Ctx;

/// The levers a project sets, by the names the flags and the API use.
pub struct WorkerLevers {
    pub min_instances: Option<u32>,
    pub max_instances: Option<u32>,
    pub concurrency: Option<u32>,
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub startup_boost: Option<bool>,
    pub cpu_always_allocated: Option<bool>,
}

pub enum WorkersAction {
    Show,
    Set(WorkerLevers),
    /// Put these levers back on the install's (every lever when empty).
    Reset(Vec<String>),
}

// SYNC: lever names <-> crates/weft-platform-traits/src/runner.rs (WorkerOverrides)
const LEVERS: [&str; 7] =
    ["min_instances", "max_instances", "concurrency", "cpu", "memory", "startup_boost", "cpu_always_allocated"];

pub async fn run(ctx: Ctx, action: WorkersAction) -> anyhow::Result<()> {
    let (client, id, _) = super::resolve_project(&ctx)?;
    let path = format!("/projects/{id}/workers");
    let current = client.get_json(&path).await?;
    let answer = match action {
        WorkersAction::Show => current,
        WorkersAction::Set(l) => {
            let mut project = current["project"].as_object().cloned().unwrap_or_default();
            let mut put = |k: &str, v: Option<serde_json::Value>| {
                if let Some(v) = v {
                    project.insert(k.to_string(), v);
                }
            };
            put("min_instances", l.min_instances.map(Into::into));
            put("max_instances", l.max_instances.map(Into::into));
            put("concurrency", l.concurrency.map(Into::into));
            put("cpu", l.cpu.map(Into::into));
            put("memory", l.memory.map(Into::into));
            put("startup_boost", l.startup_boost.map(Into::into));
            put("cpu_always_allocated", l.cpu_always_allocated.map(Into::into));
            client.put_json(&path, &serde_json::Value::Object(project)).await?
        }
        WorkersAction::Reset(names) => {
            for n in &names {
                anyhow::ensure!(LEVERS.contains(&n.as_str()), "'{n}' is not a worker lever; the levers are {}", LEVERS.join(", "));
            }
            let mut project = current["project"].as_object().cloned().unwrap_or_default();
            if names.is_empty() {
                project.clear();
            } else {
                project.retain(|k, _| !names.contains(k));
            }
            client.put_json(&path, &serde_json::Value::Object(project)).await?
        }
    };
    if ctx.json_out(&answer)? {
        return Ok(());
    }
    let project = answer["project"].as_object().cloned().unwrap_or_default();
    for lever in LEVERS {
        let value = &answer["effective"][lever];
        let from = if project.contains_key(lever) { "this project" } else { "the install" };
        println!("{lever:<22} {:<8} ({from})", value.to_string().trim_matches('"'));
    }
    Ok(())
}
