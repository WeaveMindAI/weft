//! CLI subcommand implementations. Each module is a verb; most are
//! thin wrappers over an HTTP call to the dispatcher.

pub mod new;
pub mod build;
pub mod ensure;
pub mod run;
pub mod follow;
pub mod stop;
pub mod activate;
pub mod deactivate;
pub mod ps;
pub mod rm;
pub mod logs;
pub mod daemon;
pub mod infra;
pub mod add;
pub mod describe_nodes;
pub mod executions;
pub mod status;
pub mod token;

pub struct Ctx {
    pub dispatcher: Option<String>,
}

impl Ctx {
    pub fn client(&self) -> crate::client::DispatcherClient {
        crate::client::DispatcherClient::new(
            crate::client::resolve_dispatcher_url(self.dispatcher.as_deref())
        )
    }
}

/// Resolve a project id from either an explicit CLI argument
/// (UUID or short name) or by walking up from cwd to find a
/// `weft.toml`. Returns the UUID string the dispatcher uses.
///
/// Only used by commands that want a "talk about THIS project"
/// default. Commands that are process-wide (ps, describe-nodes)
/// don't need it.
pub fn resolve_project_id(explicit: Option<String>) -> anyhow::Result<String> {
    if let Some(raw) = explicit {
        // Accept either a UUID or treat as-is (for name-based
        // lookups the dispatcher can resolve). Dispatcher's
        // endpoints today only accept UUIDs; pass through.
        return Ok(raw);
    }
    let cwd = std::env::current_dir()?;
    let project = weft_compiler::project::Project::discover(&cwd)
        .map_err(|e| anyhow::anyhow!("no project under cwd: {e}"))?;
    Ok(project.id().to_string())
}
