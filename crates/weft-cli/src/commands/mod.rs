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
pub mod cancel_activate;
pub mod cancel_build;
pub mod cancel_running;
pub mod resync;
pub mod ps;
pub mod rm;
pub mod logs;
pub mod daemon;
pub mod infra;
pub mod add;
pub mod catalog;
pub mod describe_nodes;
pub mod parse;
pub mod executions;
pub mod status;
pub mod token;
pub mod listener;
pub mod files;

use std::sync::Arc;

use weft_compiler::project::Project;

/// Per-invocation CLI context. Built once in `main.rs`:
///   - `dispatcher_url` is resolved from the `--dispatcher` flag, the
///     cwd-discovered `weft.toml`, then the localhost default. ONE
///     resolution at startup; verbs read the result, never re-resolve.
///   - `project` holds the cwd-discovered Project (lazy-loaded once).
///     Verbs that need project metadata (id, name) call
///     `Ctx::project()`. Verbs that only talk to the dispatcher
///     (`ps`, `describe-nodes`, `daemon`) don't touch it.
#[derive(Clone)]
pub struct Ctx {
    dispatcher_url: String,
    json: bool,
    project: Arc<std::sync::OnceLock<anyhow::Result<Project>>>,
}

impl Ctx {
    /// Build a Ctx from CLI flags. Resolves the dispatcher URL once;
    /// project discovery is deferred so verbs that don't need a
    /// project (ps, describe-nodes) never pay for it.
    pub fn new(dispatcher_override: Option<String>, json: bool) -> Self {
        let dispatcher_url = match dispatcher_override {
            Some(u) => u,
            None => Project::discover(
                &std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")),
            )
            .map(|p| p.dispatcher_url())
            .unwrap_or_else(|_| "http://localhost:9999".to_string()),
        };
        Self {
            dispatcher_url,
            json,
            project: Arc::new(std::sync::OnceLock::new()),
        }
    }

    pub fn json(&self) -> bool {
        self.json
    }

    pub fn dispatcher_url(&self) -> &str {
        &self.dispatcher_url
    }

    pub fn client(&self) -> crate::client::DispatcherClient {
        crate::client::DispatcherClient::new(self.dispatcher_url.clone())
    }

    /// The cwd-discovered project, lazy-loaded and cached. Returns
    /// the same Result every call (a single discover, success or
    /// failure). Verbs that REQUIRE a project return the error;
    /// verbs that don't ignore it.
    pub fn project(&self) -> anyhow::Result<&Project> {
        self.project
            .get_or_init(|| {
                let cwd = std::env::current_dir()?;
                Project::discover(&cwd).map_err(|e| anyhow::anyhow!("discover project: {e}"))
            })
            .as_ref()
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Build a fresh Progress emitter for a verb. Threading the
    /// emitter through helper functions (rather than reaching for
    /// `Ctx`) keeps the verb scope explicit at every call site.
    pub fn progress(&self, verb: crate::progress::ActionVerb) -> crate::progress::Progress {
        crate::progress::Progress::new(verb, self.json)
    }

    /// Run a verb body with one shared Progress emitter. The body
    /// receives `&Progress` so it can fire phase events. On error,
    /// `progress.error(...)` is called automatically (so verbs
    /// don't have to repeat the trap), then the error propagates.
    pub async fn with_progress<F, Fut>(
        &self,
        verb: crate::progress::ActionVerb,
        body: F,
    ) -> anyhow::Result<()>
    where
        F: FnOnce(crate::progress::Progress) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        let progress = self.progress(verb);
        match body(progress.clone()).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Skip the auto-trap if the body already emitted a
                // structured error: the editor would otherwise see
                // a second `error` phase with a flattened message
                // and overwrite the structured one in its store.
                if !progress.has_emitted_error() {
                    progress.error(&format!("{e}"));
                }
                Err(e)
            }
        }
    }
}

/// Resolve a project id: explicit CLI argument wins; otherwise read
/// it from the cwd-discovered project on `Ctx`. Pass-through for
/// name-vs-uuid: the dispatcher's endpoints accept uuids, so name
/// lookups would need a `/projects/by-name` round-trip; today we
/// pass the raw arg through and the dispatcher rejects non-uuids.
pub fn resolve_project_id(ctx: &Ctx, explicit: Option<String>) -> anyhow::Result<String> {
    if let Some(raw) = explicit {
        return Ok(raw);
    }
    Ok(ctx.project()?.id().to_string())
}

/// Build (client, id, name) for verbs that talk about THIS project.
/// All three come from the Ctx-cached Project; the client uses the
/// already-resolved dispatcher URL.
pub fn resolve_project(
    ctx: &Ctx,
) -> anyhow::Result<(crate::client::DispatcherClient, String, String)> {
    let project = ctx.project()?;
    Ok((
        ctx.client(),
        project.id().to_string(),
        project.manifest.package.name.clone(),
    ))
}
