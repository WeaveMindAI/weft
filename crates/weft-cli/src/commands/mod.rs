//! CLI subcommand implementations. Each module is a verb; most are
//! thin wrappers over an HTTP call to the dispatcher.

pub mod new;
pub mod assets;
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
pub mod catalog;
pub mod describe_nodes;
pub mod parse;
pub mod executions;
pub mod status;
pub mod test_node;
pub mod token;
pub mod connect;
pub mod listener;
pub mod files;

use std::sync::Arc;

use weft_compiler::project::Project;

/// The cached answer of one cwd project discovery: the start path the
/// search walked, and what it found there (None = no weft.toml
/// anywhere up the tree).
type DiscoveredProject = anyhow::Result<(std::path::PathBuf, Option<Project>)>;

/// Per-invocation CLI context. Built once in `main.rs`:
///   - `dispatcher_url` is resolved from the `--dispatcher` flag, the
///     cwd-discovered `weft.toml`, then the localhost default. ONE
///     resolution at startup; verbs read the result, never re-resolve.
///   - `project` holds the cwd-discovered Project. Verbs that need
///     project metadata (id, name) call `Ctx::project()`. Verbs that
///     only talk to the dispatcher (`ps`, `describe-nodes`, `daemon`)
///     don't touch it.
#[derive(Clone)]
pub struct Ctx {
    dispatcher_url: String,
    json: bool,
    /// One cwd discovery per invocation. Carries the START PATH the
    /// search walked alongside its answer, so the "no project" error
    /// can name the directory that was really searched.
    project: Arc<std::sync::OnceLock<DiscoveredProject>>,
}

impl Ctx {
    /// Build a Ctx from CLI flags. Resolving the dispatcher URL from
    /// the cwd weft.toml (absent `--dispatcher`) needs the project
    /// walk anyway, so that ONE walk primes the project cache too;
    /// with `--dispatcher` given, discovery stays deferred so verbs
    /// that never need a project (ps, describe-nodes) never pay it.
    pub fn new(dispatcher_override: Option<String>, json: bool) -> Self {
        let lock = std::sync::OnceLock::new();
        let dispatcher_url = match dispatcher_override {
            Some(u) => u,
            None => {
                let discovered = Self::discover_here();
                let url = discovered
                    .as_ref()
                    .ok()
                    .and_then(|(_, p)| p.as_ref())
                    .map(|p| p.dispatcher_url())
                    .unwrap_or_else(|| "http://localhost:9999".to_string());
                let _ = lock.set(discovered);
                url
            }
        };
        Self {
            dispatcher_url,
            json,
            project: Arc::new(lock),
        }
    }

    /// The one cwd project walk (see [`DiscoveredProject`]).
    fn discover_here() -> DiscoveredProject {
        let cwd = std::env::current_dir()?;
        let found = Project::find(&cwd).map_err(|e| anyhow::anyhow!("discover project: {e}"))?;
        Ok((cwd, found))
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

    /// The cwd-discovered project, lazy-loaded and cached, keeping
    /// [`Project::find`]'s three answers apart: found, no weft.toml
    /// anywhere up the tree (`Ok(None)`), or a manifest that is here
    /// but broken (`Err`). Callers that act differently outside a
    /// project than inside a broken one (the forget sweep) need the
    /// distinction; everything else goes through [`Ctx::project`].
    pub fn project_here(&self) -> anyhow::Result<Option<&Project>> {
        self.project
            .get_or_init(Self::discover_here)
            .as_ref()
            .map(|(_, p)| p.as_ref())
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// [`Ctx::project_here`] for the verbs that REQUIRE a project:
    /// "no project" is an error too, in [`Project::no_project_here`]'s
    /// one wording, naming the directory the search walked.
    pub fn project(&self) -> anyhow::Result<&Project> {
        self.project_here()?.ok_or_else(|| {
            // project_here returned Ok, so the cache holds the walked
            // start path.
            let start = self
                .project
                .get()
                .and_then(|r| r.as_ref().ok())
                .map(|(p, _)| p.as_path())
                .unwrap_or_else(|| std::path::Path::new("."));
            anyhow::anyhow!("discover project: {}", Project::no_project_here(start))
        })
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
