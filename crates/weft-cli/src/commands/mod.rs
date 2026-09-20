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
pub mod bake;
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
pub mod tangle;
pub mod token;
pub mod connect;
pub mod listener;
pub mod files;
// The version tree and its verbs.
pub mod versions;
pub mod checkpoint;
pub mod branch;
pub mod tree;
pub mod diff;
pub mod freeze;
pub mod examples;
pub mod prune;
pub mod wake;

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

    /// Under `--json`, print `value` as the command's one JSON line on
    /// stdout and answer true, so the caller returns right after and
    /// nothing human follows it. Without the flag, print nothing and
    /// answer false.
    pub fn json_out(&self, value: &impl serde::Serialize) -> anyhow::Result<bool> {
        if !self.json {
            return Ok(false);
        }
        println!("{}", serde_json::to_string(value)?);
        Ok(true)
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
    /// don't have to repeat the trap), then the error propagates
    /// wrapped as [`crate::progress::Reported`]: the user has seen it,
    /// and `main` only turns it into the exit code.
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
                    // The whole chain: a context alone ("update project
                    // asset lifetimes") hides the cause the reader needs.
                    progress.error(&format!("{e:#}"));
                }
                Err(anyhow::Error::new(crate::progress::Reported(e)))
            }
        }
    }
}

/// Resolve a project id: explicit CLI argument wins; otherwise read
/// it from the cwd-discovered project on `Ctx`. Pass-through for
/// name-vs-uuid: the dispatcher's endpoints accept uuids, so name
/// lookups would need a `/projects/by-name` round-trip; today we
/// pass the raw arg through and the dispatcher rejects non-uuids.
/// The full color an execution argument names. A whole uuid is taken
/// as it is; anything shorter is the start of one, and the dispatcher
/// answers the single execution of yours it starts (or says it starts
/// none, or several). Every command that takes a color goes through
/// here, so `weft events 3f2a` works the way `weft stop 3f2a` does.
pub async fn resolve_color(ctx: &Ctx, input: &str) -> anyhow::Result<String> {
    if uuid::Uuid::parse_str(input).is_ok() {
        return Ok(input.to_string());
    }
    let resp: serde_json::Value = ctx
        .client()
        .get_json(&format!("/executions/resolve/{input}"))
        .await
        .map_err(|e| anyhow::anyhow!("'{input}' names no execution: {e}"))?;
    resp.get("color")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("dispatcher response missing color: {resp}"))
}

/// One node of the program, from the way a person spells it: `store`
/// for a node of the entry file, `setup.store` for the `store` inside
/// the file the site `setup` includes, the whole path from the entry
/// file down through every site. Outside a project the spelling is
/// taken as the node itself, at the top.
///
/// The commands that hand the daemon a place (`weft wake`, the per-node
/// infra verbs) go through here; the ones that keep the id and call
/// path for themselves (`weft events --node`) go through
/// [`resolve_spelling`], which is the one reading of a spelling both
/// share. `spell_node` is the way back out for journal rows, which are
/// still keyed by the compiled id and its frames.
///
/// What comes back is the PLACE, spelled the one way the daemon keys a
/// place by (`setup.store`, the person's own spelling written back
/// canonical): a wait parked on a node inside an included file, and the
/// infra instance of one, belong to one call of it, and the daemon
/// holds both under that spelling.
pub fn node_address_for(ctx: &Ctx, spelled: &str) -> anyhow::Result<String> {
    // Outside a project there is no program to check against, and the
    // daemon checks every node it is handed anyway (an unknown one is
    // refused there, naming the run or the project). So the spelling
    // goes as it is, and it is the top-level node it names.
    let Ok(project) = ctx.project() else { return Ok(spelled.to_string()) };
    let (definition, _) = weft_compiler::hash::load_enriched_project(project)?;
    match resolve_spelling(&definition, spelled)? {
        Spelled::Node { id, call_path } => Ok(weft_core::project::address_of(&definition, &id, &call_path)),
        Spelled::Group { .. } => anyhow::bail!(
            "'{spelled}' is a group or an include site, and this takes a node; name one of \
             its nodes, like `{spelled}.<node>`"
        ),
    }
}

/// What a spelling a person wrote names in the compiled definition:
/// the thing's own id and the call path that use of it runs under
/// (`auth.check` is the node `Auth.check` under `["auth"]`).
pub enum Spelled {
    Node { id: String, call_path: Vec<String> },
    /// A group, a loop or an include site. The commands that take a
    /// node refuse it; `weft events --node` takes it, because a
    /// group's own rows are its boundaries' and come with it.
    Group { id: String, call_path: Vec<String> },
}

/// Read a spelling the way a person writes it (`auth.check` for the
/// `check` of the file the site `auth` includes) into what it names,
/// refusing every spelling that names nothing with the one that would.
///
/// The compiled id is not something a person can be asked to type: a
/// node inside an included file is keyed by the file's path
/// (`@src:sweep.key`), which the language calls unspellable on purpose
/// and names through the call site everywhere a person can see. It
/// resolves, because it IS the id, and taking it would teach a person a
/// spelling no other command accepts; so it is refused with the one
/// that works. A boundary the compiler made (`gate__in`) is named by
/// its group.
pub fn resolve_spelling(definition: &weft_core::ProjectDefinition, spelled: &str) -> anyhow::Result<Spelled> {
    if let Some((group, _)) = spelled.rsplit_once("__in").or_else(|| spelled.rsplit_once("__out")).filter(|(_, rest)| rest.is_empty()) {
        anyhow::bail!("'{spelled}' is a group boundary the compiler made; name the group, `{group}`, and its rows come with it");
    }
    if let Some((group, _)) = spelled.rsplit_once(".__in").or_else(|| spelled.rsplit_once(".__out")).filter(|(_, rest)| rest.is_empty()) {
        anyhow::bail!("'{spelled}' is a boundary the compiler made; name the site, `{group}`, and its rows come with it");
    }
    let (id, call_path) = weft_core::project::resolve_address(definition, spelled);
    let Some(node) = definition.nodes.iter().find(|n| n.id == id) else {
        if definition.groups.iter().any(|g| g.id == id) {
            return Ok(Spelled::Group { id, call_path });
        }
        anyhow::bail!(
            "'{spelled}' names no node in this program (a node inside an included file is named \
             through its site, like `site.{}`)",
            spelled.rsplit('.').next().unwrap_or(spelled)
        );
    };
    if call_path.is_empty() && weft_core::project::selection::enclosing_body(definition, node).is_some() {
        anyhow::bail!(
            "'{spelled}' is inside an included file; name it through the site that \
             includes the file, like `site.{}`",
            weft_core::project::address_of(definition, &id, &["site".into()]).trim_start_matches("site.")
        );
    }
    Ok(Spelled::Node { id, call_path })
}

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

/// A journal unix stamp as the local wall-clock time a person reads
/// (`2026-09-02 21:36:47`), the one rendering every listing verb uses
/// so a run's start, its events and its log lines line up by eye.
/// Zero (a row that never carried a stamp) renders as a dash rather
/// than as 1970.
pub fn local_time(unix_secs: u64) -> String {
    if unix_secs == 0 {
        return "-".to_string();
    }
    // A stamp too large for the calendar prints as its number rather
    // than as a wrapped-around date.
    match i64::try_from(unix_secs).ok().and_then(|s| chrono::DateTime::<chrono::Utc>::from_timestamp(s, 0)) {
        Some(t) => t.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M:%S").to_string(),
        None => unix_secs.to_string(),
    }
}

#[cfg(test)]
mod local_time_tests {
    use super::local_time;

    /// The stamp renders as a date and a time, and the two sentinels
    /// (zero, out of range) never render as a bogus date.
    #[test]
    fn renders_a_readable_local_time() {
        let text = local_time(1_756_838_207);
        assert_eq!(text.len(), "2026-09-02 21:36:47".len(), "{text}");
        assert_eq!(&text[4..5], "-");
        assert_eq!(&text[10..11], " ");
        assert_eq!(local_time(0), "-");
        assert_eq!(local_time(u64::MAX), u64::MAX.to_string());
    }
}
