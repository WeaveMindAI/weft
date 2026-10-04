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
pub mod infra_card;
pub mod infra_env;
pub mod catalog;
pub mod describe_nodes;
pub mod parse;
pub mod executions;
pub mod status;
pub mod test_node;
pub mod tangle;
pub mod connect_lib;
pub mod instance_values;
pub mod token;
pub mod connect;
pub mod options;
pub mod files;
// The version tree and its verbs.
pub mod versions;
pub mod checkpoint;
pub mod branch;
pub mod running_source;
pub mod tree;
pub mod diff;
pub mod freeze;
pub mod examples;
pub mod prune;
pub mod wake;
pub mod workers;
pub mod domain;
pub mod frontend;
pub mod target;
pub mod ci;

use std::sync::Arc;

use weft_compiler::project::Project;

/// The cached answer of one cwd project discovery: the start path the
/// search walked, and what it found there (None = no weft.toml
/// anywhere up the tree).
type DiscoveredProject = anyhow::Result<(std::path::PathBuf, Option<Project>)>;

/// The install a command acts on: its address and this person's key for
/// it (`crate::credentials`), sent on every request.
struct Install {
    url: String,
    operator_key: Option<String>,
}

/// A raw install address and where it came from. The two are kept apart
/// because they mean different things: the `--dispatcher` flag is a
/// request to act on that install (refused beside `--on`, and by a verb
/// bound to this machine), while `WEFT_DISPATCHER_URL` is ambient tooling
/// setup (setup.sh exports it), so an explicit `--on` wins over it and a
/// verb bound to this machine does not consult it.
#[derive(Clone, Debug)]
pub enum Dispatcher {
    Flag(String),
    Env(String),
}

impl Dispatcher {
    /// The env var that stands in for `--dispatcher`.
    pub const ENV: &'static str = "WEFT_DISPATCHER_URL";

    /// The flag when given, else a non-empty `WEFT_DISPATCHER_URL`.
    pub fn from_flag_or_env(flag: Option<String>) -> Option<Self> {
        match flag {
            Some(url) => Some(Self::Flag(url)),
            None => std::env::var(Self::ENV).ok().filter(|v| !v.is_empty()).map(Self::Env),
        }
    }

    pub fn url(&self) -> &str {
        match self {
            Self::Flag(url) | Self::Env(url) => url,
        }
    }

    /// The address to act on beside `on`: the flag always (the pair is
    /// refused later), the env var only when no `--on` names a target.
    fn beside(&self, on: Option<&str>) -> Option<&str> {
        match (self, on) {
            (Self::Env(_), Some(_)) => None,
            _ => Some(self.url()),
        }
    }
}

/// Per-invocation CLI context. Built once in `main.rs`:
///   - `install` is the install this command acts on, resolved on FIRST
///     USE by [`resolve_dispatcher_url`] (through [`Ctx::client`] or
///     [`Ctx::install_access`]) and cached, error included. Lazy on
///     purpose: a command that never talks to an install (`weft daemon
///     start` on a named install that has no ports yet, `weft new`)
///     must not be refused because that install has no address yet.
///   - `project` holds the cwd-discovered Project. Verbs that need
///     project metadata (id, name) call `Ctx::project()`. Verbs that
///     only talk to the dispatcher (`ps`, `describe-nodes`, `daemon`)
///     don't touch it.
#[derive(Clone)]
pub struct Ctx {
    dispatcher: Option<Dispatcher>,
    on: Option<String>,
    install: Arc<std::sync::OnceLock<anyhow::Result<Install>>>,
    json: bool,
    /// One cwd discovery per invocation. Carries the START PATH the
    /// search walked alongside its answer, so the "no project" error
    /// can name the directory that was really searched.
    project: Arc<std::sync::OnceLock<DiscoveredProject>>,
}

/// Which install a command acts on. `--dispatcher` (or
/// `WEFT_DISPATCHER_URL`) is a raw address for tooling; `--on <name>`
/// names a target of the cwd project; neither means the local install
/// (the project's `[targets.local]` when it has one). Both at once is
/// refused: which one was meant cannot be guessed, and guessing wrong
/// can mean acting on a shared install.
pub fn resolve_dispatcher_url(
    dispatcher: Option<String>,
    on: Option<&str>,
    project: impl FnOnce() -> anyhow::Result<Option<Project>>,
) -> anyhow::Result<String> {
    use weft_compiler::project::LOCAL_TARGET;
    refuse_two_installs(dispatcher.as_deref(), on)?;
    match dispatcher {
        Some(url) => crate::credentials::url_key(&url),
        None => {
            let name = on.unwrap_or(LOCAL_TARGET);
            match project()? {
                Some(project) => project.target_url(name).map_err(|e| anyhow::anyhow!("{e}")),
                None if name == LOCAL_TARGET => weft_core::ports::local_public_url().map_err(anyhow::Error::msg),
                None => anyhow::bail!(
                    "--on {name} names a target of a project, and there is no weft.toml here \
                     or above; run it from the project, or pass --dispatcher <url>"
                ),
            }
        }
    }
}

/// `--dispatcher` and `--on` together: refused, see [`resolve_dispatcher_url`].
/// (An exported `WEFT_DISPATCHER_URL` never reaches here beside `--on`:
/// `--on` wins over it, see [`Dispatcher`].)
fn refuse_two_installs(dispatcher: Option<&str>, on: Option<&str>) -> anyhow::Result<()> {
    if let (Some(_), Some(on)) = (dispatcher, on) {
        anyhow::bail!("both --on {on} and --dispatcher name an install; pass one of them");
    }
    Ok(())
}

impl Ctx {
    /// Build a Ctx from CLI flags. Nothing is resolved yet (see
    /// [`Ctx::install`]); only flags that contradict each other are
    /// refused here, since that needs no install and no project.
    pub fn new(dispatcher: Option<Dispatcher>, on: Option<String>, json: bool) -> anyhow::Result<Self> {
        refuse_two_installs(dispatcher.as_ref().and_then(|d| d.beside(on.as_deref())), on.as_deref())?;
        Ok(Self {
            dispatcher,
            on,
            install: Arc::new(std::sync::OnceLock::new()),
            json,
            project: Arc::new(std::sync::OnceLock::new()),
        })
    }

    /// The install this command acts on, resolved once on first use.
    /// Resolving a target reads the SAME cached project walk the verbs
    /// use, so a command never walks the tree twice; with `--dispatcher`
    /// given, the walk never happens.
    fn install(&self) -> anyhow::Result<&Install> {
        self.install
            .get_or_init(|| self.resolve_install(|| Ok(self.project_here()?.cloned())))
            .as_ref()
            .map_err(|e| anyhow::anyhow!("{e:#}"))
    }

    /// Resolve the install from the flags, the given project lookup and
    /// the disk, uncached. A one-shot command goes through
    /// [`Ctx::install`] (the cwd project); a long-lived one goes through
    /// [`Ctx::fresh_client`] (the project of the request at hand).
    fn resolve_install(&self, project: impl FnOnce() -> anyhow::Result<Option<Project>>) -> anyhow::Result<Install> {
        let on = self.on.as_deref();
        let url = resolve_dispatcher_url(self.dispatcher.as_ref().and_then(|d| d.beside(on)).map(str::to_owned), on, || {
            // A broken weft.toml only matters when the project's
            // targets are read: outside the default target it is
            // the answer's error; for the local default the verb
            // that needs the project reports it.
            match project() {
                Ok(found) => Ok(found),
                Err(e) if on.is_some() => Err(e),
                Err(_) => Ok(None),
            }
        })?;
        let operator_key = crate::credentials::operator_key_for(&url, on)?;
        Ok(Install { url, operator_key })
    }

    /// Refuse `--on` and `--dispatcher` for a verb that only ever acts on
    /// this machine's install, so the flag is never silently ignored.
    /// `WEFT_DISPATCHER_URL` is not refused: it is ambient setup, not a
    /// request made to this verb (see [`Dispatcher`]).
    pub fn refuse_other_install(&self, verb: &str) -> anyhow::Result<()> {
        if let Some(on) = &self.on {
            anyhow::bail!("{verb} reports on this machine's install; --on {on} names another one (drop it, or use WEFT_INSTALL=<name> for a named local install)");
        }
        if let Some(Dispatcher::Flag(_)) = &self.dispatcher {
            anyhow::bail!("{verb} reports on this machine's install; --dispatcher names another one (drop it, or use WEFT_INSTALL=<name> for a named local install)");
        }
        Ok(())
    }

    /// The target `--on` named, if any.
    pub fn on(&self) -> Option<&str> {
        self.on.as_deref()
    }

    /// The address and operator key a request from this command carries
    /// (`weft target key` prints them for the editor).
    pub fn install_access(&self) -> anyhow::Result<(&str, Option<&str>)> {
        let install = self.install()?;
        Ok((&install.url, install.operator_key.as_deref()))
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

    /// A client for the install this command acts on; the first call
    /// resolves it, so an install with no address yet errors here.
    pub fn client(&self) -> anyhow::Result<crate::client::DispatcherClient> {
        let install = self.install()?;
        Ok(crate::client::DispatcherClient::new(install.url.clone(), install.operator_key.clone()))
    }

    /// A client resolved afresh on every call, never cached: a long-lived
    /// process (the editor's parse server) outlives any one state of the
    /// install, so an install that starts, or credentials that get fixed,
    /// after it launched are picked up by the next request, and a failure
    /// is that request's error rather than the process's. The project is
    /// the REQUEST's (a parse server serves files from many projects, and
    /// its own cwd names none of them), re-read each time so a weft.toml
    /// edit is seen.
    pub fn fresh_client(&self, project: Option<&Project>) -> anyhow::Result<crate::client::DispatcherClient> {
        let install = self.resolve_install(|| Ok(project.cloned()))?;
        Ok(crate::client::DispatcherClient::new(install.url, install.operator_key))
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
                    progress.error(&e);
                }
                Err(anyhow::Error::new(crate::progress::Reported(e)))
            }
        }
    }
}

/// The full execution an execution argument names. A whole uuid is taken
/// as it is; anything shorter is the start of one, and the dispatcher
/// answers the single execution of yours it starts (or says it starts
/// none, or several). Every command that takes an execution goes through
/// here, so `weft events 3f2a` works the way `weft stop 3f2a` does.
pub async fn resolve_execution_id(ctx: &Ctx, input: &str) -> anyhow::Result<String> {
    if uuid::Uuid::parse_str(input).is_ok() {
        return Ok(input.to_string());
    }
    let resp = ctx
        .client()?
        .get_json(&format!("/executions/resolve/{input}"))
        .await
        .map_err(|e| anyhow::anyhow!("'{input}' names no execution: {e}"))?;
    let resolved: weft_core::program::ResolvedExecution =
        serde_json::from_value(resp).map_err(|e| anyhow::anyhow!("read the resolved execution: {e}"))?;
    Ok(resolved.execution_id.to_string())
}

/// One node of the program, from the way a person spells it:
/// `store` for a node of the entry file, `setup.store` for the `store`
/// inside the file the site `setup` includes, the whole path from the
/// entry file down through every site (see [`resolve_spelling`], the
/// one reading every command shares). Outside a project the spelling is
/// taken as the node itself, at the top.
///
/// What comes back is the PLACE, spelled the one way the daemon keys a
/// place by (`setup.store`, the person's own spelling written back
/// canonical): a wait parked on a node inside an included file, and the
/// infra of one, belong to one call of it, and the daemon
/// holds both under that spelling.
pub fn node_address_for(ctx: &Ctx, spelled: &str) -> anyhow::Result<String> {
    // Outside a project there is no program to check against, and the
    // daemon checks every node it is handed anyway (an unknown one is
    // refused there, naming the run or the project). So the spelling
    // goes as it is, and it is the top-level node it names.
    let Some(project) = ctx.project_here()? else { return Ok(spelled.to_string()) };
    let (definition, _) = weft_compiler::hash::load_enriched_project(project)?;
    let (id, call_path) = resolve_node(project, &definition, spelled)?;
    Ok(weft_core::project::address_of(&definition, &id, &call_path))
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

/// [`resolve_spelling`] for a command that takes a node: a group or an
/// include site is refused, naming how to reach one of its nodes.
pub fn resolve_node(
    project: &weft_compiler::project::Project,
    definition: &weft_core::ProjectDefinition,
    name: &str,
) -> anyhow::Result<(String, Vec<String>)> {
    match resolve_spelling(project, definition, name)? {
        Spelled::Node { id, call_path } => Ok((id, call_path)),
        Spelled::Group { .. } => {
            let spelled = unqualified(name);
            anyhow::bail!(
                "'{spelled}' is a group or an include site, and this takes a node; name one of \
                 its nodes, like `{spelled}.<node>`"
            )
        }
    }
}

/// A name as a person typed it, without the `<file>:` qualifier
/// [`resolve_spelling`] accepts: the spelling to answer them by.
pub fn unqualified(name: &str) -> &str {
    name.split_once(':').map_or(name, |(_, spelled)| spelled)
}

/// Read a name the way a person writes it into what it names: the ONE
/// reading every command that takes a node shares (`weft options`,
/// `weft connect --node`, `weft wake`, the infra verbs, `weft events
/// --node`). The name is a place spelling (`auth.check` for the `check`
/// of the file the site `auth` includes), optionally qualified by the
/// file it is written in, relative to the project's root
/// (`src/auth.weft:auth.check`), which is then checked.
pub fn resolve_spelling(
    project: &weft_compiler::project::Project,
    definition: &weft_core::ProjectDefinition,
    name: &str,
) -> anyhow::Result<Spelled> {
    let Some((file, spelled)) = name.split_once(':') else { return read_spelling(definition, name) };
    let found = read_spelling(definition, spelled)?;
    let (Spelled::Node { id, .. } | Spelled::Group { id, .. }) = &found;
    let asked = project.root.join(file);
    // A node of an included file is keyed by that file's body id (the
    // part before the first dot); anything else is written in the entry
    // file.
    let written_there = match id.strip_prefix('@') {
        Some(_) => id.split('.').next() == Some(weft_compiler::source_name::body_id(&project.root, &asked).as_str()),
        None => asked == project.main_weft(),
    };
    anyhow::ensure!(
        written_there,
        "'{spelled}' is not written in {file}; name it as `{spelled}`, or qualify it with the file it is in"
    );
    Ok(found)
}

/// Read a place spelling (no file qualifier) into what it names,
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
fn read_spelling(definition: &weft_core::ProjectDefinition, spelled: &str) -> anyhow::Result<Spelled> {
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
/// Ctx's install.
pub fn resolve_project(
    ctx: &Ctx,
) -> anyhow::Result<(crate::client::DispatcherClient, String, String)> {
    let project = ctx.project()?;
    Ok((
        ctx.client()?,
        project.id().to_string(),
        project.manifest.package.name.clone(),
    ))
}

/// A journal unix stamp as the UTC time a person reads
/// (`2026-09-02 21:36:47 UTC`), the one rendering every listing verb
/// uses so a run's start, its events and its log lines line up by eye,
/// with the install's own logs too, wherever the install runs. Zero (a
/// row that never carried a stamp) renders as a dash rather than as
/// 1970.
pub fn utc_time(unix_secs: u64) -> String {
    if unix_secs == 0 {
        return "-".to_string();
    }
    // A stamp too large for the calendar prints as its number rather
    // than as a wrapped-around date.
    match i64::try_from(unix_secs).ok().and_then(|s| chrono::DateTime::<chrono::Utc>::from_timestamp(s, 0)) {
        Some(t) => t.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => unix_secs.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{utc_time, resolve_dispatcher_url, Project};

    /// The stamp renders as a date and a time, and the two sentinels
    /// (zero, out of range) never render as a bogus date.
    #[test]
    fn renders_a_readable_utc_time() {
        assert_eq!(utc_time(1_756_838_207), "2025-09-02 18:36:47 UTC");
        assert_eq!(utc_time(0), "-");
        assert_eq!(utc_time(u64::MAX), u64::MAX.to_string());
    }

    fn project_with_prod() -> (tempfile::TempDir, Project) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/main.weft"), "").unwrap();
        std::fs::write(
            dir.path().join("weft.toml"),
            "[package]\nname = \"p\"\nid = \"00000000-0000-0000-0000-000000000000\"\n\
             [targets.prod]\nurl = \"https://weft.example.com\"\n",
        )
        .unwrap();
        let project = Project::load(dir.path()).unwrap();
        (dir, project)
    }

    #[test]
    fn no_flag_means_the_local_install_even_inside_a_project_with_a_remote_target() {
        let (_d, project) = project_with_prod();
        let url = resolve_dispatcher_url(None, None, || Ok(Some(project))).unwrap();
        assert_eq!(url, weft_core::ports::local_public_url().unwrap());
        let url = resolve_dispatcher_url(None, None, || Ok(None)).unwrap();
        assert_eq!(url, weft_core::ports::local_public_url().unwrap());
    }

    #[test]
    fn on_names_a_target_of_the_project() {
        let (_d, project) = project_with_prod();
        let url = resolve_dispatcher_url(None, Some("prod"), || Ok(Some(project.clone()))).unwrap();
        assert_eq!(url, "https://weft.example.com");
        let error = resolve_dispatcher_url(None, Some("staging"), || Ok(Some(project))).unwrap_err();
        assert!(error.to_string().contains("local, prod"), "{error}");
        let error = resolve_dispatcher_url(None, Some("prod"), || Ok(None)).unwrap_err();
        assert!(error.to_string().contains("no weft.toml"), "{error}");
    }

    /// A named install that has never started has no address; building
    /// the context must still succeed (so `weft daemon start` can run),
    /// and only a command that talks to the install hears why. Sets the
    /// process's install env var: nextest runs each test in its own
    /// process, so no other test sees it.
    #[test]
    fn a_named_install_without_ports_fails_only_when_its_address_is_needed() {
        let name = format!("ctx{}", std::process::id());
        std::env::set_var(weft_core::infra::INSTALL_ENV, &name);
        let ctx = super::Ctx::new(None, None, false).expect("a Ctx needs no install address");
        let Err(error) = ctx.client() else { panic!("a named install with no ports has no address") };
        let error = error.to_string();
        assert!(error.contains(&format!("install '{name}' has no ports yet")), "{error}");
        assert!(ctx.install_access().is_err(), "the cached error answers every later use");
        // The install starts after this process did: a fresh resolution
        // sees it, the cached one keeps its answer.
        let dir = weft_core::infra::Install::from_env().unwrap().dir();
        weft_core::ports::InstallPorts::DEFAULT.save(&dir).expect("save ports");
        assert!(ctx.fresh_client(None).is_ok(), "a fresh resolution picks up the started install");
        assert!(ctx.client().is_err(), "the cached error is kept for a one-shot command");
        std::fs::remove_dir_all(&dir).unwrap();
        std::env::remove_var(weft_core::infra::INSTALL_ENV);
    }

    /// A verb bound to this machine refuses the `--dispatcher` flag but
    /// not `WEFT_DISPATCHER_URL`, which setup.sh exports for tooling.
    #[test]
    fn a_machine_bound_verb_refuses_the_flag_but_not_the_env_var() {
        let url = || "http://127.0.0.1:1".to_string();
        let flag = super::Ctx::new(Some(super::Dispatcher::Flag(url())), None, false).unwrap();
        assert!(flag.refuse_other_install("weft daemon status").is_err());
        let env = super::Ctx::new(Some(super::Dispatcher::Env(url())), None, false).unwrap();
        env.refuse_other_install("weft daemon status").expect("the env var is ambient, not a request");
    }

    /// `--on` beside the flag is two answers; beside the exported env var
    /// it simply wins, so a person with the var set can still say `--on`.
    #[test]
    fn on_wins_over_the_env_var_but_not_over_the_flag() {
        let url = || "http://127.0.0.1:1".to_string();
        assert!(super::Ctx::new(Some(super::Dispatcher::Flag(url())), Some("prod".into()), false).is_err());
        let env = super::Dispatcher::Env(url());
        assert_eq!(env.beside(Some("prod")), None, "--on wins");
        assert_eq!(env.beside(None), Some("http://127.0.0.1:1"));
        super::Ctx::new(Some(env), Some("prod".into()), false).expect("the env var yields to --on");
    }

    #[test]
    fn on_and_a_raw_address_together_are_refused() {
        let error = resolve_dispatcher_url(Some("http://x".into()), Some("prod"), || {
            panic!("the project is never read when the flags conflict")
        })
        .unwrap_err();
        assert!(error.to_string().contains("pass one of them"), "{error}");
        let url = resolve_dispatcher_url(Some("http://x/".into()), None, || {
            panic!("a raw address needs no project")
        })
        .unwrap();
        assert_eq!(url, "http://x");
    }
}
