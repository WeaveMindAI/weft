//! Shared helper: discover the cwd project, compile it, and have the
//! install build it. Every mutating project-scoped command (`run`,
//! `activate`, `bake`, `resync`, the infra verbs, `build`) calls this first
//! so users don't have to remember `weft build` as a prerequisite.
//!
//! Semantics:
//!   - Compile here first: a mistake in the program is reported at once,
//!     with every diagnostic, before anything is uploaded.
//!   - Snapshot: every covered file the tenant's assets lack is uploaded
//!     straight to the bucket; the manifest names the version.
//!   - Resolve the `@asset` refs only this machine can read (a file outside
//!     the project, a URL) and send them with the version.
//!   - `POST /projects/{id}/builds`: the install fetches the version back,
//!     compiles it, builds every stale image with its own BuildKit, and
//!     registers the result. Nothing is built or loaded on this machine,
//!     and nothing this machine computed is believed.
//!   - Registering a build is UNCONDITIONAL even while executions run on
//!     the previous image: every worker task is stamped with the image it
//!     was enqueued for and only claimable by a process baked from it, so
//!     in-flight work finishes on the old processes while new work flows to
//!     fresh, current-image processes.


use anyhow::{Context, Result};

use weft_core::builds::BuiltProgram;

use super::Ctx;
use crate::client::DispatcherClient;
use crate::progress::Progress;

/// User's local choice for how to handle in-flight Fire executions
/// when a `run` lands with a stale binary or a `deactivate` lands with
/// running execs. The type lives in weft-core (one definition shared
/// with the broker/dispatcher wire protocol); re-exported here so
/// every CLI verb keeps importing it from the gate that consumes it.
pub use weft_core::{RunningChoice, RunningPolicy};

/// Parse an optional `--running-policy` CLI flag value, mapping an
/// unrecognized value to a uniform error. `None` stays `None` (the
/// verb prompts or defaults). One parser for every verb's
/// `--running-policy` so the accepted set can't drift.
pub fn parse_running_policy_flag(flag: Option<&str>) -> Result<Option<RunningPolicy>> {
    match flag {
        None => Ok(None),
        Some(s) => RunningPolicy::parse(&s.to_ascii_lowercase())
            .map(Some)
            .ok_or_else(|| {
                anyhow::anyhow!("invalid --running-policy '{s}'; expected 'wait' or 'cancel'")
            }),
    }
}

/// The running-policy answer a verb puts on the wire: the flag parsed
/// (cancel unless it says wait) and the drain cap, which only ever
/// rides a wait. A `--drain-timeout` beside cancel is refused rather
/// than dropped: it would bound nothing, and a flag that changes
/// nothing is one the person believes did. One reading for every verb
/// that takes the pair (`activate`, `deactivate`, `resync`, the infra
/// verbs), so they cannot disagree about what the cap means.
pub fn parse_running_choice(
    policy: Option<&str>,
    drain_timeout: Option<u64>,
) -> Result<(RunningPolicy, Option<u64>)> {
    let policy = parse_running_policy_flag(policy)?.unwrap_or_default();
    if policy == RunningPolicy::Cancel && drain_timeout.is_some() {
        anyhow::bail!(
            "--drain-timeout bounds a wait, and the running policy is cancel (the default); \
             pass `--running-policy wait` with it, or drop it"
        );
    }
    Ok((policy, drain_timeout))
}

/// The running-work answer as every verb sends it: the policy always
/// (so the wire never guesses), the cap when one was given.
pub fn running_choice(policy: RunningPolicy, drain_timeout: Option<u64>) -> RunningChoice {
    RunningChoice { running_policy: Some(policy), drain_timeout_secs: drain_timeout }
}

pub struct ProjectHandle {
    pub id: String,
    pub name: String,
    pub client: DispatcherClient,
    /// Published sources verified against the files used to compile this build.
    pub manifest: super::versions::Manifest,
    /// What the install built and registered: the compiled program and its
    /// three authoritative hashes (binary = worker image identity,
    /// definition = runtime shape / resync drift, infra = infra closure /
    /// upgrade drift). Downstream verbs name the build by these.
    pub built: BuiltProgram,
}

/// What a person is told when a build moved infra places onto a new
/// image: only a copy started from now on gets it.
fn replaced_infra_images_note(places: &[String]) -> Option<String> {
    if places.is_empty() {
        return None;
    }
    Some(format!(
        "this build changed the image of infra {}: a copy started from now on gets the new image, \
         and a copy already running keeps its own until you run `weft infra upgrade` \
         (with `--instance <id>` for one instance's copy)",
        places.join(", ")
    ))
}

impl ProjectHandle {
    pub fn binary_hash(&self) -> &str {
        &self.built.binary_hash
    }
    pub fn definition_hash(&self) -> &str {
        &self.built.definition_hash
    }

    /// This build as an activate or a resync names it (no reactivate
    /// choice: the caller sets one when it has one).
    pub fn activation_target(&self) -> weft_core::activation::ActivationTarget {
        weft_core::activation::ActivationTarget { build: self.built.named(), reactivate_choice: None }
    }
}

/// Make sure the install knows this project, WITHOUT building anything.
/// Cheap and a no-op when it already does.
///
/// The version tree lives on the install, under the project's id, so a
/// verb that records a version needs the project to exist there.
/// `weft checkpoint` is the one such verb a person can reasonably reach
/// for before they have ever run (save a point, then start changing
/// things), and making them run first would build a worker image for
/// nothing.
pub async fn ensure_project_known(ctx: &Ctx) -> Result<()> {
    let project = ctx.project()?;
    let client = ctx.client()?;
    let body = weft_core::projects::DeclareRequest { id: project.id(), name: project.manifest.package.name.clone() };
    client
        .post_json("/projects", &serde_json::to_value(&body)?)
        .await
        .context("declare the project to the install")?;
    Ok(())
}

/// Discover + compile the cwd project and have the install build and
/// register it (see the module doc). Nothing here parks, drains, or
/// prompts.
pub async fn ensure_registered(
    ctx: &Ctx,
    progress: &Progress,
    node_set: weft_core::builds::NodeSet,
) -> Result<ProjectHandle> {
    let compiled = compile_project(ctx, progress)?;
    build_compiled(ctx, progress, node_set, compiled).await
}

pub struct CompiledProject {
    pub definition: weft_core::ProjectDefinition,
    sources: super::versions::Manifest,
}

/// Compile without publishing files or building an image. Commands validate
/// their requested operation against this same definition before registration.
pub fn compile_project(ctx: &Ctx, progress: &Progress) -> Result<CompiledProject> {
    let project = ctx.project()?;
    let sources = super::versions::local_manifest(project)?;

    // Compile + enrich once; both hashes are scoped to the referenced
    // / infra-closure nodes, so they need the definition + catalog.
    // Cheap, and downstream code after ensure_registered needs these
    // anyway. Use the diagnostic-bearing loader so a compile failure
    // can fire a structured progress error (the editor's action-bar
    // modal renders per-diagnostic info) instead of a single
    // flattened string.
    let (definition, catalog) = match weft_compiler::hash::load_enriched_project_with_diagnostics(project) {
        Ok(pair) => pair,
        Err(weft_compiler::hash::CompileLoadError::Read(msg)) => {
            anyhow::bail!("{msg}");
        }
        Err(weft_compiler::hash::CompileLoadError::Diagnostics(diags)) => {
            return Err(compile_failure(project, progress, &diags));
        }
    };
    // The program's own rules (a trigger inside a loop, a wire into a
    // trigger, ...) are checked here, before any verb looks at its cut or
    // spec: the diagnostic that names the node and says why is what the
    // user reads, never a later refusal about a cut it caused. The build
    // validates again on its own path; this is the same check, earlier.
    let diags = weft_compiler::validate::validate_with_mode(
        &definition,
        &catalog,
        weft_compiler::validate::ValidationMode::Structural,
    );
    if diags.iter().any(|d| matches!(d.severity, weft_compiler::Severity::Error)) {
        return Err(compile_failure(project, progress, &diags));
    }
    Ok(CompiledProject { definition, sources })
}

/// Report compile diagnostics both ways at once and hand back the error
/// to return: a structured progress event for the editor, and the same
/// per-line rendering in the error text for a terminal.
fn compile_failure(
    project: &weft_compiler::project::Project,
    progress: &Progress,
    diags: &[weft_compiler::Diagnostic],
) -> anyhow::Error {
    let summary = diags
        .iter()
        .find(|d| matches!(d.severity, weft_compiler::Severity::Error))
        .map(|d| d.message.clone())
        .unwrap_or_else(|| "compile failed".to_string());
    // The editor's action-bar modal renders one entry per
    // diagnostic from this structured event; the location's
    // `file` is the diagnostic's own source file (an @include's
    // nodes keep their file's coordinates), falling back to
    // main.weft, so a click jumps to the right buffer.
    // SYNC: diagnostic -> ActionErrorDiagnostic mapping <->
    //       extension-vscode/src/preflight.ts preflightBlock
    let main_weft = project.main_weft();
    let json_diags: Vec<serde_json::Value> = diags
        .iter()
        .map(|d| {
            let severity = match d.severity {
                weft_compiler::Severity::Error => "error",
                weft_compiler::Severity::Warning => "warning",
                weft_compiler::Severity::Info => "info",
                weft_compiler::Severity::Hint => "info",
            };
            serde_json::json!({
                "severity": severity,
                "code": d.code,
                "message": d.message,
                "location": {
                    "file": d.file.clone().unwrap_or_else(|| main_weft.to_string_lossy().into_owned()),
                    "line": d.line,
                    "column": d.column,
                },
            })
        })
        .collect();
    progress.structured_error(serde_json::json!({
        "message": summary,
        "what": "Compiling project",
        "stage": "compile",
        "diagnostics": json_diags,
    }));
    // In TTY mode there's no action-bar to read the structured
    // event, so the error itself must carry the per-line
    // locations: one `line:column message` per error, the same
    // rendering the catalog/parse path produces. A bare
    // "compile failed with N diagnostic(s)" would force the
    // user back to the editor to find WHERE.
    anyhow::anyhow!("compile failed:\n{}", weft_compiler::render_diagnostics(diags))
}

/// Upload the version the compile read, and have the install build it.
pub async fn build_compiled(
    ctx: &Ctx,
    progress: &Progress,
    node_set: weft_core::builds::NodeSet,
    compiled: CompiledProject,
) -> Result<ProjectHandle> {
    let CompiledProject { definition, sources } = compiled;
    let project = ctx.project()?;
    let client = ctx.client()?;
    let manifest = super::versions::snapshot(&client, project).await?;
    anyhow::ensure!(manifest == sources,
        "project files changed after compilation; rerun the command to build and record the same sources");

    // The `@asset` refs only this machine can read, resolved here and sent
    // with the version (a referenced file the tenant's assets lack is
    // uploaded on the way).
    let resolutions =
        crate::commands::assets::asset_resolutions(&client, &project.root, &definition, true).await?;

    let id = project.id().to_string();
    let path = format!("/projects/{id}/builds");
    let body = weft_core::builds::VersionBuildRequest {
        name: project.manifest.package.name.clone(),
        manifest: manifest.clone(),
        node_set,
        assets: resolutions.map.clone(),
    };
    progress.build_start(&project.manifest.package.name);
    progress.dispatcher_call_start(&path);
    let (status, text) = client.post_json_status(&path, &serde_json::to_value(&body)?).await.context("ask the install to build")?;
    if !(200..300).contains(&status) {
        anyhow::bail!(
            "the build failed:\n{}",
            if text.trim().is_empty() { format!("the install answered {status}") } else { text.trim().to_string() }
        );
    }
    let built: BuiltProgram = serde_json::from_str(&text).context("read the build's answer")?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id }));
    progress.build_done(&project.manifest.package.name, &built.built_images);
    if let Some(note) = replaced_infra_images_note(&built.replaced_infra_images) {
        progress.warn(&note);
    }

    // The version's blobs and the assets the built program uses are the
    // project's current references; everything else starts expiring.
    for warning in crate::commands::assets::publish_references(&client, &built.definition, &resolutions, Some(&manifest)).await? {
        progress.warn(&warning);
    }
    // The install built the files as they were when this command read
    // them; if the disk moved meanwhile, the build registered is already
    // behind it, and the next edit would look already live.
    anyhow::ensure!(
        super::versions::local_manifest(project)? == manifest,
        "project files changed while building, so the version this built is already \
         behind your disk. Run the command again (it rebuilds only what moved). If several \
         people or agents edit at once, wait for them to finish first"
    );

    Ok(ProjectHandle {
        id,
        name: project.manifest.package.name.clone(),
        client,
        manifest,
        built,
    })
}
