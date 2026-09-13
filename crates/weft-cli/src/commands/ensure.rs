//! Shared helper: discover the cwd project, compile it, register
//! it (or re-register if it already exists). Every mutating
//! project-scoped command (`run`, `activate`, `deactivate`,
//! `infra up`, `infra down`) calls this first so users don't
//! have to remember `weft run` as a prerequisite.
//!
//! Semantics:
//!   - Compile via `weft-compiler::build`: this stages the
//!     docker build context and emits the multi-stage Dockerfile
//!     but does NOT run cargo (cargo runs inside the builder
//!     image).
//!   - Registration is UNCONDITIONAL even while executions run on
//!     the previous image: every worker task is stamped with the
//!     image it was enqueued for and only claimable by a pod baked
//!     from it, so in-flight work finishes on the old pods while
//!     new work flows to fresh, current-image pods. No park, no
//!     drain, no wait-or-cancel dialog at register time. (An
//!     earlier shape parked triggers and drained here; the
//!     image-stamped claim gate made that whole gate obsolete.)
//!   - Build the per-project worker image and load it into the
//!     local kind cluster so spawned worker Pods can pull it.
//!   - Post to `POST /projects`; the dispatcher is idempotent on
//!     the `id` field (existing row gets its source updated).


use anyhow::{Context, Result};

use super::Ctx;
use crate::client::DispatcherClient;
use crate::progress::Progress;

/// User's local choice for how to handle in-flight Fire executions
/// when a `run` lands with a stale binary or a `deactivate` lands with
/// running execs. The type lives in weft-core (one definition shared
/// with the broker/dispatcher wire protocol); re-exported here so
/// every CLI verb keeps importing it from the gate that consumes it.
pub use weft_core::RunningPolicy;

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

pub struct ProjectHandle {
    pub id: String,
    pub name: String,
    pub client: DispatcherClient,
    /// Published sources verified against the files used to compile this build.
    pub manifest: super::versions::Manifest,
    /// The full build plan `ensure_registered` produced: the three authoritative
    /// hashes (binary = worker image identity, definition = runtime shape /
    /// resync drift, infra = infra-closure / upgrade drift) PLUS every image the
    /// version needs (worker + infra), each with its content-addressed ref and
    /// build context. Downstream verbs consume the plan instead of re-compiling
    /// or re-deriving tags: the plan is the single source of truth for what this
    /// project version builds.
    pub plan: weft_compiler::build_plan::BuildPlan,
}

impl ProjectHandle {
    pub fn binary_hash(&self) -> &str {
        &self.plan.binary_hash
    }
    pub fn definition_hash(&self) -> &str {
        &self.plan.definition_hash
    }
    pub fn infra_hash(&self) -> &str {
        &self.plan.infra_hash
    }

    /// Inject the three hash fields into a JSON body map using the
    /// canonical camelCase keys. Single source of truth for the wire
    /// contract.
    pub fn inject_hash_fields(&self, body: &mut serde_json::Map<String, serde_json::Value>) {
        inject_hash_fields_opt(
            body,
            Some(self.binary_hash()),
            Some(self.definition_hash()),
            Some(self.infra_hash()),
        );
    }
}

/// Inject hashes when each is independently optional (`activate.rs`'s
/// "activate-by-id" path forwards none of them). Skipping a None
/// field is the correct behavior: posting null would overwrite the
/// dispatcher's stored running hash and silently flip drift state.
pub fn inject_hash_fields_opt(
    body: &mut serde_json::Map<String, serde_json::Value>,
    binary: Option<&str>,
    definition: Option<&str>,
    infra: Option<&str>,
) {
    if let Some(h) = binary {
        body.insert("binaryHash".into(), serde_json::Value::String(h.into()));
    }
    if let Some(h) = definition {
        body.insert("definitionHash".into(), serde_json::Value::String(h.into()));
    }
    if let Some(h) = infra {
        body.insert("infraHash".into(), serde_json::Value::String(h.into()));
    }
}

/// Make sure the dispatcher knows this project, WITHOUT building
/// anything. Cheap and a no-op when it already does.
///
/// The version tree lives on the dispatcher, under the project's id, so
/// a verb that records a version needs the project to exist there.
/// `weft checkpoint` is the one such verb a person can reasonably reach
/// for before they have ever run (save a point, then start changing
/// things), and making them run first would build a worker image for
/// nothing.
///
/// What this registers is the source: the compiled definition, and none
/// of the three hashes, so the dispatcher's running pointers stay
/// untouched and the project is registered-but-not-built until a real
/// `weft run` builds it. `@asset` references are left unresolved for
/// the same reason: resolving them belongs to the register that makes
/// the project runnable, and that one re-sends the whole definition.
pub async fn ensure_project_known(ctx: &Ctx) -> Result<()> {
    let project = ctx.project()?;
    let client = ctx.client();
    let id = project.id().to_string();
    if client.get_json_if_found(&format!("/projects/{id}")).await?.is_some() {
        return Ok(());
    }
    let definition = weft_compiler::hash::load_enriched_project(project)
        .map_err(|e| anyhow::anyhow!("compile project: {e}"))?
        .0;
    client
        .post_json(
            "/projects/register",
            &serde_json::json!({
                "id": id,
                "name": project.manifest.package.name,
                "definition": definition,
            }),
        )
        .await
        .context("register the project with the dispatcher")?;
    Ok(())
}

/// Discover + compile + register the cwd project. Registration is
/// UNCONDITIONAL with respect to running executions: in-flight work
/// finishes on the pods baked from the image it was enqueued for (the
/// task rows carry that image and the claim gate enforces it), while
/// everything enqueued after this register lands on fresh
/// current-image pods. Nothing here parks, drains, or prompts.
pub async fn ensure_registered(
    ctx: &Ctx,
    progress: &Progress,
    node_set: weft_compiler::codegen::NodeSet,
) -> Result<ProjectHandle> {
    let compiled = compile_project(ctx, progress)?;
    register_compiled(ctx, progress, node_set, compiled).await
}

pub struct CompiledProject {
    pub definition: weft_core::ProjectDefinition,
    catalog: weft_catalog::FsCatalog,
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
    Ok(CompiledProject { definition, catalog, sources })
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

pub async fn register_compiled(
    ctx: &Ctx,
    progress: &Progress,
    node_set: weft_compiler::codegen::NodeSet,
    compiled: CompiledProject,
) -> Result<ProjectHandle> {
    let CompiledProject { mut definition, catalog, sources } = compiled;
    let project = ctx.project()?;
    let client = ctx.client();
    let manifest = super::versions::snapshot(&client, project).await?;
    anyhow::ensure!(manifest == sources,
        "project files changed after compilation; rerun the command to build and record the same sources");

    // Resolve `@asset` refs BEFORE the plan hashes the definition: the
    // asset sync publishes referenced files to the project's asset plane and
    // substitutes their stored-file values, so the hashes cover the resolved
    // content (a changed asset re-hashes exactly like a config change).
    crate::commands::assets::resolve_project_assets(&client, &project.root, &mut definition, Some(&manifest), true)
        .await?;

    // Plan the build from the already-compiled definition + catalog (no second
    // compile): the three hashes + the staged worker context + the infra image set,
    // via the SHARED build brain. The base is ensured first so the staged Dockerfile
    // FROMs it.
    let builder_base_ref = crate::images::builder_base_ref()?;
    let plan = weft_compiler::build_plan::plan_build_from(
        project,
        &definition,
        &catalog,
        &builder_base_ref,
        &crate::commands::build::CliTagPolicy,
        node_set,
    )
    .map_err(|e| anyhow::anyhow!("plan build: {e}"))?;

    // The FULL hashes are sent on the wire and used to tag the worker image, so
    // the dispatcher's `running_binary_hash` matches the content-addressed image
    // tag (`weft-worker:<binary_hash>`) it spawns. The hashes are compared by
    // equality, so any consistent length works; full is the canonical form the
    // image tag uses.

    let dispatcher = ctx.dispatcher_url().to_string();

    // Worker image: hash-skip + build (from the already-staged plan context) +
    // kind-load. The dispatcher gets the binary_hash on every spawn-relevant call so
    // the project row's `running_binary_hash` stays current regardless of whether we
    // rebuilt or hit the cache.
    let worker = crate::commands::build::worker_planned_image(&plan)?;
    // What must survive the post-ensure GC beyond the fresh tag (idle
    // projects' current images, draining pods): the keep-set via
    // `referenced_set_for_gc` (None + a warning when the answer is
    // unlearnable -> the GC is skipped, never guessed).
    let referenced = crate::images::referenced_set_for_gc(&client).await;
    crate::commands::build::ensure_worker_image_with_progress(
        progress,
        &project.id().to_string(),
        &worker.image_ref,
        &worker.context_dir,
        referenced.as_ref(),
    )
    .await
    .context("worker image")?;
    anyhow::ensure!(
        super::versions::local_manifest(project)? == manifest,
        "project files changed while building; run the command again to build and record the same sources"
    );
    // Send the already compiled + enriched definition (built above for
    // the infra hash). The dispatcher can't compile it: the nodes live
    // here, not in the dispatcher pod. It stores the artifact as-is.
    let register_body = serde_json::json!({
        "id": project.id().to_string(),
        "name": project.manifest.package.name,
        "definition": definition,
        "binaryHash": plan.binary_hash,
        "implementations": plan.implementations,
        "source": manifest,
        "definitionHash": plan.definition_hash,
        "infraHash": plan.infra_hash,
    });
    let register_resp: serde_json::Value = client
        .post_json("/projects/register", &register_body)
        .await
        .with_context(|| format!("register against {dispatcher}"))?;

    let id = register_resp
        .get("id")
        .and_then(|v| v.as_str())
        .context("dispatcher response missing id")?
        .to_string();

    // Image GC moved to an explicit `weft clean --images` operation.
    // The earlier shape ran here after register landed, but it
    // wiped both docker AND kind containerd tags; a running worker
    // pod restarted by the kubelet (eviction, node restart) with
    // `imagePullPolicy: IfNotPresent` then went into ImagePullBackOff
    // because the image bytes were gone from the node and there is
    // no registry to pull from in the kind workflow. Disk-pressure
    // cleanup is a developer concern, not a side-effect of every
    // register.

    Ok(ProjectHandle {
        id,
        name: project.manifest.package.name.clone(),
        client,
        manifest,
        plan,
    })
}

// (The register-time "stale-binary gate" that used to live here: prompt for
// wait-or-cancel, park the triggers, drain before registering; was deleted
// when worker tasks became image-stamped. In-flight work finishes on the pods
// baked from its own image while new work flows to fresh current-image pods,
// so registering during running executions disturbs nothing and there is no
// policy to ask for. Disturbing verbs (deactivate, infra stop/terminate/
// upgrade, resync, worker replacement) keep their explicit wait/cancel picker.)
