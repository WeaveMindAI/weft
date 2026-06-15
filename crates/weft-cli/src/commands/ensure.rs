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
//!   - When the project's current build would produce a NEW
//!     `binary_hash` (engine / node-impl / type-set / weft.toml
//!     change) AND running executions exist, prompt the user with
//!     the same wait-or-cancel dialog that deactivate / infra
//!     verbs use. Definition-only edits (config / topology) flow
//!     through silently: the running workers finish on their
//!     snapshotted definition; new executions fetch the fresh
//!     definition from the broker via the per-execution path.
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
    pub project: weft_compiler::project::Project,
    /// Binary hash sent to the dispatcher. Doubles as the worker
    /// docker image tag suffix. Flips on engine / node-impl / node-
    /// type-set / weft.toml changes. A config-only or topology-only
    /// edit does NOT flip it; the same worker image keeps serving.
    pub binary_hash: String,
    /// Definition hash sent to the dispatcher. Identifies the
    /// runtime project shape (topology + configs). Flips on every
    /// edit that survives `enrich`. Drives the resync drift signal
    /// and is the lookup key for the worker's
    /// `fetch_project_definition(project_id, hash)` call.
    pub definition_hash: String,
    /// Infra hash sent to the dispatcher. Drives the upgrade drift
    /// signal. Computed from the parsed project + the workspace.
    pub infra_hash: String,
    /// The running-executions policy the build gate RESOLVED (the
    /// supplied flag or the user's interactive answer), or `None` if
    /// the gate didn't trip. `infra sync` reuses this for its later
    /// trigger-deactivation prompt so it doesn't ask the same
    /// "what about running executions?" question twice with possibly
    /// conflicting answers.
    pub resolved_running_policy: Option<RunningPolicy>,
}

impl ProjectHandle {
    /// Inject the three hash fields into a JSON body map using the
    /// canonical camelCase keys. Single source of truth for the wire
    /// contract.
    pub fn inject_hash_fields(&self, body: &mut serde_json::Map<String, serde_json::Value>) {
        inject_hash_fields_opt(
            body,
            Some(&self.binary_hash),
            Some(&self.definition_hash),
            Some(&self.infra_hash),
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

/// Discover + compile + register the cwd project. `running_policy`
/// pre-answers the stale-binary build gate (so `--running-policy
/// {wait|cancel}` works on EVERY build verb, not just `run`); pass
/// `None` to be prompted (TTY) or to bail with a structured error
/// (`--json`) when the gate trips.
///
/// `reactivates_after_gate` says whether the CALLER re-enables triggers
/// right after `ensure_registered` returns (`activate`, `resync`). It
/// does NOT change whether we park to drain: a `wait` on an ACTIVE
/// project ALWAYS parks new fires (else a recurring trigger keeps the
/// old image busy and the running set never drains, hanging forever).
/// It only controls the user MESSAGE: a non-reactivating verb (`run`,
/// `infra`) leaves the project inactive with queued fires after the
/// park, so we tell the user to run `weft activate` to re-enable and
/// replay; a reactivating verb does that itself, so it stays silent.
pub async fn ensure_registered(
    ctx: &Ctx,
    progress: &Progress,
    running_policy: Option<RunningPolicy>,
    reactivates_after_gate: bool,
) -> Result<ProjectHandle> {
    let cwd = std::env::current_dir().context("cwd")?;
    let project = weft_compiler::project::Project::discover(&cwd)
        .map_err(|e| anyhow::anyhow!("discover project: {e}"))?;

    let weft_root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;

    // Compile + enrich once; both hashes are scoped to the referenced
    // / infra-closure nodes, so they need the definition + catalog.
    // Cheap, and downstream code after ensure_registered needs these
    // anyway. Use the diagnostic-bearing loader so a compile failure
    // can fire a structured progress error (the editor's action-bar
    // modal renders per-diagnostic info) instead of a single
    // flattened string.
    let (definition, catalog) = match crate::hash::load_enriched_project_with_diagnostics(&project) {
        Ok(pair) => pair,
        Err(crate::hash::CompileLoadError::Read(msg)) => {
            anyhow::bail!("{msg}");
        }
        Err(crate::hash::CompileLoadError::Diagnostics(diags)) => {
            let summary = diags
                .iter()
                .find(|d| matches!(d.severity, weft_compiler::Severity::Error))
                .map(|d| d.message.clone())
                .unwrap_or_else(|| "compile failed".to_string());
            // The editor's action-bar modal renders one entry per
            // diagnostic from this structured event; the location's
            // `file` is the SOURCE FILE (main.weft), not the project
            // directory, so a click jumps to the right buffer.
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
                            "file": main_weft.to_string_lossy(),
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
            anyhow::bail!(
                "compile failed:\n{}",
                weft_compiler::render_diagnostics(&diags)
            );
        }
    };

    let binary_hash =
        crate::hash::compute_binary_hash(&definition, &project, &weft_root, &catalog)?;
    let definition_hash = crate::hash::compute_definition_hash(&definition)?;
    let infra_hash =
        crate::hash::compute_infra_hash(&definition, &project.root, &weft_root, &catalog)?;

    let binary_short = crate::commands::build::short_hash(&binary_hash);
    let definition_short = crate::commands::build::short_hash(&definition_hash);
    let infra_short = crate::commands::build::short_hash(&infra_hash);

    let client = ctx.client();
    let dispatcher = ctx.dispatcher_url().to_string();

    // Stale-binary gate. If our build will produce a new binary
    // image AND running executions are currently using the old
    // image, ask the user (or apply the pre-supplied policy) before
    // paying the docker-build cost. The dispatcher answers with the
    // drift bits and running_count via /status. Pure config /
    // topology edits flow through silently: the binary cache
    // matches, so no docker build runs and no dialog is needed.
    let (resolved_running_policy, parked_to_drain) = apply_running_executions_policy(
        ctx,
        progress,
        &client,
        &project.id().to_string(),
        &binary_short,
    running_policy,
    )
    .await?;

    // If we parked the project's triggers to drain (active project +
    // wait), everything from here on runs while the project is INACTIVE
    // with fires queued in `parked_fires`. A failure now (image build,
    // register) leaves it that way, so any error must name the state +
    // the recovery (`weft activate` re-enables triggers and replays the
    // queue; the gate won't re-trip because the running set drained).
    // Without this, a docker error after the park silently deactivates a
    // previously-live project with no hint to the user.
    let strand_ctx = |stage: &str| -> String { parked_recovery_ctx(stage, parked_to_drain) };

    // Worker image: hash-skip + build + kind-load. The dispatcher
    // gets the binary_hash on every spawn-relevant call so the
    // project row's `running_binary_hash` stays current regardless
    // of whether we rebuilt or hit the cache.
    let image_tag = crate::commands::build::worker_image_tag(&project, &binary_hash);
    crate::commands::build::ensure_worker_image_with_progress(progress, &project, &image_tag)
        .await
        .with_context(|| strand_ctx("worker image"))?;
    // Send the already compiled + enriched definition (built above for
    // the infra hash). The dispatcher can't compile it: the nodes live
    // here, not in the dispatcher pod. It stores the artifact as-is.
    let register_body = serde_json::json!({
        "id": project.id().to_string(),
        "name": project.manifest.package.name,
        "definition": definition,
        "binaryHash": binary_short,
        "definitionHash": definition_short,
        "infraHash": infra_short,
    });
    let register_resp: serde_json::Value = client
        .post_json("/projects", &register_body)
        .await
        .with_context(|| strand_ctx(&format!("register against {dispatcher}")))?;

    // A non-reactivating verb (run, infra) that parked the project to
    // drain leaves triggers off with fires queued. One typed progress
    // event covers both modes: structured NDJSON in --json (the
    // extension must see that a previously-live project ended up
    // inactive with queued fires), human sentence in TTY. A
    // reactivating verb re-enables + replays itself next, so it stays
    // silent.
    if parked_to_drain && !reactivates_after_gate {
        progress.parked_inactive();
    }
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
        project,
        binary_hash: binary_short,
        definition_hash: definition_short,
        infra_hash: infra_short,
        resolved_running_policy,
    })
}

/// Stale-binary gate. Hits `/status` to learn whether the project's
/// current `running_binary_hash` matches our desired one and how many
/// executions are currently in flight. The dialog only fires when
/// BOTH conditions hold; a pure config or topology edit (drift on
/// `definition_hash` alone) doesn't touch the worker image, so the
/// running execution keeps its snapshotted definition and the new
/// run picks up the new shape via the per-execution broker fetch.
///
/// Returns `(resolved_policy, parked_to_drain)`. `resolved_policy` is
/// the policy actually RESOLVED at the gate (the supplied flag or the
/// user's interactive answer), or `None` when the gate didn't trip; a
/// caller that asks about running executions AGAIN later in the same
/// command (infra sync's trigger-deactivation) reuses it so the two
/// phases can't disagree. `parked_to_drain` is true iff we parked the
/// project's triggers (active project + wait) so the caller can warn the
/// user and attach recovery context to any later failure.
async fn apply_running_executions_policy(
    ctx: &Ctx,
    progress: &Progress,
    client: &DispatcherClient,
    project_id: &str,
    desired_binary_short: &str,
    running_policy: Option<RunningPolicy>,
) -> Result<(Option<RunningPolicy>, bool)> {
    // Status path with our desired binary hash so the dispatcher
    // can compute `binary_drift` against the project row's stored
    // `running_binary_hash`. New projects (no row yet, or row never
    // built) return 404 here; the build proceeds without a dialog
    // because there are by definition no running executions on the
    // old image. Every other failure (5xx, transport, parse) bubbles
    // so a dispatcher hiccup doesn't silently bypass the gate.
    let status_path =
        format!("/projects/{project_id}/status?desiredBinaryHash={desired_binary_short}");
    let resp = match client
        .get_json_or_missing(&status_path)
        .await
        .with_context(|| format!("status gate for project {project_id}"))?
    {
        Some(v) => v,
        None => return Ok((None, false)),
    };
    // No `unwrap_or` here: a missing field on the response is a
    // contract bug, not "no drift" / "no running". Defaulting to
    // false / 0 silently bypasses the gate, which is the exact
    // failure mode the gate exists to prevent.
    let binary_drift = resp
        .pointer("/drift/binary_drift")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| anyhow::anyhow!(
            "status gate for project {project_id}: dispatcher response \
             missing or non-bool `drift.binary_drift`; this is a wire \
             contract violation between this CLI and the dispatcher. \
             `--running-policy` cannot help: it answers the gate, but \
             the gate never gets a verdict because the field needed to \
             evaluate it is absent. Recovery: upgrade the dispatcher \
             (or this CLI) so the versions match"
        ))?;
    let running_count = resp
        .get("running_count")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow::anyhow!(
            "status gate for project {project_id}: dispatcher response \
             missing or non-numeric `running_count`; same contract \
             violation as above"
        ))?;
    if !binary_drift || running_count == 0 {
        return Ok((None, false));
    }

    // Choose policy: explicit flag wins. Otherwise prompt interactively
    // in TTY mode; in JSON mode, bail with an actionable error so the
    // extension picks one and retries.
    let policy = if let Some(p) = running_policy {
        p
    } else if ctx.json() {
        progress.structured_error(serde_json::json!({
            "message": format!(
                "{running_count} execution(s) still running on the old worker image; \
                 pass --running-policy wait | cancel to choose"
            ),
            "what": "Stale worker image with running executions",
            "stage": "build-gate",
            "runningCount": running_count,
        }));
        anyhow::bail!(
            "{running_count} execution(s) still running on the old worker image; \
             pass --running-policy wait | cancel to choose"
        );
    } else {
        prompt_running_policy(running_count)?
    };

    // Park whenever the project is ACTIVE, regardless of policy: a live
    // project keeps firing triggers onto the OLD image for the whole
    // minutes-long build, so new fires must queue (via the deactivate
    // park machinery). Under `wait` the park is also what lets the
    // running set ever empty; under `cancel` it is what stops fresh
    // fires from spawning stale-image workers between the cancel and
    // the new image landing. This is state-driven, not verb- or
    // policy-driven. The downstream absorbs the parked outcome: a
    // reactivating verb (activate / resync) replays the queue onto the
    // new image; a non-reactivating verb leaves it parked and emits
    // `ParkedInactive` so the user knows to `weft activate`. A
    // non-active project has no triggers firing, so nothing to park.
    let parked = super::deactivate::project_is_active(client, project_id).await?;
    if parked {
        park_triggers_for_drain(client, project_id).await?;
    }
    // Once parked, the project is INACTIVE with fires queued. Any failure
    // in the cancel/wait steps below leaves it that way, so name the
    // state + recovery on those errors too (the post-park image-build and
    // register steps get the same context via `strand_ctx` upstream).
    // Without this, an error here strands a previously-live project with
    // no hint, the exact failure mode the park-recovery context exists to
    // prevent.
    let park_ctx = |stage: String| -> String { parked_recovery_ctx(&stage, parked) };
    match policy {
        RunningPolicy::Cancel => {
            // The endpoint replies 204 No Content; `post_json` would
            // choke parsing the empty body AFTER the cancellations
            // already landed server-side (command errors, build never
            // happens). `post_empty` is the 204-aware helper.
            let path = format!("/projects/{project_id}/cancel-running");
            client
                .post_empty(&path)
                .await
                .with_context(|| park_ctx(format!("cancel-running against {project_id}")))?;
        }
        RunningPolicy::Wait => wait_for_drain(progress, client, project_id, parked)
            .await
            .with_context(|| park_ctx(format!("wait for drain of {project_id}")))?,
    }
    Ok((Some(policy), parked))
}

/// Park the project's triggers so new fires queue into `parked_fires`
/// instead of running on the stale image, and leave running executions
/// to drain. Reuses the deactivate park+wait machinery: the dispatcher
/// sets status=deactivating with the park gate axes already showing, and
/// the journal bridge CASes status to inactive once the running set
/// empties. A reactivating verb (activate, resync) replays the parked
/// fires onto the NEW image after the gate via
/// `reactivateChoice=execute_parked_keep_suspended` (also the correct
/// outcome: those fires would otherwise have run on a stale binary); a
/// non-reactivating verb leaves them parked for a later `weft activate`.
/// Only the deactivate POST is issued here; the wait for the drain to
/// complete is the shared `wait_for_drain` poll.
/// Attach the parked-inactive recovery hint to an error `stage` string
/// when the project's triggers were parked to drain. Shared by every
/// post-park failure site (cancel/wait inside the policy step, and the
/// later image-build/register steps) so the recovery wording can't
/// drift between them. When not parked, the stage passes through.
fn parked_recovery_ctx(stage: &str, parked: bool) -> String {
    if parked {
        format!(
            "{stage}; the project's triggers were parked to drain running executions and it is \
             now INACTIVE with queued fires. Rerun `weft activate` to re-enable triggers and \
             replay them on the new image"
        )
    } else {
        stage.to_string()
    }
}

async fn park_triggers_for_drain(client: &DispatcherClient, project_id: &str) -> Result<()> {
    let path = format!("/projects/{project_id}/deactivate");
    let spec = serde_json::json!({ "mode": "park", "runningPolicy": "wait" });
    client
        .post_with_body(&path, &spec)
        .await
        .with_context(|| format!("park triggers for drain against {project_id}"))?;
    Ok(())
}

fn prompt_running_policy(running_count: u64) -> Result<RunningPolicy> {
    println!(
        "{running_count} execution(s) are still running on the old worker image."
    );
    println!("The new build changes the binary (engine / node / weft.toml edit).");
    println!();
    println!("Choose:");
    println!("  1) wait    let the running executions finish on the old image");
    println!("  2) cancel  cancel running executions immediately and run on the new image");
    let raw = crate::prompt::prompt_line("> ", "--running-policy wait | cancel")?;
    // The wire parse is exact-match (lowercase); accept any casing
    // from the human, plus the numeric shortcuts.
    match raw.as_str() {
        "1" => Ok(RunningPolicy::Wait),
        "2" => Ok(RunningPolicy::Cancel),
        other => RunningPolicy::parse(&other.to_ascii_lowercase()).ok_or_else(|| {
            anyhow::anyhow!("invalid choice '{other}'; expected 1, 2, wait, or cancel")
        }),
    }
}

/// Poll `/status` until the project's running set has drained. The
/// completion signal depends on whether we parked first:
///   - `parked` (the project was active and we issued a park+wait
///     deactivate): the dispatcher set status=deactivating and the
///     journal bridge CASes it to inactive once `running_count` hits
///     zero. The drain is done when status LEAVES `deactivating`. We
///     watch status, not the count, because the count alone can't tell
///     "drained" from "a brand-new fire just parked and a stale read
///     raced": status is the dispatcher's own committed verdict.
///   - not parked (non-active project, no triggers firing): nothing
///     parks new work because there is none, so the drain is simply
///     `running_count == 0`.
///
/// The wait is UNBOUNDED on purpose: a user-facing workflow on a
/// running execution can legitimately last hours or days (a multi-day
/// LLM pipeline, a long human-in-the-loop pause, an infra-heavy
/// backfill). Binding it with a deadline would refuse legitimate work.
/// Instead:
///   - the user can Ctrl+C at any time to back out and pick a
///     different policy (cancel + rebuild, or come back later);
///   - a periodic `DrainWait` progress event keeps the wait legible
///     in BOTH modes (a human breadcrumb line in TTY, a structured
///     NDJSON event in `--json` so the extension can render the same
///     "still waiting on N" without a TTY-only blind spot);
///   - `/status` errors bubble loudly because the user chose
///     "wait" and a silent fallback would bulldoze past a failing
///     dispatcher.
async fn wait_for_drain(
    progress: &Progress,
    client: &DispatcherClient,
    project_id: &str,
    parked: bool,
) -> Result<()> {
    let path = format!("/projects/{project_id}/status");
    // Two cadences: poll every 2s so a drain that completes in 3-6s
    // doesn't get rounded up to 10s of perceived wait, breadcrumb
    // every 10s so a multi-hour wait isn't spammy. The dispatcher's
    // /status is cheap (one row read); 30 polls/min for a developer's
    // local drain is honest load.
    let interval = std::time::Duration::from_secs(2);
    let breadcrumb_every = std::time::Duration::from_secs(10);
    let start = tokio::time::Instant::now();
    let mut next_breadcrumb = start + breadcrumb_every;
    loop {
        let resp = client
            .get_json(&path)
            .await
            .with_context(|| format!("status during drain wait for project {project_id}"))?;
        let running_count = resp
            .get("running_count")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow::anyhow!(
                "drain wait for project {project_id}: dispatcher response \
                 missing or non-numeric `running_count`; this is a wire \
                 contract violation, treating it as 0 would silently \
                 declare the drain complete. Recovery: Ctrl+C and rerun \
                 with --running-policy cancel"
            ))?;
        let drained = if parked {
            // Status leaving `deactivating` is the dispatcher's committed
            // "running set empty" verdict (the journal bridge CASes it).
            let status = resp
                .get("status")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!(
                    "drain wait for project {project_id}: dispatcher response \
                     missing or non-string `status` after a park+wait \
                     deactivate; treating it as drained would replay parked \
                     fires onto a not-yet-empty image. Recovery: Ctrl+C and \
                     rerun with --running-policy cancel"
                ))?;
            status != "deactivating"
        } else {
            running_count == 0
        };
        if drained {
            return Ok(());
        }
        let now = tokio::time::Instant::now();
        if now >= next_breadcrumb {
            // The emitter handles mode: a human line in TTY, a
            // structured NDJSON event in --json. Either way the
            // unbounded wait stays observable.
            progress.drain_wait(running_count, (now - start).as_secs());
            next_breadcrumb = now + breadcrumb_every;
        }
        tokio::time::sleep(interval).await;
    }
}
