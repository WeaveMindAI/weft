//! HTTP API surface. The CLI, the VS Code extension, the browser
//! extension, and end users (via webhook URLs minted by the
//! listener) all talk to this surface.
//!
//! Route categories:
//! - `/projects/*`: project registration, run, stop, logs.
//! - `/runs/*`: run state queries and control.
//! - `/events/*`: SSE streams for project and run state.
//! - `/signal-token*`: signal-token minting, token-scoped signal access,
//!   and token-scoped reads of what a node is showing (`/displays`).
//!
//! The dispatcher does NO node-aware work: parse, validate, and
//! catalog introspection are client-side (the CLI reads the project's
//! `nodes/`), because the dispatcher has no access to those nodes.

use axum::{extract::DefaultBodyLimit, routing::{any, get, post}, Router};
use tower_http::cors::CorsLayer;

use crate::state::DispatcherState;

/// Max request body on the UNAUTHENTICATED public inbound doors (`/signal/{token}`
/// fire, `/connect/{*path}`, the `/{*mount_path}` public-entry catch-all). These
/// accept a JSON payload from anyone who knows/guesses the URL; without a cap a
/// caller could push arbitrarily large nested JSON (a parse-amplification vector,
/// and it accumulates into a parked trigger's queue, `parked_fire`). Sized to real
/// webhook payloads. Trusted routes keep axum's default limit.
const PUBLIC_FIRE_BODY_LIMIT: usize = 256 * 1024;

/// The longest a read of this API is held open waiting for its answer to
/// change: a client that wants longer asks again, and a hold this short
/// never meets an idle timeout on the way.
const MAX_HOLD: std::time::Duration = std::time::Duration::from_secs(25);

/// `?wait_ms=` on a read whose answer a client waits on: hold the read
/// for up to that long for the answer to change (never past [`MAX_HOLD`]).
/// Absent, the read answers at once.
#[derive(serde::Deserialize, Default)]
pub struct HoldQuery {
    #[serde(default)]
    wait_ms: u64,
}

/// Who is calling an outside door, as far as the network can tell: the
/// address read off `X-Forwarded-For` through the trusted proxies in
/// front of the listener the call came in on (`weft_core::net::caller_address`).
pub struct CallerAddress(pub std::net::IpAddr);

/// How many proxies in front of the listener a request came in on append
/// to `X-Forwarded-For` (the install's `edge.trustedProxyHops` for that
/// listener). Each listener's router puts it on every request that does
/// not carry one yet (the door puts its own on a request that came by one
/// of the install's domains, through the load balancer in front of them),
/// so the same door reads its caller right whichever way it was reached.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TrustedHops(pub(crate) usize);

/// Put `hops` on every request that carries none yet.
fn with_hops(router: Router<DispatcherState>, hops: TrustedHops) -> Router<DispatcherState> {
    router.layer(axum::middleware::from_fn(move |mut request: axum::extract::Request, next: axum::middleware::Next| async move {
        if request.extensions().get::<TrustedHops>().is_none() {
            request.extensions_mut().insert(hops);
        }
        next.run(request).await
    }))
}

impl CallerAddress {
    /// The key a per-caller count is kept under.
    pub fn key(&self) -> String {
        format!("ip:{}", self.0)
    }

    fn from_parts(parts: &axum::http::request::Parts) -> Result<Self, (axum::http::StatusCode, String)> {
        let Some(TrustedHops(hops)) = parts.extensions.get::<TrustedHops>().copied() else {
            // Every listener's router sets it; a request without it is a
            // wiring bug, refused rather than read with a guessed count.
            return Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, "no trusted proxy hops on the request".to_string()));
        };
        let peer = parts
            .extensions
            .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
            .map(|c| c.0.ip())
            .ok_or_else(|| {
                // The server is always started with connection info; a
                // request without it is a wiring bug, refused rather than
                // counted under a made-up address.
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "no peer address on the request".to_string())
            })?;
        let forwarded = parts.headers.get("x-forwarded-for").and_then(|v| v.to_str().ok());
        Ok(Self(weft_core::net::caller_address(forwarded, peer, hops)))
    }
}

impl axum::extract::FromRequestParts<DispatcherState> for CallerAddress {
    type Rejection = (axum::http::StatusCode, String);

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &DispatcherState,
    ) -> Result<Self, Self::Rejection> {
        Self::from_parts(parts)
    }
}

/// Whether a path is one of the token doors, where a refused answer
/// means somebody presented a token that does not work. A live call
/// (`/connect/`) is not one here: the dispatcher passes it on to the
/// worker, whose door counts the tokens it refuses itself
/// (`weft_engine::door::limits`), and what comes back is the program's
/// answer, whose 401 or 403 says nothing about a token.
fn is_token_door(path: &str) -> bool {
    ["/signal/", "/signal-token/", "/instance/"].iter().any(|p| path.starts_with(p))
}

/// The layer over the outside-caller surface that stops token guessing:
/// an address past the install's bound of refused tokens this minute is
/// answered 429 on every token door until the minute ends, and each
/// refusal a token door gives (401, 403, or a 404 for an unknown
/// `/signal/` token) counts toward it.
async fn guard_token_doors(
    axum::extract::State(state): axum::extract::State<DispatcherState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    let path = request.uri().path().to_string();
    if !is_token_door(&path) || state.edge.invalid_tokens_per_minute.is_none() {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    let address = match CallerAddress::from_parts(&parts) {
        Ok(a) => a.0,
        Err(e) => return e.into_response(),
    };
    let now = crate::lease::now_unix();
    match crate::entry_limits::token_guessing_blocked(&state.pg_pool, &state.edge, address, now).await {
        Ok(Some(refused)) => return crate::entry_limits::too_many(refused),
        Ok(None) => {}
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("rate limit: {e:#}")).into_response(),
    }
    let response = next.run(axum::extract::Request::from_parts(parts, body)).await;
    let status = response.status();
    let refused_token = matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
        || (status == StatusCode::NOT_FOUND && path.starts_with("/signal/"));
    if refused_token {
        // A lost count only lets one more guess through, and the refusal
        // is the answer either way.
        if let Err(e) = crate::entry_limits::note_invalid_token(&state.pg_pool, &state.edge, address, now).await {
            tracing::warn!(target: "weft_dispatcher::api", error = %e, "could not count a refused token");
        }
    }
    response
}

/// The layer that tells a CLI built from another weft commit than the
/// install runs: its answer carries one sentence the CLI prints as a
/// warning ([`weft_core::install::VERSION_NOTE_HEADER`]). Never a refusal:
/// the install compiles projects itself, so such a CLI usually works.
async fn note_cli_version(
    axum::extract::State(state): axum::extract::State<DispatcherState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let cli_commit = request
        .headers()
        .get(weft_core::install::CLI_COMMIT_HEADER)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned());
    let mut response = next.run(request).await;
    let note = cli_commit.and_then(|commit| version_note(state.install_info.source.as_ref(), &commit));
    if let Some(note) = note {
        match axum::http::HeaderValue::from_str(&note) {
            Ok(value) => {
                response.headers_mut().insert(weft_core::install::VERSION_NOTE_HEADER, value);
            }
            // The note quotes what the CLI sent, so a commit with bytes a
            // header cannot carry is not a real one: the answer goes out
            // without a note, and the log says why.
            Err(e) => tracing::warn!(target: "weft_dispatcher::api", error = %e, "a version note is not a valid header value"),
        }
    }
    response
}

/// What to tell a CLI built from `cli_commit` about the weft the install
/// runs: a sentence when the two differ, `None` when they agree or the
/// install does not know its own source (a local install).
fn version_note(source: Option<&weft_core::install::WeftSource>, cli_commit: &str) -> Option<String> {
    let source = source?;
    let cli_commit = cli_commit.trim();
    if cli_commit == source.commit {
        return None;
    }
    let short = |commit: &str| commit.chars().take(12).collect::<String>();
    Some(format!(
        "this weft CLI was built from {}, but the install runs {} at {}; if a command misbehaves, use the install's CLI",
        short(cli_commit),
        source.repository,
        short(&source.commit),
    ))
}

impl HoldQuery {
    /// When the held read answers as it stands: once its hold is up, or
    /// as soon as this copy starts stopping, so the client asks again on
    /// a running copy instead of being cut off at the exit.
    pub fn hold(&self, stopping: &weft_task_store::stopping::Stopping) -> weft_task_store::stopping::Deadline {
        let hold = std::time::Duration::from_millis(self.wait_ms).min(MAX_HOLD);
        weft_task_store::stopping::Deadline::at(tokio::time::Instant::now() + hold).or_stop(stopping)
    }
}

pub mod project;
pub mod domains;
pub mod install_cli;
// `pub` (not `pub(crate)`) like `project` above: `cancel_terminal_events`
// is exercised by the db-test rig in `tests/db/db_tags.rs` against a real
// Postgres.
pub mod run;
mod events;
mod provider_events;
pub(crate) mod signal_token;
pub(crate) mod infra;
pub(crate) mod display;
// `pub` (not `pub(crate)`) like `project` above: the parked-fire queue
// helpers (`append_parked_fire` and siblings) are the dispatcher's
// correctness-critical SQL that the db-test rig in `tests/db/db_lifecycle.rs`
// exercises against a real Postgres.
pub mod signal;
pub mod access;
pub mod instance_door;
mod picks;
mod workers;
pub mod node_tests;
pub mod storage;
pub mod versions;
mod public_page;

/// The dispatcher routes, not yet bound to state.
///
/// The admin surface (everything but `/health` and [`outside_caller_routes`])
/// sits behind [`crate::authenticator::require_caller`] as one layer, so a
/// route added here is authenticated whether or not its handler asks who is
/// calling.
///
/// It answers no browser origin, on a local install as on a cloud one: no
/// web page may drive the install, and its callers (the CLI, the editor's
/// extension host, a frontend's server) are not browsers. The
/// outside-caller surface ([`outside_caller_routes`]: fire URLs, signal
/// tokens, live connect, public mounts) is apart: its callers are
/// cross-origin by design, so it carries [`permissive_cors`].
fn core_routes(state: DispatcherState) -> Router<DispatcherState> {
    Router::new()
        .route("/install", get(project::install))
        // The weft CLI the install stores for one source tree, which a
        // project's deploy workflow downloads instead of compiling.
        .route("/install/cli/{tree}", get(install_cli::for_tree))
        // Whether the database held the install back lately (`weft status`).
        .route("/install/database", get(project::install_database))
        .route("/install/domains", get(domains::list).post(domains::add))
        .route("/install/domains/{name}", axum::routing::delete(domains::remove))
        // The install's maintenance (`weft maintenance`): where it stands
        // and what is on, switching everything off, and the lift (a task
        // the caller follows below).
        .route("/install/maintenance", get(crate::maintenance::state).post(crate::maintenance::on))
        .route("/install/maintenance/off", post(crate::maintenance::off))
        .route("/install/maintenance/follow/{task}", get(crate::maintenance::follow))
        .route("/projects/{id}/frontends", get(crate::frontends::list_route).post(crate::frontends::add))
        .route("/projects/{id}/frontends/{name}", axum::routing::delete(crate::frontends::remove))
        .route("/projects/{id}/frontends/{name}/visitor-address", axum::routing::put(crate::frontends::set_visitor_address))
        .route("/projects/{id}/frontends/{name}/token", post(crate::frontends::new_token))
        .route("/projects/{id}/frontends/{name}/token/{token}/done", post(crate::frontends::token_done))
        .route("/projects/{id}/frontends/{name}/token/{token}", axum::routing::delete(crate::frontends::drop_token))
        .route("/projects", get(project::list).post(project::declare))
        // Build one version inside the install and register it: the one
        // way a project's program and images come to exist. Answers at
        // once with the version build waiting on the image builds it
        // started (202) when images were missing; the install registers
        // it when they end, and the caller follows it below.
        .route("/projects/{id}/builds", post(project::build))
        .route("/projects/{id}", get(project::get).delete(project::remove))
        // The version tree: checkpoint, the run that records itself
        // (THE start path for the CLI and the editor), the tree, head,
        // the activation version, per-run bookkeeping, and prune.
        .route("/projects/{id}/versions", post(versions::checkpoint))
        .route("/projects/{id}/versions/runs", post(versions::run))
        .route("/projects/{id}/versions/runs/{run_id}", axum::routing::put(versions::update_run))
        .route("/projects/{id}/versions/tree", get(versions::tree))
        .route("/projects/{id}/versions/running", get(versions::running))
        .route("/projects/{id}/versions/sweep", post(versions::sweep))
        .route("/projects/{id}/versions/switch", post(versions::switch))
        .route("/projects/{id}/working-copies", get(versions::working_copies))
        .route("/projects/{id}/working-copies/state", get(versions::working_state))
        .route("/projects/{id}/working-copies/folder", get(versions::find_folder).post(versions::register_folder))
        .route("/projects/{id}/working-copies/changes", post(versions::change_files))
        .route("/projects/{id}/working-copies/{wc}", axum::routing::delete(versions::forget))
        .route("/projects/{id}/working-copies/{wc}/sync", post(versions::sync))
        .route("/projects/{id}/trigger-bakes", get(versions::trigger_bakes))
        .route("/projects/{id}/versions/{version}", axum::routing::delete(versions::prune))
        .route("/projects/{id}/status", get(project::status))
        .route("/projects/{id}/instances", get(project::instances))
        .route("/projects/{id}/builds/{build}", get(project::version_build))
        // The connections the program's own access nodes use on this
        // install, never written in the source.
        .route("/projects/{id}/picks", get(picks::list).put(picks::change))
        .route("/projects/{id}/picks/move", post(picks::move_picks))
        .route("/projects/{id}/picks/kept", get(picks::kept))
        // The project's own worker levers.
        .route("/projects/{id}/workers", get(workers::get).put(workers::put))
        .route("/projects/{id}/runs/latest", get(run::latest_for_project))
        .route("/projects/{id}/activations", post(project::activate))
        // Where a weft CLI from before activations ran as tasks asked
        // for one: told to update, never answered in a shape it cannot
        // read.
        .route("/projects/{id}/activate", post(project::activate_from_an_older_cli))
        .route("/projects/{id}/bake", post(project::bake))
        // Cancel an in-flight activate (status=Activating). Wipes
        // every signal row registered so far, cancels the
        // TriggerSetup run, CASes status to Inactive.
        .route("/projects/{id}/cancel-activate", post(project::cancel_activate))
        // Cancel an in-flight build (transition=building). CASes the
        // transition to cancelling_build and stops the project's image
        // builds.
        .route("/projects/{id}/cancel-build", post(project::cancel_build))
        .route("/projects/{id}/deactivate", post(project::deactivate))
        .route("/projects/{id}/quiesce", post(project::quiesce))
        // While the project is in `deactivating`, this endpoint
        // cancels every running, non-suspended run; the
        // journal-bridge drain-watcher then CASes status to
        // `inactive` (the lifecycle target the original deactivate
        // already wrote to the row stays in place).
        .route("/projects/{id}/resync", post(project::resync))
        .route("/projects/{id}/resync/resume", post(project::resume_resync))
        .route("/projects/{id}/follow/{task}", get(project::follow))
        // Unified subworkflow endpoint for Start / Restart / Upgrade.
        // All three CLI verbs POST here; the dispatcher uses the
        // resolved-spec-hash to decide skip-vs-apply per node.
        .route("/projects/{id}/infra/sync", post(infra::sync))
        .route("/projects/{id}/infra/upgrade", post(infra::upgrade))
        .route("/projects/{id}/infra/stop", post(infra::stop))
        .route("/projects/{id}/infra/terminate", post(infra::terminate))
        // Cancel in-flight infra work: halt claimed lifecycle commands
        // between platform calls, cancel unclaimed ones outright, and
        // interrupt the InfraSetup provisioning run. HALT, not
        // rollback: per-node partial state stays visible.
        .route("/projects/{id}/infra/cancel", post(infra::cancel))
        .route("/projects/{id}/infra/look", post(infra::look))
        // Per-node verbs for partial-state recovery.
        // `{node}` on every per-node infra route is the node's
        // PLACEMENT as a person spells it (`one.db`): an infra node inside
        // a file included twice is two placements with two names.
        .route("/projects/{id}/infra/status", get(infra::status))
        .route("/projects/{id}/infra/doors", get(infra::doors))
        .route("/projects/{id}/infra/logs", get(infra::logs))
        .route("/projects/{id}/infra/commands/{cmd_id}", get(infra::command_status))
        .route("/projects/{id}/infra/nodes/{node}/live", get(infra::live))
        .route("/projects/{id}/infra/nodes/{node}/action", post(infra::action))
        .route("/projects/{id}/infra/nodes/{node}/rebake", post(infra::rebake))
        .route("/runs/resolve/{prefix}", get(run::resolve_run_id))
        .route("/runs/{run_id}/cancel", post(run::cancel))
        // Resolve a pure time wait now (`weft wake`).
        .route("/runs/{run_id}/wake/{node}", post(run::wake))
        // Where one node of the run stands (`weft node-state`).
        .route("/runs/{run_id}/nodes/{node}/state", get(run::node_state))
        .route("/runs/{run_id}/logs", get(run::list_logs))
        .route("/runs/{run_id}/replay", get(run::replay))
        // The replay as a person reads it (`weft events`): filtered and
        // spelled against the program the run ran.
        .route("/runs/{run_id}/events", get(run::events))
        .route("/runs/{run_id}/outputs", get(run::outputs))
        // The values a run's events name, read back as the editor shows
        // them (the events carry where the record keeps each).
        .route("/runs/{run_id}/values", post(run::values))
        .route(
            "/runs/{run_id}",
            get(run::get).delete(run::delete_run),
        )
        .route("/runs", get(run::list_runs))
        .route("/runs/clean", post(run::clean))
        .route("/events/project/{id}", get(events::project_stream))
        .route("/events/project/{id}/displays", get(events::display_stream))
        .route("/events/run/{run_id}", get(events::run_stream))
        // Token administration (tenant-authenticated): mint returns the value
        // ONCE, list returns metadata only, revoke addresses the id.
        .route(
            "/signal-tokens",
            get(signal_token::list_tokens).post(signal_token::mint_token),
        )
        .route("/signal-tokens/{id}", axum::routing::delete(signal_token::revoke_token))
        .route("/images/prune", post(project::prune_images))
        // Storage plane: the `weft files` CLI surface (list, usage, download
        // handshake, remove). The dispatcher resolves the acting tenant and
        // proxies each verb to the broker (which owns the bucket + metadata);
        // bulk bytes never flow through the dispatcher.
        .route("/storage/files", get(storage::list_files).delete(storage::remove))
        .route("/storage/files/meta/{*key}", get(storage::file_meta))
        .route("/storage/files/download", post(storage::download))
        .route("/storage/usage", get(storage::usage))
        // Asset publication (the pre-build sync): the same multipart contract
        // the worker uses, proxied to the broker's admin upload surface; part
        // URLs are caller-facing so bytes go straight to the bucket.
        .route("/storage/upload/begin", post(storage::upload_begin))
        .route("/storage/import/begin", post(storage::import_begin))
        // The pre-build asset sync's diff input: the project's published assets.
        .route("/storage/assets/held", post(storage::assets_held))
        .route("/storage/upload/parts", post(storage::upload_parts))
        .route("/storage/upload/part-done", post(storage::upload_part_done))
        .route("/storage/upload/complete", post(storage::upload_complete))
        .route("/storage/upload/resume", post(storage::upload_resume))
        .route("/storage/upload/abort", post(storage::upload_abort))
        // Access store: registrations (a tenant's app identity at an
        // OAuth service), grants (connected accounts), the connect
        // flows the editor drives, and remote_select lookups. All
        // tenant-authenticated; stored values never leave the store
        // side. The OAuth callback door lives on the outside surface.
        .route("/access/doors", post(access::doors))
        .route("/access/mint-app", post(access::mint_app))
        .route("/access/grants", get(access::list_grants))
        .route("/access/grants/{id}", axum::routing::delete(access::delete_grant))
        .route("/access/connect/direct", post(access::connect_direct))
        .route("/access/connect/begin", post(access::connect_begin))
        .route("/access/connect/status", get(access::connect_status))
        .route("/access/lookup", post(access::lookup))
        .route("/access/granted", post(access::granted))
        .route("/access/picker/begin", post(access::picker_begin))
        .route("/access/migrate/key", post(access::migrate_key))
        .route("/access/migrate/export", post(access::migrate_export))
        .route("/access/migrate/import", post(access::migrate_import))
        // Node self-test runs (a short-lived test process per run).
        .route("/projects/{id}/node-tests/run", post(node_tests::run))
        .route(
            "/projects/{id}/node-tests/runs/{task}",
            get(node_tests::status),
        )
        // What a trigger node is showing (its address and how a
        // caller gets past its door), off the kind holding the signal.
        // `{node}` is the trigger's place as a person spells it.
        // Read-only: a trigger's display carries no buttons.
        // Project-token gated.
        .route(
            "/projects/{id}/signals/{node}/live",
            get(signal::live_signal),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::authenticator::require_caller,
        ))
        .merge(outside_caller_routes(state))
}

/// The outside-caller surface: routes whose callers are external by design
/// (a pasted fire URL, a browser extension holding a signal token, a webhook
/// sender, a live caller opening a connection). Each request authenticates
/// itself (capability token in the path, bearer signal token, per-signal auth
/// gate), so the caller's ORIGIN is irrelevant and permissive CORS is part of
/// the surface contract, attached here rather than left to composition. The
/// management surface ([`core_routes`]) answers no browser origin.
// SYNC: the doors <-> packages/weft-connect/src/server/passthrough.ts PASSED_DOORS
fn outside_caller_routes(state: DispatcherState) -> Router<DispatcherState> {
    Router::new()
        .route(
            "/signal/{token}",
            post(signal::fire_signal)
                .delete(signal::cancel_signal)
                .layer(DefaultBodyLimit::max(PUBLIC_FIRE_BODY_LIMIT)),
        )
        .route("/signal/{token}/skip", post(signal::skip_signal))
        // Signal-token-scoped enumeration. The signal token authenticates +
        // scopes, carried in `Authorization: Bearer` (never a URL path, where
        // proxies / access logs would capture the credential). The per-signal
        // fire token (POST /signal/{token}) is a separate credential whose
        // whole job is to be a paste-able URL.
        .route(
            "/signal-token/signals",
            get(signal::list_signals_for_token).delete(signal::clear_all_signals),
        )
        // The files door: a fresh link for a stored file a listed form
        // shows, scoped like the listing (see signal_file_for_token).
        .route(
            "/signal-token/signals/{signal_token}/files/{field}",
            get(signal::signal_file_for_token),
        )
        .route("/signal-token/health", get(signal::signal_token_health))
        // The instance door: one instance of one program, with that instance's
        // token in `Authorization: Bearer` (see `api/instance_door.rs`).
        .route("/instance/fields", get(instance_door::fields))
        .route("/instance/values", axum::routing::put(instance_door::set_values))
        .route("/instance/lookup", post(instance_door::lookup))
        .route("/instance/picker", post(instance_door::picker))
        .route("/instance/connections", get(instance_door::list_connections))
        .route("/instance/connections/{id}", axum::routing::delete(instance_door::delete_connection))
        .route("/instance/connections/direct", post(instance_door::connect_direct))
        .route("/instance/connections/begin", post(instance_door::connect_begin))
        .route("/instance/connections/status", get(instance_door::connect_status))
        .route("/instance/doors", post(instance_door::doors))
        .route("/instance/runs", get(instance_door::runs))
        // What a node is SHOWING, for a client built on top of a weft
        // program (a website that renders the bridge's QR code rather
        // than sending its user to the editor). The same two feeds the
        // editor reads under `/projects/{id}/...`, behind the signal
        // token. See `api/display.rs` for what a token may see.
        .route("/signal-token/displays", get(display::list_displays))
        .route(
            "/signal-token/displays/{project_id}/{node}",
            get(display::read_display),
        )
        .route(
            "/signal-token/displays/{project_id}/{node}/action",
            // Capped like every other body this surface accepts: a press
            // carries a small payload at most, and the caller is on the
            // open internet.
            post(display::press_display).layer(DefaultBodyLimit::max(PUBLIC_FIRE_BODY_LIMIT)),
        )
        // A page's upload through a slot, passed on to the broker, which
        // holds each verb to the slot in its body (`files_door::upload`).
        .route(
            "/files/upload/{verb}",
            post(crate::files_door::upload).layer(DefaultBodyLimit::max(PUBLIC_FIRE_BODY_LIMIT)),
        )
        .route(FILES_DOOR_PATH, files_door())
        // The root page, so a person checking the address sees
        // something deliberate.
        .route("/", get(public_page::index))
        .route("/index.html", get(public_page::index))
        .route("/logo.png", get(public_page::logo))
        // A live call at the install's shared address
        // (`/connect/<tenant>/<project>/<path>`): the address names the
        // project, and the call is passed on to its workers, whose door
        // checks it (`live_relay`). ANY method:
        // a WS handshake is a GET and a route serves whatever verbs it
        // declared; the handler answers 405 itself. `/connect/*` is more
        // specific than the catch-all, so it never falls through to
        // fire_public_entry.
        .route(
            "/connect/{*path}",
            any(signal::connect_live).layer(DefaultBodyLimit::max(PUBLIC_FIRE_BODY_LIMIT)),
        )
        // The public endpoints of infra nodes (`Expose::Public`).
        .route(&format!("{}/{{project}}/{{instance}}", crate::infra_door::INFRA_PREFIX), any(crate::infra_door::forward))
        .route(&format!("{}/{{project}}/{{instance}}/{{*rest}}", crate::infra_door::INFRA_PREFIX), any(crate::infra_door::forward))
        // The OAuth callback door: the provider redirects the user's
        // browser here after consent. No tenant bearer rides a
        // provider redirect; the state nonce's pending row (minted by
        // the tenant-authenticated begin) is what authenticates the
        // completion. More specific than the catch-all below.
        .route("/access/oauth/callback", get(access::oauth_callback))
        // The picker doors: the weft-served chooser page (opened in
        // the user's browser, like the consent) and its result post.
        // Like the OAuth callback, the state nonce's parked session
        // (minted by the tenant-authenticated begin) is what
        // authenticates them.
        .route("/access/picker/{state}", get(access::picker_page))
        .route("/access/picker/{state}/result", post(access::picker_result))
        // The public events receiver: providers deliver the pushes
        // serving dial-in event subscriptions here. More specific
        // than the catch-all; verification happens broker-side, so
        // this stays on the public surface with no gate of its own.
        .route(
            "/events/{service}/{topic}",
            post(provider_events::receive_event)
                .layer(DefaultBodyLimit::max(PUBLIC_FIRE_BODY_LIMIT)),
        )
        // Catch-all PublicEntry route: external HTTP fires land
        // here when no more-specific route matches. The handler
        // looks up the signal row by `mount_path` (an open entry
        // only; a connection-gated one is a live route served at
        // `/connect/...`), then hands the event to `signal::take_event`.
        // Public-entry signals fire via this route. Any other method is
        // answered 404, like a path nothing serves: this route matches
        // every path, so a 405 would tell a caller asking for an endpoint
        // the install does not have (a CLI asking an older weft) that it
        // exists for POST.
        .route(
            "/{*mount_path}",
            post(signal::fire_public_entry)
                .layer(DefaultBodyLimit::max(PUBLIC_FIRE_BODY_LIMIT))
                .fallback(|| async { axum::http::StatusCode::NOT_FOUND }),
        )
        .layer(axum::middleware::from_fn_with_state(state, guard_token_doors))
        .layer(permissive_cors())
}

/// The outside doors' browser-origin policy: any origin, any method, any
/// header. Every door checks its own credential (a token in the path, a
/// bearer signal token), so the caller's origin does not matter, and its
/// callers include the browser extension (`moz-extension://<id>`,
/// `chrome-extension://<id>`, which no allowlist can enumerate) and the
/// pages a person's frontend serves.
///
/// Every response header is exposed to the page (a live route's program
/// sets its own), and a preflight is remembered for a day, so a page
/// calling a route asks once rather than before every call.
fn permissive_cors() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any)
        .expose_headers(tower_http::cors::Any)
        .max_age(std::time::Duration::from_secs(24 * 3600))
}

/// Where the files door answers.
// SYNC: the files door <-> crates/weft-platform-traits/src/object_store.rs FILES_DOOR
const FILES_DOOR_PATH: &str = "/files/{*path}";

/// The files door: a link to a stored file that goes through the install,
/// passed on to the store unchanged; the link's signature is the
/// credential (`files_door::forward`). The bytes stream through, so the
/// body limit is the door's own: a PUT says its length, at most what a
/// store takes in one piece.
fn files_door() -> axum::routing::MethodRouter<DispatcherState> {
    any(crate::files_door::forward).layer(DefaultBodyLimit::disable())
}

/// The files door alone, for the port a local install's containers reach
/// the machine on (its internal port): a project's own container opens a
/// file through the link made for it (`LinkReach::Container`) there,
/// since inside a container `127.0.0.1` is the container itself. Beside
/// the roles' routes and outside their guard: the link's signature is the
/// credential, as on every other address the door answers.
pub fn container_files_router(state: DispatcherState) -> Router {
    Router::new().route(FILES_DOOR_PATH, files_door()).with_state(state)
}

/// Build the dispatcher router, bound to `state`: the management surface
/// and the outside doors (see [`core_routes`]).
pub fn router(state: DispatcherState) -> Router {
    let hops = TrustedHops(state.edge.trusted_proxy_hops.public);
    let routes = core_routes(state.clone())
        .layer(axum::middleware::from_fn_with_state(state.clone(), note_cli_version))
        // Every call may write, and a write made while the process's watch
        // is quiet would be announced to nobody, so each holds the watch
        // while it is answered. What the door sends elsewhere (a frontend)
        // does not come through here, and touches the database only to
        // route.
        .layer(axum::middleware::from_fn_with_state(state.signals.clone(), weft_task_store::pg_signal::keep_listening))
        // Probes carry no credential and touch nothing.
        .route("/health", get(|| async { "ok" }));
    with_hops(routes, hops).with_state(state)
}

/// Only the doors outside callers use (the public trigger surface, the
/// live and infra doors, the OAuth callback), for an address the open
/// internet reaches while the management API must stay off it (a local
/// install's tunnel).
pub fn outside_router(state: DispatcherState) -> Router {
    let hops = TrustedHops(state.edge.trusted_proxy_hops.outside);
    with_hops(marked_as_the_install(outside_caller_routes(state.clone())), hops).with_state(state)
}

/// The same doors as [`outside_router`], for the listener on the machine's
/// network address: its callers connect straight to it, so the caller is
/// the peer and no `X-Forwarded-For` entry is believed.
pub fn network_router(state: DispatcherState) -> Router {
    with_hops(marked_as_the_install(outside_caller_routes(state.clone())), TrustedHops(0)).with_state(state)
}

/// Mark every answer `router` gives as the install's own
/// ([`weft_core::install::INSTALL_ANSWER_HEADER`]), whatever answered it
/// (a handler, a layer refusing first, a program's route passed on), so a
/// caller reads an unmarked answer as the platform's in front of it.
/// [`router`]'s answers are marked by the door it is served behind
/// (`crate::door::router`), which answers some calls itself.
pub(crate) fn marked_as_the_install<S: Clone + Send + Sync + 'static>(router: Router<S>) -> Router<S> {
    router.layer(axum::middleware::map_response(|mut response: axum::response::Response| async move {
        response
            .headers_mut()
            .insert(weft_core::install::INSTALL_ANSWER_HEADER, axum::http::HeaderValue::from_static("1"));
        response
    }))
}

/// The address this request came in on (`weft_core::net::request_base_url`):
/// the base of a chooser page's address, which the client that asked opens
/// and the page itself posts back to. Refused when the request names no
/// usable host.
pub(crate) fn request_base(headers: &axum::http::HeaderMap) -> Result<String, (axum::http::StatusCode, String)> {
    weft_core::net::request_base_url(headers).map(|base| base.trim_end_matches('/').to_string()).ok_or_else(|| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            "the request has no valid Host (or X-Forwarded-Host / X-Forwarded-Prefix) header to build the page's address on"
                .to_string(),
        )
    })
}

/// Whether `e` failed because the database did not take the call: too
/// busy to give it a connection (`weft_task_store::db::busy`), or out of
/// reach (`weft_task_store::db::database_unreachable`), anywhere in its
/// chain. Nothing of the call itself is to blame, and the same call goes
/// through once the database answers. A call that got no connection is
/// said as what kept it (`weft_task_store::throttle::no_connection`: a
/// busy database as capacity, an unreachable one as unreachable), so
/// every wait on the database that goes through here is in the log, at
/// most once a minute.
/// The one place that decides it, for an answer and for a wait alike.
// SYNC: database_did_not_take <-> crates/weft-core/src/install.rs (asks_again)
pub(crate) fn database_did_not_take(e: &anyhow::Error) -> bool {
    if !(weft_task_store::db::database_busy(e) || weft_task_store::db::database_unreachable(e)) {
        return false;
    }
    if let Some(why) = weft_task_store::db::database_no_connection(e) {
        weft_task_store::throttle::no_connection(why);
    }
    true
}

/// The answer to a call that failed doing `what`, before anything of the
/// request it serves took effect: a `503` when the database did not take
/// it ([`database_did_not_take`]), which the caller sends again as it is
/// (`weft_core::install::asks_again`), a `500` naming the failure
/// otherwise, which asking again would not fix. Only a call nothing of
/// its request ran before (a read, or a request's first write) is
/// answered through here: a `503` says nothing ran.
// SYNC: unavailable_or_internal <-> crates/weft-core/src/install.rs (asks_again)
pub(crate) fn unavailable_or_internal(what: &str, e: anyhow::Error) -> (axum::http::StatusCode, String) {
    if let Some(answer) = not_taken(what, &e) {
        return answer;
    }
    tracing::error!(target: "weft_dispatcher::api", error = %format!("{e:#}"), "could not {what}");
    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("could not {what}: {e:#}"))
}

/// A failed access store call (`weft_access_store`: connections, instance
/// values, install picks, connect and picker sessions) as every dispatcher
/// surface answers it: a `503` when the database did not take it, decided
/// and logged as [`unavailable_or_internal`] does, under the same promise
/// (nothing of the request it serves took effect before it); anything else
/// through the store's own mapping (`weft_access_store::client_error`: a
/// refusal the store names keeps its status, the rest is an opaque `500`),
/// so the dispatcher and the broker answer a store failure alike. Every call
/// site today is a read, the request's one statement, or a transaction
/// that rolls back whole on the failure. A call that committed something
/// before the store failed must never answer `503` through here.
// SYNC: access_store_error <-> crates/weft-broker/src/handlers.rs (store_err), crates/weft-core/src/install.rs (asks_again)
pub(crate) fn access_store_error(e: anyhow::Error) -> (axum::http::StatusCode, String) {
    if let Some(answer) = not_taken("reach its database", &e) {
        return answer;
    }
    let (status, message) = weft_access_store::client_error(e);
    (axum::http::StatusCode::from_u16(status).expect("store status codes are valid"), message)
}

/// `answer` for a request part of which had already taken effect before
/// it failed: a `503` (a later step the database did not take) becomes a
/// `500`, since a `503` says nothing of the request ran and the caller
/// would send it again as it is. Every other answer is kept.
pub(crate) fn after_writes((status, message): (axum::http::StatusCode, String)) -> (axum::http::StatusCode, String) {
    if status != axum::http::StatusCode::SERVICE_UNAVAILABLE {
        return (status, message);
    }
    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("{message} (part of this request had already gone through before it)"))
}

/// The `503` for a call doing `what` that the database did not take
/// ([`database_did_not_take`]), logged; `None` for any other failure.
fn not_taken(what: &str, e: &anyhow::Error) -> Option<(axum::http::StatusCode, String)> {
    if !database_did_not_take(e) {
        return None;
    }
    tracing::warn!(target: "weft_dispatcher::api", error = %format!("{e:#}"), "could not {what}: the database is out of reach or too busy");
    Some((axum::http::StatusCode::SERVICE_UNAVAILABLE, format!("the install could not {what} right now; try again shortly")))
}

#[cfg(test)]
mod unavailable_tests {
    use axum::http::StatusCode;
    use weft_task_store::db;

    /// The call got no connection, for `why`.
    fn no_connection(why: db::NoConnection) -> anyhow::Error {
        db::Error::NoConnection { why, refused: None }.into()
    }

    /// A call the database did not take, too busy or out of reach, is a
    /// 503 (nothing ran, ask again); any other failure is a 500.
    #[test]
    fn only_a_database_that_did_not_take_the_call_answers_503() {
        let status = |e: anyhow::Error| super::unavailable_or_internal("read the build", e).0;
        // A pool whose every connection was in use for the whole wait, or a
        // database that refused a new one because it was full (53300).
        let full = no_connection(db::NoConnection::Busy);
        assert!(db::database_busy(&full), "a full database is too busy");
        assert_eq!(status(full.context("read the build")), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(status(no_connection(db::NoConnection::Unreachable)), StatusCode::SERVICE_UNAVAILABLE, "out of reach");
        let undecodable = anyhow::anyhow!("a version build state the ledger does not know: 'odd'");
        assert_eq!(status(undecodable), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(status(db::Error::NoRow.into()), StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// Once part of a request went through, a later step the database did
    /// not take is a 500, never a 503 (which says nothing ran); every
    /// other answer is kept.
    #[test]
    fn a_503_after_writes_becomes_a_500() {
        let busy = super::unavailable_or_internal("read the build", no_connection(db::NoConnection::Busy));
        assert_eq!(busy.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(super::after_writes(busy).0, StatusCode::INTERNAL_SERVER_ERROR);
        let refused = (StatusCode::CONFLICT, "in flight".to_string());
        assert_eq!(super::after_writes(refused.clone()), refused);
    }

    /// An access store call the database did not take, too busy or out of
    /// reach, is a 503 like any other; a refusal the store names keeps its
    /// status; anything else is the store's opaque 500.
    #[test]
    fn an_access_store_call_the_database_did_not_take_answers_503() {
        let status = |e: anyhow::Error| super::access_store_error(e).0;
        assert_eq!(status(no_connection(db::NoConnection::Busy).context("list grants")), StatusCode::SERVICE_UNAVAILABLE, "busy");
        assert_eq!(status(no_connection(db::NoConnection::Unreachable)), StatusCode::SERVICE_UNAVAILABLE, "out of reach");
        assert_eq!(status(weft_access_store::AccessError::NotFound.into()), StatusCode::NOT_FOUND);
        assert_eq!(status(weft_access_store::AccessError::Gone("expired".into()).into()), StatusCode::GONE);
        let (code, message) = super::access_store_error(db::Error::NoRow.into());
        assert_eq!((code, message.as_str()), (StatusCode::INTERNAL_SERVER_ERROR, "access store error"));
    }
}

#[cfg(test)]
mod token_door_tests {
    #[test]
    fn only_the_token_doors_count_refusals() {
        for door in ["/signal/abc", "/signal-token/signals", "/instance/i1/runs"] {
            assert!(super::is_token_door(door), "{door}");
        }
        // A live call's refusals are counted at the worker's door, and its
        // 401 or 403 may be the program's own answer.
        for other in ["/files/runtime/t/project/p/f", "/events/slack/message", "/local/hook", "/signals", "/connect/local/chat"] {
            assert!(!super::is_token_door(other), "{other}");
        }
    }
}

#[cfg(test)]
mod version_note_tests {
    use weft_core::install::WeftSource;

    #[test]
    fn only_a_cli_from_another_commit_of_a_known_source_is_told() {
        let source = WeftSource { repository: "me/weft".into(), commit: "0123456789abcdef0123".into() };
        assert_eq!(super::version_note(Some(&source), "0123456789abcdef0123"), None);
        assert_eq!(
            super::version_note(Some(&source), "fedcba9876543210fedc").as_deref(),
            Some(
                "this weft CLI was built from fedcba987654, but the install runs me/weft at 0123456789ab; \
                 if a command misbehaves, use the install's CLI"
            )
        );
        // A local install does not know its source, so it has nothing to compare.
        assert_eq!(super::version_note(None, "fedcba9876543210fedc"), None);
    }
}
