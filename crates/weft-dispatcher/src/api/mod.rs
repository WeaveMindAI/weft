//! HTTP API surface. The CLI, the VS Code extension, the browser
//! extension, and end users (via webhook URLs minted by the
//! listener) all talk to this surface.
//!
//! Route categories:
//! - `/projects/*`: project registration, run, stop, logs.
//! - `/executions/*`: execution state queries and control.
//! - `/events/*`: SSE streams for project and execution state.
//! - `/signal-token*`: signal-token minting + token-scoped signal access.
//! - `/dashboard/*`: the ops dashboard UI (static assets + SSE).
//!
//! The dispatcher does NO node-aware work: parse, validate, and
//! catalog introspection are client-side (the CLI reads the project's
//! `nodes/`), because the dispatcher has no access to those nodes.

use axum::{extract::DefaultBodyLimit, routing::{get, post}, Router};
use tower_http::cors::CorsLayer;

use crate::state::DispatcherState;

/// Max request body on the UNAUTHENTICATED public inbound doors (`/signal/{token}`
/// fire, `/connect/{*path}`, the `/{*mount_path}` public-entry catch-all). These
/// accept a JSON payload from anyone who knows/guesses the URL; without a cap a
/// caller could push arbitrarily large nested JSON (a parse-amplification vector,
/// and it accumulates into a parked project's `parked_fires`). Sized to real
/// webhook payloads. Trusted routes keep axum's default limit.
const PUBLIC_FIRE_BODY_LIMIT: usize = 256 * 1024;

pub mod project;
pub(crate) mod execution;
mod events;
mod signal_token;
mod signal_token_names;
mod dashboard;
mod infra;
pub(crate) mod signal;
pub mod storage;

/// The dispatcher routes, not yet bound to state. Additional routes can be
/// `.merge`d onto this `Router<DispatcherState>` before binding state (so the
/// merged routes resolve against the same `DispatcherState`), then `.with_state(state)`.
///
/// `cors` is a COMPOSITION-TIME input, never a baked-in constant, so the right
/// browser-origin policy for the tenant/admin surface is chosen where the router
/// is assembled: [`permissive_cors`] when browsers hit this surface directly
/// (localhost-only deployments), or a tight policy (e.g. `CorsLayer::new()`,
/// which emits no CORS headers at all) when browsers only arrive through a
/// same-origin proxy. The outside-caller surface ([`outside_caller_routes`]:
/// fire URLs, signal tokens, live connect, public mounts) is exempt: its
/// callers are cross-origin by design, so it always carries [`permissive_cors`].
pub fn core_routes(cors: CorsLayer) -> Router<DispatcherState> {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/projects", get(project::list))
        .route("/projects/{id}", get(project::get).delete(project::remove))
        .route("/projects/{id}/run", post(project::run))
        .route("/projects/{id}/status", get(project::status))
        .route("/projects/{id}/executions/latest", get(execution::latest_for_project))
        .route("/projects/{id}/activate", post(project::activate))
        // Cancel an in-flight activate (status=Activating). Wipes
        // every signal row registered so far, cancels the
        // TriggerSetup color, CASes status to Inactive.
        .route("/projects/{id}/cancel-activate", post(project::cancel_activate))
        // Cancel an in-flight build (transition=building). CASes the
        // transition to cancelling_build (the durable cross-Pod signal
        // the build gate polls) + interrupts the local builder job.
        .route("/projects/{id}/cancel-build", post(project::cancel_build))
        .route("/projects/{id}/deactivate", post(project::deactivate))
        // While the project is in `deactivating`, this endpoint
        // cancels every running, non-suspended execution; the
        // journal-bridge drain-watcher then CASes status to
        // `inactive` (the lifecycle target the original deactivate
        // already wrote to the row stays in place).
        .route("/projects/{id}/cancel-running", post(project::cancel_running))
        .route("/projects/{id}/resync", post(project::resync))
        // Unified subworkflow endpoint for Start / Restart / Upgrade.
        // All three CLI verbs POST here; the dispatcher uses the
        // resolved-spec-hash to decide skip-vs-apply per node.
        .route("/projects/{id}/infra/sync", post(infra::sync))
        .route("/projects/{id}/infra/stop", post(infra::stop))
        .route("/projects/{id}/infra/terminate", post(infra::terminate))
        // Cancel in-flight infra work: halt claimed lifecycle commands
        // between kubectl steps, cancel unclaimed ones outright, and
        // interrupt the InfraSetup provisioning execution. HALT, not
        // rollback: per-node partial state stays visible.
        .route("/projects/{id}/infra/cancel", post(infra::cancel))
        // Per-node verbs for partial-state recovery.
        .route("/projects/{id}/infra/nodes/{node_id}/stop", post(infra::stop_node))
        .route("/projects/{id}/infra/nodes/{node_id}/terminate", post(infra::terminate_node))
        .route("/projects/{id}/infra/status", get(infra::status))
        .route("/projects/{id}/infra/commands/{cmd_id}", get(infra::command_status))
        .route("/projects/{id}/infra/nodes/{node_id}/live", get(infra::live))
        .route("/executions/{color}/cancel", post(execution::cancel))
        .route("/executions/{color}/logs", get(execution::list_logs))
        .route("/executions/{color}/replay", get(execution::replay))
        .route(
            "/executions/{color}",
            get(execution::get).delete(execution::delete_execution),
        )
        .route("/executions", get(execution::list_executions))
        .route("/events/project/{id}", get(events::project_stream))
        .route("/events/execution/{color}", get(events::execution_stream))
        // Token administration (tenant-authenticated): mint returns the value
        // ONCE, list returns metadata only, revoke addresses the id.
        .route(
            "/signal-tokens",
            get(signal_token::list_tokens).post(signal_token::mint_token),
        )
        .route("/signal-tokens/{id}", axum::routing::delete(signal_token::revoke_token))
        .route("/", get(dashboard::serve_root))
        .route("/dashboard", get(dashboard::serve_root))
        .route("/dashboard/{*path}", get(dashboard::serve))
        .route("/listener/inspect", get(signal::listener_inspect))
        // Storage plane: the `weft files` CLI surface (list, usage, download
        // handshake, remove). The dispatcher resolves the acting tenant and
        // proxies each verb to the broker (which owns the bucket + metadata);
        // bulk bytes never flow through the dispatcher.
        .route("/storage/files", get(storage::list_files).delete(storage::remove))
        .route("/storage/files/download", post(storage::download))
        .route("/storage/public-base", get(storage::public_base))
        .route("/storage/usage", get(storage::usage))
        // Asset publication (the pre-build sync): the same multipart contract
        // the worker uses, proxied to the broker's admin upload surface; part
        // URLs are caller-facing so bytes go straight to the bucket.
        .route("/storage/upload/begin", post(storage::upload_begin))
        // The pre-build asset sync's diff input: the project's published assets.
        .route("/storage/assets/list", post(storage::assets_list))
        .route("/storage/upload/parts", post(storage::upload_parts))
        .route("/storage/upload/part-done", post(storage::upload_part_done))
        .route("/storage/upload/complete", post(storage::upload_complete))
        .route("/storage/upload/resume", post(storage::upload_resume))
        .route("/storage/upload/abort", post(storage::upload_abort))
        // Inspector proxy: project-scoped read of signal display
        // info (mount_path, plaintext key while listener still
        // holds it, etc). Project-token gated.
        .route(
            "/projects/{id}/signals/{node_id}/display",
            get(signal::display_signal),
        )
        // Inspector proxy: project-scoped action invocation. The
        // listener's kind impl owns the action's payload schema.
        // Project-token gated.
        .route(
            "/projects/{id}/signals/{node_id}/action",
            post(signal::action_signal),
        )
        .layer(cors)
        .merge(outside_caller_routes())
}

/// The outside-caller surface: routes whose callers are external by design
/// (a pasted fire URL, a browser extension holding a signal token, a webhook
/// sender, a live caller opening a connection). Each request authenticates
/// itself (capability token in the path, bearer signal token, per-signal auth
/// gate), so the caller's ORIGIN is irrelevant and permissive CORS is part of
/// the surface contract, attached here rather than left to composition. The
/// composition-time `cors` on [`core_routes`] governs only the tenant/admin
/// surface.
fn outside_caller_routes() -> Router<DispatcherState> {
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
        .route("/signal-token/health", get(signal::signal_token_health))
        // Live caller connection handshake: an outside caller hits
        // `/connect/<path>` to open a held connection. The handler
        // authenticates, ensures a worker pod is up, starts a fresh
        // execution pinned to it, and points the caller at the gateway
        // URL for that pod (307 for HTTP, return-URL for WebSocket).
        // GET + POST: a WS handshake is a GET; an HTTP live request may
        // be any method, so accept both. `/connect/*` is more specific
        // than the catch-all, so it never falls through to
        // fire_public_entry.
        .route(
            "/connect/{*path}",
            get(signal::connect_live)
                .post(signal::connect_live)
                .layer(DefaultBodyLimit::max(PUBLIC_FIRE_BODY_LIMIT)),
        )
        // Catch-all PublicEntry route: external HTTP fires land
        // here when no more-specific route matches. The handler
        // looks up the signal row by `mount_path`, applies the
        // auth gate (api_key check, future schemes), then
        // forwards to dispatch_listener_outcome. Public-entry
        // signals fire via this route. Methods other than POST or
        // unmatched paths fall to axum's default 404.
        .route(
            "/{*mount_path}",
            post(signal::fire_public_entry).layer(DefaultBodyLimit::max(PUBLIC_FIRE_BODY_LIMIT)),
        )
        .layer(permissive_cors())
}

/// A permissive browser-origin policy: any origin, any method, any header. Right
/// for a localhost-only dispatcher whose browser callers include the extension
/// popup / task page (origins like `moz-extension://<id>` or
/// `chrome-extension://<id>`, which no allowlist can enumerate). A publicly
/// exposed surface wants a tight `CorsLayer` instead.
pub fn permissive_cors() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any)
}

/// The CLI-only door. `/projects/register` takes a pre-assembled
/// `ProjectDefinition` with locally-computed hashes + source: a developer
/// registers a project from a folder the CLI already BUILT on their machine.
/// Because it trusts caller-supplied definition + hashes, it is a TRUSTED-caller
/// door and must be mounted ONLY where every caller is trusted (`router` below).
/// It MUST NOT be exposed to untrusted browsers, which could otherwise register
/// an arbitrary definition + hashes bypassing any scaffold / build / quota checks.
pub fn cli_routes() -> Router<DispatcherState> {
    Router::new().route("/projects/register", post(project::register))
}

/// Build the trusted-caller dispatcher router: the core routes PLUS the CLI door,
/// bound to `state`. Assemblies that face untrusted browsers compose
/// `core_routes()` WITHOUT `cli_routes()`, so the trusted-caller register door is
/// never reachable there.
pub fn router(state: DispatcherState) -> Router {
    core_routes(permissive_cors()).merge(cli_routes()).with_state(state)
}
