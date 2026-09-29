use std::sync::Arc;

use crate::authenticator::Authenticator;
use crate::events::EventBus;
use crate::journal::Journal;
use crate::listener::ListenerClient;
use crate::project_store::ProjectStore;
use crate::tenant::TenantRouter;

/// Top-level dispatcher state. Shared across HTTP handlers via
/// `axum::extract::State`. All fields are `Arc`-friendly.
#[derive(Clone)]
pub struct DispatcherState {
    /// This process instance's id: the holder of the leases, claims and
    /// builds it takes. Minted at boot; a sibling (another copy of the
    /// dispatcher) has its own.
    pub instance: String,
    pub journal: Arc<dyn Journal>,
    /// Direct Postgres pool handle. Owned here (not threaded through
    /// Journal) so lease management, EventBus pub/sub, and other
    /// DB-backed primitives can share connections without extending the
    /// Journal trait into a kitchen sink.
    pub pg_pool: sqlx::PgPool,
    /// Connections used ONLY to hold a project's transition lock.
    ///
    /// A Postgres advisory lock lives in a transaction, so holding one
    /// holds a connection, and that connection does no work: every query
    /// the locked operation makes takes a SECOND connection. Taken from
    /// the work pool, that is a deadlock waiting for a busy day. Sixteen
    /// `weft run`s at once on sixteen unrelated projects, with nothing to
    /// wait for, each grabbed a lock connection until the pool was gone,
    /// and then every one of them waited for a connection none of them
    /// would release until it got one. A pool of its own makes holding a
    /// lock cost nothing that doing the work needs.
    pub lock_pool: sqlx::PgPool,
    /// The process's one Postgres `LISTEN` connection, on (at least)
    /// every channel in [`crate::app::DISPATCHER_CHANNELS`]. Every loop
    /// and request that waits on a row sleeps on it
    /// (`weft_task_store::drain`, the picker, command waits) instead of
    /// polling.
    pub signals: Arc<weft_task_store::pg_signal::PgSignalWatch>,
    /// What the nodes on the graphs open against this dispatcher are
    /// showing, looked at only while an editor watches (see
    /// `display_feeds`).
    pub displays: Arc<crate::display_feeds::DisplayFeeds>,
    /// Where the project's workers run and how to reach them.
    pub runner: Arc<dyn weft_platform_traits::Runner>,
    /// Where the projects' infrastructure runs: read here for its logs
    /// (the supervisor is what changes it).
    pub host: Arc<dyn weft_platform_traits::InfraHost>,
    /// The worker settings every project starts from.
    pub worker_defaults: weft_platform_traits::WorkerSettings,
    /// Builds a project version inside the install (`crate::build`): the
    /// one way a project's program and images come to exist.
    pub builder: Arc<crate::build::VersionBuilder>,
    /// What the install tells about itself (`GET /install`).
    pub install_info: weft_core::install::InstallInfo,
    pub projects: ProjectStore,
    /// Which triggers listen, per trigger per owner
    /// (`crate::activation_store`).
    pub activations: crate::activation_store::ActivationStore,
    /// The version tree (`crate::versions`): every version a project has
    /// been, every run under one, and head.
    pub versions: crate::versions::VersionStore,
    pub events: EventBus,
    /// The listener role: registers signals, processes fires.
    pub listener: ListenerClient,
    /// Authenticates a user-facing request to the tenant making it. The
    /// default returns `local` for every request (no token); a
    /// token-verifying impl reads the caller's signed token.
    pub authenticator: Arc<dyn Authenticator>,
    /// Resolves the owning tenant for a given project. The default returns
    /// `local`.
    pub tenant_router: Arc<dyn TenantRouter>,
    /// Frees a deleted project's stored data, run as the project is
    /// removed (before the project row is dropped). Canonical doc on the
    /// `ProjectReclaimer` trait in `reclaim.rs`.
    pub project_reclaimer: Arc<dyn crate::reclaim::ProjectReclaimer>,
    /// The STABLE base URL people and editors reach this install at.
    pub public_base_url: String,
    /// An ADDITIONAL address the open internet reaches this install at (a
    /// tunnel's), when one exists. Never a replacement for the base:
    /// local surfaces (the OAuth callback shown to the operator, storage
    /// links) stay on the stable base, and only internet-facing surfaces
    /// (activation URLs, event pushes) prefer this one via
    /// [`DispatcherState::external_base_url`].
    pub internet_url: Option<String>,
    /// What the install allows at its public edge (trusted proxy hops,
    /// the token-guessing bound); see `entry_limits`.
    pub edge: crate::entry_limits::EdgeConfig,
    /// The broker's admin surface, which the dispatcher fronts for the
    /// CLI's `weft files` verbs (the broker owns the runtime-file bucket
    /// and its metadata; the dispatcher never touches bytes).
    pub broker: crate::role_client::RoleClient,
    /// The process's one HTTP client: the role clients above, an infra
    /// unit's `/live` and `/action`, and a worker's `/_weft/...`. Follows
    /// no redirect (see `app.rs`): every peer answers in place.
    pub http: reqwest::Client,
    /// HMAC secret the dispatcher signs live-caller routing tickets with
    /// (the worker verifies with the same secret).
    pub caller_token_secret: Arc<Vec<u8>>,
    /// Wakes a role that may be scaled to zero when work waits for it.
    pub kick: Arc<dyn weft_platform_traits::Kick>,
}

impl DispatcherState {
    /// The base for URLs handed to OUTSIDE callers (webhook activation
    /// URLs, addresses a provider posts events to): the additional
    /// internet address when one exists, the stable base otherwise.
    pub fn external_base_url(&self) -> &str {
        self.internet_url.as_deref().unwrap_or(&self.public_base_url)
    }
}
