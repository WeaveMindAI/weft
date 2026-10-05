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
    /// This process's replica id: the holder of the leases, claims and
    /// builds it takes. Minted at boot; a sibling replica (another copy
    /// of the dispatcher) has its own.
    pub replica: String,
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
    /// Where a project's frontend runs, when the install hosts it
    /// (`crate::frontends`).
    pub frontends: Arc<dyn weft_platform_traits::FrontendHosting>,
    /// The door in front of the install's domains (`crate::domains`).
    pub domains: Arc<dyn weft_platform_traits::DomainHosting>,
    /// How many holders run (`crate::holders`), and how many signals each
    /// takes.
    pub holder_pool: Arc<dyn weft_platform_traits::HolderPool>,
    pub holder_settings: weft_platform_traits::config::HolderSettings,
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
    /// The dispatcher's line to the broker (`weft_broker_client::line`): the
    /// admin and verify calls it forwards (`crate::broker_admin`), one of
    /// which stands in front of every gated live call, ride it instead of
    /// a request each.
    pub broker_line: weft_broker_client::BrokerLink,
    /// The process's one HTTP client: the role clients above, an infra
    /// unit's `/live` and `/action`, and a worker's `/_weft/...`. Follows
    /// no redirect (see `app.rs`): every peer answers in place.
    pub http: reqwest::Client,
    /// HMAC secret the dispatcher signs live-caller routing tickets with
    /// (the worker verifies with the same secret).
    pub caller_token_secret: Arc<Vec<u8>>,
    /// Programs already read and parsed, by project and definition hash
    /// (see [`DispatcherState::program`]): a definition never changes
    /// under its hash, so a busy route stops reading and parsing its
    /// program on every call.
    pub programs: Arc<weft_core::content_cache::ContentCache<(uuid::Uuid, String), weft_core::ProjectDefinition>>,
    /// The rows every live call reads, held in memory and read again when
    /// they change (`crate::held`).
    pub held: Arc<crate::held::Held>,
}

impl DispatcherState {
    /// The base for URLs handed to OUTSIDE callers (webhook activation
    /// URLs, addresses a provider posts events to): the additional
    /// internet address when one exists, the stable base otherwise.
    pub fn external_base_url(&self) -> &str {
        self.internet_url.as_deref().unwrap_or(&self.public_base_url)
    }

    /// The program `project` recorded under `hash`, parsed; `None` when
    /// no such version was ever recorded. THE way the dispatcher reads a
    /// recorded program. A recorded program that no longer parses is an
    /// [`UnreadableProgram`] error, which a caller that shows runs tells
    /// apart from the store failing.
    pub async fn program(
        &self,
        project: uuid::Uuid,
        hash: &str,
    ) -> anyhow::Result<Option<Arc<weft_core::ProjectDefinition>>> {
        if let Some(program) = self.programs.get(&(project, hash.to_string())) {
            return Ok(Some(program));
        }
        let Some(json) = self.projects.definition_for_hash(project, hash).await? else { return Ok(None) };
        let program: Arc<weft_core::ProjectDefinition> =
            Arc::new(serde_json::from_str(&json).map_err(|e| UnreadableProgram(e.to_string()))?);
        self.programs.put((project, hash.to_string()), program.clone());
        Ok(Some(program))
    }
}

/// A recorded program that no longer parses as a definition, and why.
#[derive(Debug)]
pub struct UnreadableProgram(pub String);

impl std::fmt::Display for UnreadableProgram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the recorded program no longer reads: {}", self.0)
    }
}

impl std::error::Error for UnreadableProgram {}
