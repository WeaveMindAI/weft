use std::sync::Arc;

use crate::authenticator::Authenticator;
use crate::events::EventBus;
use crate::listener::ListenerClient;
use crate::project_store::ProjectStore;
use crate::store::{BakeStore, RunStore, SignalStore, TokenStore};
use crate::tenant::TenantRouter;

/// Top-level dispatcher state. Shared across HTTP handlers via
/// `axum::extract::State`. All fields are `Arc`-friendly.
#[derive(Clone)]
pub struct DispatcherState {
    /// This process's replica id: the holder of the leases, claims and
    /// builds it takes. Minted at boot; a sibling replica (another copy
    /// of the dispatcher) has its own.
    pub replica: String,
    /// The runs: their rows and records (`crate::store`).
    pub runs: Arc<dyn RunStore>,
    /// The signals: what wakes a trigger, what a parked run waits on.
    pub registrations: Arc<dyn SignalStore>,
    /// The callers' tokens.
    pub tokens: Arc<dyn TokenStore>,
    /// The trigger setups and what each captured.
    pub bakes: Arc<dyn BakeStore>,
    /// Direct Postgres pool handle. Owned here (not threaded through the
    /// stores) so lease management, EventBus pub/sub, and other
    /// DB-backed primitives can share connections without growing a
    /// store into a kitchen sink.
    pub pg_pool: weft_task_store::db::Pool,
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
    pub lock_pool: weft_task_store::db::Pool,
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
    /// What the machine the workers run on lacks of what a program's nodes
    /// need (`crate::needs`).
    pub needs: Arc<dyn weft_platform_traits::MachineNeeds>,
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
    /// What tells the holders the held set or a connection's access
    /// changed (`crate::holders`).
    pub holder_news: Arc<dyn weft_platform_traits::HolderNews>,
    /// The worker settings every project starts from.
    pub worker_defaults: weft_platform_traits::WorkerSettings,
    /// How long weft's own work may keep waiting before it is written down
    /// for `weft status`, in seconds at this install's pace: the install's
    /// alert delay (`InstallConfig::alert_delay`; a run its workers keep
    /// answering busy, `crate::delivery::busy`).
    pub alert_after_secs: i64,
    /// Builds a project version inside the install (`crate::build`): the
    /// one way a project's program and images come to exist.
    pub builder: Arc<crate::build::VersionBuilder>,
    /// What the install tells about itself (`GET /install`), as its
    /// config says it: its `public_url` is the platform's own address,
    /// and `GET /install` puts [`Self::address`] there.
    pub install_info: weft_core::install::InstallInfo,
    pub projects: ProjectStore,
    /// Which triggers listen, per trigger per owner
    /// (`crate::activation_store`).
    pub activations: crate::activation_store::ActivationStore,
    /// The version tree (`crate::versions`): every version a project has
    /// been, every run under one, and head.
    pub versions: crate::versions::VersionStore,
    /// Where each project's code is being edited (`crate::working_copies`).
    pub working_copies: crate::working_copies::WorkingCopyStore,
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
    /// The address the install hands out in its links
    /// (`weft_task_store::install_address`): its domain once that
    /// domain's certificate is issued, else the platform's own; and where
    /// the open internet reaches it, read each time it is used (a quick
    /// tunnel opened again comes back under a new name). Local surfaces
    /// (the OAuth callback shown to the operator) stay on the stable
    /// address, and only internet-facing ones (activation URLs, event
    /// pushes) prefer the internet one via
    /// [`DispatcherState::external_base_url`].
    pub address: Arc<weft_task_store::install_address::InstallAddress>,
    /// Where the files door passes requests on to (`crate::files_door`):
    /// `None` on a store the open internet reaches itself, whose links
    /// never go through the install.
    pub files: Option<crate::files_door::FilesTarget>,
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
    /// Programs already read and parsed, by project and definition hash
    /// (see [`DispatcherState::program`]): a definition never changes
    /// under its hash, so a busy route stops reading and parsing its
    /// program on every call. Bounded by what they weigh
    /// ([`PROGRAMS_JSON_BYTES`]), never by how many: an entry is anything
    /// from a trigger's cut of a few kilobytes to a whole program of
    /// megabytes (every build registers the program under a new hash, and
    /// every run reads its own cut), so a count bounds nothing. The store's
    /// own (`PostgresStore::programs`), which a search's pass reads through
    /// too, so the bound is the whole process's.
    pub programs: Programs,
    /// The rows every live call reads, held in memory and read again when
    /// they change (`crate::held`).
    pub held: Arc<crate::held::Held>,
    /// Each project's own port, on a local install
    /// (`crate::project_ports`); `None` on a platform that gives none.
    pub project_ports: Option<Arc<crate::project_ports::ProjectPorts>>,
    /// The looks of the dispatcher's loops, when it scales to zero: a loop
    /// that makes work another loop must look at by a moment books that
    /// look (`crate::lapses`). `None` on a machine, where every loop looks
    /// on its own.
    pub booking: Option<weft_task_store::drain::Booking>,
    /// Whether this copy of the dispatcher is stopping
    /// (`weft_task_store::stopping`): once it is, the picker claims nothing
    /// more and every event stream ends, so its followers open their
    /// stream again on a running copy.
    pub stopping: weft_task_store::stopping::Stopping,
}

/// Parsed programs by project and definition hash, bounded by what they
/// weigh ([`programs_cache`]).
pub type Programs = Arc<weft_core::content_cache::ContentCache<(uuid::Uuid, String), weft_core::ProjectDefinition>>;

/// An empty [`Programs`], weighed by [`program_json_bytes`] and bounded by
/// [`PROGRAMS_JSON_BYTES`].
pub fn programs_cache() -> Programs {
    Arc::new(weft_core::content_cache::ContentCache::weighed(PROGRAMS_JSON_BYTES, program_json_bytes))
}

/// How many bytes of JSON the programs a dispatcher keeps parsed
/// ([`DispatcherState::programs`]) may weigh together
/// ([`program_json_bytes`]). A parsed program holds about three times its
/// JSON (`tests/resync_memory.rs` measures it), so they hold about 90 MiB
/// at most, inside the 512Mi a dispatcher is never given less than.
pub const PROGRAMS_JSON_BYTES: usize = 32 * 1024 * 1024;

/// What `program` weighs in [`DispatcherState::programs`]: the length of
/// its JSON, counted as it is written out, nothing kept. Run once per
/// program read from the store, never on a cache hit.
pub fn program_json_bytes(program: &weft_core::ProjectDefinition) -> usize {
    struct Counted(usize);
    impl std::io::Write for Counted {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counted = Counted(0);
    serde_json::to_writer(&mut counted, program).expect("a program read from its JSON writes back as JSON");
    counted.0
}

/// The base for outside URLs of one answer
/// ([`DispatcherState::external_base`]): read the first time a path is put
/// on it, then kept for the rest of that answer only, so one answer hands
/// out every address under the same base.
pub struct ExternalBase<'a> {
    state: &'a DispatcherState,
    base: tokio::sync::OnceCell<String>,
}

impl ExternalBase<'_> {
    /// `path` (an address's path on this install) as outside callers reach
    /// it. The first call reads the base, and fails as
    /// [`DispatcherState::external_base_url`] does.
    pub async fn url(&self, path: &str) -> anyhow::Result<String> {
        let base = self.base.get_or_try_init(|| self.state.external_base_url()).await?;
        Ok(weft_core::infra::public_url(base, path))
    }
}

impl DispatcherState {
    /// The address the open internet reaches this install at right now,
    /// `None` when it reaches none. A quick tunnel that does not say its
    /// address is an error naming it.
    pub async fn internet_url(&self) -> anyhow::Result<Option<String>> {
        self.address.internet().await
    }

    /// The base for URLs handed to OUTSIDE callers (webhook activation
    /// URLs, addresses a provider posts events to): the internet address
    /// as it is now when there is one, the stable address otherwise.
    pub async fn external_base_url(&self) -> anyhow::Result<String> {
        self.address.outside().await
    }

    /// The base for outside URLs ([`DispatcherState::external_base_url`]),
    /// read only when an answer first puts a path on it: an answer that
    /// hands out no outside address (a cron-only activation, a run whose
    /// waits have no resume link) never waits on a tunnel, and fails on
    /// none that is down or still connecting.
    pub fn external_base(&self) -> ExternalBase<'_> {
        ExternalBase { state: self, base: tokio::sync::OnceCell::new() }
    }

    /// The program `project` recorded under `hash`, parsed; `None` when
    /// no such version was ever recorded. THE way the dispatcher reads a
    /// recorded program, kept once read. A recorded program that no longer
    /// reads is a `weft_core::stored::DecodeError` naming it, which a
    /// caller that shows runs tells apart from the store failing.
    pub async fn program(
        &self,
        project: uuid::Uuid,
        hash: &str,
    ) -> anyhow::Result<Option<Arc<weft_core::ProjectDefinition>>> {
        if let Some(program) = self.programs.get(&(project, hash.to_string())) {
            return Ok(Some(program));
        }
        let Some(program) = self.projects.program(project, hash).await? else { return Ok(None) };
        let program = Arc::new(program);
        self.programs.put((project, hash.to_string()), program.clone());
        Ok(Some(program))
    }

    /// The program identity of the run whose rows are `events`
    /// ([`Self::registered_identity`] of what its birth names).
    pub async fn run_program_identity(
        &self,
        events: &[weft_journal::RunEvent],
    ) -> anyhow::Result<weft_core::project::hash::ProgramIdentity> {
        let Some(weft_journal::RunEvent::RunStarted {
            run_id,
            project_id,
            definition_hash: Some(definition_hash),
            cut_from,
            binary_hash: Some(binary_hash),
            ..
        }) = events.first()
        else {
            anyhow::bail!("the run's history does not start with a birth naming its program");
        };
        self.registered_identity(*run_id, *project_id, definition_hash, cut_from.as_deref(), binary_hash).await
    }

    /// The identity of the registered program run `run_id` of `project_id`
    /// ran against, its birth naming its program `definition_hash` (cut
    /// from `cut_from`, for a run of a cut) on worker `binary_hash`: read
    /// from the project's stored code for that binary. A run never copies
    /// the implementation fingerprints, so this is the way back to them.
    /// A run of a cut answers with the registered program it was cut
    /// from: what its bakes are for and what a seed holds its results
    /// against.
    pub async fn registered_identity(
        &self,
        run_id: weft_core::RunId,
        project_id: uuid::Uuid,
        definition_hash: &str,
        cut_from: Option<&str>,
        binary_hash: &str,
    ) -> anyhow::Result<weft_core::project::hash::ProgramIdentity> {
        let registered = cut_from.unwrap_or(definition_hash);
        self.projects
            .program_identity(project_id, registered, binary_hash)
            .await?
            .ok_or_else(|| anyhow::anyhow!("run {run_id} ran on worker {binary_hash}, which its project no longer records"))
    }
}
