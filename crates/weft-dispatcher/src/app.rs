//! The dispatcher application: its building blocks, which the runtime
//! (`weft-runtime`) composes with the other roles.
//!
//! The runtime runs the schema, builds the state from the install config
//! and the platform's implementations, registers the task executors, runs
//! the background loops (in a local install's one process, or once per
//! tick when the dispatcher is a service of its own), and serves the
//! router behind the door that routes by name (`crate::door`). Each
//! building block takes what it needs as plain construction input.

use std::sync::Arc;

use anyhow::Context;
use tracing::info;
use weft_platform_traits::config::{AuthMode, InstallConfig};
use weft_platform_traits::CoreRole;
use weft_task_store::drain::{DrainLoop, WakeOn};

use crate::authenticator::local_authenticator;
use crate::journal::postgres::PostgresJournal;
use crate::listener::ListenerClient;
use crate::role_client::RoleClient;
use crate::reclaim::{default_reclaimer, ProjectReclaimer};
use crate::tenant;
use crate::DispatcherState;

/// Apply every schema group the dispatcher owns against `pool`, the ONE
/// schema entry point: one guarded pass over [`ALL_GROUPS`] in dependency
/// order, so every pending migration across every group runs in one
/// global id order (the same order the agreement test replays).
/// Serialization across racing copies is the guard's own job:
/// `apply_groups` runs everything in one transaction under its advisory
/// lock. Runs BEFORE the journal or any store is constructed; none of them
/// applies schema on its own.
pub async fn apply_core_schema(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    // Dependency order: the `dispatcher_cursor` table group precedes the
    // `infra_event_bridge_cursor` seed that writes into it. The guard
    // refuses to run any group whose stamped fingerprint no longer matches
    // its DDL, naming the tables to drop.
    weft_task_store::apply_groups(pool, ALL_GROUPS)
        .await
        .context("apply core schema groups")
}

/// Every schema group the dispatcher owns, in dependency order, journal
/// first. One list answers "what is the dispatcher's schema": the boot
/// applies it whole, the migration generator writes its origins from it,
/// and the agreement test builds from it.
pub static ALL_GROUPS: &[&weft_task_store::SchemaGroup] = &[
    &crate::journal::postgres::GROUP,
    // The exclusive infra ownership leases.
    &crate::infra_owner::GROUP,
    &weft_task_store::tasks::GROUP,
    &weft_access_store::GROUP,
    &crate::infra_node::GROUP,
    &crate::infra_event::GROUP,
    &crate::infra_lifecycle_command::GROUP,
    &crate::journal_bridge::GROUP,
    &crate::infra_event_bridge::GROUP,
    // The durable terminate-sweep queue (no FK; the dispatcher owns it and
    // the reaper drains it by asking the broker to sweep a terminated
    // execution's files).
    &crate::storage::GROUP,
    // In the one list with everything else, so a mismatch anywhere
    // surfaces in ONE error naming every stale group together.
    &crate::project_store::GROUP,
    // The install's domains; a project's go with it.
    &crate::domains::GROUP,
    // A project's frontends; they go with it.
    &crate::frontends::GROUP,
    // Hangs off `project` (its rows cascade with the project).
    &crate::activation_store::GROUP,
    // The version tree hangs off `project` (its foreign keys and the head
    // columns), so it comes after.
    &crate::versions::GROUP,
    // The public edge's counters: per-minute buckets and the runs each
    // entry has going (no FK: a slot outlives nothing it names).
    &crate::entry_limits::GROUP,
    // The image builds running and done, by image ref (`build::ledger`).
    &crate::build::ledger::GROUP,
    // The wakes a local install has set and not yet delivered (a cloud
    // keeps them in its own queue; the table stays empty there).
    &weft_task_store::alarm::GROUP,
    // When each loop of a role that scales to zero next wants a look
    // (`weft_task_store::drain`); empty on a local install.
    &weft_task_store::drain::GROUP,
];

/// The construction-time policies threaded into `build_state`: who a
/// request authenticates as, which tenant owns a project, and how a
/// deleted project's data is reclaimed. Plain construction input, not an
/// injection bundle: nothing here is overridden later.
pub struct Defaults {
    pub authenticator: Arc<dyn crate::authenticator::Authenticator>,
    pub tenant_router: Arc<dyn crate::tenant::TenantRouter>,
    /// Frees a deleted project's stored data before its row is dropped.
    /// Canonical doc on the `ProjectReclaimer` trait in `reclaim.rs`.
    pub project_reclaimer: Arc<dyn ProjectReclaimer>,
}

impl Defaults {
    /// The install's policies for its auth mode: one tenant `local`, every
    /// request authenticated as it on a local install, by an operator key
    /// on a shared one.
    pub fn for_auth(auth: AuthMode) -> Self {
        let authenticator: Arc<dyn crate::authenticator::Authenticator> = match auth {
            AuthMode::Local => local_authenticator(),
            AuthMode::OperatorKeys => {
                Arc::new(crate::authenticator::OperatorKeyAuthenticator { tenant: crate::tenant::TenantId::local() })
            }
        };
        Self { authenticator, tenant_router: tenant::local_router(), project_reclaimer: default_reclaimer() }
    }
}

/// Every channel the dispatcher listens on. The process's one `LISTEN`
/// connection must hold them all.
pub const DISPATCHER_CHANNELS: &[&str] = &[
    weft_task_store::tasks::TASK_READY_CHANNEL,
    weft_task_store::terminal::TERMINAL_CHANNEL,
    weft_journal::EXEC_EVENT_CHANNEL,
    weft_journal::unrecorded::UNRECORDED_ENDED_CHANNEL,
    weft_broker_client::lifecycle_command::INFRA_COMMAND_CHANNEL,
    crate::infra_event_bridge::INFRA_EVENT_CHANNEL,
    crate::events::NOTIFY_CHANNEL,
    crate::reaper::PARKED_FIRE_CHANNEL,
    crate::reaper::STORAGE_SWEEP_CHANNEL,
    crate::display_feeds::LOOK_NOW_CHANNEL,
    crate::holders::HELD_SIGNALS_CHANNEL,
    crate::domains::DOMAINS_CHANNEL,
];

/// What the dispatcher is built from: the install's config and what the
/// runtime process shares among its roles.
pub struct DispatcherSettings<'a> {
    pub config: &'a InstallConfig,
    /// This process's replica id.
    pub replica: String,
    pub pool: sqlx::PgPool,
    /// A pool of its own for the project transition lock (see
    /// `DispatcherState::lock_pool`).
    pub lock_pool: sqlx::PgPool,
    /// The process's one `LISTEN` connection, on (at least)
    /// [`DISPATCHER_CHANNELS`].
    pub signals: Arc<weft_task_store::pg_signal::PgSignalWatch>,
    pub runner: Arc<dyn weft_platform_traits::Runner>,
    pub host: Arc<dyn weft_platform_traits::InfraHost>,
    pub images: Arc<dyn weft_platform_traits::ImageBuilder>,
    pub frontends: Arc<dyn weft_platform_traits::FrontendHosting>,
    pub domains: Arc<dyn weft_platform_traits::DomainHosting>,
    pub holder_pool: Arc<dyn weft_platform_traits::HolderPool>,
    /// The dispatcher's identity for its calls to the other roles.
    pub tokens: Arc<dyn weft_platform_traits::IdentityTokens>,
    /// Signs live-caller routing tickets (`WEFT_CALLER_TOKEN_SECRET`).
    pub caller_token_secret: Vec<u8>,
}

/// Build the dispatcher state.
pub async fn build_state(settings: DispatcherSettings<'_>, defaults: Defaults) -> anyhow::Result<DispatcherState> {
    let Defaults { authenticator, tenant_router, project_reclaimer } = defaults;
    let DispatcherSettings {
        config,
        replica,
        pool,
        lock_pool,
        signals,
        runner,
        host,
        images,
        frontends,
        domains,
        holder_pool,
        tokens,
        caller_token_secret,
    } = settings;
    for channel in DISPATCHER_CHANNELS {
        signals.require(channel)?;
    }
    anyhow::ensure!(
        !caller_token_secret.is_empty(),
        "WEFT_CALLER_TOKEN_SECRET is empty: live-caller tickets would verify under an empty key"
    );
    info!("dispatcher replica: {replica}");
    let journal = PostgresJournal::from_pool(pool.clone());
    let projects: crate::ProjectStore = Arc::new(crate::PostgresProjectStore::new(pool.clone()));
    let activations: crate::activation_store::ActivationStore =
        Arc::new(crate::activation_store::PostgresActivationStore::new(pool.clone()));
    let versions: crate::versions::VersionStore = Arc::new(crate::versions::PostgresVersionStore::new(pool.clone()));
    let event_bus = crate::EventBus::with_notify(pool.clone(), &signals)?;
    let displays = crate::display_feeds::DisplayFeeds::with_look_now(&signals)?;
    // The other roles as this dispatcher reaches them, from where its
    // own placement puts it (a local install's loopback is only its own
    // process's).
    let addresses = config.role_addresses(config.roles.of(CoreRole::Dispatcher).vantage());
    let builder = Arc::new(crate::build::VersionBuilder {
        bases: weft_compiler::worker_image::BaseImages {
            builder: config.build.builder_base_image.clone(),
            runtime: config.build.runtime_base_image.clone(),
        },
        images,
        pool: pool.clone(),
        compile_lanes: config.build.compile_lanes,
        poll_every: weft_core::time_scale::scaled(std::time::Duration::from_secs(2)),
        prunes: Default::default(),
        blobs: crate::build::blob_cache::BlobCache::in_temp_dir(),
    });
    // Every peer this client talks to (the broker and listener roles, an
    // infra unit's `/live` and `/action`, a worker) answers in place; a
    // redirect is a peer trying to send the dispatcher somewhere else, so
    // none is followed.
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("a default reqwest client builds");
    Ok(DispatcherState {
        replica,
        journal: Arc::new(journal),
        pg_pool: pool,
        lock_pool,
        signals,
        displays,
        runner,
        host,
        frontends,
        domains,
        holder_pool,
        holder_settings: config.holders,
        worker_defaults: config.workers.clone(),
        builder,
        install_info: install_info(config),
        projects,
        activations,
        versions,
        events: event_bus,
        listener: ListenerClient::new(RoleClient::new(CoreRole::Listener, addresses.listener.clone(), tokens.clone(), http.clone())),
        authenticator,
        tenant_router,
        project_reclaimer,
        public_base_url: config.public_url.trim_end_matches('/').to_string(),
        internet_url: config.internet_url.clone(),
        edge: config.edge,
        broker: RoleClient::new(CoreRole::Broker, addresses.broker.clone(), tokens, http.clone()),
        http,
        caller_token_secret: Arc::new(caller_token_secret),
    })
}

/// What the install tells about itself (`GET /install`), from its config.
pub fn install_info(config: &InstallConfig) -> weft_core::install::InstallInfo {
    use weft_platform_traits::config::PlatformConfig;
    weft_core::install::InstallInfo {
        public_url: config.public_url.clone(),
        cloud: match &config.platform {
            PlatformConfig::Local(_) => None,
            PlatformConfig::Gcp(g) => Some(weft_core::install::CloudInstall::Gcp(weft_core::install::GcpInstall {
                project: g.project.clone(),
                region: g.region.clone(),
                artifact_registry: g.artifact_registry.clone(),
                network: g.network.clone(),
                subnet: g.subnet.clone(),
                deployer_service_account: g.deployer_service_account.clone(),
                frontend_service_account: g.frontend_service_account.clone(),
                workload_identity_provider: g.workload_identity_provider.clone(),
            })),
        },
        source: config.source.clone(),
    }
}

/// A task-registry builder pre-loaded with the dispatcher's core task
/// executors. Extra task kinds chain `.register_str` on the returned
/// builder before `.build()`.
pub fn core_task_registry_builder() -> crate::task_executor::TaskRegistryBuilder {
    use weft_task_store::TaskKind;
    crate::task_executor::TaskRegistry::builder()
        .register(TaskKind::RegisterSignal, Arc::new(crate::task_kinds::RegisterSignalExecutor))
        .register(TaskKind::RouteEntry, Arc::new(crate::task_kinds::RouteEntryExecutor))
        .register(TaskKind::LiveArrival, Arc::new(crate::task_kinds::LiveArrivalExecutor))
        .register(TaskKind::FireSignal, Arc::new(crate::task_kinds::FireSignalExecutor))
        .register(TaskKind::RecordCost, Arc::new(crate::task_kinds::RecordCostExecutor))
        .register(TaskKind::RecordLog, Arc::new(crate::task_kinds::RecordLogExecutor))
        .register(TaskKind::StopTagged, Arc::new(crate::task_kinds::StopTaggedExecutor))
        .register(TaskKind::ProgramCall, Arc::new(crate::task_kinds::ProgramCallExecutor))
        .register_str(
            crate::task_kinds::run_node_test::RUN_NODE_TEST_KIND,
            Arc::new(crate::task_kinds::RunNodeTestExecutor),
        )
}

/// The dispatcher's background loops: its task picker, the delivery of
/// executions to workers, the lifecycle claimer, the two bridges and the
/// reapers. The runtime runs them where the dispatcher is placed (see
/// `weft_task_store::drain`).
pub fn drain_loops(state: &DispatcherState, registry: crate::task_executor::TaskRegistry) -> anyhow::Result<Vec<DrainLoop>> {
    let picker_store: Arc<dyn weft_task_store::TaskStoreClient> = Arc::new(
        weft_task_store::PostgresTaskStoreClient::new(state.pg_pool.clone(), state.signals.clone())
            .context("the dispatcher's signal watch listens on every task channel")?,
    );
    // The picker, the delivery and the claimer also rescue a claim whose
    // holder died, which nothing announces: they look again while
    // anything is in motion.
    let in_motion = |l| crate::reaper::while_in_motion(state, l);
    let mut loops = vec![
        in_motion(weft_task_store::dispatcher_picker_loop(picker_store, state.clone(), registry, state.replica.clone())),
        in_motion(crate::delivery::drain_loop(state.clone())),
        in_motion(crate::lifecycle_claimer::drain_loop(state.clone())),
        crate::journal_bridge::drain_loop(state.clone()),
        crate::infra_event_bridge::drain_loop(state.clone()),
        crate::build::follow::drain_loop(state),
        crate::holders::drain_loop(state),
        crate::domains::drain_loop(state),
    ];
    loops.extend(crate::reaper::drain_loops(state));
    // A dispatcher at zero is woken by name for what `loop_wakes` says,
    // so a loop woken by a write that the list misses would never run on
    // one: refused here, at every boot, instead.
    let listed = loop_wakes();
    for l in loops.iter().filter(|l| !l.wake_on.is_empty()) {
        anyhow::ensure!(
            listed.iter().any(|(name, wake_on)| *name == l.name && std::ptr::eq(*wake_on, l.wake_on)),
            "the dispatcher's loop '{}' wakes by rules `loop_wakes` does not name",
            l.name
        );
    }
    for (name, _) in &listed {
        anyhow::ensure!(loops.iter().any(|l| l.name == *name), "`loop_wakes` names '{name}', which is no loop of the dispatcher");
    }
    Ok(loops)
}

/// What wakes each of the dispatcher's loops that a write wakes, by
/// name: what a writer reads to wake a dispatcher at zero for exactly the
/// loops a write concerns (`weft_runtime::role_waker`).
/// `drain_loops` refuses to boot when it and the loops disagree. Read at
/// run time from each loop's own static, so a rule here IS the loop's (one
/// static is one address; a copy made at compile time would not be).
pub fn loop_wakes() -> Vec<(&'static str, &'static [WakeOn])> {
    vec![
        ("dispatcher_picker", weft_task_store::executor::DISPATCHER_READY),
        ("delivery", crate::delivery::WAKE_ON),
        ("lifecycle_claimer", crate::lifecycle_claimer::WAKE_ON),
        ("journal_bridge", crate::journal_bridge::ON_EXEC_EVENT),
        ("infra_event_bridge", crate::infra_event_bridge::ON_INFRA_EVENT),
        ("parked_fires", crate::reaper::ON_PARKED_FIRE),
        ("storage_sweep", crate::reaper::ON_STORAGE_SWEEP),
        ("holders", crate::holders::ON_HELD_SIGNALS),
        ("domains_door", crate::domains::ON_DOMAINS),
    ]
}

/// The dispatcher's relays of this process's notifications to the
/// requests and loops of this same process. They live while the process
/// does, whatever its placement: they only serve requests this process is
/// answering.
pub fn spawn_relays(state: &DispatcherState) {
    let endings = state.clone();
    spawn_supervised("unrecorded_endings", async move {
        crate::journal_bridge::run_unrecorded_endings(endings).await;
    });
    // Builds and removals ask for a reclaim of the images nothing uses,
    // but one a container still ran from, or one a process died before
    // reclaiming, would wait for the next build. So the reclaim also runs
    // at every start and on a timer.
    let images = state.clone();
    spawn_supervised("image_reclaim", async move {
        loop {
            images.builder.prunes.request(&images, None);
            tokio::time::sleep(IMAGE_RECLAIM_EVERY).await;
        }
    });
}

/// How often the dispatcher reclaims the images nothing uses
/// (`crate::build::prune::AfterBuildPrunes`) without a build asking.
const IMAGE_RECLAIM_EVERY: std::time::Duration = std::time::Duration::from_secs(6 * 3600);

/// Spawn a background task under supervision: one that is only ever
/// meant to run for the process's whole life, so the wrapped future NEVER
/// completing is the normal case. If it DOES complete, it unwound (a panic,
/// an unexpected early return): a partial death where one function is
/// silently dead while the process keeps serving. The dispatcher holds no
/// coordination state a sibling could not rebuild from Postgres, so the
/// honest recovery is to crash the whole process and let its host restart
/// it onto a clean slate, never to limp on with a missing loop.
pub fn spawn_supervised<Fut>(name: &'static str, fut: Fut)
where
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let joined = tokio::spawn(fut).await;
        match joined {
            Ok(()) => tracing::error!(
                target: "weft_dispatcher",
                loop_name = name,
                "background task exited unexpectedly (it must run for the process's whole life); \
                 crashing the process so its host restarts it"
            ),
            Err(e) => tracing::error!(
                target: "weft_dispatcher",
                loop_name = name,
                error = %e,
                "background task PANICKED; crashing the process so its host restarts it"
            ),
        }
        std::process::exit(1);
    });
}

/// Record the install's bootstrap operator key (`WEFT_BOOTSTRAP_OPERATOR_KEY`)
/// unless an operator key already exists (see `Journal::seed_operator_token`).
/// Only a shared install has one.
pub async fn seed_bootstrap_operator_key(state: &DispatcherState, key: &str) -> anyhow::Result<()> {
    use weft_core::signal_token as names;
    let key = key.trim();
    anyhow::ensure!(
        !key.is_empty(),
        "WEFT_BOOTSTRAP_OPERATOR_KEY is empty: a shared install needs its first operator key"
    );
    let token = crate::journal::SignalToken {
        id: uuid::Uuid::new_v4(),
        kind: weft_core::signal_token::TokenKind::Operator,
        token_hash: names::token_hash(key),
        recognizer: names::recognizer(key),
        tenant_id: crate::tenant::TenantId::local().as_str().to_string(),
        name: Some("bootstrap".into()),
        allowed_projects: Vec::new(),
        allowed_tags: Vec::new(),
        allowed_displays: Vec::new(),
        all_displays: false,
        created_at: crate::lease::now_unix() as u64,
        instance: None,
        expires_at: None,
    };
    if state.journal.seed_operator_token(&token).await.context("seed the bootstrap operator key")? {
        info!("recorded the install's bootstrap operator key ({})", token.recognizer);
    }
    Ok(())
}

/// The CORS policy of the management surface for an auth mode: a local
/// install answers the editor's webviews and extension (browser origins no
/// allowlist can enumerate); a shared install answers no browser origin (a
/// frontend calls it from its server, and the editor is not a browser
/// origin).
pub fn cors_for(auth: AuthMode) -> tower_http::cors::CorsLayer {
    match auth {
        AuthMode::Local => crate::api::permissive_cors(),
        AuthMode::OperatorKeys => tower_http::cors::CorsLayer::new(),
    }
}
