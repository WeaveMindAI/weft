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
use weft_platform_traits::config::InstallConfig;
use weft_platform_traits::CoreRole;
use weft_task_store::drain::{DrainLoop, WakeOn};

use crate::store::postgres::PostgresStore;
use crate::listener::ListenerClient;
use crate::role_client::RoleClient;
use crate::reclaim::{default_reclaimer, ProjectReclaimer};
use crate::tenant;
use crate::DispatcherState;

/// Every schema group the dispatcher owns, in dependency order: each group
/// after the ones whose tables its DDL names (the groups the run and
/// signal tables' triggers sit on come before theirs). Its part of the install's one schema
/// list (`weft_runtime::schema::groups()`), which the boot applies in one
/// guarded pass and the migration generator and the agreement test build
/// from; a test of the dispatcher's own SQL applies just this part.
pub static DISPATCHER_GROUPS: &[&weft_task_store::SchemaGroup] = &[
    // What the writes a run makes announce, sent in batches: every
    // trigger on a run's path writes to it.
    &weft_task_store::announce::GROUP,
    // Before the stores' group: a trigger of theirs on `access_grant`
    // marks the signals acting as a connection that changed.
    &weft_access_store::GROUP,
    // Which worker processes are alive, and what their doors counted.
    // Before the stores' group: a trigger of theirs on `worker_lease`
    // wakes the delivery when a worker of a project whose runs could not
    // reach one comes alive.
    &weft_task_store::worker_door::GROUP,
    &crate::store::postgres::GROUP,
    // The exclusive infra ownership leases.
    &crate::infra_owner::GROUP,
    // How many holders the platform's pool was last set to run.
    &crate::holders::GROUP,
    &weft_task_store::tasks::GROUP,
    &crate::infra_node::GROUP,
    &crate::infra_event::GROUP,
    &crate::infra_lifecycle_command::GROUP,
    // The events waiting for a parked trigger, one row each (their notify
    // trigger reads `signal`, so after the stores' group).
    &weft_task_store::parked_fires::GROUP,
    // The durable terminate-sweep queue (no FK; the dispatcher owns it and
    // the reaper drains it by asking the broker to sweep a terminated
    // run's files).
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
    // Whether the install is in maintenance, and the infra copies it
    // stopped (they cascade with their project).
    &crate::maintenance::GROUP,
    // The version tree hangs off `project` (its foreign keys and the head
    // columns), so it comes after.
    &crate::versions::GROUP,
    // The public edge's counters: per-minute buckets and the runs each
    // entry has going (no FK: a slot outlives nothing it names).
    &crate::entry_limits::GROUP,
    // Every finished run's words, for finding a run by what went through
    // it (`search_index`).
    &crate::search_index::GROUP,
    // The image builds running and done, by image ref (`build::ledger`).
    &crate::build::ledger::GROUP,
    // The wakes a local install has set and not yet delivered (a cloud
    // keeps them in its own queue; the table stays empty there).
    &weft_task_store::alarm::GROUP,
    // When each loop of a role that scales to zero next wants a look
    // (`weft_task_store::drain`); empty on a local install.
    &weft_task_store::drain::GROUP,
    // weft's own calls that keep failing (a role it cannot wake, workers
    // it cannot hand a run to), for `weft status` to show.
    &weft_task_store::unanswered::GROUP,
    // What one holder across the install holds right now: a sweep
    // (`crate::reaper`), a project's front, a tenant's signal registrations.
    &weft_task_store::locks::GROUP,
    // The stretches in which requests waited for a database connection,
    // for `weft status` to show.
    &weft_task_store::throttle::GROUP,
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
    /// The install's policies: one tenant `local`, every management
    /// request authenticated by an operator key (`install_key` the
    /// install's own), on a local install as on a cloud one.
    pub fn for_install(install_key: &str) -> anyhow::Result<Self> {
        Ok(Self {
            authenticator: Arc::new(crate::authenticator::OperatorKeyAuthenticator::new(crate::tenant::TenantId::local(), install_key)?),
            tenant_router: tenant::local_router(),
            project_reclaimer: default_reclaimer(),
        })
    }
}

/// Every channel the dispatcher listens on. The process's one `LISTEN`
/// connection must hold them all.
pub const DISPATCHER_CHANNELS: &[&str] = &[
    weft_task_store::tasks::TASK_READY_CHANNEL,
    weft_task_store::terminal::TERMINAL_CHANNEL,
    weft_task_store::runs::RUN_QUEUED_CHANNEL,
    weft_journal::RUN_LOG_CHANNEL,
    weft_journal::RUN_ENDED_CHANNEL,
    weft_journal::RETENTION_CHANNEL,
    weft_task_store::worker_door::LEASE_ALIVE_CHANNEL,
    crate::delivery::WORKERS_BACK_CHANNEL,
    weft_broker_client::lifecycle_command::INFRA_COMMAND_CHANNEL,
    crate::infra_event_bridge::INFRA_EVENT_CHANNEL,
    weft_task_store::runs::EVENTS_CHANNEL,
    weft_task_store::parked_fires::PARKED_FIRE_CHANNEL,
    crate::working_copies::WORKING_COPY_CHANNEL,
    crate::reaper::STORAGE_SWEEP_CHANNEL,
    crate::display_feeds::LOOK_NOW_CHANNEL,
    crate::holders::HELD_SIGNALS_CHANNEL,
    crate::holders::ACCESS_REMADE_CHANNEL,
    crate::domains::DOMAINS_CHANNEL,
    crate::held::ROUTES_CHANNEL,
    crate::held::WORKER_SETTINGS_CHANNEL,
    crate::held::INFRA_STATUS_CHANNEL,
    crate::held::ACCESS_CHANNEL,
    crate::build::follow::BUILD_CLAIMED_CHANNEL,
    crate::build::waiting::VERSION_BUILD_ENDED_CHANNEL,
    crate::task_kinds::resync::RESYNC_GIVEN_UP_CHANNEL,
    weft_task_store::locks::LEASE_RELEASED_CHANNEL,
];

/// How long the dispatcher's client may take to open a connection to a
/// peer (a role, a worker, a store, a node's container) before the call
/// fails. A peer that is up accepts within milliseconds inside the
/// install's network and within a few seconds across the internet; an
/// address that swallows the attempt would otherwise hold the call for
/// the system's own connect limit, which is minutes.
const PEER_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// What the dispatcher is built from: the install's config and what the
/// runtime process shares among its roles.
pub struct DispatcherSettings<'a> {
    pub config: &'a InstallConfig,
    /// This process's replica id.
    pub replica: String,
    pub pool: weft_task_store::db::Pool,
    /// A pool of its own for the project transition lock (see
    /// `DispatcherState::lock_pool`).
    pub lock_pool: weft_task_store::db::Pool,
    /// The process's one `LISTEN` connection, on (at least)
    /// [`DISPATCHER_CHANNELS`].
    pub signals: Arc<weft_task_store::pg_signal::PgSignalWatch>,
    pub runner: Arc<dyn weft_platform_traits::Runner>,
    pub needs: Arc<dyn weft_platform_traits::MachineNeeds>,
    pub host: Arc<dyn weft_platform_traits::InfraHost>,
    pub images: Arc<dyn weft_platform_traits::ImageBuilder>,
    pub frontends: Arc<dyn weft_platform_traits::FrontendHosting>,
    pub domains: Arc<dyn weft_platform_traits::DomainHosting>,
    pub holder_pool: Arc<dyn weft_platform_traits::HolderPool>,
    /// See `DispatcherState::holder_news`.
    pub holder_news: Arc<dyn weft_platform_traits::HolderNews>,
    /// The dispatcher's identity for its calls to the other roles.
    pub tokens: Arc<dyn weft_platform_traits::IdentityTokens>,
    /// See `DispatcherState::booking`.
    pub booking: Option<weft_task_store::drain::Booking>,
    /// See `DispatcherState::aside`.
    pub aside: weft_task_store::aside::StepAside,
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
        needs,
        host,
        images,
        frontends,
        domains,
        holder_pool,
        holder_news,
        tokens,
        booking,
        aside,
    } = settings;
    for channel in DISPATCHER_CHANNELS {
        signals.require(channel)?;
    }
    info!("dispatcher replica: {replica}");
    let store = Arc::new(PostgresStore::from_pool(pool.clone()));
    let programs = store.programs().clone();
    let projects: crate::ProjectStore = Arc::new(crate::PostgresProjectStore::new(pool.clone()));
    let activations: crate::activation_store::ActivationStore =
        Arc::new(crate::activation_store::PostgresActivationStore::new(pool.clone()));
    let versions: crate::versions::VersionStore = Arc::new(crate::versions::PostgresVersionStore::new(pool.clone()));
    let working_copies: crate::working_copies::WorkingCopyStore = Arc::new(crate::working_copies::PostgresWorkingCopyStore::new(pool.clone()));
    let event_bus = crate::EventBus::with_notify(pool.clone(), &signals)?;
    let displays = crate::display_feeds::DisplayFeeds::with_look_now(&signals)?;
    let held = Arc::new(crate::held::Held::follow(&signals)?);
    // The other roles as this dispatcher reaches them, from where its
    // own placement puts it (a local install's loopback is only its own
    // process's).
    let addresses = config.role_addresses(config.roles.of(CoreRole::Dispatcher).vantage());
    let builder = Arc::new(crate::build::VersionBuilder {
        images,
        pool: pool.clone(),
        prunes: Default::default(),
        blobs: crate::build::blob_cache::BlobCache::in_temp_dir(config.roles.of(CoreRole::Dispatcher).files_take_memory()),
    });
    // Every peer this client talks to (the broker and listener roles, an
    // infra unit's `/live` and `/action`, a worker) answers in place; a
    // redirect is a peer trying to send the dispatcher somewhere else, so
    // none is followed. An address that swallows the connection attempt
    // fails within `PEER_CONNECT_TIMEOUT`, and a held call whose path died
    // within a minute or so (`notice_dead_connections`). The client puts
    // no limit on a call's whole length: a call to a worker is held for a
    // run's whole life, and a proxied call lasts as long as its caller
    // wants. Each short internal call bounds itself where it is made
    // (`RequestBuilder::timeout`).
    let http = weft_core::net::notice_dead_connections(
        weft_core::net::http_client().redirect(reqwest::redirect::Policy::none()).connect_timeout(PEER_CONNECT_TIMEOUT),
    )
    .build()
    .expect("a reqwest client with only a redirect policy and timeouts builds");
    let address = Arc::new(weft_task_store::install_address::InstallAddress::follow(
        pool.clone(),
        &signals,
        &config.public_url,
        config.internet_address(),
    )?);
    Ok(DispatcherState {
        replica: replica.clone(),
        runs: store.clone(),
        registrations: store.clone(),
        tokens: store.clone(),
        bakes: store,
        pg_pool: pool,
        lock_pool,
        signals,
        displays,
        runner,
        needs,
        host,
        frontends,
        domains,
        holder_pool,
        holder_settings: config.holders,
        holder_news,
        worker_defaults: config.worker_settings(),
        alert_after_secs: whole_secs_up(config.alert_delay()),
        builder,
        install_info: install_info(config),
        address,
        projects,
        activations,
        versions,
        working_copies,
        events: event_bus,
        listener: ListenerClient::new(RoleClient::new(CoreRole::Listener, addresses.listener.clone(), tokens.clone(), http.clone())),
        authenticator,
        tenant_router,
        project_reclaimer,
        files: match &config.object_store {
            weft_platform_traits::config::ObjectStoreSettings::S3(store) if !store.public_internet => {
                Some(crate::files_door::FilesTarget { endpoint: store.endpoint.clone(), bucket: store.bucket.clone() })
            }
            _ => None,
        },
        edge: config.edge,
        broker: RoleClient::new(CoreRole::Broker, addresses.broker.clone(), tokens.clone(), http.clone()),
        broker_line: weft_broker_client::BrokerLink::new(
            addresses.broker.clone(),
            weft_broker_client::TokenSource::role(tokens, replica.clone(), CoreRole::Dispatcher),
        ),
        http,
        programs,
        held,
        project_ports: match &config.platform {
            weft_platform_traits::config::PlatformConfig::Local(local) => {
                Some(crate::project_ports::ProjectPorts::new(local.listen.public, local.project_ports))
            }
            weft_platform_traits::config::PlatformConfig::Gcp(_) => None,
        },
        booking,
        aside,
    })
}

/// `delay` in whole seconds, rounded up: a delay scaled under a test pace
/// to a fraction of a second still waits a second, and only no delay at
/// all (`alert_after_minutes = 0`, at the first failure) reads 0.
fn whole_secs_up(delay: std::time::Duration) -> i64 {
    i64::try_from(delay.as_millis().div_ceil(1000)).unwrap_or(i64::MAX)
}

/// What the install tells about itself (`GET /install`), from its config:
/// its address there is the platform's own, which `GET /install` answers
/// with the install's domain once that domain is the address.
pub fn install_info(config: &InstallConfig) -> weft_core::install::InstallInfo {
    use weft_platform_traits::config::PlatformConfig;
    weft_core::install::InstallInfo {
        public_url: config.public_url.clone(),
        service_url: config.public_url.clone(),
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

/// The executor of every task kind (`weft_task_store::TaskKind`).
pub fn task_registry() -> crate::task_executor::TaskRegistry {
    use weft_task_store::TaskKind;
    crate::task_executor::TaskRegistry::every_kind(|kind| match kind {
        TaskKind::RegisterSignal => Arc::new(crate::task_kinds::RegisterSignalExecutor),
        TaskKind::WithdrawSignal => Arc::new(crate::task_kinds::WithdrawSignalExecutor),
        TaskKind::CancelCall => Arc::new(crate::task_kinds::CancelCallExecutor),
        TaskKind::FireSignal => Arc::new(crate::task_kinds::FireSignalExecutor),
        TaskKind::StopTagged => Arc::new(crate::task_kinds::StopTaggedExecutor),
        TaskKind::ProgramCall => Arc::new(crate::task_kinds::ProgramCallExecutor),
        TaskKind::RunNodeTest => Arc::new(crate::task_kinds::RunNodeTestExecutor),
        TaskKind::Resync => Arc::new(crate::task_kinds::ResyncExecutor),
        TaskKind::TakeDownWait => Arc::new(crate::task_kinds::TakeDownWaitExecutor),
        TaskKind::RemovalWait => Arc::new(crate::task_kinds::RemovalWaitExecutor),
        TaskKind::MaintenanceLift => Arc::new(crate::task_kinds::MaintenanceLiftExecutor),
        TaskKind::Activate => Arc::new(crate::task_kinds::ActivateExecutor),
        TaskKind::StartRun => Arc::new(crate::task_kinds::StartRunExecutor),
    })
}

/// The dispatcher's background loops: its task picker, the delivery of
/// queued runs to workers, the lifecycle claimer, what runs' endings leave
/// to do, the drains, the look at lapsed holds, retention and the reapers.
/// The runtime runs them where the dispatcher is placed (see
/// `weft_task_store::drain`).
pub fn drain_loops(state: &DispatcherState, registry: crate::task_executor::TaskRegistry) -> anyhow::Result<Vec<DrainLoop>> {
    let picker_store = Arc::new(
        weft_task_store::PostgresTaskStoreClient::new(state.pg_pool.clone(), state.signals.clone())
            .context("the dispatcher's signal watch listens on every task channel")?,
    );
    // The picker, the delivery and the claimer also rescue a claim of
    // their own whose holder died, which nothing announces: each looks
    // again when its earliest claim lapses. Every other hold that lapses
    // unannounced is the `lapses` loop's.
    let mut loops = vec![
        weft_task_store::dispatcher_picker_loop(picker_store, state.clone(), registry, state.replica.clone(), state.aside.clone()),
        crate::delivery::drain_loop(state.clone()),
        crate::lifecycle_claimer::drain_loop(state.clone()),
        crate::lapses::drain_loop(state.clone()),
        crate::run_ends::drain_loop(state.clone()),
        crate::retention::drain_loop(state.clone()),
        crate::build::follow::drain_loop(state),
        crate::holders::drain_loop(state),
        crate::holders::access_loop(state),
        crate::domains::drain_loop(state),
        crate::domains::certificates_loop(state),
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
        (crate::lapses::LAPSES, crate::lapses::WAKE_ON),
        ("run_ends", crate::run_ends::WAKE_ON),
        ("retention", crate::retention::WAKE_ON),
        ("parked_fires", crate::reaper::ON_PARKED_FIRE),
        ("storage_sweep", crate::reaper::ON_STORAGE_SWEEP),
        (crate::reaper::DOOR_WINDOWS, crate::reaper::ON_DOOR_WINDOW),
        (crate::reaper::INFRA_RETURNS, crate::reaper::ON_INFRA_RETURNS),
        (crate::reaper::RESYNC_RETURNS, crate::reaper::ON_INFRA_RETURNS),
        ("holders", crate::holders::ON_HELD_SIGNALS),
        (crate::holders::HOLDER_ACCESS, crate::holders::ON_ACCESS),
        ("domains_door", crate::domains::ON_DOMAINS),
        ("domains_certificates", crate::domains::ON_DOMAINS),
        ("image_builds", crate::build::follow::WAKE_ON),
    ]
}

/// The dispatcher's relays of this process's notifications to the
/// requests and loops of this same process. They live while the process
/// does, whatever its placement: they only serve requests this process is
/// answering.
pub fn spawn_relays(state: &DispatcherState) {
    spawn_supervised("live_view", crate::live_view::run(state.clone()));
    spawn_supervised("infra_event_bridge", crate::infra_event_bridge::run(state.clone()));
    // Builds and removals ask for a reclaim of the images nothing uses. On
    // a machine an image a container still ran from cannot go
    // (`ImageDeleted::InUse`), so an infra copy changing (stopped,
    // replaced) asks again there.
    if state.project_ports.is_some() {
        spawn_supervised("image_reclaim", reclaim_after_infra_changes(state.clone()));
    }
}

/// Ask for a reclaim of the images nothing uses each time an infra copy
/// changes (`crate::held::INFRA_STATUS_CHANNEL`). Returns only when the
/// signal watch stops, which crashes the process through its supervisor.
async fn reclaim_after_infra_changes(state: DispatcherState) {
    let mut heard = state.signals.subscribe();
    loop {
        match heard.next().await {
            Ok(weft_task_store::pg_signal::Heard::Signal { channel, .. }) if channel == crate::held::INFRA_STATUS_CHANNEL => {
                state.builder.prunes.request(&state, None);
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(target: "weft_dispatcher::build", error = %e, "reclaiming images after infra changes stopped");
                return;
            }
        }
    }
}

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
            // The tokio runtime is shutting down (the process is ending
            // already, e.g. on a failed start): nothing died.
            Err(e) if e.is_cancelled() => return,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    /// The alert delay in whole seconds never rounds a delay down to none.
    #[test]
    fn a_delay_rounds_up_to_whole_seconds() {
        assert_eq!(super::whole_secs_up(Duration::ZERO), 0, "no delay: at the first failure");
        assert_eq!(super::whole_secs_up(Duration::from_millis(300)), 1);
        assert_eq!(super::whole_secs_up(Duration::from_secs(60)), 60);
        assert_eq!(super::whole_secs_up(Duration::from_millis(60_001)), 61);
    }
}
