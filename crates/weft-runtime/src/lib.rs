//! `weft-runtime`: the one process every install runs.
//!
//! Every role weft has is a library (`weft-dispatcher`, `weft-broker`,
//! `weft-listener`, `weft-infra-supervisor`); this crate is where they
//! are put together over the platform the install config names. A local
//! install runs every role in one process (the machine), each under its
//! own path prefix, each role's loops running for as long as the process
//! does. On a cloud each role is a process of its own (`serve --role
//! <role>`) serving that one role at its root: placed serverless it runs
//! its loops once per tick and sets its next wake, and the holder, in a
//! pool, holds its share of the held connections until it is stopped.
//!
//! - `platform`: the trait objects every role takes, per platform.
//! - `server`: the ports and what they carry.
//! - `guard`: the identity check on every internal route.
//! - `role_waker`: waking the roles that scale to zero when work is
//!   written for them.
//! - `log_file`: a local install's bounded log file.
//! - `unit_agent`: the agent beside every infra unit.

pub mod guard;
pub mod log_file;
pub mod platform;
pub mod role_waker;
pub mod server;
pub mod unit_agent;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use axum::Router;
use weft_platform_traits::config::{AuthMode, InstallConfig, PlatformConfig};
use weft_platform_traits::{CoreRole, Placement, Vantage};
use weft_task_store::drain::DrainLoop;

/// How often the supervisor renews its project leases: a third of the
/// lease (`weft_broker_client::lifecycle_command::infra_owner_lease_secs`).
const SUPERVISOR_OWNERSHIP_EVERY: Duration = Duration::from_secs(15);

/// How often the supervisor looks at every owned project's health.
const SUPERVISOR_HEALTH_EVERY: Duration = Duration::from_secs(30);

/// The work pool's size, and how long a request waits for one of its
/// connections.
const WORK_POOL: (u32, Duration) = (16, Duration::from_secs(5));

/// The pool a role holds its long locks on (the dispatcher's project
/// transition lock, the broker's lock on one signal's subscriptions). A
/// lock is held for the length of an operation and its connection does no
/// work, so a lock taken from the work pool is a connection the operation
/// itself then has to wait for (see `weft_dispatcher::lease`).
const LOCK_POOL: (u32, Duration) = (16, Duration::from_secs(120));

/// The pool the broker writes workers' batches of records on
/// (`weft_broker::records`): a burst of records never starves the
/// broker's other work of connections, and a batch waits for one rather
/// than fails (a worker's lane has one batch in flight, so the wait is
/// how the database slows the workers down).
const RECORD_POOL_WAIT: Duration = Duration::from_secs(60);

/// A required secret from the environment (`SECRET_ENV`).
fn secret(name: &str) -> anyhow::Result<String> {
    optional_secret(name).ok_or_else(|| anyhow::anyhow!("{name} is required; the install puts it in this process's environment"))
}

/// The install's caller-ticket secret (`WEFT_CALLER_TOKEN_SECRET`), the root
/// every project's own secret is derived from
/// (`weft_core::caller_token::ProjectSecret`). It stays in weft's own
/// processes; a project's workers are given only their project's.
fn install_secret() -> anyhow::Result<Vec<u8>> {
    hex::decode(secret("WEFT_CALLER_TOKEN_SECRET")?.trim()).context("WEFT_CALLER_TOKEN_SECRET is not hex")
}

/// A secret the environment may leave out.
fn optional_secret(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// A role's loops, run where the role is placed.
enum Loops {
    Drain(Vec<DrainLoop>),
    Supervisor(weft_infra_supervisor::SupervisorState),
}

/// Run the roles of this process: every role, in a local install's one
/// process, or, with `only`, that one role, placed in a process of its
/// own.
pub async fn serve(config: InstallConfig, only: Option<CoreRole>) -> anyhow::Result<()> {
    let hosted: Vec<CoreRole> = match only {
        Some(role) => {
            anyhow::ensure!(
                config.roles.of(role) != Placement::Machine,
                "--role {role} runs a role in a process of its own, and roles.{role} is in the local install's one process, which already runs it"
            );
            vec![role]
        }
        None => {
            anyhow::ensure!(
                matches!(config.platform, PlatformConfig::Local(_)),
                "a cloud install has no process that runs every role; run one with `serve --role <role>`"
            );
            config.roles.on_machine()
        }
    };
    let placement = only.map(|r| config.roles.of(r));
    // Whether the loops run once per tick (a role that scales to zero)
    // rather than for the life of the process.
    let serverless = placement == Some(Placement::Serverless);
    // Where this process stands, which decides how it reaches every other
    // role: a local install's loopback is only its own process's.
    let here = placement.map_or(Vantage::Machine, Placement::vantage);
    // A process of one role serves it at its root; the local install's
    // process serves each of its roles under the role's prefix.
    let mount = |on: Router, role: CoreRole, routes: Router| match only {
        Some(_) => on.merge(routes),
        None => on.nest(role.internal_prefix().expect("the local process mounts only roles with internal routes"), routes),
    };
    let runs = |r: CoreRole| hosted.contains(&r);
    let replica = weft_platform_traits::identity::mint_replica_id("runtime");
    let addresses = config.role_addresses(here);
    // The processes that write: every write is the dispatcher's or the
    // broker's (every other role writes through the broker).
    let writes = runs(CoreRole::Dispatcher) || runs(CoreRole::Broker);

    // The database: for the roles that reach it, and for a local
    // install's wakes, which it keeps there.
    let needs_db = writes || matches!(config.platform, PlatformConfig::Local(_));
    let pool = if needs_db {
        let url = secret("WEFT_DATABASE_URL")?;
        let pool = weft_task_store::db::connect(&url, WORK_POOL.0, WORK_POOL.1).await.context("connect to the database")?;
        weft_dispatcher::app::apply_core_schema(&pool).await?;
        Some((url, pool))
    } else {
        None
    };
    let parts = platform::build(&config, pool.as_ref().map(|(_, p)| p)).await?;
    // A writer that is up wakes the roles at zero its writes concern.
    let waker = match writes {
        true => role_waker::RoleWaker::new(&config, &addresses, parts.tokens.clone())?.map(Arc::new),
        false => None,
    };

    // The process's one LISTEN connection, on every channel its roles
    // and its waker wait on, for as long as the process is up. A pooled
    // database address cannot hold one, so it may have an address of its
    // own (`WEFT_DATABASE_LISTEN_URL`).
    let signals = match &pool {
        Some((url, _)) if writes => {
            let mut channels: Vec<&'static str> = Vec::new();
            if let Some(waker) = &waker {
                channels.extend(waker.channels());
            }
            if runs(CoreRole::Dispatcher) {
                channels.extend(weft_dispatcher::app::DISPATCHER_CHANNELS);
            }
            if runs(CoreRole::Broker) {
                channels.extend(weft_broker::state::BROKER_CHANNELS);
            }
            channels.sort_unstable();
            channels.dedup();
            // Once per process: the watch holds its channel list for life.
            let channels: &'static [&'static str] = Box::leak(channels.into_boxed_slice());
            let listen_url = optional_secret("WEFT_DATABASE_LISTEN_URL").unwrap_or_else(|| url.clone());
            let connect: sqlx::postgres::PgConnectOptions =
                listen_url.parse().context("WEFT_DATABASE_LISTEN_URL (or WEFT_DATABASE_URL) is not a Postgres address")?;
            Some(weft_task_store::pg_signal::PgSignalWatch::start(&connect, channels).await.context("listen for Postgres signals")?)
        }
        _ => None,
    };

    let mut internal = Router::new();
    let mut public = Router::new();
    let mut outside: Option<Router> = None;
    let mut loops: Vec<(CoreRole, Loops)> = Vec::new();
    let mut after_bind: Vec<futures::future::BoxFuture<'static, anyhow::Result<()>>> = Vec::new();
    let mut holding: Option<weft_listener::ListenerState> = None;

    // The internal routes of the roles weft calls, behind the guard, which
    // accepts every address a caller anywhere was given for the role.
    let guard_for = |role: CoreRole| guard::CoreOnly { identity: parts.identity.clone(), audiences: Arc::new(config.role_audiences(role)) };

    if runs(CoreRole::Broker) {
        let (url, pool) = pool.as_ref().expect("the broker's process holds the database");
        let lock_pool = weft_task_store::db::connect(url, LOCK_POOL.0, LOCK_POOL.1).await.context("connect the broker's lock pool")?;
        let record_pool = weft_task_store::db::connect_record_pool(url, &replica, RECORD_POOL_WAIT).await.context("connect the broker's record pool")?;
        let state = weft_broker::BrokerState::new(
            pool.clone(),
            lock_pool,
            record_pool,
            signals.clone().expect("the broker's process listens"),
            weft_broker::state::BrokerSettings {
                auth: weft_broker::AuthConfig {
                    audiences: config.role_audiences(CoreRole::Broker),
                },
                identity: parts.identity.clone(),
                object_store: platform::object_store(&config.object_store, &parts).await?,
                entitlements: Arc::new(weft_broker::entitlement::LocalEntitlementSource::from_env()),
                credentials: Arc::new(weft_broker::credential::FileCredentialSource::from_env()),
                app_provider: Arc::new(weft_broker::app_provider::FileAppProvider::from_env()),
                public_base_url: config.public_url.clone(),
                internet_url: config.internet_url.clone(),
                object_store_public_internet: config.object_store.public_internet(),
            },
        )
        .await?;
        loops.push((CoreRole::Broker, Loops::Drain(weft_broker::drain_loops(state.clone()))));
        internal = mount(internal, CoreRole::Broker, weft_broker::router(state));
    }

    if runs(CoreRole::Dispatcher) {
        let (url, pool) = pool.as_ref().expect("the dispatcher's process holds the database");
        let lock_pool = weft_task_store::db::connect(url, LOCK_POOL.0, LOCK_POOL.1).await.context("connect the lock pool")?;
        let state = weft_dispatcher::app::build_state(
            weft_dispatcher::app::DispatcherSettings {
                config: &config,
                replica: replica.clone(),
                pool: pool.clone(),
                lock_pool,
                signals: signals.clone().expect("the dispatcher's process listens"),
                runner: parts.runner.clone(),
                host: parts.host.clone(),
                images: parts.images.clone(),
                frontends: parts.frontends.clone(),
                domains: parts.domains.clone(),
                holder_pool: parts.holder_pool.clone(),
                tokens: parts.tokens.clone(),
            },
            weft_dispatcher::app::Defaults::for_auth(config.auth),
        )
        .await?;
        if config.auth == AuthMode::OperatorKeys {
            weft_dispatcher::app::seed_bootstrap_operator_key(&state, &secret("WEFT_BOOTSTRAP_OPERATOR_KEY")?).await?;
        }
        let registry = weft_dispatcher::app::core_task_registry_builder().build();
        loops.push((CoreRole::Dispatcher, Loops::Drain(weft_dispatcher::app::drain_loops(&state, registry)?)));
        weft_dispatcher::app::spawn_relays(&state);
        if let PlatformConfig::Local(local) = &config.platform {
            if local.listen.outside.is_some() {
                outside = Some(weft_dispatcher::api::outside_router(state.clone()));
            }
        }
        let api = weft_dispatcher::api::router(state.clone(), weft_dispatcher::app::cors_for(config.auth));
        let door = weft_dispatcher::door::router(state.clone(), api);
        // The projects that take work answer at their own address again:
        // a machine's fronts went with the install's last process. A
        // cloud's front is the platform's to keep, and outlives every boot
        // of the install. On a task of its own, so a front slow to start
        // holds up no other part of the boot; each project says on its row
        // and in the log what it came to.
        if matches!(config.platform, PlatformConfig::Local(_)) {
            let fronts = state.clone();
            tokio::spawn(async move {
                if let Err(e) = weft_dispatcher::front::serve_all(&fronts, weft_dispatcher::front::Say::Every).await {
                    tracing::error!(target: "weft_runtime", error = %format!("{e:#}"), "could not put the projects' fronts back; the reaper looks again shortly");
                }
            });
        }
        public = public.merge(door);
    }

    // The listener and the holder are one code; in a local install's one
    // process they are one state, so the listener answers for the
    // connections this same process holds.
    if runs(CoreRole::Listener) || runs(CoreRole::Holder) {
        // A local install's one process holds under a name that survives
        // its restart, so a new process takes its old claims back at once
        // instead of waiting for them to lapse. A cloud's holders are
        // copies that come and go, each its own.
        let replica = match only {
            None => format!("{}-local", config.install.label_value()),
            Some(_) => replica.clone(),
        };
        let token = weft_broker_client::TokenSource::role(parts.tokens.clone(), replica.clone(), CoreRole::Listener);
        let link = weft_broker_client::BrokerLink::new(addresses.broker.clone(), token);
        let tasks = weft_broker_client::BrokerTaskStoreClient::new(link.clone());
        let state = weft_listener::ListenerState::new(
            weft_listener::ListenerConfig {
                replica: replica.clone(),
                holds_here: runs(CoreRole::Holder),
                prefer_push: !matches!(config.platform, PlatformConfig::Local(_)),
            },
            tasks,
            link,
            parts.alarm.clone(),
            // An entry's event goes straight to its project's worker, with
            // the project's worker key.
            weft_listener::fire_sink::HttpWorkerDoors::new(install_secret()?),
        );
        if runs(CoreRole::Listener) {
            if only.is_none() {
                let rehydrating = state.clone();
                after_bind.push(Box::pin(async move {
                    // A row that cannot come up does not hold the boot: the
                    // other rows' signals run, and the listener keeps
                    // retrying it, logging each attempt and showing it down
                    // on its node. A cloud's wakes are kept by its alarm, so
                    // only the local process brings its rows back at boot.
                    weft_listener::registry::rehydrate(&rehydrating, None, &[])
                        .await
                        .map(|_down| ())
                        .context("bring back the signals")
                }));
            }
            internal = mount(internal, CoreRole::Listener, guard_for(CoreRole::Listener).guard(weft_listener::router(state.clone())));
        }
        if runs(CoreRole::Holder) {
            holding = Some(state);
        }
    }

    if runs(CoreRole::Supervisor) {
        let token = weft_broker_client::TokenSource::role(parts.tokens.clone(), replica.clone(), CoreRole::Supervisor);
        let state = weft_infra_supervisor::SupervisorState {
            broker: weft_broker_client::BrokerSupervisorClient::new(weft_broker_client::BrokerLink::new(addresses.broker.clone(), token)),
            replica: replica.clone(),
            host: parts.host.clone(),
            clock: Arc::new(weft_platform_traits::SystemClock),
            ownership_interval: weft_core::time_scale::scaled(SUPERVISOR_OWNERSHIP_EVERY),
            health_interval: weft_core::time_scale::scaled(SUPERVISOR_HEALTH_EVERY),
            health: Arc::default(),
            ownership_wanted: Arc::default(),
            project_locks: Arc::default(),
            pass: Arc::default(),
        };
        loops.push((CoreRole::Supervisor, Loops::Supervisor(state)));
    }

    // Where the loops run: for the life of the process, or once per tick.
    for (role, l) in loops {
        if serverless {
            let run: Arc<dyn Fn(Vec<String>) -> futures::future::BoxFuture<'static, Duration> + Send + Sync> = match l {
                // When each loop is due lives in the database, shared by
                // every instance of the role, since a tick may land on any.
                Loops::Drain(loops) => {
                    // A process that hears the database itself (a role that
                    // writes, so it is up with CPU of its own whenever an
                    // instance of it is) also runs its loops for as long as
                    // it lives, woken by what it hears, the way the local
                    // process does: a task written for it is picked up the
                    // moment it is announced, never after a ring's trip
                    // through the platform, which waits behind the tick
                    // already running. The tick still starts the role when it
                    // is at zero, and runs what is due; both claim the same
                    // rows, so whichever reaches a task first runs it.
                    if let Some(signals) = &signals {
                        for l in loops.clone() {
                            let subscription = signals.subscribe();
                            weft_dispatcher::app::spawn_supervised(l.name, async move { l.run_forever(subscription).await });
                        }
                    }
                    let loops = Arc::new(loops);
                    let (_, pool) = pool.as_ref().expect("a process with drain loops holds the database");
                    let pool = pool.clone();
                    Arc::new(move |woken| {
                        let (loops, pool) = (loops.clone(), pool.clone());
                        Box::pin(async move { weft_task_store::drain::drain_due(&pool, role.as_str(), &loops, &woken).await })
                    })
                }
                // The supervisor's one pass covers everything it does, and
                // answers when it next has something to look at; with
                // nothing anywhere, a command being issued wakes it.
                Loops::Supervisor(state) => Arc::new(move |_woken| {
                    let state = state.clone();
                    Box::pin(async move {
                        match weft_infra_supervisor::tick(&state).await {
                            Ok(next) => next.unwrap_or(weft_task_store::drain::IDLE_LOOK),
                            Err(e) => {
                                tracing::warn!(target: "weft_runtime", error = %format!("{e:#}"), "a supervisor tick failed; the next one retries");
                                state.health_interval
                            }
                        }
                    })
                }),
            };
            let t = server::Tick { role, run, alarm: parts.alarm.clone() };
            internal = mount(internal, role, guard_for(role).guard(server::tick_route(t)));
        } else {
            match l {
                Loops::Drain(loops) => {
                    let signals = signals.clone().expect("a process with drain loops listens");
                    for l in loops {
                        let subscription = signals.subscribe();
                        weft_dispatcher::app::spawn_supervised(l.name, async move { l.run_forever(subscription).await });
                    }
                }
                Loops::Supervisor(state) => {
                    weft_dispatcher::app::spawn_supervised("supervisor", async move {
                        if let Err(e) = weft_infra_supervisor::run_loops(state).await {
                            tracing::error!(target: "weft_runtime", error = %format!("{e:#}"), "the supervisor stopped");
                        }
                    });
                }
            }
        }
    }
    // Kept to let the rings still out be answered once the process is
    // told to stop.
    let waker_at_exit = waker.clone();
    if let (Some(waker), Some(signals)) = (waker, &signals) {
        let heard = signals.subscribe();
        weft_dispatcher::app::spawn_supervised("role_waker", async move {
            if let Err(e) = waker.run(heard).await {
                tracing::error!(target: "weft_runtime", error = %format!("{e:#}"), "the role waker stopped");
            }
        });
    }
    for (name, work) in parts.background {
        weft_dispatcher::app::spawn_supervised(name, async move {
            if let Err(e) = work.await {
                tracing::error!(target: "weft_runtime", work = name, error = %format!("{e:#}"), "platform work stopped");
            }
        });
    }

    // A holder in a pool takes no calls: it holds its share until it is
    // stopped, then gives its claims up so another holder takes them at
    // once.
    if placement == Some(Placement::Pool) {
        let state = holding.expect("only the holder runs in a pool");
        let room = Some(config.holders.signals_per_copy);
        tracing::info!(target: "weft_runtime", room = config.holders.signals_per_copy, "holding");
        weft_listener::hold::run(state, room, server::shutdown()).await;
        return Ok(());
    }

    // Bind before anything waits on another role: the listener's first
    // look at its signals goes through the broker, which may be this same
    // process.
    let serving = match &config.platform {
        // A service of one role: one port, the platform's.
        _ if serverless => {
            let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8080);
            let addr: std::net::SocketAddr = ([0, 0, 0, 0], port).into();
            let app = public.merge(internal);
            let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("bind {addr}"))?;
            tracing::info!(target: "weft_runtime", role = ?only, %addr, "serving");
            tokio::spawn(server::serve(listener, app))
        }
        PlatformConfig::Local(local) => {
            let internal_listener = tokio::net::TcpListener::bind(local.listen.internal)
                .await
                .with_context(|| format!("bind the internal port {}", local.listen.internal))?;
            let public_listener = tokio::net::TcpListener::bind(local.listen.public)
                .await
                .with_context(|| format!("bind the public port {}", local.listen.public))?;
            if let Some(state) = holding {
                // The local process holds every held connection itself,
                // under a name every local process of the install shares:
                // only once its ports are bound, which no second one of
                // them can be, does it take the claims.
                weft_dispatcher::app::spawn_supervised("hold", weft_listener::hold::run(state, None, std::future::pending()));
            }
            tracing::info!(target: "weft_runtime", roles = ?hosted, public = %local.listen.public, internal = %local.listen.internal, "serving");
            // The internal routes are reached on the public port too, for
            // a caller outside the machine (`Vantage::Public`). A local
            // install trusts every caller of its public API, and its
            // internal port is one containers reach, so that port stays
            // roles only.
            let front = public.nest(weft_platform_traits::roles::INTERNAL_DOOR, internal.clone());
            let mut servers = vec![tokio::spawn(server::serve(internal_listener, internal)), tokio::spawn(server::serve(public_listener, front))];
            if let (Some(addr), Some(app)) = (local.listen.outside, outside) {
                let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("bind the outside port {addr}"))?;
                servers.push(tokio::spawn(server::serve(listener, app)));
            }
            tokio::spawn(async move {
                for s in futures::future::join_all(servers).await {
                    s??;
                }
                Ok(())
            })
        }
        PlatformConfig::Gcp(_) => unreachable!("a cloud install's processes are each serverless or in a pool (checked above)"),
    };
    for work in after_bind {
        work.await?;
    }
    let served = serving.await;
    // The servers stop once the process is told to (`server::shutdown`),
    // or the task serving them ended; a ring still out would otherwise go
    // with the process either way.
    if let Some(waker) = waker_at_exit {
        waker.settle(role_waker::RING_GRACE).await;
    }
    served?
}
