//! `weft-runtime`: the one process every install runs.
//!
//! Every role weft has is a library (`weft-dispatcher`, `weft-broker`,
//! `weft-listener`, `weft-infra-supervisor`); this crate is where they
//! are put together over the platform the install config names. On the
//! machine one process runs every role placed there, each under its own
//! path prefix, each role's loops running for as long as the process does.
//! A role placed anywhere else is a process of its own (`serve --role
//! <role>`) serving that one role at its root: placed serverless it runs
//! its loops once per tick and sets its next wake; on a machine of its own
//! they run for as long as the process does.
//!
//! - `platform`: the trait objects every role takes, per platform.
//! - `front_door`: HTTPS on a cloud machine's own address, with its
//!   certificates and routing by domain.
//! - `server`: the ports and what they carry.
//! - `guard`: the identity check on every internal route.
//! - `kick`: telling a role that work waits for it.
//! - `log_file`: a local install's bounded log file.
//! - `unit_agent`: the agent beside every infra unit.

pub mod front_door;
pub mod guard;
pub mod kick;
pub mod log_file;
pub mod platform;
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

/// A required secret from the environment (`SECRET_ENV`).
fn secret(name: &str) -> anyhow::Result<String> {
    std::env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("{name} is required; the install puts it in this process's environment"))
}

/// A role's loops, run where the role is placed.
enum Loops {
    Drain(Vec<DrainLoop>),
    Supervisor(weft_infra_supervisor::SupervisorState),
}

/// Run the roles of this process: every role placed on the machine, or,
/// with `only`, that one role, placed in a process of its own.
pub async fn serve(config: InstallConfig, only: Option<CoreRole>) -> anyhow::Result<()> {
    let hosted: Vec<CoreRole> = match only {
        Some(role) => {
            anyhow::ensure!(
                config.roles.of(role) != Placement::Machine,
                "--role {role} runs a role in a process of its own, and roles.{role} is on the machine; the machine's process already runs it"
            );
            vec![role]
        }
        None => config.roles.on_machine(),
    };
    // Whether the loops run once per tick (a role that scales to zero)
    // rather than for the life of the process.
    let serverless = only.is_some_and(|r| config.roles.of(r) == Placement::Serverless);
    // Where this process stands, which decides how it reaches every other
    // role: the machine's loopback is only the machine's.
    let here = match only {
        Some(role) => config.roles.of(role).vantage(),
        None => Vantage::Machine,
    };
    // A process of one role serves it at its root; the machine's process
    // serves each of its roles under the role's prefix.
    let mount = |on: Router, role: CoreRole, routes: Router| match only {
        Some(_) => on.merge(routes),
        None => on.nest(role.internal_prefix().expect("the machine's process mounts only roles with internal routes"), routes),
    };
    let runs = |r: CoreRole| hosted.contains(&r);
    let instance = weft_platform_traits::identity::mint_instance_id("runtime");
    let addresses = config.role_addresses(here);
    let caller_token_secret = secret("WEFT_CALLER_TOKEN_SECRET")?;
    let caller_secret_bytes =
        hex::decode(caller_token_secret.trim()).context("WEFT_CALLER_TOKEN_SECRET is not hex")?;

    // The database: for the roles that reach it, for a local install's
    // wakes, and for the machine's front door (the domains it serves).
    let needs_db = runs(CoreRole::Dispatcher)
        || runs(CoreRole::Broker)
        || matches!(config.platform, PlatformConfig::Local(_))
        || (only.is_none() && config.front_door.is_some());
    let pool = if needs_db {
        let url = secret("WEFT_DATABASE_URL")?;
        let pool = weft_dispatcher::journal::postgres::PostgresJournal::connect_pool(&url)
            .await
            .context("connect to the database")?;
        weft_dispatcher::app::apply_core_schema(&pool).await?;
        Some((url, pool))
    } else {
        None
    };
    let parts = platform::build(&config, pool.as_ref().map(|(_, p)| p), &caller_token_secret).await?;
    let kick: Arc<dyn weft_platform_traits::Kick> = Arc::new(kick::RoleKick::new(&config, &addresses, parts.tokens.clone()));

    // The process's one LISTEN connection, on every channel its roles
    // wait on.
    let signals = match &pool {
        Some((_, pool)) if runs(CoreRole::Dispatcher) || runs(CoreRole::Broker) => {
            let mut channels: Vec<&'static str> = Vec::new();
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
            Some(weft_task_store::pg_signal::PgSignalWatch::start(pool, channels).await.context("listen for Postgres signals")?)
        }
        _ => None,
    };

    let mut internal = Router::new();
    let mut public = Router::new();
    let mut outside: Option<Router> = None;
    let mut loops: Vec<(CoreRole, Loops)> = Vec::new();
    let mut after_bind: Vec<futures::future::BoxFuture<'static, anyhow::Result<()>>> = Vec::new();

    // The internal routes of the roles weft calls, behind the door, which
    // accepts every address a caller anywhere was given for the role.
    let door_for = |role: CoreRole| guard::CoreOnly { identity: parts.identity.clone(), audiences: Arc::new(config.role_audiences(role)) };

    if runs(CoreRole::Broker) {
        let (_, pool) = pool.as_ref().expect("the broker's process holds the database");
        let state = weft_broker::BrokerState::new(
            pool.clone(),
            signals.clone().expect("the broker's process listens"),
            weft_broker::state::BrokerSettings {
                auth: weft_broker::AuthConfig {
                    audiences: config.role_audiences(CoreRole::Broker),
                },
                identity: parts.identity.clone(),
                object_store: weft_platform_traits::object_store_for(&config.object_store).await?,
                entitlements: Arc::new(weft_broker::entitlement::LocalEntitlementSource::from_env()),
                credentials: Arc::new(weft_broker::credential::FileCredentialSource::from_env()),
                app_provider: Arc::new(weft_broker::app_provider::FileAppProvider::from_env()),
                public_base_url: config.public_url.clone(),
                internet_url: config.internet_url.clone(),
                object_store_public_internet: config.object_store.public_internet,
                kick: kick.clone(),
            },
        )
        .await?;
        loops.push((CoreRole::Broker, Loops::Drain(weft_broker::drain_loops(state.clone()))));
        internal = mount(internal, CoreRole::Broker, weft_broker::router(state));
    }

    if runs(CoreRole::Dispatcher) {
        let (url, pool) = pool.as_ref().expect("the dispatcher's process holds the database");
        let lock_pool = weft_dispatcher::journal::postgres::PostgresJournal::connect_pool_sized(url, 16, Duration::from_secs(120))
            .await
            .context("connect the lock pool")?;
        let state = weft_dispatcher::app::build_state(
            weft_dispatcher::app::DispatcherSettings {
                config: &config,
                instance: instance.clone(),
                pool: pool.clone(),
                lock_pool,
                signals: signals.clone().expect("the dispatcher's process listens"),
                runner: parts.runner.clone(),
                host: parts.host.clone(),
                images: parts.images.clone(),
                tokens: parts.tokens.clone(),
                kick: kick.clone(),
                caller_token_secret: caller_secret_bytes.clone(),
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
        if only.is_none() && config.listen.outside.is_some() {
            outside = Some(weft_dispatcher::api::outside_router(state.clone()));
        }
        public = public.merge(weft_dispatcher::api::router(state, weft_dispatcher::app::cors_for(config.auth)));
    } else if only.is_none() {
        // The machine passes the public API on to the dispatcher's own
        // service.
        let to = server::ToDispatcher {
            base_url: addresses.of(CoreRole::Dispatcher)?.to_string(),
            tokens: parts.tokens.clone(),
            http: reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build()?,
        };
        public = public.merge(Router::new().fallback(server::to_dispatcher).with_state(to));
    }

    if runs(CoreRole::Listener) {
        let token = weft_broker_client::TokenSource::role(parts.tokens.clone(), instance.clone(), CoreRole::Listener);
        let tasks = weft_broker_client::BrokerTaskStoreClient::new(addresses.broker.clone(), token.clone());
        let state = weft_listener::ListenerState::new(
            weft_listener::ListenerConfig {
                instance: instance.clone(),
                broker_url: addresses.broker.clone(),
                placement: config.roles.listener,
            },
            tasks,
            token,
            parts.alarm.clone(),
        );
        let rehydrating = state.clone();
        after_bind.push(Box::pin(async move {
            // A row that cannot come up does not hold the boot: the
            // other rows' triggers run, and the listener keeps retrying
            // it, logging each attempt and showing it down on its node.
            weft_listener::registry::rehydrate(&rehydrating, None, &[])
                .await
                .map(|_down| ())
                .context("bring back the held signals")
        }));
        internal = mount(internal, CoreRole::Listener, door_for(CoreRole::Listener).guard(weft_listener::router(state)));
    }

    if runs(CoreRole::Supervisor) {
        let token = weft_broker_client::TokenSource::role(parts.tokens.clone(), instance.clone(), CoreRole::Supervisor);
        let state = weft_infra_supervisor::SupervisorState {
            broker: weft_broker_client::BrokerSupervisorClient::new(addresses.broker.clone(), token),
            instance: instance.clone(),
            host: parts.host.clone(),
            clock: Arc::new(weft_platform_traits::SystemClock),
            ownership_interval: weft_core::time_scale::scaled(SUPERVISOR_OWNERSHIP_EVERY),
            health_interval: weft_core::time_scale::scaled(SUPERVISOR_HEALTH_EVERY),
            health: Arc::default(),
            ownership_wanted: Arc::default(),
            project_locks: Arc::default(),
        };
        loops.push((CoreRole::Supervisor, Loops::Supervisor(state)));
    }

    // Where the loops run: for the life of the process on the machine, or
    // once per tick.
    for (role, l) in loops {
        if serverless {
            let run: Arc<dyn Fn() -> futures::future::BoxFuture<'static, Duration> + Send + Sync> = match l {
                Loops::Drain(loops) => {
                    let loops = Arc::new(loops);
                    Arc::new(move || {
                        let loops = loops.clone();
                        Box::pin(async move { weft_task_store::drain::drain_all(&loops).await })
                    })
                }
                Loops::Supervisor(state) => Arc::new(move || {
                    let state = state.clone();
                    Box::pin(async move {
                        if let Err(e) = weft_infra_supervisor::tick(&state).await {
                            tracing::warn!(target: "weft_runtime", error = %format!("{e:#}"), "a supervisor tick failed; the next one retries");
                        }
                        state.health_interval
                    })
                }),
            };
            let t = server::Tick { role, run, alarm: parts.alarm.clone() };
            internal = mount(internal, role, door_for(role).guard(server::tick_route(t)));
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
    for (name, work) in parts.background {
        weft_dispatcher::app::spawn_supervised(name, async move {
            if let Err(e) = work.await {
                tracing::error!(target: "weft_runtime", work = name, error = %format!("{e:#}"), "platform work stopped");
            }
        });
    }

    // Bind before anything waits on another role: the listener's first
    // look at its signals goes through the broker, which may be this same
    // process.
    let serving = if only.is_some() {
        // One port for everything: the platform's on a service of its
        // own, the internal one on a machine of its own.
        let addr: std::net::SocketAddr = if serverless {
            let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8080);
            ([0, 0, 0, 0], port).into()
        } else {
            config.listen.internal
        };
        let app = public.merge(internal);
        let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("bind {addr}"))?;
        tracing::info!(target: "weft_runtime", role = ?only, %addr, "serving");
        tokio::spawn(server::serve(listener, app))
    } else {
        // A role on a machine of its own has no public address: the
        // machine passes its calls from outside (a cloud queue's wakes)
        // on to it.
        let http = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build()?;
        for role in CoreRole::ALL.into_iter().filter(|r| config.roles.of(*r) == Placement::OwnMachine) {
            let to = server::ToRole { base_url: addresses.of(role)?.to_string(), http: http.clone() };
            let prefix = role.internal_prefix().expect("only the listener has a machine of its own (InstallConfig::validate)");
            internal = internal.nest(prefix, Router::new().fallback(server::to_role).with_state(to));
        }
        let internal_listener = tokio::net::TcpListener::bind(config.listen.internal)
            .await
            .with_context(|| format!("bind the internal port {}", config.listen.internal))?;
        let public_listener = tokio::net::TcpListener::bind(config.listen.public)
            .await
            .with_context(|| format!("bind the public port {}", config.listen.public))?;
        tracing::info!(target: "weft_runtime", roles = ?hosted, public = %config.listen.public, internal = %config.listen.internal, "serving");
        let front = public.clone().nest(weft_platform_traits::roles::INTERNAL_DOOR, internal.clone());
        // The internal port also serves the public API when every call to
        // it carries its own operator key, so a frontend on the private
        // network has a private address for the install. A local install
        // trusts every caller of its public API, and its internal port is
        // one containers reach, so there it stays roles only. The public
        // API has no route under a role's prefix, so the two never meet.
        let internal = match config.auth {
            AuthMode::OperatorKeys => public.merge(internal),
            AuthMode::Local => internal,
        };
        let mut servers = vec![tokio::spawn(server::serve(internal_listener, internal))];
        if let Some(door) = config.front_door.clone() {
            let (_, pool) = pool.as_ref().expect("the machine's process holds the database when it has a front door");
            servers.push(front_door::start(door, pool.clone(), front.clone()).await?);
        }
        servers.push(tokio::spawn(server::serve(public_listener, front)));
        if let (Some(addr), Some(app)) = (config.listen.outside, outside) {
            let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("bind the outside port {addr}"))?;
            servers.push(tokio::spawn(server::serve(listener, app)));
        }
        tokio::spawn(async move {
            for s in futures::future::join_all(servers).await {
                s??;
            }
            Ok(())
        })
    };
    for work in after_bind {
        work.await?;
    }
    serving.await?
}
