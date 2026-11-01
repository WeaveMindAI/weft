//! The platform an install runs on, as the trait objects every role takes.
//!
//! The install config names the platform once; this is the only place
//! that reads which, and everything past it sees traits.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use weft_platform_traits::config::{InstallConfig, ObjectStoreSettings, PlatformConfig};
use weft_platform_traits::{Alarm, CallerIdentity, IdentityTokens, ImageBuilder, InfraHost, Runner, Vantage};

/// What every role of this process takes from the platform.
pub struct Parts {
    pub runner: Arc<dyn Runner>,
    pub images: Arc<dyn ImageBuilder>,
    /// Which of a program's nodes' needs the machine its workers run on
    /// lacks.
    pub needs: Arc<dyn weft_platform_traits::MachineNeeds>,
    pub host: Arc<dyn InfraHost>,
    pub alarm: Arc<dyn Alarm>,
    /// Where a project's frontend runs, when the install hosts it.
    pub frontends: Arc<dyn weft_platform_traits::FrontendHosting>,
    /// The door in front of the install's domains.
    pub domains: Arc<dyn weft_platform_traits::DomainHosting>,
    /// How many holders run.
    pub holder_pool: Arc<dyn weft_platform_traits::HolderPool>,
    /// Who is calling, for the internal endpoints this process serves.
    pub identity: Arc<dyn CallerIdentity>,
    /// This process's own identity, for its calls to other roles.
    pub tokens: Arc<dyn IdentityTokens>,
    /// The Google API client and its token cache, on GCP: one per process,
    /// shared by everything that calls Google (the object store too).
    pub google: Option<weft_platform_gcp::Google>,
    /// Work the platform itself needs done for as long as the process
    /// lives (delivering local wakes, stopping idle local workers).
    pub background: Vec<(&'static str, futures::future::BoxFuture<'static, anyhow::Result<()>>)>,
}

/// How often idle local workers are looked for.
const IDLE_SWEEP: Duration = Duration::from_secs(30);

/// How often a local install bounds BuildKit's cache
/// (`weft_platform_local::bound_build_cache`).
const BUILD_CACHE_BOUND: Duration = Duration::from_secs(6 * 3600);

pub async fn build(config: &InstallConfig, pool: Option<&weft_task_store::db::Pool>) -> anyhow::Result<Parts> {
    match &config.platform {
        PlatformConfig::Local(local) => {
            let pool = pool.context("a local install's process runs every role, so it holds the database")?;
            let docker: Arc<dyn weft_platform_local::Docker> = Arc::new(weft_platform_local::DockerCli);
            let identity = Arc::new(weft_platform_local::LocalIdentity::from_env()?);
            let scratch = local.data_dir.join("run");
            let runner = Arc::new(weft_platform_local::LocalRunner::new(
                Arc::new(weft_platform_local::MachineProcesses),
                Arc::new(weft_dispatcher::working_copies::FoldersHere::new(Arc::new(
                    weft_dispatcher::working_copies::PostgresWorkingCopyStore::new(pool.clone()),
                ))?),
                identity.clone(),
                Arc::new(weft_platform_traits::SystemClock),
                weft_platform_local::LocalRunnerConfig {
                    // A worker is a process on the machine.
                    broker_url: config.role_addresses(Vantage::Machine).broker,
                    install_secret: crate::install_secret()?,
                    idle_stop: local.worker_idle_stop(),
                    install: weft_platform_local::InstallLayout::new(local.data_dir.clone()),
                    workers_dir: weft_platform_local::MachineLayout::here().workers(),
                    machine_env: machine_env()?,
                    time_scale: weft_core::time_scale::factor(),
                    edge: config.edge,
                    project_ip: local.listen.public.ip(),
                },
            )?);
            let gpu = weft_platform_local::LocalInfraHost::detect_gpu(docker.as_ref()).await?;
            let host = Arc::new(weft_platform_local::LocalInfraHost::new(
                docker.clone(),
                weft_platform_local::LocalInfraHostConfig {
                    agent_image: local.runtime_image.clone(),
                    scratch_dir: scratch,
                    gpu,
                    disks: weft_platform_local::DiskBacking::Volumes,
                    publish: weft_platform_local::Publish::Loopback,
                    install: config.install.clone(),
                    // A unit's agent is a container, so it reaches the
                    // broker where containers do.
                    agent_broker: weft_platform_local::AgentBroker {
                        url: config.role_addresses(Vantage::Private).broker,
                        identity: weft_platform_local::AgentIdentity::Minted(identity.clone()),
                    },
                },
            ));
            let alarm = Arc::new(weft_platform_local::LocalAlarm::new(pool.clone()));
            // A local install's one process delivers its wakes.
            let deliver: Arc<dyn weft_platform_local::Deliver> =
                Arc::new(weft_platform_local::HttpDeliver::new(config.role_addresses(Vantage::Machine), identity.clone()));
            let wakes = alarm.clone();
            let sweeper = runner.clone();
            let cache_docker = docker.clone();
            Ok(Parts {
                runner,
                images: Arc::new(weft_platform_local::LocalImageBuilder::new(
                    docker,
                    Arc::new(weft_platform_local::MachineToolchain),
                    config.install.clone(),
                    weft_platform_local::InstallLayout::new(local.data_dir.clone()),
                    weft_platform_local::MachineLayout::here(),
                    weft_platform_local::lanes::lane_count()?,
                )),
                needs: Arc::new(weft_platform_local::LocalNeeds),
                host,
                alarm,
                frontends: Arc::new(weft_platform_local::NoFrontendHosting),
                domains: Arc::new(weft_platform_local::NoDomains),
                holder_pool: Arc::new(weft_platform_local::OneProcessHolds),
                identity: identity.clone(),
                tokens: identity,
                google: None,
                background: vec![
                    ("local_alarm", Box::pin(async move { wakes.run(deliver).await })),
                    (
                        "idle_workers",
                        Box::pin(async move {
                            loop {
                                tokio::time::sleep(IDLE_SWEEP).await;
                                if let Err(e) = sweeper.sweep_idle().await {
                                    tracing::warn!(target: "weft_runtime::platform", error = %format!("{e:#}"), "could not stop idle workers");
                                }
                            }
                        }),
                    ),
                    (
                        "build_cache",
                        Box::pin(async move {
                            loop {
                                if let Err(e) = weft_platform_local::bound_build_cache(cache_docker.as_ref()).await {
                                    tracing::warn!(target: "weft_runtime::platform", error = %format!("{e:#}"), "could not bound the BuildKit cache");
                                }
                                tokio::time::sleep(BUILD_CACHE_BOUND).await;
                            }
                        }),
                    ),
                ],
            })
        }
        PlatformConfig::Gcp(gcp) => {
            let gcp: &weft_platform_traits::config::GcpPlatform = gcp;
            let tokens = Arc::new(weft_platform_gcp::MetadataTokens::new());
            let google = weft_platform_gcp::Google::new(tokens.clone());
            Ok(Parts {
                runner: Arc::new(weft_platform_gcp::CloudRunRunner::new(
                    google.clone(),
                    gcp.clone(),
                    config.role_addresses(Vantage::Private).broker,
                    config.install.clone(),
                    config.edge,
                    crate::install_secret()?,
                )),
                images: Arc::new(weft_platform_gcp::CloudBuildImages::new(google.clone(), gcp.clone())?),
                needs: Arc::new(weft_platform_gcp::ImageNeeds),
                host: Arc::new(weft_platform_gcp::ComputeInfraHost::new(
                    google.clone(),
                    gcp.clone(),
                    config.install.clone(),
                    config.role_addresses(Vantage::Private).broker,
                )),
                alarm: Arc::new(weft_platform_gcp::CloudTasksAlarm::new(google.clone(), gcp.clone(), config.role_addresses(Vantage::Public))),
                frontends: Arc::new(weft_platform_gcp::CloudRunFrontends::new(google.clone(), gcp.clone())),
                domains: Arc::new(weft_platform_gcp::LoadBalancerDomains::new(google.clone(), gcp.clone())),
                holder_pool: Arc::new(weft_platform_gcp::WorkerPoolHolders::new(google.clone(), gcp.holder_pool.clone())),
                google: Some(google),
                identity: Arc::new(weft_platform_gcp::GoogleIdentity::new(
                    gcp.project.clone(),
                    gcp.core_service_account.clone(),
                    pool.cloned(),
                )),
                tokens,
                background: Vec::new(),
            })
        }
    }
}

/// What a local worker gets of this process's environment: the `PATH`
/// that finds the person's programs (the one the needs check looks on) and
/// `HOME`. Both are there in any process a person or their service
/// manager starts; one missing is said.
fn machine_env() -> anyhow::Result<std::collections::BTreeMap<String, String>> {
    ["PATH", "HOME"]
        .into_iter()
        .map(|name| {
            let value = std::env::var(name).ok().filter(|v| !v.is_empty()).with_context(|| {
                format!("{name} is not set in the runtime's environment, and a local worker gets the machine's {name} to find the person's programs")
            })?;
            Ok((name.to_string(), value))
        })
        .collect()
}

/// Where this process writes the line weft logs when its own work is stuck
/// (`weft_platform_traits::ALERT_TARGET`): on Google Cloud, when the
/// install alerts, the line its alert policy matches; nowhere otherwise.
pub fn alert_lines(config: &InstallConfig) -> Option<Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync>> {
    match (&config.platform, config.alert_after()) {
        (PlatformConfig::Gcp(_), Some(_)) => Some(Box::new(weft_platform_gcp::alert_line::layer())),
        (PlatformConfig::Gcp(_), None) | (PlatformConfig::Local(_), _) => None,
    }
}

/// How this process tells the install's other copies what happened to a
/// run, and hears it (`weft_platform_traits::news`): on a machine, nothing
/// (its one process listens to the database for good); on Google Cloud, the
/// install's Pub/Sub topic.
pub fn news(config: &InstallConfig, parts: &Parts) -> anyhow::Result<Arc<dyn weft_platform_traits::News>> {
    Ok(match &config.platform {
        PlatformConfig::Local(_) => Arc::new(weft_platform_local::DatabaseNews::default()),
        PlatformConfig::Gcp(gcp) => {
            let google = parts.google.clone().context("a process on Google Cloud holds its Google client")?;
            Arc::new(weft_platform_gcp::PubSubNews::new(google, gcp))
        }
    })
}

/// What the holders hear (the held set changed, a connection was made
/// again or removed), told by the dispatcher: on a local install, relayed
/// from the one process's own database watch; on Google Cloud, the
/// holders' topic.
pub fn holder_news(
    config: &InstallConfig,
    parts: &Parts,
    signals: Option<&Arc<weft_task_store::pg_signal::PgSignalWatch>>,
) -> anyhow::Result<Arc<dyn weft_platform_traits::HolderNews>> {
    Ok(match &config.platform {
        PlatformConfig::Local(_) => Arc::new(weft_platform_local::DatabaseHolderNews::new(
            signals.context("a local install's process listens to its database")?.clone(),
            weft_dispatcher::holders::HELD_SIGNALS_CHANNEL,
            weft_dispatcher::holders::ACCESS_REMADE_CHANNEL,
        )),
        PlatformConfig::Gcp(gcp) => {
            let google = parts.google.clone().context("a process on Google Cloud holds its Google client")?;
            Arc::new(weft_platform_gcp::PubSubHolderNews::new(google, gcp))
        }
    })
}

/// How a dead holder is noticed: a check it keeps pushing ahead through the
/// alarm on Google Cloud; nothing on a local install, whose one process
/// holds under a name it takes back at its restart.
pub fn holder_liveness(config: &InstallConfig, parts: &Parts) -> Arc<dyn weft_platform_traits::HolderLiveness> {
    match &config.platform {
        PlatformConfig::Local(_) => Arc::new(weft_platform_local::OneProcessHolds),
        PlatformConfig::Gcp(_) => Arc::new(weft_platform_traits::AlarmLiveness::new(parts.alarm.clone())),
    }
}

/// How long this process's platform lets one request live before cutting
/// it, which is how long a line to it lasts: a role placed serverless on
/// Google Cloud is a Cloud Run service; anything else keeps a request for
/// good.
pub fn line_max_age(config: &InstallConfig, serverless: bool) -> Option<Duration> {
    match (&config.platform, serverless) {
        (PlatformConfig::Gcp(_), true) => Some(weft_platform_gcp::REQUEST_CAP),
        _ => None,
    }
}

/// How the platform bounds a role placed serverless, as its passes and
/// its stopping need to know.
#[derive(Debug, Clone, Copy)]
pub struct ServerlessLimits {
    /// How long one tick's pass drains before it answers
    /// (`weft_task_store::drain::drain_due`'s `slice`): well inside the
    /// platform's cut of a request, so a loop that always has more never
    /// holds the tick open until it is cut.
    pub tick_slice: Duration,
    /// How long a stopping process waits for its rings still out
    /// (`crate::role_waker::RoleWaker::settle`): inside the time the
    /// platform gives a stopping process, with room to log what was not
    /// delivered.
    pub ring_grace: Duration,
}

/// On Google Cloud a role placed serverless is a Cloud Run service: it cuts
/// a request at `weft_platform_gcp::REQUEST_CAP` (an hour), and gives a
/// stopping instance `weft_platform_gcp::STOP_GRACE` after SIGTERM, of
/// which the rings take all but 2 seconds.
const GCP_SERVERLESS: ServerlessLimits = ServerlessLimits {
    tick_slice: Duration::from_secs(60),
    ring_grace: Duration::from_secs(weft_platform_gcp::STOP_GRACE.as_secs() - 2),
};
const _: () = assert!(GCP_SERVERLESS.tick_slice.as_secs() < weft_platform_gcp::REQUEST_CAP.as_secs());

/// The bounds of a role placed serverless on this install's platform;
/// `None` on a platform that places no role so (a local install runs every
/// role in its one process).
pub fn serverless_limits(config: &InstallConfig) -> Option<ServerlessLimits> {
    match &config.platform {
        PlatformConfig::Gcp(_) => Some(GCP_SERVERLESS),
        PlatformConfig::Local(_) => None,
    }
}

/// The object store the install config names. A Cloud Storage bucket is
/// reached through the platform's own Google client, so only on GCP.
pub async fn object_store(settings: &ObjectStoreSettings, parts: &Parts) -> anyhow::Result<weft_platform_traits::SharedObjectStore> {
    Ok(match settings {
        ObjectStoreSettings::S3(s3) => weft_platform_traits::object_store_for(s3).await?,
        ObjectStoreSettings::Gcs(gcs) => {
            let google = parts
                .google
                .clone()
                .context("objectStore is a Cloud Storage bucket (`kind: gcs`), which an install off GCP cannot reach")?;
            Arc::new(weft_platform_gcp::GcsObjectStore::new(google, gcs.bucket.clone()).await?)
        }
    })
}
