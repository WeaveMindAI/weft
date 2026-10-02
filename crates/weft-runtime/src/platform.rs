//! The platform an install runs on, as the trait objects every role takes.
//!
//! The install config names the platform once; this is the only place
//! that reads which, and everything past it sees traits.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use weft_platform_traits::config::{InstallConfig, PlatformConfig};
use weft_platform_traits::{Alarm, CallerIdentity, IdentityTokens, ImageBuilder, InfraHost, Runner, Vantage};

/// What every role of this process takes from the platform.
pub struct Parts {
    pub runner: Arc<dyn Runner>,
    pub images: Arc<dyn ImageBuilder>,
    pub host: Arc<dyn InfraHost>,
    pub alarm: Arc<dyn Alarm>,
    /// Who is calling, for the internal endpoints this process serves.
    pub identity: Arc<dyn CallerIdentity>,
    /// This process's own identity, for its calls to other roles.
    pub tokens: Arc<dyn IdentityTokens>,
    /// Work the platform itself needs done for as long as the process
    /// lives (delivering local wakes, stopping idle local workers).
    pub background: Vec<(&'static str, futures::future::BoxFuture<'static, anyhow::Result<()>>)>,
}

/// How often idle local workers are looked for.
const IDLE_SWEEP: Duration = Duration::from_secs(30);

/// How often a local install bounds BuildKit's cache
/// (`weft_platform_local::bound_build_cache`).
const BUILD_CACHE_BOUND: Duration = Duration::from_secs(6 * 3600);

pub async fn build(config: &InstallConfig, pool: Option<&sqlx::PgPool>, caller_token_secret: &str) -> anyhow::Result<Parts> {
    match &config.platform {
        PlatformConfig::Local(local) => {
            let pool = pool.context("a local install's process runs every role, so it holds the database")?;
            let docker: Arc<dyn weft_platform_local::Docker> = Arc::new(weft_platform_local::DockerCli);
            let identity = Arc::new(weft_platform_local::LocalIdentity::from_env()?);
            let scratch = local.data_dir.join("run");
            let runner = Arc::new(weft_platform_local::LocalRunner::new(
                docker.clone(),
                identity.clone(),
                Arc::new(weft_platform_traits::SystemClock),
                weft_platform_local::LocalRunnerConfig {
                    broker_url: config.role_addresses(Vantage::Private).broker,
                    caller_token_secret: Some(caller_token_secret.to_string()),
                    idle_stop: Duration::from_secs(local.worker_idle_stop_seconds),
                    scratch_dir: scratch.clone(),
                    install: config.install.clone(),
                    time_scale: weft_core::time_scale::factor(),
                },
            ));
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
                },
            ));
            let alarm = Arc::new(weft_platform_local::LocalAlarm::new(pool.clone()));
            // A local install's one process is the machine's, which delivers
            // its wakes.
            let deliver: Arc<dyn weft_platform_local::Deliver> =
                Arc::new(weft_platform_local::HttpDeliver::new(config.role_addresses(Vantage::Machine), identity.clone()));
            let wakes = alarm.clone();
            let sweeper = runner.clone();
            let cache_docker = docker.clone();
            Ok(Parts {
                runner,
                images: Arc::new(weft_platform_local::DockerImageBuilder::new(docker, config.install.clone())),
                host,
                alarm,
                identity: identity.clone(),
                tokens: identity,
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
                runner: Arc::new(weft_platform_gcp::CloudRunRunner::new(google.clone(), gcp.clone(), config.role_addresses(Vantage::Private).broker, config.install.clone())),
                images: Arc::new(weft_platform_gcp::CloudBuildImages::new(google.clone(), gcp.clone())?),
                host: Arc::new(weft_platform_gcp::ComputeInfraHost::new(google.clone(), gcp.clone(), config.install.clone())),
                alarm: Arc::new(weft_platform_gcp::CloudTasksAlarm::new(google, gcp.clone(), config.role_addresses(Vantage::Public))),
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
