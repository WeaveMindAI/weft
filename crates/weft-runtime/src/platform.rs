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

pub async fn build(config: &InstallConfig, pool: Option<&sqlx::PgPool>) -> anyhow::Result<Parts> {
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
                    // A local worker gets the ticket secret from the runner
                    // that starts it; a GCP one reads it from Secret Manager.
                    caller_token_secret: crate::secret("WEFT_CALLER_TOKEN_SECRET")?,
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
            // A local install's one process delivers its wakes.
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
                runner: Arc::new(weft_platform_gcp::CloudRunRunner::new(google.clone(), gcp.clone(), config.role_addresses(Vantage::Private).broker, config.install.clone())),
                images: Arc::new(weft_platform_gcp::CloudBuildImages::new(google.clone(), gcp.clone())?),
                host: Arc::new(weft_platform_gcp::ComputeInfraHost::new(google.clone(), gcp.clone(), config.install.clone())),
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
