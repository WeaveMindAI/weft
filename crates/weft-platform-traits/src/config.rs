//! The install's configuration: one file every role reads at boot.
//!
//! `weft daemon start` writes it for a local install; the cloud install
//! writes it for a cloud one. It holds every setting that is not a secret
//! (secrets reach the process through its environment, which the host
//! injects: see `SECRET_ENV`). Every size and placement in it is a lever:
//! changing it and restarting (or redeploying the affected service) is how
//! an install grows.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use weft_core::infra::Install;

use crate::roles::{CoreRole, Placement, RoleAddresses, RolePlacement, Vantage, INTERNAL_DOOR};
use weft_core::workers::{WorkerOverrides, WorkerSettings};

// SYNC: InstallConfig <-> deploy/terraform/gcp/serverless.tf (local.install_config)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallConfig {
    /// Which install this is when a machine holds several.
    #[serde(default)]
    pub install: Install,
    pub platform: PlatformConfig,
    /// The stable address people and editors reach the install at.
    #[serde(rename = "publicUrl")]
    pub public_url: String,
    /// An ADDITIONAL address the open internet reaches the install at (a
    /// tunnel's), when the public one is not internet-reachable.
    #[serde(default, rename = "internetUrl", skip_serializing_if = "Option::is_none")]
    pub internet_url: Option<String>,
    #[serde(default)]
    pub roles: RolePlacement,
    /// The internal address of each role placed `serverless`: the root its
    /// service serves the role at. Required for exactly those roles.
    #[serde(default, rename = "roleUrls", skip_serializing_if = "BTreeMap::is_empty")]
    pub role_urls: BTreeMap<CoreRole, String>,
    /// The worker levers this install sets for every project; each one it
    /// leaves out follows weft's own default ([`Self::worker_settings`]),
    /// so a default weft changes reaches the install.
    #[serde(default, skip_serializing_if = "WorkerOverrides::is_empty")]
    pub workers: WorkerOverrides,
    /// How the holders share the held connections, where they run in a
    /// pool.
    #[serde(default)]
    pub holders: HolderSettings,
    /// The most copies the platform runs at once of each role that opens
    /// the database ([`DATABASE_ROLES`]) placed serverless, one entry
    /// each: every copy sizes its pools to an equal share of what the
    /// database takes (`weft_task_store::budget`), so this is how many
    /// shares there are. Empty on a local install, whose one process takes
    /// the whole database.
    // SYNC: maxCopies <-> deploy/terraform/gcp/serverless.tf (local.max_copies)
    #[serde(default, rename = "maxCopies", skip_serializing_if = "BTreeMap::is_empty")]
    pub max_copies: BTreeMap<CoreRole, u32>,
    pub edge: EdgeConfig,
    #[serde(rename = "objectStore")]
    pub object_store: ObjectStoreSettings,
    /// The weft source the install runs (`owner/name` and commit), which a
    /// project's CI asks for to build the same CLI, and a CLI from another
    /// commit is warned against. Absent on a local install.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<weft_core::install::WeftSource>,
    /// How many minutes weft's own work may keep failing while weft
    /// still retries it before it counts as stuck: an install that alerts
    /// ([`Self::alert_after`]) alerts then.
    #[serde(default = "default_alert_after_minutes", rename = "alertAfterMinutes")]
    pub alert_after_minutes: u32,
}

/// The roles that open the install's database themselves: every other role
/// reaches it through the broker.
pub const DATABASE_ROLES: [CoreRole; 2] = [CoreRole::Dispatcher, CoreRole::Broker];

/// [`InstallConfig::alert_after_minutes`] when the config leaves it out.
pub const DEFAULT_ALERT_AFTER_MINUTES: u32 = 5;

fn default_alert_after_minutes() -> u32 {
    DEFAULT_ALERT_AFTER_MINUTES
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PlatformConfig {
    Local(Box<LocalPlatform>),
    Gcp(Box<GcpPlatform>),
}

/// A laptop or a server: plain processes and the local Docker daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalPlatform {
    /// Where the install keeps its files.
    #[serde(rename = "dataDir")]
    pub data_dir: std::path::PathBuf,
    /// The machine's internal port as a container reaches it
    /// (`http://host.docker.internal:14113`): the base an infra
    /// container's agent calls the broker under, and the one a link to a
    /// stored file for a project's containers goes through
    /// (`InstallConfig::container_files_base`).
    #[serde(rename = "containerInternalUrl")]
    pub container_internal_url: String,
    /// The image `weft-runtime` runs from (the agent beside every infra
    /// unit).
    #[serde(rename = "runtimeImage")]
    pub runtime_image: String,
    /// How long a worker with nothing to do stays up before it is
    /// stopped, when this install sets it ([`Self::worker_idle_stop`]).
    /// Raise it to keep workers warm longer between runs.
    #[serde(default, rename = "workerIdleStopSeconds", skip_serializing_if = "Option::is_none")]
    pub worker_idle_stop_seconds: Option<u64>,
    /// The ports the install's one process serves.
    pub listen: Listen,
    /// The ports the install gives its projects, one each, on the public
    /// port's address: a project's own address, serving its routes at its
    /// root. Unset, each project's port is a free one the machine picks
    /// the first time (an install beside another, whose block is taken).
    #[serde(default, rename = "projectPorts", skip_serializing_if = "Option::is_none")]
    pub project_ports: Option<PortRange>,
    /// Where the internal port is reached from the machine itself
    /// (`http://127.0.0.1:14113`): the base every role answers under, each
    /// at its own prefix, for a caller on the machine (this process, a
    /// project's worker). A container reaches it at
    /// `container_internal_url`.
    #[serde(rename = "internalUrl")]
    pub internal_url: String,
    /// Where devices on this machine's network reach the outside doors
    /// (`http://192.168.1.20:<outside port>`), when the person set a
    /// network address (`WEFT_NETWORK_ADDRESS`): the outside doors then
    /// listen a second time, on that address alone (`Listen::network`),
    /// and a link to a stored file for a project with no visitor address
    /// and no tunnel is built on it, so a phone on the same network
    /// opens it.
    #[serde(default, rename = "networkUrl", skip_serializing_if = "Option::is_none")]
    pub network_url: Option<String>,
    /// Where the install's quick tunnel says its address, when its tunnel
    /// is a quick one (`http://127.0.0.1:<port>`, cloudflared's metrics
    /// server). A quick tunnel's address changes whenever it reconnects,
    /// so the address is asked here each time a link is built on it,
    /// never kept from the start.
    #[serde(default, rename = "quickTunnelMetrics", skip_serializing_if = "Option::is_none")]
    pub quick_tunnel_metrics: Option<String>,
}

/// The address the open internet reaches an install at, as it is right
/// now ([`InstallConfig::internet_address`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InternetAddress {
    /// The open internet does not reach the install.
    None,
    /// A stable address: a named tunnel's, a domain's.
    Fixed(String),
    /// A quick tunnel, whose address is asked of cloudflared's metrics
    /// server at `metrics` each time.
    QuickTunnel { metrics: String },
}

impl InternetAddress {
    /// The address, as it is now. A quick tunnel that does not answer is
    /// an error naming it: a link through a tunnel that is down would
    /// work for nobody.
    pub async fn current(&self, http: &reqwest::Client) -> anyhow::Result<Option<String>> {
        match self {
            Self::None => Ok(None),
            Self::Fixed(url) => Ok(Some(url.trim_end_matches('/').to_string())),
            Self::QuickTunnel { metrics } => {
                #[derive(Deserialize)]
                struct Said {
                    hostname: String,
                }
                let asked = format!("{}/quicktunnel", metrics.trim_end_matches('/'));
                let said: Said = async {
                    let resp = http.get(&asked).timeout(std::time::Duration::from_secs(5)).send().await?;
                    resp.error_for_status()?.json().await
                }
                .await
                .map_err(|e: reqwest::Error| {
                    anyhow::anyhow!(
                        "weft's quick tunnel does not say its address at {asked} ({e}); `weft daemon start` opens it again"
                    )
                })?;
                anyhow::ensure!(
                    !said.hostname.trim().is_empty(),
                    "weft's quick tunnel has no address yet ({asked}); it is still connecting"
                );
                Ok(Some(format!("https://{}", said.hostname.trim())))
            }
        }
    }
}

/// How long a local worker with nothing to do stays up, unless the install
/// says otherwise: long enough to serve the next call of someone trying
/// things out without starting again, short enough that a laptop is not
/// left running a container for nothing.
pub const DEFAULT_WORKER_IDLE_STOP: std::time::Duration = std::time::Duration::from_secs(30);

impl LocalPlatform {
    /// How long a worker with nothing to do stays up.
    pub fn worker_idle_stop(&self) -> std::time::Duration {
        self.worker_idle_stop_seconds.map_or(DEFAULT_WORKER_IDLE_STOP, std::time::Duration::from_secs)
    }
}

/// Google Cloud, with no machine of the install's own: weft's roles and
/// a project's workers on Cloud Run (the holder on a worker pool), builds
/// on Cloud Build, wakes on Cloud Tasks, infra on Compute Engine, the
/// database wherever its URL points.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcpPlatform {
    /// The prefix of every resource the install makes for itself
    /// (`weft`), so two installs can share a project.
    pub name: String,
    pub project: String,
    pub region: String,
    /// The zone infra machines run in.
    pub zone: String,
    /// The VPC network and subnet everything private lives on, as
    /// resource paths (`projects/<p>/global/networks/<n>`,
    /// `projects/<p>/regions/<r>/subnetworks/<s>`).
    pub network: String,
    pub subnet: String,
    /// The Artifact Registry repository images live in
    /// (`us-central1-docker.pkg.dev/<project>/<repo>`).
    #[serde(rename = "artifactRegistry")]
    pub artifact_registry: String,
    /// The bucket build contexts are staged in for Cloud Build.
    #[serde(rename = "buildBucket")]
    pub build_bucket: String,
    /// The service account Cloud Build runs a project's build as.
    #[serde(rename = "builderServiceAccount")]
    pub builder_service_account: String,
    /// The prebuilt builder base a project's worker image compiles in
    /// (`weft build-images` makes it; the install workflow passes its ref).
    #[serde(rename = "builderBaseImage")]
    pub builder_base_image: String,
    /// The Cloud Tasks queue what keeps a user's triggers going goes
    /// through (their timers, crons, polls and provider renewals, and a
    /// holder's liveness check), retried with no time limit unless the
    /// install sets one (`crate::alarm::WakeQueue::Triggers`).
    #[serde(rename = "tasksQueue")]
    pub tasks_queue: String,
    /// How many deliveries of one trigger's wake in `tasks_queue` the
    /// listener answers while it keeps failing (`crate::alarm::Delivery`;
    /// one that never reached it is not counted): a failure on the last
    /// gives up on the trigger the wake was for, shown on its node, until
    /// it is registered again. The listener's alone: the holder's liveness
    /// check is retried for as long as the queue retries it.
    #[serde(rename = "wakeAttempts")]
    pub wake_attempts: u32,
    /// The Cloud Tasks queue weft's own wakes go through (a role's loop
    /// coming due, a ring that failed), retried briefly
    /// (`crate::alarm::WakeQueue::Internal`).
    #[serde(rename = "ticksQueue")]
    pub ticks_queue: String,
    /// The Pub/Sub topic news goes through (`crate::news`).
    #[serde(rename = "newsTopic")]
    pub news_topic: String,
    /// The Pub/Sub topic the holders hear on: the set of held signals
    /// changed, or a connection's access did.
    #[serde(rename = "holdersTopic")]
    pub holders_topic: String,
    /// The service account weft's own roles run as.
    #[serde(rename = "coreServiceAccount")]
    pub core_service_account: String,
    /// The Cloud Run worker pool the holder runs on
    /// (`projects/<p>/locations/<r>/workerPools/<n>`), whose size weft
    /// sets to what the held signals need.
    #[serde(rename = "holderPool")]
    pub holder_pool: String,
    /// The dispatcher's Cloud Run service name, which a domain's load
    /// balancer sends its requests to.
    #[serde(rename = "dispatcherService")]
    pub dispatcher_service: String,
    /// The image `weft-runtime` runs from (serverless roles, infra agents).
    #[serde(rename = "runtimeImage")]
    pub runtime_image: String,
    /// The service account a project's CI deploys a frontend as.
    #[serde(rename = "deployerServiceAccount")]
    pub deployer_service_account: String,
    /// The service account a project's frontend runs as.
    #[serde(rename = "frontendServiceAccount")]
    pub frontend_service_account: String,
    /// The Workload Identity Federation provider GitHub Actions
    /// authenticates through.
    #[serde(rename = "workloadIdentityProvider")]
    pub workload_identity_provider: String,
    /// The network tag every infra machine carries: the firewall opens its
    /// ports to the install's workers, roles and other infra machines, and
    /// to nothing else.
    #[serde(rename = "infraNetworkTag")]
    pub infra_network_tag: String,
    /// The Cloud Build machine a project's worker image compiles on (a
    /// `machineType`, `E2_HIGHCPU_8`), or `None` for Cloud Build's own
    /// default, the one its free build minutes cover. A bigger one compiles
    /// faster and is paid for by the minute. An image is built once per
    /// version of a program, never per worker started.
    #[serde(default, rename = "buildMachine", skip_serializing_if = "Option::is_none")]
    pub build_machine: Option<String>,
    /// Whether weft logs its stuck failures as alerts
    /// ([`InstallConfig::alert_after`]), which the install's alert policy
    /// emails; off when the install made no policy.
    pub alerts: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listen {
    /// The public port: the management API, the public door, the relay.
    pub public: SocketAddr,
    /// The internal port, on loopback: every role's internal endpoints,
    /// for the processes on this machine (its workers). The runtime also
    /// opens it on the address the project's containers reach this
    /// machine at, when that is not loopback, which it asks Docker on
    /// every start (`weft_platform_local::docker::containers_call_in_at`),
    /// never on every interface.
    pub internal: SocketAddr,
    /// A port carrying only the doors outside callers use, for a tunnel
    /// that brings the open internet to an install whose management API
    /// must stay off it (a local install, which trusts every caller of its
    /// public port).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outside: Option<SocketAddr>,
    /// The outside doors again, on the address devices on this machine's
    /// network reach it at, when a network address is set
    /// (`LocalPlatform::network_url`). Its callers connect straight to it,
    /// so it trusts no forwarded address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<SocketAddr>,
}

/// A block of ports, both ends included.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortRange {
    pub first: u16,
    pub last: u16,
}

impl PortRange {
    pub fn ports(self) -> std::ops::RangeInclusive<u16> {
        self.first..=self.last
    }
}

/// How the holders in a pool share the held connections
/// (`crate::holder_pool`). The local install's one process holds every
/// one of them whatever this says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HolderSettings {
    /// The most held signals one holder takes. Lower it when holders run
    /// short of memory or of connections; each more holder costs one more
    /// copy up.
    #[serde(default = "HolderSettings::default_signals_per_copy", rename = "signalsPerCopy")]
    pub signals_per_copy: u32,
    /// A holder whose CPU, averaged over a minute, reaches this share
    /// (percent of the CPUs it is given) hands part of its signals over,
    /// keeping what brings it back to [`Self::target_cpu_percent`], to a
    /// spare when one is up and else to any holder; one connection is
    /// never split. 70 leaves a busy holder room for a burst while another
    /// takes them. Under it, a holder over the target hands work over only
    /// to a spare.
    #[serde(default = "HolderSettings::default_high_cpu_percent", rename = "highCpuPercent")]
    pub high_cpu_percent: u8,
    /// The CPU each holder is meant to run at (percent of the CPUs it is
    /// given): weft runs as many holders as the load they say they carry,
    /// summed, needs at this share each. It must stay at least two bands
    /// ([`LOAD_BAND_PERCENT`]) under [`Self::high_cpu_percent`] (50 against
    /// 70): a holder says its load again only once it moved a band, so two
    /// holders merged into one when one stops can carry up to a band each
    /// more than they said, and the one left must still be under the high
    /// mark, or it hands work over at once, a copy comes back, and the pool
    /// grows and shrinks for good.
    #[serde(default = "HolderSettings::default_target_cpu_percent", rename = "targetCpuPercent")]
    pub target_cpu_percent: u8,
}

/// How far (percent of its CPUs) a holder's load moves before it says it
/// again: a load that stays within a band of what it last said writes
/// nothing (`weft_listener::load`).
pub const LOAD_BAND_PERCENT: u8 = 10;

impl HolderSettings {
    fn default_signals_per_copy() -> u32 {
        200
    }

    fn default_high_cpu_percent() -> u8 {
        70
    }

    fn default_target_cpu_percent() -> u8 {
        50
    }
}

impl Default for HolderSettings {
    fn default() -> Self {
        Self {
            signals_per_copy: Self::default_signals_per_copy(),
            high_cpu_percent: Self::default_high_cpu_percent(),
            target_cpu_percent: Self::default_target_cpu_percent(),
        }
    }
}

/// What the install allows at its public edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeConfig {
    /// How many proxies in front of each listener append to
    /// `X-Forwarded-For`. The caller's address is the entry that many
    /// hops from the right, so a caller cannot forge it by sending the
    /// header.
    #[serde(rename = "trustedProxyHops")]
    pub trusted_proxy_hops: ProxyHops,
    /// Refused tokens one address may present per minute on the token
    /// doors before every token door refuses it for the rest of the
    /// minute. `null` turns the block off; the key is required, so the
    /// block is never off by omission (a `deserialize_with` keeps serde
    /// from reading a missing `Option` as `None`).
    #[serde(rename = "invalidTokensPerMinute", deserialize_with = "Option::deserialize")]
    pub invalid_tokens_per_minute: Option<u32>,
}

/// The bound on refused tokens as a worker's environment spells it
/// (`WEFT_INVALID_TOKENS_PER_MINUTE`): a count, or `none`.
// SYNC: the spelling <-> crates/weft-engine/src/door/mod.rs (Edge::from_env)
pub fn invalid_tokens_env(bound: Option<u32>) -> String {
    bound.map_or_else(|| "none".to_string(), |n| n.to_string())
}

/// The trusted proxy hops in front of each listener that serves outside
/// callers: they differ because one install can be reached through
/// different paths (a local install's public port straight from the
/// machine, its outside port through a tunnel that appends the caller).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyHops {
    /// In front of [`Listen::public`] (0 when callers connect straight to it).
    pub public: usize,
    /// In front of [`Listen::outside`].
    pub outside: usize,
    /// In front of the public door for a request that came by one of the
    /// install's own domains (`weft domain add`), through the door the
    /// platform puts in front of them (`crate::DomainHosting`).
    pub domains: usize,
}

/// The object store: one bucket, and how the install reaches it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObjectStoreSettings {
    /// Any S3-compatible bucket, reached with a key pair
    /// (`WEFT_OBJECT_STORE_ACCESS_KEY` and `WEFT_OBJECT_STORE_SECRET_KEY`).
    S3(S3StoreSettings),
    /// A Cloud Storage bucket, reached as the process's own service
    /// account: no key exists for it.
    Gcs(GcsStoreSettings),
}

impl ObjectStoreSettings {
    /// Whether the open internet reaches the store's presigned URLs, so a
    /// shared file link can hand one out instead of relaying the bytes.
    pub fn public_internet(&self) -> bool {
        match self {
            Self::S3(s) => s.public_internet,
            // Cloud Storage's signed URLs answer anyone holding one.
            Self::Gcs(_) => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3StoreSettings {
    pub endpoint: String,
    pub bucket: String,
    #[serde(default = "S3StoreSettings::default_region")]
    pub region: String,
    #[serde(default = "yes", rename = "forcePathStyle")]
    pub force_path_style: bool,
    /// The endpoint presigned URLs are signed for, when outside callers
    /// reach the store at another address than the install does.
    #[serde(default, rename = "publicEndpoint", skip_serializing_if = "Option::is_none")]
    pub public_endpoint: Option<String>,
    /// Whether the open internet reaches the store's presigned URLs.
    #[serde(default, rename = "publicInternet")]
    pub public_internet: bool,
}

impl S3StoreSettings {
    fn default_region() -> String {
        "us-east-1".into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcsStoreSettings {
    pub bucket: String,
}

fn yes() -> bool {
    true
}

/// Every secret the runtime reads from its environment (the object
/// store's two keys only for an `s3` store, the listen URL only when the
/// database URL goes through a pooler that cannot hold a `LISTEN`).
// SYNC: SECRET_ENV <-> crates/weft-cli/src/commands/daemon.rs (local
//       secrets file), deploy/terraform/gcp/secrets.tf
pub const SECRET_ENV: &[&str] = &[
    "WEFT_DATABASE_URL",
    "WEFT_DATABASE_LISTEN_URL",
    "CREDENTIAL_ENCRYPTION_KEY",
    "WEFT_IDENTITY_KEY",
    "WEFT_CALLER_TOKEN_SECRET",
    weft_core::signal_token::INSTALL_KEY_SECRET,
    crate::object_store::OBJECT_STORE_ACCESS_KEY_ENV,
    crate::object_store::OBJECT_STORE_SECRET_KEY_ENV,
];

impl InstallConfig {
    /// Read and check the config file at `path`.
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read the install config {}: {e}", path.display()))?;
        let cfg: Self = serde_json::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("the install config {} is not valid: {e}", path.display()))?;
        cfg.validate().map_err(|e| anyhow::anyhow!("the install config {}: {e}", path.display()))?;
        Ok(cfg)
    }

    /// The worker settings every project starts from: weft's defaults with
    /// the levers this install sets.
    pub fn worker_settings(&self) -> WorkerSettings {
        WorkerSettings::default().with(&self.workers)
    }

    /// How long a failure weft still retries is current before it counts
    /// as stuck ([`Self::alert_after_minutes`]), at this install's pace
    /// (`weft_core::time_scale`): the one reading of the setting, for what
    /// `weft status` is shown and for when the install alerts.
    pub fn alert_delay(&self) -> std::time::Duration {
        weft_core::time_scale::scaled(std::time::Duration::from_secs(u64::from(self.alert_after_minutes) * 60))
    }

    /// When this install alerts on its stuck failures, how long one weft
    /// still retries is current before it counts as stuck
    /// ([`Self::alert_delay`]); `None` when it does not alert: a local
    /// install shows them in `weft status` only, and a cloud install only
    /// alerts where its config says so.
    pub fn alert_after(&self) -> Option<std::time::Duration> {
        match &self.platform {
            PlatformConfig::Gcp(gcp) if gcp.alerts => Some(self.alert_delay()),
            PlatformConfig::Gcp(_) | PlatformConfig::Local(_) => None,
        }
    }

    /// Refuse a config no install could run, naming the setting.
    pub fn validate(&self) -> Result<(), String> {
        self.worker_settings().validate()?;
        if self.holders.signals_per_copy == 0 {
            return Err("holders.signalsPerCopy is 0: no holder could take a signal; set it to 1 or more".into());
        }
        let (high, target) = (self.holders.high_cpu_percent, self.holders.target_cpu_percent);
        // SYNC: the holders' CPU rule <-> deploy/terraform/gcp/serverless.tf (the config secret's precondition), deploy/terraform/gcp/variables.tf (holder_*_cpu_percent validation)
        if high > 100 {
            return Err(format!("holders.highCpuPercent is {high}: a holder never uses more than 100% of its CPUs; set it to 100 or less"));
        }
        if target == 0 {
            return Err("holders.targetCpuPercent is 0: every holder uses some CPU, so no count of them would be enough; set it to 1 or more".into());
        }
        if target >= high {
            return Err(format!(
                "holders.targetCpuPercent ({target}) is not under holders.highCpuPercent ({high}): a holder running at the share the pool \
                 is counted for would hand its work over; set targetCpuPercent under highCpuPercent"
            ));
        }
        if u16::from(target) + 2 * u16::from(LOAD_BAND_PERCENT) > u16::from(high) {
            return Err(format!(
                "holders.targetCpuPercent ({target}) is less than {} under holders.highCpuPercent ({high}): a holder says its load again \
                 only once it moved {LOAD_BAND_PERCENT} points, so two holders merged into one because their loads fit under the target \
                 can carry up to {} points more than they said; over the high mark the one left hands work over at once, a holder comes \
                 back, and the pool would grow and shrink for good. Set targetCpuPercent to {} or less",
                2 * LOAD_BAND_PERCENT,
                2 * LOAD_BAND_PERCENT,
                u16::from(high).saturating_sub(2 * u16::from(LOAD_BAND_PERCENT))
            ));
        }
        for role in CoreRole::ALL {
            let placed = self.roles.of(role);
            // Only the holder holds connections between calls, the one
            // reason for a pool; and nothing can call a pool, so the holder
            // is the only role that can live there.
            match (role, placed) {
                (CoreRole::Holder, Placement::Serverless) => {
                    return Err("roles.holder is serverless, where nothing stays up between calls to hold a connection; place it in a pool".into())
                }
                (CoreRole::Holder, _) | (_, Placement::Machine | Placement::Serverless) => {}
                (_, Placement::Pool) => {
                    return Err(format!("roles.{role} is pool, which only the holder can be: a pool is never called; place {role} serverless"))
                }
            }
            let has_url = self.role_urls.contains_key(&role);
            match (placed, has_url) {
                (Placement::Serverless, false) => {
                    return Err(format!("roles.{role} is serverless but roleUrls names no address for it"))
                }
                (Placement::Machine | Placement::Pool, true) => {
                    return Err(format!("roleUrls names an address for {role}, which is not serverless"))
                }
                _ => {}
            }
            let shared = DATABASE_ROLES.contains(&role) && placed == Placement::Serverless;
            match (shared, self.max_copies.get(&role)) {
                (true, None) => {
                    return Err(format!(
                        "roles.{role} is serverless and opens the database, but maxCopies names no ceiling for it: \
                         each copy sizes its pools to its share of the database, so the number of copies must be known"
                    ))
                }
                (true, Some(0)) => return Err(format!("maxCopies.{role} is 0: no copy of {role} could run; set it to 1 or more")),
                (false, Some(_)) => {
                    return Err(format!("maxCopies names {role}, which is not a serverless role that opens the database; remove it"))
                }
                _ => {}
            }
        }
        match &self.platform {
            PlatformConfig::Local(_) => {
                if self.roles.on_machine().len() != CoreRole::ALL.len() {
                    return Err("a local install runs every role in its one process; remove the other placements".into());
                }
            }
            PlatformConfig::Gcp(gcp) => {
                if gcp.wake_attempts == 0 {
                    return Err("platform.wakeAttempts is 0: no wake could be delivered even once; set it to 1 or more".into());
                }
                if let Some(role) = self.roles.on_machine().first() {
                    return Err(format!(
                        "roles.{role} is on the machine, and a GCP install has none; place it serverless (the holder in a pool)"
                    ));
                }
            }
        }
        Ok(())
    }

    /// The local install's internal routes as a caller at `from` reaches
    /// them; `None` on a platform without a machine.
    fn machine_base(&self, from: Vantage) -> Option<String> {
        let PlatformConfig::Local(local) = &self.platform else { return None };
        let base = match from {
            Vantage::Machine => local.internal_url.clone(),
            Vantage::Private => local.container_internal_url.clone(),
            Vantage::Public => format!("{}{INTERNAL_DOOR}", self.public_url.trim_end_matches('/')),
        };
        Some(base.trim_end_matches('/').to_string())
    }

    /// Where a project's own containers reach the install's files door
    /// (`object_store::FILES_DOOR`), when they cannot reach the store at
    /// the address the workers do: a local install's internal port as a
    /// container reaches it, since a container's `127.0.0.1` is itself.
    /// `None` on a platform without a machine, whose containers reach the
    /// store where the workers do.
    pub fn container_files_base(&self) -> Option<String> {
        self.machine_base(Vantage::Private)
    }

    /// One role's internal address as a caller at `from` reaches it;
    /// `None` for the dispatcher on the machine and for the holder, which
    /// have none.
    fn role_address(&self, role: CoreRole, from: Vantage) -> Option<String> {
        match self.roles.of(role) {
            Placement::Serverless => Some(self.role_urls[&role].trim_end_matches('/').to_string()),
            Placement::Pool => None,
            Placement::Machine => Some(format!("{}{}", self.machine_base(from)?, role.internal_prefix()?)),
        }
    }

    /// Every role's internal address, as a caller at `from` reaches it.
    pub fn role_addresses(&self, from: Vantage) -> RoleAddresses {
        let at = |role: CoreRole| {
            self.role_address(role, from).expect("the broker, the listener and the supervisor always have an internal address (validate)")
        };
        RoleAddresses {
            dispatcher: self.role_address(CoreRole::Dispatcher, from),
            broker: at(CoreRole::Broker),
            listener: at(CoreRole::Listener),
            supervisor: at(CoreRole::Supervisor),
        }
    }

    /// Every address any caller reaches `role`'s internal routes at: the
    /// audiences a credential for it may name. Built from the same
    /// [`Self::role_addresses`] every caller addresses it by, so a caller
    /// can never be refused for using the address it was given.
    pub fn role_audiences(&self, role: CoreRole) -> Vec<String> {
        let mut audiences: Vec<String> = Vantage::ALL.into_iter().filter_map(|from| self.role_address(role, from)).collect();
        audiences.sort();
        audiences.dedup();
        audiences
    }

    /// The address the open internet reaches the install at: a quick
    /// tunnel's (asked each time), else the stable internet address, else
    /// the public address when it is not a loopback one.
    pub fn internet_address(&self) -> InternetAddress {
        let quick = match &self.platform {
            PlatformConfig::Local(local) => local.quick_tunnel_metrics.clone(),
            PlatformConfig::Gcp(_) => None,
        };
        match (quick, &self.internet_url) {
            (Some(metrics), _) => InternetAddress::QuickTunnel { metrics },
            (None, Some(url)) => InternetAddress::Fixed(url.clone()),
            (None, None) if !weft_core::net::is_loopback_url(&self.public_url) => InternetAddress::Fixed(self.public_url.clone()),
            (None, None) => InternetAddress::None,
        }
    }

    /// The install's own address for a link outside callers open when
    /// neither a site nor the internet carries it: the network address
    /// when one is set, else the public one.
    pub fn own_base_url(&self) -> &str {
        match &self.platform {
            PlatformConfig::Local(local) => local.network_url.as_deref().unwrap_or(&self.public_url),
            PlatformConfig::Gcp(_) => &self.public_url,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn local() -> InstallConfig {
        InstallConfig {
            install: Install::default_install(),
            platform: PlatformConfig::Local(Box::new(LocalPlatform {
                data_dir: "/home/u/.local/share/weft".into(),
                container_internal_url: "http://host.docker.internal:14113".into(),
                runtime_image: "weft-runtime:dev".into(),
                worker_idle_stop_seconds: None,
                listen: Listen { public: "127.0.0.1:14111".parse().unwrap(), internal: "127.0.0.1:14113".parse().unwrap(), outside: None, network: None },
                project_ports: Some(PortRange { first: 14200, last: 14999 }),
                internal_url: "http://127.0.0.1:14113".into(),
                network_url: None,
                quick_tunnel_metrics: None,
            })),
            public_url: "http://127.0.0.1:14111".into(),
            internet_url: None,
            roles: RolePlacement::default(),
            role_urls: BTreeMap::new(),
            workers: WorkerOverrides::default(),
            holders: HolderSettings::default(),
            max_copies: BTreeMap::new(),
            edge: EdgeConfig { trusted_proxy_hops: ProxyHops { public: 0, outside: 0, domains: 0 }, invalid_tokens_per_minute: Some(30) },
            object_store: ObjectStoreSettings::S3(S3StoreSettings {
                endpoint: "http://127.0.0.1:8333".into(),
                bucket: "weft".into(),
                region: "us-east-1".into(),
                force_path_style: true,
                public_endpoint: None,
                public_internet: false,
            }),
            source: None,
            alert_after_minutes: DEFAULT_ALERT_AFTER_MINUTES,
        }
    }

    /// A cloud install: every role serverless, the holder in a pool.
    pub fn gcp() -> InstallConfig {
        let mut c = local();
        c.platform = PlatformConfig::Gcp(Box::new(GcpPlatform {
            name: "weft".into(),
            project: "p".into(),
            region: "r".into(),
            zone: "z".into(),
            network: "n".into(),
            subnet: "s".into(),
            artifact_registry: "a".into(),
            build_bucket: "b".into(),
            builder_service_account: "builder@p".into(),
            builder_base_image: "base".into(),
            tasks_queue: "q".into(),
            wake_attempts: 20,
            ticks_queue: "t".into(),
            news_topic: "weft-news".into(),
            holders_topic: "weft-holders".into(),
            core_service_account: "core@p".into(),
            holder_pool: "projects/p/locations/r/workerPools/weft-holder".into(),
            dispatcher_service: "weft-role-dispatcher".into(),
            runtime_image: "i".into(),
            deployer_service_account: "deployer@p".into(),
            frontend_service_account: "frontend@p".into(),
            workload_identity_provider: "w".into(),
            infra_network_tag: "it".into(),
            build_machine: None,
            alerts: true,
        }));
        c.public_url = "https://d.run.app".into();
        for role in [CoreRole::Dispatcher, CoreRole::Broker, CoreRole::Listener, CoreRole::Supervisor] {
            c.roles.set(role, Placement::Serverless);
            c.role_urls.insert(role, format!("https://{}.run.app/", role.as_str()));
        }
        c.roles.set(CoreRole::Holder, Placement::Pool);
        c.max_copies = BTreeMap::from([(CoreRole::Dispatcher, 2), (CoreRole::Broker, 4)]);
        c.validate().unwrap();
        c
    }

    /// A local install's containers reach its files door at the internal
    /// port as a container reaches it; a cloud install's reach the store
    /// where its workers do, so it names none.
    #[test]
    fn a_local_install_names_where_its_containers_reach_the_files_door() {
        assert_eq!(local().container_files_base().as_deref(), Some("http://host.docker.internal:14113"));
        assert_eq!(gcp().container_files_base(), None);
    }

    /// Every serverless role that opens the database has a ceiling on its
    /// copies, and only those roles have one.
    #[test]
    fn the_roles_that_open_the_database_name_their_most_copies() {
        let mut c = gcp();
        c.max_copies.remove(&CoreRole::Broker);
        assert!(c.validate().unwrap_err().contains("maxCopies names no ceiling for it"));
        let mut c = gcp();
        c.max_copies.insert(CoreRole::Broker, 0);
        assert!(c.validate().unwrap_err().contains("maxCopies.broker is 0"));
        let mut c = gcp();
        c.max_copies.insert(CoreRole::Listener, 3);
        assert!(c.validate().unwrap_err().contains("maxCopies names listener"));
        let mut c = local();
        c.max_copies.insert(CoreRole::Dispatcher, 1);
        assert!(c.validate().unwrap_err().contains("maxCopies names dispatcher"), "the local process takes the whole database");
    }

    #[test]
    fn only_a_cloud_install_whose_config_says_so_alerts_and_the_delay_defaults_to_five_minutes() {
        assert_eq!(local().alert_after(), None, "a local install shows its failures in `weft status` only");
        let mut cloud = gcp();
        assert_eq!(cloud.alert_after(), Some(std::time::Duration::from_secs(5 * 60)));
        cloud.alert_after_minutes = 12;
        assert_eq!(cloud.alert_after(), Some(std::time::Duration::from_secs(12 * 60)));
        if let PlatformConfig::Gcp(platform) = &mut cloud.platform {
            platform.alerts = false;
        }
        assert_eq!(cloud.alert_after(), None, "alerts turned off");
        let mut unset = serde_json::to_value(local()).unwrap();
        unset.as_object_mut().unwrap().remove("alertAfterMinutes");
        assert_eq!(serde_json::from_value::<InstallConfig>(unset).unwrap().alert_after_minutes, DEFAULT_ALERT_AFTER_MINUTES);
    }

    /// What reaches an install from the internet, and its own address for
    /// everything else: a quick tunnel is asked, a stable address is kept,
    /// a loopback public address reaches nobody outside, and a network
    /// address stands for the machine.
    #[test]
    fn the_internet_reaches_the_tunnel_and_the_network_reaches_its_address() {
        let mut c = local();
        assert_eq!(c.internet_address(), InternetAddress::None);
        assert_eq!(c.own_base_url(), "http://127.0.0.1:14111");
        c.internet_url = Some("https://weft.example".into());
        assert_eq!(c.internet_address(), InternetAddress::Fixed("https://weft.example".into()));
        if let PlatformConfig::Local(local) = &mut c.platform {
            local.quick_tunnel_metrics = Some("http://127.0.0.1:14120".into());
            local.network_url = Some("http://192.168.1.20:14112".into());
        }
        assert_eq!(c.internet_address(), InternetAddress::QuickTunnel { metrics: "http://127.0.0.1:14120".into() });
        assert_eq!(c.own_base_url(), "http://192.168.1.20:14112");
        assert_eq!(gcp().internet_address(), InternetAddress::Fixed(gcp().public_url));
    }

    /// The holders' CPU shares default when left out, and a pair the pool
    /// could not settle on is refused naming the setting.
    #[test]
    fn the_holders_cpu_shares_default_and_must_leave_two_bands() {
        let mut unset = serde_json::to_value(gcp()).unwrap();
        unset["holders"] = serde_json::json!({});
        let read = serde_json::from_value::<InstallConfig>(unset).unwrap().holders;
        assert_eq!((read.signals_per_copy, read.high_cpu_percent, read.target_cpu_percent), (200, 70, 50));
        gcp().validate().unwrap();
        let mut c = gcp();
        c.holders.target_cpu_percent = 51;
        assert!(c.validate().unwrap_err().contains("holders.targetCpuPercent (51) is less than 20 under holders.highCpuPercent (70)"));
        assert!(c.validate().unwrap_err().contains("Set targetCpuPercent to 50 or less"));
        c.holders.target_cpu_percent = 70;
        assert!(c.validate().unwrap_err().contains("holders.targetCpuPercent (70) is not under holders.highCpuPercent (70)"));
        c.holders.target_cpu_percent = 0;
        assert!(c.validate().unwrap_err().contains("holders.targetCpuPercent is 0"));
        let mut c = gcp();
        c.holders.high_cpu_percent = 101;
        assert!(c.validate().unwrap_err().contains("holders.highCpuPercent is 101"));
        let mut c = gcp();
        (c.holders.high_cpu_percent, c.holders.target_cpu_percent) = (100, 80);
        c.validate().unwrap();
        (c.holders.high_cpu_percent, c.holders.target_cpu_percent) = (20, 1);
        assert!(c.validate().is_err(), "under two bands of room, no target fits");
    }

    #[test]
    fn a_cloud_storage_store_names_its_bucket_and_nothing_else() {
        let store: ObjectStoreSettings = serde_json::from_value(serde_json::json!({ "kind": "gcs", "bucket": "p-weft-files" })).unwrap();
        assert_eq!(store, ObjectStoreSettings::Gcs(GcsStoreSettings { bucket: "p-weft-files".into() }));
        assert!(store.public_internet(), "a signed Cloud Storage link answers anyone holding it");
        let keyed = serde_json::json!({ "kind": "gcs", "bucket": "b", "endpoint": "https://storage.googleapis.com" });
        assert!(serde_json::from_value::<ObjectStoreSettings>(keyed).is_err(), "an endpoint means nothing there");
    }

    #[test]
    fn a_config_round_trips_and_refuses_what_it_does_not_know() {
        for c in [local(), gcp()] {
            let v = serde_json::to_value(&c).unwrap();
            assert_eq!(serde_json::from_value::<InstallConfig>(v.clone()).unwrap(), c);
            let mut typo = v;
            typo["publicURL"] = serde_json::json!("x");
            assert!(serde_json::from_value::<InstallConfig>(typo).is_err());
            c.validate().unwrap();
        }
        let mut machine = serde_json::to_value(gcp()).unwrap();
        machine["platform"]["machineInternalUrl"] = serde_json::json!("http://10.10.0.2:14113");
        assert!(serde_json::from_value::<InstallConfig>(machine).is_err(), "a GCP install has no machine to name");
    }

    #[test]
    fn every_role_on_the_machine_answers_under_its_prefix() {
        let c = local();
        assert_eq!(c.role_addresses(Vantage::Machine).broker, "http://127.0.0.1:14113/broker");
        assert_eq!(c.role_addresses(Vantage::Private).broker, "http://host.docker.internal:14113/broker");
        assert_eq!(c.role_addresses(Vantage::Public).broker, "http://127.0.0.1:14111/_internal/broker");
        assert_eq!(c.role_addresses(Vantage::Machine).dispatcher, None, "no internal routes on the machine");
        assert!(c.role_addresses(Vantage::Machine).of(CoreRole::Dispatcher).is_err());
        assert!(c.role_addresses(Vantage::Machine).of(CoreRole::Holder).is_err(), "nothing calls the holder");
    }

    #[test]
    fn a_serverless_role_needs_its_address_and_no_other_has_one() {
        let mut c = local();
        c.roles.set(CoreRole::Listener, Placement::Serverless);
        assert!(c.validate().unwrap_err().contains("roles.listener"));
        c.role_urls.insert(CoreRole::Listener, "https://l.run.app".into());
        assert!(c.validate().unwrap_err().contains("local install"), "local runs all in one process");
        let mut c = local();
        c.role_urls.insert(CoreRole::Broker, "https://b.run.app".into());
        assert!(c.validate().unwrap_err().contains("not serverless"));
        let mut c = gcp();
        c.role_urls.insert(CoreRole::Holder, "https://h.run.app".into());
        assert!(c.validate().unwrap_err().contains("not serverless"), "nothing calls a pool");
    }

    #[test]
    fn only_the_holder_lives_in_a_pool_and_it_lives_nowhere_serverless() {
        for role in [CoreRole::Dispatcher, CoreRole::Broker, CoreRole::Listener, CoreRole::Supervisor] {
            let mut c = gcp();
            c.roles.set(role, Placement::Pool);
            c.role_urls.remove(&role);
            let refused = c.validate().unwrap_err();
            assert!(refused.contains("only the holder"), "{role}: {refused}");
        }
        let mut c = gcp();
        c.roles.set(CoreRole::Holder, Placement::Serverless);
        c.role_urls.insert(CoreRole::Holder, "https://h.run.app".into());
        assert!(c.validate().unwrap_err().contains("roles.holder is serverless"));
    }

    #[test]
    fn a_gcp_install_has_no_machine() {
        let mut c = gcp();
        c.roles.set(CoreRole::Supervisor, Placement::Machine);
        c.role_urls.remove(&CoreRole::Supervisor);
        assert!(c.validate().unwrap_err().contains("roles.supervisor is on the machine"));
    }

    #[test]
    fn a_gcp_install_delivers_each_wake_at_least_once() {
        let mut c = gcp();
        let PlatformConfig::Gcp(gcp) = &mut c.platform else { unreachable!() };
        gcp.wake_attempts = 0;
        assert!(c.validate().unwrap_err().contains("platform.wakeAttempts is 0"));
    }

    /// A serverless role is at its own address wherever the caller stands,
    /// and the holder at none.
    #[test]
    fn a_serverless_role_is_reached_at_its_own_address() {
        let c = gcp();
        for from in Vantage::ALL {
            let a = c.role_addresses(from);
            assert_eq!(a.broker, "https://broker.run.app");
            assert_eq!(a.dispatcher.as_deref(), Some("https://dispatcher.run.app"));
            assert!(a.of(CoreRole::Holder).is_err());
        }
        assert_eq!(c.role_audiences(CoreRole::Broker), vec!["https://broker.run.app"]);
        assert!(c.role_audiences(CoreRole::Holder).is_empty());
    }

    #[test]
    fn the_token_guessing_block_is_never_off_by_omission() {
        let mut v = serde_json::to_value(local()).unwrap();
        v["edge"].as_object_mut().unwrap().remove("invalidTokensPerMinute");
        assert!(serde_json::from_value::<InstallConfig>(v.clone()).is_err(), "required");
        v["edge"]["invalidTokensPerMinute"] = serde_json::Value::Null;
        let off: InstallConfig = serde_json::from_value(v).unwrap();
        assert_eq!(off.edge.invalid_tokens_per_minute, None, "null turns it off");
    }
}
