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
use crate::runner::WorkerSettings;

// SYNC: InstallConfig <-> deploy/terraform/gcp/serverless.tf (local.install_config)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallConfig {
    /// Which install this is when a machine holds several.
    #[serde(default)]
    pub install: Install,
    pub platform: PlatformConfig,
    pub auth: AuthMode,
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
    /// The worker settings every project starts from.
    #[serde(default)]
    pub workers: WorkerSettings,
    /// How the holders share the held connections, where they run in a
    /// pool.
    #[serde(default)]
    pub holders: HolderSettings,
    pub build: BuildConfig,
    pub edge: EdgeConfig,
    #[serde(rename = "objectStore")]
    pub object_store: ObjectStoreSettings,
    /// The weft source the install runs (`owner/name` and commit), so a
    /// project's CI builds the same CLI. Absent on a local install.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<weft_core::install::WeftSource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PlatformConfig {
    Local(LocalPlatform),
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
    /// (`http://host.docker.internal:14113`): the base a worker or an
    /// infra container calls the broker under.
    #[serde(rename = "containerInternalUrl")]
    pub container_internal_url: String,
    /// The image `weft-runtime` runs from (the agent beside every infra
    /// unit).
    #[serde(rename = "runtimeImage")]
    pub runtime_image: String,
    /// How long a worker with nothing to do stays up before it is
    /// stopped. Raise it to keep workers warm longer between runs.
    #[serde(default = "LocalPlatform::default_worker_idle_stop_seconds", rename = "workerIdleStopSeconds")]
    pub worker_idle_stop_seconds: u64,
    /// The ports the install's one process serves.
    pub listen: Listen,
    /// Where the internal port is reached from the machine itself
    /// (`http://127.0.0.1:14113`): the base every role answers under, each
    /// at its own prefix, for a caller in the same process. A container
    /// reaches it at `container_internal_url`.
    #[serde(rename = "internalUrl")]
    pub internal_url: String,
}

impl LocalPlatform {
    fn default_worker_idle_stop_seconds() -> u64 {
        300
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
    /// The Cloud Tasks queue wakes go through.
    #[serde(rename = "tasksQueue")]
    pub tasks_queue: String,
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
    /// The Secret Manager secret holding `WEFT_CALLER_TOKEN_SECRET`, which
    /// every project's workers read to check a live caller's ticket.
    #[serde(rename = "callerTokenSecret")]
    pub caller_token_secret: String,
    /// The network tag every infra machine carries: the firewall opens its
    /// ports to the install's workers, roles and other infra machines, and
    /// to nothing else.
    #[serde(rename = "infraNetworkTag")]
    pub infra_network_tag: String,
}

/// How the install authenticates its management API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// Loopback-bound: every request is tenant `local`, browser origins
    /// allowed (the editor's webviews).
    Local,
    /// Every management request carries an operator key; the first one
    /// is `WEFT_BOOTSTRAP_OPERATOR_KEY` when the install holds none.
    OperatorKeys,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listen {
    /// The public port: the management API, the public door, the relay.
    pub public: SocketAddr,
    /// The internal port: every role's internal endpoints.
    pub internal: SocketAddr,
    /// A port carrying only the doors outside callers use, for a tunnel
    /// that brings the open internet to an install whose management API
    /// must stay off it (a local install, which trusts every caller of its
    /// public port).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outside: Option<SocketAddr>,
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
}

impl HolderSettings {
    fn default_signals_per_copy() -> u32 {
        200
    }
}

impl Default for HolderSettings {
    fn default() -> Self {
        Self { signals_per_copy: Self::default_signals_per_copy() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildConfig {
    /// How many worker builds compile side by side, each in a compile
    /// cache of its own.
    #[serde(rename = "compileLanes")]
    pub compile_lanes: u32,
    /// The prebuilt builder a worker image compiles in.
    #[serde(rename = "builderBaseImage")]
    pub builder_base_image: String,
    /// The image a worker runs on when its project names none.
    #[serde(rename = "runtimeBaseImage")]
    pub runtime_base_image: String,
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
    /// The endpoint a project's worker reaches the store at, when it is
    /// not `endpoint` (a local install: the runtime is a process on the
    /// machine, a worker a container on the install's Docker network).
    #[serde(default, rename = "workerEndpoint", skip_serializing_if = "Option::is_none")]
    pub worker_endpoint: Option<String>,
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
    "WEFT_BOOTSTRAP_OPERATOR_KEY",
    "WEFT_OBJECT_STORE_ACCESS_KEY",
    "WEFT_OBJECT_STORE_SECRET_KEY",
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

    /// Refuse a config no install could run, naming the setting.
    pub fn validate(&self) -> Result<(), String> {
        self.workers.validate()?;
        if self.holders.signals_per_copy == 0 {
            return Err("holders.signalsPerCopy is 0: no holder could take a signal; set it to 1 or more".into());
        }
        if self.build.compile_lanes == 0 {
            return Err("build.compileLanes is 0: nothing could compile; set it to 1 or more".into());
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
        }
        match &self.platform {
            PlatformConfig::Local(_) => {
                if self.roles.on_machine().len() != CoreRole::ALL.len() {
                    return Err("a local install runs every role in its one process; remove the other placements".into());
                }
            }
            PlatformConfig::Gcp(_) => {
                // `local` trusts every caller as the one local tenant, and a
                // GCP install's management API is on the internet.
                if self.auth != AuthMode::OperatorKeys {
                    return Err("auth is local on a GCP install, which would let anyone on the internet manage it; set auth to operator_keys".into());
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

    /// The base for URLs handed to OUTSIDE callers (webhook targets, the
    /// address a provider posts to): the internet address when there is
    /// one, the public one otherwise.
    pub fn external_base_url(&self) -> &str {
        self.internet_url.as_deref().unwrap_or(&self.public_url)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn local() -> InstallConfig {
        InstallConfig {
            install: Install::default_install(),
            platform: PlatformConfig::Local(LocalPlatform {
                data_dir: "/home/u/.local/share/weft".into(),
                container_internal_url: "http://host.docker.internal:14113".into(),
                runtime_image: "weft-runtime:dev".into(),
                worker_idle_stop_seconds: 300,
                listen: Listen { public: "127.0.0.1:14111".parse().unwrap(), internal: "127.0.0.1:14113".parse().unwrap(), outside: None },
                internal_url: "http://127.0.0.1:14113".into(),
            }),
            auth: AuthMode::Local,
            public_url: "http://127.0.0.1:14111".into(),
            internet_url: None,
            roles: RolePlacement::default(),
            role_urls: BTreeMap::new(),
            workers: WorkerSettings::default(),
            holders: HolderSettings::default(),
            build: BuildConfig { compile_lanes: 4, builder_base_image: "b".into(), runtime_base_image: "r".into() },
            edge: EdgeConfig { trusted_proxy_hops: ProxyHops { public: 0, outside: 0, domains: 0 }, invalid_tokens_per_minute: Some(30) },
            object_store: ObjectStoreSettings::S3(S3StoreSettings {
                endpoint: "http://127.0.0.1:8333".into(),
                bucket: "weft".into(),
                region: "us-east-1".into(),
                force_path_style: true,
                public_endpoint: None,
                worker_endpoint: None,
                public_internet: false,
            }),
            source: None,
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
            tasks_queue: "q".into(),
            core_service_account: "core@p".into(),
            holder_pool: "projects/p/locations/r/workerPools/weft-holder".into(),
            dispatcher_service: "weft-role-dispatcher".into(),
            runtime_image: "i".into(),
            deployer_service_account: "deployer@p".into(),
            frontend_service_account: "frontend@p".into(),
            workload_identity_provider: "w".into(),
            caller_token_secret: "c".into(),
            infra_network_tag: "it".into(),
        }));
        c.auth = AuthMode::OperatorKeys;
        c.public_url = "https://d.run.app".into();
        for role in [CoreRole::Dispatcher, CoreRole::Broker, CoreRole::Listener, CoreRole::Supervisor] {
            c.roles.set(role, Placement::Serverless);
            c.role_urls.insert(role, format!("https://{}.run.app/", role.as_str()));
        }
        c.roles.set(CoreRole::Holder, Placement::Pool);
        c.validate().unwrap();
        c
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
    fn a_gcp_install_refuses_local_auth() {
        let mut c = gcp();
        c.auth = AuthMode::Local;
        assert!(c.validate().unwrap_err().contains("set auth to operator_keys"));
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
