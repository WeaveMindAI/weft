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
    pub listen: Listen,
    /// Where this machine's internal port is reached from the machine
    /// itself (`http://127.0.0.1:14113`): the base every role placed on the
    /// machine answers under, each at its own prefix, for a caller on the
    /// machine. Anywhere else reaches it at the platform's own address for
    /// it (`InstallConfig::role_addresses`).
    #[serde(rename = "internalUrl")]
    pub internal_url: String,
    #[serde(default)]
    pub roles: RolePlacement,
    /// The internal address of each role that runs in a process of its own
    /// (placed `serverless` or `own_machine`): the root that process serves
    /// the role at. Required for exactly those roles.
    #[serde(default, rename = "roleUrls", skip_serializing_if = "BTreeMap::is_empty")]
    pub role_urls: BTreeMap<CoreRole, String>,
    /// The worker settings every project starts from.
    #[serde(default)]
    pub workers: WorkerSettings,
    pub build: BuildConfig,
    pub edge: EdgeConfig,
    #[serde(rename = "objectStore")]
    pub object_store: ObjectStoreSettings,
    /// The install's own TLS front door, on a machine the internet reaches
    /// directly (a cloud install). `None` on a local install, which the
    /// open internet reaches through its tunnel.
    #[serde(default, rename = "frontDoor", skip_serializing_if = "Option::is_none")]
    pub front_door: Option<FrontDoor>,
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
}

impl LocalPlatform {
    fn default_worker_idle_stop_seconds() -> u64 {
        300
    }
}

/// Google Cloud: the machine on Compute Engine, workers and serverless
/// roles on Cloud Run, builds on Cloud Build, wakes on Cloud Tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcpPlatform {
    pub project: String,
    pub region: String,
    /// The zone the machine and infra machines run in.
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
    /// The machine's internal port as a Cloud Run service or an infra
    /// machine reaches it (`http://10.10.0.2:14113`).
    #[serde(rename = "machineInternalUrl")]
    pub machine_internal_url: String,
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
}

/// The object store, minus its credentials (`WEFT_OBJECT_STORE_ACCESS_KEY`
/// and `WEFT_OBJECT_STORE_SECRET_KEY`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectStoreSettings {
    pub endpoint: String,
    pub bucket: String,
    #[serde(default = "ObjectStoreSettings::default_region")]
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
    /// Whether the open internet reaches the store's presigned URLs, so a
    /// shared file link can hand one out instead of relaying the bytes.
    #[serde(default, rename = "publicInternet")]
    pub public_internet: bool,
}

impl ObjectStoreSettings {
    fn default_region() -> String {
        "us-east-1".into()
    }
}

fn yes() -> bool {
    true
}

/// The machine's front door: HTTPS on its static address, with
/// certificates it gets and renews itself from an ACME authority (Let's
/// Encrypt), one for the address and one per domain the install stores.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrontDoor {
    /// The machine's static public address: the install's own address
    /// while it has no domain, and what every domain's DNS record points
    /// at.
    pub address: std::net::IpAddr,
    /// Where HTTPS is served (`0.0.0.0:443`).
    pub https: SocketAddr,
    /// Where plain HTTP is served (`0.0.0.0:80`): the authority's
    /// challenges, and a redirect to HTTPS for everything else.
    pub http: SocketAddr,
    /// The ACME directory certificates come from.
    #[serde(default = "FrontDoor::default_acme_directory", rename = "acmeDirectory")]
    pub acme_directory: String,
    /// Where the account key and the certificates are kept, so a restart
    /// reuses them instead of asking again.
    #[serde(rename = "stateDir")]
    pub state_dir: std::path::PathBuf,
}

impl FrontDoor {
    /// Let's Encrypt's production directory.
    pub const LETS_ENCRYPT: &'static str = "https://acme-v02.api.letsencrypt.org/directory";

    fn default_acme_directory() -> String {
        Self::LETS_ENCRYPT.into()
    }
}

/// Every secret the runtime reads from its environment.
// SYNC: SECRET_ENV <-> crates/weft-cli/src/commands/daemon.rs (local
//       secrets file), deploy/terraform/gcp/secrets.tf
pub const SECRET_ENV: &[&str] = &[
    "WEFT_DATABASE_URL",
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
        if self.build.compile_lanes == 0 {
            return Err("build.compileLanes is 0: nothing could compile; set it to 1 or more".into());
        }
        for role in CoreRole::ALL {
            let placed = self.roles.of(role);
            // Only the listener holds connections between calls, the one
            // reason for a machine of its own, and only its placement there
            // is wired (the machine's port passes its outside calls on).
            if placed == Placement::OwnMachine && role != CoreRole::Listener {
                return Err(format!(
                    "roles.{role} is own_machine, which only the listener can be; place {role} on the machine or serverless"
                ));
            }
            let has_url = self.role_urls.contains_key(&role);
            match (placed, has_url) {
                (Placement::Serverless | Placement::OwnMachine, false) => {
                    return Err(format!("roles.{role} runs in a process of its own but roleUrls names no address for it"))
                }
                (Placement::Machine, true) => {
                    return Err(format!("roleUrls names an address for {role}, which is placed on the machine"))
                }
                _ => {}
            }
        }
        if matches!(self.platform, PlatformConfig::Local(_)) && !self.roles.on_machine().len().eq(&CoreRole::ALL.len()) {
            return Err("a local install runs every role on the machine; remove the other placements".into());
        }
        if self.listen.outside.is_some() && self.roles.dispatcher != Placement::Machine {
            return Err("listen.outside serves the dispatcher's outside doors from the machine, and roles.dispatcher is not on the machine; remove listen.outside".into());
        }
        if matches!(self.platform, PlatformConfig::Local(_)) && self.front_door.is_some() {
            return Err("a local install has no front door (the open internet reaches it through its tunnel); remove frontDoor".into());
        }
        Ok(())
    }

    /// The machine's internal routes as a caller at `from` reaches them.
    fn machine_base(&self, from: Vantage) -> String {
        let base = match (from, &self.platform) {
            (Vantage::Machine, _) => self.internal_url.clone(),
            (Vantage::Private, PlatformConfig::Local(l)) => l.container_internal_url.clone(),
            (Vantage::Private, PlatformConfig::Gcp(g)) => g.machine_internal_url.clone(),
            (Vantage::Public, _) => format!("{}{INTERNAL_DOOR}", self.public_url.trim_end_matches('/')),
        };
        base.trim_end_matches('/').to_string()
    }

    /// One role's internal address as a caller at `from` reaches it;
    /// `None` for the dispatcher on the machine, which has none.
    fn role_address(&self, role: CoreRole, from: Vantage) -> Option<String> {
        let own = || self.role_urls[&role].trim_end_matches('/').to_string();
        match (self.roles.of(role), from) {
            (Placement::Serverless, _) => Some(own()),
            // A machine of its own has no public address: the machine's
            // public port passes the call on to it.
            (Placement::OwnMachine, Vantage::Public) => {
                role.internal_prefix().map(|prefix| format!("{}{prefix}", self.machine_base(from)))
            }
            (Placement::OwnMachine, Vantage::Machine | Vantage::Private) => Some(own()),
            (Placement::Machine, _) => role.internal_prefix().map(|prefix| format!("{}{prefix}", self.machine_base(from))),
        }
    }

    /// Every role's internal address, as a caller at `from` reaches it.
    pub fn role_addresses(&self, from: Vantage) -> RoleAddresses {
        let at = |role: CoreRole| self.role_address(role, from).expect("every role but the dispatcher has an internal address");
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
            }),
            auth: AuthMode::Local,
            public_url: "http://127.0.0.1:14111".into(),
            internet_url: None,
            listen: Listen { public: "127.0.0.1:14111".parse().unwrap(), internal: "127.0.0.1:14113".parse().unwrap(), outside: None },
            internal_url: "http://127.0.0.1:14113".into(),
            roles: RolePlacement::default(),
            role_urls: BTreeMap::new(),
            workers: WorkerSettings::default(),
            build: BuildConfig { compile_lanes: 4, builder_base_image: "b".into(), runtime_base_image: "r".into() },
            edge: EdgeConfig { trusted_proxy_hops: ProxyHops { public: 0, outside: 0 }, invalid_tokens_per_minute: Some(30) },
            object_store: ObjectStoreSettings {
                endpoint: "http://127.0.0.1:8333".into(),
                bucket: "weft".into(),
                region: "us-east-1".into(),
                force_path_style: true,
                public_endpoint: None,
                worker_endpoint: None,
                public_internet: false,
            },
            front_door: None,
            source: None,
        }
    }

    #[test]
    fn a_config_round_trips_and_refuses_what_it_does_not_know() {
        let c = local();
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(serde_json::from_value::<InstallConfig>(v.clone()).unwrap(), c);
        let mut typo = v;
        typo["publicURL"] = serde_json::json!("x");
        assert!(serde_json::from_value::<InstallConfig>(typo).is_err());
        c.validate().unwrap();
    }

    #[test]
    fn every_role_on_the_machine_answers_under_its_prefix() {
        let c = local();
        assert_eq!(c.role_addresses(Vantage::Machine).broker, "http://127.0.0.1:14113/broker");
        assert_eq!(c.role_addresses(Vantage::Private).broker, "http://host.docker.internal:14113/broker");
        assert_eq!(c.role_addresses(Vantage::Machine).dispatcher, None, "no internal routes on the machine");
        assert!(c.role_addresses(Vantage::Machine).of(CoreRole::Dispatcher).is_err());
    }

    #[test]
    fn a_serverless_role_needs_its_address_and_a_machine_role_has_none() {
        let mut c = local();
        c.roles.set(CoreRole::Listener, Placement::Serverless);
        assert!(c.validate().unwrap_err().contains("roles.listener"));
        c.role_urls.insert(CoreRole::Listener, "https://l.run.app".into());
        assert!(c.validate().unwrap_err().contains("local install"), "local runs all on the machine");
        let mut c = local();
        c.role_urls.insert(CoreRole::Broker, "https://b.run.app".into());
        assert!(c.validate().unwrap_err().contains("placed on the machine"));
    }

    #[test]
    fn only_the_listener_gets_a_machine_of_its_own() {
        for role in [CoreRole::Dispatcher, CoreRole::Broker, CoreRole::Supervisor] {
            let mut c = gcp();
            c.roles.set(role, Placement::OwnMachine);
            c.role_urls.insert(role, "http://10.10.0.3:14113".into());
            let refused = c.validate().unwrap_err();
            assert!(refused.contains("only the listener"), "{role}: {refused}");
        }
    }

    #[test]
    fn the_outside_port_needs_the_dispatcher_on_the_machine() {
        let mut c = gcp();
        c.listen.outside = Some("127.0.0.1:14112".parse().unwrap());
        assert!(c.validate().unwrap_err().contains("listen.outside"));
    }

    /// A cloud install: the dispatcher, broker and supervisor serverless,
    /// the listener on a machine of its own, nothing else on the machine.
    fn gcp() -> InstallConfig {
        let mut c = local();
        c.platform = PlatformConfig::Gcp(Box::new(GcpPlatform {
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
            machine_internal_url: "http://10.10.0.2:14113/".into(),
            runtime_image: "i".into(),
            deployer_service_account: "deployer@p".into(),
            frontend_service_account: "frontend@p".into(),
            workload_identity_provider: "w".into(),
            caller_token_secret: "c".into(),
            infra_network_tag: "it".into(),
        }));
        c.public_url = "https://34.1.2.3/".into();
        for role in [CoreRole::Dispatcher, CoreRole::Broker, CoreRole::Supervisor] {
            c.roles.set(role, Placement::Serverless);
        }
        c.role_urls.insert(CoreRole::Dispatcher, "https://d.run.app".into());
        c.role_urls.insert(CoreRole::Broker, "https://b.run.app/".into());
        c.role_urls.insert(CoreRole::Supervisor, "https://s.run.app".into());
        c.roles.set(CoreRole::Listener, Placement::OwnMachine);
        c.role_urls.insert(CoreRole::Listener, "http://10.10.0.3:14113/".into());
        c.validate().unwrap();
        c
    }

    /// The machine's loopback is the machine's alone: a role anywhere else
    /// reaches a machine role at the machine's private address, and a
    /// caller outside through the public port.
    #[test]
    fn where_a_caller_stands_decides_the_address() {
        let mut c = gcp();
        c.roles.set(CoreRole::Listener, Placement::Machine);
        c.role_urls.remove(&CoreRole::Listener);
        c.validate().unwrap();
        assert_eq!(c.role_addresses(Vantage::Machine).listener, "http://127.0.0.1:14113/listener");
        assert_eq!(c.role_addresses(Vantage::Private).listener, "http://10.10.0.2:14113/listener");
        assert_eq!(c.role_addresses(Vantage::Public).listener, "https://34.1.2.3/_internal/listener");
        for from in Vantage::ALL {
            let a = c.role_addresses(from);
            assert_eq!(a.broker, "https://b.run.app", "a serverless role is at its own address from anywhere");
            assert_eq!(a.dispatcher.as_deref(), Some("https://d.run.app"));
        }
        assert_eq!(
            c.role_audiences(CoreRole::Listener),
            vec!["http://10.10.0.2:14113/listener", "http://127.0.0.1:14113/listener", "https://34.1.2.3/_internal/listener"],
            "every address a caller is given is one the listener accepts"
        );
        assert_eq!(c.role_audiences(CoreRole::Broker), vec!["https://b.run.app"]);
    }

    /// A role on a machine of its own is reached at that machine's root
    /// from the private network, and through the machine's public port
    /// from outside.
    #[test]
    fn a_role_of_its_own_is_reached_at_its_root() {
        let c = gcp();
        assert_eq!(c.role_addresses(Vantage::Machine).listener, "http://10.10.0.3:14113");
        assert_eq!(c.role_addresses(Vantage::Private).listener, "http://10.10.0.3:14113");
        assert_eq!(c.role_addresses(Vantage::Public).listener, "https://34.1.2.3/_internal/listener");
        assert_eq!(c.role_audiences(CoreRole::Listener), vec!["http://10.10.0.3:14113", "https://34.1.2.3/_internal/listener"]);
    }

    #[test]
    fn a_local_install_has_no_front_door() {
        let mut c = local();
        c.front_door = Some(FrontDoor {
            address: "34.1.2.3".parse().unwrap(),
            https: "0.0.0.0:443".parse().unwrap(),
            http: "0.0.0.0:80".parse().unwrap(),
            acme_directory: FrontDoor::LETS_ENCRYPT.into(),
            state_dir: "/var/lib/weft/tls".into(),
        });
        assert!(c.validate().unwrap_err().contains("frontDoor"));
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
