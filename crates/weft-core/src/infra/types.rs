//! What an infra node asks for, said the same way for every platform.
//!
//! An infra node implements `Node::provision_infra`, which returns an
//! [`InfraSpec`]: the programs to run, the disks they keep, and the ports
//! other things reach them on. Nothing in it names a platform. The project's
//! supervisor resolves it (`super::resolve`) and hands it to the platform's
//! `InfraHost`, which runs each unit as a container on the local Docker
//! daemon, or as a machine of its own on a cloud (the only place a GPU is
//! ever attached).
//!
//! A unit is ONE running copy of a small group of containers that share a
//! network and can share scratch space and disks, the way a program and its
//! helper sit side by side on one machine. More copies means more units.
//!
//! Endpoint addresses are computed by the host, never by the node: the
//! node's code asks `ctx.endpoint(name)` at run time.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// What one infra node wants running.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InfraSpec {
    /// The running copies. Most nodes have exactly one.
    #[serde(default)]
    pub units: Vec<Unit>,

    /// Named storage. A disk outlives stop and upgrade; terminate
    /// deletes it unless it is listed in `keep_on_terminate`.
    #[serde(default)]
    pub volumes: Vec<Volume>,

    /// The ports this node offers, by name. `ctx.endpoint(name)` answers
    /// where each one is reached.
    #[serde(default)]
    pub endpoints: Vec<Endpoint>,

    /// Disks (by volume name) terminate keeps. Usually empty.
    #[serde(default, rename = "keepOnTerminate", alias = "keep_on_terminate", skip_serializing_if = "Vec::is_empty")]
    pub keep_on_terminate: Vec<String>,
}

/// What a terminate does with the disks the node lists in
/// `keep_on_terminate`. A property of each terminate command, never of the
/// copy: the same copy is terminated keeping them by a person's stop and
/// wiping them when its member is wiped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminateDisks {
    /// Keep the listed disks, so a later copy under the same id finds
    /// them again.
    KeepListed,
    /// Delete every disk, the listed ones too: the owner is going for good.
    DeleteAll,
}

impl TerminateDisks {
    /// The disks to keep out of the ones a copy lists.
    pub fn kept(self, listed: &[String]) -> &[String] {
        match self {
            TerminateDisks::KeepListed => listed,
            TerminateDisks::DeleteAll => &[],
        }
    }
}

// =============================================================
// Units
// =============================================================

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Unit {
    /// Its name within this node; endpoints name it.
    pub name: String,

    /// The containers that run for the unit's whole life, side by side.
    #[serde(default)]
    pub containers: Vec<Container>,

    /// Containers run one after another before `containers` start, each
    /// to completion; a failing one fails the start.
    #[serde(default, rename = "initContainers", alias = "init_containers")]
    pub init_containers: Vec<Container>,

    /// The machine the unit needs. On a local install the containers
    /// share the host and this is checked (a GPU the host lacks is
    /// refused); on a cloud it chooses the machine.
    #[serde(default)]
    pub machine: MachineShape,

    /// The group every disk the unit mounts is made readable and
    /// writable by, and that every container of the unit runs in besides
    /// its own, for containers that run as different non-root users and
    /// share files through it.
    #[serde(default, rename = "fsGroup", alias = "fs_group", skip_serializing_if = "Option::is_none")]
    pub fs_group: Option<u32>,

    /// What stop does to this unit.
    #[serde(default, rename = "onStop", alias = "on_stop")]
    pub on_stop: StopBehavior,

    /// Health windows for this unit's flaky/recovered transitions. Unset
    /// fields fall back to the supervisor's defaults.
    #[serde(default)]
    pub health: UnitHealth,
}

/// The machine a unit needs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineShape {
    /// CPUs, as a number (`"2"`, `"0.5"`). Unset: the platform's
    /// smallest that fits the containers' own limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    /// Memory (`"4Gi"`, `"512Mi"`). Unset: as for `cpu`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    /// GPUs attached to the unit's machine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu: Option<Gpu>,
}

/// GPUs for a unit: which kind and how many. The kind is the platform's
/// accelerator name (`nvidia-l4`, `nvidia-tesla-t4`); a local install
/// ignores the kind and hands every GPU the host has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gpu {
    pub kind: String,
    #[serde(default = "one")]
    pub count: u32,
}

fn one() -> u32 {
    1
}

/// Per-unit health window overrides. `None` means "use the supervisor's
/// default" (`FLAKY_AFTER` / `RECOVERY_AFTER`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnitHealth {
    /// Seconds a unit must be continuously NOT ready before the
    /// supervisor declares it flaky.
    #[serde(default, rename = "flakyAfterSeconds", alias = "flaky_after_seconds", skip_serializing_if = "Option::is_none")]
    pub flaky_after_seconds: Option<u32>,
    /// Seconds a flaky unit must be continuously ready before it is
    /// declared recovered.
    #[serde(default, rename = "recoveryAfterSeconds", alias = "recovery_after_seconds", skip_serializing_if = "Option::is_none")]
    pub recovery_after_seconds: Option<u32>,
}

/// What stop does to one unit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StopBehavior {
    /// Stop the unit's containers (its machine, on a cloud). Disks are
    /// kept.
    #[default]
    Stop,
    /// Leave the unit running on stop; only terminate removes it. For a
    /// unit that must persist across a project stop (a license server, a
    /// model that takes long to load).
    KeepRunning,
}

// =============================================================
// Containers
// =============================================================

/// No `Default`: a container without an image means nothing. Build one
/// with [`Container::new`] and the `with_*` setters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Container {
    pub name: String,
    pub image: Image,

    /// Replaces the image's entrypoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,

    #[serde(default)]
    pub args: Vec<String>,

    #[serde(default)]
    pub env: Vec<EnvEntry>,

    #[serde(default)]
    pub ports: Vec<ContainerPort>,

    #[serde(default)]
    pub limits: Limits,

    #[serde(default)]
    pub mounts: Vec<Mount>,

    /// When the container counts as ready. Absent: ready once running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness: Option<Probe>,

    /// When the container counts as broken and is restarted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liveness: Option<Probe>,

    /// The user (and optionally group) the container runs as,
    /// `"uid"` or `"uid:gid"`. Absent: the image's own.
    #[serde(default, rename = "runAs", alias = "run_as", skip_serializing_if = "Option::is_none")]
    pub run_as: Option<String>,
}

impl Container {
    pub fn new(name: impl Into<String>, image: Image) -> Self {
        Self {
            name: name.into(),
            image,
            command: None,
            args: Vec::new(),
            env: Vec::new(),
            ports: Vec::new(),
            limits: Limits::default(),
            mounts: Vec::new(),
            readiness: None,
            liveness: None,
            run_as: None,
        }
    }

    pub fn with_command(mut self, command: Vec<String>) -> Self {
        self.command = Some(command);
        self
    }

    pub fn with_args(mut self, args: Vec<String>) -> Self {
        self.args = args;
        self
    }

    pub fn with_env(mut self, env: Vec<EnvEntry>) -> Self {
        self.env = env;
        self
    }

    pub fn with_ports(mut self, ports: Vec<ContainerPort>) -> Self {
        self.ports = ports;
        self
    }

    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_mounts(mut self, mounts: Vec<Mount>) -> Self {
        self.mounts = mounts;
        self
    }

    pub fn with_readiness(mut self, probe: Probe) -> Self {
        self.readiness = Some(probe);
        self
    }

    pub fn with_liveness(mut self, probe: Probe) -> Self {
        self.liveness = Some(probe);
        self
    }

    pub fn with_run_as(mut self, run_as: impl Into<String>) -> Self {
        self.run_as = Some(run_as.into());
        self
    }
}

/// Where a container's image comes from.
///
/// An upstream reference is used VERBATIM: a mutable tag is not resolved
/// to a digest, so `postgres:16` moving underneath changes nothing in the
/// spec and re-applies nothing. Pin by digest to make an image change land.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Image {
    /// From a registry: `"postgres:18"`, `"ghcr.io/huggingface/tgi@sha256:..."`.
    Upstream { reference: String },
    /// Built from a directory listed in the node's `metadata.images`. A
    /// version build builds it as `weft-infra-{name}:{hash}` and registers
    /// the ref per infra place; resolve swaps the name for that ref.
    Local { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvEntry {
    pub name: String,
    pub value: String,
}

impl EnvEntry {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self { name: name.into(), value: value.into() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerPort {
    /// Named so endpoints can reference it.
    pub name: String,
    pub port: u16,
    #[serde(default)]
    pub protocol: Protocol,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Protocol {
    #[default]
    #[serde(rename = "TCP")]
    Tcp,
    #[serde(rename = "UDP")]
    Udp,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

/// The most one container may use. Absent: no limit beyond the unit's
/// machine.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mount {
    /// The volume's name (`Volume.name`).
    pub volume: String,
    pub path: String,
    /// Mount only this directory of the volume, for a container that has
    /// no business with the rest of it. It must exist when the container
    /// starts: an init container of the unit makes it.
    #[serde(default, rename = "subPath", alias = "sub_path", skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
    #[serde(default, rename = "readOnly", alias = "read_only")]
    pub read_only: bool,
}

impl Mount {
    /// The whole of `volume`, at `path`, writable.
    pub fn new(volume: impl Into<String>, path: impl Into<String>) -> Self {
        Self { volume: volume.into(), path: path.into(), sub_path: None, read_only: false }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Probe {
    pub kind: ProbeKind,
    #[serde(default, rename = "initialDelaySeconds", alias = "initial_delay_seconds")]
    pub initial_delay_seconds: u32,
    #[serde(default = "default_period_seconds", rename = "periodSeconds", alias = "period_seconds")]
    pub period_seconds: u32,
    #[serde(default = "default_timeout_seconds", rename = "timeoutSeconds", alias = "timeout_seconds")]
    pub timeout_seconds: u32,
    #[serde(default = "default_failure_threshold", rename = "failureThreshold", alias = "failure_threshold")]
    pub failure_threshold: u32,
}

fn default_period_seconds() -> u32 {
    10
}
fn default_timeout_seconds() -> u32 {
    1
}
fn default_failure_threshold() -> u32 {
    3
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProbeKind {
    /// Ready when `GET path` on `port` answers 2xx or 3xx.
    Http { path: String, port: u16 },
    /// Ready once `port` accepts connections.
    Tcp { port: u16 },
    /// Ready when `command`, run inside the container, exits 0.
    Exec { command: Vec<String> },
}

impl Probe {
    fn of(kind: ProbeKind) -> Self {
        Self {
            kind,
            initial_delay_seconds: 0,
            period_seconds: default_period_seconds(),
            timeout_seconds: default_timeout_seconds(),
            failure_threshold: default_failure_threshold(),
        }
    }

    pub fn http(path: impl Into<String>, port: u16) -> Self {
        Self::of(ProbeKind::Http { path: path.into(), port })
    }

    /// For a service that answers the readiness question ITSELF
    /// (`pg_isready`, `redis-cli ping`): the honest probe for anything
    /// that accepts connections before it is actually serving.
    pub fn exec(command: Vec<String>) -> Self {
        Self::of(ProbeKind::Exec { command })
    }

    /// Ready once it accepts connections on `port`. Weaker than
    /// [`Self::exec`], which a service that can answer for itself should
    /// use.
    pub fn tcp(port: u16) -> Self {
        Self::of(ProbeKind::Tcp { port })
    }

    pub fn with_initial_delay(mut self, seconds: u32) -> Self {
        self.initial_delay_seconds = seconds;
        self
    }
}

// =============================================================
// Volumes
// =============================================================

// No `deny_unknown_fields`: serde cannot combine it with the flattened
// kind, which carries the rest of the fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Volume {
    pub name: String,
    #[serde(flatten)]
    pub kind: VolumeKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VolumeKind {
    /// A disk that outlives stop and upgrade; see
    /// `InfraSpec::keep_on_terminate` for terminate.
    Disk {
        /// `"10Gi"`.
        size: String,
        /// The platform's disk kind, when it offers several
        /// (`pd-balanced`, `pd-ssd`). Absent: its default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        class: Option<String>,
    },
    /// Scratch space the unit's containers share, emptied whenever the
    /// unit starts.
    Scratch {
        #[serde(default, rename = "sizeLimit", alias = "size_limit", skip_serializing_if = "Option::is_none")]
        size_limit: Option<String>,
    },
}

// =============================================================
// Endpoints
// =============================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    /// What `ctx.endpoint(name)` asks for.
    pub name: String,
    pub target: EndpointTarget,
    #[serde(default)]
    pub expose: Expose,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EndpointTarget {
    /// A named port of one container of one of this node's units.
    Unit { unit: String, container: String, port: String },
    /// A service that already exists somewhere else: nothing is started
    /// for it, and the endpoint answers this address.
    External { url: String },
}

/// Where one declared endpoint of a running infra node answers: what
/// `ctx.endpoint(name)` resolves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointAddress {
    /// The address the project's own workers use.
    pub url: String,
    /// The full address a caller outside the install uses, for an
    /// [`Expose::Public`] endpoint; `None` for every other endpoint.
    pub public_url: Option<String>,
}

/// Who may reach an endpoint besides the project's own workers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Expose {
    /// The project's workers and its other infra only.
    #[default]
    Project,
    /// HTTP through the install's front door, at
    /// `/infra/<project>/<instance>/<path>` (rewritten to `<path>` on the
    /// way in). Reachable from the internet.
    Public { path: String },
    /// The network the install runs on: the machine itself on a local
    /// install (the port is bound on loopback), the install's private
    /// network on a cloud. Never the internet.
    ///
    /// An endpoint that HANDS OUT a credential is never this, however
    /// convenient: a Postgres node marks the port Postgres answers on,
    /// because reaching it still costs a password, and keeps the little
    /// server that mints that password `Project` for ever.
    SameNetwork,
}

// =============================================================
// Provision context
// =============================================================

/// Handed to `Node::provision_infra`.
///
/// `Image::Local { name }` references are resolved to built image refs
/// by the supervisor at apply time, never by the provision body: the body
/// only declares the name, so it stays deterministic given its inputs.
#[derive(Debug, Clone)]
pub struct InfraProvisionContext {
    pub project_id: uuid::Uuid,
    /// The node being provisioned, spelled the way the program writes it
    /// (`db`, or `one.db` inside the file the site `one` includes). One
    /// per INSTANCE: a file included twice provisions its infra once per
    /// call, and this is the name that tells the two apart.
    pub node: String,
    pub tenant_id: String,
}

impl InfraProvisionContext {
    pub fn new(project_id: uuid::Uuid, node: String, tenant_id: String) -> Self {
        Self { project_id, node, tenant_id }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProvisionContextError {
    #[error(
        "no image declared with name '{0}'; add it to NodeMetadata.images and provide a Dockerfile at images/{0}/Dockerfile"
    )]
    UnknownImage(String),
}

/// Environment variables, as a map, for a platform that takes them so.
pub fn env_map(env: &[EnvEntry]) -> BTreeMap<String, String> {
    env.iter().map(|e| (e.name.clone(), e.value.clone())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> InfraSpec {
        InfraSpec {
            units: vec![Unit {
                name: "bridge".into(),
                containers: vec![Container::new("main", Image::Local { name: "bridge".into() })
                    .with_args(vec!["--port".into(), "8090".into()])
                    .with_env(vec![EnvEntry::new("MODE", "prod")])
                    .with_ports(vec![ContainerPort { name: "http".into(), port: 8090, protocol: Protocol::Tcp }])
                    .with_limits(Limits { cpu: Some("1".into()), memory: Some("128Mi".into()) })
                    .with_mounts(vec![Mount::new("auth", "/auth")])
                    .with_readiness(Probe::http("/health", 8090))],
                machine: MachineShape { gpu: Some(Gpu { kind: "nvidia-l4".into(), count: 1 }), ..Default::default() },
                ..Default::default()
            }],
            volumes: vec![Volume { name: "auth".into(), kind: VolumeKind::Disk { size: "100Mi".into(), class: None } }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                target: EndpointTarget::Unit { unit: "bridge".into(), container: "main".into(), port: "http".into() },
                expose: Expose::Project,
            }],
            keep_on_terminate: Vec::new(),
        }
    }

    #[test]
    fn a_spec_round_trips() {
        let original = spec();
        let v = serde_json::to_value(&original).unwrap();
        let back: InfraSpec = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(serde_json::to_value(&back).unwrap(), v);
        assert_eq!(v["volumes"][0]["kind"], "disk");
        assert_eq!(v["units"][0]["machine"]["gpu"]["kind"], "nvidia-l4");
    }

    #[test]
    fn an_unknown_field_is_refused_rather_than_ignored() {
        let mut v = serde_json::to_value(spec()).unwrap();
        v["units"][0]["instanceOptions"] = serde_json::json!({});
        assert!(serde_json::from_value::<InfraSpec>(v).is_err());
    }

    #[test]
    fn defaults_are_the_quiet_ones() {
        let u = Unit::default();
        assert_eq!(u.on_stop, StopBehavior::Stop);
        assert_eq!(Expose::default(), Expose::Project);
        let g: Gpu = serde_json::from_value(serde_json::json!({ "kind": "nvidia-t4" })).unwrap();
        assert_eq!(g.count, 1);
        assert_eq!(serde_json::to_string(&Protocol::Tcp).unwrap(), "\"TCP\"");
    }
}
