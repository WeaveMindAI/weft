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
//! A unit may instead run one [`Process`] straight on the machine weft
//! runs on (a local install's), as the person running weft, for a
//! program that needs what only that machine has: their logins, a folder
//! of theirs, a program they installed.
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
    #[serde(default, rename = "keepOnTerminate", skip_serializing_if = "Vec::is_empty")]
    pub keep_on_terminate: Vec<String>,

    /// What the supervisor does about a copy whose units stop being ready
    /// (see `super::health`): read in order, on each copy alone. `None`:
    /// the node writes none and runs `super::health::built_in_rules`; an
    /// empty list: it runs none.
    #[serde(default, rename = "healthRules", skip_serializing_if = "Option::is_none")]
    pub health_rules: Option<Vec<super::health::HealthRule>>,

    /// What the node says a person should know about the copy it declares
    /// while it runs (it runs short of something it would normally have),
    /// one plain sentence each. `weft infra status` prints them as
    /// warnings, beside what the host itself runs differently from what
    /// was asked. Usually empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

/// What a terminate does with the disks the node lists in
/// `keep_on_terminate`. A property of each terminate command, never of the
/// copy: the same copy is terminated keeping them by a person's stop and
/// wiping them when its instance is wiped.
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
    /// fields fall back to the supervisor's defaults. What is done about a
    /// flaky unit is the node's `InfraSpec::health_rules`.
    #[serde(default)]
    pub health: UnitHealth,

    /// The one process the unit runs on the machine weft runs on, in place
    /// of containers: a unit runs containers or a process, never both
    /// (`super::resolve` refuses either mix, and a unit with neither).
    /// Left out of the serialized unit when `None`, so a container unit
    /// reads and hashes as it did before units could run a process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<Process>,
}

// =============================================================
// Process units
// =============================================================

/// One program a unit runs as a process on the machine weft runs on, as
/// the person running weft: for a program that needs what only that
/// machine has (the person's logins, a folder of theirs, a program they
/// installed). Only a local install has such a machine; a cloud install
/// refuses the unit before anything starts.
///
/// The process starts in a folder of its own (its working directory),
/// with `files` written into it before every start, the machine's `PATH`
/// with the program folders of each of its `needs` weft installs itself
/// ([`crate::needs::Recipe`]) put first, `HOME` (and the few variables a
/// program run by the person reads, when the machine sets them), `env`,
/// and one free loopback port per entry of `ports`. A process that exits on its own is started again,
/// the way a container is.
///
/// Built with [`Process::new`] and the `with_*` setters. A process mounts
/// no volume, takes no memory or CPU limit and runs as no other user: it
/// shares the machine with everything else the person runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
    /// The program and its arguments; the first word is looked up on the
    /// `PATH` the process gets.
    pub command: Vec<String>,
    /// Files written into the process's folder before every start, by
    /// their path in it (`server.py`, `conf/settings.json`) to their
    /// content. Part of the unit's hash, so changing one starts the
    /// process again with it. What the process writes there itself stays
    /// across its restarts and stops; terminate removes the folder.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvEntry>,
    /// The ports it listens on, by name: the host picks a free loopback
    /// port for each and puts its number in the variable the port names.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<ProcessPort>,
    /// When it counts as ready. Absent: ready once it runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness: Option<ProcessReadiness>,
    /// What it needs from the machine (its `command`'s program, what that
    /// one runs), checked before it starts: one missing refuses the start,
    /// naming it and how to install it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs: Vec<crate::needs::Need>,
}

impl Process {
    pub fn new(command: Vec<String>) -> Self {
        Self { command, files: BTreeMap::new(), env: Vec::new(), ports: Vec::new(), readiness: None, needs: Vec::new() }
    }

    /// The file at `path` in the process's folder holds `content`.
    pub fn with_file(mut self, path: impl Into<String>, content: impl Into<String>) -> Self {
        self.files.insert(path.into(), content.into());
        self
    }

    pub fn with_env(mut self, env: Vec<EnvEntry>) -> Self {
        self.env = env;
        self
    }

    pub fn with_ports(mut self, ports: Vec<ProcessPort>) -> Self {
        self.ports = ports;
        self
    }

    pub fn with_readiness(mut self, readiness: ProcessReadiness) -> Self {
        self.readiness = Some(readiness);
        self
    }

    pub fn with_needs(mut self, needs: Vec<crate::needs::Need>) -> Self {
        self.needs = needs;
        self
    }
}

/// A port a process listens on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessPort {
    /// Named so endpoints and the readiness check can reference it.
    pub name: String,
    /// The environment variable the process finds the port's number in
    /// (`PORT`, `WEFT_AGENT_PORT`).
    pub env: String,
}

impl ProcessPort {
    pub fn new(name: impl Into<String>, env: impl Into<String>) -> Self {
        Self { name: name.into(), env: env.into() }
    }
}

/// When a process counts as ready: `GET path` on its port `port` answers
/// 2xx or 3xx.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessReadiness {
    /// The port, by its name in `Process::ports`.
    pub port: String,
    pub path: String,
    /// How long after it starts it may fail the check and still count as
    /// starting rather than not ready: what a container's probe makes of
    /// its own defaults (no delay, three failures ten seconds apart).
    #[serde(default = "default_startup_seconds", rename = "startupSeconds")]
    pub startup_seconds: u32,
    #[serde(default = "default_timeout_seconds", rename = "timeoutSeconds")]
    pub timeout_seconds: u32,
}

fn default_startup_seconds() -> u32 {
    30
}

impl ProcessReadiness {
    /// Ready when `GET path` on the port named `port` answers 2xx or 3xx.
    pub fn http(port: impl Into<String>, path: impl Into<String>) -> Self {
        Self { port: port.into(), path: path.into(), startup_seconds: default_startup_seconds(), timeout_seconds: default_timeout_seconds() }
    }

    pub fn with_startup_seconds(mut self, seconds: u32) -> Self {
        self.startup_seconds = seconds;
        self
    }
}

/// A memory size as `MachineShape::memory` and a container's limit write
/// it (`4Gi`, `512Mi`, `2G`, `1500M`), in MiB, unrounded: a platform
/// sizing a machine rounds up, a node planning what fits rounds down.
/// `None` when it is not one.
pub fn memory_mib(raw: &str) -> Option<f64> {
    let raw = raw.trim();
    // A gigabyte (10^9 bytes) is about 953.7 MiB.
    [("Gi", 1024.0), ("Mi", 1.0), ("G", 1e9 / 1048576.0), ("M", 1e6 / 1048576.0)]
        .into_iter()
        .find_map(|(suffix, per)| raw.strip_suffix(suffix).map(|n| n.parse::<f64>().ok().map(|n| n * per)))
        .flatten()
}

/// The machine a unit needs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineShape {
    /// CPUs, as a number (`"2"`, `"0.5"`, `"250m"`). Unset: the sum of
    /// the containers' own limits. A cloud picks the cheapest machine
    /// that holds both numbers, shared-core ones included (a quarter of
    /// a CPU with 1 GB is Compute Engine's `e2-micro`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    /// Memory (`"4Gi"`, `"512Mi"`). Unset: as for `cpu`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    /// GPUs attached to the unit's machine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu: Option<Gpu>,
    /// The platform's own machine type, used as is (`e2-micro`,
    /// `n2-highmem-8` on Compute Engine): a cloud then skips picking
    /// one from `cpu` and `memory`. A local install ignores it.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

/// A node's own sizes for the machine inputs the language gives it
/// (`"features": { "machineInputs": { "cpu": "1", "memory": "2Gi" } }`):
/// what its `cpu` and `memory` hold when the program writes neither.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineDefaults {
    pub cpu: String,
    pub memory: String,
}

impl MachineShape {
    /// The inputs `features.machineInputs` adds, in this order: the CPUs,
    /// the memory, and the platform's own machine type.
    pub const NODE_FIELDS: [&'static str; 3] = ["cpu", "memory", "machineType"];
    /// What a `cpu` reads as: a number, or thousandths with `m`.
    const CPU_PATTERN: &'static str = r"^[0-9]+(\.[0-9]+)?m?$";
    /// What a `memory` reads as: a number followed by Gi, Mi, G or M.
    const MEMORY_PATTERN: &'static str = r"^[0-9]+(\.[0-9]+)?(Gi|Mi|G|M)$";

    /// The three inputs `features.machineInputs` gives an infra node,
    /// `cpu` and `memory` defaulting to the node's own `defaults`. A
    /// default that does not read as its size is refused, naming it.
    pub fn node_inputs(defaults: &MachineDefaults) -> Result<Vec<crate::node::InputSpec>, String> {
        let [cpu, memory, machine_type] = Self::NODE_FIELDS;
        for (name, value, pattern) in [(cpu, &defaults.cpu, Self::CPU_PATTERN), (memory, &defaults.memory, Self::MEMORY_PATTERN)] {
            if !regex::Regex::new(pattern).expect("a valid pattern").is_match(value) {
                return Err(format!("features.machineInputs: its `{name}` of '{value}' is no size a machine reads"));
            }
        }
        let input = |spec: serde_json::Value| -> crate::node::InputSpec {
            serde_json::from_value(spec).expect("a machine input is a valid InputSpec")
        };
        Ok(vec![
            input(serde_json::json!({
                "name": cpu,
                "type": "String",
                "default": defaults.cpu,
                "label": "CPUs",
                "description": "CPUs the machine sustains (`1`, `0.25`, `500m`). A cloud install picks its cheapest machine holding this and `memory`: a quarter CPU with 1Gi is Compute Engine's `e2-micro`. A local install does not cap the container.",
            })),
            input(serde_json::json!({
                "name": memory,
                "type": "String",
                "default": defaults.memory,
                "label": "Memory",
                "description": "Memory, a number followed by Gi, Mi, G or M (`2Gi`). Sized with `cpu`; a local install does not cap the container.",
            })),
            input(serde_json::json!({
                "name": machine_type,
                "type": "String",
                "label": "Machine type",
                "placeholder": "picked from CPUs and memory",
                "description": "Last resort: a cloud's own machine type, used as is instead of the one picked from `cpu` and `memory` (`n2-standard-4` on Compute Engine). A local install ignores it.",
            })),
        ])
    }

    /// The checks on [`Self::node_inputs`], one per size, for a node of
    /// type `node_type`: a `cpu` or a `memory` that does not read as one
    /// is refused, written or wired.
    pub fn node_rules(node_type: &str) -> Vec<crate::node::ValidationRule> {
        let [cpu, memory, _] = Self::NODE_FIELDS;
        let rule = |field: &str, pattern: &str, write: &str| -> crate::node::ValidationRule {
            serde_json::from_value(serde_json::json!({
                "when": { "kind": "all", "of": [
                    { "kind": "value", "field": field },
                    { "kind": "not", "of": { "kind": "value", "field": field, "matches": pattern } }
                ] },
                "then": {
                    "message": format!("{node_type} '{{id}}' has a `{field}` it cannot read. {write}"),
                    "level": "structural",
                    "severity": "error",
                    "port": field
                }
            }))
            .expect("a machine rule is a valid ValidationRule")
        };
        vec![
            rule(cpu, Self::CPU_PATTERN, "Write a number, or thousandths with m (1, 500m)."),
            rule(memory, Self::MEMORY_PATTERN, "Write a number followed by Gi, Mi, G or M (2Gi)."),
        ]
    }

    /// The machine a node with `features.machineInputs` asked for, read
    /// off its inputs: its `cpu` and `memory` (which the language checked),
    /// and its `machineType` when one is written (a blank one is absent).
    #[cfg(feature = "runtime")]
    pub fn from_inputs(inputs: &crate::context::ValueBag) -> crate::error::WeftResult<Self> {
        let [cpu, memory, machine_type] = Self::NODE_FIELDS;
        Ok(Self { cpu: inputs.opt(cpu)?, memory: inputs.opt(memory)?, gpu: None, kind: inputs.opt(machine_type)? })
    }
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

impl StopBehavior {
    /// Its `kind`, as serde writes it and the unit's row stores it
    /// (`infra_unit.stop_behavior`).
    pub const fn kind(self) -> &'static str {
        match self {
            StopBehavior::Stop => "stop",
            StopBehavior::KeepRunning => "keep_running",
        }
    }
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
    /// A named port of the process one of this node's units runs
    /// (`Unit::process`).
    Process { unit: String, port: String },
    /// A service that already exists somewhere else: nothing is started
    /// for it, and the endpoint answers this address.
    External { url: String },
    /// A path inside another infra node's endpoint: `infra` is that
    /// endpoint, as the program wired it into this node's input (a
    /// program this node started inside a computer, which the computer's
    /// own server passes `path` on to). Nothing is started for it, and
    /// its address is never kept: every lookup reads where `infra`
    /// answers now and adds `path` (`EndpointRecord::Within`), so the
    /// other node moving (recreated on another port) moves this one with
    /// it. One hop: `infra` must not itself be such an endpoint, and it is
    /// reached by the project's own workers only (`Expose::Project`).
    ///
    /// Whatever answers at `path` was put there by this node, so a
    /// terminate of this node asks the other one to remove it: weft sends
    /// `DELETE` to `path` itself (never below it), and the other node's
    /// server answers 2xx once nothing of it is left there, also when
    /// nothing was (`weft_infra_supervisor::inside`). A stop asks nothing.
    Within { infra: super::InfraHandle, path: String },
}

impl EndpointTarget {
    /// The unit that serves it; `None` for an outside address or a path
    /// inside another node's endpoint.
    pub fn unit(&self) -> Option<&str> {
        match self {
            EndpointTarget::Unit { unit, .. } | EndpointTarget::Process { unit, .. } => Some(unit),
            EndpointTarget::External { .. } | EndpointTarget::Within { .. } => None,
        }
    }
}

/// Where one endpoint of a running copy answers, as the copy's row keeps
/// it (`infra_node.endpoints_json`, endpoint name to this): the address
/// its host gave it, or, for an [`EndpointTarget::Within`], the other
/// node's endpoint and the path under it, which every reader follows
/// itself ([`EndpointRecord::address`]). An address is a plain string on
/// the wire, the way every row stored one before an endpoint could live
/// inside another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EndpointRecord {
    Url(String),
    Within(WithinRecord),
}

/// An [`EndpointTarget::Within`] as its copy's row keeps it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WithinRecord {
    pub within: super::InfraHandle,
    pub path: String,
}

impl EndpointRecord {
    /// The endpoint this one lives inside, which a reader reads the record
    /// of before asking [`Self::address`]; `None` for an address.
    pub fn within(&self) -> Option<&super::InfraHandle> {
        match self {
            EndpointRecord::Url(_) => None,
            EndpointRecord::Within(record) => Some(&record.within),
        }
    }

    /// What a status line says of it: the address, or where it lives
    /// (`inside endpoint 'api' of 'box' at /apps/claude`), which a reader
    /// follows at every lookup.
    pub fn summary(&self) -> String {
        match self {
            EndpointRecord::Url(url) => url.clone(),
            EndpointRecord::Within(WithinRecord { within, path }) => format!("inside {within} at {path}"),
        }
    }

    /// The address this record says, given the record of the endpoint it
    /// lives inside when it does (`inner`, read for [`Self::within`]:
    /// `None` when that copy is not running or has no such endpoint).
    /// Followed one hop: an endpoint inside one that is itself inside
    /// another is refused. `Err` says why, for a person.
    pub fn address(self, inner: Option<EndpointRecord>) -> Result<String, String> {
        let WithinRecord { within, path } = match self {
            EndpointRecord::Url(url) => return Ok(url),
            EndpointRecord::Within(record) => record,
        };
        match inner {
            Some(EndpointRecord::Url(url)) => Ok(within_url(&url, &path)),
            Some(EndpointRecord::Within(_)) => Err(format!(
                "{within} is itself inside another infra node's endpoint, and an endpoint inside one is followed one \
                 step only: point it at the node that runs the server"
            )),
            None => Err(format!(
                "the infra node '{}' this endpoint runs inside is not running: start it with `weft infra start`",
                crate::project::plain_id(within.place())
            )),
        }
    }
}

/// The address of a path `path` inside the endpoint answering at `url`.
pub fn within_url(url: &str, path: &str) -> String {
    format!("{}{path}", url.trim_end_matches('/'))
}

/// Where one declared endpoint of a running infra node answers: what
/// `ctx.endpoint(name)` resolves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    /// `/infra/<project>/<copy_id>/<path>` (rewritten to `<path>` on the
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

    /// The checks `machineInputs` adds refuse a size a machine cannot
    /// read and take one it can.
    #[test]
    fn the_machine_checks_hold_each_size_to_its_shape() {
        let cpu = &MachineShape::node_rules("Box")[0];
        let reads = |value: &str| regex::Regex::new(MachineShape::CPU_PATTERN).unwrap().is_match(value);
        assert!(reads("1") && reads("0.25") && reads("500m"));
        assert!(!reads("one") && !reads("1 cpu"));
        assert_eq!(cpu.then.port.as_deref(), Some("cpu"));
        let memory = regex::Regex::new(MachineShape::MEMORY_PATTERN).unwrap();
        assert!(memory.is_match("2Gi") && memory.is_match("512Mi") && memory.is_match("1.5G"));
        assert!(!memory.is_match("2GB") && !memory.is_match("2"));
    }

    /// A node reads its machine off its inputs in one call, a blank
    /// machine type (taken out of the bag as no value) left to the
    /// platform's pick.
    #[cfg(feature = "runtime")]
    #[test]
    fn a_machine_is_read_off_the_inputs() {
        let bag = |values: serde_json::Value| {
            let mut delivered = values.as_object().unwrap().clone();
            crate::run::ready::drop_blank_texts(&mut delivered, |name| name == "machineType");
            crate::context::ValueBag::inputs(delivered, Default::default(), Vec::new())
        };
        let machine = MachineShape::from_inputs(&bag(serde_json::json!({ "cpu": "1", "memory": "2Gi", "machineType": " " }))).unwrap();
        assert_eq!(machine, MachineShape { cpu: Some("1".into()), memory: Some("2Gi".into()), gpu: None, kind: None });
        let typed = MachineShape::from_inputs(&bag(serde_json::json!({ "cpu": "1", "memory": "2Gi", "machineType": "e2-micro" }))).unwrap();
        assert_eq!(typed.kind.as_deref(), Some("e2-micro"));
    }

    /// The kind a unit's row stores is the one serde writes.
    #[test]
    fn a_stop_behaviors_kind_is_its_serde_tag() {
        for behavior in [StopBehavior::Stop, StopBehavior::KeepRunning] {
            assert_eq!(serde_json::to_value(behavior).unwrap()["kind"], behavior.kind());
        }
    }

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
            health_rules: None,
            notes: Vec::new(),
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

    /// A container unit serializes exactly as it did before units could
    /// run a process: the applied hash of every running copy is taken over
    /// this shape, so one more key would replace every copy at the next
    /// apply.
    #[test]
    fn a_container_unit_serializes_as_it_always_did() {
        let unit = Unit {
            name: "db".into(),
            containers: vec![Container::new("pg", Image::Upstream { reference: "postgres:18".into() })
                .with_ports(vec![ContainerPort { name: "sql".into(), port: 5432, protocol: Protocol::Tcp }])],
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_string(&unit).unwrap(),
            r#"{"name":"db","containers":[{"name":"pg","image":{"kind":"upstream","reference":"postgres:18"},"args":[],"env":[],"ports":[{"name":"sql","port":5432,"protocol":"TCP"}],"limits":{},"mounts":[]}],"initContainers":[],"machine":{},"onStop":{"kind":"stop"},"health":{}}"#
        );
    }

    /// A process unit and the endpoint to one of its ports round-trip,
    /// in the casing every JSON field weft produces has, and the parts a
    /// process leaves unset are left out.
    #[test]
    fn a_process_unit_round_trips() {
        let python = crate::needs::Need {
            name: "python3".into(),
            stage: crate::needs::NeedStage::Run,
            check: vec!["python3".into(), "--version".into()],
            install: BTreeMap::new(),
            how: None,
            recipe: None,
        };
        let unit = Unit {
            name: "server".into(),
            process: Some(
                Process::new(vec!["python3".into(), "server.py".into()])
                    .with_file("server.py", "print('hi')\n")
                    .with_env(vec![EnvEntry::new("MODE", "prod")])
                    .with_ports(vec![ProcessPort::new("http", "PORT")])
                    .with_readiness(ProcessReadiness::http("http", "/health").with_startup_seconds(60))
                    .with_needs(vec![python]),
            ),
            ..Default::default()
        };
        let v = serde_json::to_value(&unit).unwrap();
        assert_eq!(v["process"]["files"]["server.py"], "print('hi')\n");
        assert_eq!(v["process"]["ports"][0], serde_json::json!({ "name": "http", "env": "PORT" }));
        assert_eq!(v["process"]["readiness"], serde_json::json!({ "port": "http", "path": "/health", "startupSeconds": 60, "timeoutSeconds": 1 }));
        assert_eq!(serde_json::from_value::<Unit>(v).unwrap(), unit);
        let bare = serde_json::to_value(Process::new(vec!["true".into()])).unwrap();
        assert_eq!(bare, serde_json::json!({ "command": ["true"] }));
        let readiness: ProcessReadiness = serde_json::from_value(serde_json::json!({ "port": "http", "path": "/" })).unwrap();
        assert_eq!(readiness, ProcessReadiness::http("http", "/"));

        let target = EndpointTarget::Process { unit: "server".into(), port: "http".into() };
        let v = serde_json::to_value(&target).unwrap();
        assert_eq!(v, serde_json::json!({ "kind": "process", "unit": "server", "port": "http" }));
        assert_eq!(serde_json::from_value::<EndpointTarget>(v).unwrap(), target);
        assert_eq!(target.unit(), Some("server"));
        assert_eq!(EndpointTarget::External { url: "http://x".into() }.unit(), None);
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
