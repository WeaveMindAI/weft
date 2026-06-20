//! Typed declarations for infrastructure nodes.
//!
//! An infra node implements `Node::provision`, which returns an
//! [`InfraSpec`]. The dispatcher's apply executor compiles the spec
//! to a list of kubernetes manifests, resolves image digests, hashes
//! the resolved spec, and applies via `kubectl`. The same spec is
//! re-derived on every restart / upgrade so drift detection collapses
//! to "does the resolved hash match what's currently applied".
//!
//! Design notes:
//! - Weft labels (`weft.dev/{role,tenant,project,node,instance}`) are
//!   stamped by the spec compiler, NOT by node authors. Specs that
//!   include weft.dev labels manually have them overridden.
//! - Endpoint URLs are computed deterministically from the project
//!   namespace + endpoint name + container port. The node body never
//!   constructs URLs; it asks the context for `endpoint_url(name)`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

// =============================================================
// Top-level InfraSpec
// =============================================================

/// What an infra node wants the cluster to look like.
///
/// Returned by `Node::provision`. Pure value: hashing the resolved
/// form (after image-digest resolution) yields the canonical drift
/// signal. The compiler turns this into a list of kubernetes
/// manifests; everything users care about is here.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InfraSpec {
    /// Pod templates. Most nodes have exactly one Unit.
    #[serde(default)]
    pub units: Vec<Unit>,

    /// Persistent storage. PVCs preserved across upgrades by name.
    #[serde(default)]
    pub volumes: Vec<Volume>,

    /// Secrets and configmaps. Either declared inline (literal) or
    /// referenced by name (must already exist in the project
    /// namespace).
    #[serde(default)]
    pub config: Vec<ConfigSource>,

    /// Named ports exposed via Services. `endpoint_url(name)` resolves
    /// the cluster-internal URL at runtime.
    #[serde(default)]
    pub endpoints: Vec<Endpoint>,

    /// NetworkPolicy ingress/egress overrides on top of the project
    /// default-deny baseline.
    #[serde(default)]
    pub access: Access,

    /// What stop/upgrade/terminate mean for this node.
    #[serde(default)]
    pub lifecycle: Lifecycle,
}

// =============================================================
// Units (workloads)
// =============================================================

/// One Pod template. The compiler turns this into a Deployment,
/// StatefulSet, DaemonSet, or Job depending on `kind`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Unit {
    /// Local name within this node. Used to reference the Unit from
    /// Endpoint targets and the compiler's label stamping.
    pub name: String,

    #[serde(default)]
    pub kind: UnitKind,

    #[serde(default)]
    pub containers: Vec<Container>,

    #[serde(default, rename = "initContainers", alias = "init_containers")]
    pub init_containers: Vec<Container>,

    #[serde(default, rename = "podOptions", alias = "pod_options")]
    pub pod_options: PodOptions,

    /// Replicas + optional autoscale. Lives ON THE UNIT (not on the
    /// node) because different units inside one infra node may want
    /// different scale shapes (e.g. a "primary" StatefulSet at 1
    /// replica next to a "sentinel" Deployment at 3).
    ///
    /// For DaemonSet / Job kinds, replicas is meaningless and the
    /// compiler ignores it. For autoscale, the HPA emits only when
    /// `Some` is present.
    #[serde(default)]
    pub scaling: ScalingPolicy,

    /// Upgrade strategy. Per-Unit (not per-node) because a node
    /// with a StatefulSet + Deployment may want Recreate for the SS
    /// (e.g. exclusive file lock) and Rolling for the Deployment.
    /// Only applied to `UnitKind::Deployment`; ignored for
    /// StatefulSet / DaemonSet / Job (those have their own k8s
    /// update strategies).
    #[serde(default, rename = "onUpgrade", alias = "on_upgrade")]
    pub on_upgrade: UpgradeBehavior,

    /// Stop behavior. Per-Unit (not per-node): a stateless cache unit
    /// can scale to zero on stop while a license-server unit in the
    /// same node stays up until terminate.
    #[serde(default, rename = "onStop", alias = "on_stop")]
    pub on_stop: StopBehavior,

    /// Health windows for this unit's flaky/recovered transitions.
    /// Per-Unit: a slow-starting model unit can tolerate a longer
    /// not-ready window than a sidecar in the same node. Unset fields
    /// fall back to the supervisor's global defaults.
    #[serde(default)]
    pub health: UnitHealth,
}

/// Per-unit health window overrides. `None` means "use the
/// supervisor's global default" (`FLAKY_AFTER` / `RECOVERY_AFTER`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnitHealth {
    /// Seconds a unit must be continuously NOT ready before the
    /// supervisor declares it flaky. `None` -> global default.
    #[serde(
        default,
        rename = "flakyAfterSeconds",
        alias = "flaky_after_seconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub flaky_after_seconds: Option<u32>,
    /// Seconds a flaky unit must be continuously ready before it's
    /// declared recovered. `None` -> global default.
    #[serde(
        default,
        rename = "recoveryAfterSeconds",
        alias = "recovery_after_seconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery_after_seconds: Option<u32>,
}


#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnitKind {
    #[default]
    Deployment,
    StatefulSet,
    DaemonSet,
    Job,
}

/// Pod-level options. Most nodes leave this default; power-users
/// override node_selector / tolerations / security context.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PodOptions {
    /// k8s ServiceAccount name. Default `weft-infra-sa` (created
    /// per project namespace by the dispatcher).
    #[serde(default, rename = "serviceAccount", alias = "service_account")]
    pub service_account: Option<String>,

    /// `BTreeMap` (not `HashMap`): the compiled manifest is hashed
    /// for skip-vs-replace and HashMap iteration order would
    /// randomize the hash. Every map reachable from `InfraSpec`
    /// must be `BTreeMap`.
    #[serde(default, rename = "nodeSelector", alias = "node_selector")]
    pub node_selector: Option<std::collections::BTreeMap<String, String>>,

    #[serde(default)]
    pub tolerations: Vec<Toleration>,

    #[serde(default, rename = "priorityClass", alias = "priority_class")]
    pub priority_class: Option<String>,

    #[serde(
        default,
        rename = "securityContext",
        alias = "security_context",
        skip_serializing_if = "Option::is_none"
    )]
    pub security_context: Option<PodSecurityContext>,

    /// Pod-level termination grace period. Should match the longest
    /// container's preStop hook wait.
    #[serde(
        default,
        rename = "terminationGracePeriodSeconds",
        alias = "termination_grace_period_seconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub termination_grace_period_seconds: Option<i64>,
}

/// k8s Toleration. Mirrors the upstream shape.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Toleration {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub operator: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub effect: Option<String>,
    #[serde(
        default,
        rename = "tolerationSeconds",
        alias = "toleration_seconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub toleration_seconds: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PodSecurityContext {
    #[serde(default, rename = "runAsNonRoot", alias = "run_as_non_root")]
    pub run_as_non_root: Option<bool>,
    #[serde(default, rename = "runAsUser", alias = "run_as_user")]
    pub run_as_user: Option<i64>,
    #[serde(default, rename = "runAsGroup", alias = "run_as_group")]
    pub run_as_group: Option<i64>,
    #[serde(default, rename = "fsGroup", alias = "fs_group")]
    pub fs_group: Option<i64>,
    #[serde(default, rename = "seccompProfile", alias = "seccomp_profile")]
    pub seccomp_profile: Option<Value>,
}

// =============================================================
// Containers
// =============================================================

/// No `Default` derive: an empty `Container` has no meaningful
/// image. Authors must construct containers explicitly with a name
/// and an `Image`. Callers that need a partial Container in tests
/// can build one inline with the fields they care about and `..`
/// pattern won't work; that's intentional.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Container {
    pub name: String,
    pub image: Image,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,

    #[serde(default)]
    pub args: Vec<String>,

    #[serde(default)]
    pub env: Vec<EnvEntry>,

    #[serde(default)]
    pub ports: Vec<ContainerPort>,

    #[serde(default)]
    pub resources: Resources,

    #[serde(default)]
    pub mounts: Vec<Mount>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness: Option<Probe>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liveness: Option<Probe>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub startup: Option<Probe>,

    #[serde(
        default,
        rename = "securityContext",
        alias = "security_context",
        skip_serializing_if = "Option::is_none"
    )]
    pub security_context: Option<ContainerSecurityContext>,

    /// k8s preStop lifecycle hook. Runs inside the container before
    /// SIGTERM. Use for graceful shutdown (HTTP /drain, flush logs,
    /// etc). Weft does NOT call any Rust callback at stop time;
    /// graceful shutdown lives entirely in the container.
    #[serde(default, rename = "preStop", alias = "pre_stop", skip_serializing_if = "Option::is_none")]
    pub pre_stop: Option<PreStopHook>,
}

impl Container {
    /// Construct a Container with the two required fields (name +
    /// image) and empty defaults for everything else. `Container`
    /// has no `Default` derive on purpose: the image is mandatory,
    /// so it can't be left unset and slip into a real spec. Set the
    /// optional fields with the chainable `.with_*` builders below, so
    /// a node author writes only the fields they care about and never
    /// needs a struct literal or `..Default::default()`.
    pub fn new(name: impl Into<String>, image: Image) -> Self {
        Self {
            name: name.into(),
            image,
            command: None,
            args: Vec::new(),
            env: Vec::new(),
            ports: Vec::new(),
            resources: Resources::default(),
            mounts: Vec::new(),
            readiness: None,
            liveness: None,
            startup: None,
            security_context: None,
            pre_stop: None,
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

    pub fn with_resources(mut self, resources: Resources) -> Self {
        self.resources = resources;
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

    pub fn with_startup(mut self, probe: Probe) -> Self {
        self.startup = Some(probe);
        self
    }

    pub fn with_security_context(mut self, ctx: ContainerSecurityContext) -> Self {
        self.security_context = Some(ctx);
        self
    }

    pub fn with_pre_stop(mut self, hook: PreStopHook) -> Self {
        self.pre_stop = Some(hook);
        self
    }
}

/// Image source. Local images are built from a directory in the
/// node's catalog package and hash-tagged by the CLI. Upstream images
/// may be digest-pinned or use mutable tags; mutable tags are
/// resolved to digests at apply time before hashing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Image {
    /// Pull from an external registry. e.g. `"postgres:16"`,
    /// `"ghcr.io/huggingface/tgi@sha256:..."`.
    Upstream { reference: String },

    /// Build from a directory listed in `metadata.images`. The CLI
    /// hashes the directory and tags as `weft-infra-{name}:{hash}`.
    /// The dispatcher's `InfraProvisionContext::image_for(name)`
    /// resolves the name to its concrete tag.
    Local { name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EnvEntry {
    Literal { name: String, value: String },
    FromConfigMap {
        name: String,
        #[serde(rename = "configMap", alias = "config_map")]
        config_map: String,
        key: String,
    },
    FromSecret {
        name: String,
        secret: String,
        key: String,
    },
    Downward {
        name: String,
        #[serde(rename = "fieldPath", alias = "field_path")]
        field_path: String,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContainerPort {
    /// Named so endpoints can reference by name.
    pub name: String,
    /// Container port (1..=65535).
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
    #[serde(rename = "SCTP")]
    Sctp,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Resources {
    #[serde(default, rename = "cpuRequest", alias = "cpu_request", skip_serializing_if = "Option::is_none")]
    pub cpu_request: Option<String>,
    #[serde(default, rename = "cpuLimit", alias = "cpu_limit", skip_serializing_if = "Option::is_none")]
    pub cpu_limit: Option<String>,
    #[serde(default, rename = "memoryRequest", alias = "memory_request", skip_serializing_if = "Option::is_none")]
    pub memory_request: Option<String>,
    #[serde(default, rename = "memoryLimit", alias = "memory_limit", skip_serializing_if = "Option::is_none")]
    pub memory_limit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu: Option<u32>,
    /// Other resource keys (ephemeral-storage, custom resource names).
    /// Free-form for power users. `BTreeMap` for hash determinism;
    /// see `PodOptions.node_selector`.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub extra: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Mount {
    /// Volume name (matches `Volume.name`).
    pub volume: String,
    pub path: String,
    #[serde(default, rename = "subPath", alias = "sub_path", skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
    #[serde(default, rename = "readOnly", alias = "read_only")]
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Probe {
    pub kind: ProbeKind,
    #[serde(default, rename = "initialDelaySeconds", alias = "initial_delay_seconds")]
    pub initial_delay_seconds: i32,
    #[serde(default = "default_period_seconds", rename = "periodSeconds", alias = "period_seconds")]
    pub period_seconds: i32,
    #[serde(default = "default_timeout_seconds", rename = "timeoutSeconds", alias = "timeout_seconds")]
    pub timeout_seconds: i32,
    #[serde(default = "default_success_threshold", rename = "successThreshold", alias = "success_threshold")]
    pub success_threshold: i32,
    #[serde(default = "default_failure_threshold", rename = "failureThreshold", alias = "failure_threshold")]
    pub failure_threshold: i32,
}

fn default_period_seconds() -> i32 { 10 }
fn default_timeout_seconds() -> i32 { 1 }
fn default_success_threshold() -> i32 { 1 }
fn default_failure_threshold() -> i32 { 3 }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProbeKind {
    Http {
        path: String,
        port: u16,
        #[serde(default, skip_serializing_if = "Vec::is_empty", rename = "httpHeaders", alias = "http_headers")]
        http_headers: Vec<HttpHeader>,
    },
    Tcp { port: u16 },
    Exec { command: Vec<String> },
}

impl Probe {
    pub fn http(path: impl Into<String>, port: u16) -> Self {
        Self {
            kind: ProbeKind::Http {
                path: path.into(),
                port,
                http_headers: Vec::new(),
            },
            initial_delay_seconds: 0,
            period_seconds: default_period_seconds(),
            timeout_seconds: default_timeout_seconds(),
            success_threshold: default_success_threshold(),
            failure_threshold: default_failure_threshold(),
        }
    }

    pub fn with_initial_delay(mut self, seconds: i32) -> Self {
        self.initial_delay_seconds = seconds;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpHeader {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContainerSecurityContext {
    #[serde(default, rename = "allowPrivilegeEscalation", alias = "allow_privilege_escalation")]
    pub allow_privilege_escalation: Option<bool>,
    #[serde(default, rename = "readOnlyRootFilesystem", alias = "read_only_root_filesystem")]
    pub read_only_root_filesystem: Option<bool>,
    #[serde(default)]
    pub capabilities: Option<Value>,
    #[serde(default, rename = "runAsNonRoot", alias = "run_as_non_root")]
    pub run_as_non_root: Option<bool>,
    #[serde(default, rename = "runAsUser", alias = "run_as_user")]
    pub run_as_user: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PreStopHook {
    /// Run an HTTP request against the container.
    Http { path: String, port: u16 },
    /// Run a command inside the container.
    Exec { command: Vec<String> },
}

// =============================================================
// Volumes
// =============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Volume {
    pub name: String,
    #[serde(flatten)]
    pub kind: VolumeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VolumeKind {
    /// PVC. Preserved across stop (scale to 0) and upgrade. Deleted
    /// on terminate unless listed in `Lifecycle.on_terminate.preserve_pvcs`.
    Persistent {
        /// k8s quantity string (e.g. "10Gi").
        size: String,
        #[serde(default, rename = "storageClass", alias = "storage_class", skip_serializing_if = "Option::is_none")]
        storage_class: Option<String>,
        #[serde(default = "default_access_modes", rename = "accessModes", alias = "access_modes")]
        access_modes: Vec<AccessMode>,
    },
    /// EmptyDir on the pod.
    EmptyDir {
        #[serde(default, rename = "sizeLimit", alias = "size_limit", skip_serializing_if = "Option::is_none")]
        size_limit: Option<String>,
    },
    /// Mount an existing ConfigMap by name. Keyed by ConfigMap key →
    /// mount path. `BTreeMap` (not `HashMap`) is load-bearing: the
    /// compiled manifest is hashed for skip-vs-replace decisions,
    /// and HashMap iteration order randomizes per process → every
    /// apply would produce a different hash and silently disable
    /// the skip-fast path.
    ConfigMap {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        items: Option<std::collections::BTreeMap<String, String>>,
    },
    /// Mount an existing Secret by name. Same BTreeMap rationale as
    /// `ConfigMap` above.
    Secret {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        items: Option<std::collections::BTreeMap<String, String>>,
    },
}

fn default_access_modes() -> Vec<AccessMode> {
    vec![AccessMode::ReadWriteOnce]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessMode {
    ReadWriteOnce,
    ReadWriteMany,
    ReadOnlyMany,
    ReadWriteOncePod,
}

// =============================================================
// Config sources
// =============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConfigSource {
    /// Create a new Secret in the project namespace with literal data.
    /// Data is rendered into the apply stream; treat with care.
    /// `BTreeMap` for hash determinism; see `PodOptions.node_selector`.
    SecretLiteral {
        name: String,
        data: std::collections::BTreeMap<String, String>,
    },
    /// Reference a pre-existing Secret in the project namespace.
    /// Created externally (kubectl, another infra node).
    SecretRef { name: String },
    /// Create a new ConfigMap in the project namespace.
    /// `BTreeMap` for hash determinism; see `PodOptions.node_selector`.
    ConfigMapLiteral {
        name: String,
        data: std::collections::BTreeMap<String, String>,
    },
    /// Reference a pre-existing ConfigMap in the project namespace.
    ConfigMapRef { name: String },
}

// =============================================================
// Endpoints
// =============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    /// Local name. The compiler builds a Service named
    /// `<instance>-<name>` selecting the targeted Unit's Pods.
    /// `endpoint_url(name)` resolves to this Service.
    pub name: String,
    /// Which Unit (by `Unit.name`).
    pub unit: String,
    /// Which Container in the Unit (by `Container.name`).
    pub container: String,
    /// Which named port on the container (by `ContainerPort.name`).
    pub port: String,
    #[serde(default)]
    pub expose: Expose,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Expose {
    /// ClusterIP only. Reachable from same-namespace pods (workers,
    /// other infra nodes that have egress allowed).
    #[default]
    ClusterInternal,
    /// ClusterIP + Ingress at `<tenant-host>/<path>`. IP-level
    /// restriction comes from `Access.ingress` (e.g. FromCidrs).
    TenantPublic { path: String },
    /// NodePort. Rare; useful for testing.
    NodePort { port: u16 },
}

// =============================================================
// Access (NetworkPolicy)
// =============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Access {
    #[serde(default = "default_ingress")]
    pub ingress: Vec<IngressRule>,
    #[serde(default = "default_egress")]
    pub egress: Vec<EgressRule>,
}

impl Default for Access {
    fn default() -> Self {
        Self {
            ingress: default_ingress(),
            egress: default_egress(),
        }
    }
}

fn default_ingress() -> Vec<IngressRule> {
    vec![IngressRule::FromWorkers]
}

fn default_egress() -> Vec<EgressRule> {
    vec![EgressRule::ToInternet]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IngressRule {
    /// Workers in this project.
    FromWorkers,
    /// Another infra node in this project (compiled to a pod
    /// selector matching `weft.dev/node=<node_id>`).
    FromNode { node_id: String },
    /// 0.0.0.0/0. Typically paired with `Expose::TenantPublic`.
    FromInternet,
    /// Specific source CIDR list.
    FromCidrs(Vec<String>),
    /// Same-namespace pods with a specific label.
    FromLabel { key: String, value: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EgressRule {
    ToInternet,
    /// Another infra node in this project (matches `weft.dev/node`).
    ToNode { node_id: String },
    /// Specific destination CIDR list.
    ToCidrs(Vec<String>),
}

// =============================================================
// Lifecycle
// =============================================================

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Lifecycle {
    /// Terminate policy. PVC-preserve list is per-node; individual
    /// PVCs are named, not per-unit. (Stop behavior is per-unit and
    /// lives on `Unit.on_stop`, not here.)
    #[serde(default, rename = "onTerminate", alias = "on_terminate")]
    pub on_terminate: TerminateBehavior,
}

/// Per-unit stop behavior. Stop is an operational action on a workload,
/// so it belongs on the `Unit` (like `scaling` and `on_upgrade`), not
/// spec-wide: one node may want a stateless cache unit scaled to zero
/// while a license-server unit stays up until terminate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StopBehavior {
    /// Default. Set the workload's replicas to 0. PVCs preserved,
    /// Service kept.
    #[default]
    ScaleToZero,
    /// Leave the unit running on stop. Only terminate removes it. For
    /// a unit that must persist across a project stop (a license
    /// server, a long-warmup model the user doesn't want to re-pull).
    NoOp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UpgradeBehavior {
    /// kubectl apply lets k8s rolling-update the Deployment.
    Rolling {
        #[serde(default, rename = "maxUnavailable", alias = "max_unavailable", skip_serializing_if = "Option::is_none")]
        max_unavailable: Option<String>,
        #[serde(default, rename = "maxSurge", alias = "max_surge", skip_serializing_if = "Option::is_none")]
        max_surge: Option<String>,
        #[serde(
            default = "default_progress_deadline_seconds",
            rename = "progressDeadlineSeconds",
            alias = "progress_deadline_seconds"
        )]
        progress_deadline_seconds: u32,
    },
    /// Delete then re-apply. Needed when two versions can't coexist
    /// (port conflicts, exclusive file locks).
    Recreate,
}

fn default_progress_deadline_seconds() -> u32 { 600 }

impl Default for UpgradeBehavior {
    fn default() -> Self {
        Self::Rolling {
            max_unavailable: None,
            max_surge: None,
            progress_deadline_seconds: default_progress_deadline_seconds(),
        }
    }
}

impl UpgradeBehavior {
    /// True iff this behavior is the implicit default (Rolling
    /// with no explicit fields set). Used by the compiler to
    /// distinguish "user expressed intent" from "user didn't say
    /// anything"; non-Deployment kinds reject the former.
    pub fn is_default(&self) -> bool {
        match self {
            Self::Rolling {
                max_unavailable,
                max_surge,
                progress_deadline_seconds,
            } => {
                max_unavailable.is_none()
                    && max_surge.is_none()
                    && *progress_deadline_seconds == default_progress_deadline_seconds()
            }
            Self::Recreate => false,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TerminateBehavior {
    /// PVC names to preserve even on terminate. Usually empty.
    #[serde(default, rename = "preservePvcs", alias = "preserve_pvcs")]
    pub preserve_pvcs: Vec<String>,
}

// =============================================================
// Scaling
// =============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingPolicy {
    /// Static replica count. Per Unit (each Unit has its own).
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    /// Optional autoscale. Compiles to HorizontalPodAutoscaler.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub autoscale: Option<AutoscaleSpec>,
}

impl Default for ScalingPolicy {
    fn default() -> Self {
        Self {
            replicas: default_replicas(),
            autoscale: None,
        }
    }
}

fn default_replicas() -> u32 { 1 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoscaleSpec {
    #[serde(rename = "minReplicas", alias = "min_replicas")]
    pub min_replicas: u32,
    #[serde(rename = "maxReplicas", alias = "max_replicas")]
    pub max_replicas: u32,
    #[serde(default)]
    pub metrics: Vec<AutoscaleMetric>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior: Option<AutoscaleBehavior>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AutoscaleMetric {
    CpuUtilization { target_percent: u32 },
    MemoryUtilization { target_percent: u32 },
    /// Requires metrics-server / KEDA / Prometheus-adapter installed
    /// in the cluster. Weft doesn't ship those; user opts in.
    Custom { name: String, target: String },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AutoscaleBehavior {
    #[serde(default = "default_stabilization_seconds", rename = "scaleUpStabilizationSeconds", alias = "scale_up_stabilization_seconds")]
    pub scale_up_stabilization_seconds: u32,
    #[serde(default = "default_stabilization_seconds", rename = "scaleDownStabilizationSeconds", alias = "scale_down_stabilization_seconds")]
    pub scale_down_stabilization_seconds: u32,
}

fn default_stabilization_seconds() -> u32 { 60 }

// =============================================================
// Health
// =============================================================

// =============================================================
// Provision context
// =============================================================

/// Context handed to `Node::provision`. Carries runtime identity
/// (project, node, tenant, namespace) plus arbitrary metadata that
/// the dispatcher may inject.
///
/// `Image::Local { name }` references are resolved to concrete docker
/// tags by the dispatcher's apply executor at apply time using the
/// CLI-computed image hash map, NOT by the provision body. The body
/// just declares the name; the dispatcher does the substitution. This
/// keeps `Node::provision` deterministic given inputs (image tags can
/// change without changing the spec the body returns).
///
/// Distinct from `ExecutionContext`: provision runs BEFORE apply, so
/// it has no endpoint URLs yet. `ExecutionContext` is what gets
/// passed to `execute` post-apply.
#[derive(Debug, Clone)]
pub struct InfraProvisionContext {
    pub project_id: String,
    pub node_id: String,
    /// The project namespace (`wm-project-{tenant}-{project}`).
    pub namespace: String,
    pub tenant_id: String,
}

impl InfraProvisionContext {
    pub fn new(
        project_id: String,
        node_id: String,
        namespace: String,
        tenant_id: String,
    ) -> Self {
        Self {
            project_id,
            node_id,
            namespace,
            tenant_id,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProvisionContextError {
    #[error(
        "no image declared with name '{0}'; add it to NodeMetadata.images and provide a Dockerfile at images/{0}/Dockerfile"
    )]
    UnknownImage(String),
}

// =============================================================
// Tests
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_spec_serializes_compactly() {
        let spec = InfraSpec::default();
        let s = serde_json::to_string(&spec).unwrap();
        assert!(s.contains("\"units\":[]"));
    }

    #[test]
    fn infra_spec_round_trip() {
        let original = InfraSpec {
            units: vec![Unit {
                name: "bridge".into(),
                kind: UnitKind::Deployment,
                containers: vec![Container {
                    args: vec!["--port".into(), "8090".into()],
                    env: vec![EnvEntry::Literal {
                        name: "MODE".into(),
                        value: "prod".into(),
                    }],
                    ports: vec![ContainerPort {
                        name: "http".into(),
                        port: 8090,
                        protocol: Protocol::Tcp,
                    }],
                    resources: Resources {
                        cpu_request: Some("100m".into()),
                        memory_request: Some("128Mi".into()),
                        ..Default::default()
                    },
                    readiness: Some(Probe::http("/health", 8090)),
                    ..Container::new("main", Image::Local { name: "bridge".into() })
                }],
                scaling: ScalingPolicy {
                    replicas: 2,
                    autoscale: None,
                },
                on_upgrade: UpgradeBehavior::Recreate,
                ..Default::default()
            }],
            volumes: vec![Volume {
                name: "auth".into(),
                kind: VolumeKind::Persistent {
                    size: "100Mi".into(),
                    storage_class: None,
                    access_modes: vec![AccessMode::ReadWriteOnce],
                },
            }],
            config: vec![ConfigSource::SecretRef {
                name: "creds".into(),
            }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                unit: "bridge".into(),
                container: "main".into(),
                port: "http".into(),
                expose: Expose::ClusterInternal,
            }],
            access: Access {
                ingress: vec![IngressRule::FromWorkers],
                egress: vec![EgressRule::ToInternet],
            },
            lifecycle: Lifecycle::default(),
        };

        let json_value = serde_json::to_value(&original).expect("serialize");
        let restored: InfraSpec =
            serde_json::from_value(json_value.clone()).expect("deserialize");

        // Round-trip through JSON to verify everything survives.
        let round_trip_json = serde_json::to_value(&restored).expect("re-serialize");
        assert_eq!(json_value, round_trip_json, "round-trip mismatch");
    }

    #[test]
    fn default_access_is_workers_in_internet_out() {
        let access = Access::default();
        assert!(matches!(access.ingress.first(), Some(IngressRule::FromWorkers)));
        assert!(matches!(access.egress.first(), Some(EgressRule::ToInternet)));
    }

    #[test]
    fn provision_context_construction() {
        let ctx = InfraProvisionContext::new(
            "proj".into(),
            "node".into(),
            "wm-project-x-y".into(),
            "x".into(),
        );
        assert_eq!(ctx.project_id, "proj");
        assert_eq!(ctx.node_id, "node");
        assert_eq!(ctx.namespace, "wm-project-x-y");
        assert_eq!(ctx.tenant_id, "x");
    }

    #[test]
    fn upgrade_behavior_default_is_rolling() {
        let u = Unit::default();
        assert!(matches!(u.on_upgrade, UpgradeBehavior::Rolling { .. }));
    }

    #[test]
    fn scaling_policy_default_replicas_is_one() {
        assert_eq!(ScalingPolicy::default().replicas, 1);
    }

    #[test]
    fn protocol_serializes_uppercase() {
        let p = Protocol::Tcp;
        assert_eq!(serde_json::to_string(&p).unwrap(), "\"TCP\"");
    }

    #[test]
    fn image_local_round_trip() {
        let img = Image::Local { name: "bridge".into() };
        let s = serde_json::to_string(&img).unwrap();
        let restored: Image = serde_json::from_str(&s).unwrap();
        match restored {
            Image::Local { name } => assert_eq!(name, "bridge"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn volume_persistent_round_trip() {
        let v = Volume {
            name: "data".into(),
            kind: VolumeKind::Persistent {
                size: "1Gi".into(),
                storage_class: None,
                access_modes: vec![AccessMode::ReadWriteOnce],
            },
        };
        let json = serde_json::to_value(&v).unwrap();
        // VolumeKind uses internal `kind` tag via #[serde(flatten)]
        // on Volume, so the resulting JSON has both `name` and the
        // tagged variant flat.
        assert_eq!(json["name"], "data");
        assert_eq!(json["kind"], "persistent");
        assert_eq!(json["size"], "1Gi");
    }
}
