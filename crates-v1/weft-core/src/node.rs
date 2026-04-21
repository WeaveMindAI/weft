use serde::{Deserialize, Serialize};

/// How other nodes communicate with this infrastructure pod
#[derive(Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
pub struct ActionEndpoint {
    /// Port the sidecar/service listens on for action requests
    pub port: u16,
    /// HTTP path for action dispatch (e.g., "/action")
    pub path: String,
}

/// A Kubernetes resource manifest template.
/// The platform will inject namespace, instance ID, labels, etc.
#[derive(Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
pub struct KubeManifest {
    /// Raw K8s manifest as JSON (Deployment, Service, PVC, StatefulSet, etc.)
    pub manifest: serde_json::Value,
}

/// Infrastructure specification defined by the node itself.
/// This tells the platform exactly how to deploy and communicate with this node.
/// Node authors provide this,the platform is infrastructure-agnostic.
#[derive(Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
pub struct InfrastructureSpec {
    /// Name of the sidecar (e.g., "postgres-database"). Image will be resolved to {REGISTRY}/sidecar-{name}:latest
    pub sidecarName: String,
    /// Kubernetes manifests to apply (Deployment/StatefulSet, Service, PVC, etc.)
    pub manifests: Vec<KubeManifest>,
    /// How other nodes send actions to this infrastructure
    pub actionEndpoint: ActionEndpoint,
}

/// Category of trigger - determines how the trigger operates
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
pub enum TriggerCategory {
    /// Receives incoming HTTP requests (e.g., GitHub webhook)
    Webhook,
    /// Maintains persistent connection (e.g., Discord gateway)
    Socket,
    /// Periodically checks for changes (e.g., RSS feed)
    Polling,
    /// Fires on a schedule (e.g., cron)
    Schedule,
    /// Watches local resources (e.g., file watcher)
    Local,
    /// Manually triggered
    #[default]
    Manual,
}

impl std::fmt::Display for TriggerCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TriggerCategory::Webhook => write!(f, "Webhook"),
            TriggerCategory::Socket => write!(f, "Socket"),
            TriggerCategory::Polling => write!(f, "Polling"),
            TriggerCategory::Schedule => write!(f, "Schedule"),
            TriggerCategory::Local => write!(f, "Local"),
            TriggerCategory::Manual => write!(f, "Manual"),
        }
    }
}

/// Feature flags for nodes - describes what capabilities a node has
#[derive(Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
pub struct NodeFeatures {
    /// Node can act as a project trigger (has start/stop lifecycle)
    #[serde(default)]
    pub isTrigger: bool,
    /// Trigger category (only relevant if isTrigger is true)
    #[serde(default)]
    pub triggerCategory: Option<TriggerCategory>,
    /// Trigger requires a running instance (false for Webhook/Manual which are event-driven)
    #[serde(default = "default_requires_running_instance")]
    pub requiresRunningInstance: bool,
    /// Node allows adding custom input ports
    #[serde(default)]
    pub canAddInputPorts: bool,
    /// Node allows adding custom output ports
    #[serde(default)]
    pub canAddOutputPorts: bool,
    /// Node is an infrastructure node (long-running, provides actions to other nodes)
    #[serde(default)]
    pub isInfrastructure: bool,
    /// Infrastructure deployment specification (only for infrastructure nodes).
    /// Defines the K8s manifests, readiness check, and action endpoint.
    /// The node itself controls how it gets deployed.
    #[serde(default)]
    pub infrastructureSpec: Option<InfrastructureSpec>,
    /// Sidecar exposes a /live endpoint with typed data items for real-time dashboard display.
    #[serde(default)]
    pub hasLiveData: bool,
    /// Node has a dynamic form schema. Ports are derived from config.fields at build time.
    /// enrich.rs preserves these ports even though canAddInputPorts/canAddOutputPorts are false.
    #[serde(default)]
    pub hasFormSchema: bool,
    /// Groups of ports where at least one must be non-null for the node to execute.
    /// If all ports in a group are null/missing, the node is skipped.
    /// e.g. [["text", "media"]] = at least one of text/media must be non-null.
    #[serde(default)]
    pub oneOfRequired: Vec<Vec<String>>,
}

fn default_requires_running_instance() -> bool {
    true
}

impl Default for NodeFeatures {
    fn default() -> Self {
        Self {
            isTrigger: false,
            triggerCategory: None,
            requiresRunningInstance: true,
            canAddInputPorts: false,
            canAddOutputPorts: false,
            isInfrastructure: false,
            infrastructureSpec: None,
            hasLiveData: false,
            hasFormSchema: false,
            oneOfRequired: Vec::new(),
        }
    }
}

