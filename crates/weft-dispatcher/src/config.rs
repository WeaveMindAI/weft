use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatcherConfig {
    /// TCP port to bind the HTTP server. Default 9999 for local dev.
    pub http_port: u16,

    /// Which worker backend to use. Values: "subprocess" (local
    /// default), "kubernetes" (cloud / BYOC). The closed-source
    /// weavemind repo adds cloud-specific backends on top.
    pub worker_backend: String,

    /// Which infra backend to use. Values: "kind" (local),
    /// "kubernetes" (cloud / BYOC).
    pub infra_backend: String,

    /// URL listeners and workers use to call back into the
    /// dispatcher. Set to the cluster-internal Service DNS
    /// (`http://weft-dispatcher.weft-system.svc.cluster.local:9999`)
    /// when the dispatcher runs in-cluster, or to `http://127.0.0.1:PORT`
    /// for subprocess-mode local dev.
    pub dispatcher_callback_url: String,

    /// Template for Pod-to-Pod internal URLs. `{pod}` is replaced
    /// with the target Pod's id (StatefulSet pod name). Default:
    /// `http://{pod}.weft-dispatcher-headless.weft-system.svc.cluster.local:9999`.
    pub internal_url_template: String,

    /// Shared secret for `/internal/*` routes. Loaded from env
    /// `WEFT_INTERNAL_SECRET` or generated at first run.
    pub internal_secret: String,
}

impl DispatcherConfig {
    /// URL listeners and workers should use to reach the dispatcher
    /// from inside their namespace. Mirrors `dispatcher_callback_url`
    /// for now; kept as a method so callers don't have to know
    /// about cluster vs subprocess routing.
    pub fn cluster_dispatcher_url(&self) -> String {
        self.dispatcher_callback_url.clone()
    }
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            http_port: 9999,
            worker_backend: "subprocess".into(),
            infra_backend: "kind".into(),
            dispatcher_callback_url: "http://127.0.0.1:9999".into(),
            internal_url_template:
                "http://{pod}.weft-dispatcher-headless.weft-system.svc.cluster.local:9999".into(),
            internal_secret: "local-dev-internal-secret".into(),
        }
    }
}
