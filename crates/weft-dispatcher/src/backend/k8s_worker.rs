//! K8s worker backend. Spawns one Pod per project pool. Each Pod
//! multiplexes N concurrent executions for its project (running the
//! content-addressed `weft-worker:<binary_hash>` image) and idle-shuts
//! itself down. The dispatcher's cold-start trigger
//! ensures a Pod exists whenever there's pending worker-target work
//! for a project.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use weft_platform_traits::{Clock, DeleteOpts, KubeClient};

use crate::backend::{SpawnPodSpec, WorkerBackend, WorkerHandle};
use crate::project_namespace::SafeLabel;

/// How long to watch a freshly-applied worker pod for an image-pull
/// failure before assuming it's pulling/starting normally.
const PULL_WATCH_SECS: u64 = 5;
const PULL_POLL: Duration = Duration::from_millis(500);

/// TCP port the worker's live caller connection server listens on. SYNC:
/// must match the generated worker's `WEFT_CONNECTION_PORT` default and
/// the gateway's upstream port. One definition; the manifest sets the env
/// and the container port, the gateway forwards here.
// SYNC: WORKER_CONNECTION_PORT <-> crates/weft-compiler/src/codegen.rs (WEFT_CONNECTION_PORT default), deploy/k8s/gateway.yaml (Backend/route upstream port)
pub const WORKER_CONNECTION_PORT: u16 = 9091;

/// Namespace the Envoy Gateway controller + its data-plane proxies run
/// in (Envoy Gateway's default install namespace). The only source the
/// worker NetworkPolicy admits on the connection port. SYNC with the
/// gateway install manifests.
// SYNC: GATEWAY_NAMESPACE <-> deploy/k8s/gateway.yaml (gatewayClass/Gateway namespace), crates/weft-cli/src/commands/daemon.rs (gateway install)
pub const GATEWAY_NAMESPACE: &str = "envoy-gateway-system";

/// Name of the per-PROJECT headless Service that gives each worker pod a
/// stable DNS name (`<pod-name>.<svc>.<ns>.svc.cluster.local`). ONE
/// Service per project (not per pod): headless Services carry zero
/// kube-proxy / iptables load and the object count is O(projects), so it
/// scales to thousands of pods. The gateway dynamic-resolves the per-pod
/// DNS; the Service exists only to publish the pod A-records.
pub fn worker_headless_service_name() -> &'static str {
    "weft-workers"
}

pub struct K8sWorkerBackend {
    /// Broker URL injected into worker Pods. Workers never speak
    /// directly to Postgres in arch-5; everything goes through the
    /// broker, which validates the worker's projected SA token and
    /// scopes every operation per-tenant.
    broker_url: String,
    /// Shared kube client: same trait the listener backend + reaper
    /// use, so the worker spawn/kill path is fakeable at layer 3 and
    /// there's one apply implementation across the dispatcher.
    kube: Arc<dyn KubeClient>,
    /// Injected so `wait_for_pull_ok`'s poll loop is deterministic
    /// in tests (FakeClock fast-forwards the 5s watch window to
    /// microseconds). Production passes `SystemClock`.
    clock: Arc<dyn Clock>,
    /// Decides whether a worker pod runs under a sandbox runtime
    /// (`runtimeClassName`). The default returns none (host runtime); a sandbox
    /// class applies to shared-pool workers. The worker backend owns this because
    /// it is the one that renders the pod manifest.
    sandbox: Arc<dyn crate::placement::SandboxPolicy>,
    /// Worker-image registry config, present iff worker images are pulled from a
    /// registry they were pushed to. When set, the worker image is the
    /// registry-qualified CONTENT-addressed ref (`<registry>/weft-worker:<hash>`,
    /// the SAME ref the build pushed) and the pod references the registry pull
    /// secret. When `None`, the image is the bare content-addressed tag the CLI
    /// built + `kind load`ed (`weft-worker:<hash>`), pulled `IfNotPresent` from the
    /// node, no registry, no pull secret. Both forms are content-addressed and
    /// project-independent,
    /// so a build is shared across every project with the same binary hash.
    registry: Option<crate::registry::RegistryConfig>,
}

impl K8sWorkerBackend {
    pub fn new(
        broker_url: String,
        kube: Arc<dyn KubeClient>,
        clock: Arc<dyn Clock>,
        sandbox: Arc<dyn crate::placement::SandboxPolicy>,
        registry: Option<crate::registry::RegistryConfig>,
    ) -> Self {
        Self { broker_url, kube, clock, sandbox, registry }
    }

    /// The image ref to spawn at `binary_hash`. Registry-qualified
    /// content-addressed ref when a registry is configured (matches what the build
    /// pushed); the bare content-addressed tag otherwise (matches what the CLI
    /// built + loaded onto the node). Both are project-INDEPENDENT and content-addressed
    /// (`weft-worker:<hash>`), so byte-identical builds across projects resolve to
    /// ONE image. The single source of truth for "which image does the worker
    /// pull", mirroring the build's mint so push and pull never disagree.
    fn worker_image_ref(&self, binary_hash: &str) -> String {
        match &self.registry {
            Some(reg) => reg.worker_image_ref(binary_hash),
            None => crate::registry::bare_worker_image_ref(binary_hash),
        }
    }

    /// Poll the pod's first-container waiting reason for ~5s; bail on
    /// `ImagePullBackOff` / `ErrImagePull` so a missing image surfaces
    /// immediately instead of after the readiness timeout. Returns Ok
    /// once the window passes without a pull failure (the pod is
    /// pulling / starting normally).
    async fn wait_for_pull_ok(&self, pod_name: &str, namespace: &str) -> anyhow::Result<()> {
        let deadline = self.clock.now() + Duration::from_secs(PULL_WATCH_SECS);
        while self.clock.now() < deadline {
            self.clock.sleep(PULL_POLL).await;
            if let Some(reason) = self.kube.pod_waiting_reason(namespace, pod_name).await? {
                if matches!(reason.as_str(), "ImagePullBackOff" | "ErrImagePull") {
                    anyhow::bail!(
                        "ImagePullBackOff for pod {pod_name}: image weft-worker-* not present in cluster"
                    );
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl WorkerBackend for K8sWorkerBackend {
    async fn spawn_pod(
        &self,
        pod_name: &str,
        spec: SpawnPodSpec,
    ) -> anyhow::Result<WorkerHandle> {
        // Hash-tagged tags are the only path. If the CLI never set
        // a hash (e.g. someone POSTed /run before /projects), fail
        // loudly instead of falling back to `:latest`.
        let hash = spec.binary_hash.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "spawn_pod for project {}: no running_binary_hash set; \
                 register the project via the CLI (which builds + sets the hash) before \
                 calling /run, /activate, or /infra/start",
                spec.project_id,
            )
        })?;
        let image = self.worker_image_ref(hash);
        let pull_secret = self.registry.as_ref().and_then(|r| r.pull_secret.as_deref());

        let project_label = SafeLabel::new(&spec.project_id, 63);
        // Apply the headless Service FIRST (idempotent), so a pod's DNS
        // A-record is publishable the moment it is Ready. One Service per
        // NAMESPACE (selecting all workers by role), so in the shared
        // namespace every project's spawn re-applies the same Service:
        // a no-op, never a project-scoped overwrite.
        let svc = render_headless_service(&spec.namespace);
        self.kube.apply_yaml(&svc).await?;

        // Sandbox decision: the policy returns a sandbox runtime class for the
        // namespaces it chooses to isolate, or none (host runtime). The namespace
        // is the policy's discriminator.
        let runtime_class = self.sandbox.runtime_class(&spec.namespace);
        let manifest = render_pod_manifest(
            pod_name,
            &spec.namespace,
            &image,
            &project_label,
            &SafeLabel::new(&spec.tenant, 63),
            &self.broker_url,
            &spec.owner_dispatcher,
            &spec.caller_token_secret_hex,
            runtime_class.as_deref(),
            pull_secret,
        );
        self.kube.apply_yaml(&manifest).await?;
        self.wait_for_pull_ok(pod_name, &spec.namespace).await?;
        Ok(WorkerHandle {
            pod_name: pod_name.to_string(),
        })
    }

    async fn kill_pod(&self, pod_name: String, namespace: String) -> anyhow::Result<()> {
        // Fire-and-forget: the reaper's sweep loop must not block
        // on a slow pod delete.
        self.kube
            .delete_named(&namespace, "pod", &pod_name, DeleteOpts::no_wait())
            .await
    }
}

pub(crate) fn short_project_id(project_id: &str) -> String {
    project_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(8)
        .collect()
}

/// Render the per-project headless Service. `clusterIP: None` = no
/// virtual IP, no kube-proxy involvement; the endpoints controller
/// publishes one DNS A-record per Ready worker pod as
/// `<pod-name>.weft-workers.<ns>.svc.cluster.local`. `publishNotReadyAddresses`
/// is true so a pod's record exists as soon as the pod has an IP (the
/// caller may be routed the instant the worker is up, before k8s readiness
/// probes would mark it Ready). The port is the connection server's port.
fn render_headless_service(namespace: &str) -> String {
    let svc = worker_headless_service_name();
    // ONE headless Service per namespace, selecting EVERY worker by role
    // (NOT by project). The Service exists only to publish per-pod
    // A-records (`<pod>.{svc}.<ns>.svc.cluster.local`) via the pod's
    // `hostname`+`subdomain`; those records are keyed by pod name, so the
    // selector just needs to make each worker an endpoint. A
    // project-scoped selector would be actively wrong in the SHARED
    // namespace, where many projects' workers coexist under this single
    // (name-constant) Service: each project's apply would overwrite the
    // selector to its own project and de-list every other project's pods
    // from DNS. Role-only selection is correct in both the shared
    // namespace and a per-project namespace (which holds one project).
    format!(
        r#"apiVersion: v1
kind: Service
metadata:
  name: {svc}
  namespace: {namespace}
  labels:
    weft.dev/role: worker-headless
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    weft.dev/role: worker
  ports:
    - name: caller
      port: {port}
      targetPort: {port}
"#,
        port = WORKER_CONNECTION_PORT,
    )
}

#[allow(clippy::too_many_arguments)]
fn render_pod_manifest(
    pod_name: &str,
    namespace: &str,
    image: &str,
    // SafeLabel (not &str): the type forces the caller to sanitize
    // these ids before they reach the YAML. A free-form `tenant_id`
    // with a newline / `"` therefore can't break the manifest or
    // smuggle a label. Single-tenant ids (UUID project_id, `local`
    // tenant) sanitize to themselves, so it's a no-op for them.
    project_label: &crate::project_namespace::SafeLabel,
    tenant: &crate::project_namespace::SafeLabel,
    broker_url: &str,
    owner_dispatcher: &str,
    caller_token_secret_hex: &str,
    // The sandbox runtime class for this worker, or `None` for the host runtime.
    // Resolved by the `SandboxPolicy`: the default is always `None`; a sandbox
    // class applies to shared-pool workers.
    runtime_class: Option<&str>,
    // The k8s imagePullSecret to pull the worker image, or `None` when the cluster
    // authenticates pulls implicitly (a node service account) or the image is
    // loaded onto the node directly (no registry).
    pull_secret: Option<&str>,
) -> String {
    // Minimal pod: SA token mount (auth) + weft labels (routing /
    // cleanup). No security context, no resource limits. Cross-tenant
    // isolation comes from the surrounding NetworkPolicies (a per-
    // project namespace's default-deny + worker-policy, or the shared
    // namespace's blanket pod-to-pod deny), not from per-pod hardening.
    // Per-pod sandboxing of the worker process, when a deployment wants it,
    // arrives as the `runtimeClassName` line below; the shared namespace is the
    // natural population to sandbox first.
    let headless_svc = worker_headless_service_name();
    let conn_port = WORKER_CONNECTION_PORT;
    // The sandbox runtime line, emitted only when the policy selected one. A
    // trailing newline keeps the rest of the spec aligned whether or not it is
    // present, and an absent class leaves the pod on the host runtime.
    let runtime_class_line = match runtime_class {
        Some(rc) => format!("  runtimeClassName: {rc}\n"),
        None => String::new(),
    };
    // The image-pull secret line, emitted only when a registry credential is
    // configured. Absent when the image is loaded onto the node directly and for
    // clusters that authenticate pulls implicitly (a node service account).
    // Trailing newline keeps the spec aligned whether or not it is present.
    let image_pull_secrets_line = match pull_secret {
        Some(secret) => format!("  imagePullSecrets:\n    - name: {secret}\n"),
        None => String::new(),
    };
    format!(
        r#"apiVersion: v1
kind: Pod
metadata:
  name: {pod_name}
  namespace: {namespace}
  labels:
    weft.dev/role: worker
    weft.dev/tenant: "{tenant}"
    weft.dev/project: "{project_label}"
spec:
{runtime_class_line}{image_pull_secrets_line}  # `hostname` + `subdomain` + the headless Service in this pod's
  # namespace give it the stable DNS name
  # `<pod-name>.{headless_svc}.<ns>.svc.cluster.local`, the address the
  # gateway dynamic-resolves a live caller to (per-pod pinning without a
  # Service per pod). The Service is applied into whichever namespace the
  # worker lands in (per-project or shared). BOTH `hostname` and
  # `subdomain` are required: k8s only publishes the per-pod A record
  # when `hostname` is set (with just `subdomain`, only the Service-level
  # record resolves, and the per-pod name 404s/DNS-fails at the gateway).
  # The pod name is a valid DNS label.
  hostname: {pod_name}
  subdomain: {headless_svc}
  serviceAccountName: weft-worker-sa
  automountServiceAccountToken: false
  # `Never`: a crashed container does NOT restart in-place. The
  # broker's `register_alive` requires `status='spawning'` (load-
  # bearing for generation fencing); an in-place restart would
  # call register_alive against an already-alive row and bail.
  # Recovery model: pod dies -> dispatcher reaper marks_dead +
  # kubectl delete + reclaims tasks -> cold_start spawns a fresh
  # pod (new pod_name) -> new pod registers cleanly -> picker
  # claims the orphaned tasks -> journal-replay resumes work.
  restartPolicy: Never
  containers:
    - name: worker
      image: {image}
      imagePullPolicy: IfNotPresent
      ports:
        - name: caller
          containerPort: {conn_port}
      env:
        - name: WEFT_PROJECT_ID
          value: "{project_label}"
        - name: WEFT_BROKER_URL
          value: "{broker_url}"
        - name: WEFT_BROKER_TOKEN_PATH
          value: "/var/run/weft/sa/token"
        - name: WEFT_NAMESPACE
          value: "{namespace}"
        - name: WEFT_OWNER_DISPATCHER
          value: "{owner_dispatcher}"
        - name: WEFT_POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: WEFT_TENANT_ID
          value: "{tenant}"
        - name: WEFT_CONNECTION_PORT
          value: "{conn_port}"
        - name: WEFT_CALLER_TOKEN_SECRET
          value: "{caller_token_secret_hex}"
      volumeMounts:
        - name: weft-sa-token
          mountPath: /var/run/weft/sa
          readOnly: true
  volumes:
    - name: weft-sa-token
      projected:
        sources:
          - serviceAccountToken:
              audience: weft-broker
              expirationSeconds: 3600
              path: token
"#,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_platform_traits::clock::FakeClock;
    use weft_platform_traits::FakeKube;

    fn spec() -> SpawnPodSpec {
        SpawnPodSpec {
            project_id: "p1".into(),
            tenant: "t1".into(),
            namespace: "wft-p1".into(),
            owner_dispatcher: "disp-0".into(),
            binary_hash: Some("abc123".into()),
            caller_token_secret_hex: "deadbeef".into(),
        }
    }

    /// The rendered pod manifest exposes the connection port + the
    /// subdomain (for per-pod DNS) + the token-secret env, and the
    /// headless Service is rendered per project. Guards the wiring the
    /// gateway relies on.
    #[test]
    fn manifest_wires_connection_server() {
        let pod = render_pod_manifest(
            "wp-1",
            "wft-p1",
            "img:tag",
            &SafeLabel::new("p1", 63),
            &SafeLabel::new("t1", 63),
            "http://broker",
            "disp-0",
            "deadbeef",
            None,
            None,
        );
        // No sandbox by default: the pod spec carries no runtimeClassName.
        assert!(!pod.contains("runtimeClassName"), "no sandbox by default");
        // No registry pull secret by default (image loaded onto the node).
        assert!(!pod.contains("imagePullSecrets"), "no pull secret by default");
        assert!(pod.contains("containerPort: 9091"), "port exposed");
        assert!(pod.contains("subdomain: weft-workers"), "pod DNS subdomain");
        assert!(pod.contains("WEFT_CONNECTION_PORT"), "port env");
        assert!(
            pod.contains("WEFT_CALLER_TOKEN_SECRET") && pod.contains("deadbeef"),
            "token secret env"
        );
        let svc = render_headless_service("wft-p1");
        assert!(svc.contains("clusterIP: None"), "headless");
        assert!(svc.contains("name: weft-workers"), "headless service name");
        // Selector is role-only (NOT project-scoped): one Service per
        // namespace serving every worker, so a shared namespace's
        // per-project spawns don't overwrite each other's selector.
        assert!(svc.contains("weft.dev/role: worker"), "role selector");
        assert!(
            !svc.contains("weft.dev/project"),
            "no project selector on the headless service"
        );
    }

    /// When the sandbox policy selects a runtime, the pod spec carries exactly
    /// that `runtimeClassName` line, placed in the pod spec so kubernetes routes
    /// the pod to it.
    #[test]
    fn manifest_emits_runtime_class_when_sandboxed() {
        let pod = render_pod_manifest(
            "wp-1",
            "wft-shared-workers",
            "img:tag",
            &SafeLabel::new("p1", 63),
            &SafeLabel::new("t1", 63),
            "http://broker",
            "disp-0",
            "deadbeef",
            Some("gvisor"),
            Some("weft-registry-pull"),
        );
        assert!(pod.contains("runtimeClassName: gvisor"), "sandbox runtime set");
        // The pull secret is emitted when configured.
        assert!(
            pod.contains("imagePullSecrets:") && pod.contains("name: weft-registry-pull"),
            "pull secret set when configured"
        );
        // It sits in the pod spec (after `spec:`), before the containers.
        let spec_idx = pod.find("\nspec:").expect("has spec");
        let rc_idx = pod.find("runtimeClassName").expect("has runtimeClassName");
        let containers_idx = pod.find("containers:").expect("has containers");
        assert!(spec_idx < rc_idx && rc_idx < containers_idx, "runtimeClassName in pod spec");
    }

    /// Happy path: no waiting-reason seeded, the 5s watch window
    /// elapses (FakeClock fast-forwards), spawn returns a handle.
    #[tokio::test]
    async fn spawn_ok_when_no_pull_failure() {
        let kube = FakeKube::new();
        let backend = K8sWorkerBackend::new(
            "http://broker".into(),
            kube.clone(),
            FakeClock::new(),
            crate::placement::no_sandbox(),
            None,
        );
        let handle = backend.spawn_pod("wp-1", spec()).await.unwrap();
        assert_eq!(handle.pod_name, "wp-1");
    }

    /// ImagePullBackOff seeded → spawn bails with the image message
    /// rather than silently waiting out the readiness timeout.
    #[tokio::test]
    async fn spawn_bails_on_image_pull_backoff() {
        let kube = FakeKube::new();
        kube.set_pod_waiting_reason("wft-p1", "wp-1", "ImagePullBackOff");
        let backend = K8sWorkerBackend::new(
            "http://broker".into(),
            kube.clone(),
            FakeClock::new(),
            crate::placement::no_sandbox(),
            None,
        );
        let err = backend.spawn_pod("wp-1", spec()).await.unwrap_err();
        assert!(
            err.to_string().contains("ImagePullBackOff"),
            "got: {err}"
        );
    }

    /// No binary_hash → fail loud (don't fall back to :latest).
    #[tokio::test]
    async fn spawn_fails_without_binary_hash() {
        let kube = FakeKube::new();
        let backend = K8sWorkerBackend::new(
            "http://broker".into(),
            kube,
            FakeClock::new(),
            crate::placement::no_sandbox(),
            None,
        );
        let mut s = spec();
        s.binary_hash = None;
        let err = backend.spawn_pod("wp-1", s).await.unwrap_err();
        assert!(err.to_string().contains("no running_binary_hash"), "got: {err}");
    }
}
