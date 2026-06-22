//! Per-project k8s namespace bundle.
//!
//! Created lazily the first time a project actually needs infra (the
//! supervisor's infra-apply path), NOT on registration. A project that
//! never declares infra never gets a per-project namespace: its worker
//! runs in the shared worker namespace ([`SHARED_WORKER_NAMESPACE`])
//! alongside other tenants' no-infra workers. Namespace-per-project
//! burns the cluster's namespace ceiling, so only infra projects (whose
//! worker MUST sit next to its infra pods to reach them) pay for one.
//!
//! Holds:
//!   - the namespace itself
//!   - ServiceAccounts: `weft-worker-sa` (project workers),
//!     `weft-infra-sa` (every pod the supervisor applies from an
//!     InfraSpec)
//!   - NetworkPolicies: default-deny, worker-policy, infra-policy
//!   - RoleBindings binding the supervisor + listener
//!     ClusterRoles (defined in cluster-rbac.yaml) into this namespace.
//!
//! Naming convention:
//!   `wm-project-<tenant>--<project>` (both ids sanitized + truncated to
//!   a stable 12-char prefix, joined by a DOUBLE dash, to fit in the
//!   63-char DNS label limit). See [`name_for`].

use anyhow::Result;

/// The single shared namespace that holds every no-infra project's
/// worker, across all tenants. Created once, lazily, the first time a
/// no-infra worker is placed (see `shared_worker_namespace::ensure`);
/// never torn down (it is cluster-singleton infrastructure). Cross-
/// tenant isolation inside it is a blanket pod-to-pod-deny
/// NetworkPolicy: workers never talk to each other.
// SYNC: SHARED_WORKER_NAMESPACE <-> crates/weft-broker/src/auth.rs SHARED_WORKER_NAMESPACE, crates/weft-e2e/tests/worker_placement.rs SHARED_WORKER_NAMESPACE
pub const SHARED_WORKER_NAMESPACE: &str = "wm-shared-workers";

/// The k8s namespace a project's WORKER pods run in, the single source
/// of truth for worker placement. A project with infra gets its own
/// per-project namespace (the worker sits next to its infra pods); a
/// project with no infra shares [`SHARED_WORKER_NAMESPACE`]. Every
/// worker spawn, DNS computation, and teardown routes through this one
/// function so there is no second answer to "where does this project's
/// worker live."
pub fn worker_namespace(has_infra: bool, tenant: &str, project_id: &str) -> String {
    if has_infra {
        name_for(tenant, project_id)
    } else {
        SHARED_WORKER_NAMESPACE.to_string()
    }
}

/// Compute the project namespace name from tenant + project ids.
/// Both are sanitized + truncated so the resulting name fits in 63
/// chars and uses only `[a-z0-9-]`.
/// Project namespace name: `wm-project-<tenant>--<project>`.
///
/// The DOUBLE dash between tenant and project is the unambiguous
/// separator: both tenant and project labels may contain single
/// dashes (truncated UUIDs do), but `short_label` collapses all
/// runs of dashes to one, so neither side can produce a `--`. This
/// keeps the namespace name an unambiguous join of its two parts.
/// Tenant resolution itself does NOT parse this string: the broker
/// looks the namespace up in the `weft_namespace_tenant` registry
/// (written by `ensure` below), so a tenant can never forge their
/// tenant id by crafting a namespace name.
pub fn name_for(tenant: &str, project_id: &str) -> String {
    let t = short_label(tenant, 12);
    let p = short_label(project_id, 12);
    format!("wm-project-{t}--{p}")
}

/// A string that has been sanitized to a k8s-label-safe form
/// (`[a-z0-9-]`, dash-runs collapsed, no leading/trailing dash,
/// length-capped). The ONLY constructor is [`SafeLabel::new`], which
/// runs the sanitizer, so a value of this type is a proof that the
/// sanitization happened. Manifest renderers take `SafeLabel` for any
/// id interpolated into YAML; that makes "forgot to sanitize a
/// tenant / project id before interpolating" a COMPILE error rather
/// than a latent YAML-injection / label-smuggling seam (a free-form
/// cloud `tenant_id` with a newline or `"` can't reach a manifest
/// raw).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafeLabel(String);

impl SafeLabel {
    /// Sanitize `raw` to a k8s-label-safe form capped at `max` chars.
    pub fn new(raw: &str, max: usize) -> Self {
        SafeLabel(short_label(raw, max))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SafeLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn short_label(s: &str, max: usize) -> String {
    let mut out = String::with_capacity(max);
    let mut last_dash = false;
    for c in s.chars() {
        let lc = c.to_ascii_lowercase();
        if lc.is_ascii_alphanumeric() {
            out.push(lc);
            last_dash = false;
            if out.len() >= max {
                break;
            }
        } else if lc == '-' {
            // Collapse runs of dashes: the namespace name uses `--`
            // as the tenant/project separator and counts on neither
            // side producing a `--` itself.
            if !last_dash {
                out.push(lc);
                last_dash = true;
                if out.len() >= max {
                    break;
                }
            }
        }
    }
    while out.starts_with('-') {
        out.remove(0);
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        out.push_str("x");
    }
    out
}

pub struct ProjectNamespaceArgs<'a> {
    // RAW (not SafeLabel) deliberately: `tenant_id` is dual-use here.
    // `render` sanitizes it for the manifest LABEL (below), but
    // `ensure` also writes it RAW to the namespace registry, which is
    // the broker's TokenReview key and MUST be the real tenant id, not
    // a sanitized form. The two cross-module manifest renderers
    // (k8s_worker, tenant_namespace) take `SafeLabel` because they
    // can forget to sanitize; this one is the module that owns
    // `short_label` and needs the raw value regardless, so it
    // sanitizes inline at the one interpolation point.
    pub project_id: &'a str,
    pub tenant_id: &'a str,
    /// Namespace name (as produced by [`name_for`]).
    pub namespace: &'a str,
    /// Pod CIDR for NetworkPolicy egress exclusions. Read from
    /// `cluster_config()` on the dispatcher side.
    pub pod_cidr: &'a str,
    /// Service CIDR for the same purpose.
    pub service_cidr: &'a str,
    /// The ingress controller's namespace (typically `ingress-nginx`).
    pub ingress_namespace: &'a str,
    /// The tenant namespace, used in NetworkPolicy `namespaceSelector`
    /// to allow the tenant's storage box into this project ns.
    pub tenant_namespace: &'a str,
    /// The control-plane namespace, where the pooled (trusted, tenant-
    /// agnostic) listener + supervisor run. Their RoleBindings into this
    /// project namespace bind the SAs HERE (not in the tenant ns), and
    /// the NetworkPolicies allow listener/supervisor traffic FROM here.
    pub control_plane_namespace: &'a str,
}

/// Render the project-namespace bundle as a single multi-doc YAML.
///
/// Includes: Namespace, ServiceAccounts (worker, infra),
/// NetworkPolicies (default-deny + worker-policy + infra-policy),
/// RoleBindings (supervisor + listener -> their ClusterRoles).
pub fn render(args: &ProjectNamespaceArgs<'_>) -> String {
    let ProjectNamespaceArgs {
        project_id,
        tenant_id,
        namespace,
        pod_cidr,
        service_cidr,
        ingress_namespace,
        tenant_namespace,
        control_plane_namespace,
    } = args;
    // Sanitize the ids for the manifest LABEL values (the raw
    // `tenant_id` is kept by `ensure` for the registry key). `_label`
    // shadows so the raw values can't accidentally be interpolated
    // below.
    let tenant_id = SafeLabel::new(tenant_id, 63);
    let project_id = SafeLabel::new(project_id, 63);
    // The worker->box egress port is the box's fixed listen port; pull
    // it from the one constant so it can never drift from the box's own
    // Service/containerPort (a literal here once meant changing
    // STORAGE_PORT would silently break the data path).
    let storage_port = weft_storage::config::STORAGE_PORT;
    // Namespace the Envoy Gateway runs in (its default install namespace);
    // the only source allowed to reach worker connection ports. Worker
    // port pulled from the one constant so it can't drift from the pod
    // manifest's containerPort.
    let gateway_namespace = crate::backend::k8s_worker::GATEWAY_NAMESPACE;
    let connection_port = crate::backend::k8s_worker::WORKER_CONNECTION_PORT;
    format!(
        r#"---
apiVersion: v1
kind: Namespace
metadata:
  name: {namespace}
  labels:
    weft.dev/role: project
    weft.dev/tenant: "{tenant_id}"
    weft.dev/project: "{project_id}"
    kubernetes.io/metadata.name: {namespace}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: weft-worker-sa
  namespace: {namespace}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: weft-infra-sa
  namespace: {namespace}
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny
  namespace: {namespace}
spec:
  podSelector: {{}}
  policyTypes:
    - Ingress
    - Egress
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: worker-policy
  namespace: {namespace}
spec:
  podSelector:
    matchLabels:
      weft.dev/role: worker
  policyTypes:
    - Ingress
    - Egress
  ingress:
    # Live caller connections: the gateway forwards a routed caller to
    # the worker's connection port. Only the gateway namespace may reach
    # workers (callers never touch a worker directly; the signed routing
    # token is the second gate inside the worker).
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {gateway_namespace}
      ports:
        - protocol: TCP
          port: {connection_port}
  egress:
    # Broker (cross-ns to weft-db).
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: weft-db
          podSelector:
            matchLabels:
              weft.dev/role: broker
      ports:
        - protocol: TCP
          port: 9090
    # DNS resolution.
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
    # Same-namespace infra pods.
    - to:
        - podSelector:
            matchLabels:
              weft.dev/role: infra
    # Tenant-namespace storage box (the ctx.storage data path:
    # worker<->box byte streaming).
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {tenant_namespace}
          podSelector:
            matchLabels:
              weft.dev/role: storage
      ports:
        - protocol: TCP
          port: {storage_port}
    # Internet egress (HTTP APIs, model downloads, etc).
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - {pod_cidr}
              - {service_cidr}
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: infra-policy
  namespace: {namespace}
spec:
  podSelector:
    matchLabels:
      weft.dev/role: infra
  policyTypes:
    - Ingress
    - Egress
  ingress:
    # Same-namespace workers.
    - from:
        - podSelector:
            matchLabels:
              weft.dev/role: worker
    # Pooled listener in the control-plane namespace (SSE subscribes to
    # infra /events).
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {control_plane_namespace}
          podSelector:
            matchLabels:
              weft.dev/role: listener
    # Pooled supervisor in the control-plane namespace (HTTP health
    # probes).
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {control_plane_namespace}
          podSelector:
            matchLabels:
              weft.dev/role: infra-supervisor
    # Dispatcher (cross-ns from weft-system) for /live-proxy.
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: weft-system
          podSelector:
            matchLabels:
              weft.dev/role: dispatcher
    # Ingress controller for TenantPublic endpoints.
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {ingress_namespace}
          podSelector:
            matchLabels:
              app.kubernetes.io/name: ingress-nginx
  egress:
    # Internet egress. Per-node InfraSpec.access.egress may further
    # restrict via additional NetworkPolicies stamped at apply time.
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - {pod_cidr}
              - {service_cidr}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: weft-infra-supervisor
  namespace: {namespace}
subjects:
  - kind: ServiceAccount
    name: weft-infra-supervisor-sa
    namespace: {control_plane_namespace}
roleRef:
  kind: ClusterRole
  name: weft-infra-supervisor-clusterrole
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: weft-listener
  namespace: {namespace}
subjects:
  - kind: ServiceAccount
    name: weft-listener-sa
    namespace: {control_plane_namespace}
roleRef:
  kind: ClusterRole
  name: weft-listener-clusterrole
  apiGroup: rbac.authorization.k8s.io
"#,
    )
}

/// Apply the project-namespace bundle. Idempotent (kubectl apply).
/// Called on the first infra apply for a project (`api::infra::sync`,
/// gated on the project declaring infra) and on cleanup retries.
/// A no-infra project never reaches here: its worker lives in the
/// shared worker namespace, so it never gets a per-project namespace.
/// Writes the `(namespace, tenant_id)` row to the namespace registry
/// alongside the kubectl apply so the broker's TokenReview path can
/// resolve the tenant without parsing the namespace string.
pub async fn ensure(
    pool: &sqlx::PgPool,
    kube: &dyn weft_platform_traits::KubeClient,
    args: &ProjectNamespaceArgs<'_>,
) -> Result<()> {
    let manifest = render(args);
    kube.apply_yaml(&manifest).await?;
    crate::namespace_registry::register(pool, args.namespace, args.tenant_id).await
}

/// Delete the entire namespace. Used by `weft rm` (after the
/// supervisor has terminated any infra). Takes RoleBindings, Pods,
/// PVCs, Services, etc with it.
pub async fn delete(kube: &dyn weft_platform_traits::KubeClient, namespace: &str) -> Result<()> {
    kube.delete_namespace(namespace).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_for_truncates_and_normalizes() {
        let n = name_for("Tenant-FOO", "abcdef-12345678-9999");
        assert!(n.starts_with("wm-project-"));
        assert!(n.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
        assert!(n.len() <= 63);
    }

    #[test]
    fn safe_label_neutralizes_yaml_injection() {
        // A free-form (e.g. future cloud) id with characters that
        // could break the manifest or smuggle a label/field. SafeLabel
        // is the type the manifest renderers require, so this is the
        // only form an id can take in a manifest. It must come out as
        // pure `[a-z0-9-]`, no quotes / newlines / colons / braces.
        let evil = "alice\"\n  weft.dev/role: admin\n  x: \"y";
        let safe = SafeLabel::new(evil, 63);
        assert!(
            safe.as_str().chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "got {:?}",
            safe.as_str()
        );
        // A UUID (the OSS project_id) survives intact: sanitize is a
        // no-op on already-label-safe input.
        let uuid = "88d7eec8-6ffc-4cb4-8582-1a2b3c4d5e6f";
        assert_eq!(SafeLabel::new(uuid, 63).as_str(), uuid);
    }

    #[test]
    fn name_for_is_deterministic() {
        let a = name_for("local", "deadbeef");
        let b = name_for("local", "deadbeef");
        assert_eq!(a, b);
    }

    #[test]
    fn worker_namespace_routes_on_has_infra() {
        // No infra: the shared namespace, regardless of tenant/project.
        assert_eq!(
            worker_namespace(false, "local", "p1"),
            SHARED_WORKER_NAMESPACE
        );
        assert_eq!(
            worker_namespace(false, "tenant-xyz", "p2"),
            SHARED_WORKER_NAMESPACE
        );
        // Infra: the project's own namespace (== name_for).
        assert_eq!(
            worker_namespace(true, "local", "p1"),
            name_for("local", "p1")
        );
        // Two infra projects never collide; a no-infra and an infra
        // project never share a namespace.
        assert_ne!(
            worker_namespace(true, "local", "p1"),
            worker_namespace(true, "local", "p2")
        );
        assert_ne!(
            worker_namespace(false, "local", "p1"),
            worker_namespace(true, "local", "p1")
        );
    }

    #[test]
    fn name_for_uses_distinct_components() {
        // Different tenants for the same project id must yield
        // distinct namespaces.
        let a = name_for("t1", "p1");
        let b = name_for("t2", "p1");
        assert_ne!(a, b);
        // And vice versa.
        let c = name_for("t1", "p1");
        let d = name_for("t1", "p2");
        assert_ne!(c, d);
    }

    #[test]
    fn name_for_uses_double_dash_separator() {
        let n = name_for("local", "88d7eec8-6ffc-4cb4-8582-380fd65f2643");
        // The double-dash is the unambiguous separator between
        // tenant + project, keeping the namespace name a lossless
        // join of its two parts (tenant resolution itself is via the
        // weft_namespace_tenant registry, not string parsing).
        assert!(n.contains("--"), "{n}");
        assert!(n.starts_with("wm-project-local--"), "{n}");
    }

    #[test]
    fn short_label_collapses_dash_runs() {
        // The double-dash separator only works if neither component
        // produces `--`. Multi-dash tenant inputs must collapse.
        let n = name_for("user--x", "proj1");
        // After collapsing runs: tenant="user-x", project="proj1".
        // Separator stays "--".
        assert_eq!(n, "wm-project-user-x--proj1");
    }

    fn args() -> ProjectNamespaceArgs<'static> {
        ProjectNamespaceArgs {
            project_id: "proj1",
            tenant_id: "alice",
            namespace: "wm-project-alice--proj1",
            pod_cidr: "10.244.0.0/16",
            service_cidr: "10.96.0.0/12",
            ingress_namespace: "ingress-nginx",
            tenant_namespace: "wm-tenant-alice",
            control_plane_namespace: "weft-system",
        }
    }

    #[test]
    fn render_emits_namespace_and_sas() {
        let yaml = render(&args());
        assert!(yaml.contains("kind: Namespace"));
        assert!(yaml.contains("name: wm-project-alice--proj1"));
        assert!(yaml.contains("name: weft-worker-sa"));
        assert!(yaml.contains("name: weft-infra-sa"));
    }

    #[test]
    fn render_emits_network_policies() {
        let yaml = render(&args());
        assert!(yaml.contains("name: default-deny"));
        assert!(yaml.contains("name: worker-policy"));
        assert!(yaml.contains("name: infra-policy"));
    }

    #[test]
    fn render_emits_role_bindings_to_clusterroles() {
        let yaml = render(&args());
        assert!(yaml.contains("name: weft-infra-supervisor-clusterrole"));
        assert!(yaml.contains("name: weft-listener-clusterrole"));
        // RoleBinding subjects point at the CONTROL-PLANE namespace's
        // SAs: the pooled listener + supervisor are trusted services
        // that run there (not per tenant) and act across all project
        // namespaces via these per-namespace bindings.
        assert!(yaml.contains("name: weft-infra-supervisor-sa"));
        assert!(yaml.contains("name: weft-listener-sa"));
        assert!(yaml.contains("namespace: weft-system"));
    }

    #[test]
    fn render_stamps_project_and_tenant_labels() {
        let yaml = render(&args());
        assert!(yaml.contains("weft.dev/tenant: \"alice\""));
        assert!(yaml.contains("weft.dev/project: \"proj1\""));
    }

    #[test]
    fn render_excludes_ingress_egress_cidrs_from_internet() {
        let yaml = render(&args());
        assert!(yaml.contains("10.244.0.0/16"));
        assert!(yaml.contains("10.96.0.0/12"));
    }

    #[test]
    fn render_infra_policy_allows_ingress_controller() {
        let yaml = render(&args());
        assert!(yaml.contains("ingress-nginx"));
    }
}
