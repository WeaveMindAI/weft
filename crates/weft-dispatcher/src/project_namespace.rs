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
//!   `<install prefix><tenant>--<project>`, `wft-project-<tenant>--<project>`
//!   on the default install (both ids sanitized + truncated to a stable
//!   12-char prefix, joined by a DOUBLE dash, to fit in the 63-char DNS
//!   label limit). See [`name_for`].

use anyhow::Result;
use weft_core::infra::Instance;

/// The k8s namespace a project's WORKER pods run in, the single source
/// of truth for worker placement. A project with infra gets its own
/// per-project namespace (the worker sits next to its infra pods); a
/// project with no infra shares the install's shared worker namespace
/// (`Instance::shared_worker_namespace`: created lazily the first time a
/// no-infra worker is placed, never torn down, every worker walled off
/// from the others by a pod-to-pod-deny NetworkPolicy). Every worker
/// spawn, DNS computation, and teardown routes through this one function
/// so there is no second answer to "where does this project's worker
/// live."
pub fn worker_namespace(instance: &Instance, has_infra: bool, tenant: &str, project_id: uuid::Uuid) -> String {
    if has_infra {
        name_for(instance, tenant, project_id)
    } else {
        instance.shared_worker_namespace()
    }
}

/// Compute the project namespace name from tenant + project ids.
/// Both are sanitized + truncated so the resulting name fits in 63
/// chars and uses only `[a-z0-9-]`.
/// Project namespace name: the install's project prefix, then
/// `<tenant>--<project>` (`wft-project-<tenant>--<project>` on the
/// default install).
///
/// The DOUBLE dash between tenant and project is the unambiguous
/// separator: both tenant and project labels may contain single
/// dashes (truncated UUIDs do), but `short_label` collapses all
/// runs of dashes to one, so neither side can produce a `--`. This
/// keeps the namespace name an unambiguous join of its two parts.
/// Nothing resolves a tenant from this string, and nothing resolves
/// one from the namespace at all: the broker identifies a worker by
/// its pod, so a crafted namespace name names nobody.
pub fn name_for(instance: &Instance, tenant: &str, project_id: uuid::Uuid) -> String {
    let t = short_label(tenant, 12);
    let p = short_label(&project_id.to_string(), 12);
    format!("{}{t}--{p}", instance.project_namespace_prefix())
}

/// A string that has been sanitized to a k8s-label-safe form
/// (`[a-z0-9-]`, dash-runs collapsed, no leading/trailing dash,
/// length-capped). The ONLY constructor is [`SafeLabel::new`], which
/// runs the sanitizer, so a value of this type is a proof that the
/// sanitization happened. Manifest renderers take `SafeLabel` for any
/// id interpolated into YAML; that makes "forgot to sanitize a
/// tenant / project id before interpolating" a COMPILE error rather
/// than a latent YAML-injection / label-smuggling seam (a free-form
/// `tenant_id` with a newline or `"` can't reach a manifest raw).
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
        out.push('x');
    }
    out
}

pub struct ProjectNamespaceArgs<'a> {
    // RAW (not SafeLabel) deliberately: `tenant_id` is dual-use here.
    // `render` sanitizes it for the manifest LABEL (below), but
    // `ensure` also writes it RAW to the namespace registry, which is
    // the broker's TokenReview key and MUST be the real tenant id, not
    // a sanitized form. The cross-module manifest renderer
    // (`k8s_worker`) takes `SafeLabel` because it
    // can forget to sanitize; this one is the module that owns
    // `short_label` and needs the raw value regardless, so it
    // sanitizes inline at the one interpolation point.
    pub project_id: uuid::Uuid,
    pub tenant_id: &'a str,
    /// Namespace name (as produced by [`name_for`]).
    pub namespace: &'a str,
    /// Pod CIDR for NetworkPolicy egress exclusions. Read from
    /// `cluster_config()` on the dispatcher side.
    pub pod_cidr: &'a str,
    /// Service CIDR for the same purpose.
    pub service_cidr: &'a str,
    /// The install this namespace belongs to. Its system namespace is where
    /// the pooled (trusted, tenant-agnostic) listener + supervisor pods and
    /// the dispatcher run: their RoleBindings into this project namespace
    /// bind the SAs THERE, and the NetworkPolicies allow their traffic FROM
    /// there. Its db namespace is where the broker the workers call runs.
    /// (There is no per-tenant `storage` pod: a worker's runtime file bytes
    /// go DIRECTLY to the object store via presigned URLs, so the worker
    /// egress allows the broker (control) + the object store (bytes), not a
    /// storage-pod relay.)
    pub instance: &'a Instance,
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
        instance,
    } = args;
    let control_plane_namespace = instance.system_namespace();
    let db_namespace = instance.db_namespace();
    // Sanitize the ids for the manifest LABEL values (the raw
    // `tenant_id` is kept by `ensure` for the registry key). `_label`
    // shadows so the raw values can't accidentally be interpolated
    // below.
    let tenant_id = SafeLabel::new(tenant_id, 63);
    let project_id = SafeLabel::new(&project_id.to_string(), 63);
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
    # Broker (cross-ns to the install's db namespace).
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {db_namespace}
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
    # Internet egress (HTTP APIs, model downloads, AND the object store). The object
    # store is ALWAYS external to the cluster, reached over S3 by its configured
    # endpoint. The worker uploads/downloads runtime-file
    # bytes DIRECTLY to it via broker-signed presigned URLs (the broker never carries
    # the bytes), reaching it over this internet egress like any external host. The
    # security wall is the presigned URL (per-key, per-method, short-TTL) plus the
    # worker holding NO bucket credentials, NOT network reachability.
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - {pod_cidr}
              - {service_cidr}
              # Link-local. 169.254.169.254 is the node's cloud-provider metadata
              # / credential endpoint (the #1 container-escape target); excluding
              # the whole 169.254.0.0/16 keeps a compromised pod off it
              # regardless of what the provider also blocks at the node.
              - 169.254.0.0/16
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
    # Dispatcher (cross-ns from the install's system namespace) for
    # /live-proxy.
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {control_plane_namespace}
          podSelector:
            matchLabels:
              weft.dev/role: dispatcher
    # The front door's proxy, for TenantPublic endpoints (an HTTPRoute
    # on its `local` listener, see weft-core `infra::compile`).
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {gateway_namespace}
          podSelector:
            matchLabels:
              app.kubernetes.io/name: envoy
  egress:
    # Internet egress. Per-node InfraSpec.access.egress may further
    # restrict via additional NetworkPolicies stamped at apply time.
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - {pod_cidr}
              - {service_cidr}
              # Link-local. 169.254.169.254 is the node's cloud-provider metadata
              # / credential endpoint (the #1 container-escape target); excluding
              # the whole 169.254.0.0/16 keeps a compromised pod off it
              # regardless of what the provider also blocks at the node.
              - 169.254.0.0/16
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

/// Apply the project-namespace bundle. Idempotent (server-side apply).
/// Called on the first infra apply for a project (`api::infra::sync`,
/// gated on the project declaring infra) and on cleanup retries.
/// A no-infra project never reaches here: its worker lives in the
/// shared worker namespace, so it never gets a per-project namespace.
pub async fn ensure(
    kube: &dyn weft_platform_traits::KubeClient,
    args: &ProjectNamespaceArgs<'_>,
) -> Result<()> {
    kube.apply_yaml(&render(args)).await
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

    fn default_install() -> Instance {
        Instance::default_install()
    }

    fn name_for(tenant: &str, project_id: uuid::Uuid) -> String {
        super::name_for(&default_install(), tenant, project_id)
    }

    fn worker_namespace(has_infra: bool, tenant: &str, project_id: uuid::Uuid) -> String {
        super::worker_namespace(&default_install(), has_infra, tenant, project_id)
    }

    #[test]
    fn name_for_truncates_and_normalizes() {
        let n = name_for("Tenant-FOO", uuid::Uuid::from_u128(u128::MAX));
        assert!(n.starts_with("wft-project-"));
        assert!(n.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
        assert!(n.len() <= 63);
    }

    #[test]
    fn safe_label_neutralizes_yaml_injection() {
        // A free-form id with characters that
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
        let a = name_for("local", uuid::Uuid::from_u128(0xdeadbeef));
        let b = name_for("local", uuid::Uuid::from_u128(0xdeadbeef));
        assert_eq!(a, b);
    }

    // Distinct in the leading characters the namespace keeps, the way
    // two random project ids are.
    const P1: uuid::Uuid = uuid::Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111);
    const P2: uuid::Uuid = uuid::Uuid::from_u128(0x2222_2222_2222_2222_2222_2222_2222_2222);

    #[test]
    fn worker_namespace_routes_on_has_infra() {
        // No infra: the shared namespace, regardless of tenant/project.
        assert_eq!(worker_namespace(false, "local", P1), "wft-shared-workers");
        assert_eq!(worker_namespace(false, "tenant-xyz", P2), "wft-shared-workers");
        // Infra: the project's own namespace (== name_for).
        assert_eq!(
            worker_namespace(true, "local", P1),
            name_for("local", P1)
        );
        // Two infra projects never collide; a no-infra and an infra
        // project never share a namespace.
        assert_ne!(
            worker_namespace(true, "local", P1),
            worker_namespace(true, "local", P2)
        );
        assert_ne!(
            worker_namespace(false, "local", P1),
            worker_namespace(true, "local", P1)
        );
    }

    #[test]
    fn name_for_uses_distinct_components() {
        // Different tenants for the same project id must yield
        // distinct namespaces.
        let a = name_for("t1", P1);
        let b = name_for("t2", P1);
        assert_ne!(a, b);
        // And vice versa.
        let c = name_for("t1", P1);
        let d = name_for("t1", P2);
        assert_ne!(c, d);
    }

    #[test]
    fn name_for_uses_double_dash_separator() {
        let n = name_for("local", uuid::Uuid::from_u128(0x88d7eec86ffc4cb48582380fd65f2643));
        // The double-dash is the unambiguous separator between
        // tenant + project, keeping the namespace name a lossless
        // join of its two parts. Nothing resolves a tenant by parsing
        // it; the name is for humans reading `kubectl get ns`.
        assert!(n.contains("--"), "{n}");
        assert!(n.starts_with("wft-project-local--"), "{n}");
    }

    #[test]
    fn short_label_collapses_dash_runs() {
        // The double-dash separator only works if neither component
        // produces `--`. Multi-dash tenant inputs must collapse.
        let n = name_for("user--x", uuid::Uuid::from_u128(1));
        // After collapsing runs: tenant="user-x", project cut to its
        // first 12 characters. Separator stays "--".
        assert_eq!(n, "wft-project-user-x--00000000-000");
    }

    #[test]
    fn a_named_install_places_workers_in_its_own_namespaces() {
        let cell = Instance::named("cell1").unwrap();
        assert_eq!(super::worker_namespace(&cell, false, "local", uuid::Uuid::from_u128(1)), "wft-cell1-shared-workers");
        assert_eq!(super::name_for(&cell, "local", uuid::Uuid::from_u128(1)), "wft-cell1-project-local--00000000-000");
    }

    fn args_for(instance: &Instance) -> ProjectNamespaceArgs<'_> {
        ProjectNamespaceArgs {
            project_id: uuid::Uuid::from_u128(1),
            tenant_id: "alice",
            namespace: "wft-project-alice--proj1",
            pod_cidr: "10.244.0.0/16",
            service_cidr: "10.96.0.0/12",
            instance,
        }
    }

    fn args() -> ProjectNamespaceArgs<'static> {
        static DEFAULT: std::sync::OnceLock<Instance> = std::sync::OnceLock::new();
        args_for(DEFAULT.get_or_init(Instance::default_install))
    }

    #[test]
    fn render_names_the_installs_own_control_plane_and_broker() {
        let cell = Instance::named("cell1").unwrap();
        let yaml = render(&args_for(&cell));
        assert!(yaml.contains("kubernetes.io/metadata.name: weft-cell1-db"), "{yaml}");
        assert!(yaml.contains("namespace: weft-cell1-system"), "{yaml}");
        assert!(!yaml.contains("weft-system") && !yaml.contains("name: weft-db"), "{yaml}");
    }

    #[test]
    fn render_emits_namespace_and_sas() {
        let yaml = render(&args());
        assert!(yaml.contains("kind: Namespace"));
        assert!(yaml.contains("name: wft-project-alice--proj1"));
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
    fn worker_egress_allows_broker_and_external_object_store() {
        // The worker reaches the broker (control: sign + record) in-cluster, and the
        // object store (an EXTERNAL S3, or a host S3 server locally) over the internet
        // egress. The store is never an in-cluster pod, so there is no object-store
        // pod egress rule. No leftover per-tenant `storage` role or namespace ref.
        let yaml = render(&args());
        let worker_policy = yaml
            .split("name: worker-policy")
            .nth(1)
            .unwrap()
            .split("name: infra-policy")
            .next()
            .unwrap();
        assert!(worker_policy.contains("weft.dev/role: broker"), "broker (control) egress present");
        assert!(worker_policy.contains("cidr: 0.0.0.0/0"), "internet egress covers the external object store");
        assert!(!worker_policy.contains("weft.dev/role: object-store"), "object store is external, not a pod");
        assert!(!worker_policy.contains("weft.dev/role: storage"), "no dead storage-pod role remains");
        assert!(!yaml.contains("wft-tenant-"), "no per-tenant storage namespace ref remains");
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
        assert!(yaml.contains("weft.dev/project: \"00000000-0000-0000-0000-000000000001\""));
    }

    #[test]
    fn render_excludes_ingress_egress_cidrs_from_internet() {
        let yaml = render(&args());
        assert!(yaml.contains("10.244.0.0/16"));
        assert!(yaml.contains("10.96.0.0/12"));
        // Link-local (cloud metadata / credential endpoint) is excluded from
        // BOTH egress policies (worker-policy AND infra-policy): a compromised
        // worker or infra pod must never reach 169.254.169.254.
        assert_eq!(
            yaml.matches("- 169.254.0.0/16").count(),
            2,
            "link-local excluded in both worker-policy and infra-policy egress"
        );
    }

    #[test]
    fn render_infra_policy_admits_the_front_door_proxy() {
        let yaml = render(&args());
        let infra = &yaml[yaml.find("name: infra-policy").expect("an infra policy")..];
        assert!(infra.contains("kubernetes.io/metadata.name: envoy-gateway-system"));
        assert!(infra.contains("app.kubernetes.io/name: envoy"));
        assert!(!yaml.contains("ingress-nginx"));
    }
}
