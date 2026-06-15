//! Per-tenant k8s namespace bundle.
//!
//! Houses tenant-scoped services that are shared across the
//! tenant's projects:
//!   - the listener pod (`weft-listener-sa`)
//!   - the infra-supervisor pod (`weft-infra-supervisor-sa`)
//!
//! Workers + infra pods live in PROJECT namespaces, not here.
//! The per-project bundle (`project_namespace.rs`) creates those
//! namespaces, their SAs, NetworkPolicies, and RoleBindings to the
//! cluster-scoped supervisor / listener ClusterRoles.
//!
//! Idempotent: `kubectl apply` reconciles whatever state the cluster
//! already has.

use anyhow::Result;
use weft_platform_traits::KubeClient;

pub struct ClusterCidrs<'a> {
    pub pod_cidr: &'a str,
    pub service_cidr: &'a str,
    pub ingress_namespace: &'a str,
}

pub async fn ensure_tenant_namespace(
    pool: &sqlx::PgPool,
    kube: &dyn KubeClient,
    namespace: &str,
    tenant: &str,
    cidrs: ClusterCidrs<'_>,
) -> Result<()> {
    let manifest = render_tenant_namespace(
        namespace,
        &crate::project_namespace::SafeLabel::new(tenant, 63),
        cidrs,
    );
    kube.apply_yaml(&manifest).await?;
    // Register the namespace → tenant mapping in the same step
    // as the kubectl apply. The broker's TokenReview path uses
    // this registry as the SoT (no string parsing). Writing it
    // here means every namespace the dispatcher creates has a
    // row by the time any pod inside it can authenticate.
    crate::namespace_registry::register(pool, namespace, tenant).await
}

/// Render + apply the per-tenant supervisor Deployment. Idempotent
/// via `kubectl apply` on a deterministic name; safe to call on
/// every project register.
pub async fn ensure_supervisor_deployment(
    kube: &dyn KubeClient,
    namespace: &str,
    tenant: &str,
    supervisor_image: &str,
) -> Result<()> {
    let manifest = render_supervisor_deployment(
        namespace,
        &crate::project_namespace::SafeLabel::new(tenant, 63),
        supervisor_image,
    );
    kube.apply_yaml(&manifest).await
}

pub fn render_supervisor_deployment(
    namespace: &str,
    // SafeLabel: the type forces sanitization before this id reaches
    // the manifest (closes the cloud free-form-tenant injection seam).
    tenant: &crate::project_namespace::SafeLabel,
    supervisor_image: &str,
) -> String {
    format!(
        r#"---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: weft-infra-supervisor
  namespace: {namespace}
  labels:
    weft.dev/role: infra-supervisor
    weft.dev/tenant: "{tenant}"
spec:
  replicas: 1
  selector:
    matchLabels:
      weft.dev/role: infra-supervisor
      weft.dev/tenant: "{tenant}"
  template:
    metadata:
      labels:
        weft.dev/role: infra-supervisor
        weft.dev/tenant: "{tenant}"
    spec:
      serviceAccountName: weft-infra-supervisor-sa
      containers:
        - name: supervisor
          image: {supervisor_image}
          imagePullPolicy: IfNotPresent
          env:
            - name: WEFT_TENANT_ID
              value: "{tenant}"
            - name: WEFT_BROKER_URL
              value: "http://weft-broker.weft-db.svc.cluster.local:9090"
            - name: WEFT_BROKER_TOKEN_PATH
              value: "/var/run/weft/sa/token"
            - name: WEFT_POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
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

pub fn render_tenant_namespace(
    namespace: &str,
    // SafeLabel: forces sanitization before interpolation (see
    // `render_supervisor_deployment`).
    tenant: &crate::project_namespace::SafeLabel,
    cidrs: ClusterCidrs<'_>,
) -> String {
    let pod_cidr = cidrs.pod_cidr;
    let service_cidr = cidrs.service_cidr;
    let ingress_ns = cidrs.ingress_namespace;
    format!(
        r#"---
apiVersion: v1
kind: Namespace
metadata:
  name: {namespace}
  labels:
    weft.dev/role: tenant
    weft.dev/tenant: "{tenant}"
    kubernetes.io/metadata.name: {namespace}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: weft-listener-sa
  namespace: {namespace}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: weft-infra-supervisor-sa
  namespace: {namespace}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: weft-storage-sa
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
  name: listener-policy
  namespace: {namespace}
spec:
  podSelector:
    matchLabels:
      weft.dev/role: listener
  policyTypes:
    - Ingress
    - Egress
  ingress:
    # Dispatcher (weft-system, /register, /process).
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: weft-system
          podSelector:
            matchLabels:
              weft.dev/role: dispatcher
      ports:
        - protocol: TCP
          port: 8080
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
    # DNS.
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
    # ANY namespace belonging to this tenant (project namespaces are
    # labeled `weft.dev/tenant=<tenant>`). The listener subscribes to
    # SSE endpoints exposed by infra pods in the tenant's project
    # namespaces.
    - to:
        - namespaceSelector:
            matchLabels:
              weft.dev/tenant: "{tenant}"
    # Internet (for kinds whose URL is external, e.g. third-party SSE).
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
  name: supervisor-policy
  namespace: {namespace}
spec:
  podSelector:
    matchLabels:
      weft.dev/role: infra-supervisor
  policyTypes:
    - Ingress
    - Egress
  ingress:
    # The supervisor doesn't serve HTTP: no ingress allowed.
  egress:
    # Broker.
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
    # DNS.
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
    # k8s API server (kubernetes.default.svc, in `default` namespace).
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: default
      ports:
        - protocol: TCP
          port: 443
    # ANY namespace belonging to this tenant. The supervisor's
    # kube-rs watches + infra-pod HTTP probes need cross-ns egress
    # into the tenant's project namespaces.
    - to:
        - namespaceSelector:
            matchLabels:
              weft.dev/tenant: "{tenant}"
  # Ingress namespace not needed; the supervisor doesn't talk to
  # ingress controllers.
---
# Cross-ns egress controls happen via NetworkPolicy above. The
# ingress controller's namespace label `{ingress_ns}` is referenced
# from project namespace `infra-policy` (for `Expose::TenantPublic`
# endpoints), not from this tenant namespace.
"#,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cidrs() -> ClusterCidrs<'static> {
        ClusterCidrs {
            pod_cidr: "10.244.0.0/16",
            service_cidr: "10.96.0.0/12",
            ingress_namespace: "ingress-nginx",
        }
    }

    fn sl(s: &str) -> crate::project_namespace::SafeLabel {
        crate::project_namespace::SafeLabel::new(s, 63)
    }

    #[test]
    fn renders_namespace_and_service_accounts() {
        let yaml = render_tenant_namespace("wm-tenant-alice", &sl("alice"), cidrs());
        assert!(yaml.contains("kind: Namespace"));
        assert!(yaml.contains("name: wm-tenant-alice"));
        // Tenant namespace holds listener + supervisor SAs ONLY.
        // Worker / infra SAs are in project namespaces.
        assert!(yaml.contains("name: weft-listener-sa"));
        assert!(yaml.contains("name: weft-infra-supervisor-sa"));
        assert!(yaml.contains("name: weft-storage-sa"));
        assert!(!yaml.contains("name: weft-worker-sa"));
        assert!(!yaml.contains("name: weft-infra-sa"));
    }

    #[test]
    fn renders_supervisor_and_listener_policies() {
        let yaml = render_tenant_namespace("wm-tenant-alice", &sl("alice"), cidrs());
        assert!(yaml.contains("name: listener-policy"));
        assert!(yaml.contains("name: supervisor-policy"));
    }

    #[test]
    fn supervisor_policy_egress_into_tenant_projects() {
        let yaml = render_tenant_namespace("wm-tenant-alice", &sl("alice"), cidrs());
        // The supervisor's egress allows tenant-scoped cross-ns reach
        // via namespaceSelector on `weft.dev/tenant=<tenant>`.
        assert!(yaml.contains("weft.dev/tenant: \"alice\""));
    }

    #[test]
    fn listener_policy_egress_into_tenant_projects() {
        let yaml = render_tenant_namespace("wm-tenant-alice", &sl("alice"), cidrs());
        // Same label-selector trick for the listener (SSE
        // subscriptions to project-namespace infra pods).
        let listener_section = yaml
            .split("name: listener-policy")
            .nth(1)
            .expect("listener-policy section");
        assert!(listener_section.contains("weft.dev/tenant: \"alice\""));
    }

    #[test]
    fn tenant_label_stamped_on_namespace() {
        let yaml = render_tenant_namespace("wm-tenant-alice", &sl("alice"), cidrs());
        // The namespace itself carries the tenant label so external
        // selectors (project-namespace policies) can match it.
        assert!(yaml.contains("weft.dev/role: tenant"));
        assert!(yaml.contains("weft.dev/tenant: \"alice\""));
    }

    #[test]
    fn supervisor_render_includes_namespace_and_image() {
        let yaml = render_supervisor_deployment(
            "wm-alice",
            &sl("alice"),
            "weft-infra-supervisor:local",
        );
        assert!(yaml.contains("namespace: wm-alice"));
        assert!(yaml.contains("image: weft-infra-supervisor:local"));
        assert!(yaml.contains("weft-infra-supervisor-sa"));
    }

    #[test]
    fn supervisor_render_carries_tenant_label() {
        let yaml = render_supervisor_deployment("wm-alice", &sl("alice"), "img:1");
        assert!(yaml.contains("weft.dev/role: infra-supervisor"));
        assert!(yaml.contains("weft.dev/tenant: \"alice\""));
    }

    #[test]
    fn supervisor_render_injects_tenant_env() {
        let yaml = render_supervisor_deployment("wm-alice", &sl("alice"), "img:1");
        assert!(yaml.contains("name: WEFT_TENANT_ID"));
        assert!(yaml.contains("value: \"alice\""));
        assert!(yaml.contains("name: WEFT_BROKER_URL"));
    }

    #[test]
    fn supervisor_render_mounts_projected_sa_token() {
        let yaml = render_supervisor_deployment("wm-alice", &sl("alice"), "img:1");
        assert!(yaml.contains("audience: weft-broker"));
        assert!(yaml.contains("mountPath: /var/run/weft/sa"));
    }
}
