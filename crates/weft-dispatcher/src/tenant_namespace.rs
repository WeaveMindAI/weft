//! Per-tenant k8s namespace bundle.
//!
//! Creates the tenant namespace itself + its default-deny
//! NetworkPolicies, and registers the namespace -> tenant mapping the
//! broker authenticates against. The pooled listener and infra-
//! supervisor pods do NOT live here: they are tenant-agnostic and run
//! in the control-plane namespace, spawned by their respective pools
//! (`listener.rs`, `supervisor_pool.rs`). Workers + user infra pods
//! live in PROJECT namespaces (`project_namespace.rs`).
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

pub fn render_tenant_namespace(
    namespace: &str,
    // SafeLabel: forces sanitization before the tenant string is
    // interpolated into the rendered manifest.
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
# No listener / supervisor NetworkPolicies here: those pods are
# pooled, trusted, tenant-agnostic services that run in the CONTROL-
# PLANE namespace, not in tenant namespaces. Their network rules live
# in `deploy/k8s/system-namespace.yaml`; their access INTO this
# tenant's project namespaces is granted by the project-namespace
# RoleBindings + NetworkPolicies (project_namespace.rs), which target
# the control-plane namespace. The `{ingress_ns}` /
# `{pod_cidr}` / `{service_cidr}` knobs remain referenced by the
# project-namespace policies, not here.
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
        // Tenant namespace holds ONLY the storage SA now. The listener
        // and supervisor are pooled, tenant-agnostic services in the
        // control-plane namespace, so their SAs live there (see
        // system-namespace.yaml), not per tenant.
        assert!(yaml.contains("name: weft-storage-sa"));
        assert!(!yaml.contains("name: weft-listener-sa"));
        assert!(!yaml.contains("name: weft-infra-supervisor-sa"));
        assert!(!yaml.contains("name: weft-worker-sa"));
        assert!(!yaml.contains("name: weft-infra-sa"));
    }

    #[test]
    fn omits_listener_and_supervisor_policies() {
        // Pooled listener/supervisor run in the control-plane namespace;
        // their network rules are there, not in tenant namespaces.
        let yaml = render_tenant_namespace("wm-tenant-alice", &sl("alice"), cidrs());
        assert!(!yaml.contains("name: listener-policy"));
        assert!(!yaml.contains("name: supervisor-policy"));
        assert!(!yaml.contains("weft.dev/role: listener"));
        assert!(!yaml.contains("weft.dev/role: infra-supervisor"));
    }

    #[test]
    fn tenant_label_stamped_on_namespace() {
        let yaml = render_tenant_namespace("wm-tenant-alice", &sl("alice"), cidrs());
        // The namespace itself carries the tenant label so external
        // selectors (project-namespace policies) can match it.
        assert!(yaml.contains("weft.dev/role: tenant"));
        assert!(yaml.contains("weft.dev/tenant: \"alice\""));
    }

}
