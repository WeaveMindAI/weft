//! The shared worker namespace bundle.
//!
//! One namespace (`project_namespace::SHARED_WORKER_NAMESPACE`) holds
//! every NO-INFRA project's worker, across all tenants. A no-infra
//! project never gets its own namespace (that would burn the cluster's
//! namespace ceiling for a project that needs no infra pods next to its
//! worker), so its worker runs here instead.
//!
//! Created once, lazily, the first time a no-infra worker is placed
//! (`ensure`, called from the worker spawn path). Never torn down: it is
//! cluster-singleton infrastructure, not project- or tenant-scoped.
//!
//! Holds:
//!   - the namespace itself
//!   - ServiceAccount `weft-worker-sa` (the SA every worker pod runs as)
//!   - NetworkPolicies:
//!       - `deny-pod-to-pod`: the blanket cross-tenant isolation rule.
//!         Workers from different tenants share this namespace, so the
//!         primary, explicit rule is that NO pod here may reach any
//!         other pod here. Workers never talk to each other.
//!       - `worker-egress`: the egress a worker legitimately needs
//!         (broker, DNS, every tenant's storage box, internet). Same
//!         allowances as the per-project `worker-policy`, minus the
//!         same-namespace-infra rule (there is no infra here).
//!       - `worker-ingress`: only the gateway may reach a worker's
//!         connection port (live caller connections). The signed routing
//!         token is the second gate inside the worker.
//!
//! It does NOT register a `weft_namespace_tenant` row: this namespace
//! maps to no single tenant. A worker here resolves its tenant from its
//! own pod identity (`worker_pod` row) in the broker, NOT from the
//! namespace. See `weft_broker::auth`.

use anyhow::Result;
use weft_platform_traits::KubeClient;

use crate::project_namespace::SHARED_WORKER_NAMESPACE;

pub struct SharedWorkerNamespaceArgs<'a> {
    /// Pod CIDR for NetworkPolicy egress exclusions.
    pub pod_cidr: &'a str,
    /// Service CIDR for the same purpose.
    pub service_cidr: &'a str,
}

/// Apply the shared-worker-namespace bundle. Idempotent (kubectl
/// apply). Unlike the tenant / project namespaces, this writes NO
/// namespace-registry row: the namespace has no single owning tenant,
/// so worker auth resolves the tenant per-pod (via the `worker_pod`
/// row) instead of per-namespace.
pub async fn ensure(
    kube: &dyn KubeClient,
    args: &SharedWorkerNamespaceArgs<'_>,
) -> Result<()> {
    let manifest = render(args);
    kube.apply_yaml(&manifest).await
}

pub fn render(args: &SharedWorkerNamespaceArgs<'_>) -> String {
    let SharedWorkerNamespaceArgs {
        pod_cidr,
        service_cidr,
    } = args;
    let namespace = SHARED_WORKER_NAMESPACE;
    // Pull ports/namespaces from the one constant each so they can't
    // drift from the worker pod manifest / storage box Service.
    let storage_port = weft_storage::config::STORAGE_PORT;
    let gateway_namespace = crate::backend::k8s_worker::GATEWAY_NAMESPACE;
    let connection_port = crate::backend::k8s_worker::WORKER_CONNECTION_PORT;
    format!(
        r#"---
apiVersion: v1
kind: Namespace
metadata:
  name: {namespace}
  labels:
    weft.dev/role: shared-workers
    kubernetes.io/metadata.name: {namespace}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: weft-worker-sa
  namespace: {namespace}
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: deny-pod-to-pod
  namespace: {namespace}
spec:
  # Applies to every pod in the namespace. The blanket cross-tenant
  # isolation rule: no pod here may send to or receive from any other
  # pod here. (worker-egress / worker-ingress below re-open ONLY the
  # cross-namespace traffic a worker needs; an empty rule set on a
  # policyType means "deny", and NetworkPolicies are additive, so the
  # only intra-namespace traffic allowed is what some policy explicitly
  # permits, which is none.)
  podSelector: {{}}
  policyTypes:
    - Ingress
    - Egress
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: worker-ingress
  namespace: {namespace}
spec:
  podSelector:
    matchLabels:
      weft.dev/role: worker
  policyTypes:
    - Ingress
  ingress:
    # Live caller connections: the gateway forwards a routed caller to
    # the worker's connection port. Only the gateway namespace may reach
    # workers (callers never touch a worker directly; the signed routing
    # token is the second gate inside the worker). NOT a same-namespace
    # rule: the gateway is in its own namespace, so this does not weaken
    # the blanket pod-to-pod deny between workers.
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {gateway_namespace}
      ports:
        - protocol: TCP
          port: {connection_port}
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: worker-egress
  namespace: {namespace}
spec:
  podSelector:
    matchLabels:
      weft.dev/role: worker
  policyTypes:
    - Egress
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
    # Storage boxes: a worker here may belong to ANY tenant, so it must
    # reach ANY tenant's storage box (selected by the storage role in any
    # tenant namespace). Network reachability alone grants nothing: the
    # box authenticates every request against the caller's broker-issued
    # SA token and rejects a wrong-tenant token, the same posture the
    # per-project worker-policy relies on.
    - to:
        - namespaceSelector:
            matchLabels:
              weft.dev/role: tenant
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
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered() -> String {
        render(&SharedWorkerNamespaceArgs {
            pod_cidr: "10.244.0.0/16",
            service_cidr: "10.96.0.0/12",
        })
    }

    #[test]
    fn render_creates_namespace_and_worker_sa() {
        let yaml = rendered();
        assert!(yaml.contains(&format!("name: {SHARED_WORKER_NAMESPACE}")));
        assert!(yaml.contains("name: weft-worker-sa"));
    }

    #[test]
    fn render_blanket_pod_to_pod_deny() {
        let yaml = rendered();
        // The deny policy selects ALL pods (empty podSelector) and names
        // both policy types with no allow rules, the blanket isolation.
        assert!(yaml.contains("name: deny-pod-to-pod"));
        // The deny policy's selector is the empty one.
        let deny = yaml
            .split("name: deny-pod-to-pod")
            .nth(1)
            .expect("deny policy present");
        assert!(deny.contains("podSelector: {}"), "deny selects all pods");
    }

    #[test]
    fn render_worker_egress_allows_broker_dns_storage_internet() {
        let yaml = rendered();
        assert!(yaml.contains("weft.dev/role: broker"));
        assert!(yaml.contains("port: 53"));
        assert!(yaml.contains("weft.dev/role: storage"));
        assert!(yaml.contains("cidr: 0.0.0.0/0"));
    }

    #[test]
    fn render_storage_egress_spans_all_tenants_not_one_namespace() {
        let yaml = rendered();
        // Storage egress selects tenant namespaces by ROLE, not by a
        // single namespace name: a shared-ns worker may belong to any
        // tenant, so it must reach any tenant's box.
        let egress = yaml
            .split("name: worker-egress")
            .nth(1)
            .expect("egress policy present");
        assert!(
            egress.contains("weft.dev/role: tenant"),
            "storage egress selects tenant namespaces by role"
        );
    }

    #[test]
    fn render_no_infra_artifacts() {
        let yaml = rendered();
        // No infra SA, no infra policy, no supervisor RoleBinding: there
        // is never infra in the shared namespace.
        assert!(!yaml.contains("weft-infra-sa"));
        assert!(!yaml.contains("infra-policy"));
        assert!(!yaml.contains("weft-infra-supervisor"));
    }

    #[test]
    fn render_only_gateway_reaches_worker_ingress() {
        let yaml = rendered();
        let ingress = yaml
            .split("name: worker-ingress")
            .nth(1)
            .expect("ingress policy present");
        assert!(ingress.contains(crate::backend::k8s_worker::GATEWAY_NAMESPACE));
    }
}
