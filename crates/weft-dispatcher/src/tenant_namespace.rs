//! Render and apply the per-tenant namespace bundle: namespace
//! itself, three role-scoped ServiceAccounts, default-deny + per-role
//! NetworkPolicies. Idempotent: `kubectl apply` reconciles whatever
//! state the cluster already has.
//!
//! Called from the project-register path the first time the
//! dispatcher sees a new tenant; the static `user-namespace.yaml`
//! local-dev manifest pre-creates the same bundle for `wm-local` so
//! a fresh install works without booting the dispatcher first.

use anyhow::Result;
use tokio::process::Command;

pub struct ClusterCidrs<'a> {
    pub pod_cidr: &'a str,
    pub service_cidr: &'a str,
    pub ingress_namespace: &'a str,
}

pub async fn ensure_tenant_namespace(
    namespace: &str,
    tenant: &str,
    cidrs: ClusterCidrs<'_>,
) -> Result<()> {
    let manifest = render_tenant_namespace(namespace, tenant, cidrs);
    apply(&manifest).await
}

pub fn render_tenant_namespace(
    namespace: &str,
    tenant: &str,
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
  name: weft-worker-sa
  namespace: {namespace}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: weft-sidecar-sa
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
    # Only the dispatcher in weft-system. AND'd inside one `from:`.
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
    # Broker for journal / signal / task ops.
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
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
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
  name: worker-policy
  namespace: {namespace}
spec:
  podSelector:
    matchLabels:
      weft.dev/role: worker
  policyTypes:
    - Ingress
    - Egress
  egress:
    # Broker only.
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
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
    # Sidecars in this same tenant namespace.
    - to:
        - podSelector:
            matchLabels:
              weft.dev/role: sidecar
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
  name: sidecar-policy
  namespace: {namespace}
spec:
  podSelector:
    matchLabels:
      weft.dev/role: sidecar
  policyTypes:
    - Ingress
    - Egress
  ingress:
    # Tenant workers (in this same namespace).
    - from:
        - podSelector:
            matchLabels:
              weft.dev/role: worker
    # ONLY the dispatcher pod from weft-system: namespace + pod
    # selector AND'd inside one `from:` entry (two list items inside
    # one rule are AND'd; two top-level rules are OR'd). Without the
    # AND, every pod in weft-system reaches the sidecar, including
    # the broker (which is a credentialed attacker target if pwned
    # and has no business calling tenant sidecars).
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: weft-system
          podSelector:
            matchLabels:
              weft.dev/role: dispatcher
    # Ingress controller for public webhooks / browser session
    # streams. Restricted to the actual ingress pods, not "any pod
    # in the ingress namespace" (which often includes admission
    # controllers etc).
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {ingress_ns}
          podSelector:
            matchLabels:
              app.kubernetes.io/name: ingress-nginx
  egress:
    # Internet, but not other Pods or Services in this cluster: a
    # compromised sidecar must not be able to reach other tenant
    # namespaces or the broker via cluster-internal addresses.
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - {pod_cidr}
              - {service_cidr}
"#,
    )
}

async fn apply(manifest: &str) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut child = Command::new("kubectl")
        .args(["apply", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(manifest.as_bytes()).await?;
    }
    let output = child.wait_with_output().await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("kubectl apply (tenant namespace) failed: {stderr}");
    }
    Ok(())
}
