//! `weft daemon start|stop|status|restart|logs`. Owns the kind
//! cluster lifecycle and the dispatcher deployment inside it.
//!
//! Everything runs as Pods: dispatcher, listener, worker, infra.
//! `start` uses `kind` to host the cluster and `kind load docker-image`
//! to fill the image cache without a registry push; a registry-backed
//! cluster applies the same manifests with images pulled from the
//! registry instead.

use std::fs;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::Command;
use tokio::time::sleep;

use super::Ctx;
use crate::images;

/// Cluster / namespace / image config the CLI talks to.
///
/// Two namespace concepts: `system_namespace` (where the
/// dispatcher Pod, its Service, PVC and Ingress live) and
/// `default_user_namespace` (where workers, listeners, infra
/// for tenant `local` run). User namespaces are per tenant; here
/// there is one, for the `local` tenant.
pub struct ClusterConfig {
    pub cluster_name: String,
    pub kube_context: String,
    pub system_namespace: String,
    pub db_namespace: String,
    pub default_user_namespace: String,
    pub dispatcher_image: String,
    pub listener_image: String,
    pub broker_image: String,
    pub supervisor_image: String,
    pub dispatcher_port: u16,
    /// Local port the daemon forwards the cluster ingress controller
    /// to. Storage file downloads (and any ingress-served URL) are
    /// minted as `http://127.0.0.1:<ingress_port>/...` in local dev,
    /// reached via this forward. Distinct from `dispatcher_port`
    /// (the dispatcher's own API): downloads stream straight from the
    /// storage box through the ingress, never through the dispatcher.
    pub ingress_port: u16,
    /// Local port the daemon forwards the live-connection gateway (Envoy
    /// Gateway) to. A caller's URL is minted as
    /// `http://<pod>.<ns>.<host>:<gateway_port>/...` in local dev and
    /// reached via this forward. Distinct from `ingress_port` (the nginx
    /// ingress for storage downloads); the live gateway is a separate
    /// front door.
    pub gateway_port: u16,
    /// Local port the daemon forwards the bundled SeaweedFS object store to.
    /// Runtime-file downloads are presigned BUCKET urls signed for this
    /// host-reachable address (the broker's in-cluster I/O endpoint is
    /// unreachable from the host / a browser).
    pub seaweed_port: u16,
    /// Cluster Service CIDR. The apiserver's ClusterIP lives in this
    /// range; the broker NetworkPolicy allows TokenReview egress to
    /// it, and the dispatcher gets it as env. kind's default is
    /// `10.96.0.0/12`; a non-kind operator sets WEFT_CLUSTER_SERVICE_CIDR.
    pub service_cidr: String,
    /// Cluster Pod CIDR. Passed to the dispatcher for NetworkPolicy
    /// rendering. kind's default is `10.244.0.0/16`.
    pub pod_cidr: String,
    /// The cluster DNS Service's address, for the public proxy's
    /// nginx `resolver`. Empty means "ask the cluster", which is the
    /// normal path; every Kubernetes distribution names that Service
    /// `kube-system/kube-dns`, and WEFT_CLUSTER_DNS_IP is for one that
    /// does not. Validated as an IP address where the manifests are
    /// rendered, like the CIDRs.
    pub cluster_dns_ip: String,
    /// `kind` for local dev (uses `kind create` + `kind load`);
    /// `k8s` for targeting an external cluster (skips kind
    /// bootstrap, images come from whatever registry the
    /// cluster can pull from).
    pub backend: ClusterBackend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterBackend {
    Kind,
    K8s,
}

/// Resolved once per process. Reads env vars, caches the
/// result so repeated reads don't fan out to the OS.
pub fn cluster_config() -> &'static ClusterConfig {
    use std::sync::OnceLock;
    static CFG: OnceLock<ClusterConfig> = OnceLock::new();
    CFG.get_or_init(ClusterConfig::from_env)
}

impl ClusterConfig {
    pub fn from_env() -> Self {
        let cluster_name = std::env::var("WEFT_CLUSTER_NAME")
            .unwrap_or_else(|_| "weft-local".into());
        let kube_context = std::env::var("WEFT_KUBE_CONTEXT")
            .unwrap_or_else(|_| format!("kind-{cluster_name}"));
        let system_namespace = std::env::var("WEFT_SYSTEM_NAMESPACE")
            .unwrap_or_else(|_| "weft-system".into());
        let db_namespace = std::env::var("WEFT_DB_NAMESPACE")
            .unwrap_or_else(|_| "weft-db".into());
        let default_user_namespace = std::env::var("WEFT_DEFAULT_USER_NAMESPACE")
            .unwrap_or_else(|_| "wft-local".into());
        let dispatcher_image = std::env::var("WEFT_DISPATCHER_IMAGE")
            .unwrap_or_else(|_| "weft-dispatcher:local".into());
        let listener_image = std::env::var("WEFT_LISTENER_IMAGE")
            .unwrap_or_else(|_| "weft-listener:local".into());
        let broker_image = std::env::var("WEFT_BROKER_IMAGE")
            .unwrap_or_else(|_| "weft-broker:local".into());
        let supervisor_image = std::env::var("WEFT_SUPERVISOR_IMAGE")
            .unwrap_or_else(|_| "weft-infra-supervisor:local".into());
        let dispatcher_port = std::env::var("WEFT_DISPATCHER_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(9999);
        let ingress_port = std::env::var("WEFT_INGRESS_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(9998);
        let gateway_port = std::env::var("WEFT_GATEWAY_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(9097);
        let seaweed_port = std::env::var("WEFT_SEAWEED_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(9096);
        // Raw here; validated where they're consumed (the manifest
        // apply, `apply_static_manifests`), not at config load: a
        // malformed CIDR should fail the apply loudly, not break
        // unrelated commands (stop / logs / rm) that read this config
        // but never touch the CIDRs.
        let service_cidr = std::env::var("WEFT_CLUSTER_SERVICE_CIDR")
            .unwrap_or_else(|_| "10.96.0.0/12".into());
        let pod_cidr = std::env::var("WEFT_CLUSTER_POD_CIDR")
            .unwrap_or_else(|_| "10.244.0.0/16".into());
        let cluster_dns_ip =
            std::env::var("WEFT_CLUSTER_DNS_IP").unwrap_or_default().trim().to_string();
        let backend = match std::env::var("WEFT_CLUSTER_BACKEND")
            .as_deref()
            .ok()
        {
            Some("k8s") => ClusterBackend::K8s,
            _ => ClusterBackend::Kind,
        };
        Self {
            cluster_name,
            kube_context,
            system_namespace,
            db_namespace,
            default_user_namespace,
            dispatcher_image,
            listener_image,
            broker_image,
            supervisor_image,
            dispatcher_port,
            ingress_port,
            gateway_port,
            seaweed_port,
            service_cidr,
            pod_cidr,
            cluster_dns_ip,
            backend,
        }
    }
}

/// Validate a CIDR string. Always rejects unparseable values; when
/// `strict` (the security-critical Service CIDR), also rejects ranges
/// broad enough to admit public addresses, since that CIDR scopes the
/// broker's apiserver-egress NetworkPolicy.
fn check_cidr(raw: &str, strict: bool) -> std::result::Result<(), String> {
    let net: ipnet::IpNet = raw.parse().map_err(|e| format!("not a valid CIDR: {e}"))?;
    if strict {
        // The apiserver ClusterIP lives in a private Service CIDR.
        // Refuse anything that isn't an RFC1918 / private range, or
        // whose prefix is so short it admits public space (e.g.
        // `0.0.0.0/0`, `10.0.0.0/4`). A cluster Service CIDR is always
        // a tight private block (kind: 10.96.0.0/12); a broad one here
        // would let the broker egress to the public internet.
        let too_broad = match net {
            ipnet::IpNet::V4(n) => n.prefix_len() < 8 || !is_private_v4(n.network()),
            ipnet::IpNet::V6(n) => n.prefix_len() < 8 || !n.network().is_unique_local(),
        };
        if too_broad {
            return Err(
                "too broad / not a private range; the broker's apiserver-egress \
                 NetworkPolicy is scoped to this CIDR and a broad value would open the \
                 broker's egress to public addresses. Use the cluster's actual (private) \
                 Service CIDR."
                    .to_string(),
            );
        }
    }
    Ok(())
}

/// RFC1918 + CGNAT private IPv4 ranges (10/8, 172.16/12, 192.168/16,
/// 100.64/10). A cluster Service CIDR is always one of these.
fn is_private_v4(ip: std::net::Ipv4Addr) -> bool {
    ip.is_private() || matches!(ip.octets(), [100, b, ..] if (64..=127).contains(&b))
}

/// Validate cluster inputs and return the `${VAR}` substitution
/// pairs for `kubectl_apply_*`: the CIDRs + derived apiserver
/// ClusterIP (broker egress NetworkPolicy scope), plus the
/// dispatcher's public base URL + local-dev flag (storage download /
/// webhook addressing). The ONLY way to get the substitution vars,
/// so a manifest carrying a placeholder can never be applied without
/// the CIDRs having passed validation first. Validated at the apply
/// boundary, not at config load, so commands that don't apply
/// manifests (stop / logs / rm) aren't gated on the env being
/// well-formed.
///
/// Public base URL policy:
/// - Kind (local dev): default to `http://127.0.0.1:<ingress_port>`
///   (the daemon forwards the cluster ingress there) and set
///   WEFT_LOCAL_DEV=1 so the dispatcher accepts the loopback host.
///   An operator may still override WEFT_DISPATCHER_PUBLIC_BASE_URL.
/// - K8s (real cluster): the operator MUST set
///   WEFT_DISPATCHER_PUBLIC_BASE_URL to the external ingress host;
///   WEFT_LOCAL_DEV is empty, so a loopback there fails loud.
/// - WEFT_DISPATCHER_INTERNET_URL is the ADDITIONAL internet-reachable
///   address (the public tunnel mints it), never a replacement: the
///   base URL keeps every local surface stable, and only the surfaces
///   the open internet must reach prefer this one.
async fn manifest_template_vars(
    cfg: &ClusterConfig,
    internet_url: Option<&str>,
) -> Result<Vec<(&'static str, String)>> {
    check_cidr(&cfg.service_cidr, true)
        .map_err(|e| anyhow::anyhow!("WEFT_CLUSTER_SERVICE_CIDR='{}': {e}", cfg.service_cidr))?;
    check_cidr(&cfg.pod_cidr, false)
        .map_err(|e| anyhow::anyhow!("WEFT_CLUSTER_POD_CIDR='{}': {e}", cfg.pod_cidr))?;
    let apiserver_ip = apiserver_clusterip(&cfg.service_cidr)
        .map_err(|e| anyhow::anyhow!("WEFT_CLUSTER_SERVICE_CIDR='{}': {e}", cfg.service_cidr))?;

    // Object-store slot: the broker's runtime-file plane (`ctx.storage`) writes
    // bytes to this bucket, and workers read/write it DIRECTLY via presigned URLs.
    // The store is ALWAYS external to the cluster, reached over S3 (endpoint from
    // env), so both endpoints below reach OUT of the cluster. An operator can
    // override any of these via env for their own S3.
    //
    // INTERNAL endpoint (the broker's I/O + the host pods reach): the object store
    // container runs on the HOST, and in-cluster pods reach the host via the kind
    // network's gateway IP. This is the host string presigned Internal URLs are
    // signed for, and the host pods then connect to (the two must match for SigV4).
    let object_store_endpoint = match std::env::var("WEFT_OBJECT_STORE_ENDPOINT") {
        Ok(v) => v,
        Err(_) => format!("http://{}:{}", kind_network_gateway_ipv4().await?, cfg.seaweed_port),
    };

    // The broker's egress denies every private range so an access
    // outbound call reaches external services only. An object store
    // that sits on a private address needs exactly that address
    // re-permitted, and the endpoint above already names it: derive
    // the /32 from it, so the opening can never drift from the store
    // the broker actually dials. A store reached over a public
    // endpoint (or a hostname) derives a benign public /32 that
    // re-permits nothing (a hostname that resolves privately gets an
    // informational pointer at the override). An IPv6 host cannot be
    // expressed as a derived IPv4 /32 at all, so it fails loud naming
    // WEFT_STORE_ALLOW_CIDR, which overrides every derivation.
    let store_allow_cidr = match std::env::var("WEFT_STORE_ALLOW_CIDR") {
        Ok(v) => v,
        Err(_) => match endpoint_host(&object_store_endpoint) {
            EndpointHost::PrivateIpv4(ip) => format!("{ip}/32"),
            EndpointHost::PublicIpv4 => "192.0.2.0/32".into(),
            EndpointHost::Hostname(host) => {
                // The lookup only feeds an advisory note; a stalled
                // resolver must never stall daemon start, so it gets
                // a short deadline and a miss just skips the note.
                if let Ok(Ok(addrs)) = tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    tokio::net::lookup_host((host.as_str(), 0)),
                )
                .await
                {
                    let private: Vec<String> = addrs
                        .filter_map(|a| match a.ip() {
                            std::net::IpAddr::V4(ip) if is_private_ipv4(ip) => {
                                Some(ip.to_string())
                            }
                            _ => None,
                        })
                        .collect();
                    if !private.is_empty() {
                        println!(
                            "note: the object store host '{host}' resolves to private \
                             address(es) {}; the broker's egress will NOT reach them \
                             unless you set WEFT_STORE_ALLOW_CIDR to the store's range",
                            private.join(", ")
                        );
                    }
                }
                "192.0.2.0/32".into()
            }
            EndpointHost::Ipv6(host) => anyhow::bail!(
                "the object store endpoint host '{host}' is an IPv6 address, which the \
                 derived egress opening cannot express; set WEFT_STORE_ALLOW_CIDR to \
                 the store's IPv6 range instead"
            ),
        },
    };
    check_cidr(&store_allow_cidr, false)
        .map_err(|e| anyhow::anyhow!("WEFT_STORE_ALLOW_CIDR='{store_allow_cidr}': {e}"))?;

    // The one extra private CIDR a listener's watch URLs may dial. On
    // kind it defaults to the operator's own machine (the docker
    // network gateway), so a trigger can watch a service running right
    // there; on a real cluster it defaults to a benign unused /32.
    let listener_allow_cidr = match std::env::var("WEFT_LISTENER_ALLOW_CIDR") {
        Ok(v) => v,
        Err(_) => match cfg.backend {
            ClusterBackend::Kind => format!("{}/32", kind_network_gateway_ipv4().await?),
            ClusterBackend::K8s => "192.0.2.0/32".into(),
        },
    };
    check_cidr(&listener_allow_cidr, false)
        .map_err(|e| anyhow::anyhow!("WEFT_LISTENER_ALLOW_CIDR='{listener_allow_cidr}': {e}"))?;

    let (public_base_url, local_dev) = match cfg.backend {
        ClusterBackend::Kind => {
            let url = std::env::var("WEFT_DISPATCHER_PUBLIC_BASE_URL")
                .unwrap_or_else(|_| format!("http://127.0.0.1:{}", cfg.ingress_port));
            (url, "1".to_string())
        }
        ClusterBackend::K8s => {
            let url = std::env::var("WEFT_DISPATCHER_PUBLIC_BASE_URL").map_err(|_| {
                anyhow::anyhow!(
                    "WEFT_DISPATCHER_PUBLIC_BASE_URL is required for the k8s backend; \
                     set it to the cluster's external ingress host (e.g. \
                     https://files.example.com)"
                )
            })?;
            (url, String::new())
        }
    };

    // Live connection gateway vars. GATEWAY_HOST is the public wildcard
    // suffix the gateway listener matches (`*.<GATEWAY_HOST>`); the
    // dispatcher prepends `<pod>.<ns>.` to it when minting a caller URL.
    // GATEWAY_BASE_URL is the scheme + that host + the reachable port,
    // also prepended with the pod subdomain by the dispatcher.
    // CALLER_TOKEN_SECRET (hex) is the HMAC both the dispatcher and every
    // worker use for the routing token.
    let (gateway_host, gateway_base_url, caller_token_secret) = match cfg.backend {
        ClusterBackend::Kind => {
            // nip.io wildcard: `<anything>.127-0-0-1.nip.io` -> 127.0.0.1,
            // reached via the daemon's gateway port-forward. A fixed dev
            // secret keeps tokens stable across local restarts.
            let host = std::env::var("WEFT_GATEWAY_HOST")
                .unwrap_or_else(|_| "127-0-0-1.nip.io".to_string());
            let base = std::env::var("WEFT_GATEWAY_BASE_URL")
                .unwrap_or_else(|_| format!("http://{host}:{}", cfg.gateway_port));
            let secret = std::env::var("WEFT_CALLER_TOKEN_SECRET")
                .unwrap_or_else(|_| "6465762d6c6f63616c2d63616c6c65722d746f6b656e2d736563726574".to_string());
            (host, base, secret)
        }
        ClusterBackend::K8s => {
            let host = std::env::var("WEFT_GATEWAY_HOST").map_err(|_| {
                anyhow::anyhow!(
                    "WEFT_GATEWAY_HOST is required for the k8s backend; set it to the \
                     live-connection wildcard host (e.g. live.example.com, with a \
                     `*.live.example.com` DNS record + TLS cert pointed at the gateway)"
                )
            })?;
            let base = std::env::var("WEFT_GATEWAY_BASE_URL")
                .unwrap_or_else(|_| format!("https://{host}"));
            let secret = std::env::var("WEFT_CALLER_TOKEN_SECRET").map_err(|_| {
                anyhow::anyhow!(
                    "WEFT_CALLER_TOKEN_SECRET (hex) is required for the k8s backend; it is \
                     the HMAC the dispatcher signs live-connection routing tokens with"
                )
            })?;
            (host, base, secret)
        }
    };

    let object_store_bucket =
        std::env::var("WEFT_OBJECT_STORE_BUCKET").unwrap_or_else(|_| "weft".to_string());
    let object_store_region =
        std::env::var("WEFT_OBJECT_STORE_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let object_store_access_key =
        std::env::var("WEFT_OBJECT_STORE_ACCESS_KEY").unwrap_or_else(|_| "weft-local".to_string());
    let object_store_secret_key = std::env::var("WEFT_OBJECT_STORE_SECRET_KEY")
        .unwrap_or_else(|_| "weft-local-dev-secret".to_string());
    // PUBLIC endpoint (the browser reaches): the store container is published on the
    // host's loopback too, so the browser hits 127.0.0.1:<seaweed_port> directly.
    // An operator sets this to the S3 store's public URL via env.
    let object_store_public_endpoint = std::env::var("WEFT_OBJECT_STORE_PUBLIC_ENDPOINT")
        .unwrap_or_else(|_| format!("http://127.0.0.1:{}", cfg.seaweed_port));

    Ok(vec![
        ("WEFT_CLUSTER_SERVICE_CIDR", cfg.service_cidr.clone()),
        ("WEFT_CLUSTER_POD_CIDR", cfg.pod_cidr.clone()),
        ("WEFT_STORE_ALLOW_CIDR", store_allow_cidr),
        ("WEFT_LISTENER_ALLOW_CIDR", listener_allow_cidr),
        ("WEFT_APISERVER_CLUSTERIP", apiserver_ip),
        ("WEFT_DISPATCHER_PUBLIC_BASE_URL", public_base_url),
        // The ADDITIONAL internet-reachable address, empty when none:
        // the base URL above stays the stable local address either
        // way. The reconciled tunnel's address arrives as the
        // `internet_url` PARAMETER (a computed value, passed, never
        // smuggled through the process env); the env var remains the
        // operator's seat for a deployment with no tunnel at all.
        (
            "WEFT_DISPATCHER_INTERNET_URL",
            internet_url
                .map(str::to_string)
                .or_else(|| std::env::var("WEFT_DISPATCHER_INTERNET_URL").ok())
                .unwrap_or_default(),
        ),
        ("WEFT_LOCAL_DEV", local_dev),
        ("GATEWAY_HOST", gateway_host),
        ("WEFT_GATEWAY_BASE_URL", gateway_base_url),
        ("WEFT_CALLER_TOKEN_SECRET", caller_token_secret),
        ("WEFT_OBJECT_STORE_ENDPOINT", object_store_endpoint),
        ("WEFT_OBJECT_STORE_BUCKET", object_store_bucket),
        ("WEFT_OBJECT_STORE_REGION", object_store_region),
        ("WEFT_OBJECT_STORE_ACCESS_KEY", object_store_access_key),
        ("WEFT_OBJECT_STORE_SECRET_KEY", object_store_secret_key),
        ("WEFT_OBJECT_STORE_PUBLIC_ENDPOINT", object_store_public_endpoint),
    ])
}

/// The Kubernetes apiserver's ClusterIP: by convention the FIRST
/// usable address of the Service CIDR (kind: 10.96.0.1 for
/// 10.96.0.0/12). The broker's egress NetworkPolicy is scoped to this
/// single /32 so a compromised broker can reach only the apiserver
/// (TokenReview), not every ClusterIP Service in the cluster.
fn apiserver_clusterip(service_cidr: &str) -> std::result::Result<String, String> {
    let net: ipnet::IpNet = service_cidr.parse().map_err(|e| format!("not a valid CIDR: {e}"))?;
    match net {
        ipnet::IpNet::V4(n) => {
            let base = u32::from(n.network());
            Ok(std::net::Ipv4Addr::from(base + 1).to_string())
        }
        ipnet::IpNet::V6(n) => {
            let base = u128::from(n.network());
            Ok(std::net::Ipv6Addr::from(base + 1).to_string())
        }
    }
}

pub enum DaemonAction {
    Start { rebuild: bool, public_url: Option<bool> },
    Stop,
    Status,
    Restart { rebuild: bool, public_url: Option<bool> },
    Logs { tail: usize, follow: bool },
}

pub async fn run(ctx: Ctx, action: DaemonAction) -> Result<()> {
    match action {
        DaemonAction::Start { rebuild, public_url } => {
            set_public_url_choice(public_url)?;
            start(&ctx, rebuild).await
        }
        DaemonAction::Stop => stop().await,
        DaemonAction::Status => status(&ctx).await,
        DaemonAction::Restart { rebuild, public_url } => {
            set_public_url_choice(public_url)?;
            restart(&ctx, rebuild).await
        }
        DaemonAction::Logs { tail, follow } => logs(tail, follow).await,
    }
}

/// `daemon restart` semantics: rebuild images if their inputs
/// changed, then roll the StatefulSet pod ONLY if at least one
/// image changed. If neither image changed AND the daemon is
/// already healthy, this is a true no-op: no pod restart, no
/// port-forward rebuild.
async fn restart(ctx: &Ctx, rebuild: bool) -> Result<()> {
    let cfg = cluster_config();
    require_binary("kubectl").await?;
    require_binary("docker").await?;

    let built = provision_images(cfg, rebuild).await?;
    let BuiltImages {
        dispatcher: dispatcher_built,
        listener: listener_built,
        broker: broker_built,
        supervisor: supervisor_built,
    } = built;

    // The cluster + its ingress controller + the Envoy Gateway controller
    // are infrastructure both `start` and `restart` must GUARANTEE before
    // applying any manifests (gateway.yaml's CRs need Envoy's CRDs;
    // everything needs a cluster to apply into). All three are idempotent:
    // a no-op once present. `restart` runs these (not just `start`) so it
    // is self-healing over a missing/partial cluster, e.g. a fresh machine,
    // a `kind delete`, or `setup.sh` choosing `restart` because the daemon
    // process is alive while its cluster is gone. Without `ensure_cluster`
    // here, `ensure_envoy_gateway` fails with "context kind-weft-local does
    // not exist" against the absent cluster. Mirrors `start`'s ordering.
    if cfg.backend == ClusterBackend::Kind {
        require_binary("kind").await?;
        ensure_cluster(cfg).await?;
        ensure_object_store(cfg).await?;
        ensure_ingress_controller().await?;
        ensure_envoy_gateway().await?;
    }

    // Re-apply k8s manifests on every restart. NetworkPolicy /
    // ClusterRole / SA-label tweaks land via the manifest files in
    // deploy/k8s; without re-applying them on restart, a manifest
    // change picked up only by a fresh `daemon start`. Apply is
    // idempotent: unchanged manifests are no-ops at the
    // kube-apiserver layer (resourceVersion match).
    let manifests_changed = apply_static_manifests(cfg).await?;
    // Re-pack the sealing key on every restart; a change rolls the
    // dispatcher and broker below (env is read at pod start). Provider
    // keys ride the access-apps secret (a mounted file, re-read per
    // lookup: no rollout needed for a key edit).
    let sealing_key_changed = apply_sealing_key_secret(cfg).await?;
    // The OAuth apps file rides along. It mounts as a FILE and is read
    // per lookup: a content EDIT of an already-mounted secret needs no
    // rollout (the kubelet refreshes the mount in place), but a secret
    // CREATED after the broker pod started never attaches to the
    // running pod (optional secret volumes are not retroactively
    // mounted), so a change here rolls the broker below.
    let apps_changed = apply_access_apps_secret(cfg).await?;

    // The apps file mounts on the BROKER alone, so on its own it rolls
    // only the broker (below, after this block); everything else here
    // needs the dispatcher-first rollout order.
    if dispatcher_built
        || listener_built
        || broker_built
        || supervisor_built
        || manifests_changed
        || sealing_key_changed
    {
        if cfg.backend == ClusterBackend::Kind {
            // System tags are reused (`:local`), so tag presence on the
            // kind node DOESN'T imply matching content. Use the _force
            // variant so a freshly-built image actually replaces the
            // stale one inside the node. Load all in parallel:
            // kind-load is independent per image and the kind node
            // tolerates concurrent loads.
            tokio::try_join!(
                images::kind_load_force(&cfg.cluster_name, &cfg.dispatcher_image),
                images::kind_load_force(&cfg.cluster_name, &cfg.listener_image),
                images::kind_load_force(&cfg.cluster_name, &cfg.broker_image),
                images::kind_load_force(&cfg.cluster_name, &cfg.supervisor_image),
            )?;
        }
        // Roll the dispatcher pod so it picks up the new image OR
        // the new manifest (e.g. an updated env var or resource
        // limit). The port-forward is bound to a single Pod IP, so
        // a Pod recreate kills it; we refresh it after the rollout.
        let status = kubectl(&[
            "-n",
            &cfg.system_namespace,
            "rollout",
            "restart",
            "statefulset/weft-dispatcher",
        ])
        .status()
        .await?;
        if !status.success() {
            anyhow::bail!("rollout restart failed");
        }
        wait_for_statefulset_ready("weft-dispatcher").await?;
        kill_existing_port_forwards();
        start_port_forwards().await?;
        wait_for_http(&format!("http://127.0.0.1:{}/health", cfg.dispatcher_port)).await?;
        // Once the dispatcher is back up, roll the dependent
        // deployments concurrently. They are independent rollouts
        // against different controllers, so wall-clock is bounded
        // by the slowest:
        //   - listener: every pooled listener Deployment the dispatcher
        //     created dynamically. Without this they stay on the old
        //     image and break the dispatcher<->listener wire contract.
        //   - broker: a single Deployment under weft-db.
        //   - supervisor: every pooled infra-supervisor Deployment the
        //     dispatcher created dynamically (same rationale as listeners).
        let db_namespace = cfg.db_namespace.to_string();
        let broker_rollout = async move {
            if broker_built || sealing_key_changed || apps_changed {
                let _ = kubectl(&[
                    "-n", &db_namespace, "rollout", "restart", "deployment/weft-broker",
                ])
                .status()
                .await;
            }
            Ok::<(), anyhow::Error>(())
        };
        let listener_rollout = async {
            if listener_built {
                roll_listener_deployments(cfg).await?;
            }
            Ok::<(), anyhow::Error>(())
        };
        let supervisor_rollout = async {
            if supervisor_built {
                roll_role_deployments("infra-supervisor", "infra-supervisor").await?;
            }
            Ok::<(), anyhow::Error>(())
        };
        tokio::try_join!(
            listener_rollout,
            broker_rollout,
            supervisor_rollout,
        )?;
        println!("daemon refreshed; new image / manifests rolled out");
    } else if apps_changed {
        // Only the shared-credentials secret changed: the file mounts on
        // the broker alone, so roll just it and leave the dispatcher
        // (and its port-forwards) untouched.
        let status = kubectl(&[
            "-n",
            &cfg.db_namespace,
            "rollout",
            "restart",
            "deployment/weft-broker",
        ])
        .status()
        .await?;
        if !status.success() {
            anyhow::bail!("broker rollout restart failed");
        }
        println!("shared-credentials secret changed; broker rolled to mount it");
    } else {
        // Nothing to roll out, but a port-forward the daemon owns may be missing
        // or dead: a background `kubectl port-forward` dies if its pod restarts out
        // of band or its port is squatted, AND the GATEWAY forward is SKIPPED on the
        // very first boot (the Envoy Gateway Service is not programmed yet), so it
        // is simply absent until something reconciles it. A "restart" must always
        // leave the COMPLETE forward set working, so check EVERY desired forward
        // (not just the dispatcher's /health): the gateway, now programmed, is in
        // the desired set but has no live pid, so this re-establishes it.
        let all_alive = all_forwards_alive(&cfg).await;
        if all_alive {
            println!("daemon already running with the latest images and manifests; nothing to do");
        } else {
            kill_existing_port_forwards();
            start_port_forwards().await?;
            wait_for_http(&format!("http://127.0.0.1:{}/health", cfg.dispatcher_port)).await?;
            println!("daemon already on the latest images / manifests; port-forwards re-established");
        }
    }
    let _ = ctx;
    Ok(())
}

/// Which of the system images were actually rebuilt by a provisioning pass.
/// Drives the "anything to roll out?" decision.
struct BuiltImages {
    dispatcher: bool,
    listener: bool,
    broker: bool,
    supervisor: bool,
}

/// Build the daemon system images AND pre-warm the per-project
/// worker builder base, all concurrently (independent input sets,
/// per-image buildkit cache mounts). Shared by `start` and `restart`
/// so the two verbs cannot drift on input lists or failure policy.
///
/// Failure policy, split by criticality (not by verb):
/// - a system image failure fails the command: the daemon cannot run
///   without them.
/// - a builder-base failure only warns: it is a pre-warm for future
///   `weft run`s, which re-ensure the image and surface the real
///   error with the user present.
///
/// `tokio::join!`, NOT `try_join!`: an early bail would drop the
/// sibling futures while their `docker build` children keep running
/// detached (orphaned builds churning CPU with nobody reading the
/// result). join! lets every build finish, then all errors are
/// aggregated into one loud failure.
async fn provision_images(cfg: &ClusterConfig, rebuild: bool) -> Result<BuiltImages> {
    // Builder-base pre-warm: never errors (warn-and-continue), and
    // breadcrumbs every 15s in TTY so the long first build on a
    // clean machine stays legible.
    let worker_base_prewarm = async {
        let tty = std::io::stderr().is_terminal();
        let start = std::time::Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_secs(15));
        ticker.tick().await; // consume the immediate first tick
        let mut build = std::pin::pin!(images::ensure_worker_builder_base());
        loop {
            tokio::select! {
                res = &mut build => {
                    if let Err(e) = res {
                        tracing::warn!(
                            target: "weft_cli::daemon",
                            error = %e,
                            "pre-warm of worker builder base failed; next `weft run` will retry"
                        );
                    }
                    break;
                }
                _ = ticker.tick() => {
                    if tty {
                        let elapsed = start.elapsed().as_secs();
                        let _ = writeln!(
                            std::io::stderr(),
                            "still warming worker builder base ({elapsed}s elapsed; first build on a clean machine takes 5-10 min)"
                        );
                    }
                }
            }
        }
    };
    // Only the dispatcher stages `catalog/` (describe / compile
    // endpoints); the others must not rebuild on a catalog edit.
    let (dispatcher, listener, broker, supervisor, ()) = tokio::join!(
        images::ensure_system_image(&cfg.dispatcher_image, "dispatcher", &["catalog"], rebuild),
        images::ensure_system_image(&cfg.listener_image, "listener", &[], rebuild),
        images::ensure_system_image(&cfg.broker_image, "broker", &[], rebuild),
        images::ensure_system_image(&cfg.supervisor_image, "supervisor", &[], rebuild),
        worker_base_prewarm,
    );
    let mut failures: Vec<String> = Vec::new();
    let mut unwrap = |name: &str, res: Result<bool>| match res {
        Ok(b) => b,
        Err(e) => {
            failures.push(format!("{name}: {e:#}"));
            false
        }
    };
    let built = BuiltImages {
        dispatcher: unwrap("dispatcher", dispatcher),
        listener: unwrap("listener", listener),
        broker: unwrap("broker", broker),
        supervisor: unwrap("supervisor", supervisor),
    };
    if !failures.is_empty() {
        anyhow::bail!("system image build failed:\n  {}", failures.join("\n  "));
    }
    Ok(built)
}

/// Reconcile the public tunnel, then compute the template vars that
/// carry its address into the dispatcher manifest. THE one sequence
/// both boot paths run: the tunnel must be resolved BEFORE the vars,
/// or the dispatcher gets applied with an empty internet address,
/// silently un-wiring a tunnel that is still running (https consents
/// block, event pushes point at nothing).
async fn tunnel_then_template_vars(
    cfg: &ClusterConfig,
    manifests: &Path,
) -> Result<Vec<(&'static str, String)>> {
    let tunnel_url = reconcile_public_tunnel(manifests).await?;
    manifest_template_vars(cfg, tunnel_url.as_deref()).await
}

/// Apply every static manifest in `deploy/k8s`. Returns true iff
/// `kubectl apply` reported a change (non-`unchanged` line) on any
/// manifest, signalling that a pod rollout is warranted.
async fn apply_static_manifests(cfg: &ClusterConfig) -> Result<bool> {
    let repo_root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let manifests = repo_root.join("deploy/k8s");
    let template_vars = manifest_template_vars(cfg, None).await?;
    let mut any_changed = false;
    // Namespaces first: everything below (the tunnel included) lands
    // inside them, and on a fresh cluster they do not exist yet.
    for name in ["system-namespace.yaml", "db-namespace.yaml"] {
        any_changed |= kubectl_apply_changed(&manifests.join(name), &template_vars).await?;
    }
    // The public tunnel next, when opted in: its minted address is an
    // ADDITIONAL internet-reachable door, substituted into the
    // dispatcher + broker manifests below. It never replaces the base
    // URL: everything local (the OAuth callback, storage links) keeps
    // the stable loopback address, and only the surfaces the open
    // internet must reach (event pushes, activation URLs) prefer the
    // tunnel.
    //
    // broker + dispatcher carry ${...} placeholders (CIDRs, and for
    // the dispatcher the public base URL + local-dev flag), all
    // substituted from `template_vars`; the others have no
    // placeholders so the same applier is a no-op substitution for
    // them. `cluster-rbac.yaml`: ClusterRoles for the POOLED
    // supervisor + listener pods (tenant-agnostic, in the control-plane
    // namespace), bound into project namespaces by RoleBindings the
    // dispatcher creates at first infra apply. Cluster-scoped; in the
    // rolling-apply list so RBAC drift (e.g. the supervisor's surface
    // growing) stays in sync.
    let template_vars = tunnel_then_template_vars(cfg, &manifests).await?;
    for name in [
        "system-namespace.yaml",
        "db-namespace.yaml",
        "postgres.yaml",
        "broker.yaml",
        "dispatcher.yaml",
        "ingress.yaml",
        "cluster-rbac.yaml",
        // Live caller connection gateway (Envoy Gateway CRs). Applied
        // after the controller install (`ensure_envoy_gateway`) so the
        // CRDs exist. `${GATEWAY_HOST}` is substituted from template vars.
        "gateway.yaml",
    ] {
        // Every manifest goes through the same applier with the
        // template vars; substitution is a no-op for the manifests
        // without placeholders (system-namespace, broker, dispatcher,
        // gateway carry them).
        any_changed |= kubectl_apply_changed(&manifests.join(name), &template_vars).await?;
    }
    Ok(any_changed)
}

pub fn data_dir() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    home.join(".local/share/weft")
}

// ----- The opt-in public trigger surface ------------------------------
//
// `--public-url` runs a filtering proxy + an outbound tunnel inside
// the cluster (deploy/k8s/public-tunnel.yaml) so a local install gets
// a public https address for exactly its public trigger surface
// (`/events/...`, `/signal/...`) and nothing else. The choice is
// PERSISTED (a marker file) so every later daemon start keeps it
// until `--no-public-url`.

fn public_url_marker() -> PathBuf {
    data_dir().join("public-url-enabled")
}

/// Where the live public address is recorded, so anything that needs
/// it after the daemon ran (the installer's summary, `daemon status`)
/// reads ONE place instead of re-deriving it from pod logs.
fn public_url_file() -> PathBuf {
    data_dir().join("public-url")
}

/// The public address this install currently answers at, or `None`
/// when the surface is closed.
pub fn current_public_url() -> Option<String> {
    if !public_url_enabled() {
        return None;
    }
    std::fs::read_to_string(public_url_file())
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Record the operator's choice when the flag was given; keep the
/// persisted one when it was not.
fn set_public_url_choice(choice: Option<bool>) -> Result<()> {
    match choice {
        None => Ok(()),
        Some(true) => {
            std::fs::create_dir_all(data_dir())?;
            std::fs::write(public_url_marker(), b"")?;
            Ok(())
        }
        Some(false) => {
            match std::fs::remove_file(public_url_marker()) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
    }
}

fn public_url_enabled() -> bool {
    public_url_marker().exists()
}

/// The operator's named-tunnel choice, from the environment (the shell
/// or the `.env` the CLI loaded): the Cloudflare tunnel token plus the
/// stable https hostname its dashboard config routes to the proxy.
/// Both or neither; one without the other is a half-configured tunnel
/// and fails loudly naming the missing half.
fn named_tunnel_config() -> Result<Option<(String, String)>> {
    let token = std::env::var("WEFT_PUBLIC_TUNNEL_TOKEN").ok().filter(|v| !v.is_empty());
    let hostname =
        std::env::var("WEFT_PUBLIC_TUNNEL_HOSTNAME").ok().filter(|v| !v.is_empty());
    match (token, hostname) {
        (Some(token), Some(hostname)) => {
            Ok(Some((token, canonical_tunnel_hostname(&hostname)?)))
        }
        (None, None) => Ok(None),
        (Some(_), None) => anyhow::bail!(
            "WEFT_PUBLIC_TUNNEL_TOKEN is set but WEFT_PUBLIC_TUNNEL_HOSTNAME is not; \
             set both (the hostname the tunnel's Cloudflare config routes to weft)"
        ),
        (None, Some(_)) => anyhow::bail!(
            "WEFT_PUBLIC_TUNNEL_HOSTNAME is set but WEFT_PUBLIC_TUNNEL_TOKEN is not; \
             set both (the token from the tunnel's Cloudflare dashboard page)"
        ),
    }
}

/// Validate + normalize the named tunnel's hostname to
/// `https://<host>`. The value becomes the base every public URL is
/// joined onto, so a stray path, query, port, or scheme would produce
/// registered-then-404ing addresses with no diagnostic; each is
/// refused naming the offending part.
fn canonical_tunnel_hostname(raw: &str) -> Result<String> {
    let parsed = url::Url::parse(raw).map_err(|e| {
        anyhow::anyhow!("WEFT_PUBLIC_TUNNEL_HOSTNAME '{raw}' is not a URL: {e}")
    })?;
    anyhow::ensure!(
        parsed.scheme() == "https",
        "WEFT_PUBLIC_TUNNEL_HOSTNAME must be https (got '{raw}')"
    );
    let host = parsed
        .host_str()
        .filter(|h| h.contains('.'))
        .ok_or_else(|| {
            anyhow::anyhow!("WEFT_PUBLIC_TUNNEL_HOSTNAME '{raw}' has no hostname")
        })?;
    anyhow::ensure!(
        parsed.port().is_none(),
        "WEFT_PUBLIC_TUNNEL_HOSTNAME must not carry a port (a named tunnel serves \
         on 443); got '{raw}'"
    );
    anyhow::ensure!(
        parsed.path() == "/" && parsed.query().is_none() && parsed.fragment().is_none(),
        "WEFT_PUBLIC_TUNNEL_HOSTNAME must be the bare hostname with no path or query \
         (e.g. https://weft.example.com); got '{raw}'"
    );
    Ok(format!("https://{host}"))
}

/// Bring the tunnel + filtering proxy up (or tear them down) to match
/// the persisted choice, and answer the public https address when one
/// is up. Quick mode reads the minted random address from the tunnel's
/// own logs (the only authority for a per-connection address); named
/// mode's address is the configured hostname.
async fn reconcile_public_tunnel(manifests: &std::path::Path) -> Result<Option<String>> {
    let manifest = manifests.join("public-tunnel.yaml");
    // The manifest carries `${TUNNEL_ARGS}` (the mode's argv); any
    // kubectl that PARSES it needs the substitution, deletes included.
    let quick_args = r#"["tunnel", "--no-autoupdate", "--url", "http://weft-public-proxy.weft-system.svc.cluster.local:8080"]"#;
    if !public_url_enabled() {
        // Local state first, cluster second: a transient apiserver
        // failure below must never leave the stamps or the recorded
        // address AHEAD of the cluster (a later re-open would trust a
        // stamp for resources that were only partially deleted and
        // skip the rollout gates). Without the stamps a re-open is
        // unambiguously "changed".
        let _ = std::fs::remove_file(public_url_file());
        let _ = std::fs::remove_file(manifest_stamp_file(&manifest));
        let _ = std::fs::remove_file(manifest_stamp_file(Path::new("weft-tunnel-token")));
        let _ = std::fs::remove_file(manifest_stamp_file(Path::new("weft-public-page")));
        // Teardown closes a PUBLIC surface, so every kubectl step is
        // checked: reporting success while the tunnel still serves
        // would leave the operator believing a door is shut that is
        // not. Neither substituted value reaches the cluster on this
        // path (kubectl deletes by kind/name and never reads the
        // bodies); they only have to render the manifest parseable, so
        // a delete never depends on the cluster still answering.
        kubectl_delete_rendered(
            &manifest,
            &[
                ("TUNNEL_ARGS", quick_args.to_string()),
                ("CLUSTER_DNS", "127.0.0.1".to_string()),
            ],
        )
        .await?;
        delete_tunnel_token_secret().await?;
        let out = kubectl(&[
            "-n",
            "weft-system",
            "delete",
            "configmap",
            "weft-public-page",
            "--ignore-not-found",
        ])
        .output()
        .await?;
        anyhow::ensure!(
            out.status.success(),
            "deleting the weft-public-page configmap failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        return Ok(None);
    }
    // Named mode (a stable operator-owned hostname) or the free quick
    // tunnel (a random address per connection). The mode decides the
    // container's argv; a mode switch changes the Deployment spec and
    // rolls the tunnel pod on its own.
    let named = named_tunnel_config()?;
    let tunnel_args = match &named {
        Some(_) => r#"["tunnel", "--no-autoupdate", "run"]"#,
        None => quick_args,
    };
    // The token Secret before the manifest in named mode, so a rolling
    // pod always finds it. Its content feeds the same change signal as
    // the manifest: a ROTATED token with an unchanged manifest must
    // still reach the pod, and env from a secretKeyRef is injected
    // only at pod start.
    let mut secret_changed = false;
    if let Some((token, _)) = &named {
        let secret = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": { "name": "weft-tunnel-token", "namespace": "weft-system" },
            "type": "Opaque",
            "stringData": { "TUNNEL_TOKEN": token },
        })
        .to_string();
        kubectl_apply_stdin(&secret, "weft-tunnel-token secret").await?;
        secret_changed = manifest_apply_changed_by_stamp(Path::new("weft-tunnel-token"), &secret);
    }
    // Content-hash apply: when the manifest actually changed (e.g. the
    // proxy's nginx allowlist gained a location, or the mode's argv
    // switched), the running proxy must be ROLLED, because nginx never
    // re-reads a mounted ConfigMap on its own. A tunnel-Deployment
    // spec change rolls the tunnel pod by itself; a proxy-only change
    // leaves it alone (a quick tunnel's minted address survives a
    // proxy roll). A secret-only change rolls the tunnel explicitly,
    // since nothing else would re-inject the new token; a page-only
    // change rolls the proxy the same way (its files mount at start).
    // The root page's files before the manifest, so the proxy pod the
    // manifest (or a roll below) creates always finds its mount.
    let page_dir = manifests
        .parent()
        .expect("deploy/k8s has a parent")
        .join("public-page");
    let page_changed = apply_public_page_configmap(&page_dir).await?;
    let manifest_changed = kubectl_apply_changed(
        &manifest,
        &[
            ("TUNNEL_ARGS", tunnel_args.to_string()),
            ("CLUSTER_DNS", cluster_dns_ip().await?),
        ],
    )
    .await?;
    if secret_changed {
        let status = kubectl(&[
            "-n",
            "weft-system",
            "rollout",
            "restart",
            "deployment/weft-tunnel",
        ])
        .status()
        .await?;
        if !status.success() {
            anyhow::bail!("restarting the tunnel after a token change failed");
        }
    }
    if manifest_changed || page_changed {
        let status = kubectl(&[
            "-n",
            "weft-system",
            "rollout",
            "restart",
            "deployment/weft-public-proxy",
        ])
        .status()
        .await?;
        if !status.success() {
            anyhow::bail!("restarting the public proxy after a manifest change failed");
        }
        let status = kubectl(&[
            "-n",
            "weft-system",
            "rollout",
            "status",
            "deployment/weft-public-proxy",
            "--timeout=120s",
        ])
        .status()
        .await?;
        if !status.success() {
            anyhow::bail!("the public proxy never became ready after its restart");
        }
    }
    // Unconditional readiness gate, changed or not: the address
    // answered below is only meaningful while exactly one tunnel pod
    // is running and ready. Without this, the steady-state re-run
    // would print a confident address over a crash-looping pod (a
    // revoked token, an evicted node), and the quick-mode log read
    // below could hit a terminating pod's stale banner.
    let status = kubectl(&[
        "-n",
        "weft-system",
        "rollout",
        "status",
        "deployment/weft-tunnel",
        "--timeout=120s",
    ])
    .status()
    .await?;
    if !status.success() {
        anyhow::bail!(
            "the tunnel never became ready.\n\
             Inspect it: kubectl -n weft-system describe deployment/weft-tunnel\n\
             and:        kubectl -n weft-system logs deployment/weft-tunnel"
        );
    }
    // Quick mode leaves no stale token behind: a later named run must
    // prove its own token, and an orphaned credential in the cluster
    // is junk nobody acts on. Deleted only AFTER the apply and the
    // readiness gate succeeded, so a failed apply never strands a
    // still-named Deployment with its token already gone.
    if named.is_none() {
        delete_tunnel_token_secret().await?;
    }
    let url = match &named {
        // The named tunnel's address is the operator's own hostname;
        // the pod's logs never carry it (routing lives in the tunnel's
        // Cloudflare config), so the env is the authority.
        Some((_, hostname)) => hostname.clone(),
        None => wait_for_quick_tunnel_url().await?,
    };
    std::fs::create_dir_all(data_dir())?;
    std::fs::write(public_url_file(), &url)?;
    println!("public trigger surface reachable at {url}");
    println!("  exposed through the filtering proxy: /events/... (provider event pushes), /signal/... (per-signal fire tokens), and /public/files/... (minted expiring media links); everything else answers 404.");
    println!("  Rerun with --no-public-url to close it.");
    if named.is_none() {
        println!(
            "  NOTE: this free-tunnel address changes whenever the tunnel reconnects, \
             and everything registered against it (a provider's event push URL, an \
             OAuth redirect) rots until re-registered. For a stable address, set \
             WEFT_PUBLIC_TUNNEL_TOKEN + WEFT_PUBLIC_TUNNEL_HOSTNAME (docs/stable-public-address.md)."
        );
    }
    Ok(Some(url))
}

/// The QUICK tunnel's minted address, scraped from its own pod's log
/// banner (`https://<random>.trycloudflare.com`, printed a few seconds
/// after start); polled until it shows. Named tunnels never come here:
/// their address is configuration, not a log line. The log read
/// targets the single Running pod BY NAME: `logs deployment/...`
/// picks one matching pod, and during a rollout that can be the
/// terminating one whose banner carries the previous address.
async fn wait_for_quick_tunnel_url() -> Result<String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        let pod = kubectl(&[
            "-n",
            "weft-system",
            "get",
            "pod",
            "-l",
            "app=weft-tunnel",
            "--field-selector=status.phase=Running",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ])
        .output()
        .await?;
        let pod = String::from_utf8_lossy(&pod.stdout).trim().to_string();
        if pod.is_empty() {
            // The readiness gate upstream makes this transient (a pod
            // is running); re-poll inside the same deadline.
            if std::time::Instant::now() > deadline {
                anyhow::bail!(
                    "no running tunnel pod to read the minted address from.\n\
                     Inspect it: kubectl -n weft-system describe deployment/weft-tunnel"
                );
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        }
        let out = kubectl(&[
            "-n",
            "weft-system",
            "logs",
            &format!("pod/{pod}"),
            "--tail",
            "200",
        ])
        .output()
        .await?;
        let text = String::from_utf8_lossy(&out.stdout);
        // The LAST address in the window: a quick tunnel re-registers
        // under a fresh random address when its edge connection drops,
        // and the startup banner's old address can still sit earlier
        // in the log. Only the newest one answers.
        if let Some(url) = text
            .split_whitespace()
            .filter(|w| w.starts_with("https://") && w.contains(".trycloudflare.com"))
            .last()
        {
            return Ok(url.trim_end_matches('/').to_string());
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!(
                "the tunnel pod reported no public address (its minting banner may \
                 have rotated out of the log).\n\
                 Mint a fresh one with: kubectl -n weft-system rollout restart \
                 deployment/weft-tunnel\n\
                 then re-run `weft daemon start` (and re-register the new address \
                 wherever the old one was registered)."
            );
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// A background `kubectl port-forward` the daemon owns. Each forward
/// tracks its own pid + log file (keyed by `name`) so they start,
/// stop, and report liveness independently.
struct PortForward {
    /// Stable key for the pid/log filenames (e.g. "dispatcher").
    name: &'static str,
    namespace: String,
    /// Service name. Most are fixed; the live gateway's data-plane
    /// Service name is generated by Envoy Gateway, so it is resolved by
    /// label at `port_forwards` build time (hence `String`, not `&str`).
    service: String,
    local_port: u16,
    remote_port: u16,
}

/// Every port-forward the daemon maintains:
/// - dispatcher: the control plane API (CLI / extension talk here).
///   Always present.
/// - ingress: the kind ingress controller, so storage file downloads
///   minted as `http://127.0.0.1:<ingress_port>/storage/...` are
///   reachable from the operator's machine. Downloads stream straight
///   from the storage box through the ingress; the dispatcher is
///   never in the byte path, so this is a separate forward, not a
///   route through the dispatcher port. Kind-only: a real k8s
///   operator sets a real external ingress host
///   (WEFT_DISPATCHER_PUBLIC_BASE_URL) reachable without a forward,
///   and the ingress-nginx Service name may differ in their cluster.
async fn port_forwards(cfg: &ClusterConfig) -> Vec<PortForward> {
    let mut forwards = vec![PortForward {
        name: "dispatcher",
        namespace: cfg.system_namespace.clone(),
        service: "weft-dispatcher".to_string(),
        local_port: cfg.dispatcher_port,
        remote_port: 9999,
    }];
    if cfg.backend == ClusterBackend::Kind {
        forwards.push(PortForward {
            name: "ingress",
            namespace: "ingress-nginx".to_string(),
            service: "ingress-nginx-controller".to_string(),
            local_port: cfg.ingress_port,
            remote_port: 80,
        });
        // No object-store port-forward: the store runs as a HOST docker container
        // (see `ensure_object_store`) already published on 127.0.0.1:<seaweed_port>,
        // so the browser reaches it directly and in-cluster pods reach it via the
        // kind gateway IP. Nothing to forward out of the cluster.
        // Live connection gateway: forward the local gateway port to the
        // Envoy Gateway data-plane Service. Its name is generated by Envoy
        // Gateway, so resolve it by the owning-gateway label. Skipped if
        // not yet present (first boot before the Gateway is programmed);
        // the next restart picks it up.
        if let Some(svc) = resolve_envoy_gateway_service().await {
            forwards.push(PortForward {
                name: "gateway",
                namespace: "envoy-gateway-system".to_string(),
                service: svc,
                local_port: cfg.gateway_port,
                remote_port: 80,
            });
        }
    }
    forwards
}

/// Resolve the Envoy Gateway data-plane Service name for our Gateway.
/// Envoy Gateway generates it (e.g. `envoy-envoy-gateway-system-weft-...`),
/// labeled with the owning gateway, so we look it up rather than hardcode.
/// Returns `None` if not yet created (the Gateway isn't programmed yet).
async fn resolve_envoy_gateway_service() -> Option<String> {
    // Short request timeout: this is called from `port_forwards`, which
    // `status` awaits BEFORE its reachability probe. Without a bound, a
    // slow/down apiserver would hang `weft daemon status` instead of letting
    // it report quickly. A miss (svc not yet programmed, or apiserver slow)
    // simply means "no gateway forward yet", recovered on the next call.
    let out = kubectl(&[
        "--request-timeout=5s",
        "-n",
        "envoy-gateway-system",
        "get",
        "svc",
        "-l",
        "gateway.envoyproxy.io/owning-gateway-name=weft-live-gateway",
        "-o",
        "jsonpath={.items[0].metadata.name}",
    ])
    .output()
    .await
    .ok()?;
    if !out.status.success() {
        return None;
    }
    let name = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

pub fn data_dir_pid_file(name: &str) -> PathBuf {
    data_dir().join(format!("port-forward-{name}.pid"))
}

fn pf_log_file(name: &str) -> PathBuf {
    data_dir().join(format!("port-forward-{name}.log"))
}

async fn start(ctx: &Ctx, rebuild: bool) -> Result<()> {
    let cfg = cluster_config();
    require_binary("kubectl").await?;
    require_binary("docker").await?;
    if cfg.backend == ClusterBackend::Kind {
        require_binary("kind").await?;
        ensure_cluster(cfg).await?;
        // The object store is a HOST docker container the cluster reaches OUT to
        // (the local stand-in for a real S3 provider). Bring it up before anything
        // that needs a bucket.
        ensure_object_store(cfg).await?;
        ensure_ingress_controller().await?;
        ensure_envoy_gateway().await?;
    }

    provision_images(cfg, rebuild).await?;
    if cfg.backend == ClusterBackend::Kind {
        // System tags are reused (`:local`), so kind tag presence does
        // not imply matching content; force-load every time so a
        // freshly-rebuilt image always lands inside the node.
        images::kind_load_force(&cfg.cluster_name, &cfg.dispatcher_image).await?;
        images::kind_load_force(&cfg.cluster_name, &cfg.listener_image).await?;
        images::kind_load_force(&cfg.cluster_name, &cfg.broker_image).await?;
        images::kind_load_force(&cfg.cluster_name, &cfg.supervisor_image).await?;
    }

    let repo_root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let manifests = repo_root.join("deploy/k8s");
    // system-namespace carries cluster-specific placeholders (the
    // dispatcher's and pooled listener's egress-NetworkPolicy CIDRs +
    // apiserver ClusterIP), so it goes through the templated applier.
    kubectl_apply_templated(
        &manifests.join("system-namespace.yaml"),
        &manifest_template_vars(cfg, None).await?,
    )
    .await?;
    kubectl_apply_file(&manifests.join("db-namespace.yaml")).await?;
    // The public tunnel (when opted in) before the dispatcher: the
    // shared sequence resolves its address into the vars the
    // dispatcher manifest interpolates.
    let template_vars = tunnel_then_template_vars(cfg, &manifests).await?;
    // No per-tenant namespace exists anymore: storage is a shared pooled
    // pod in the control-plane namespace (placed lazily on first write),
    // and a project gets its own namespace only at first infra apply.
    // Register creates no namespace.
    kubectl_apply_file(&manifests.join("postgres.yaml")).await?;
    wait_for_deployment_ready_in_ns("weft-postgres", &cfg.db_namespace).await?;
    // broker + dispatcher carry cluster-specific placeholders (the
    // broker's TokenReview-egress NetworkPolicy CIDRs, the
    // dispatcher's CIDRs + public base URL + local-dev flag);
    // substitute them so a non-kind operator sets them once via env
    // instead of hand-editing manifests.
    // Provider keys + OAuth apps before the broker: its pods import the
    // key secret via `envFrom` and MOUNT the apps secret at start, so
    // both must exist when they come up.
    apply_sealing_key_secret(cfg).await?;
    apply_access_apps_secret(cfg).await?;
    kubectl_apply_templated(&manifests.join("broker.yaml"), &template_vars).await?;
    wait_for_deployment_ready_in_ns("weft-broker", &cfg.db_namespace).await?;
    kubectl_apply_templated(&manifests.join("dispatcher.yaml"), &template_vars).await?;
    kubectl_apply_file(&manifests.join("ingress.yaml")).await?;
    // Cluster-scoped RBAC: ClusterRoles the supervisor + listener
    // RoleBindings (created per project namespace by the dispatcher)
    // reference. Applied once during daemon boot.
    kubectl_apply_file(&manifests.join("cluster-rbac.yaml")).await?;
    // Live caller connection gateway (Envoy Gateway CRs). The controller
    // was installed above (`ensure_envoy_gateway`), so its CRDs exist.
    // `${GATEWAY_HOST}` is substituted from template vars.
    kubectl_apply_templated(&manifests.join("gateway.yaml"), &template_vars).await?;

    wait_for_statefulset_ready("weft-dispatcher").await?;
    // Kill any stale forwards from a previous daemon before
    // re-establishing, so a restarted daemon doesn't leak processes
    // or bind-conflict on the local ports.
    kill_existing_port_forwards();
    start_port_forwards().await?;
    wait_for_http(&format!("http://127.0.0.1:{}/health", cfg.dispatcher_port)).await?;

    let _ = ctx;
    let backend = match cfg.backend {
        ClusterBackend::Kind => "kind",
        ClusterBackend::K8s => "k8s",
    };
    println!(
        "daemon ready at http://127.0.0.1:{} ({} cluster '{}', system ns '{}', default user ns '{}')",
        cfg.dispatcher_port,
        backend,
        cfg.cluster_name,
        cfg.system_namespace,
        cfg.default_user_namespace,
    );
    Ok(())
}

async fn stop() -> Result<()> {
    let cfg = cluster_config();
    kill_existing_port_forwards();
    let _ = kubectl(&[
        "-n", &cfg.system_namespace, "scale", "statefulset/weft-dispatcher", "--replicas=0",
    ])
    .status()
    .await;
    println!("daemon stopped");
    Ok(())
}

/// Kill every running `kubectl port-forward` we previously spawned
/// (dispatcher + ingress + gateway). Called on stop and before we
/// re-establish forwards after a Pod rollout. Idempotent.
/// Stable names of every port-forward the daemon may own. Used for
/// pid-file lifecycle (kill / liveness) without resolving live cluster
/// state. The actual forward set (`port_forwards`) is a subset depending
/// on backend + what's programmed yet; killing a name with no pid file is
/// a no-op, so listing the superset here is safe.
const PORT_FORWARD_NAMES: &[&str] = &["dispatcher", "ingress", "gateway"];

fn kill_existing_port_forwards() {
    for name in PORT_FORWARD_NAMES {
        let pid_file = data_dir_pid_file(name);
        if let Some(pid) = read_pid(&pid_file) {
            let _ = signal_term(pid);
            let _ = fs::remove_file(&pid_file);
        }
    }
}

/// Are ALL the daemon's CURRENTLY-DESIRED port-forwards alive (a live pid each)?
/// Iterates the real set (`port_forwards`), not a static name list: the gateway
/// forward is absent from the set until the Envoy Gateway is programmed, so it
/// never drags liveness down before it exists; once it IS in the set, a missing
/// pid correctly reports down (no fail-open). The single source of truth for both
/// `status` (report up/down) and `restart` (decide whether to re-establish).
async fn all_forwards_alive(cfg: &ClusterConfig) -> bool {
    port_forwards(cfg).await.iter().all(|pf| {
        read_pid(&data_dir_pid_file(pf.name))
            .map(process_alive)
            .unwrap_or(false)
    })
}

async fn status(ctx: &Ctx) -> Result<()> {
    let cfg = cluster_config();
    let pf_alive = all_forwards_alive(&cfg).await;
    match ctx.client().get_json("/projects").await {
        Ok(v) => {
            let n = v.as_array().map(|a| a.len()).unwrap_or(0);
            println!(
                "daemon: running (cluster '{}', system ns '{}', user ns '{}', port-forward {}); {} project(s)",
                cfg.cluster_name,
                cfg.system_namespace,
                cfg.default_user_namespace,
                if pf_alive { "up" } else { "down" },
                n,
            );
        }
        Err(e) => {
            println!("daemon: unreachable at {}: {e}", ctx.client().base());
        }
    }
    // The public trigger surface, when it is open: what providers
    // deliver events to, and what a trigger's setup instructions ask
    // the operator to paste at the provider.
    match current_public_url() {
        Some(url) => println!(
            "public trigger surface: {url} (events + signal fire routes only; \
             ./setup.sh --no-public-url closes it)"
        ),
        None => println!("public trigger surface: closed"),
    }
    Ok(())
}

async fn logs(tail: usize, follow: bool) -> Result<()> {
    let cfg = cluster_config();
    let tail_arg = format!("--tail={tail}");
    let mut args: Vec<&str> = vec![
        "-n", &cfg.system_namespace,
        "logs", "-l", "app=weft-dispatcher", "--prefix",
        &tail_arg,
    ];
    if follow {
        args.push("-f");
    }
    let status = kubectl(&args).status().await?;
    if !status.success() {
        anyhow::bail!("kubectl logs exited {status}");
    }
    Ok(())
}

// ----- Cluster + ingress bootstrap ----------------------------------

async fn ensure_cluster(cfg: &ClusterConfig) -> Result<()> {
    let out = Command::new("kind").args(["get", "clusters"]).output().await?;
    let list = String::from_utf8_lossy(&out.stdout);
    if list.lines().any(|n| n == cfg.cluster_name) {
        // The kind cluster exists, but the kubeconfig CONTEXT can be absent even so:
        // a reset/rotated kubeconfig, a different `$KUBECONFIG`, or a prior partial
        // run leaves the node running with no `kind-<name>` context. Everything
        // after this (ingress install, Envoy, manifest apply) targets that context
        // and fails with "context kind-<name> does not exist". Re-export the
        // kubeconfig so the context is present; idempotent when it already is.
        let status = Command::new("kind")
            .args(["export", "kubeconfig", "--name", &cfg.cluster_name])
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!(
                "kind export kubeconfig for existing cluster '{}' failed with {status}",
                cfg.cluster_name
            );
        }
        return Ok(());
    }
    println!(
        "creating kind cluster '{}' (first run)",
        cfg.cluster_name,
    );
    let config = r#"kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    kubeadmConfigPatches:
      - |
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "ingress-ready=true"
    extraPortMappings:
      - containerPort: 80
        hostPort: 80
        protocol: TCP
      - containerPort: 443
        hostPort: 443
        protocol: TCP
"#;
    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), config)?;
    let status = Command::new("kind")
        .args(["create", "cluster", "--name", &cfg.cluster_name, "--config"])
        .arg(tmp.path())
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("kind create cluster failed with {status}");
    }
    Ok(())
}

/// Docker container name for the daemon's object store. A distinct,
/// weft-prefixed name (and a non-default host port) so it runs side by side
/// with any other object store on the host without clashing.
// SYNC: OBJECT_STORE_CONTAINER <-> setup.sh (purge step removing the
//       container, its "<container>-data" volume, and the seaweedfs image)
const OBJECT_STORE_CONTAINER: &str = "weft-object-store";

/// The S3 identities config the object store validates signatures against: one
/// admin identity holding the local-dev key/secret.
fn object_store_s3_config(access_key: &str, secret_key: &str) -> String {
    format!(
        r#"{{
  "identities": [
    {{
      "name": "weft",
      "credentials": [ {{ "accessKey": "{access_key}", "secretKey": "{secret_key}" }} ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }}
  ]
}}
"#
    )
}

/// The IPv4 gateway of the `kind` docker network: the address IN-CLUSTER pods use
/// to reach a service running on the HOST (the object store container). On Linux
/// there is no `host.docker.internal` in pods, so this gateway IP is the reliable
/// route out to the host. Discovered at runtime (it varies per machine/network).
async fn kind_network_gateway_ipv4() -> Result<String> {
    let out = Command::new("docker")
        .args(["network", "inspect", "kind", "-f", "{{range .IPAM.Config}}{{.Gateway}} {{end}}"])
        .output()
        .await?;
    if !out.status.success() {
        anyhow::bail!("docker network inspect kind failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    // The network has both an IPv6 and an IPv4 gateway; pick the IPv4 one (the one
    // with dots and no colons).
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .find(|g| g.contains('.') && !g.contains(':'))
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("no IPv4 gateway on the kind docker network"))
}

/// The cluster DNS Service's address, for nginx's `resolver`.
///
/// READ from the cluster, where its sibling `apiserver_clusterip`
/// DERIVES from the configured service CIDR. The difference is what
/// each is for: that one renders a NetworkPolicy, which must be
/// buildable with nothing but configuration, while a resolver pointing
/// at the wrong address turns every proxied request into a lookup
/// failure, so this one asks.
///
/// `kube-dns` is the Service name every Kubernetes distribution uses;
/// `WEFT_CLUSTER_DNS_IP` names the address directly for one that does
/// not.
///
/// A configured address is PARSED, like the CIDRs beside it, and a
/// value that is not an IP address fails here naming the variable.
/// The alternative is the failure this whole directive exists to
/// prevent, arriving silently: nginx accepts almost any token as a
/// resolver, starts happily, passes the rollout gate, and then fails
/// every proxied request with nothing anywhere naming the cause.
async fn cluster_dns_ip() -> Result<String> {
    if let Some(configured) = configured_dns_ip(&cluster_config().cluster_dns_ip)? {
        return Ok(configured);
    }
    let out = kubectl(&[
        "-n",
        "kube-system",
        "get",
        "service",
        "kube-dns",
        "-o",
        "jsonpath={.spec.clusterIP}",
    ])
    .output()
    .await?;
    if !out.status.success() {
        anyhow::bail!(
            "reading the cluster DNS service address failed: {}. Set WEFT_CLUSTER_DNS_IP if \
             this cluster's DNS Service is not kube-system/kube-dns.",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let ip = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if ip.is_empty() {
        anyhow::bail!(
            "the cluster DNS service (kube-system/kube-dns) has no address; set \
             WEFT_CLUSTER_DNS_IP to this cluster's DNS Service address"
        );
    }
    Ok(ip)
}

/// The DNS address an operator configured, or `None` when they
/// configured none. Anything that is not an IP address is refused
/// here, naming the variable, rather than reaching nginx.
fn configured_dns_ip(raw: &str) -> Result<Option<String>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    raw.parse::<std::net::IpAddr>()
        .map_err(|e| anyhow::anyhow!("WEFT_CLUSTER_DNS_IP='{raw}': {e}"))?;
    Ok(Some(raw.to_string()))
}

/// An endpoint URL's host, classified for the store egress opening.
#[derive(Debug, PartialEq, Eq)]
enum EndpointHost {
    /// A private IPv4 address (RFC-1918, CGNAT, or link-local): the
    /// one shape that derives a /32 opening.
    PrivateIpv4(std::net::Ipv4Addr),
    /// A public IPv4 address: re-permits nothing.
    PublicIpv4,
    /// An IPv6 literal (bracketed or bare): the derived-/32 shape
    /// cannot express it; the caller decides how to fail.
    Ipv6(String),
    /// A DNS name: re-permits nothing (it may still resolve
    /// privately; the caller may check and point at the override).
    Hostname(String),
}

/// Parse and classify the endpoint URL's host. Userinfo before an
/// `@` is dropped; a bracketed literal (`http://[fd00::1]:9096`) is
/// unwrapped; an authority with several `:` is a bare IPv6 literal,
/// otherwise the single `:` splits off the port.
fn endpoint_host(endpoint: &str) -> EndpointHost {
    let rest = endpoint.split("://").nth(1).unwrap_or(endpoint);
    let authority = rest.split('/').next().unwrap_or(rest);
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    let host = if let Some(stripped) = authority.strip_prefix('[') {
        stripped.split(']').next().unwrap_or(stripped).to_string()
    } else if authority.matches(':').count() > 1 {
        authority.to_string()
    } else {
        authority.split(':').next().unwrap_or(authority).to_string()
    };
    if let Ok(ip) = host.parse::<std::net::Ipv4Addr>() {
        if is_private_ipv4(ip) {
            return EndpointHost::PrivateIpv4(ip);
        }
        return EndpointHost::PublicIpv4;
    }
    if host.parse::<std::net::Ipv6Addr>().is_ok() {
        return EndpointHost::Ipv6(host);
    }
    EndpointHost::Hostname(host)
}

/// RFC-1918, CGNAT, or link-local.
fn is_private_ipv4(ip: std::net::Ipv4Addr) -> bool {
    let cgnat = ip.octets()[0] == 100 && (64..128).contains(&ip.octets()[1]);
    ip.is_private() || ip.is_link_local() || cgnat
}

/// Bring up the object store as a HOST docker container (SeaweedFS's S3 gateway).
/// The cluster reaches OUT to it over S3: the store is never inside
/// the cluster. Idempotent: a running container is left alone, a stopped one is
/// started, else it is created. `-s3.externalUrl` is deliberately UNSET so the
/// gateway validates each presigned request against its own incoming Host header
/// (v3.80 behavior), letting the SAME instance accept URLs signed for the host
/// gateway IP (pods) AND for 127.0.0.1 (the browser).
/// Remove the Docker-created root-owned `s3.config.json` directory and
/// chown the config dir back to the invoking user, with docker's own
/// (root) privileges. Only called after a plain remove failed.
async fn heal_root_owned_config_dir(cfg_dir: &std::path::Path) -> Result<()> {
    let id_of = |flag: &'static str| async move {
        let out = Command::new("id").arg(flag).output().await?;
        anyhow::ensure!(out.status.success(), "id {flag} failed");
        Ok::<String, anyhow::Error>(String::from_utf8_lossy(&out.stdout).trim().to_string())
    };
    let (uid, gid) = (id_of("-u").await?, id_of("-g").await?);
    let status = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &format!("{}:/heal", cfg_dir.display()),
            "alpine:3",
            "sh",
            "-c",
            &format!("rm -rf /heal/s3.config.json && chown {uid}:{gid} /heal"),
        ])
        .status()
        .await?;
    anyhow::ensure!(status.success(), "the docker-run cleanup exited with {status}");
    Ok(())
}

async fn ensure_object_store(cfg: &ClusterConfig) -> Result<()> {
    let running = Command::new("docker")
        .args(["ps", "-q", "-f", &format!("name=^{OBJECT_STORE_CONTAINER}$")])
        .output()
        .await?;
    if running.status.success() && !running.stdout.is_empty() {
        return Ok(());
    }
    // The s3 identities file the container bind-mounts. Ensure it exists AS A
    // FILE before any `docker start/run`: a start whose bind-mount source is
    // missing makes Docker auto-create the source as an empty DIRECTORY, and a
    // directory cannot be mounted onto the container's file target, which
    // wedges every later start (`not a directory: mounting ... onto a file`).
    // So we self-heal a stray directory here and (re)write the file, on BOTH
    // the restart and the fresh-run path, instead of only writing it for a
    // brand-new container.
    let cfg_dir = data_dir().join("object-store");
    let cfg_path = cfg_dir.join("s3.config.json");
    if cfg_path.is_dir() {
        if let Err(remove_err) = std::fs::remove_dir_all(&cfg_path) {
            // Docker auto-creates a missing bind-mount source as a
            // ROOT-owned directory, so the plain remove fails with
            // permission denied for the invoking user. Docker made
            // the mess as root; it can clean it as root: a one-shot
            // container removes the stray directory and hands the
            // config dir back to the user.
            heal_root_owned_config_dir(&cfg_dir).await.with_context(|| {
                format!(
                    "the object-store config path {} is a directory (Docker auto-created it \
                     from a missing bind-mount source) and removing it failed \
                     ({remove_err}); remove it by hand: sudo rm -rf {}",
                    cfg_path.display(),
                    cfg_dir.display()
                )
            })?;
        }
    }
    std::fs::create_dir_all(&cfg_dir)?;
    std::fs::write(
        &cfg_path,
        object_store_s3_config("weft-local", "weft-local-dev-secret"),
    )?;

    // A stopped container with our name (previous daemon run): start it, preserving
    // its data volume. Its config file is now guaranteed present as a file above.
    let exists = Command::new("docker")
        .args(["ps", "-aq", "-f", &format!("name=^{OBJECT_STORE_CONTAINER}$")])
        .output()
        .await?;
    if exists.status.success() && !exists.stdout.is_empty() {
        let status =
            Command::new("docker").args(["start", OBJECT_STORE_CONTAINER]).status().await?;
        if !status.success() {
            anyhow::bail!("docker start {OBJECT_STORE_CONTAINER} failed with {status}");
        }
        return Ok(());
    }
    println!("starting object store container '{OBJECT_STORE_CONTAINER}' on port {}", cfg.seaweed_port);
    let port_map = format!("{}:8333", cfg.seaweed_port);
    let mount = format!("{}:/etc/seaweedfs/s3.config.json:ro", cfg_path.display());
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            OBJECT_STORE_CONTAINER,
            "--restart",
            "unless-stopped",
            "-p",
            &port_map,
            "-v",
            &format!("{OBJECT_STORE_CONTAINER}-data:/data"),
            "-v",
            &mount,
            "chrislusf/seaweedfs:3.80",
            "server",
            "-dir=/data",
            "-s3",
            "-s3.port=8333",
            "-s3.config=/etc/seaweedfs/s3.config.json",
            "-master.volumeSizeLimitMB=1024",
        ])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker run {OBJECT_STORE_CONTAINER} failed with {status}");
    }
    Ok(())
}

async fn ensure_ingress_controller() -> Result<()> {
    let out = kubectl(&["get", "namespace", "ingress-nginx", "-o", "name"])
        .output()
        .await?;
    if out.status.success() && !out.stdout.is_empty() {
        return Ok(());
    }
    println!("installing nginx-ingress controller");
    let status = kubectl(&[
        "apply",
        "-f",
        "https://kind.sigs.k8s.io/examples/ingress/deploy-ingress-nginx.yaml",
    ])
    .status()
    .await?;
    if !status.success() {
        anyhow::bail!("ingress install failed with {status}");
    }
    // `kubectl wait --for=condition=ready pod --selector=...` errors
    // immediately if zero pods exist at the moment of the call.
    // Right after `kubectl apply`, the Deployment is created but the
    // ReplicaSet hasn't materialized any pods yet. `rollout status`
    // handles that case (polls until at least one replica is ready).
    let wait = kubectl(&[
        "-n",
        "ingress-nginx",
        "rollout",
        "status",
        "deployment/ingress-nginx-controller",
        "--timeout=180s",
    ])
    .status()
    .await?;
    if !wait.success() {
        anyhow::bail!("ingress controller failed to become ready");
    }
    Ok(())
}

/// Envoy Gateway version installed for the live caller connection
/// gateway. Pinned so a local install matches the manifests in
/// `deploy/k8s/gateway.yaml` (which use Envoy Gateway CRDs).
const ENVOY_GATEWAY_VERSION: &str = "v1.8.1";

/// Install the Envoy Gateway controller (idempotent) into the cluster.
/// This is the live caller connection front door: it routes an outside
/// caller to a specific worker pod. The public host + TLS are set via the
/// gateway manifest's `${GATEWAY_HOST}` (applied by `apply_static_manifests`).
async fn ensure_envoy_gateway() -> Result<()> {
    let out = kubectl(&["get", "namespace", "envoy-gateway-system", "-o", "name"])
        .output()
        .await?;
    if !(out.status.success() && !out.stdout.is_empty()) {
        println!("installing Envoy Gateway controller ({ENVOY_GATEWAY_VERSION})");
        let url = format!(
            "https://github.com/envoyproxy/gateway/releases/download/{ENVOY_GATEWAY_VERSION}/install.yaml"
        );
        let status = kubectl(&["apply", "--server-side", "-f", &url]).status().await?;
        if !status.success() {
            anyhow::bail!("Envoy Gateway install failed with {status}");
        }
    }
    // Wait for the controller before applying our Gateway/Backend CRs
    // (a CR applied before the CRDs register would 404).
    let wait = kubectl(&[
        "-n",
        "envoy-gateway-system",
        "rollout",
        "status",
        "deployment/envoy-gateway",
        "--timeout=180s",
    ])
    .status()
    .await?;
    if !wait.success() {
        anyhow::bail!("Envoy Gateway controller failed to become ready");
    }
    enable_envoy_backend_api().await?;
    Ok(())
}

/// Enable the Backend API (DynamicResolver) in the controller's config.
/// `EnvoyGateway` is the config FILE's kind, living in the
/// `envoy-gateway-config` ConfigMap, not a cluster CR, so we patch the
/// ConfigMap's `extensionApis` and restart the controller to pick it up.
/// Idempotent: a no-op once `enableBackend: true` is already present.
async fn enable_envoy_backend_api() -> Result<()> {
    let out = kubectl(&[
        "-n",
        "envoy-gateway-system",
        "get",
        "configmap",
        "envoy-gateway-config",
        "-o",
        r"jsonpath={.data.envoy-gateway\.yaml}",
    ])
    .output()
    .await?;
    if !out.status.success() {
        anyhow::bail!("could not read envoy-gateway-config ConfigMap");
    }
    let current = String::from_utf8_lossy(&out.stdout).to_string();
    if current.contains("enableBackend: true") {
        return Ok(()); // already enabled
    }
    // Replace the empty `extensionApis: {}` with the enabled block. The
    // controller writes `extensionApis: {}` by default; if a future
    // version changes that spelling this match misses and we bail loud
    // rather than silently leaving the Backend API off.
    if !current.contains("extensionApis: {}") {
        anyhow::bail!(
            "envoy-gateway-config has an unexpected extensionApis shape; \
             cannot enable the Backend API automatically. Set \
             `extensionApis.enableBackend: true` in the ConfigMap manually."
        );
    }
    let patched = current.replace(
        "extensionApis: {}",
        "extensionApis:\n  enableBackend: true",
    );
    // Apply the new ConfigMap data. `kubectl patch --type merge` with the
    // full data key replaces just that field.
    let patch = serde_json::json!({ "data": { "envoy-gateway.yaml": patched } }).to_string();
    let status = kubectl(&[
        "-n",
        "envoy-gateway-system",
        "patch",
        "configmap",
        "envoy-gateway-config",
        "--type",
        "merge",
        "-p",
        &patch,
    ])
    .status()
    .await?;
    if !status.success() {
        anyhow::bail!("failed to patch envoy-gateway-config for the Backend API");
    }
    // Restart the controller to reload the config, and WAIT for it to come
    // back ready. Both must succeed: if the reload fails or never becomes
    // ready, the Backend API (DynamicResolver) the live-caller gateway
    // depends on is not loaded, and every live connection would 503. Fail
    // loud at provisioning rather than ship a gateway that silently can't
    // route live callers.
    let restart = kubectl(&[
        "-n",
        "envoy-gateway-system",
        "rollout",
        "restart",
        "deployment/envoy-gateway",
    ])
    .status()
    .await?;
    if !restart.success() {
        anyhow::bail!(
            "envoy-gateway controller restart failed; the Backend API config was patched \
             but not reloaded, so live caller connections would not route"
        );
    }
    let ready = kubectl(&[
        "-n",
        "envoy-gateway-system",
        "rollout",
        "status",
        "deployment/envoy-gateway",
        "--timeout=180s",
    ])
    .status()
    .await?;
    if !ready.success() {
        anyhow::bail!(
            "envoy-gateway controller did not become ready after the Backend API reload; \
             live caller connections would not route"
        );
    }
    Ok(())
}

async fn wait_for_deployment_ready_in_ns(name: &str, namespace: &str) -> Result<()> {
    let status = kubectl(&[
        "-n", namespace,
        "rollout", "status", &format!("deployment/{name}"),
        "--timeout=180s",
    ])
    .status()
    .await?;
    if !status.success() {
        anyhow::bail!("{name} did not reach Ready within 180s");
    }
    Ok(())
}

async fn wait_for_statefulset_ready(name: &str) -> Result<()> {
    let cfg = cluster_config();
    let status = kubectl(&[
        "-n", &cfg.system_namespace,
        "rollout", "status", &format!("statefulset/{name}"),
        "--timeout=180s",
    ])
    .status()
    .await?;
    if !status.success() {
        anyhow::bail!("{name} did not reach Ready within 180s");
    }
    Ok(())
}

/// Roll every per-tenant listener Deployment in the user
/// namespace so they pick up a freshly-loaded listener image.
/// Listener Deployments are named `listener-<tenant>`; we list
/// by name prefix and `rollout restart` each one. Best-effort:
/// errors are surfaced as warnings rather than aborting the
/// daemon refresh, since a listener that fails to roll today is
/// still recoverable next time the dispatcher re-spawns it.
async fn roll_listener_deployments(cfg: &ClusterConfig) -> Result<()> {
    let out = kubectl(&[
        "-n",
        &cfg.default_user_namespace,
        "get",
        "deployments",
        "-o",
        "jsonpath={.items[*].metadata.name}",
    ])
    .output()
    .await?;
    if !out.status.success() {
        tracing::warn!(
            target: "weft_cli::daemon",
            "listing listener deployments failed; skipping listener roll"
        );
        return Ok(());
    }
    let names = String::from_utf8_lossy(&out.stdout);
    let listeners: Vec<String> = names
        .split_whitespace()
        .filter(|n| n.starts_with("listener-"))
        .map(|s| s.to_string())
        .collect();
    // Roll every tenant's listener concurrently. Each kubectl call is
    // independent; bounded by the slowest single rollout instead of
    // the sum across tenants.
    let ns = cfg.default_user_namespace.to_string();
    let tasks = listeners.iter().map(|name| {
        let name = name.clone();
        let ns = ns.clone();
        async move {
            let status = kubectl(&[
                "-n", &ns, "rollout", "restart", &format!("deployment/{name}"),
            ])
            .status()
            .await?;
            if !status.success() {
                tracing::warn!(
                    target: "weft_cli::daemon",
                    "rollout restart deployment/{name} failed"
                );
                return Ok::<(), anyhow::Error>(());
            }
            // Block briefly on each rollout so subsequent register
            // calls hit the new Pod, not the old one mid-termination.
            let wait = kubectl(&[
                "-n", &ns, "rollout", "status", &format!("deployment/{name}"),
                "--timeout=120s",
            ])
            .status()
            .await?;
            if !wait.success() {
                tracing::warn!(
                    target: "weft_cli::daemon",
                    "deployment/{name} did not reach Ready within 120s"
                );
            }
            Ok(())
        }
    });
    futures::future::try_join_all(tasks).await?;
    if !listeners.is_empty() {
        println!("rolled {} listener deployment(s)", listeners.len());
    }
    Ok(())
}

/// Roll every per-tenant Deployment carrying `weft.dev/role=<role>`
/// after its image was rebuilt, so tenants pick up the new binary
/// instead of running stale code until their pod happens to restart.
/// These deployments live one-per-tenant across `wft-*` namespaces and
/// are created by the DISPATCHER at runtime (not by static manifests),
/// so the daemon-refresh kind-load alone doesn't reach an
/// already-running pod: it must be rolled explicitly.
///
/// `rollout restart` has no `--all-namespaces`, so we list the
/// (namespace, name) pairs by role label cluster-wide, then roll each
/// in its namespace. Graceful (rolling, not a hard pod-delete).
/// Best-effort: a failure doesn't fail the refresh (the next
/// reconcile catches it), but we log it loudly. `noun` is the
/// user-facing label for the summary line + the stale-pod warning.
async fn roll_role_deployments(role: &str, noun: &str) -> Result<()> {
    let out = kubectl(&[
        "get",
        "deployments",
        "--all-namespaces",
        "-l",
        &format!("weft.dev/role={role}"),
        "-o",
        "jsonpath={range .items[*]}{.metadata.namespace} {.metadata.name}{\"\\n\"}{end}",
    ])
    .output()
    .await?;
    if !out.status.success() {
        tracing::warn!(
            target: "weft_cli::daemon",
            "listing {noun} deployments failed; skipping {noun} roll"
        );
        return Ok(());
    }
    let listing = String::from_utf8_lossy(&out.stdout);
    let pairs: Vec<(String, String)> = listing
        .lines()
        .filter_map(|line| {
            line.trim().split_once(' ').map(|(ns, name)| (ns.to_string(), name.to_string()))
        })
        .collect();
    // Roll every matching pod in parallel: independent kubectl calls
    // across different namespaces, no shared state.
    let tasks = pairs.iter().map(|(ns, name)| async move {
        let status = kubectl(&["-n", ns, "rollout", "restart", &format!("deployment/{name}")])
            .status()
            .await?;
        if !status.success() {
            tracing::warn!(
                target: "weft_cli::daemon",
                "rollout restart {ns}/{name} failed; tenant may run a stale {noun} \
                 until its pod restarts"
            );
            return Ok::<bool, anyhow::Error>(false);
        }
        Ok(true)
    });
    let results = futures::future::try_join_all(tasks).await?;
    let rolled = results.into_iter().filter(|ok| *ok).count();
    if rolled > 0 {
        println!("rolled {rolled} {noun} deployment(s)");
    }
    Ok(())
}

async fn start_port_forwards() -> Result<()> {
    let cfg = cluster_config();
    fs::create_dir_all(data_dir())?;
    for pf in port_forwards(cfg).await {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(pf_log_file(pf.name))?;
        let err = log.try_clone()?;
        let child = std::process::Command::new("kubectl")
            .args([
                "--context", &cfg.kube_context,
                "-n", &pf.namespace,
                "port-forward", &format!("svc/{}", pf.service),
                &format!("{}:{}", pf.local_port, pf.remote_port),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err))
            .spawn()
            .with_context(|| format!("spawn kubectl port-forward ({})", pf.name))?;
        fs::write(data_dir_pid_file(pf.name), child.id().to_string())?;
    }
    Ok(())
}

async fn wait_for_http(url: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("{url} did not become reachable within 30s");
        }
        if let Ok(r) = client.get(url).send().await {
            if r.status().is_success() {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
}

// ----- Low-level helpers --------------------------------------------

/// Build a kubectl Command pinned to the configured context so
/// the user's current-context never interferes.
fn kubectl(args: &[&str]) -> Command {
    let cfg = cluster_config();
    let mut cmd = Command::new("kubectl");
    cmd.arg("--context").arg(&cfg.kube_context);
    cmd.args(args);
    cmd
}

async fn kubectl_apply_file(path: &Path) -> Result<()> {
    kubectl_apply_changed(path, &[]).await.map(|_| ())
}

async fn kubectl_apply_templated(path: &Path, vars: &[(&str, String)]) -> Result<()> {
    kubectl_apply_changed(path, vars).await.map(|_| ())
}

/// Read a manifest and substitute its `${VAR}` placeholders; a
/// placeholder left over after substitution is a loud error, never a
/// literal handed to kubectl. THE one renderer for every path that
/// parses a templated manifest (apply and delete alike).
async fn render_manifest(path: &Path, vars: &[(&str, String)]) -> Result<String> {
    let mut manifest = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| anyhow::anyhow!("read {}: {e}", path.display()))?;
    for (key, value) in vars {
        manifest = manifest.replace(&format!("${{{key}}}"), value);
    }
    if let Some(idx) = manifest.find("${") {
        let tail = &manifest[idx..(idx + 40).min(manifest.len())];
        anyhow::bail!(
            "unsubstituted placeholder in {} near `{tail}`: pass it in `vars`",
            path.display()
        );
    }
    Ok(manifest)
}

/// Delete every resource a templated manifest declares, checked: a
/// deletion that silently fails would leave resources running that the
/// operator was just told are gone.
async fn kubectl_delete_rendered(path: &Path, vars: &[(&str, String)]) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let manifest = render_manifest(path, vars).await?;
    let mut child = kubectl(&["delete", "-f", "-", "--ignore-not-found"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| anyhow::anyhow!("spawn kubectl delete: {e}"))?;
    child
        .stdin
        .take()
        .expect("stdin piped")
        .write_all(manifest.as_bytes())
        .await
        .map_err(|e| anyhow::anyhow!("write manifest to kubectl stdin: {e}"))?;
    let out = child.wait_with_output().await?;
    if !out.status.success() {
        anyhow::bail!(
            "kubectl delete ({}) failed: {}",
            path.display(),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

/// Ship `deploy/public-page/` (the static page the proxy serves on
/// the tunnel's bare root) into the cluster as the `weft-public-page`
/// ConfigMap: text files as plain data, everything else as binary.
/// Returns whether the content changed since the last apply (the
/// proxy pod mounts the files at start, so a change needs its roll).
async fn apply_public_page_configmap(page_dir: &Path) -> Result<bool> {
    use base64::Engine as _;
    let mut data = serde_json::Map::new();
    let mut binary = serde_json::Map::new();
    let entries = std::fs::read_dir(page_dir)
        .map_err(|e| anyhow::anyhow!("read the public page dir {}: {e}", page_dir.display()))?;
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let bytes = std::fs::read(entry.path())?;
        match String::from_utf8(bytes) {
            Ok(text) => {
                data.insert(name, serde_json::Value::String(text));
            }
            Err(raw) => {
                binary.insert(
                    name,
                    serde_json::Value::String(
                        base64::engine::general_purpose::STANDARD.encode(raw.into_bytes()),
                    ),
                );
            }
        }
    }
    anyhow::ensure!(
        data.contains_key("index.html"),
        "{} has no index.html; the proxy's root page needs one",
        page_dir.display()
    );
    let configmap = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "weft-public-page", "namespace": "weft-system" },
        "data": data,
        "binaryData": binary,
    })
    .to_string();
    kubectl_apply_stdin(&configmap, "weft-public-page configmap").await?;
    Ok(manifest_apply_changed_by_stamp(Path::new("weft-public-page"), &configmap))
}

/// Delete the named tunnel's token Secret, checked (missing is fine;
/// a failed delete of a credential is not).
async fn delete_tunnel_token_secret() -> Result<()> {
    let out = kubectl(&[
        "-n",
        "weft-system",
        "delete",
        "secret",
        "weft-tunnel-token",
        "--ignore-not-found",
    ])
    .output()
    .await?;
    if !out.status.success() {
        anyhow::bail!(
            "deleting the weft-tunnel-token secret failed: {}\n\
             Remove it by hand: kubectl -n weft-system delete secret weft-tunnel-token",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

/// The one `kubectl apply` path: renders the manifest, pipes it to
/// `kubectl apply -f -`, and reports whether the CONTENT changed since
/// the last apply (the stamp below), which is what decides pod
/// rollouts.
async fn kubectl_apply_changed(path: &Path, vars: &[(&str, String)]) -> Result<bool> {
    let manifest = render_manifest(path, vars).await?;
    kubectl_apply_stdin(&manifest, &path.display().to_string()).await?;
    // "changed" is decided by the CONTENT we applied, not kubectl's verb. Some
    // server-defaulted resources (the Envoy-Gateway HTTPRoute) report
    // "configured" on EVERY apply even when our manifest is byte-identical,
    // which would force a needless image-reload + dispatcher restart on every
    // `daemon restart`. We stamp the substituted manifest's hash per file and
    // call it changed only when that hash differs from the last apply (or no
    // stamp exists yet). We still apply unconditionally above (cheap, keeps the
    // cluster in sync); only the change SIGNAL is stamp-gated.
    Ok(manifest_apply_changed_by_stamp(path, &manifest))
}

/// Pipe a manifest to `kubectl apply -f -`. `what` names the manifest in
/// errors (a file path, or a description for generated manifests).
async fn kubectl_apply_stdin(manifest: &str, what: &str) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut child = kubectl(&["apply", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| anyhow::anyhow!("spawn kubectl apply: {e}"))?;
    child
        .stdin
        .take()
        .expect("stdin piped")
        .write_all(manifest.as_bytes())
        .await
        .map_err(|e| anyhow::anyhow!("write manifest to kubectl stdin: {e}"))?;
    let out = child.wait_with_output().await?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        anyhow::bail!("kubectl apply ({what}) failed: {stderr}");
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    print!("{stdout}");
    Ok(())
}

/// Pack `CREDENTIAL_ENCRYPTION_KEY` (the access store's at-rest sealing
/// key, from the shell or the `.env` the CLI loaded) into the
/// `weft-sealing-key` Secret. Dispatcher and broker both open sealed
/// rows, so it is applied in BOTH namespaces (Secrets are
/// namespace-scoped) on every daemon start/restart. Provider API keys
/// are NOT env-packed: the runtime's own keys are `api_key` entries in
/// the shared-credentials file, which ships whole via
/// [`apply_access_apps_secret`]. Returns whether the key changed since
/// the last apply (pods read env at start, so a change needs a rollout).
async fn apply_sealing_key_secret(cfg: &ClusterConfig) -> Result<bool> {
    let keys: std::collections::BTreeMap<String, String> = std::env::vars()
        .filter(|(name, value)| name == "CREDENTIAL_ENCRYPTION_KEY" && !value.is_empty())
        .collect();
    let mut changed = false;
    for namespace in [&cfg.db_namespace, &cfg.system_namespace] {
        let secret = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": { "name": "weft-sealing-key", "namespace": namespace },
            "type": "Opaque",
            "stringData": &keys,
        })
        .to_string();
        kubectl_apply_stdin(&secret, "weft-sealing-key secret").await?;
        changed |= manifest_apply_changed_by_stamp(
            Path::new(&format!("sealing-key-secret-{namespace}")),
            &secret,
        );
    }
    Ok(changed)
}

/// The OAuth apps file, packed into the `weft-access-apps`
/// Secret the broker mounts. `WEFT_ACCESS_APPS_FILE` (the shell, or the
/// `.env` the CLI loaded) names it; the default is `access-apps.json`
/// in the working directory. Absent = an empty secret, so the broker
/// simply has no apps configured and every service needs a
/// project-declared one. Returns whether the secret's content changed
/// since the last apply. The kubelet's in-place mount refresh only
/// works for a secret that already existed when the pod started; a
/// secret created AFTER the pod started never mounts into the running
/// pod, so a change here must roll the broker.
/// A file that IS named but unreadable is loud:
/// silently shipping no apps would surface later as a confusing
/// "no app configured" on a node the operator thought was set up.
async fn apply_access_apps_secret(cfg: &ClusterConfig) -> Result<bool> {
    let path = std::env::var(weft_core::access::spec::APPS_FILE_ENV)
        .ok()
        .filter(|p| !p.trim().is_empty())
        .unwrap_or_else(|| "access-apps.json".to_string());
    let explicit = std::env::var(weft_core::access::spec::APPS_FILE_ENV).is_ok();
    let contents = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !explicit => String::new(),
        Err(e) => {
            return Err(anyhow::Error::new(e)).with_context(|| {
                format!("read access apps file '{path}' (from WEFT_ACCESS_APPS_FILE)")
            })
        }
    };
    let secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "weft-access-apps", "namespace": cfg.db_namespace },
        "type": "Opaque",
        "stringData": { ACCESS_APPS_SECRET_KEY: contents },
    })
    .to_string();
    kubectl_apply_stdin(&secret, "weft-access-apps secret").await?;
    Ok(manifest_apply_changed_by_stamp(Path::new("access-apps-secret"), &secret))
}

/// The key the apps json is stored under in the secret, and therefore
/// the file name it mounts as in the broker pod.
// SYNC: ACCESS_APPS_SECRET_KEY <-> deploy/k8s/broker.yaml (volume subPath +
//       WEFT_ACCESS_APPS_FILE)
const ACCESS_APPS_SECRET_KEY: &str = "access-apps.json";

/// Per-manifest content stamp: returns true iff `manifest` differs from the
/// last applied content for `path` (or there is no prior stamp). Mirrors the
/// image-build stamp pattern. A stamp-write failure conservatively returns
/// true (treat as changed) so we never SKIP a real rollout on an I/O hiccup.
/// Where a manifest's last-applied content hash is stamped. THE one
/// place the naming convention lives, so the teardown that removes a
/// stamp and the stamper that writes it cannot drift apart.
fn manifest_stamp_file(path: &Path) -> PathBuf {
    let stem = path
        .file_name()
        .map(|s| s.to_string_lossy().replace(['/', ':'], "_"))
        .unwrap_or_else(|| "manifest".into());
    data_dir().join("manifest-stamps").join(format!("{stem}.hash"))
}

fn manifest_apply_changed_by_stamp(path: &Path, manifest: &str) -> bool {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(manifest.as_bytes());
    let want = format!("{:x}", hasher.finalize());
    let stamp = manifest_stamp_file(path);
    let have = std::fs::read_to_string(&stamp).ok().map(|s| s.trim().to_string());
    if have.as_deref() == Some(want.as_str()) {
        return false;
    }
    if let Some(parent) = stamp.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if std::fs::write(&stamp, &want).is_err() {
        // Couldn't persist the stamp: treat as changed so a real change is
        // never silently dropped (the next run will also see "changed").
        return true;
    }
    true
}

/// True iff a `kubectl apply` stdout reports at least one changed
/// resource. Lines look like `networkpolicy.../foo created` /
/// `configured` / `unchanged`; only `unchanged` is a no-op.
async fn require_binary(name: &str) -> Result<()> {
    let out = Command::new("which").arg(name).output().await;
    if matches!(out, Ok(o) if o.status.success()) {
        return Ok(());
    }
    anyhow::bail!("`{name}` not found on PATH. Install it and retry.");
}

fn read_pid(pid_file: &Path) -> Option<i32> {
    fs::read_to_string(pid_file).ok()?.trim().parse().ok()
}

fn process_alive(pid: i32) -> bool {
    unsafe {
        extern "C" {
            fn kill(pid: i32, sig: i32) -> i32;
        }
        kill(pid, 0) == 0
    }
}

fn signal_term(pid: i32) -> Result<()> {
    unsafe {
        extern "C" {
            fn kill(pid: i32, sig: i32) -> i32;
        }
        if kill(pid, 15) != 0 {
            return Err(anyhow::anyhow!("kill SIGTERM failed"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        apiserver_clusterip, canonical_tunnel_hostname, check_cidr, configured_dns_ip,
        endpoint_host, EndpointHost,
    };

    #[test]
    fn tunnel_hostname_accepts_only_a_bare_https_host() {
        assert_eq!(
            canonical_tunnel_hostname("https://weft.example.com").expect("bare host"),
            "https://weft.example.com"
        );
        // A trailing slash is the same address, normalized away.
        assert_eq!(
            canonical_tunnel_hostname("https://weft.example.com/").expect("trailing slash"),
            "https://weft.example.com"
        );
        for bad in [
            "http://weft.example.com",     // not https
            "https://weft.example.com/x",  // a path would double-join
            "https://weft.example.com?x=1", // query
            "https://weft.example.com:8443", // named tunnels serve on 443
            "https://localhost",           // no dot: not a public hostname
            "not a url",
        ] {
            assert!(canonical_tunnel_hostname(bad).is_err(), "'{bad}' must refuse");
        }
    }

    #[test]
    fn rejects_unparseable_cidr() {
        assert!(check_cidr("not-a-cidr", false).is_err());
        assert!(check_cidr("10.0.0.0/99", false).is_err());
    }

    #[test]
    fn strict_accepts_tight_private_ranges() {
        // kind default + the common private Service CIDRs.
        assert!(check_cidr("10.96.0.0/12", true).is_ok());
        assert!(check_cidr("172.20.0.0/16", true).is_ok());
        assert!(check_cidr("192.168.0.0/16", true).is_ok());
    }

    #[test]
    fn strict_rejects_the_containment_breach_cases() {
        // The whole point: a broad / public CIDR here would open the
        // broker's apiserver-egress NetworkPolicy to the internet.
        assert!(check_cidr("0.0.0.0/0", true).is_err());
        assert!(check_cidr("10.0.0.0/4", true).is_err()); // prefix too short
        assert!(check_cidr("8.8.8.0/24", true).is_err()); // public range
    }

    #[test]
    fn non_strict_allows_any_valid_cidr() {
        // The pod CIDR isn't the containment boundary; only parse-check it.
        assert!(check_cidr("0.0.0.0/0", false).is_ok());
        assert!(check_cidr("10.244.0.0/16", false).is_ok());
    }

    #[test]
    fn apiserver_clusterip_is_first_address_of_service_cidr() {
        // The broker egress NetworkPolicy is scoped to this /32, so it
        // must be the apiserver's real ClusterIP (network + 1).
        assert_eq!(apiserver_clusterip("10.96.0.0/12").unwrap(), "10.96.0.1");
        assert_eq!(apiserver_clusterip("172.20.0.0/16").unwrap(), "172.20.0.1");
        // IPv6: same network+1 derivation (the egress /32 is security-critical).
        assert_eq!(apiserver_clusterip("fd00::/108").unwrap(), "fd00::1");
        assert!(apiserver_clusterip("garbage").is_err());
    }

    /// A resolver address nginx would accept as a token but never
    /// resolve with is the exact failure the directive exists to
    /// prevent, so a configured one is parsed rather than trusted.
    #[test]
    fn a_configured_dns_address_must_be_an_ip() {
        assert_eq!(configured_dns_ip("10.96.0.10").unwrap().as_deref(), Some("10.96.0.10"));
        assert_eq!(configured_dns_ip("  10.96.0.10  ").unwrap().as_deref(), Some("10.96.0.10"));
        assert_eq!(configured_dns_ip("fd00::a").unwrap().as_deref(), Some("fd00::a"));
        // Unset means "ask the cluster", which is the normal path.
        assert!(configured_dns_ip("").unwrap().is_none());
        assert!(configured_dns_ip("   ").unwrap().is_none());
        // A hostname, an address with a port, and a typo all read as
        // valid nginx tokens and none of them resolve.
        for bad in ["kube-dns.kube-system.svc", "10.96.0.10:53", "10.96.0", "hello"] {
            let e = configured_dns_ip(bad).expect_err("{bad} must be refused");
            assert!(e.to_string().contains("WEFT_CLUSTER_DNS_IP"), "{e}");
        }
    }

    #[test]
    fn endpoint_host_classifies_every_shape() {
        let ip = |s: &str| s.parse::<std::net::Ipv4Addr>().unwrap();
        // The kind-gateway shape the local daemon derives.
        assert_eq!(
            endpoint_host("http://172.19.0.1:9096"),
            EndpointHost::PrivateIpv4(ip("172.19.0.1"))
        );
        assert_eq!(
            endpoint_host("http://10.1.2.3:9000/path"),
            EndpointHost::PrivateIpv4(ip("10.1.2.3"))
        );
        assert_eq!(
            endpoint_host("http://192.168.0.9"),
            EndpointHost::PrivateIpv4(ip("192.168.0.9"))
        );
        assert_eq!(
            endpoint_host("http://100.64.0.1:9000"),
            EndpointHost::PrivateIpv4(ip("100.64.0.1"))
        );
        // Public addresses re-permit nothing.
        assert_eq!(endpoint_host("https://8.8.8.8:443"), EndpointHost::PublicIpv4);
        assert_eq!(endpoint_host("http://100.128.0.1:9000"), EndpointHost::PublicIpv4);
        // A hostname keeps its name (the caller may resolve it).
        assert_eq!(
            endpoint_host("https://s3.amazonaws.com"),
            EndpointHost::Hostname("s3.amazonaws.com".into())
        );
        // IPv6: bracketed and bare literals both classify as IPv6
        // (a bracketed one must not mis-split on ':').
        assert_eq!(
            endpoint_host("http://[fd00::1]:9096"),
            EndpointHost::Ipv6("fd00::1".into())
        );
        assert_eq!(endpoint_host("http://fd00::1"), EndpointHost::Ipv6("fd00::1".into()));
        // Userinfo is dropped before classification: with a password
        // the authority holds two ':' and must not be misread as a
        // bare IPv6 literal.
        assert_eq!(
            endpoint_host("http://user@10.0.0.5:9000"),
            EndpointHost::PrivateIpv4("10.0.0.5".parse().unwrap())
        );
        assert_eq!(
            endpoint_host("http://user:pw@10.0.0.5:9000"),
            EndpointHost::PrivateIpv4("10.0.0.5".parse().unwrap())
        );
    }
}
