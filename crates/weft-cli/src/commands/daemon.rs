//! `weft daemon start|stop|status|restart|logs`. Owns the kind
//! cluster lifecycle and the dispatcher deployment inside it.
//!
//! Everything runs as Pods: dispatcher, listener, worker, infra.
//! `start` and `restart` are one reconcile (see `reconcile`); on kind
//! the shared images land in the node via `kind load docker-image`, on
//! a registry-backed cluster the same manifests pull them instead.

use std::fs;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::Engine as _;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::sleep;

use super::Ctx;
use crate::images;

/// Cluster / namespace / port config the CLI talks to.
/// `system_namespace` holds the dispatcher Pod, its Service, PVC and
/// Ingress; `db_namespace` holds postgres + broker. Per-project
/// namespaces are created by the dispatcher at first infra apply.
pub struct ClusterConfig {
    pub cluster_name: String,
    pub kube_context: String,
    pub system_namespace: String,
    pub db_namespace: String,
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
    static CFG: OnceLock<ClusterConfig> = OnceLock::new();
    CFG.get_or_init(ClusterConfig::from_env)
}

impl ClusterConfig {
    pub fn from_env() -> Self {
        let cluster_name = std::env::var("WEFT_CLUSTER_NAME")
            .unwrap_or_else(|_| "weft-local".into());
        let kube_context = std::env::var("WEFT_KUBE_CONTEXT")
            .unwrap_or_else(|_| format!("kind-{cluster_name}"));
        // Constants, not env knobs (see weft_core::infra's namespace
        // constants for why). The fields stay so call sites read one
        // authority.
        let system_namespace = weft_core::infra::SYSTEM_NAMESPACE.to_string();
        let db_namespace = weft_core::infra::DB_NAMESPACE.to_string();
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
        // apply, in `manifest_template_vars`), not at config load: a
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
    imgs: &images::SystemImages,
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
        // The derived default only exists on kind (the store is a host
        // docker container reached via the kind network's gateway); a
        // real cluster has no such address to derive, so absence there
        // is a configuration error, named instead of surfacing as a
        // "docker network inspect kind failed".
        Err(_) => match cfg.backend {
            ClusterBackend::Kind => {
                format!("http://{}:{}", kind_network_gateway_ipv4().await?, cfg.seaweed_port)
            }
            ClusterBackend::K8s => anyhow::bail!(
                "WEFT_OBJECT_STORE_ENDPOINT is required for the k8s backend; set it to \
                 the S3 endpoint the cluster reaches"
            ),
        },
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

    // The content-addressed image refs the manifests pin, one var per
    // service, named by the same `image_env` the override read uses so
    // the substitution and the override can never disagree on a name.
    // Rolling is free with these: a new tag IS a spec diff, so the
    // apply itself rolls the pod.
    let mut vars: Vec<(&'static str, String)> = images::SystemService::ALL
        .iter()
        .map(|&svc| (svc.image_env(), imgs.get(svc).to_string()))
        .collect();
    vars.extend([
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
    ]);
    Ok(vars)
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
    /// One verb for boot AND refresh (`restart` is a CLI alias): the
    /// reconcile below is idempotent, so "the daemon was fully up" and
    /// "nothing exists yet" are just states it converges from.
    Start { rebuild: bool, rebuild_cluster: bool, public_url: Option<bool> },
    Stop,
    Status,
    Logs { tail: usize, follow: bool },
}

pub async fn run(ctx: Ctx, action: DaemonAction) -> Result<()> {
    match action {
        DaemonAction::Start { rebuild, rebuild_cluster, public_url } => {
            set_public_url_choice(public_url)?;
            reconcile(rebuild, rebuild_cluster).await
        }
        DaemonAction::Stop => stop().await,
        DaemonAction::Status => status(&ctx).await,
        DaemonAction::Logs { tail, follow } => logs(tail, follow).await,
    }
}

/// THE daemon boot: `daemon start` and `daemon restart` both run this
/// one reconcile. Make the shared images exist (present -> pull ->
/// build), load them into the node, apply every manifest and secret in
/// the canonical order (readiness gates are no-ops when already up),
/// roll what the detected changes demand, persist the change stamps
/// only after those rolls landed, then bring the pooled tiers onto the
/// resolved refs and reclaim stale image tags.
///
/// One path on purpose: the two verbs once carried separate copies of
/// the apply sequence, and the start-side copy recorded a secret
/// change as handled without rolling the broker (a stopped daemon plus
/// a rotated sealing key reached exactly that copy via setup.sh).
/// Every step is idempotent, so "the daemon was fully up" and "the
/// cluster does not even exist yet" are both just states this
/// converges from.
async fn reconcile(rebuild: bool, rebuild_cluster: bool) -> Result<()> {
    let cfg = cluster_config();
    require_binary("kubectl").await?;
    require_binary("docker").await?;
    // The port-forward pid verification shells out to ps on every
    // reconcile; missing ps must fail here, not mid-restart.
    require_binary("ps").await?;

    // On the k8s backend a local rebuild has no route into the
    // cluster: there is no kind node to load into and this path never
    // pushes. Refuse up front instead of burning a build and then
    // restarting three tiers onto exactly the bytes they already run.
    if rebuild && cfg.backend == ClusterBackend::K8s {
        anyhow::bail!(
            "--rebuild only reaches a kind cluster; for the k8s backend, rebuild and \
             publish with `WEFT_IMAGE_REGISTRY=<your registry> weft build-images --push` \
             (the same variable the manifests template the image refs from, so the \
             cluster pulls what you pushed), then re-run `weft daemon start`."
        );
    }

    // The cluster + its ingress controller + the Envoy Gateway
    // controller before anything else (gateway.yaml's CRs need Envoy's
    // CRDs; everything needs a cluster to apply into). All idempotent:
    // a no-op once present, which is what makes this self-healing over
    // a missing/partial cluster (a fresh machine, a `kind delete`, a
    // daemon process alive while its cluster is gone).
    if cfg.backend == ClusterBackend::Kind {
        require_binary("kind").await?;
        ensure_cluster(cfg, rebuild_cluster).await?;
        // The object store is a HOST docker container the cluster
        // reaches OUT to (the local stand-in for a real S3 provider);
        // up before anything that needs a bucket.
        ensure_object_store(cfg).await?;
        ensure_ingress_controller().await?;
        ensure_envoy_gateway().await?;
    }

    let imgs = provision_images(rebuild).await?;

    // The manifests applied below name the new content-addressed tags,
    // and the pods roll onto them the moment they are applied, so the
    // images must be inside the node FIRST or the rolled pods sit in
    // ErrImagePull until a load races them. Content tags skip the load
    // when the node already holds them; a forced rebuild changed the
    // BYTES under unchanged tags, so it must force the load too.
    if cfg.backend == ClusterBackend::Kind {
        tokio::try_join!(
            images::kind_load(&cfg.cluster_name, &imgs.dispatcher, rebuild),
            images::kind_load(&cfg.cluster_name, &imgs.listener, rebuild),
            images::kind_load(&cfg.cluster_name, &imgs.broker, rebuild),
            images::kind_load(&cfg.cluster_name, &imgs.supervisor, rebuild),
        )?;
    }

    // Apply everything. Stamps queue on `pending` and flush only after
    // the rollout block below: a run that dies mid-way re-detects and
    // re-rolls next time (over-rolling is a cheap restart;
    // under-rolling once left a stale broker silently stripping
    // journal fields for a day).
    let mut pending = PendingStamps::default();
    let changes = apply_platform_state(cfg, &imgs, &mut pending).await?;

    // The dispatcher exists (or was scaled back up) after the apply,
    // and a spec change (a new image tag rides the manifest) is
    // already rolling; wait before deciding the explicit rolls so a
    // `rollout restart` below never races a pod still being created.
    wait_workload_ready("statefulset", "weft-dispatcher", &cfg.system_namespace).await?;

    // One linear pass, so no combination of change flags can skip a
    // step it needed: the dispatcher roll, the broker roll and the
    // forward reconcile each fire on their own condition (see
    // `rolls_for` for what triggers each), never on a branch agreeing
    // with two others.
    let rolls = rolls_for(&changes, rebuild, all_forward_pids_live(cfg).await);
    if rolls.dispatcher {
        roll_workload("statefulset", "weft-dispatcher", &cfg.system_namespace).await?;
    }
    if rolls.broker {
        roll_workload("deployment", "weft-broker", &cfg.db_namespace).await?;
    }
    // The port-forward is bound to a single Pod IP, so a dispatcher
    // recreate (the apply's own roll on a dispatcher.yaml change, or
    // the explicit roll above) kills it; and even with nothing rolled,
    // a background `kubectl port-forward` dies if its pod restarts out
    // of band or its port is squatted, AND the GATEWAY forward is
    // SKIPPED on the very first boot (the Envoy Gateway Service is not
    // programmed yet). Every boot must leave the COMPLETE forward set
    // working, so `rolls.forwards` checks EVERY desired forward's pid,
    // not just the dispatcher's.
    let mut forwards_restarted = false;
    if rolls.forwards {
        restart_port_forwards().await?;
        forwards_restarted = true;
    }
    // Reachability is the gate, never pid liveness alone: a recorded
    // pid can be live and useless (a reboot recycled it into an
    // unrelated process; a kubectl wedged on a gone pod), so every
    // boot ends by probing /health. A failed probe over forwards this
    // run did NOT rebuild gets one rebuild-and-reprobe; a failure
    // after a rebuild is real and bails.
    let health = format!("http://127.0.0.1:{}/health", cfg.dispatcher_port);
    if let Err(probe) = wait_for_dispatcher_health(&health).await {
        if rolls.forwards {
            return Err(probe);
        }
        restart_port_forwards().await?;
        forwards_restarted = true;
        wait_for_dispatcher_health(&health).await?;
    }
    // The summary says what actually happened: applies and rolls are
    // different facts (a manifest change is applied but may roll
    // nothing here; a --rebuild rolls everything with no stamp moving),
    // and a line deduced from the wrong one lies in both directions.
    let mut applied: Vec<&str> = Vec::new();
    if changes.dispatcher_manifest || changes.postgres_manifest || changes.other_manifests {
        applied.push("manifests");
    }
    if changes.sealing_key {
        applied.push("sealing key");
    }
    if changes.apps {
        applied.push("shared-credentials apps");
    }
    if rebuild {
        applied.push("rebuilt images");
    }
    let mut rolled: Vec<&str> = Vec::new();
    if rolls.dispatcher {
        rolled.push("dispatcher");
    }
    // A dispatcher.yaml change gets no summary entry of its own: the
    // apply only recreates the pod when the change touched the pod
    // template (a Service-only edit rolls nothing), so a deduced
    // "rolled by its apply" line would lie on those edits. "applied:
    // manifests" already covers what happened.
    if rolls.broker {
        rolled.push("broker");
    }
    if forwards_restarted {
        rolled.push("port-forwards");
    }
    if applied.is_empty() && rolled.is_empty() {
        println!("daemon already on the latest images and manifests; nothing to roll");
    } else if rolled.is_empty() {
        println!("daemon refreshed; applied: {}", applied.join(", "));
    } else if applied.is_empty() {
        println!("daemon refreshed; rolled: {}", rolled.join(", "));
    } else {
        println!(
            "daemon refreshed; applied: {}; rolled: {}",
            applied.join(", "),
            rolled.join(", ")
        );
    }

    // Every roll the detected changes demanded has completed: persist
    // the change stamps.
    pending.flush();

    // The pooled listener / supervisor Deployments were rendered by the
    // dispatcher with the image IT knew at spawn time, so an image
    // change leaves them pointing at the old tag until someone updates
    // their specs. Reconcile them on every boot (a no-op when nothing
    // moved), which also heals a boot that died between the apply
    // above and here.
    let pooled_failures = reconcile_pooled_images(&imgs, rebuild).await;
    // Reclaim other tags of the system repos ONLY once every pooled
    // Deployment SPEC provably names a current ref: an unpatched spec
    // still needs its old image to restart its pod, and a locally
    // built tag exists nowhere else. (Specs, not running pods: a
    // rolling update's outgoing pods hold their image busy, so the
    // warn-only removal skips them.)
    if pooled_failures.is_empty() {
        let kind_cluster =
            (cfg.backend == ClusterBackend::Kind).then_some(cfg.cluster_name.as_str());
        images::gc_stale_system_images(kind_cluster, &imgs).await;
    }

    // A boot that leaves the pooled tiers unproven must not print the
    // ready line over it: the core is up, but "ready" would bury the
    // one signal that something may still run old bytes. Each line
    // says what it is (a failed patch, a roll that never became
    // Ready, or a listing this pass could not read at all).
    anyhow::ensure!(
        pooled_failures.is_empty(),
        "the daemon is up at http://127.0.0.1:{}, but the pooled listener / \
         supervisor tiers could not be fully reconciled:\n  {}\nRe-run \
         `weft daemon start` to retry, and inspect with \
         `kubectl --context {} get deploy -A -l weft.dev/role -o wide`.",
        cfg.dispatcher_port,
        pooled_failures.join("\n  "),
        cfg.kube_context,
    );
    let backend = match cfg.backend {
        ClusterBackend::Kind => "kind",
        ClusterBackend::K8s => "k8s",
    };
    println!(
        "daemon ready at http://127.0.0.1:{} ({} cluster '{}', system ns '{}')",
        cfg.dispatcher_port, backend, cfg.cluster_name, cfg.system_namespace,
    );
    Ok(())
}

/// Make every shared image exist locally (present -> pull -> build)
/// via `images::ensure_all_shared_images` (builder-base failures only
/// warn here: it is a pre-warm for future `weft run`s, which re-ensure
/// it with the user present), wrapped in a 15s TTY breadcrumb so the
/// long first build on a clean machine stays legible.
async fn provision_images(rebuild: bool) -> Result<images::SystemImages> {
    let tty = std::io::stderr().is_terminal();
    let started = std::time::Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_secs(15));
    ticker.tick().await; // consume the immediate first tick
    let mut ensure = std::pin::pin!(images::ensure_all_shared_images(
        rebuild,
        images::BaseFailure::Warn,
        None,
    ));
    loop {
        tokio::select! {
            res = &mut ensure => {
                return Ok(res?.system);
            }
            _ = ticker.tick() => {
                if tty {
                    let elapsed = started.elapsed().as_secs();
                    let _ = writeln!(
                        std::io::stderr(),
                        "still preparing shared images ({elapsed}s elapsed; a first build on a clean machine takes 5-10 min, a pull far less)"
                    );
                }
            }
        }
    }
}

/// What one apply pass detected as changed since the last COMPLETED
/// apply-and-roll (content stamps, not kubectl's verb; see
/// `kubectl_apply_changed`), plus the caller's forced-rebuild intent
/// (`--rebuild` re-made the images under their unchanged content
/// tags, so no stamp can see it; it rides this struct so `rolls_for`
/// decides from ONE input).
struct DetectedChanges {
    /// dispatcher.yaml moved: its apply recreates the dispatcher pod,
    /// which kills the pod-bound forwards.
    dispatcher_manifest: bool,
    /// postgres.yaml moved: it carries the database-credentials
    /// Secret, which BOTH the dispatcher and the broker read as env
    /// (`secretKeyRef` -> WEFT_DATABASE_URL) injected only at pod
    /// start, invisible to either pod spec. Any postgres.yaml change
    /// rolls both (over-rolling on a non-credential edit is a cheap
    /// restart; under-rolling once left services on a dead URL).
    postgres_manifest: bool,
    /// Any other manifest moved (namespaces, broker, ingress, RBAC,
    /// gateway): applied, and either self-rolling (a spec change) or
    /// effective with no restart (policies, routes); feeds only the
    /// summary line.
    other_manifests: bool,
    sealing_key: bool,
    apps: bool,
}

/// What one boot owes beyond the applies themselves. The two secrets
/// and the postgres credentials are invisible to pod specs (env read
/// or mount at pod start), so each forces its service(s); a forced
/// rebuild (`rebuilt`, the caller's --rebuild intent, not a stamp)
/// changed image BYTES under unchanged tags, so it forces everything.
/// A dispatcher SPEC change is deliberately NOT a dispatcher-roll
/// trigger: the apply itself rolls it and is gated Ready before
/// `rolls_for` runs; re-adding it here would roll the dispatcher
/// twice on every code edit. `forwards` says whether the port-forward
/// set must be torn down and re-spawned: a dispatcher pod recreate
/// (the apply on a dispatcher.yaml change, or the explicit roll)
/// kills the pod-bound forwards, and dead recorded pids mean the set
/// is already broken.
struct Rolls {
    dispatcher: bool,
    broker: bool,
    forwards: bool,
}

fn rolls_for(c: &DetectedChanges, rebuilt: bool, forward_pids_live: bool) -> Rolls {
    let dispatcher = c.sealing_key || c.postgres_manifest || rebuilt;
    Rolls {
        dispatcher,
        broker: c.sealing_key || c.apps || c.postgres_manifest || rebuilt,
        forwards: c.dispatcher_manifest || dispatcher || !forward_pids_live,
    }
}

/// Restart `kind/name` in `namespace` and hold until Ready: callers
/// flush change stamps on the strength of a roll having landed, so
/// returning before the gate would let a crash-looping new pod (old
/// ReplicaSet still serving old bytes) get recorded as done. Loud,
/// never swallowed: a broker left on old bytes once silently stripped
/// journal fields in transit for a day.
async fn roll_workload(kind: &str, name: &str, namespace: &str) -> Result<()> {
    let status = kubectl(&[
        "-n",
        namespace,
        "rollout",
        "restart",
        &format!("{kind}/{name}"),
    ])
    .status()
    .await?;
    anyhow::ensure!(status.success(), "{name} rollout restart failed");
    wait_workload_ready(kind, name, namespace).await
}

/// Block until `kind/name` in `namespace` is Ready, bailing with the
/// inspect commands after 180s. The recovery commands carry the
/// `--context`: every kubectl the daemon runs is pinned to it, so a
/// suggested command without it could hit whatever cluster the user's
/// current-context happens to be.
async fn wait_workload_ready(kind: &str, name: &str, namespace: &str) -> Result<()> {
    let status = kubectl(&[
        "-n",
        namespace,
        "rollout",
        "status",
        &format!("{kind}/{name}"),
        "--timeout=180s",
    ])
    .status()
    .await?;
    if !status.success() {
        let ctx = &cluster_config().kube_context;
        anyhow::bail!(
            "{name} did not reach Ready within 180s.\n\
             Inspect it: kubectl --context {ctx} -n {namespace} describe {kind}/{name}\n\
             and:        kubectl --context {ctx} -n {namespace} logs {kind}/{name}"
        );
    }
    Ok(())
}

/// Apply the whole platform state (every manifest in `deploy/k8s` +
/// the two secrets) in the one canonical order, with readiness gates
/// where a fresh cluster needs them (no-ops when already up). Change
/// detections queue their stamps on `pending`; the caller owes a
/// `flush` once the rolls those detections demand have completed.
async fn apply_platform_state(
    cfg: &ClusterConfig,
    imgs: &images::SystemImages,
    pending: &mut PendingStamps,
) -> Result<DetectedChanges> {
    let repo_root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let manifests = repo_root.join("deploy/k8s");
    let mut other_manifests = false;
    // The var set is computed ONCE (it is not free: a docker network
    // inspect, a DNS advisory lookup, and it prints the private-store
    // note). Only the tunnel address can change it, patched in below.
    let mut template_vars = manifest_template_vars(cfg, imgs, None).await?;
    // Namespaces first: everything below (the tunnel included) lands
    // inside them, and on a fresh cluster they do not exist yet. Their
    // placeholders (CIDRs + apiserver ClusterIP) never depend on the
    // tunnel, so the pre-tunnel var set is their final render.
    for name in ["system-namespace.yaml", "db-namespace.yaml"] {
        other_manifests |=
            kubectl_apply_changed(&manifests.join(name), &template_vars, pending).await?;
    }
    // The public tunnel next, when opted in: its minted address is an
    // ADDITIONAL internet-reachable door, substituted into the
    // dispatcher + broker manifests below. It never replaces the base
    // URL: everything local (the OAuth callback, storage links) keeps
    // the stable loopback address, and only the surfaces the open
    // internet must reach (event pushes, activation URLs) prefer the
    // tunnel. The tunnel must resolve BEFORE those manifests render,
    // or the dispatcher gets applied with an empty internet address,
    // silently un-wiring a tunnel that is still running (https
    // consents block, event pushes point at nothing).
    if let Some(url) = reconcile_public_tunnel(&manifests).await? {
        for (key, value) in template_vars.iter_mut() {
            if *key == "WEFT_DISPATCHER_INTERNET_URL" {
                *value = url.clone();
            }
        }
    }
    // Postgres before everything that needs a database, gated to Ready
    // so a fresh cluster's broker never crash-loops on a missing DB.
    // Its change flag is its OWN: postgres.yaml carries the database
    // credentials both the dispatcher and the broker read as env at
    // pod start (see `DetectedChanges::postgres_manifest`).
    prepare_postgres_apply(&manifests.join("postgres.yaml")).await?;
    let postgres_manifest =
        kubectl_apply_changed(&manifests.join("postgres.yaml"), &template_vars, pending).await?;
    wait_workload_ready("deployment", "weft-postgres", &cfg.db_namespace).await?;
    // Provider keys + OAuth apps before the broker: its pods import
    // the key secret via `envFrom` and MOUNT the apps secret at start,
    // so both must exist when a fresh pod comes up. A change to either
    // is invisible to the pod SPEC, which is why they carry their own
    // change signals (the caller rolls the broker on them).
    let sealing_key = apply_sealing_key_secret(cfg, pending).await?;
    let apps = apply_access_apps_secret(cfg, pending).await?;
    other_manifests |=
        kubectl_apply_changed(&manifests.join("broker.yaml"), &template_vars, pending).await?;
    wait_workload_ready("deployment", "weft-broker", &cfg.db_namespace).await?;
    // broker + dispatcher carry ${...} placeholders (CIDRs, images,
    // and for the dispatcher the public base URL + local-dev flag);
    // the rest go through the same applier with substitution as a
    // no-op. `cluster-rbac.yaml`: ClusterRoles for the POOLED
    // supervisor + listener pods (tenant-agnostic, in the
    // control-plane namespace), bound into project namespaces by
    // RoleBindings the dispatcher creates at first infra apply.
    // dispatcher.yaml's change flag is its own too: its apply
    // recreates the dispatcher pod, which the forward reconcile keys
    // on (see `DetectedChanges::dispatcher_manifest`).
    let dispatcher_manifest =
        kubectl_apply_changed(&manifests.join("dispatcher.yaml"), &template_vars, pending).await?;
    for name in [
        "ingress.yaml",
        "cluster-rbac.yaml",
        // Live caller connection gateway (Envoy Gateway CRs). Applied
        // after the controller install (`ensure_envoy_gateway`) so the
        // CRDs exist. `${GATEWAY_HOST}` is substituted from template vars.
        "gateway.yaml",
    ] {
        other_manifests |=
            kubectl_apply_changed(&manifests.join(name), &template_vars, pending).await?;
    }
    Ok(DetectedChanges {
        dispatcher_manifest,
        postgres_manifest,
        other_manifests,
        sealing_key,
        apps,
    })
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
    let cfg = cluster_config();
    let manifest = manifests.join("public-tunnel.yaml");
    // The manifest carries `${TUNNEL_ARGS}` (the mode's argv); any
    // kubectl that PARSES it needs the substitution, deletes included.
    let quick_args = format!(
        r#"["tunnel", "--no-autoupdate", "--url", "http://weft-public-proxy.{}.svc.cluster.local:8080"]"#,
        cfg.system_namespace
    );
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
            &cfg.system_namespace,
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
        Some(_) => r#"["tunnel", "--no-autoupdate", "run"]"#.to_string(),
        None => quick_args.clone(),
    };
    // The token Secret before the manifest in named mode, so a rolling
    // pod always finds it. Its content feeds the same change signal as
    // the manifest: a ROTATED token with an unchanged manifest must
    // still reach the pod, and env from a secretKeyRef is injected
    // only at pod start.
    // The tunnel's stamps are self-contained: the rolls the changes
    // demand happen inside this function, so they flush here too, right
    // after the readiness gates prove the rolls landed.
    let mut pending = PendingStamps::default();
    let mut secret_changed = false;
    if let Some((token, _)) = &named {
        let secret = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": { "name": "weft-tunnel-token", "namespace": cfg.system_namespace },
            "type": "Opaque",
            "stringData": { "TUNNEL_TOKEN": token },
        })
        .to_string();
        kubectl_apply_stdin(&secret, "weft-tunnel-token secret").await?;
        secret_changed =
            manifest_apply_changed_by_stamp(Path::new("weft-tunnel-token"), &secret, &mut pending);
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
    let page_changed = apply_public_page_configmap(&page_dir, &mut pending).await?;
    let manifest_changed = kubectl_apply_changed(
        &manifest,
        &[
            ("TUNNEL_ARGS", tunnel_args.to_string()),
            ("CLUSTER_DNS", cluster_dns_ip().await?),
        ],
        &mut pending,
    )
    .await?;
    if secret_changed {
        roll_workload("deployment", "weft-tunnel", &cfg.system_namespace).await?;
    }
    if manifest_changed || page_changed {
        roll_workload("deployment", "weft-public-proxy", &cfg.system_namespace).await?;
    }
    // Unconditional readiness gate, changed or not: the address
    // answered below is only meaningful while exactly one tunnel pod
    // is running and ready. Without this, the steady-state re-run
    // would print a confident address over a crash-looping pod (a
    // revoked token, an evicted node), and the quick-mode log read
    // below could hit a terminating pod's stale banner.
    wait_workload_ready("deployment", "weft-tunnel", &cfg.system_namespace).await?;
    // Both readiness gates passed: every roll the detected changes
    // demanded has landed, so the stamps can persist.
    pending.flush();
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
             WEFT_PUBLIC_TUNNEL_TOKEN + WEFT_PUBLIC_TUNNEL_HOSTNAME \
             (https://weavemindai.github.io/weft/connections/public-address.html)."
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
    let cfg = cluster_config();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        let pod = kubectl(&[
            "-n",
            &cfg.system_namespace,
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
                let cfg = cluster_config();
                anyhow::bail!(
                    "no running tunnel pod to read the minted address from.\n\
                     Inspect it: kubectl --context {} -n {} describe deployment/weft-tunnel",
                    cfg.kube_context,
                    cfg.system_namespace
                );
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        }
        let out = kubectl(&[
            "-n",
            &cfg.system_namespace,
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
            .split_whitespace().rfind(|w| w.starts_with("https://") && w.contains(".trycloudflare.com"))
        {
            return Ok(url.trim_end_matches('/').to_string());
        }
        if std::time::Instant::now() > deadline {
            let cfg = cluster_config();
            anyhow::bail!(
                "the tunnel pod reported no public address (its minting banner may \
                 have rotated out of the log).\n\
                 Mint a fresh one with: kubectl --context {} -n {} rollout restart \
                 deployment/weft-tunnel\n\
                 then re-run `weft daemon start` (and re-register the new address \
                 wherever the old one was registered).",
                cfg.kube_context,
                cfg.system_namespace
            );
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// A background `kubectl port-forward` the daemon owns. Each forward
/// tracks its own pid + log file (keyed by `name`) so they start,
/// stop, and report liveness independently.
struct PortForward {
    /// Which forward this is; `name.as_str()` keys the pid/log files.
    name: Forward,
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
        name: Forward::Dispatcher,
        namespace: cfg.system_namespace.clone(),
        service: "weft-dispatcher".to_string(),
        local_port: Forward::Dispatcher.local_port(cfg),
        remote_port: 9999,
    }];
    if cfg.backend == ClusterBackend::Kind {
        forwards.push(PortForward {
            name: Forward::Ingress,
            namespace: "ingress-nginx".to_string(),
            service: "ingress-nginx-controller".to_string(),
            local_port: Forward::Ingress.local_port(cfg),
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
                name: Forward::Gateway,
                namespace: "envoy-gateway-system".to_string(),
                service: svc,
                local_port: Forward::Gateway.local_port(cfg),
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

fn data_dir_pid_file(fwd: Forward) -> PathBuf {
    data_dir().join(format!("port-forward-{}.pid", fwd.as_str()))
}

fn pf_log_file(fwd: Forward) -> PathBuf {
    data_dir().join(format!("port-forward-{}.log", fwd.as_str()))
}

async fn stop() -> Result<()> {
    let cfg = cluster_config();
    // ps is what the kill below verifies pids with: check it up front
    // so a missing tool fails before anything is half-stopped.
    require_binary("ps").await?;
    kill_existing_port_forwards(cfg).with_context(|| {
        format!(
            "the port-forwards were left running and the dispatcher was NOT scaled down; \
             clear them and re-run `weft daemon stop`, or scale it by hand: \
             kubectl --context {} -n {} scale statefulset/weft-dispatcher --replicas=0",
            cfg.kube_context, cfg.system_namespace
        )
    })?;
    let _ = kubectl(&[
        "-n", &cfg.system_namespace, "scale", "statefulset/weft-dispatcher", "--replicas=0",
    ])
    .status()
    .await;
    println!("daemon stopped");
    Ok(())
}

/// THE identity of each forward the daemon may own: its stable name
/// (the pid/log filename key) and its local port, the one piece of a
/// forward's identity known WITHOUT resolving live cluster state (the
/// gateway's Service name is generated by Envoy Gateway). One type
/// feeds both the spawner (`port_forwards`) and the pid verification,
/// so the two can never disagree, and a typo is a compile error. The
/// actual forward set is a subset depending on backend + what's
/// programmed yet; killing a name with no pid file is a no-op, so
/// `ALL` being a superset is safe.
#[derive(Clone, Copy)]
enum Forward {
    Dispatcher,
    Ingress,
    Gateway,
}

impl Forward {
    const ALL: [Forward; 3] = [Forward::Dispatcher, Forward::Ingress, Forward::Gateway];

    fn as_str(self) -> &'static str {
        match self {
            Forward::Dispatcher => "dispatcher",
            Forward::Ingress => "ingress",
            Forward::Gateway => "gateway",
        }
    }

    fn local_port(self, cfg: &ClusterConfig) -> u16 {
        match self {
            Forward::Dispatcher => cfg.dispatcher_port,
            Forward::Ingress => cfg.ingress_port,
            Forward::Gateway => cfg.gateway_port,
        }
    }
}

/// Kill every running `kubectl port-forward` we previously spawned
/// (dispatcher + ingress + gateway). Called on stop and before we
/// re-establish forwards after a Pod rollout. Idempotent.
fn kill_existing_port_forwards(cfg: &ClusterConfig) -> Result<()> {
    for fwd in Forward::ALL {
        let (name, local_port) = (fwd.as_str(), fwd.local_port(cfg));
        let pid_file = data_dir_pid_file(fwd);
        if let Some(pid) = read_pid(&pid_file) {
            // The record is only a number, and a reboot recycles pids:
            // verify it still names OUR kubectl port-forward before
            // signalling, or a stale file would SIGTERM whatever
            // unrelated process inherited the pid.
            if pid_runs_our_forward(pid, local_port, cfg)? {
                signal_term(pid).with_context(|| {
                    format!(
                        "could not signal the {name} port-forward (pid {pid}); \
                         it still holds port {local_port}; kill it by hand: kill -9 {pid}"
                    )
                })?;
            } else if process_alive(pid) {
                eprintln!(
                    "note: the recorded {name} port-forward pid ({pid}) now runs \
                     something else (recycled after a reboot?); dropping the record"
                );
            }
            // A pid whose process is simply gone is the ordinary end
            // of a forward (its pod rolled, it exited); nothing to
            // say, just drop the record.
            let _ = fs::remove_file(&pid_file);
        } else if pid_file.exists() {
            // Unusable record (empty or garbled, e.g. a crash between
            // the file's truncate and its write). The forward it named
            // may still be alive and holding the port, so look for one
            // before dropping the record: dropping over a live forward
            // would make the next start lose the bind with nothing
            // left that knows the pid.
            if let Some(pid) = find_forward_process(local_port, cfg)? {
                anyhow::bail!(
                    "the recorded {name} port-forward pid file is unusable, but a \
                     kubectl port-forward on port {local_port} is still running \
                     (pid {pid}); kill it by hand: kill -9 {pid}"
                );
            }
            eprintln!("note: dropping an unusable {name} port-forward pid record");
            let _ = fs::remove_file(&pid_file);
        }
    }
    Ok(())
}

/// Find a running kubectl port-forward of OURS (our kube context, this
/// local port) by scanning the whole process table: the recovery for a
/// pid record that exists but cannot be read.
fn find_forward_process(local_port: u16, cfg: &ClusterConfig) -> Result<Option<i32>> {
    // Separate -o flags: BSD/macOS ps reads everything after `=` in
    // one -o argument as header text, so a combined "pid=,args="
    // would yield a pid-only column there.
    let out = std::process::Command::new("ps")
        .args(["-ax", "-o", "pid=", "-o", "args="])
        .output()
        .context("cannot run `ps` to scan for a stray port-forward")?;
    anyhow::ensure!(out.status.success(), "ps exited {}", out.status);
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let line = line.trim_start();
        if forward_args_match(line, local_port, cfg) {
            if let Some(pid) = line.split_whitespace().next().and_then(|p| p.parse().ok()) {
                return Ok(Some(pid));
            }
        }
    }
    Ok(None)
}

/// Whether `pid` currently runs OUR kubectl port-forward: the one
/// pinned to our kube context and binding this forward's local port.
/// Matching the bare verb is not enough (a recycled pid can land on
/// the user's own unrelated `kubectl port-forward`, which we must
/// never signal). `ps -o args=` is portable across Linux and macOS; a
/// non-success ps exit means the process is gone (Ok(false)), while
/// ps itself being unrunnable is an error the caller must surface
/// (guessing either way would signal or strand a process we could
/// not identify).
fn pid_runs_our_forward(pid: i32, local_port: u16, cfg: &ClusterConfig) -> Result<bool> {
    let out = std::process::Command::new("ps")
        .args(["-o", "args=", "-p", &pid.to_string()])
        .output()
        .with_context(|| {
            format!(
                "cannot run `ps` to verify pid {pid}; check it by hand and, if it is a \
                 stale weft port-forward, kill it"
            )
        })?;
    if !out.status.success() {
        return Ok(false);
    }
    Ok(forward_args_match(&String::from_utf8_lossy(&out.stdout), local_port, cfg))
}

/// Whether a ps args line names OUR kubectl port-forward on this
/// local port. THE one matcher, shared by the per-pid verification
/// and the whole-table scan, so the two cannot drift.
fn forward_args_match(args: &str, local_port: u16, cfg: &ClusterConfig) -> bool {
    args.contains("kubectl")
        && args.contains("port-forward")
        && args.contains(&format!("--context {}", cfg.kube_context))
        && args.contains(&format!(" {local_port}:"))
}

/// Tear the forward set down and bring it back up: THE one sequence
/// every rebuild takes. SIGTERM is asynchronous, so between kill and
/// spawn the old processes are DRAINED (a bounded wait on actual
/// process exit): a replacement racing a dying forward for the same
/// local port would lose the bind, die into its log file, and leave a
/// pid file pointing at a corpse. Our own spawned children are reaped
/// first (see `reap_spawned_forwards`): an unreaped zombie answers
/// `kill(pid, 0)` forever and would wedge the drain.
async fn restart_port_forwards() -> Result<()> {
    reap_spawned_forwards();
    // Only pids that ARE port-forwards get drained: the kill below
    // drops stale records (a reboot-recycled pid) without signalling,
    // and waiting on such a pid would sit out the whole deadline
    // watching an unrelated process.
    let cfg = cluster_config();
    let mut pids: Vec<i32> = Vec::new();
    for fwd in Forward::ALL {
        if let Some(pid) = read_pid(&data_dir_pid_file(fwd)) {
            if pid_runs_our_forward(pid, fwd.local_port(cfg), cfg)? {
                pids.push(pid);
            }
        }
    }
    kill_existing_port_forwards(cfg).context(
        "clearing the old port-forwards failed; deal with the named process, \
         then re-run `weft daemon start`",
    )?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while pids.iter().any(|&pid| process_alive(pid)) {
        reap_spawned_forwards();
        if std::time::Instant::now() >= deadline {
            anyhow::bail!(
                "kubectl port-forward pid {:?} is still alive 10s after SIGTERM; \
                 kill it by hand and re-run `weft daemon start`.",
                pids.iter().filter(|&&p| process_alive(p)).collect::<Vec<_>>()
            );
        }
        sleep(Duration::from_millis(100)).await;
    }
    start_port_forwards().await
}

/// Every port-forward child THIS process spawned, held so they can be
/// reaped: `start_port_forwards` never waits on its children, and an
/// exited-but-unreaped child stays a zombie whose pid answers
/// `kill(pid, 0)` with success, which would make the drain above read
/// it as alive forever.
fn spawned_forwards() -> &'static std::sync::Mutex<Vec<std::process::Child>> {
    static CHILDREN: OnceLock<std::sync::Mutex<Vec<std::process::Child>>> = OnceLock::new();
    CHILDREN.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

fn reap_spawned_forwards() {
    let mut children = spawned_forwards().lock().expect("forwards mutex");
    children.retain_mut(|child| !matches!(child.try_wait(), Ok(Some(_))));
}

/// Does every CURRENTLY-DESIRED port-forward have a live recorded pid?
/// A live pid is NOT proof the forward works (a reboot can recycle a
/// recorded pid into an unrelated process, a kubectl can wedge on a
/// gone pod), so this is only the cheap pre-check; the boot's real
/// gate is the /health probe in `reconcile`. Iterates the real set
/// (`port_forwards`), not a static name list: the gateway forward is
/// absent from the set until the Envoy Gateway is programmed, so it
/// never drags liveness down before it exists; once it IS in the set,
/// a missing pid correctly reports down (no fail-open).
async fn all_forward_pids_live(cfg: &ClusterConfig) -> bool {
    port_forwards(cfg).await.iter().all(|pf| {
        read_pid(&data_dir_pid_file(pf.name))
            .map(process_alive)
            .unwrap_or(false)
    })
}

async fn status(ctx: &Ctx) -> Result<()> {
    let cfg = cluster_config();
    let pf_alive = all_forward_pids_live(cfg).await;
    match ctx.client().get_json("/projects").await {
        Ok(v) => {
            let n = v.as_array().map(|a| a.len()).unwrap_or(0);
            println!(
                "daemon: running (cluster '{}', system ns '{}', port-forward {}); {} project(s)",
                cfg.cluster_name,
                cfg.system_namespace,
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

/// Where the database's files live: a directory on this machine, mounted into
/// the cluster's node.
///
/// A kind node is a container, so everything stored inside it dies with the
/// cluster, and the cluster has to be rebuilt whenever its own shape changes
/// (a port mapping, a node image), which Docker cannot do in place. Keeping
/// the database's bytes out here is what makes that rebuild cost nothing:
/// the new node mounts the same directory and every project and execution is
/// still there.
pub fn postgres_data_dir() -> PathBuf {
    data_dir().join("postgres-data")
}

/// The path the node sees it at. Named in the PersistentVolume the local
/// Postgres manifest declares.
// SYNC: NODE_POSTGRES_PATH <-> deploy/k8s/postgres.yaml (the PersistentVolume's
//       hostPath)
const NODE_POSTGRES_PATH: &str = "/var/weft-postgres";

/// The cluster's shape. Fingerprinted, so a change here rebuilds the node
/// rather than being silently ignored on every machine that already has one.
fn kind_cluster_config() -> String {
    let host_dir = postgres_data_dir();
    format!(
        r#"kind: Cluster
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
    extraMounts:
      - hostPath: {}
        containerPath: {NODE_POSTGRES_PATH}
"#,
        host_dir.display()
    )
}

/// Whether the running kind node carries the postgres host mount. The
/// stamp is an optimization over this probe: when the stamp is missing,
/// the node itself is the evidence of whether it has the shape the code
/// below depends on.
async fn node_has_postgres_mount(cluster: &str) -> Result<bool> {
    let out = images::docker()
        .args([
            "inspect",
            &format!("{cluster}-control-plane"),
            "--format",
            "{{range .Mounts}}{{.Destination}}\n{{end}}",
        ])
        .output()
        .await?;
    anyhow::ensure!(
        out.status.success(),
        "docker inspect of the kind node failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(String::from_utf8_lossy(&out.stdout).lines().any(|l| l.trim() == NODE_POSTGRES_PATH))
}

/// What a node rebuild does to the databases, stated from the ground
/// truth: the system database survives only if its files are already on
/// the host. On the first rebuild onto host-mounted storage they are
/// not, and saying otherwise would promise survival that does not happen.
fn rebuild_data_notice() -> String {
    let host_db = postgres_data_dir();
    if host_database_exists() {
        format!(
            "Rebuilding it keeps the system database (its files live in {}), \
             but DESTROYS every project's own database (their volumes live \
             inside the node).",
            host_db.display()
        )
    } else {
        "Rebuilding it DESTROYS the system database (projects, journal, \
         execution history) and every project's own database: they all \
         live inside the current node, and nothing is on the host yet."
            .to_string()
    }
}

/// Whether the third-party bundle this cluster holds is the one the code now
/// asks for. Answers true on a cluster that has one but was never stamped,
/// which is any cluster built before the stamp existed.
fn install_is_stale(name: &str, want: &str) -> bool {
    let path = data_dir().join(format!("installed-{name}.txt"));
    std::fs::read_to_string(path).ok().map(|s| s.trim().to_string()).as_deref() != Some(want)
}

/// Record what was just installed, after it succeeded.
fn record_install(name: &str, want: &str) -> Result<()> {
    std::fs::create_dir_all(data_dir())?;
    std::fs::write(data_dir().join(format!("installed-{name}.txt")), want)?;
    Ok(())
}

/// Where the fingerprint of the config the current node was built from is
/// kept.
fn kind_config_stamp() -> PathBuf {
    data_dir().join("kind-cluster-config.sha256")
}


async fn ensure_cluster(cfg: &ClusterConfig, rebuild_cluster: bool) -> Result<()> {
    let config = kind_cluster_config();
    // The kind binary's version is part of the node's identity: the Kubernetes
    // version a node runs comes from kind's default node image, so a different
    // kind builds a different cluster even from identical config. Without this
    // an upgraded kind would give a fresh machine one Kubernetes and leave
    // every existing machine on another.
    let kind_version = images::quiet_stdout("kind").arg("version").output().await?;
    let want = {
        let mut h = Sha256::new();
        h.update(config.as_bytes());
        h.update(&kind_version.stdout);
        format!("{:x}", h.finalize())
    };

    let out = images::quiet_stdout("kind").args(["get", "clusters"]).output().await?;
    let list = String::from_utf8_lossy(&out.stdout);
    let exists = list.lines().any(|n| n == cfg.cluster_name);

    // A node is a container, so its shape (port mappings, mounts, image)
    // cannot change in place: the only way to apply a change is to build
    // a new node. The SYSTEM database survives that (its files live on
    // the host, mounted back in), but every project's own database (a
    // PostgresDatabase infra node's volume) lives inside the node and
    // dies with it, so rebuilding is never a silent side effect.
    if exists {
        let have = std::fs::read_to_string(kind_config_stamp()).ok();
        let have = have.as_deref().map(str::trim);
        // No stamp is not evidence either way: it is a cluster from
        // before the stamp existed, or a wiped data dir. The node
        // itself settles it: one built with the postgres host mount is
        // adoptable as current; one without it predates the mount and
        // must be rebuilt like any other shape change.
        let adoptable =
            have.is_none() && node_has_postgres_mount(&cfg.cluster_name).await?;
        if adoptable {
            std::fs::create_dir_all(data_dir())?;
            std::fs::write(kind_config_stamp(), &want)?;
        } else if have != Some(want.as_str()) {
            if !rebuild_cluster {
                anyhow::bail!(
                    "the cluster's shape changed (kind config or kind version), and \
                     a node cannot change in place.\n\
                     {}\n\
                     Run `weft daemon start --rebuild-cluster` to rebuild it.",
                    rebuild_data_notice()
                );
            }
            println!("rebuilding the kind node. {}", rebuild_data_notice());
            let status = images::quiet_stdout("kind")
                .args(["delete", "cluster", "--name", &cfg.cluster_name])
                .status()
                .await?;
            anyhow::ensure!(status.success(), "kind delete cluster failed with {status}");
            return create_cluster(cfg, &config, &want).await;
        }
        // The kind cluster exists, but the kubeconfig CONTEXT can be absent even so:
        // a reset/rotated kubeconfig, a different `$KUBECONFIG`, or a prior partial
        // run leaves the node running with no `kind-<name>` context. Everything
        // after this (ingress install, Envoy, manifest apply) targets that context
        // and fails with "context kind-<name> does not exist". Re-export the
        // kubeconfig so the context is present; idempotent when it already is.
        let status = images::quiet_stdout("kind")
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
    println!("creating kind cluster '{}' (first run)", cfg.cluster_name);
    create_cluster(cfg, &config, &want).await
}

/// Build the node from `config` and record the fingerprint it was built from.
async fn create_cluster(cfg: &ClusterConfig, config: &str, fingerprint: &str) -> Result<()> {
    // The config bind-mounts this directory into the node, and kind
    // refuses a mount whose source does not exist. This is the one
    // moment the directory has to be there.
    std::fs::create_dir_all(postgres_data_dir())?;
    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), config)?;
    let status = images::quiet_stdout("kind")
        .args(["create", "cluster", "--name", &cfg.cluster_name, "--config"])
        .arg(tmp.path())
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("kind create cluster failed with {status}");
    }
    // Stamped only after the node is up: a failed create must not leave a
    // fingerprint claiming this shape is live.
    std::fs::create_dir_all(data_dir())?;
    std::fs::write(kind_config_stamp(), fingerprint)?;
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
    let out = images::docker()
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

/// One shell script in a throwaway `alpine:3` container with `dir`
/// mounted at `mount_spec` (a container path, optionally `:ro`). The
/// ONE way this file reaches for a root-owned or postgres-owned path
/// the host user cannot touch; pinned to the same image everywhere
/// (setup.sh's purge uses alpine:3 too), so a machine that has it
/// never pulls again.
async fn run_in_alpine(
    dir: &std::path::Path,
    mount_spec: &str,
    script: &str,
) -> Result<std::process::Output> {
    // A colon in the host path would read as an extra -v field and
    // docker would refuse with an unrelated message; name the real
    // problem instead.
    let host = dir.display().to_string();
    anyhow::ensure!(
        !host.contains(':'),
        "cannot docker-mount {host}: docker's -v syntax cannot carry a path \
         containing ':' (move the weft data directory to a colon-free path)"
    );
    Ok(images::docker()
        .args(["run", "--rm", "-v"])
        .arg(format!("{host}:{mount_spec}"))
        .args(["alpine:3", "sh", "-c", script])
        .output()
        .await?)
}

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
    let out = run_in_alpine(
        cfg_dir,
        "/heal",
        &format!("rm -rf /heal/s3.config.json && chown {uid}:{gid} /heal"),
    )
    .await?;
    anyhow::ensure!(
        out.status.success(),
        "the docker-run cleanup exited with {}: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

/// Bring up the object store as a HOST docker container (SeaweedFS's S3 gateway).
/// The cluster reaches OUT to it over S3: the store is never inside
/// the cluster. Idempotent: a running container is left alone, a stopped one is
/// started, else it is created. `-s3.externalUrl` is deliberately UNSET so the
/// gateway validates each presigned request against its own incoming Host header
/// (v3.80 behavior), letting the SAME instance accept URLs signed for the host
/// gateway IP (pods) AND for 127.0.0.1 (the browser).
async fn ensure_object_store(cfg: &ClusterConfig) -> Result<()> {
    // What the container would be run with today. A container cannot change
    // its ports, mounts or image in place, so when this text moves the
    // container is rebuilt. Its data volume is named and is not touched, so
    // rebuilding costs nothing but the restart.
    let want = object_store_run_args(cfg).join(" ");
    let stamp = data_dir().join("object-store-run.sha256");
    let want_hash = {
        let mut h = Sha256::new();
        h.update(want.as_bytes());
        format!("{:x}", h.finalize())
    };
    let stale = std::fs::read_to_string(&stamp).ok().map(|s| s.trim().to_string())
        != Some(want_hash.clone());

    let running = images::docker()
        .args(["ps", "-q", "-f", &format!("name=^{OBJECT_STORE_CONTAINER}$")])
        .output()
        .await?;
    if running.status.success() && !running.stdout.is_empty() {
        if !stale {
            return Ok(());
        }
        println!("the object store's settings changed; rebuilding its container");
        let _ = images::docker().args(["rm", "-f", OBJECT_STORE_CONTAINER]).status().await;
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
    let exists = images::docker()
        .args(["ps", "-aq", "-f", &format!("name=^{OBJECT_STORE_CONTAINER}$")])
        .output()
        .await?;
    if !stale && exists.status.success() && !exists.stdout.is_empty() {
        let status =
            images::docker().args(["start", OBJECT_STORE_CONTAINER]).status().await?;
        if !status.success() {
            anyhow::bail!("docker start {OBJECT_STORE_CONTAINER} failed with {status}");
        }
        return Ok(());
    }
    if exists.status.success() && !exists.stdout.is_empty() {
        let _ = images::docker().args(["rm", "-f", OBJECT_STORE_CONTAINER]).status().await;
    }
    println!(
        "starting object store container '{OBJECT_STORE_CONTAINER}' on port {}",
        cfg.seaweed_port
    );
    let status = images::docker().args(object_store_run_args(cfg)).status().await?;
    if !status.success() {
        anyhow::bail!("docker run {OBJECT_STORE_CONTAINER} failed with {status}");
    }
    std::fs::create_dir_all(data_dir())?;
    std::fs::write(&stamp, &want_hash)?;
    Ok(())
}

/// Everything `docker run` is given for the object store. One place, so the
/// fingerprint that decides whether to rebuild covers exactly what was run.
fn object_store_run_args(cfg: &ClusterConfig) -> Vec<String> {
    let cfg_path = data_dir().join("object-store").join("s3.config.json");
    [
        "run",
        "-d",
        "--name",
        OBJECT_STORE_CONTAINER,
        "--restart",
        "unless-stopped",
        "-p",
        &format!("{}:8333", cfg.seaweed_port),
        "-v",
        &format!("{OBJECT_STORE_CONTAINER}-data:/data"),
        "-v",
        &format!("{}:/etc/seaweedfs/s3.config.json:ro", cfg_path.display()),
        "chrislusf/seaweedfs:3.80",
        "server",
        "-dir=/data",
        "-s3",
        "-s3.port=8333",
        "-s3.config=/etc/seaweedfs/s3.config.json",
        "-master.volumeSizeLimitMB=1024",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// The ingress controller's bundle. Changing this re-applies it on every
/// machine, which is why it is a constant rather than a literal at the call
/// site.
const INGRESS_MANIFEST: &str = "https://kind.sigs.k8s.io/examples/ingress/deploy-ingress-nginx.yaml";

async fn ensure_ingress_controller() -> Result<()> {
    let installed = kubectl(&["get", "namespace", "ingress-nginx", "-o", "name"])
        .output()
        .await?;
    let present = installed.status.success() && !installed.stdout.is_empty();
    // Present is not the same as current. The stamp says WHICH bundle this
    // cluster was given, so changing the bundle re-applies it here instead of
    // only reaching machines that have never installed one.
    if present && !install_is_stale("ingress", INGRESS_MANIFEST) {
        return Ok(());
    }
    println!("installing nginx-ingress controller");
    let status = kubectl(&["apply", "-f", INGRESS_MANIFEST]).status().await?;
    if !status.success() {
        anyhow::bail!("ingress install failed with {status}");
    }
    record_install("ingress", INGRESS_MANIFEST)?;
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
/// gateway manifest's `${GATEWAY_HOST}` (applied by `apply_platform_state`).
async fn ensure_envoy_gateway() -> Result<()> {
    let out = kubectl(&["get", "namespace", "envoy-gateway-system", "-o", "name"])
        .output()
        .await?;
    let present = out.status.success() && !out.stdout.is_empty();
    // A bumped version has to reach clusters that already have the old one, so
    // the check is on the version installed rather than on the namespace
    // existing. Server-side apply is the upstream upgrade path.
    if !present || install_is_stale("envoy-gateway", ENVOY_GATEWAY_VERSION) {
        println!("installing Envoy Gateway controller ({ENVOY_GATEWAY_VERSION})");
        let url = format!(
            "https://github.com/envoyproxy/gateway/releases/download/{ENVOY_GATEWAY_VERSION}/install.yaml"
        );
        let status = kubectl(&["apply", "--server-side", "-f", &url]).status().await?;
        if !status.success() {
            anyhow::bail!("Envoy Gateway install failed with {status}");
        }
        record_install("envoy-gateway", ENVOY_GATEWAY_VERSION)?;
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

/// The pooled tiers the dispatcher spawns dynamically, addressed by
/// the role label its renderers stamp on each Deployment.
/// SYNC: the weft.dev/role values <->
///       crates/weft-dispatcher/src/listener.rs (render_listener_manifest),
///       crates/weft-dispatcher/src/supervisor_pool.rs (the supervisor manifest),
///       deploy/k8s/system-namespace.yaml (the pooled-listener /
///       pooled-supervisor NetworkPolicy podSelectors)
const POOLED_TIERS: [(images::SystemService, &str); 2] = [
    (images::SystemService::Listener, "listener"),
    (images::SystemService::Supervisor, "infra-supervisor"),
];

/// Point every pooled listener / infra-supervisor Deployment at the
/// resolved image refs. These Deployments are rendered by the
/// DISPATCHER at spawn time (not by static manifests) with the image
/// IT knew then, so after an image change their specs still name the
/// old tag; patching the image is what rolls them (content-addressed
/// tags: same tag == same content, so a matching spec needs nothing).
/// Runs on every boot, which makes it self-healing: a boot that died
/// mid-way is caught by the next one.
///
/// Best-effort per deployment: a pod that fails to roll today is
/// recoverable the next reconcile, so one failure never aborts the
/// pass or starves the other tiers. Every failure is RETURNED (one
/// line each, naming what stayed stale); the caller both blocks the
/// image GC on a non-empty list (an unpatched Deployment spec still
/// names its old image and needs it to restart) and refuses to print
/// the ready line over it. `force_roll` additionally restarts every
/// listed deployment: a forced rebuild changed image BYTES under
/// unchanged tags, which the spec compare cannot see.
async fn reconcile_pooled_images(imgs: &images::SystemImages, force_roll: bool) -> Vec<String> {
    let mut failures: Vec<String> = Vec::new();
    for (svc, role) in POOLED_TIERS {
        let image = imgs.get(svc);
        // One error policy for every way the listing can fail (kubectl
        // absent, spawn failure, non-zero exit): record, move on.
        let out = kubectl(&[
            "get",
            "deployments",
            "--all-namespaces",
            "-l",
            &format!("weft.dev/role={role}"),
            "-o",
            "jsonpath={range .items[*]}{.metadata.namespace} {.metadata.name} \
             {.spec.template.spec.containers[0].image}{\"\\n\"}{end}",
        ])
        .output()
        .await;
        let out = match out {
            Ok(out) if out.status.success() => out,
            Ok(_) | Err(_) => {
                failures.push(format!(
                    "{role}: listing its deployments failed, so none were reconciled"
                ));
                continue;
            }
        };
        let listing = String::from_utf8_lossy(&out.stdout);
        let (all, malformed) = parse_pooled_listing(&listing);
        // A line this pass cannot read is a deployment it cannot
        // prove current: recorded, so the GC stays blocked, never
        // silently dropped.
        failures.extend(
            malformed
                .into_iter()
                .map(|line| format!("{role}: unreadable listing line '{line}'; not reconciled")),
        );
        // Reconcile every deployment concurrently: independent kubectl
        // calls, bounded by the slowest instead of the sum. Each task
        // answers (did it move?, what failed?).
        let tasks = all.iter().map(|(ns, name, current)| async move {
            if current != image {
                // `*=`: every container in the pod (these pods have
                // one); the set itself triggers the rolling update.
                let patched = kubectl(&[
                    "-n",
                    ns,
                    "set",
                    "image",
                    &format!("deployment/{name}"),
                    &format!("*={image}"),
                ])
                .status()
                .await;
                if !matches!(patched, Ok(status) if status.success()) {
                    return (
                        false,
                        Some(format!(
                            "{ns}/{name}: pointing it at {image} failed; it keeps \
                             running the old image"
                        )),
                    );
                }
                (true, None)
            } else if force_roll {
                // The spec already names the right tag, but the tag's
                // bytes were just rebuilt: only a restart makes the
                // pod pick them up.
                let restarted = kubectl(&[
                    "-n",
                    ns,
                    "rollout",
                    "restart",
                    &format!("deployment/{name}"),
                ])
                .status()
                .await;
                if !matches!(restarted, Ok(status) if status.success()) {
                    return (
                        false,
                        Some(format!(
                            "{ns}/{name}: restarting it onto the rebuilt image failed; \
                             it keeps running the old bytes"
                        )),
                    );
                }
                (true, None)
            } else {
                (false, None)
            }
        });
        let mut moved: Vec<(&String, &String)> = Vec::new();
        for ((ns, name, _), (did_move, failure)) in
            all.iter().zip(futures::future::join_all(tasks).await)
        {
            if did_move {
                moved.push((ns, name));
            }
            failures.extend(failure);
        }
        // Hold each moved deployment to Ready before reporting it
        // done: a `weft register` fired the moment the boot returns
        // must hit the NEW pod, not the old one mid-termination, and
        // the GC below the caller relies on these specs being truly
        // current. A roll that never becomes Ready lands in `failures`
        // like a failed patch. Concurrent like the patch pass, so the
        // wait is bounded by the slowest deployment, never the sum.
        let waits = moved.iter().map(|(ns, name)| async move {
            wait_workload_ready("deployment", name, ns)
                .await
                .err()
                .map(|e| format!("{ns}/{name}: {e:#}"))
        });
        failures.extend(futures::future::join_all(waits).await.into_iter().flatten());
        if !moved.is_empty() {
            println!("moved {} {role} deployment(s) onto {image}", moved.len());
        }
    }
    failures
}

/// Split one pooled-deployments listing (jsonpath: `ns name image`
/// per line) into the readable rows and the lines that could not be
/// read (fewer than three fields, e.g. a deployment whose container
/// image resolved empty). Pure, so the split is unit-testable.
fn parse_pooled_listing(listing: &str) -> (Vec<(String, String, String)>, Vec<String>) {
    let mut rows = Vec::new();
    let mut malformed = Vec::new();
    for line in listing.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        match (parts.next(), parts.next(), parts.next()) {
            (Some(ns), Some(name), Some(image)) => {
                rows.push((ns.to_string(), name.to_string(), image.to_string()));
            }
            _ => malformed.push(line.to_string()),
        }
    }
    (rows, malformed)
}

async fn start_port_forwards() -> Result<()> {
    let cfg = cluster_config();
    fs::create_dir_all(data_dir())?;
    for pf in port_forwards(cfg).await {
        // TRUNCATED per spawn: the two health-probe bails tail this
        // file as evidence about the CURRENT forward, and an appended
        // log would show a previous run's happy banner (and grow
        // forever).
        let log = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
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
            .with_context(|| format!("spawn kubectl port-forward ({})", pf.name.as_str()))?;
        let pid = child.id();
        // Into the reap list FIRST: from here the child can always be
        // found again. A pid-file write that fails must not strand a
        // live forward recorded nowhere (holding the port with nothing
        // able to kill it), so on that failure the child dies with the
        // error.
        spawned_forwards().lock().expect("forwards mutex").push(child);
        if let Err(e) = fs::write(data_dir_pid_file(pf.name), pid.to_string()) {
            let mut children = spawned_forwards().lock().expect("forwards mutex");
            if let Some(child) = children.iter_mut().find(|c| c.id() == pid) {
                let _ = child.kill();
                let _ = child.wait();
            }
            return Err(anyhow::Error::new(e).context(format!(
                "record the {} port-forward's pid (the forward was stopped again)",
                pf.name.as_str()
            )));
        }
    }
    Ok(())
}

/// Block until the dispatcher's /health answers through its forward,
/// bailing with the forward's log tail after 30s (the log is where a
/// lost bind race or a dead pod names itself).
async fn wait_for_dispatcher_health(url: &str) -> Result<()> {
    // A per-request timeout well under the deadline: without one, a
    // forward that ACCEPTS the connection and never answers (a kubectl
    // wedged on a gone pod, the exact case this probe exists to catch)
    // would hang `send()` forever and the 30s deadline, checked only
    // between attempts, would never fire.
    let client = reqwest::Client::builder().timeout(Duration::from_secs(3)).build()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if std::time::Instant::now() >= deadline {
            let log = pf_log_file(Forward::Dispatcher);
            let tail = std::fs::read_to_string(&log)
                .map(|s| {
                    s.lines().rev().take(5).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n  ")
                })
                .unwrap_or_default();
            if tail.trim().is_empty() {
                // No log means kubectl never got as far as writing
                // one: the spawn itself failed, or nothing answered.
                let cfg = cluster_config();
                anyhow::bail!(
                    "{url} did not become reachable within 30s, and the port-forward \
                     log ({}) is empty or missing, so kubectl never started forwarding; \
                     check the cluster: kubectl --context {} -n {} get pods",
                    log.display(),
                    cfg.kube_context,
                    cfg.system_namespace
                );
            }
            anyhow::bail!(
                "{url} did not become reachable within 30s.\n\
                 The dispatcher port-forward's log ({}) ends with:\n  {tail}",
                log.display()
            );
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
async fn apply_public_page_configmap(
    page_dir: &Path,
    pending: &mut PendingStamps,
) -> Result<bool> {
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
        "metadata": { "name": "weft-public-page", "namespace": cluster_config().system_namespace },
        "data": data,
        "binaryData": binary,
    })
    .to_string();
    kubectl_apply_stdin(&configmap, "weft-public-page configmap").await?;
    Ok(manifest_apply_changed_by_stamp(Path::new("weft-public-page"), &configmap, pending))
}

/// Delete the named tunnel's token Secret, checked (missing is fine;
/// a failed delete of a credential is not).
async fn delete_tunnel_token_secret() -> Result<()> {
    let out = kubectl(&[
        "-n",
        &cluster_config().system_namespace,
        "delete",
        "secret",
        "weft-tunnel-token",
        "--ignore-not-found",
    ])
    .output()
    .await?;
    if !out.status.success() {
        let cfg = cluster_config();
        anyhow::bail!(
            "deleting the weft-tunnel-token secret failed: {}\n\
             Remove it by hand: kubectl --context {} -n {} delete secret weft-tunnel-token",
            String::from_utf8_lossy(&out.stderr),
            cfg.kube_context,
            cfg.system_namespace
        );
    }
    Ok(())
}

/// The one `kubectl apply` path: renders the manifest, pipes it to
/// `kubectl apply -f -`, and reports whether the CONTENT changed since
/// the last apply (the stamp below), which is what decides pod
/// rollouts.
async fn kubectl_apply_changed(
    path: &Path,
    vars: &[(&str, String)],
    pending: &mut PendingStamps,
) -> Result<bool> {
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
    Ok(manifest_apply_changed_by_stamp(path, &manifest, pending))
}

/// Everything that must hold before `postgres.yaml` can be applied.
/// Lives next to nothing but the apply itself so no apply path can
/// forget it: both the fresh-start path and the rolling-restart path
/// call this immediately before applying the manifest.
async fn prepare_postgres_apply(manifest: &Path) -> Result<()> {
    guard_postgres_data_major(manifest).await?;
    rebind_released_postgres_volume().await
}

/// The data directory now outlives the node, which makes the Postgres
/// major version a data-format contract: a newer major refuses to start
/// on an older major's files, and the pod would just CrashLoop. Refuse
/// the apply up front when the bytes on the host disagree with the
/// manifest's image, naming both ways out.
async fn guard_postgres_data_major(manifest: &Path) -> Result<()> {
    let data = postgres_data_dir();
    // Postgres owns pgdata (uid 70, mode 0700), so the host user cannot
    // read PG_VERSION directly; a throwaway container can. Only reached
    // when the directory exists, so a fresh machine pays nothing.
    if !host_database_exists() {
        return Ok(()); // no database on the host yet: any major is fine
    }
    // `__ABSENT__` separates "PG_VERSION is not there" (a half-born
    // database: Postgres died mid-initdb) from "docker could not run"
    // (offline pull, daemon down), which must not block the start over
    // a healthy database NOR silently wave a broken one through.
    let out = run_in_alpine(
        &data,
        "/d:ro",
        "if [ -f /d/pgdata/PG_VERSION ]; then cat /d/pgdata/PG_VERSION; else echo __ABSENT__; fi",
    )
    .await?;
    anyhow::ensure!(
        out.status.success(),
        "docker could not read {}/pgdata to check the Postgres major version \
         before applying (is the docker daemon up, and the alpine:3 image \
         reachable?): {}",
        data.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    let on_disk = String::from_utf8_lossy(&out.stdout).trim().to_string();
    anyhow::ensure!(
        on_disk != "__ABSENT__",
        "{data}/pgdata exists but holds no PG_VERSION: Postgres died before \
         finishing initdb, and nothing in it is recoverable. The directory is \
         owned by the container's postgres user, so remove it the same way:\n  \
         docker run --rm -v {data}:/d alpine:3 sh -c 'rm -rf /d/pgdata'\n\
         then start again to initialize a fresh database.",
        data = data.display()
    );
    let wanted = postgres_major_in_manifest(
        &std::fs::read_to_string(manifest)
            .map_err(|e| anyhow::anyhow!("read {}: {e}", manifest.display()))?,
    )
    .ok_or_else(|| {
        anyhow::anyhow!(
            "{} names no `image: ...postgres:<major>...` line to check the \
             on-host data directory against",
            manifest.display()
        )
    })?;
    anyhow::ensure!(
        wanted == on_disk,
        "the database files in {data} were written by Postgres {on_disk}, and the \
         manifest asks for Postgres {wanted}, which refuses to start on them.\n\
         Either upgrade the data directory with pg_upgrade, or start from an \
         empty database by removing it (it is owned by the container's \
         postgres user, so remove it the same way):\n  \
         docker run --rm -v {data}:/d alpine:3 sh -c 'rm -rf /d/pgdata'",
        data = data.display()
    );
    Ok(())
}

/// Whether the system database's files exist on the host. `is_dir` on
/// the pgdata directory itself, never a file inside it: Postgres owns
/// pgdata as uid 70 mode 0700, so the host user can see the directory
/// but cannot read into it.
fn host_database_exists() -> bool {
    postgres_data_dir().join("pgdata").is_dir()
}

/// The major version of the postgres image a manifest runs, whatever
/// the YAML spelling (a list item, quotes, a registry prefix). The
/// image NAME must be exactly `postgres` (`acme/mypostgres:14` is not
/// it), and a manifest running postgres containers on two DIFFERENT
/// majors (a pg_upgrade initContainer) is refused as ambiguous rather
/// than guessed at. Pure, so the parse is pinned by tests.
fn postgres_major_in_manifest(text: &str) -> Option<String> {
    let mut majors = text
        .lines()
        .map(|l| l.trim().trim_start_matches("- "))
        .filter_map(|l| l.strip_prefix("image:"))
        .map(|v| v.trim().trim_matches(|c| c == '"' || c == '\''))
        .filter_map(|img| {
            let (name, tag) = img.rsplit_once(':')?;
            let bare = name.rsplit('/').next().unwrap_or(name);
            (bare == "postgres").then_some(tag)
        })
        .filter_map(|tag| {
            let major: String = tag.chars().take_while(|c| c.is_ascii_digit()).collect();
            (!major.is_empty()).then_some(major)
        });
    let first = majors.next()?;
    for other in majors {
        if other != first {
            return None;
        }
    }
    Some(first)
}

/// If the postgres PersistentVolume is `Released` (its claim was
/// deleted; the data survived under reclaim policy Retain), clear the
/// stale claimRef uid so the recreated claim can rebind. A missing
/// volume (first run) is a no-op.
async fn rebind_released_postgres_volume() -> Result<()> {
    // `--ignore-not-found` exits 0 with EMPTY output for a missing
    // volume (the apply below creates it), so absence never has to be
    // told apart from a real failure (API server down, a wrong kube
    // context) by string-matching stderr; every real failure bails.
    let out = kubectl(&[
        "get", "pv", "weft-postgres-data", "--ignore-not-found", "-o",
        "jsonpath={.status.phase}",
    ])
    .output()
    .await?;
    anyhow::ensure!(
        out.status.success(),
        "checking the postgres volume's phase failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    if out.stdout.is_empty() {
        return Ok(()); // no volume yet
    }
    if String::from_utf8_lossy(&out.stdout).trim() == "Released" {
        println!("postgres volume is Released; clearing its stale claim so the new claim rebinds");
        // A merge patch, not a JSON patch: `remove` fails the whole
        // patch when a path is absent, and a released volume does not
        // always carry both fields. Nulling them is a no-op for a
        // missing one.
        let status = kubectl(&[
            "patch", "pv", "weft-postgres-data", "--type=merge", "-p",
            r#"{"spec":{"claimRef":{"uid":null,"resourceVersion":null}}}"#,
        ])
        .status()
        .await?;
        anyhow::ensure!(status.success(), "clearing the postgres volume's stale claimRef failed");
    }
    Ok(())
}

/// Pipe a manifest to `kubectl apply -f -`. `what` names the manifest in
/// errors (a file path, or a description for generated manifests).
///
/// Applied DOCUMENT BY DOCUMENT: a multi-doc file applied whole would
/// make an immutable-field refusal anywhere in it a decision about the
/// whole bundle, and the replace/refuse choice below is per object.
async fn kubectl_apply_stdin(manifest: &str, what: &str) -> Result<()> {
    for doc in split_yaml_documents(manifest) {
        kubectl_apply_one_document(doc, what).await?;
    }
    Ok(())
}

/// The documents of a (possibly multi-doc) YAML text, `---` separators
/// removed. A document with no content (blank, or comments only, like
/// a file-header comment above the first `---`) holds no object and is
/// dropped: kubectl ignores those in a whole file but refuses one as
/// its entire stdin ("no objects passed to apply").
fn split_yaml_documents(manifest: &str) -> Vec<&str> {
    let mut docs = Vec::new();
    let mut start = 0;
    let mut at = 0;
    for line in manifest.split_inclusive('\n') {
        if line.trim_end() == "---" {
            docs.push(&manifest[start..at]);
            start = at + line.len();
        }
        at += line.len();
    }
    docs.push(&manifest[start..]);
    docs.retain(|d| {
        d.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with('#')
        })
    });
    docs
}

/// The `kind:` of one YAML document, read from its top-level line.
fn yaml_document_kind(doc: &str) -> Option<&str> {
    doc.lines()
        .find_map(|l| l.strip_prefix("kind:"))
        .map(str::trim)
}

async fn kubectl_apply_one_document(manifest: &str, what: &str) -> Result<()> {
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
        // Some fields cannot be updated in place (a Service's clusterIP, a
        // Job's template, a selector). Kubernetes refuses the update rather
        // than doing it, so the only way to land the change is to replace the
        // object. `--force` is the same apply with a delete-and-recreate for
        // exactly the object that refused, which is why it is reached for
        // here and nowhere else.
        // The refusal wording varies by resource ("field is immutable",
        // "spec is immutable after creation", "may not be changed"), so
        // match the shared fragment.
        if stderr.contains("is immutable") || stderr.contains("may not be changed") {
            // Replacing means deleting first, so it is refused for the two
            // kinds that stand between the cluster and the database's files.
            // A claim recreated behind Postgres's back comes up bound to
            // nothing, and the operator would be left with a running cluster
            // pointed at an empty disk.
            let kind = yaml_document_kind(manifest).unwrap_or("");
            if kind == "PersistentVolume" || kind == "PersistentVolumeClaim" {
                anyhow::bail!(
                    "{what}: a field on a volume or a claim cannot be updated in place, \
                     and replacing it would take the database's binding with it. Change \
                     it by hand, or move the data first: {stderr}"
                );
            }
            println!("{what}: a field there cannot be updated in place; replacing the object");
            return kubectl_apply_replacing(manifest, what).await;
        }
        anyhow::bail!("kubectl apply ({what}) failed: {stderr}");
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    print!("{stdout}");
    Ok(())
}

/// The same apply, allowed to delete and recreate whatever refused to change.
///
/// Only ever reached from the immutable-field path above: a plain apply is
/// tried first every time, so nothing is deleted that could have been updated.
async fn kubectl_apply_replacing(manifest: &str, what: &str) -> Result<()> {
    let mut child = kubectl(&["apply", "--force", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| anyhow::anyhow!("spawn kubectl apply --force: {e}"))?;
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
        anyhow::bail!("kubectl apply --force ({what}) failed: {stderr}");
    }
    print!("{}", String::from_utf8_lossy(&out.stdout));
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
async fn apply_sealing_key_secret(
    cfg: &ClusterConfig,
    pending: &mut PendingStamps,
) -> Result<bool> {
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
            pending,
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
async fn apply_access_apps_secret(
    cfg: &ClusterConfig,
    pending: &mut PendingStamps,
) -> Result<bool> {
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
    Ok(manifest_apply_changed_by_stamp(Path::new("access-apps-secret"), &secret, pending))
}

/// The key the apps json is stored under in the secret, and therefore
/// the file name it mounts as in the broker pod.
// SYNC: ACCESS_APPS_SECRET_KEY <-> deploy/k8s/broker.yaml (volume subPath +
//       WEFT_ACCESS_APPS_FILE)
const ACCESS_APPS_SECRET_KEY: &str = "access-apps.json";

/// Where a manifest's last-applied content hash is stamped. THE one
/// place the naming convention lives, so the teardown that removes a
/// stamp and the stamper that writes it cannot drift apart. Scoped by
/// kube context: a stamp answers "did this content change since the
/// last apply TO THIS CLUSTER", and one CLI pointed at two clusters
/// (WEFT_KUBE_CONTEXT) must not let cluster A's apply record cluster
/// B's roll as done (the exact silent-stale-broker gap the stamps
/// exist to close, arriving through the key instead of the timing).
fn manifest_stamp_file(path: &Path) -> PathBuf {
    // Whitelist sanitizing: anything but [A-Za-z0-9._-] becomes `_`,
    // so no context string (however hostile: `..`, a backslash) can
    // name a directory outside the stamp root. A digest of the RAW
    // context rides along because sanitizing collides (`a/b` and
    // `a:b` both read `a_b`), and two contexts sharing one stamp dir
    // would let cluster A's apply record cluster B's roll as done,
    // the exact gap the scoping exists to close.
    let raw = &cluster_config().kube_context;
    let sanitized: String = raw
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || ".-_".contains(c) { c } else { '_' })
        .collect();
    let sanitized =
        if sanitized.trim_matches('.').is_empty() { "_".to_string() } else { sanitized };
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    let cluster = format!("{sanitized}-{}", &digest[..8]);
    let stem = path
        .file_name()
        .map(|s| s.to_string_lossy().replace(['/', ':'], "_"))
        .unwrap_or_else(|| "manifest".into());
    data_dir().join("manifest-stamps").join(cluster).join(format!("{stem}.hash"))
}

/// Stamp writes held back until the change they record has been fully
/// ACTED ON (applied and rolled). Writing a stamp at detection time
/// opens a gap: a run that records "seen" and dies before the rollout
/// leaves the pods on old content forever, with every later run
/// reading "unchanged" (the exact gap that once left a stale broker
/// silently stripping journal fields for a day). Held stamps flush
/// only after the rolls; a crash before the flush makes the next run
/// re-detect and re-roll, and over-rolling is a cheap restart.
///
/// A map keyed by stamp PATH: `manifest_stamp_file` keys on the file's
/// basename alone, so two detections resolving to one stamp (however
/// they arise) can never queue two writes; last-write-wins is a
/// property of the type instead of an accident of push order.
#[derive(Default)]
struct PendingStamps(std::collections::BTreeMap<PathBuf, String>);

impl PendingStamps {
    /// Persist every held stamp. Call ONLY once the rolls the detected
    /// changes demanded have completed. A failed write only costs a
    /// redundant re-roll next run, so it warns rather than fails.
    fn flush(&mut self) {
        for (stamp, want) in std::mem::take(&mut self.0) {
            if let Some(parent) = stamp.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(e) = std::fs::write(&stamp, &want) {
                eprintln!(
                    "warning: could not write manifest stamp {} ({e}); \
                     the next run re-detects this change and rolls again",
                    stamp.display()
                );
            }
        }
    }
}

/// Whether `manifest` differs from what the last COMPLETED apply+roll
/// recorded for `path`. A detected change queues its stamp on
/// `pending`; nothing is persisted until `pending.flush()`.
fn manifest_apply_changed_by_stamp(
    path: &Path,
    manifest: &str,
    pending: &mut PendingStamps,
) -> bool {
    let mut hasher = Sha256::new();
    hasher.update(manifest.as_bytes());
    let want = format!("{:x}", hasher.finalize());
    let stamp = manifest_stamp_file(path);
    let have = std::fs::read_to_string(&stamp).ok().map(|s| s.trim().to_string());
    if have.as_deref() == Some(want.as_str()) {
        return false;
    }
    pending.0.insert(stamp, want);
    true
}

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
        // EPERM means the pid EXISTS but belongs to someone else:
        // that process is alive, just not signalable by us.
        kill(pid, 0) == 0
            || std::io::Error::last_os_error().raw_os_error()
                == Some(1 /* EPERM */)
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
        endpoint_host, parse_pooled_listing, rolls_for, split_yaml_documents,
        yaml_document_kind, DetectedChanges, EndpointHost,
    };

    /// The roll decision, pinned as an explicit table (never a
    /// restatement of the formula) so editing `rolls_for` cannot
    /// silently keep this green. Inputs are (dispatcher_manifest,
    /// postgres_manifest, other_manifests, sealing_key, apps, rebuilt,
    /// forward_pids_live); expectations are (dispatcher, broker,
    /// forwards). Every single-trigger row plus the historically
    /// dangerous combinations: a dispatcher spec change resets only
    /// the forwards (the apply itself rolled the pod); a postgres
    /// change rolls BOTH pods (its credentials Secret is env at pod
    /// start on each, invisible to their specs); other manifests roll
    /// and reset nothing; the sealing key rolls both; apps rolls the
    /// broker alone (the case a branchy version once used to skip the
    /// forward reconcile on); a forced rebuild rolls both; dead
    /// forward pids reset the forwards even with nothing changed.
    #[test]
    fn each_roll_trigger_fires_exactly_where_it_must() {
        type Inputs = (bool, bool, bool, bool, bool, bool, bool);
        type Expected = (bool, bool, bool);
        #[rustfmt::skip]
        let table: &[(Inputs, Expected)] = &[
            // nothing changed
            ((false, false, false, false, false, false, true),  (false, false, false)),
            ((false, false, false, false, false, false, false), (false, false, true)),
            // dispatcher.yaml only: forwards, no explicit rolls
            ((true,  false, false, false, false, false, true),  (false, false, true)),
            ((true,  false, false, false, false, false, false), (false, false, true)),
            // postgres.yaml only: both pods (credentials env), and the
            // dispatcher roll drags the forwards
            ((false, true,  false, false, false, false, true),  (true,  true,  true)),
            // other manifests only: applied, nothing rolls or resets
            ((false, false, true,  false, false, false, true),  (false, false, false)),
            // sealing key only
            ((false, false, false, true,  false, false, true),  (true,  true,  true)),
            // apps only: broker WITHOUT dispatcher, forwards untouched
            ((false, false, false, false, true,  false, true),  (false, true,  false)),
            ((false, false, false, false, true,  false, false), (false, true,  true)),
            // rebuilt only
            ((false, false, false, false, false, true,  true),  (true,  true,  true)),
            // combinations
            ((true,  false, false, true,  false, false, true),  (true,  true,  true)),
            ((true,  false, true,  false, true,  false, true),  (false, true,  true)),
            ((false, true,  false, false, true,  false, true),  (true,  true,  true)),
            ((false, false, false, true,  true,  false, false), (true,  true,  true)),
            ((true,  true,  true,  true,  true,  true,  false), (true,  true,  true)),
        ];
        for &(inputs, want) in table {
            let (dispatcher_manifest, postgres_manifest, other_manifests, sealing_key, apps, rebuilt, pids_live) =
                inputs;
            let rolls = rolls_for(
                &DetectedChanges {
                    dispatcher_manifest,
                    postgres_manifest,
                    other_manifests,
                    sealing_key,
                    apps,
                },
                rebuilt,
                pids_live,
            );
            assert_eq!(
                (rolls.dispatcher, rolls.broker, rolls.forwards),
                want,
                "inputs: {inputs:?}"
            );
        }
    }

    /// A line the pooled-listing parser cannot read must come back as
    /// malformed (the caller records it and blocks the image GC),
    /// never be silently dropped.
    #[test]
    fn pooled_listing_reports_unreadable_lines_instead_of_dropping_them() {
        let (rows, malformed) = parse_pooled_listing(
            "weft-t1 weft-listener-a weft-listener:abc\n\
             weft-t2 weft-listener-b\n\
             \n\
             weft-t3 weft-listener-c weft-listener:def\n",
        );
        assert_eq!(
            rows,
            vec![
                ("weft-t1".into(), "weft-listener-a".into(), "weft-listener:abc".into()),
                ("weft-t3".into(), "weft-listener-c".into(), "weft-listener:def".into()),
            ]
        );
        assert_eq!(malformed, vec!["weft-t2 weft-listener-b".to_string()]);
        let (rows, malformed) = parse_pooled_listing("");
        assert!(rows.is_empty() && malformed.is_empty());
    }

    #[test]
    fn yaml_documents_split_on_bare_separators_only() {
        // Leading and trailing separators, an empty document between
        // two separators, and CRLF endings all collapse away.
        let docs = split_yaml_documents("---\nkind: A\n---\r\n---\nkind: B\n---\n");
        assert_eq!(docs.iter().map(|d| d.trim()).collect::<Vec<_>>(), ["kind: A", "kind: B"]);
        // A `---` that is not alone on its line is content, not a
        // separator (a string value, a heredoc marker).
        let docs = split_yaml_documents("kind: A\ndata: \"--- not a split\"\n");
        assert_eq!(docs.len(), 1);
        // No separator at all: the whole text is one document.
        assert_eq!(split_yaml_documents("kind: A\n"), ["kind: A\n"]);
        // A comments-only document (a file-header comment above the
        // first separator) holds no object and is dropped; a comment
        // INSIDE a real document stays with it.
        let docs = split_yaml_documents("# header\n# more\n---\n# note\nkind: A\n");
        assert_eq!(docs, ["# note\nkind: A\n"]);
    }

    /// The REAL manifests through the REAL splitter: every document
    /// the boot would pipe to kubectl must carry an object. This is
    /// the test that catches a file-header edit (a comment above the
    /// first `---`) producing an object-less document, which kubectl
    /// refuses as its whole stdin and which kills the boot ("no
    /// objects passed to apply", found in production once).
    #[test]
    fn every_shipped_manifest_splits_into_object_documents() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/k8s");
        let mut checked = 0;
        for entry in std::fs::read_dir(&dir).expect("deploy/k8s readable") {
            let path = entry.expect("dir entry").path();
            if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("manifest readable");
            let docs = split_yaml_documents(&text);
            assert!(!docs.is_empty(), "{} split to zero documents", path.display());
            for doc in docs {
                assert!(
                    yaml_document_kind(doc).is_some(),
                    "{} yields a document with no `kind:` (an object-less chunk \
                     the apply would feed kubectl):\n{doc}",
                    path.display()
                );
            }
            checked += 1;
        }
        assert!(checked >= 5, "expected the deploy/k8s manifests, found {checked}");
    }

    #[test]
    fn postgres_major_survives_yaml_spellings() {
        use super::postgres_major_in_manifest as major;
        assert_eq!(major("          image: postgres:18-alpine"), Some("18".into()));
        assert_eq!(major("- image: postgres:16"), Some("16".into()));
        assert_eq!(major("  image: \"postgres:15-alpine\""), Some("15".into()));
        assert_eq!(major("image: registry.example.com/library/postgres:14"), Some("14".into()));
        // A non-postgres image line never answers, and the image NAME
        // must be exactly `postgres`.
        assert_eq!(major("image: redis:7"), None);
        assert_eq!(major("image: postgres:latest"), None);
        assert_eq!(major("image: acme/mypostgres:14"), None);
        // Two postgres containers on different majors (a pg_upgrade
        // initContainer) are ambiguous, never guessed at; the same
        // major twice is fine.
        assert_eq!(major("image: postgres:16\nimage: postgres:18"), None);
        assert_eq!(major("image: postgres:18\nimage: postgres:18-alpine"), Some("18".into()));
    }

    #[test]
    fn yaml_document_kind_reads_only_the_top_level_line() {
        assert_eq!(yaml_document_kind("apiVersion: v1\nkind: PersistentVolume\n"), Some("PersistentVolume"));
        // An indented `kind:` is a nested field (a kubeadm patch, a
        // pod template), never the document's own kind.
        assert_eq!(yaml_document_kind("data:\n  kind: InitConfiguration\n"), None);
        assert_eq!(yaml_document_kind(""), None);
    }

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
