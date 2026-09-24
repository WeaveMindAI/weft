//! `weft daemon start|stop|status|restart|logs`. Owns the kind
//! cluster lifecycle and the dispatcher deployment inside it.
//!
//! Everything runs as Pods: dispatcher, listener, worker, infra.
//! `start` and `restart` are one reconcile (see `reconcile`); on kind
//! the shared images land in the node via `kind load docker-image`, on
//! a registry-backed cluster the same manifests pull them instead.

use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
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
/// `system_namespace` holds the dispatcher Pod, its Service and PVC;
/// `db_namespace` holds postgres + broker. Per-project
/// namespaces are created by the dispatcher at first infra apply.
pub struct ClusterConfig {
    pub cluster_name: String,
    pub kube_context: String,
    /// Which install on the cluster this CLI drives (`WEFT_INSTANCE`,
    /// unset for the default one). A named install lives beside the
    /// default one with namespaces of its own; see
    /// `weft_core::infra::Instance`.
    pub instance: weft_core::infra::Instance,
    /// How fast the install's own timers run (`WEFT_TIME_SCALE`, `1`
    /// when unset), written into the dispatcher and broker manifests;
    /// see `weft_core::time_scale`.
    pub time_scale: f64,
    /// `instance.system_namespace()`, kept as a field so call sites read
    /// one authority.
    pub system_namespace: String,
    /// `instance.db_namespace()`, likewise.
    pub db_namespace: String,
    /// Loopback port the dispatcher's API answers at on kind: the kind
    /// node maps it to the dispatcher's node port (see [`MappedPort`]).
    /// Baked into the node's shape, so changing it rebuilds the node.
    pub dispatcher_port: u16,
    /// Loopback port the front door's `local` listener answers at on
    /// kind, mapped the same way: every path to the dispatcher. Every
    /// link the dispatcher mints for this machine (a storage download, a
    /// signal URL) is `http://127.0.0.1:<local_port>/...` in local dev,
    /// the dispatcher's public base URL.
    pub local_port: u16,
    /// Loopback port the front door's `http` listener (live caller
    /// connections) answers at on kind, mapped the same way. A caller's
    /// URL is minted as `http://<pod>.<ns>.<host>:<gateway_port>/...` in
    /// local dev.
    pub gateway_port: u16,
    /// Loopback port the bundled SeaweedFS object store's docker
    /// container (a host container, not a pod; see `ensure_object_store`)
    /// is published on. Runtime-file downloads are presigned BUCKET urls
    /// signed for this host-reachable address (the broker's in-cluster
    /// I/O endpoint is unreachable from the host / a browser).
    pub seaweed_port: u16,
    /// Cluster Service CIDR. The apiserver's ClusterIP lives in this
    /// range; the broker NetworkPolicy allows TokenReview egress to
    /// it, and the dispatcher gets it as env. kind's default is
    /// `10.96.0.0/12`; a non-kind operator sets WEFT_CLUSTER_SERVICE_CIDR.
    pub service_cidr: String,
    /// Cluster Pod CIDR. Passed to the dispatcher for NetworkPolicy
    /// rendering. kind's default is `10.244.0.0/16`.
    pub pod_cidr: String,
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
        // Like the backend below, a malformed install name or pace is
        // not something a later consumer can catch (every namespace and
        // every timer follows from them), so it stops the process here.
        let instance = weft_core::infra::Instance::from_env().unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(2);
        });
        let time_scale = weft_core::time_scale::parse(
            std::env::var(weft_core::time_scale::TIME_SCALE_ENV).ok().as_deref(),
        )
        .unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(2);
        });
        let system_namespace = instance.system_namespace();
        let db_namespace = instance.db_namespace();
        let dispatcher_port = std::env::var("WEFT_DISPATCHER_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(9999);
        let local_port = std::env::var("WEFT_LOCAL_PORT")
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
        // Unlike the CIDRs above, a misread backend is not something a
        // later consumer can catch: every command branches on it, and
        // the default branch is the one that runs `kind create cluster`
        // against whatever cluster the operator's kubectl points at. So
        // a value we do not recognise stops the process here, where the
        // typo is, rather than becoming "kind" everywhere. It exits
        // instead of returning an error because this config is a
        // process-wide `OnceLock` every command reads, with no caller
        // in a position to answer for it.
        let backend = match std::env::var("WEFT_CLUSTER_BACKEND").as_deref() {
            Ok("k8s") => ClusterBackend::K8s,
            Ok("kind") => ClusterBackend::Kind,
            Err(_) => ClusterBackend::Kind,
            Ok(other) if other.trim().is_empty() => ClusterBackend::Kind,
            Ok(other) => {
                eprintln!(
                    "WEFT_CLUSTER_BACKEND is set to '{other}', which weft does not know. \
                     It takes 'kind' (the local cluster weft builds and owns) or 'k8s' \
                     (a cluster you already run). Unset it for 'kind'."
                );
                std::process::exit(2);
            }
        };
        Self {
            cluster_name,
            kube_context,
            instance,
            time_scale,
            system_namespace,
            db_namespace,
            dispatcher_port,
            local_port,
            gateway_port,
            seaweed_port,
            service_cidr,
            pod_cidr,
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
/// - Kind (local dev): default to `http://127.0.0.1:<local_port>`
///   (the kind node maps the front door's `local` listener there) and set
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

    // The docker network the cluster sits on; a real cluster has none.
    let kind_network = match cfg.backend {
        ClusterBackend::Kind => Some(kind_network_ipv4().await?),
        ClusterBackend::K8s => None,
    };

    // The operator's machine, as the cluster sees it, in both
    // directions. OUT: the address a pod dials to reach a port
    // published on this machine (the object store, a service a
    // trigger watches). IN: the address a packet from this machine
    // carries when it enters the node through a mapped port (the CLI
    // reaching the dispatcher). On docker for Linux both are the kind
    // network's gateway; on Docker Desktop the way out of its virtual
    // machine is a different address from the one its port proxy
    // connects from, so the two are read separately.
    let host_gateway = match &kind_network {
        Some(net) => Some(machine_address_for_pods(cfg, &net.gateway).await?),
        None => None,
    };
    let node_port_source = kind_network.as_ref().map(|net| net.gateway.clone());

    // Where the apiserver answers, for the two egress rules that admit
    // it (the dispatcher's and the broker's).
    let apiserver = apiserver_endpoint().await?;
    let apiserver_peers = match &kind_network {
        // The apiserver is a process inside the node, and the node is a
        // container on this network at an address docker may hand out
        // differently after a restart. The network's range on the
        // apiserver's port is stable, and nothing else on that network
        // answers on it.
        Some(net) => apiserver_peers_yaml(&[net.subnet.to_string()]),
        None => apiserver_peers_yaml(
            &apiserver.addresses.iter().map(|ip| format!("{ip}/32")).collect::<Vec<_>>(),
        ),
    };

    // Object-store slot: the broker's runtime-file plane (`ctx.storage`) writes
    // bytes to this bucket, and workers read/write it DIRECTLY via presigned URLs.
    // The store is ALWAYS external to the cluster, reached over S3 (endpoint from
    // env), so both endpoints below reach OUT of the cluster. An operator can
    // override any of these via env for their own S3.
    //
    // INTERNAL endpoint (what the broker and the worker pods dial): the object
    // store is a docker container on this machine with its S3 port published
    // here, and pods reach this machine at the address above. This is the host
    // string presigned internal URLs are signed for, and the pods then connect
    // to (the two must match for SigV4).
    let object_store_endpoint = match std::env::var("WEFT_OBJECT_STORE_ENDPOINT") {
        Ok(v) => v,
        // The derived default only exists on kind (where weft runs the
        // store itself); a real cluster has no address to derive, so
        // absence there is a configuration error, named rather than
        // surfacing as a failed docker inspect.
        Err(_) => match &host_gateway {
            Some(host) => format!("http://{host}:{}", cfg.seaweed_port),
            None => anyhow::bail!(
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
    // kind it defaults to the operator's own machine as pods reach it,
    // so a trigger can watch a service running right there; on a real
    // cluster it defaults to a benign unused /32.
    let listener_allow_cidr = match std::env::var("WEFT_LISTENER_ALLOW_CIDR") {
        Ok(v) => v,
        Err(_) => match &host_gateway {
            Some(gateway) => format!("{gateway}/32"),
            None => "192.0.2.0/32".into(),
        },
    };
    check_cidr(&listener_allow_cidr, false)
        .map_err(|e| anyhow::anyhow!("WEFT_LISTENER_ALLOW_CIDR='{listener_allow_cidr}': {e}"))?;

    // Where a packet that entered through a mapped node port comes
    // from, for the dispatcher's ingress NetworkPolicy. The kind node
    // maps the operator's loopback ports to NodePort Services
    // (deploy/k8s/kind-node-ports.yaml), and a connection arriving that
    // way carries the docker network's gateway as its source, on Linux
    // and on Docker Desktop alike (docker's port proxy connects from
    // the bridge): kube-proxy rewrites the destination before the
    // policy engine sees the packet and the source only after, so that
    // /32 is exactly what the policy compares against. A real cluster
    // maps no node port, so it opens a benign unused address instead.
    let node_port_source_cidr = match &node_port_source {
        Some(gateway) => format!("{gateway}/32"),
        None => "192.0.2.0/32".into(),
    };

    let (public_base_url, local_dev) = match cfg.backend {
        ClusterBackend::Kind => {
            // A named install's links are pointed at its own door once
            // that door has a port (`apply_platform_state`).
            let url = std::env::var("WEFT_DISPATCHER_PUBLIC_BASE_URL")
                .unwrap_or_else(|_| format!("http://127.0.0.1:{}", cfg.local_port));
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
            // where the kind node maps the gateway's node port. A fixed
            // dev secret keeps tokens stable across local restarts.
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

    // One bucket per install on the store: a named install's files never
    // sit beside the default install's (the tenant-wide `shared/` scope
    // would otherwise be one scope for both), and removing it drops its
    // bucket whole. The broker creates it at boot.
    let object_store_bucket = std::env::var("WEFT_OBJECT_STORE_BUCKET")
        .unwrap_or_else(|_| object_store_bucket(&cfg.instance));
    let object_store_region =
        std::env::var("WEFT_OBJECT_STORE_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    // The credentials and the browser-facing address. Their defaults
    // describe the store `ensure_object_store` starts on this machine:
    // a known key pair and a loopback port. On a real cluster each of
    // them would be a wrong answer nobody notices (a bucket that
    // refuses every write, or download links pointing at the reader's
    // own machine), so like their siblings above they are required
    // rather than defaulted there.
    let object_store_access_key = match std::env::var("WEFT_OBJECT_STORE_ACCESS_KEY") {
        Ok(v) => v,
        Err(_) if cfg.backend == ClusterBackend::Kind => "weft-local".to_string(),
        Err(_) => anyhow::bail!(
            "WEFT_OBJECT_STORE_ACCESS_KEY is required for the k8s backend; the default is \
             the key pair of the store weft runs on a developer's own machine"
        ),
    };
    let object_store_secret_key = match std::env::var("WEFT_OBJECT_STORE_SECRET_KEY") {
        Ok(v) => v,
        Err(_) if cfg.backend == ClusterBackend::Kind => "weft-local-dev-secret".to_string(),
        Err(_) => anyhow::bail!(
            "WEFT_OBJECT_STORE_SECRET_KEY is required for the k8s backend; the default is \
             the key pair of the store weft runs on a developer's own machine"
        ),
    };
    // PUBLIC endpoint (the browser reaches): the store container is published on the
    // host's loopback too, so the browser hits 127.0.0.1:<seaweed_port> directly.
    let object_store_public_endpoint = match std::env::var("WEFT_OBJECT_STORE_PUBLIC_ENDPOINT") {
        Ok(v) => v,
        Err(_) if cfg.backend == ClusterBackend::Kind => {
            format!("http://127.0.0.1:{}", cfg.seaweed_port)
        }
        Err(_) => anyhow::bail!(
            "WEFT_OBJECT_STORE_PUBLIC_ENDPOINT is required for the k8s backend; set it to \
             the address a BROWSER reaches the store at, which is what download links \
             are signed for"
        ),
    };

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
        ("WEFT_NODE_PORT_SOURCE_CIDR", node_port_source_cidr),
        ("WEFT_APISERVER_PEERS", apiserver_peers),
        ("WEFT_APISERVER_PORT", apiserver.port.to_string()),
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
    vars.extend(install_template_vars(&cfg.instance, cfg.time_scale));
    // The node ports the kind node's port mappings land on, so the
    // NodePort Services and the kind config are rendered from ONE set
    // of numbers (see `MappedPort`).
    vars.extend(MappedPort::ALL.iter().map(|&m| (m.template_var(), m.node_port().to_string())));
    Ok(vars)
}

/// The template vars that tell one install on the cluster from another:
/// its namespaces, the names of its cluster-wide objects, where its
/// Postgres keeps its files, the database URL that follows from its db
/// namespace, and how fast its timers run. For the default install they
/// render exactly the names every existing cluster already has, so a
/// default install's manifests hash the same as before this existed.
fn install_template_vars(
    instance: &weft_core::infra::Instance,
    time_scale: f64,
) -> Vec<(&'static str, String)> {
    let database_url_base64 =
        base64::engine::general_purpose::STANDARD.encode(install_database_url(instance));
    vec![
        ("WEFT_SYSTEM_NAMESPACE", instance.system_namespace()),
        ("WEFT_DB_NAMESPACE", instance.db_namespace()),
        ("WEFT_INSTANCE", instance.name().unwrap_or_default().to_string()),
        ("WEFT_INSTANCE_SUFFIX", instance.name().map(|n| format!("-{n}")).unwrap_or_default()),
        ("WEFT_TIME_SCALE", time_scale.to_string()),
        ("WEFT_POSTGRES_VOLUME", postgres_volume(instance)),
        ("WEFT_POSTGRES_NODE_PATH", postgres_node_path(instance)),
        ("WEFT_DATABASE_URL_BASE64", database_url_base64),
    ]
}

/// The bucket an install keeps its runtime files in, on the object store
/// every install on the machine shares.
fn object_store_bucket(instance: &weft_core::infra::Instance) -> String {
    instance.cluster_object("weft")
}

/// The URL the install's dispatcher and broker reach its Postgres at.
// SYNC: local-dev PG credentials <-> deploy/k8s/postgres.yaml (the
//       credentials Secret), crates/weft-e2e/src/platform.rs
//       (PG_USER/PG_PASSWORD/PG_DBNAME)
fn install_database_url(instance: &weft_core::infra::Instance) -> String {
    format!(
        "postgres://weft:weft-local-dev@weft-postgres.{}.svc.cluster.local:5432/weft",
        instance.db_namespace()
    )
}

/// Where inside the kind node an install's Postgres keeps its files.
/// The default install's path is the one the node mounts from this
/// machine ([`postgres_data_dir`]), so its database outlives the node. A
/// named install's is a plain directory inside the node: it is a
/// throwaway install, and `weft daemon remove` deletes it with the rest.
fn postgres_node_path(instance: &weft_core::infra::Instance) -> String {
    match instance.name() {
        None => NODE_POSTGRES_PATH.to_string(),
        Some(name) => format!("{NAMED_INSTALLS_NODE_PATH}/{name}/postgres"),
    }
}

/// The directory inside the kind node that holds every named install's
/// files, one subdirectory each.
const NAMED_INSTALLS_NODE_PATH: &str = "/var/weft-installs";

/// The three doors the operator's machine reaches inside the kind
/// cluster, each at a loopback port of its own: the dispatcher's API
/// (the CLI and the editor), and two of the front door's listeners
/// (deploy/k8s/gateway.yaml): `local`, where every link the dispatcher
/// mints for this machine lands, and `http`, the live callers.
///
/// Each is exposed by a NodePort Service pinned to a fixed node port
/// (`deploy/k8s/kind-node-ports.yaml`), and the kind config maps that
/// node port to the configured loopback port on the host, so the
/// docker port mapping is the whole path in: no process on this
/// machine holds it open. This type is the one place the node ports
/// live; the manifest and the kind config both render from it.
///
/// A real cluster (the k8s backend) applies none of this: its services
/// are reached at the operator's external addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MappedPort {
    Dispatcher,
    Local,
    Gateway,
}

impl MappedPort {
    const ALL: [MappedPort; 3] = [MappedPort::Dispatcher, MappedPort::Local, MappedPort::Gateway];

    /// The fixed node port. All three sit in the lower band of the
    /// default node-port range (30000-32767): the apiserver reserves
    /// the first min(max(16, range/16), 128) = 128 ports of the range
    /// for explicitly requested node ports and hands dynamic
    /// allocations (the Service Envoy Gateway generates) ports from the
    /// rest first, so a Service that
    /// is created before ours never takes one of these.
    /// SYNC: node ports <-> deploy/k8s/kind-node-ports.yaml (rendered
    ///       from these through the template vars, never typed there)
    fn node_port(self) -> u16 {
        match self {
            MappedPort::Dispatcher => 30099,
            MappedPort::Local => 30098,
            MappedPort::Gateway => 30097,
        }
    }

    /// The loopback port on this machine the node port is mapped to.
    fn host_port(self, cfg: &ClusterConfig) -> u16 {
        match self {
            MappedPort::Dispatcher => cfg.dispatcher_port,
            MappedPort::Local => cfg.local_port,
            MappedPort::Gateway => cfg.gateway_port,
        }
    }

    /// The `${...}` placeholder the manifest names this node port by.
    fn template_var(self) -> &'static str {
        match self {
            MappedPort::Dispatcher => "WEFT_DISPATCHER_NODE_PORT",
            MappedPort::Local => "WEFT_LOCAL_NODE_PORT",
            MappedPort::Gateway => "WEFT_GATEWAY_NODE_PORT",
        }
    }
}

/// Where the Kubernetes apiserver actually answers: the addresses and
/// port behind the `kubernetes` Service, read from its endpoint slice.
struct ApiserverEndpoint {
    addresses: Vec<String>,
    port: u16,
}

/// The apiserver's endpoint, for the egress rules that admit it.
///
/// The `kubernetes` Service's ClusterIP is NOT usable there: a
/// NetworkPolicy engine sees a packet after kube-proxy has rewritten
/// the ClusterIP to the endpoint behind it, so a rule naming the
/// ClusterIP matches nothing and the default deny drops every
/// apiserver call (on kind: no worker is ever created, `weft run`
/// hangs after "started"). The endpoint slice is what the packet
/// carries when the engine looks.
async fn apiserver_endpoint() -> Result<ApiserverEndpoint> {
    let out = kubectl(&[
        "-n",
        "default",
        "get",
        "endpointslices",
        "-l",
        "kubernetes.io/service-name=kubernetes",
        "-o",
        "json",
    ])
    .output()
    .await?;
    if !out.status.success() {
        anyhow::bail!(
            "reading the apiserver's endpoint slice failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let listing: serde_json::Value = serde_json::from_slice(&out.stdout)
        .context("the apiserver's endpoint slice listing is not JSON")?;
    apiserver_endpoint_from_slices(&listing)
}

/// The addresses and the port out of an EndpointSlice listing. Pure,
/// so the parse is testable against a captured listing.
fn apiserver_endpoint_from_slices(listing: &serde_json::Value) -> Result<ApiserverEndpoint> {
    let items = listing["items"].as_array().cloned().unwrap_or_default();
    let addresses: Vec<String> = items
        .iter()
        .flat_map(|slice| slice["endpoints"].as_array().cloned().unwrap_or_default())
        .flat_map(|endpoint| endpoint["addresses"].as_array().cloned().unwrap_or_default())
        .filter_map(|address| address.as_str().map(str::to_string))
        .collect();
    let port = items
        .iter()
        .flat_map(|slice| slice["ports"].as_array().cloned().unwrap_or_default())
        .find_map(|port| port["port"].as_u64())
        .and_then(|port| u16::try_from(port).ok());
    match (addresses.is_empty(), port) {
        (false, Some(port)) => Ok(ApiserverEndpoint { addresses, port }),
        _ => anyhow::bail!(
            "the `kubernetes` endpoint slice names no apiserver address and port; the \
             cluster's control plane is not reachable through it"
        ),
    }
}

/// The `- ipBlock:` entries a NetworkPolicy `to:` list gets for the
/// apiserver, one per range, at the indentation the shipped manifests
/// place the placeholder at (column 0, immediately after the `- to:`
/// line). Ends with a newline so the manifest's next line stays its
/// own.
// SYNC: indentation <-> deploy/k8s/system-namespace.yaml and
//       deploy/k8s/broker.yaml (the ${WEFT_APISERVER_PEERS} lines)
fn apiserver_peers_yaml(cidrs: &[String]) -> String {
    cidrs
        .iter()
        .map(|cidr| format!("        - ipBlock:\n            cidr: {cidr}\n"))
        .collect()
}

pub enum DaemonAction {
    /// One verb for boot AND refresh (`restart` is a CLI alias): the
    /// reconcile below is idempotent, so "the daemon was fully up" and
    /// "nothing exists yet" are just states it converges from.
    Start { rebuild: bool, rebuild_cluster: bool, public_url: Option<bool>, clear_access_apps: bool },
    Stop,
    /// Take a named install off the cluster (`remove_named_install`).
    Remove,
    Status,
    Logs { tail: usize, follow: bool },
}

pub async fn run(ctx: Ctx, action: DaemonAction) -> Result<()> {
    match action {
        DaemonAction::Start { rebuild, rebuild_cluster, public_url, clear_access_apps } => {
            // The public surface is the front door's, which routes to the
            // default install only.
            if let Some(name) = cluster_config().instance.name() {
                anyhow::ensure!(
                    public_url.is_none(),
                    "--public-url / --no-public-url open or close the default install's \
                     public surface; the install '{name}' has none"
                );
            } else {
                set_public_url_choice(public_url)?;
            }
            reconcile(&ctx, rebuild, rebuild_cluster, clear_access_apps).await
        }
        DaemonAction::Remove => remove_named_install().await,
        DaemonAction::Stop => stop().await,
        DaemonAction::Status => status(&ctx).await,
        DaemonAction::Logs { tail, follow } => logs(tail, follow).await,
    }
}

/// Where this boot proves the dispatcher reachable, and what the ready
/// line names: on kind, the loopback port the node maps to the
/// dispatcher's node port; on a real cluster, the address the CLI is
/// configured to talk to (`--dispatcher` / WEFT_DISPATCHER_URL), which
/// is the operator's external ingress host.
async fn dispatcher_reach_url(cfg: &ClusterConfig, ctx: &Ctx) -> Result<String> {
    Ok(match (cfg.backend, cfg.instance.name()) {
        (ClusterBackend::Kind, None) => format!("http://127.0.0.1:{}", cfg.dispatcher_port),
        // A named install answers at its own door (instance-door.yaml).
        (ClusterBackend::Kind, Some(_)) => {
            format!("http://127.0.0.1:{}", instance_door_port(cfg).await?)
        }
        (ClusterBackend::K8s, _) => ctx.dispatcher_url().trim_end_matches('/').to_string(),
    })
}

/// A named install moves into the cluster the default install built,
/// and relies on what that install set up for everyone: the node, the
/// front door's gateway, the object store. Refuse, saying so, when it
/// is not there, rather than failing half way through an apply.
async fn require_default_install_up(cfg: &ClusterConfig, name: &str) -> Result<()> {
    let default_ns = weft_core::infra::Instance::default_install().system_namespace();
    let out = kubectl(&["get", "namespace", &default_ns, "-o", "name"]).output().await?;
    anyhow::ensure!(
        out.status.success(),
        "the install '{name}' lives beside the default install in cluster '{}', and that \
         one is not up (no namespace {default_ns}). Bring it up first with `./setup.sh` \
         or `weft daemon start` without {}.",
        cfg.cluster_name,
        weft_core::infra::INSTANCE_ENV
    );
    Ok(())
}

/// Why `weft daemon start` stops on the k8s backend, and what the
/// operator would have to bring for it to mean anything.
///
/// The backend exists because parts of weft are cluster-shaped already
/// (images pull from a registry, the manifests template every address).
/// What it does not have is anybody who installs the pieces `reconcile`
/// assumes: on kind weft installs the Envoy Gateway controller itself,
/// and then patches that controller's config to turn on the two
/// extension APIs `gateway.yaml` is written against.
/// None of that runs there, yet `gateway.yaml` is applied on both
/// backends. The kinder of the two outcomes is a raw apiserver error
/// about a CRD nobody registered; the other one is a clean apply onto a
/// controller running upstream defaults, where every live connection
/// then fails and the cluster looks healthy.
///
/// So this refuses, and says what it would need, rather than getting
/// half way and leaving the operator to work out which half.
fn refuse_k8s_backend() -> anyhow::Error {
    anyhow::anyhow!(
        "WEFT_CLUSTER_BACKEND=k8s: weft cannot bring a daemon up on a cluster it does not \
         build. Deploying weft beyond one machine is not designed yet, and the pieces \
         `weft daemon start` assumes are the ones it installs for itself on kind:\n\
         \n\
         \x20 - the Envoy Gateway controller, with `extensionApis.enableBackend` and \
         `extensionApis.enableEnvoyPatchPolicy` set to true in its `envoy-gateway-config` \
         ConfigMap and the controller restarted onto them. `deploy/k8s/gateway.yaml` is \
         written against both, and applies without complaint onto a controller that has \
         neither.\n\
         \x20 - the gateway variables: WEFT_GATEWAY_HOST (a wildcard host with DNS and a \
         certificate pointed at the gateway), WEFT_GATEWAY_BASE_URL, WEFT_CALLER_TOKEN_SECRET.\n\
         \x20 - the object store: WEFT_OBJECT_STORE_ENDPOINT, WEFT_OBJECT_STORE_PUBLIC_ENDPOINT, \
         WEFT_OBJECT_STORE_ACCESS_KEY, WEFT_OBJECT_STORE_SECRET_KEY, plus \
         WEFT_DISPATCHER_PUBLIC_BASE_URL.\n\
         \x20 - a kube context (WEFT_KUBE_CONTEXT) pointing at that cluster, and images in a \
         registry it can pull from (`WEFT_IMAGE_REGISTRY=<registry> weft build-images --push`).\n\
         \n\
         Unset WEFT_CLUSTER_BACKEND to run the local cluster weft owns."
    )
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
async fn reconcile(ctx: &Ctx, rebuild: bool, rebuild_cluster: bool, clear_access_apps: bool) -> Result<()> {
    let cfg = cluster_config();
    // The checkout this install comes from: manifests and the
    // shared-credentials file are read from it, and a successful boot
    // records it so a later `weft daemon start` from anywhere (a
    // project folder, say) finds the same one.
    let repo_root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    require_binary("kubectl").await?;
    require_binary("docker").await?;

    if cfg.backend == ClusterBackend::K8s {
        return Err(refuse_k8s_backend());
    }

    // The cluster + the Envoy Gateway controller before anything else (gateway.yaml's CRs need Envoy's
    // CRDs and its two extension APIs; everything needs a cluster to
    // apply into). All idempotent: a no-op once present, which is what
    // makes this self-healing over a missing/partial cluster (a fresh
    // machine, a `kind delete`, a daemon process alive while its
    // cluster is gone). Everything past the refusal above is on kind,
    // so this is not a branch so much as the shape of the one backend
    // that gets here.
    //
    // All of that is the cluster's, and the default install owns it. A
    // named install moves into a cluster the default install already
    // built, and never touches the node: rebuilding it would take the
    // default install's projects with it.
    if cfg.backend == ClusterBackend::Kind {
        require_binary("kind").await?;
        match cfg.instance.name() {
            None => {
                ensure_cluster(cfg, rebuild_cluster).await?;
                // The object store is a HOST docker container the
                // cluster reaches OUT to (the local stand-in for a real
                // S3 provider); up before anything that needs a bucket.
                ensure_object_store(cfg).await?;
                ensure_envoy_gateway().await?;
                retire_replaced_front_doors(cfg).await?;
                trim_single_node_control_plane(cfg).await?;
            }
            Some(name) => {
                anyhow::ensure!(
                    !rebuild_cluster,
                    "--rebuild-cluster rebuilds the node every install on it lives in; \
                     run it without {} (the default install owns the cluster), not for '{name}'",
                    weft_core::infra::INSTANCE_ENV
                );
                require_default_install_up(cfg, name).await?;
            }
        }
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
    let created = CreatedThisBoot {
        dispatcher: !workload_exists("statefulset", "weft-dispatcher", &cfg.system_namespace).await?,
        broker: !workload_exists("deployment", "weft-broker", &cfg.db_namespace).await?,
    };
    let (changes, tunnel) =
        apply_platform_state(cfg, &imgs, &repo_root, clear_access_apps, &mut pending).await?;

    recover_failed_dispatcher_update(&cfg.system_namespace, &imgs.dispatcher).await?;

    // The dispatcher exists (or was scaled back up) after the apply,
    // and a spec change (a new image tag rides the manifest) is
    // already rolling; wait before deciding the explicit rolls so a
    // `rollout restart` below never races a pod still being created.
    wait_workload_ready("statefulset", "weft-dispatcher", &cfg.system_namespace).await?;

    // One linear pass, so no combination of change flags can skip a
    // step it needed: the dispatcher roll and the broker roll each fire
    // on their own condition (see `rolls_for` for what triggers each),
    // never on a branch agreeing with the other.
    let rolls = rolls_for(&changes, rebuild, &created);
    if rolls.dispatcher {
        roll_workload("statefulset", "weft-dispatcher", &cfg.system_namespace).await?;
    }
    if rolls.broker {
        roll_workload("deployment", "weft-broker", &cfg.db_namespace).await?;
    }
    // Reachability from THIS machine is the gate: the rollout above
    // proved the pod Ready inside the cluster, and this proves the path
    // in (the node's port mapping and the NodePort Service on kind).
    let reach = dispatcher_reach_url(cfg, ctx).await?;
    wait_for_dispatcher_health(&reach).await?;
    if let Some(tunnel) = tunnel {
        announce_public_tunnel(tunnel).await?;
    }
    // The summary says what actually happened: applies and rolls are
    // different facts (a manifest change is applied but may roll
    // nothing here; a --rebuild rolls everything with no stamp moving),
    // and a line deduced from the wrong one lies in both directions.
    let mut applied: Vec<&str> = Vec::new();
    if changes.postgres_manifest || changes.other_manifests {
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
    // the change stamps, and the checkout this boot installed from.
    pending.flush();
    if cfg.instance.name().is_none() {
        record_repo_root(&repo_root)?;
    }

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
        "the daemon is up at {reach}, but the pooled listener / \
         supervisor tiers could not be fully reconciled:\n  {}\nRe-run \
         `weft daemon start` to retry, and inspect with \
         `kubectl --context {} get deploy -A -l weft.dev/role -o wide`.",
        pooled_failures.join("\n  "),
        cfg.kube_context,
    );
    let backend = match cfg.backend {
        ClusterBackend::Kind => "kind",
        ClusterBackend::K8s => "k8s",
    };
    println!(
        "daemon ready at {reach} ({} cluster '{}', system ns '{}')",
        backend, cfg.cluster_name, cfg.system_namespace,
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
    /// postgres.yaml moved: it carries the database-credentials
    /// Secret, which BOTH the dispatcher and the broker read as env
    /// (`secretKeyRef` -> WEFT_DATABASE_URL) injected only at pod
    /// start, invisible to either pod spec. Any postgres.yaml change
    /// rolls both (over-rolling on a non-credential edit is a cheap
    /// restart; under-rolling once left services on a dead URL).
    postgres_manifest: bool,
    /// Any other manifest moved (namespaces, dispatcher, broker,
    /// ingress, RBAC, gateway, the kind node ports): applied, and
    /// either self-rolling (a spec change) or effective with no
    /// restart (policies, routes, Services); feeds only the summary
    /// line.
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
/// twice on every code edit.
struct Rolls {
    dispatcher: bool,
    broker: bool,
}

/// Which workloads this boot's apply created. A pod that first started
/// after every secret, manifest and image of this boot was in place
/// already runs all of it, so it owes no restart (a fresh install's
/// restarts once doubled its start time).
struct CreatedThisBoot {
    dispatcher: bool,
    broker: bool,
}

fn rolls_for(c: &DetectedChanges, rebuilt: bool, created: &CreatedThisBoot) -> Rolls {
    Rolls {
        dispatcher: !created.dispatcher && (c.sealing_key || c.postgres_manifest || rebuilt),
        broker: !created.broker && (c.sealing_key || c.apps || c.postgres_manifest || rebuilt),
    }
}

/// Whether `kind/name` exists in `namespace`.
async fn workload_exists(kind: &str, name: &str, namespace: &str) -> Result<bool> {
    let out = kubectl(&["-n", namespace, "get", &format!("{kind}/{name}"), "--ignore-not-found", "-o", "name"])
        .output()
        .await?;
    anyhow::ensure!(
        out.status.success(),
        "kubectl get {kind}/{name} -n {namespace} failed: {}",
        String::from_utf8_lossy(&out.stderr).trim()
    );
    Ok(!out.stdout.is_empty())
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
        print_workload_diagnostics(kind, name, namespace).await;
        let ctx = &cluster_config().kube_context;
        anyhow::bail!(
            "{name} did not reach Ready within 180s.\n\
             Inspect it: kubectl --context {ctx} -n {namespace} describe {kind}/{name}\n\
             and:        kubectl --context {ctx} -n {namespace} logs {kind}/{name}"
        );
    }
    Ok(())
}

/// Print what the cluster says about a workload that never came up:
/// the pods, the namespace's recent events, and the container's logs.
/// This runs where the failure is READ, because whoever hits it is
/// usually installing for the first time and reports the console
/// output they have, not the output of two commands they were told to
/// run afterwards.
async fn print_workload_diagnostics(kind: &str, name: &str, namespace: &str) {
    let target = format!("{kind}/{name}");
    print_kubectl_read("pods", &["-n", namespace, "get", "pods", "-o", "wide"]).await;
    print_kubectl_read("events", &["-n", namespace, "get", "events", "--sort-by=.lastTimestamp"])
        .await;
    print_kubectl_read(
        &format!("{name} logs"),
        &["-n", namespace, "logs", &target, "--all-containers", "--tail=80"],
    )
    .await;
    // A container that crashed and was restarted holds the error that
    // killed it in the PREVIOUS container's log; the running one is
    // usually still empty when the rollout gives up.
    print_kubectl_read(
        &format!("{name} logs, the container before the restart"),
        &["-n", namespace, "logs", &target, "--all-containers", "--tail=80", "--previous"],
    )
    .await;
}

/// One titled block of kubectl output on stderr. Best effort on
/// purpose: the rollout has already failed with its own message, and a
/// read that fails (a pod too young to have logs, a kubectl that is
/// gone) must add nothing rather than bury it.
async fn print_kubectl_read(title: &str, args: &[&str]) {
    let Ok(out) = kubectl(args).output().await else {
        return;
    };
    if !out.status.success() {
        return;
    }
    let body = String::from_utf8_lossy(&out.stdout);
    let body = body.trim_end();
    if body.trim().is_empty() {
        return;
    }
    eprintln!("\n--- {title} ---\n{body}");
}

/// The kubelet's `waiting` reasons for a pod that will not come up on its
/// own: the process crashes, or its image cannot be pulled or configured.
const STUCK_WAITING_REASONS: [&str; 4] =
    ["CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError"];

/// StatefulSet updates wait for an unhealthy old pod even after its template
/// has been corrected. Replace only a stuck pod on a superseded image;
/// a failure on the requested image must remain visible for diagnosis.
fn dispatcher_needs_replacement(pod: &serde_json::Value, owner_uid: &str, image: &str) -> bool {
    if !pod["metadata"]["deletionTimestamp"].is_null() {
        return false;
    }
    let owned = pod["metadata"]["ownerReferences"].as_array().is_some_and(|owners| {
        owners.iter().any(|owner| owner["uid"] == owner_uid && owner["controller"] == true)
    });
    let ready = pod["status"]["conditions"].as_array().is_some_and(|conditions| {
        conditions.iter().any(|condition| condition["type"] == "Ready" && condition["status"] == "True")
    });
    owned && !ready && pod["spec"]["containers"].as_array().is_some_and(|containers| {
        containers.iter().any(|container| {
            container["name"] == "dispatcher"
                && container["image"].as_str().is_some_and(|old| old != image)
                && pod["status"]["containerStatuses"].as_array().is_some_and(|statuses| {
                    statuses.iter().any(|status| status["name"] == container["name"]
                        && status["state"]["waiting"]["reason"].as_str()
                            .is_some_and(|reason| STUCK_WAITING_REASONS.contains(&reason)))
                })
        })
    })
}

async fn recover_failed_dispatcher_update(namespace: &str, image: &str) -> Result<()> {
    let output = kubectl(&["-n", namespace, "get", "statefulset/weft-dispatcher", "-o", "json"])
        .output().await?;
    anyhow::ensure!(output.status.success(), "inspect dispatcher update: {}", String::from_utf8_lossy(&output.stderr));
    let statefulset: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let owner = statefulset["metadata"]["uid"].as_str().context("dispatcher StatefulSet has no UID")?;
    let output = kubectl(&["-n", namespace, "get", "pods", "-o", "json"]).output().await?;
    anyhow::ensure!(output.status.success(), "inspect dispatcher pods: {}", String::from_utf8_lossy(&output.stderr));
    let pods: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    for pod in pods["items"].as_array().context("pod list has no items")? {
        if !dispatcher_needs_replacement(pod, owner, image) {
            continue;
        }
        let name = pod["metadata"]["name"].as_str().context("dispatcher pod has no name")?;
        let uid = pod["metadata"]["uid"].as_str().context("dispatcher pod has no UID")?;
        // The UID alone pins the pod that was inspected: a replacement the
        // controller already made has a new one. Its resourceVersion is
        // not pinned, because the kubelet rewrites a crashing pod's status
        // on every backoff, and a stale version would turn this into a
        // conflict on exactly the pods it exists to replace.
        let options = serde_json::json!({
            "apiVersion": "v1", "kind": "DeleteOptions",
            "preconditions": { "uid": uid }
        });
        let file = tempfile::NamedTempFile::new()?;
        serde_json::to_writer(file.as_file(), &options)?;
        eprintln!("replacing stuck dispatcher {name} from a superseded image");
        // Graceful deletion preserves shutdown guarantees. The precondition
        // forbids deleting a replacement.
        let path = format!("/api/v1/namespaces/{namespace}/pods/{name}");
        let output = kubectl(&["delete", "--raw", &path, "-f", file.path().to_str().context("delete options path is not UTF-8")?])
            .output().await?;
        anyhow::ensure!(output.status.success(), "replace stale dispatcher {name}: {}; rerun setup to inspect its current state", String::from_utf8_lossy(&output.stderr));
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
    repo_root: &Path,
    clear_access_apps: bool,
    pending: &mut PendingStamps,
) -> Result<(DetectedChanges, Option<OpenedTunnel>)> {
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
    // The node ports the kind node maps to this machine's loopback
    // (see `MappedPort`): one NodePort Service per door, in the
    // namespaces the cluster and gateway installs above created. Kind only; a real cluster is reached at the operator's
    // external addresses. A named install has one door of its own
    // instead, at whatever node port the apiserver gives it, and every
    // link it mints points there.
    if cfg.backend == ClusterBackend::Kind {
        match cfg.instance.name() {
            None => {
                other_manifests |= kubectl_apply_changed(
                    &manifests.join("kind-node-ports.yaml"),
                    &template_vars,
                    pending,
                )
                .await?;
            }
            Some(_) => {
                other_manifests |= kubectl_apply_changed(
                    &manifests.join("instance-door.yaml"),
                    &template_vars,
                    pending,
                )
                .await?;
                let door = instance_door_port(cfg).await?;
                set_template_var(
                    &mut template_vars,
                    "WEFT_DISPATCHER_PUBLIC_BASE_URL",
                    format!("http://127.0.0.1:{door}"),
                );
            }
        }
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
    // The tunnel belongs to the default install: it opens the front
    // door's public listener, which routes to that install only.
    // Its reachability is proven later (`announce_public_tunnel`), once
    // the gateway and dispatcher it leads to are up.
    let mut tunnel = None;
    if cfg.instance.name().is_none() {
        tunnel = reconcile_public_tunnel(&manifests).await?;
        if let Some(t) = &tunnel {
            set_template_var(&mut template_vars, "WEFT_DISPATCHER_INTERNET_URL", t.url.clone());
        }
    }
    // Postgres before everything that needs a database, gated to Ready
    // so a fresh cluster's broker never crash-loops on a missing DB.
    // Its change flag is its OWN: postgres.yaml carries the database
    // credentials both the dispatcher and the broker read as env at
    // pod start (see `DetectedChanges::postgres_manifest`).
    prepare_postgres_apply(cfg, &manifests.join("postgres.yaml")).await?;
    let postgres_manifest =
        kubectl_apply_changed(&manifests.join("postgres.yaml"), &template_vars, pending).await?;
    wait_workload_ready("deployment", "weft-postgres", &cfg.db_namespace).await?;
    // Provider keys + OAuth apps before the broker: its pods import
    // the key secret via `envFrom` and MOUNT the apps secret at start,
    // so both must exist when a fresh pod comes up. A change to either
    // is invisible to the pod SPEC, which is why they carry their own
    // change signals (the caller rolls the broker on them).
    let sealing_key = apply_sealing_key_secret(cfg, pending).await?;
    let apps = apply_access_apps_secret(cfg, repo_root, clear_access_apps, pending).await?;
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
    for name in ["dispatcher.yaml", "cluster-rbac.yaml"] {
        other_manifests |=
            kubectl_apply_changed(&manifests.join(name), &template_vars, pending).await?;
    }
    // The front door (Envoy Gateway CRs): live callers, the local door
    // to the dispatcher, the public allowlist. These need the
    // controller's CRDs registered and its two extension APIs turned
    // on, which `ensure_envoy_gateway` did above. It only ran because
    // we are on kind: the k8s backend is refused at the top of
    // `reconcile` precisely so this never applies onto a controller
    // nobody set up. `${GATEWAY_HOST}` is substituted from template
    // vars. The default install owns it: a named install's live
    // callers ride the same gateway (its routes attach to it by
    // namespace), and it has no local door there.
    if cfg.instance.name().is_none() {
        other_manifests |=
            kubectl_apply_changed(&manifests.join("gateway.yaml"), &template_vars, pending)
                .await?;
    }
    Ok((
        DetectedChanges {
            postgres_manifest,
            other_manifests,
            sealing_key,
            apps,
        },
        tunnel,
    ))
}

/// Replace one template var already in the set: for the values only
/// known once part of the install is applied (the tunnel's address, a
/// named install's door).
fn set_template_var(vars: &mut [(&'static str, String)], key: &str, value: String) {
    let slot = vars
        .iter_mut()
        .find(|(k, _)| *k == key)
        .unwrap_or_else(|| panic!("template var {key} is always in the set"));
    slot.1 = value;
}

/// The node port the apiserver gave a named install's door
/// (deploy/k8s/instance-door.yaml).
async fn instance_door_port(cfg: &ClusterConfig) -> Result<u16> {
    let out = kubectl(&[
        "-n",
        &cfg.system_namespace,
        "get",
        "service",
        "weft-dispatcher-node-port",
        "-o",
        "jsonpath={.spec.ports[0].nodePort}",
    ])
    .output()
    .await?;
    anyhow::ensure!(
        out.status.success(),
        "reading the node port of {}'s door failed: {}",
        cfg.system_namespace,
        String::from_utf8_lossy(&out.stderr)
    );
    let raw = String::from_utf8_lossy(&out.stdout).trim().to_string();
    raw.parse().map_err(|_| {
        anyhow::anyhow!("{}'s door has no node port yet (read '{raw}')", cfg.system_namespace)
    })
}

pub fn data_dir() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    home.join(".local/share/weft")
}

// ----- The opt-in public trigger surface ------------------------------
//
// `--public-url` runs an outbound tunnel inside the cluster
// (deploy/k8s/public-tunnel.yaml) onto the front door's `public`
// listener, so a local install gets a public https address for exactly
// its public trigger surface (the allowlist in deploy/k8s/gateway.yaml)
// and nothing else. The choice is
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

/// Bring the tunnel and its door up (or tear them down) to match
/// the persisted choice, and answer the public https address when one
/// is up. Quick mode reads the minted random address from the tunnel's
/// own logs (the only authority for a per-connection address); named
/// mode's address is the configured hostname.
async fn reconcile_public_tunnel(manifests: &std::path::Path) -> Result<Option<OpenedTunnel>> {
    let cfg = cluster_config();
    let manifest = manifests.join("public-tunnel.yaml");
    // The manifest carries `${TUNNEL_ARGS}` (the mode's argv); any
    // kubectl that PARSES it needs the substitution, deletes included.
    let quick_args = format!(r#"["tunnel", "--no-autoupdate", "--url", "{PUBLIC_DOOR_URL}"]"#);
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
        // Teardown closes a PUBLIC surface, so every kubectl step is
        // checked: reporting success while the tunnel still serves
        // would leave the operator believing a door is shut that is
        // not. The substituted value never reaches the cluster on this
        // path (kubectl deletes by kind/name and never reads the
        // bodies); it only has to render the manifest parseable, so a
        // delete never depends on the cluster still answering.
        kubectl_delete_rendered(&manifest, &[("TUNNEL_ARGS", quick_args)]).await?;
        delete_tunnel_token_secret().await?;
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
    // Content-hash apply. A tunnel-Deployment spec change (the mode's
    // argv switched) rolls the tunnel pod by itself; a secret-only
    // change rolls it explicitly, since nothing else would re-inject the
    // new token. The door the tunnel forwards to is the gateway's
    // `public` listener, applied with the rest of the front door.
    kubectl_apply_changed(&manifest, &[("TUNNEL_ARGS", tunnel_args)], &mut pending).await?;
    if secret_changed {
        roll_workload("deployment", "weft-tunnel", &cfg.system_namespace).await?;
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
    Ok(Some(OpenedTunnel { url, named: named.is_some() }))
}

/// A public tunnel that is up, not yet proven to reach the door.
struct OpenedTunnel {
    url: String,
    named: bool,
}

/// Prove the tunnel reaches the door, then record and announce its
/// address. Runs only once the gateway (whose `public` listener the
/// tunnel forwards to) is applied and the dispatcher behind it answers
/// health: before that, a fresh install's check could never pass.
async fn announce_public_tunnel(tunnel: OpenedTunnel) -> Result<()> {
    let OpenedTunnel { url, named } = tunnel;
    // The pod being ready says the tunnel reached Cloudflare, nothing
    // about the hop after it: a named tunnel's destination is set in the
    // Cloudflare dashboard, where nothing here can see it. So the address
    // is recorded only once a request through it reached the door.
    verify_public_url(&url, named).await?;
    std::fs::create_dir_all(data_dir())?;
    std::fs::write(public_url_file(), &url)?;
    println!("public trigger surface reachable at {url}");
    println!("  exposed through the public door: /events/... (provider event pushes), /signal/... (per-signal fire tokens), /signal-token/... (token listing), /public/files/... (minted expiring media links), and the OAuth callback; everything else answers 404.");
    println!("  Rerun with --no-public-url to close it.");
    if !named {
        println!(
            "  NOTE: this free-tunnel address changes whenever the tunnel reconnects, \
             and everything registered against it (a provider's event push URL, an \
             OAuth redirect) rots until re-registered. For a stable address, set \
             WEFT_PUBLIC_TUNNEL_TOKEN + WEFT_PUBLIC_TUNNEL_HOSTNAME \
             (https://weavemindai.github.io/weft/build/public-address.html)."
        );
    }
    Ok(())
}

/// Where the tunnel sends what arrives: the Service in front of the
/// gateway's `public` listener. A named tunnel's Cloudflare dashboard
/// must name exactly this.
// SYNC: weft-public-door:8080 <-> deploy/k8s/public-tunnel.yaml, deploy/k8s/gateway.yaml (listener `public`)
const PUBLIC_DOOR_URL: &str = "http://weft-public-door.envoy-gateway-system.svc.cluster.local:8080";

/// Fetch the public page at the bare root of `url`, through the tunnel,
/// until it answers or the tunnel had long enough to settle (a quick
/// tunnel's fresh hostname takes a while to resolve). An internal wait:
/// nothing a person controls is on the other end.
async fn verify_public_url(url: &str, named: bool) -> Result<()> {
    let client = reqwest::Client::builder().timeout(std::time::Duration::from_secs(10)).build()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
    loop {
        let seen = match client.get(format!("{url}/")).send().await {
            Ok(r) if r.status().is_success() => return Ok(()),
            Ok(r) => format!("it answered {}", r.status()),
            Err(e) => format!("the request failed: {e}"),
        };
        if std::time::Instant::now() > deadline {
            anyhow::bail!("{}", public_url_unreachable(url, named, &seen));
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// What to say when the public address does not reach the door.
fn public_url_unreachable(url: &str, named: bool, seen: &str) -> String {
    let cfg = cluster_config();
    let logs = format!(
        "kubectl --context {} -n {} logs deployment/weft-tunnel",
        cfg.kube_context, cfg.system_namespace
    );
    if named {
        format!(
            "{url} does not reach weft's public door ({seen}). The tunnel is connected, so \
             the likely cause is where Cloudflare sends the traffic: in the Cloudflare \
             dashboard, the tunnel's public hostname for {url} must have the service \
             {PUBLIC_DOOR_URL}. The tunnel's log names the destination it tried: {logs}"
        )
    } else {
        format!(
            "{url} does not reach weft's public door ({seen}). The tunnel's log says why: {logs}"
        )
    }
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

/// `weft daemon remove`: take a NAMED install off the cluster, leaving
/// nothing of it behind: its namespaces (every project's with them),
/// its cluster-wide objects, its database files inside the node, its
/// bucket, and its change stamps. The default install is refused: it
/// holds everyone's projects, and `./setup.sh --uninstall` is its way
/// out, with the choices that deserves.
async fn remove_named_install() -> Result<()> {
    let cfg = cluster_config();
    let Some(name) = cfg.instance.name() else {
        anyhow::bail!(
            "`weft daemon remove` takes a named install off the cluster, and {} is not \
             set, which means the default install. That one holds every project on this \
             machine; `./setup.sh --uninstall` is how it goes.",
            weft_core::infra::INSTANCE_ENV
        );
    };
    let instance = &cfg.instance;
    // Postgres first, and waited for: its files are removed from the
    // node next, and a server still running would write into a
    // directory being deleted under it.
    let postgres = kubectl(&[
        "-n", &cfg.db_namespace, "delete", "deployment", "weft-postgres",
        "--ignore-not-found", "--wait=true",
    ])
    .output()
    .await?;
    anyhow::ensure!(
        postgres.status.success(),
        "stopping {name}'s Postgres failed: {}",
        String::from_utf8_lossy(&postgres.stderr)
    );
    if cfg.backend == ClusterBackend::Kind {
        let node = format!("{}-control-plane", cfg.cluster_name);
        let dir = format!("{NAMED_INSTALLS_NODE_PATH}/{name}");
        let out = images::docker().args(["exec", &node, "rm", "-rf", &dir]).output().await?;
        anyhow::ensure!(
            out.status.success(),
            "removing {dir} inside the node {node} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    // Every namespace the install created: its two own, and every one
    // its dispatcher made for tenants (they all start with the
    // install's prefix, the shared worker namespace included). Not
    // waited for: a namespace finishes terminating on its own, and
    // nothing of another install can be in it.
    let listing = kubectl(&["get", "namespaces", "-o", "jsonpath={.items[*].metadata.name}"])
        .output()
        .await?;
    anyhow::ensure!(
        listing.status.success(),
        "listing namespaces failed: {}",
        String::from_utf8_lossy(&listing.stderr)
    );
    let namespaces = install_namespaces(instance, &String::from_utf8_lossy(&listing.stdout));
    if !namespaces.is_empty() {
        let mut args = vec!["delete", "namespace", "--ignore-not-found", "--wait=false"];
        args.extend(namespaces.iter().map(String::as_str));
        let out = kubectl(&args).output().await?;
        anyhow::ensure!(
            out.status.success(),
            "deleting {name}'s namespaces failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    // Its cluster-wide objects, by the names `install_template_vars`
    // gave them.
    for (kind, object) in [
        ("clusterrolebinding", instance.cluster_object("weft-dispatcher")),
        ("clusterrolebinding", instance.cluster_object("weft-broker-tokenreview")),
        ("persistentvolume", postgres_volume(instance)),
    ] {
        let out = kubectl(&["delete", kind, &object, "--ignore-not-found", "--wait=false"])
            .output()
            .await?;
        anyhow::ensure!(
            out.status.success(),
            "deleting {kind} {object} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    if cfg.backend == ClusterBackend::Kind {
        delete_install_bucket(&object_store_bucket(instance)).await?;
    }
    let stamps = install_stamp_dir();
    match std::fs::remove_dir_all(&stamps) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => anyhow::bail!("removing {}: {e}", stamps.display()),
    }
    println!("install '{name}' removed");
    Ok(())
}

/// Which of the cluster's namespaces (a space-separated listing) belong
/// to a named install: its system and db namespaces, and every tenant
/// namespace its dispatcher created.
fn install_namespaces(instance: &weft_core::infra::Instance, listing: &str) -> Vec<String> {
    let own = [instance.system_namespace(), instance.db_namespace()];
    let tenant_prefix = instance.tenant_prefix();
    listing
        .split_whitespace()
        .filter(|ns| own.iter().any(|o| o == ns) || ns.starts_with(tenant_prefix.as_str()))
        .map(str::to_string)
        .collect()
}

/// Drop a named install's bucket from the object store. The store's
/// shell exits 0 on errors, so the bucket listing afterwards is what
/// says whether it is gone.
async fn delete_install_bucket(bucket: &str) -> Result<()> {
    let weed = |command: String| {
        images::docker()
            .args(["exec", OBJECT_STORE_CONTAINER, "sh", "-c", &format!("echo '{command}' | weed shell")])
            .output()
    };
    weed(format!("s3.bucket.delete -name {bucket}")).await?;
    let listing = weed("s3.bucket.list".to_string()).await?;
    anyhow::ensure!(
        listing.status.success(),
        "listing the object store's buckets failed: {}",
        String::from_utf8_lossy(&listing.stderr)
    );
    let still_there = String::from_utf8_lossy(&listing.stdout)
        .split_whitespace()
        .any(|word| word == bucket);
    anyhow::ensure!(
        !still_there,
        "the bucket {bucket} is still on the object store after deleting it; remove it with \
         `docker exec {OBJECT_STORE_CONTAINER} sh -c 'echo \"s3.bucket.delete -name {bucket}\" | weed shell'`"
    );
    Ok(())
}

async fn stop() -> Result<()> {
    let cfg = cluster_config();
    let _ = kubectl(&[
        "-n", &cfg.system_namespace, "scale", "statefulset/weft-dispatcher", "--replicas=0",
    ])
    .status()
    .await;
    println!("daemon stopped");
    Ok(())
}

async fn status(ctx: &Ctx) -> Result<()> {
    let cfg = cluster_config();
    match ctx.client().get_json("/projects").await {
        Ok(v) => {
            let n = v.as_array().map(|a| a.len()).unwrap_or(0);
            println!(
                "daemon: running (cluster '{}', system ns '{}'); {} project(s)",
                cfg.cluster_name, cfg.system_namespace, n,
            );
        }
        Err(e) => {
            println!("daemon: unreachable at {}: {e}", ctx.client().base());
        }
    }
    // The public trigger surface, when it is open: what providers
    // deliver events to, and what a trigger's setup instructions ask
    // the operator to paste at the provider.
    // The surface belongs to the default install (the tunnel opens the
    // front door's public listener, which routes to it alone), so the
    // machine-wide address says nothing about a named one.
    if let Some(name) = cfg.instance.name() {
        println!(
            "public trigger surface: none for install '{name}' (the default install owns it)"
        );
        return Ok(());
    }
    match current_public_url() {
        Some(url) => println!(
            "public trigger surface: {url} (the public trigger routes only; \
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

// ----- Cluster bootstrap ------------------------------------------------

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
///
/// Only what a running node cannot change belongs here: a rebuild
/// destroys every project's own database, so a setting that can be
/// applied to the live node (a label, a component flag) is applied there
/// instead (see `trim_single_node_control_plane`).
///
/// The port mappings are the whole path from this machine into the
/// cluster: docker publishes each configured loopback port straight to
/// the node port its NodePort Service pins (see [`MappedPort`]), so a
/// connection to `127.0.0.1:<dispatcher_port>` is DNAT'd to the
/// dispatcher pod with no process on this machine in between.
/// Loopback only: the dispatcher's API must never listen on a LAN
/// address, and kind's default listen address is `0.0.0.0`.
fn kind_cluster_config(cfg: &ClusterConfig) -> String {
    let host_dir = postgres_data_dir();
    let mappings: String = MappedPort::ALL
        .iter()
        .map(|&m| (m.node_port(), m.host_port(cfg)))
        // The band a door lands on, published at the same number on
        // both sides: one number for a person to read, and the whole
        // band rather than one port per door because a kind node's
        // mappings are fixed when it is built, and a door opened later
        // has to have somewhere to land without rebuilding the
        // cluster (which destroys every project's database).
        // ...and the rest of the node-port range, at the same number on
        // both sides. The WHOLE range, because the apiserver is what
        // allocates a door's port and a port it could pick that the
        // machine does not publish would answer nothing. Narrow enough
        // to publish (see `NODE_PORTS`), and fixed when the node is
        // built, so a door opened next week lands without rebuilding
        // the cluster (which destroys every project's database).
        .chain(
            weft_core::infra::NODE_PORTS
                .filter(|port| !MappedPort::ALL.iter().any(|m| m.node_port() == *port))
                .map(|port| (port, port)),
        )
        .map(|(node_port, host_port)| {
            format!(
                "      - containerPort: {node_port}\n        hostPort: {host_port}\n        listenAddress: \"127.0.0.1\"\n        protocol: TCP\n"
            )
        })
        .collect();
    let node_port_range = format!(
        "{}-{}",
        weft_core::infra::NODE_PORTS.start(),
        weft_core::infra::NODE_PORTS.end()
    );
    format!(
        r#"kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    kubeadmConfigPatches:
      - |
        kind: ClusterConfiguration
        apiServer:
          extraArgs:
            service-node-port-range: "{node_port_range}"
    extraPortMappings:
{mappings}    extraMounts:
      - hostPath: {}
        containerPath: {NODE_POSTGRES_PATH}
"#,
        host_dir.display(),
        node_port_range = node_port_range,
    )
}

/// Make sure every mapped loopback port can bind before a node is
/// built. Docker binds each one when the node container starts, and a
/// port something else holds fails `kind create cluster` with a docker
/// error that names neither the port's purpose nor the holder.
async fn free_mapped_ports(cfg: &ClusterConfig) -> Result<()> {
    for m in MappedPort::ALL {
        let port = m.host_port(cfg);
        if let Err(e) = std::net::TcpListener::bind(("127.0.0.1", port)) {
            anyhow::bail!(
                "127.0.0.1:{port} is not free ({e}), and the kind node needs it for the \
                 {m:?} port mapping. Find what holds it (`ss -ltnp | grep :{port}`), stop \
                 it, and re-run `weft daemon start`."
            );
        }
    }
    Ok(())
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

/// Record the checkout the daemon was just installed from, so the
/// resolver's last rung (`weft_catalog::weft_repo_root`) answers with
/// it from any working directory. setup.sh writes the same file for a
/// prebuilt CLI; the daemon writes it on every successful boot.
// SYNC: repo-root file <-> crates/weft-catalog/src/lib.rs (weft_repo_root), setup.sh (prebuilt CLI install: repo-root write)
fn record_repo_root(root: &Path) -> Result<()> {
    std::fs::create_dir_all(data_dir())?;
    let file = data_dir().join("repo-root");
    std::fs::write(&file, root.to_string_lossy().as_bytes())
        .with_context(|| format!("record the weft checkout in {}", file.display()))?;
    Ok(())
}

/// Record what was just installed, after it succeeded.
fn record_install(name: &str, want: &str) -> Result<()> {
    std::fs::create_dir_all(data_dir())?;
    std::fs::write(data_dir().join(format!("installed-{name}.txt")), want)?;
    Ok(())
}

/// Drop the record of an install the cluster no longer carries.
fn forget_install(name: &str) -> Result<()> {
    match std::fs::remove_file(data_dir().join(format!("installed-{name}.txt"))) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

/// Where the fingerprint of the config the current node was built from is
/// kept.
fn kind_config_stamp() -> PathBuf {
    data_dir().join("kind-cluster-config.sha256")
}


async fn ensure_cluster(cfg: &ClusterConfig, rebuild_cluster: bool) -> Result<()> {
    let config = kind_cluster_config(cfg);
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
    // a new node, and a shape change rebuilds on its own, because there
    // is nothing else a person could do with the old node (an install
    // has to come up by itself). The SYSTEM database survives that (its
    // files live on the host, mounted back in), but every project's own
    // database (a PostgresDatabase infra node's volume) lives inside the
    // node and dies with it, so the rebuild says so before it starts.
    // `--rebuild-cluster` forces one when the shape did not change.
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
        } else if have != Some(want.as_str()) || rebuild_cluster {
            let why = if have != Some(want.as_str()) {
                "its shape changed (kind config or kind version), and a node cannot change in place"
            } else {
                "--rebuild-cluster asked for it"
            };
            println!("rebuilding the kind node: {why}. {}", rebuild_data_notice());
            let status = images::quiet_stdout("kind")
                .args(["delete", "cluster", "--name", &cfg.cluster_name])
                .status()
                .await?;
            anyhow::ensure!(status.success(), "kind delete cluster failed with {status}");
            return create_cluster(cfg, &config, &want).await;
        }
        ensure_cluster_nodes_running(cfg).await?;
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

/// Start existing cluster nodes and preserve automatic startup after host reboots.
async fn ensure_cluster_nodes_running(cfg: &ClusterConfig) -> Result<()> {
    let out = images::quiet_stdout("kind")
        .args(["get", "nodes", "--name", &cfg.cluster_name]).output().await?;
    anyhow::ensure!(out.status.success(), "kind get nodes failed with {}", out.status);
    let names = String::from_utf8(out.stdout)?;
    anyhow::ensure!(!names.trim().is_empty(), "cluster '{}' has no nodes", cfg.cluster_name);
    for node in names.lines().map(str::trim).filter(|name| !name.is_empty()) {
        let status = images::docker().args(["update", "--restart", "unless-stopped", node]).status().await?;
        anyhow::ensure!(status.success(), "setting restart policy for {node} failed with {status}");
        let out = images::docker().args(["inspect", "--format", "{{.State.Running}}", node]).output().await?;
        anyhow::ensure!(out.status.success(), "inspecting cluster node {node} failed with {}", out.status);
        if String::from_utf8_lossy(&out.stdout).trim() != "true" {
            let status = images::docker().args(["start", node]).status().await?;
            anyhow::ensure!(status.success(), "starting cluster node {node} failed with {status}");
        }
    }
    Ok(())
}

/// Build the node from `config` and record the fingerprint it was built from.
async fn create_cluster(cfg: &ClusterConfig, config: &str, fingerprint: &str) -> Result<()> {
    free_mapped_ports(cfg).await?;
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
    ensure_cluster_nodes_running(cfg).await?;
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

/// This machine's address as the cluster's containers see it: what a
/// pod dials to reach a port published on this machine (the object
/// store, a service a trigger watches). Stable for the life of the
/// docker install, so it can sit in a pod's environment.
///
/// Two docker flavours, two answers. Docker Desktop (macOS, Windows)
/// runs docker inside a virtual machine and publishes ports on the
/// machine OUTSIDE it, so the docker network's gateway reaches
/// nothing; the one address that leads back out is the name
/// `host.docker.internal`, which every container there can resolve,
/// so the node is asked to. Docker on Linux defines no such name: the
/// machine IS the docker host, and `gateway`, the kind network's
/// gateway, is its address on that network.
// SYNC: the machine address pods dial <-> scripts/run-e2e.sh
//       (the WEFT_E2E_S3_ENDPOINT block)
async fn machine_address_for_pods(cfg: &ClusterConfig, gateway: &str) -> Result<String> {
    let node = format!("{}-control-plane", cfg.cluster_name);
    let out = images::docker()
        .args(["exec", &node, "getent", "hosts", "host.docker.internal"])
        .output()
        .await?;
    if out.status.success() {
        let stdout = String::from_utf8_lossy(&out.stdout);
        if let Some(ip) = stdout
            .split_whitespace()
            .find(|word| word.parse::<std::net::Ipv4Addr>().is_ok())
        {
            return Ok(ip.to_string());
        }
    }
    Ok(gateway.to_string())
}

/// The IPv4 side of the `kind` docker network, as docker reports it.
/// Read at runtime: docker picks the range per machine.
struct KindNetworkIpv4 {
    /// The network's gateway: this machine's address on it (docker
    /// for Linux), and the source every packet from this machine
    /// carries into the node through a published port (both flavours).
    gateway: String,
    /// The whole range, which the node's own address is somewhere in.
    subnet: ipnet::Ipv4Net,
}

async fn kind_network_ipv4() -> Result<KindNetworkIpv4> {
    let out = images::docker()
        .args([
            "network",
            "inspect",
            "kind",
            "-f",
            "{{range .IPAM.Config}}{{.Subnet}}={{.Gateway}} {{end}}",
        ])
        .output()
        .await?;
    if !out.status.success() {
        anyhow::bail!("docker network inspect kind failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    // The network has an IPv6 range beside the IPv4 one; the IPv4
    // range is the one that parses as such.
    let stdout = String::from_utf8_lossy(&out.stdout);
    stdout
        .split_whitespace()
        .find_map(|pair| {
            let (subnet, gateway) = pair.split_once('=')?;
            let subnet: ipnet::Ipv4Net = subnet.parse().ok()?;
            Some(KindNetworkIpv4 { gateway: gateway.to_string(), subnet })
        })
        .ok_or_else(|| anyhow::anyhow!("no IPv4 range on the kind docker network: {stdout}"))
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
/// The cluster reaches OUT to it over S3, exactly as it would to a real
/// provider: the store is never inside the cluster. Idempotent: a running
/// container is left alone, a stopped one is started, else it is created.
/// `-s3.externalUrl` is deliberately UNSET so the gateway validates each
/// presigned request against its own incoming Host header (v3.80 behavior),
/// letting the SAME instance accept URLs signed for this machine's address as
/// pods see it AND for 127.0.0.1 (the browser).
async fn ensure_object_store(cfg: &ClusterConfig) -> Result<()> {
    let run_args = object_store_run_args(cfg);
    // What the container would be run with today. A container cannot change
    // its ports, mounts or image in place, so when this text moves the
    // container is rebuilt. Its data volume is named and is not touched, so
    // rebuilding costs nothing but the restart.
    let want = run_args.join(" ");
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
    let status = images::docker().args(&run_args).status().await?;
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

/// The kind node's control-plane components that elect a leader among
/// copies of themselves. On one node there is only ever one copy, so the
/// election is a lease written to the apiserver every couple of seconds
/// for nothing: most of an idle cluster's writes.
const LEADER_ELECTED_MANIFESTS: [&str; 2] = [
    "/etc/kubernetes/manifests/kube-scheduler.yaml",
    "/etc/kubernetes/manifests/kube-controller-manager.yaml",
];

/// Take out what a one-node cluster pays for copies it cannot have:
/// leader election in the scheduler and the controller manager (their
/// static pod manifests, which the kubelet restarts them from when the
/// file changes), and the second CoreDNS replica. Only ever run on the
/// kind node weft builds, which is one node by construction; none of it
/// limits how much that node can run. Done here rather than in the kind
/// config so a node that already exists gets it without being rebuilt.
async fn trim_single_node_control_plane(cfg: &ClusterConfig) -> Result<()> {
    let node = format!("{}-control-plane", cfg.cluster_name);
    for manifest in LEADER_ELECTED_MANIFESTS {
        let out = images::docker().args(["exec", &node, "cat", manifest]).output().await?;
        anyhow::ensure!(
            out.status.success(),
            "reading {manifest} on the kind node failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let Some(trimmed) = without_leader_election(&String::from_utf8_lossy(&out.stdout))? else {
            continue;
        };
        println!("turning off leader election in {manifest} (one node, one copy)");
        // Written in place (the same file, truncated), never beside it:
        // the kubelet runs every manifest in that directory, so a
        // temporary file there would start a second copy.
        let mut child = images::docker()
            .args(["exec", "-i", &node, "sh", "-c", &format!("cat > {manifest}")])
            .stdin(std::process::Stdio::piped())
            .spawn()?;
        {
            use tokio::io::AsyncWriteExt;
            let mut stdin = child.stdin.take().expect("stdin was piped");
            stdin.write_all(trimmed.as_bytes()).await?;
        }
        let status = child.wait().await?;
        anyhow::ensure!(status.success(), "writing {manifest} on the kind node failed with {status}");
    }
    let out = kubectl(&["-n", "kube-system", "scale", "deployment", "coredns", "--replicas=1"])
        .output()
        .await?;
    anyhow::ensure!(
        out.status.success(),
        "scaling CoreDNS to one replica failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

/// A control-plane static pod manifest with leader election turned off,
/// or `None` when it already is. The manifest has to carry the flag the
/// way kubeadm writes it: anything else is a manifest this does not
/// understand, refused rather than edited blind.
fn without_leader_election(manifest: &str) -> Result<Option<String>> {
    const ON: &str = "- --leader-elect=true\n";
    const OFF: &str = "- --leader-elect=false\n";
    if manifest.contains(OFF) {
        return Ok(None);
    }
    anyhow::ensure!(
        manifest.matches(ON).count() == 1,
        "the manifest does not carry `--leader-elect=true` once, as kubeadm writes it"
    );
    Ok(Some(manifest.replacen(ON, OFF, 1)))
}

/// Remove the front doors the Envoy Gateway replaced, from a cluster an
/// earlier weft installed them on: the ingress-nginx controller (its
/// whole namespace, and its node port, which the gateway's `local`
/// listener now takes), the dispatcher's Ingress object, and the public
/// trigger surface's nginx proxy. Left running, the old controller would
/// hold the node port the new door needs and keep its own leader-election
/// lease renewing for nothing. Idempotent: each delete ignores what is
/// already gone, so a cluster that never had them passes through.
async fn retire_replaced_front_doors(cfg: &ClusterConfig) -> Result<()> {
    // The old controller's NodePort Service goes first, and is waited
    // out: it holds the node port `kind-node-ports.yaml` gives the local
    // door, and the namespace delete below returns before its Services
    // are gone, so the door's apply would meet "port already allocated".
    let deletes: [&[&str]; 4] = [
        &[
            "-n", "ingress-nginx", "delete", "service", "ingress-nginx-node-port",
            "--ignore-not-found", "--wait=true",
        ],
        &["delete", "namespace", "ingress-nginx", "--ignore-not-found", "--wait=false"],
        &[
            "-n", &cfg.system_namespace, "delete", "ingress", "weft-dispatcher", "--ignore-not-found",
        ],
        &[
            "-n", &cfg.system_namespace, "delete",
            "deployment/weft-public-proxy", "service/weft-public-proxy",
            "configmap/weft-public-proxy-config", "networkpolicy/public-proxy",
            "configmap/weft-public-page",
            "--ignore-not-found",
        ],
    ];
    for args in deletes {
        let out = kubectl(args).output().await?;
        anyhow::ensure!(
            out.status.success(),
            "removing a front door the Envoy Gateway replaced failed (`kubectl {}`): {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    forget_install("ingress")?;
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
    configure_envoy_gateway().await?;
    Ok(())
}

/// What weft needs from the Envoy Gateway controller's own config, set
/// in place. `EnvoyGateway` is the config FILE's kind, living in the
/// `envoy-gateway-config` ConfigMap, not a cluster CR, so the ConfigMap
/// is patched and the controller restarted onto it. A no-op once all of
/// it is there.
///
///   - `extensionApis.enableBackend`: the DynamicResolver the live route
///     resolves each worker pod through, so one route serves every pod.
///   - `extensionApis.enableEnvoyPatchPolicy`: lets `gateway.yaml` put a
///     socket option on the listener, which is how a caller who VANISHES
///     (no close, no goodbye: a closed lid, a dropped network) is ever
///     noticed. Without it the gateway holds that connection, and the
///     worker's run behind it, until the kernel gives up a quarter of an
///     hour later.
///   - `provider.kubernetes.leaderElection.disable`: the controller runs
///     one copy on a one-node cluster, so there is never another to hand
///     over to, and electing a leader is a lease written to the apiserver
///     every few seconds for nothing.
async fn configure_envoy_gateway() -> Result<()> {
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
    let Some(configured) = envoy_gateway_config(&String::from_utf8_lossy(&out.stdout))? else {
        return Ok(());
    };
    // `kubectl patch --type merge` with the full data key replaces just
    // that field.
    let patch = serde_json::json!({ "data": { "envoy-gateway.yaml": configured } }).to_string();
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
        anyhow::bail!("failed to patch envoy-gateway-config");
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
            "envoy-gateway controller restart failed; its config was patched but not \
             reloaded, so live caller connections would not route"
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
            "envoy-gateway controller did not become ready after its config reload; \
             live caller connections would not route"
        );
    }
    Ok(())
}

/// The controller config `current` with what weft needs set (see
/// [`configure_envoy_gateway`]), or `None` when it already has all of
/// it. Everything else in the document is kept as the controller wrote
/// it. A document of the wrong shape is refused rather than rewritten.
fn envoy_gateway_config(current: &str) -> Result<Option<String>> {
    use serde_yaml::{Mapping, Value};
    let mut doc: Value = serde_yaml::from_str(current)
        .map_err(|e| anyhow::anyhow!("envoy-gateway-config does not hold a YAML document: {e}"))?;
    fn section<'a>(parent: &'a mut Value, key: &str) -> Result<&'a mut Value> {
        let map = parent
            .as_mapping_mut()
            .ok_or_else(|| anyhow::anyhow!("envoy-gateway-config: expected a map above `{key}`"))?;
        let entry = map.entry(Value::from(key)).or_insert_with(|| Value::Mapping(Mapping::new()));
        anyhow::ensure!(entry.is_mapping(), "envoy-gateway-config: `{key}` is not a map");
        Ok(entry)
    }
    let before = doc.clone();
    let apis = section(&mut doc, "extensionApis")?;
    for flag in ["enableBackend", "enableEnvoyPatchPolicy"] {
        apis.as_mapping_mut().expect("checked above").insert(Value::from(flag), Value::from(true));
    }
    let provider = section(&mut doc, "provider")?;
    let kubernetes = section(provider, "kubernetes")?;
    let election = section(kubernetes, "leaderElection")?;
    election.as_mapping_mut().expect("checked above").insert(Value::from("disable"), Value::from(true));
    if doc == before {
        return Ok(None);
    }
    Ok(Some(serde_yaml::to_string(&doc)?))
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

/// Block until the dispatcher's /health answers at `base` from this
/// machine, bailing after 30s with what to look at. The pod was proved
/// Ready inside the cluster before this runs, so a failure here is the
/// path in: on kind, the node's port mapping or the NodePort Service;
/// on a real cluster, the address the CLI was told to use.
async fn wait_for_dispatcher_health(base: &str) -> Result<()> {
    // A per-request timeout well under the deadline: without one, a
    // hop that ACCEPTS the connection and never answers would hang
    // `send()` forever and the 30s deadline, checked only between
    // attempts, would never fire.
    let client = reqwest::Client::builder().timeout(Duration::from_secs(3)).build()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let url = format!("{base}/health");
    loop {
        if std::time::Instant::now() >= deadline {
            let cfg = cluster_config();
            match cfg.backend {
                ClusterBackend::Kind => anyhow::bail!(
                    "{url} did not become reachable within 30s, although the dispatcher pod \
                     is Ready. The path in is the node's port mapping and the NodePort \
                     Service; inspect them:\n  \
                     docker port {}-control-plane\n  \
                     kubectl --context {} -n {} get svc weft-dispatcher-node-port -o wide",
                    cfg.cluster_name,
                    cfg.kube_context,
                    cfg.system_namespace
                ),
                ClusterBackend::K8s => anyhow::bail!(
                    "{url} did not become reachable within 30s, although the dispatcher pod \
                     is Ready. On the k8s backend the CLI reaches the dispatcher at \
                     --dispatcher / WEFT_DISPATCHER_URL, which must name the address the \
                     cluster's ingress serves it at (WEFT_DISPATCHER_PUBLIC_BASE_URL)."
                ),
            }
        }
        if let Ok(r) = client.get(&url).send().await {
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
pub(crate) fn kubectl(args: &[&str]) -> Command {
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
    let manifest = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| anyhow::anyhow!("read {}: {e}", path.display()))?;
    substitute_placeholders(manifest, vars, path)
}

/// The substitution itself, on text already read; `path` only names
/// the file in the error.
fn substitute_placeholders(mut manifest: String, vars: &[(&str, String)], path: &Path) -> Result<String> {
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
async fn prepare_postgres_apply(cfg: &ClusterConfig, manifest: &Path) -> Result<()> {
    // Only the default install keeps its database on this machine; a
    // named install's starts empty inside the node every time.
    if cfg.instance.name().is_none() {
        guard_postgres_data_major(manifest).await?;
    }
    rebind_released_postgres_volume(&postgres_volume(&cfg.instance)).await
}

/// The name of an install's Postgres PersistentVolume (cluster-wide,
/// so one per install).
// SYNC: the volume name <-> deploy/k8s/postgres.yaml (WEFT_POSTGRES_VOLUME)
fn postgres_volume(instance: &weft_core::infra::Instance) -> String {
    instance.cluster_object("weft-postgres-data")
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
async fn rebind_released_postgres_volume(volume: &str) -> Result<()> {
    // `--ignore-not-found` exits 0 with EMPTY output for a missing
    // volume (the apply below creates it), so absence never has to be
    // told apart from a real failure (API server down, a wrong kube
    // context) by string-matching stderr; every real failure bails.
    let out = kubectl(&[
        "get", "pv", volume, "--ignore-not-found", "-o",
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
            "patch", "pv", volume, "--type=merge", "-p",
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

/// The shared-credentials file (OAuth apps and the runtime's own
/// provider keys), packed into the `weft-access-apps` Secret the broker
/// mounts. `WEFT_ACCESS_APPS_FILE` (the shell, or the `.env` the CLI
/// loaded) names it; the default is `access-apps.json` at the root of
/// the weft checkout the daemon was installed from, never the working
/// directory (a `weft daemon start` from a project folder once found
/// nothing there and wiped every shared key). Returns whether the
/// secret's content changed since the last apply. The kubelet's
/// in-place mount refresh only works for a secret that already existed
/// when the pod started; a secret created AFTER the pod started never
/// mounts into the running pod, so a change here must roll the broker.
///
/// A file that IS named but unreadable is loud: silently shipping no
/// apps would surface later as a confusing "no app configured" on a
/// node the operator thought was set up. And an absent default file
/// never empties a secret that holds keys: the cluster keeps what it
/// has and says so; `--clear-access-apps` is the one way to empty it.
async fn apply_access_apps_secret(
    cfg: &ClusterConfig,
    repo_root: &Path,
    clear: bool,
    pending: &mut PendingStamps,
) -> Result<bool> {
    let explicit = std::env::var(weft_core::access::spec::APPS_FILE_ENV)
        .ok()
        .filter(|p| !p.trim().is_empty());
    let path = access_apps_file(explicit.as_deref(), repo_root);
    let contents = match std::fs::read_to_string(&path) {
        Ok(c) => Some(c),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && explicit.is_none() => None,
        Err(e) => {
            return Err(anyhow::Error::new(e)).with_context(|| {
                format!("read access apps file '{}' (from WEFT_ACCESS_APPS_FILE)", path.display())
            })
        }
    };
    let contents = match access_apps_plan(contents, access_apps_secret_holds_keys(cfg).await?, clear) {
        AccessAppsPlan::Apply(contents) => contents,
        AccessAppsPlan::Keep => {
            println!(
                "shared-credentials file {} is absent; the cluster keeps the keys it holds \
                 (pass --clear-access-apps to remove them)",
                path.display()
            );
            return Ok(false);
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

/// Where the shared-credentials file is read from: the explicit
/// `WEFT_ACCESS_APPS_FILE` when set, else `access-apps.json` at the
/// root of the weft checkout.
fn access_apps_file(explicit: Option<&str>, repo_root: &Path) -> PathBuf {
    match explicit {
        Some(path) => PathBuf::from(path),
        None => repo_root.join(ACCESS_APPS_SECRET_KEY),
    }
}

/// What one boot does with the shared-credentials secret.
#[derive(Debug, PartialEq, Eq)]
enum AccessAppsPlan {
    /// Write this content (possibly empty) into the secret.
    Apply(String),
    /// The file is absent and the cluster holds keys: leave them.
    Keep,
}

/// The decision: a present file is applied as it is; an absent one
/// empties the secret only when it holds nothing or `clear` asks.
fn access_apps_plan(file: Option<String>, secret_holds_keys: bool, clear: bool) -> AccessAppsPlan {
    match file {
        Some(contents) => AccessAppsPlan::Apply(contents),
        None if secret_holds_keys && !clear => AccessAppsPlan::Keep,
        None => AccessAppsPlan::Apply(String::new()),
    }
}

/// Whether the cluster's `weft-access-apps` secret currently holds
/// anything. A missing secret (a fresh cluster) holds nothing; a
/// kubectl failure for any other reason is loud, since deciding
/// "empty" on it is exactly the wipe this guards against.
async fn access_apps_secret_holds_keys(cfg: &ClusterConfig) -> Result<bool> {
    let out = kubectl(&[
        "get", "secret", "weft-access-apps", "-n", &cfg.db_namespace,
        "-o", &format!("jsonpath={{.data.{}}}", ACCESS_APPS_SECRET_KEY.replace('.', "\\.")),
    ])
    .output()
    .await
    .context("kubectl get secret weft-access-apps")?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("NotFound") || stderr.contains("not found") {
            return Ok(false);
        }
        anyhow::bail!("kubectl get secret weft-access-apps failed: {}", stderr.trim());
    }
    let encoded = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if encoded.is_empty() {
        return Ok(false);
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.as_bytes())
        .context("decode the weft-access-apps secret")?;
    Ok(!String::from_utf8_lossy(&decoded).trim().is_empty())
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
    let stem = path
        .file_name()
        .map(|s| s.to_string_lossy().replace(['/', ':'], "_"))
        .unwrap_or_else(|| "manifest".into());
    install_stamp_dir().join(format!("{stem}.hash"))
}

/// The directory every stamp of THIS install on THIS cluster lives in:
/// the cluster's own for the default install (where every existing
/// stamp already is), a subdirectory per named install, since two
/// installs render the same file differently and each must answer "did
/// this change since MY last apply". `weft daemon remove` deletes a
/// named install's whole.
fn install_stamp_dir() -> PathBuf {
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
    let cluster = data_dir()
        .join("manifest-stamps")
        .join(format!("{sanitized}-{}", &digest[..8]));
    match cluster_config().instance.name() {
        None => cluster,
        Some(name) => cluster.join("installs").join(name),
    }
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{public_url_unreachable, PUBLIC_DOOR_URL, 
        access_apps_file, access_apps_plan, kind_cluster_config, substitute_placeholders,
        AccessAppsPlan, ClusterBackend, ClusterConfig, MappedPort,
    };

    fn kind_config_with_ports(dispatcher: u16, local: u16, gateway: u16) -> ClusterConfig {
        ClusterConfig {
            cluster_name: "weft-test".into(),
            kube_context: "kind-weft-test".into(),
            instance: weft_core::infra::Instance::default_install(),
            time_scale: 1.0,
            system_namespace: "weft-system".into(),
            db_namespace: "weft-db".into(),
            dispatcher_port: dispatcher,
            local_port: local,
            gateway_port: gateway,
            seaweed_port: 9096,
            service_cidr: "10.96.0.0/12".into(),
            pod_cidr: "10.244.0.0/16".into(),
            backend: ClusterBackend::Kind,
        }
    }

    /// The kind config maps each front door's node port to the
    /// configured loopback port, on loopback only, and nothing else:
    /// the old 80/443 host-port squat is gone, and a changed port
    /// changes the config (so the fingerprint rebuilds the node).
    #[test]
    fn kind_config_maps_every_front_door_to_its_loopback_port_and_nothing_else() {
        let cfg = kind_config_with_ports(19999, 19998, 19097);
        let config = kind_cluster_config(&cfg);
        for (m, host_port) in [
            (MappedPort::Dispatcher, 19999),
            (MappedPort::Local, 19998),
            (MappedPort::Gateway, 19097),
        ] {
            let mapping = format!(
                "      - containerPort: {}\n        hostPort: {host_port}\n        listenAddress: \"127.0.0.1\"\n        protocol: TCP\n",
                m.node_port()
            );
            assert!(config.contains(&mapping), "missing mapping for {m:?} in:\n{config}");
        }
        // ...plus the door band, published at the same number on both
        // sides. The whole band is mapped when the node is BUILT,
        // because a kind node's mappings cannot change afterwards and
        // a door opened next week has to land somewhere without
        // rebuilding the cluster (which destroys every project's
        // database).
        // Every other port in the range is published at the same number
        // on both sides: the apiserver allocates a door's port out of
        // this range, so one it could pick and the machine does not
        // publish would answer nothing.
        for port in weft_core::infra::NODE_PORTS {
            if MappedPort::ALL.iter().any(|m| m.node_port() == port) {
                continue; // a front door, checked above at its own host port
            }
            let mapping = format!(
                "      - containerPort: {port}\n        hostPort: {port}\n        listenAddress: \"127.0.0.1\"\n        protocol: TCP\n"
            );
            assert!(config.contains(&mapping), "node port {port} is not published in:\n{config}");
        }
        assert_eq!(
            config.matches("hostPort:").count(),
            weft_core::infra::NODE_PORTS.count(),
            "every node port the apiserver may allocate, published once:\n{config}"
        );
        // The apiserver is told to allocate only inside the published
        // range, so there is no port it could hand out that this
        // machine cannot reach.
        assert!(
            config.contains(&format!(
                "service-node-port-range: \"{}-{}\"",
                weft_core::infra::NODE_PORTS.start(),
                weft_core::infra::NODE_PORTS.end()
            )),
            "{config}"
        );
        // Every one of them is loopback: a door must never answer to
        // another machine on the network, and kind's default listen
        // address is 0.0.0.0.
        assert_eq!(
            config.matches("listenAddress: \"127.0.0.1\"").count(),
            config.matches("hostPort:").count()
        );
        assert!(!config.contains("hostPort: 80\n") && !config.contains("hostPort: 443\n"));
        assert_ne!(config, kind_cluster_config(&kind_config_with_ports(9999, 19998, 19097)));

        // No key twice at the node's own level. kind refuses the whole
        // config on a duplicate ("mapping key already defined") and the
        // cluster is ALREADY DELETED by then, so the machine is left
        // with no runtime at all. Adding a second `kubeadmConfigPatches`
        // rather than another item under the first one did exactly that.
        let mut node_keys: Vec<&str> = config
            .lines()
            .filter_map(|line| line.strip_prefix("    "))
            .filter(|rest| !rest.starts_with(' ') && !rest.starts_with('-'))
            .filter_map(|rest| rest.split(':').next())
            .collect();
        let before = node_keys.len();
        node_keys.sort_unstable();
        node_keys.dedup();
        assert_eq!(before, node_keys.len(), "a key is defined twice on the node:\n{config}");
    }

    /// Every pinned node port sits in the lower band of the default
    /// node-port range that the apiserver reserves for explicit
    /// requests (the first 128 of 30000-32767), so a Service created
    /// before ours (the one Envoy Gateway generates) never gets one
    /// of them from a dynamic allocation.
    #[test]
    fn node_ports_sit_in_the_static_band_and_are_distinct() {
        let ports: Vec<u16> = MappedPort::ALL.iter().map(|m| m.node_port()).collect();
        for &p in &ports {
            assert!((30000..30128).contains(&p), "{p} is outside the static band");
        }
        let mut unique = ports.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), ports.len(), "node ports collide: {ports:?}");
    }

    /// The shipped node-port manifest renders every front door's
    /// Service from the same numbers the kind config maps, one
    /// Service per namespace the front doors live in.
    #[test]
    fn kind_node_ports_manifest_pins_every_mapped_node_port() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/k8s/kind-node-ports.yaml");
        let text = std::fs::read_to_string(&path).expect("manifest readable");
        let vars: Vec<(&str, String)> = MappedPort::ALL
            .iter()
            .map(|&m| (m.template_var(), m.node_port().to_string()))
            .collect();
        let rendered = substitute_placeholders(text, &vars, &path).expect("renders");
        for m in MappedPort::ALL {
            let line = format!("nodePort: {}\n", m.node_port());
            assert_eq!(rendered.matches(&line).count(), 1, "{m:?} pinned once:\n{rendered}");
        }
        assert_eq!(rendered.matches("type: NodePort").count(), 3);
        for ns in ["namespace: weft-system", "namespace: envoy-gateway-system"] {
            assert!(rendered.contains(ns), "missing {ns}");
        }
    }

    /// Render a shipped manifest with an install's own vars, every other
    /// placeholder filled with a stand-in: what tells installs apart is
    /// all this looks at.
    fn render_for_install(file: &str, instance: &weft_core::infra::Instance) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/k8s").join(file);
        let mut text = std::fs::read_to_string(&path).expect("manifest readable");
        for (key, value) in super::install_template_vars(instance, 0.05) {
            text = text.replace(&format!("${{{key}}}"), &value);
        }
        while let Some(start) = text.find("${") {
            let end = start + text[start..].find('}').expect("closed placeholder");
            text.replace_range(start..=end, "stand-in");
        }
        text
    }

    const INSTALL_MANIFESTS: [&str; 6] = [
        "system-namespace.yaml",
        "db-namespace.yaml",
        "postgres.yaml",
        "broker.yaml",
        "dispatcher.yaml",
        "instance-door.yaml",
    ];

    #[test]
    fn a_named_install_renders_every_manifest_into_names_of_its_own() {
        let cell = weft_core::infra::Instance::named("cell3").unwrap();
        for file in INSTALL_MANIFESTS {
            let yaml = render_for_install(file, &cell);
            for line in yaml.lines().filter(|l| !l.trim_start().starts_with('#')) {
                for default in ["weft-system", "weft-db"] {
                    assert!(
                        !line.trim_end().ends_with(&format!(": {default}")) && !line.contains(&format!(".{default}.")),
                        "{file} still names the default install's {default}: {line}"
                    );
                }
            }
        }
        let postgres = render_for_install("postgres.yaml", &cell);
        assert!(postgres.contains("name: weft-postgres-data-cell3"));
        assert!(postgres.contains("path: /var/weft-installs/cell3/postgres"));
        let dispatcher = render_for_install("dispatcher.yaml", &cell);
        assert!(dispatcher.contains("name: weft-dispatcher-cell3"), "its own ClusterRoleBinding");
        assert!(dispatcher.contains("weft-broker.weft-cell3-db.svc"));
        assert!(dispatcher.contains("value: \"cell3\""), "WEFT_INSTANCE rides the pod");
        assert!(dispatcher.contains("value: \"0.05\""), "WEFT_TIME_SCALE rides the pod");
    }

    #[test]
    fn the_default_install_renders_the_names_every_cluster_already_has() {
        let default = weft_core::infra::Instance::default_install();
        let vars: std::collections::BTreeMap<_, _> =
            super::install_template_vars(&default, 1.0).into_iter().collect();
        assert_eq!(vars["WEFT_SYSTEM_NAMESPACE"], "weft-system");
        assert_eq!(vars["WEFT_DB_NAMESPACE"], "weft-db");
        assert_eq!(vars["WEFT_INSTANCE"], "");
        assert_eq!(vars["WEFT_INSTANCE_SUFFIX"], "");
        assert_eq!(vars["WEFT_TIME_SCALE"], "1");
        assert_eq!(vars["WEFT_POSTGRES_VOLUME"], "weft-postgres-data");
        assert_eq!(vars["WEFT_POSTGRES_NODE_PATH"], "/var/weft-postgres");
        // The exact bytes the credentials Secret carried before installs
        // were named, so an existing cluster sees no change.
        assert_eq!(
            vars["WEFT_DATABASE_URL_BASE64"],
            "cG9zdGdyZXM6Ly93ZWZ0OndlZnQtbG9jYWwtZGV2QHdlZnQtcG9zdGdyZXMud2VmdC1kYi5zdmMuY2x1c3Rlci5sb2NhbDo1NDMyL3dlZnQ="
        );
        assert_eq!(super::object_store_bucket(&default), "weft");
    }

    #[test]
    fn removing_a_named_install_takes_its_namespaces_and_nobody_elses() {
        let cell = weft_core::infra::Instance::named("cell3").unwrap();
        let listing = "default weft-system weft-db wft-shared-workers wft-project-local--abc \
                       weft-cell3-system weft-cell3-db wft-cell3-shared-workers \
                       wft-cell3-project-local--abc weft-cell30-system wft-cell30-shared-workers";
        assert_eq!(
            super::install_namespaces(&cell, listing),
            vec![
                "weft-cell3-system",
                "weft-cell3-db",
                "wft-cell3-shared-workers",
                "wft-cell3-project-local--abc",
            ]
        );
    }

    /// The controller config as a fresh v1.8 install writes it: the
    /// extension APIs off, a leader elected.
    const FRESH_ENVOY_GATEWAY_CONFIG: &str = "apiVersion: gateway.envoyproxy.io/v1alpha1
kind: EnvoyGateway
extensionApis: {}
gateway:
  controllerName: gateway.envoyproxy.io/gatewayclass-controller
provider:
  kubernetes:
    shutdownManager:
      image: envoyproxy/gateway:v1.8.1
  type: Kubernetes
";

    #[test]
    fn the_gateway_controller_gets_its_extension_apis_and_no_leader_election() {
        let configured = super::envoy_gateway_config(FRESH_ENVOY_GATEWAY_CONFIG).unwrap().expect("changed");
        let doc: serde_yaml::Value = serde_yaml::from_str(&configured).unwrap();
        assert_eq!(doc["extensionApis"]["enableBackend"], serde_yaml::Value::from(true));
        assert_eq!(doc["extensionApis"]["enableEnvoyPatchPolicy"], serde_yaml::Value::from(true));
        assert_eq!(doc["provider"]["kubernetes"]["leaderElection"]["disable"], serde_yaml::Value::from(true));
        // What the controller wrote is kept.
        assert_eq!(doc["provider"]["type"], serde_yaml::Value::from("Kubernetes"));
        assert_eq!(
            doc["provider"]["kubernetes"]["shutdownManager"]["image"],
            serde_yaml::Value::from("envoyproxy/gateway:v1.8.1")
        );
        // Once configured, nothing to do: no patch, no controller restart.
        assert_eq!(super::envoy_gateway_config(&configured).unwrap(), None);
    }

    #[test]
    fn leader_election_is_turned_off_once_and_only_where_kubeadm_put_it() {
        let manifest = "spec:\n  containers:\n  - command:\n    - kube-scheduler\n    - --leader-elect=true\n    image: x\n";
        let off = super::without_leader_election(manifest).unwrap().expect("changed");
        assert!(off.contains("    - --leader-elect=false\n"));
        assert!(!off.contains("--leader-elect=true"));
        assert_eq!(off.replace("false", "true"), manifest, "nothing else moved");
        assert_eq!(super::without_leader_election(&off).unwrap(), None, "already off: left alone");
        assert!(super::without_leader_election("spec: {}\n").is_err(), "no flag: not edited blind");
    }

    #[test]
    fn a_gateway_controller_config_of_the_wrong_shape_is_refused() {
        assert!(super::envoy_gateway_config("extensionApis: [1]\n").is_err());
        assert!(super::envoy_gateway_config("provider: plain\n").is_err());
    }

    /// Every document of a shipped manifest, parsed.
    fn manifest_documents(name: &str) -> Vec<serde_yaml::Value> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/k8s").join(name);
        let text = std::fs::read_to_string(&path).expect("manifest readable");
        text.split("\n---")
            .map(|doc| serde_yaml::from_str::<serde_yaml::Value>(doc).expect("each document parses"))
            .filter(|doc| !doc.is_null())
            .collect()
    }

    /// The public trigger surface is an allowlist, and this is its
    /// contract: the `public` listener carries exactly these routes and
    /// nothing else attaches to it, so any other path is Envoy's 404
    /// without the dispatcher ever seeing it.
    #[test]
    fn the_public_door_lets_through_exactly_the_allowlist() {
        let docs = manifest_documents("gateway.yaml");
        let gateway = docs.iter().find(|d| d["kind"] == "Gateway").expect("the Gateway");
        let public = gateway["spec"]["listeners"]
            .as_sequence()
            .unwrap()
            .iter()
            .find(|l| l["name"] == "public")
            .expect("a public listener");
        assert_eq!(public["port"], serde_yaml::Value::from(8080));
        assert!(public["hostname"].is_null());
        assert_eq!(public["allowedRoutes"]["namespaces"]["from"], serde_yaml::Value::from("Same"));

        // Every route that could land on the public listener: one naming
        // it, or one naming the Gateway without a listener (which would
        // attach to all of them).
        let routes: Vec<&serde_yaml::Value> = docs
            .iter()
            .filter(|d| d["kind"] == "HTTPRoute")
            .filter(|d| {
                d["spec"]["parentRefs"].as_sequence().unwrap().iter().any(|p| {
                    p["name"] == "weft-live-gateway"
                        && (p["sectionName"] == "public" || p["sectionName"].is_null())
                })
            })
            .collect();
        assert_eq!(routes.len(), 1, "only the public door attaches to the public listener");
        let door = routes[0];
        assert_eq!(door["metadata"]["name"], serde_yaml::Value::from("weft-public-door"));

        let mut allowed: Vec<(String, String, Option<String>, Option<String>)> = Vec::new();
        for rule in door["spec"]["rules"].as_sequence().unwrap() {
            let forwards_https = rule["filters"].as_sequence().unwrap().iter().any(|f| {
                f["type"] == "RequestHeaderModifier"
                    && f["requestHeaderModifier"]["set"].as_sequence().unwrap().iter().any(|h| {
                        h["name"] == "X-Forwarded-Proto" && h["value"] == "https"
                    })
            });
            let rewrite = rule["filters"]
                .as_sequence()
                .unwrap()
                .iter()
                .find(|f| f["type"] == "URLRewrite")
                .map(|f| f["urlRewrite"]["path"]["replaceFullPath"].as_str().unwrap().to_string());
            assert!(forwards_https || rewrite.is_some(), "a forwarded rule says https: {rule:?}");
            for backend in rule["backendRefs"].as_sequence().unwrap() {
                assert_eq!(backend["name"], serde_yaml::Value::from("weft-dispatcher"));
            }
            for m in rule["matches"].as_sequence().unwrap() {
                allowed.push((
                    m["path"]["type"].as_str().unwrap().to_string(),
                    m["path"]["value"].as_str().unwrap().to_string(),
                    m["method"].as_str().map(str::to_string),
                    rewrite.clone(),
                ));
            }
        }
        let expected = |kind: &str, path: &str, method: Option<&str>, rewrite: Option<&str>| {
            (kind.to_string(), path.to_string(), method.map(str::to_string), rewrite.map(str::to_string))
        };
        allowed.sort();
        let mut want = vec![
            expected("PathPrefix", "/events/", Some("POST"), None),
            expected("PathPrefix", "/signal/", None, None),
            expected("PathPrefix", "/signal-token/", None, None),
            expected("PathPrefix", "/public/files/", None, None),
            expected("Exact", "/access/oauth/callback", None, None),
            expected("Exact", "/", None, Some("/public-page/index.html")),
            expected("Exact", "/index.html", None, Some("/public-page/index.html")),
            expected("Exact", "/logo.png", None, Some("/public-page/logo.png")),
        ];
        want.sort();
        assert_eq!(allowed, want);

        // The allowlist is prefixes, so the path it matches must be the
        // path the dispatcher gets: an encoded slash is refused, and
        // repeated slashes are merged before the match.
        let path_policy = docs
            .iter()
            .find(|d| {
                d["kind"] == "ClientTrafficPolicy"
                    && d["spec"]["targetRefs"].as_sequence().is_some_and(|refs| {
                        refs.iter().any(|r| {
                            r["name"] == "weft-live-gateway" && r["sectionName"] == "public"
                        })
                    })
                    && !d["spec"]["path"].is_null()
            })
            .expect("a ClientTrafficPolicy on the public listener pinning path handling");
        assert_eq!(
            path_policy["spec"]["path"]["escapedSlashesAction"],
            serde_yaml::Value::from("RejectRequest")
        );
        let merge = &path_policy["spec"]["path"]["disableMergeSlashes"];
        assert!(
            merge.is_null() || merge.as_bool() == Some(false),
            "repeated slashes must be merged before the match: {merge:?}"
        );
    }

    /// Only the tunnel reaches the public listener: the door's Service is
    /// shipped with the tunnel, and no node port maps it to the machine.
    #[test]
    fn a_named_tunnel_that_misses_the_door_names_the_dashboard_setting() {
        let named = public_url_unreachable("https://w.example", true, "it answered 502 Bad Gateway");
        assert!(named.contains(PUBLIC_DOOR_URL), "{named}");
        assert!(named.contains("502"), "{named}");
        let quick = public_url_unreachable("https://x.trycloudflare.com", false, "it answered 502");
        assert!(!quick.contains("dashboard"), "{quick}");
        let tunnel = manifest_documents("public-tunnel.yaml");
        let door = tunnel.iter().find(|d| d["metadata"]["name"] == "weft-public-door").unwrap();
        assert_eq!(
            PUBLIC_DOOR_URL,
            format!(
                "http://{}.{}.svc.cluster.local:{}",
                door["metadata"]["name"].as_str().unwrap(),
                door["metadata"]["namespace"].as_str().unwrap(),
                door["spec"]["ports"][0]["port"].as_u64().unwrap()
            )
        );
    }

    #[test]
    fn nothing_but_the_tunnel_reaches_the_public_door() {
        let ports = manifest_documents("kind-node-ports.yaml");
        for service in &ports {
            for port in service["spec"]["ports"].as_sequence().unwrap() {
                assert_ne!(port["targetPort"], serde_yaml::Value::from(8080), "{service:?}");
            }
        }
        let tunnel = manifest_documents("public-tunnel.yaml");
        assert!(tunnel.iter().any(|d| d["kind"] == "Service" && d["metadata"]["name"] == "weft-public-door"));
    }

    #[test]
    fn the_shared_credentials_file_is_the_checkouts_unless_named() {
        let root = Path::new("/srv/weft");
        assert_eq!(access_apps_file(None, root), PathBuf::from("/srv/weft/access-apps.json"));
        assert_eq!(access_apps_file(Some("/etc/apps.json"), root), PathBuf::from("/etc/apps.json"));
    }

    #[test]
    fn an_absent_file_never_empties_a_secret_that_holds_keys() {
        let present = || Some("{\"apps\": []}".to_string());
        assert_eq!(access_apps_plan(present(), true, false), AccessAppsPlan::Apply(present().unwrap()));
        assert_eq!(access_apps_plan(present(), false, true), AccessAppsPlan::Apply(present().unwrap()));
        assert_eq!(access_apps_plan(None, true, false), AccessAppsPlan::Keep);
        assert_eq!(access_apps_plan(None, true, true), AccessAppsPlan::Apply(String::new()));
        assert_eq!(access_apps_plan(None, false, false), AccessAppsPlan::Apply(String::new()));
    }

    #[test]
    fn dispatcher_recovery_only_replaces_crashing_owned_old_images() {
        let pod = serde_json::json!({
            "metadata": { "ownerReferences": [{ "uid": "owner", "controller": true }] },
            "spec": { "containers": [{ "name": "dispatcher", "image": "old" }] },
            "status": {
                "conditions": [{ "type": "Ready", "status": "False" }],
                "containerStatuses": [{ "name": "dispatcher", "state": { "waiting": { "reason": "CrashLoopBackOff" } } }]
            }
        });
        assert!(super::dispatcher_needs_replacement(&pod, "owner", "new"));
        assert!(!super::dispatcher_needs_replacement(&pod, "other", "new"));
        assert!(!super::dispatcher_needs_replacement(&pod, "owner", "old"));
        let mut healthy = pod.clone();
        healthy["status"]["conditions"][0]["status"] = serde_json::json!("True");
        assert!(!super::dispatcher_needs_replacement(&healthy, "owner", "new"));
        let mut starting = pod.clone();
        starting["status"]["containerStatuses"][0]["state"] = serde_json::json!({ "running": {} });
        assert!(!super::dispatcher_needs_replacement(&starting, "owner", "new"));
        let mut unpullable = pod.clone();
        unpullable["status"]["containerStatuses"][0]["state"] = serde_json::json!({ "waiting": { "reason": "ImagePullBackOff" } });
        assert!(super::dispatcher_needs_replacement(&unpullable, "owner", "new"));
        let mut creating = pod.clone();
        creating["status"]["containerStatuses"][0]["state"] = serde_json::json!({ "waiting": { "reason": "ContainerCreating" } });
        assert!(!super::dispatcher_needs_replacement(&creating, "owner", "new"));
        let mut deleting = pod.clone();
        deleting["metadata"]["deletionTimestamp"] = serde_json::json!("2026-09-13T12:00:00Z");
        assert!(!super::dispatcher_needs_replacement(&deleting, "owner", "new"));
        let mut sidecar = pod;
        sidecar["spec"]["containers"][0]["name"] = serde_json::json!("sidecar");
        sidecar["status"]["containerStatuses"][0]["name"] = serde_json::json!("sidecar");
        assert!(!super::dispatcher_needs_replacement(&sidecar, "owner", "new"));
    }
    use super::{
        apiserver_endpoint_from_slices, apiserver_peers_yaml, canonical_tunnel_hostname,
        check_cidr, endpoint_host, parse_pooled_listing, rolls_for,
        split_yaml_documents, yaml_document_kind, CreatedThisBoot, DetectedChanges, EndpointHost,
    };

    /// The roll decision, pinned as an explicit table (never a
    /// restatement of the formula) so editing `rolls_for` cannot
    /// silently keep this green. Inputs are (postgres_manifest,
    /// other_manifests, sealing_key, apps, rebuilt); expectations are
    /// (dispatcher, broker). Every single-trigger row plus the
    /// historically dangerous combinations: a postgres change rolls
    /// BOTH pods (its credentials Secret is env at pod start on each,
    /// invisible to their specs); other manifests (the dispatcher's
    /// own included: its apply rolls it) roll nothing here; the
    /// sealing key rolls both; apps rolls the broker alone; a forced
    /// rebuild rolls both.
    #[test]
    fn each_roll_trigger_fires_exactly_where_it_must() {
        type Inputs = (bool, bool, bool, bool, bool);
        type Expected = (bool, bool);
        #[rustfmt::skip]
        let table: &[(Inputs, Expected)] = &[
            // nothing changed
            ((false, false, false, false, false), (false, false)),
            // postgres.yaml only: both pods (credentials env)
            ((true,  false, false, false, false), (true,  true)),
            // other manifests only: applied, nothing rolls
            ((false, true,  false, false, false), (false, false)),
            // sealing key only
            ((false, false, true,  false, false), (true,  true)),
            // apps only: broker WITHOUT dispatcher
            ((false, false, false, true,  false), (false, true)),
            // rebuilt only
            ((false, false, false, false, true),  (true,  true)),
            // combinations
            ((false, true,  false, true,  false), (false, true)),
            ((true,  false, false, true,  false), (true,  true)),
            ((false, false, true,  true,  false), (true,  true)),
            ((true,  true,  true,  true,  true),  (true,  true)),
        ];
        for &(inputs, want) in table {
            let (postgres_manifest, other_manifests, sealing_key, apps, rebuilt) = inputs;
            let rolls = rolls_for(
                &DetectedChanges { postgres_manifest, other_manifests, sealing_key, apps },
                rebuilt,
                &CreatedThisBoot { dispatcher: false, broker: false },
            );
            assert_eq!((rolls.dispatcher, rolls.broker), want, "inputs: {inputs:?}");
        }
    }

    /// A workload this boot created started on everything the boot put
    /// in place, so even with every trigger up it is not restarted; the
    /// one that already existed still is.
    #[test]
    fn a_workload_created_this_boot_is_never_rolled() {
        let everything = DetectedChanges { postgres_manifest: true, other_manifests: true, sealing_key: true, apps: true };
        let rolls = rolls_for(&everything, true, &CreatedThisBoot { dispatcher: true, broker: true });
        assert_eq!((rolls.dispatcher, rolls.broker), (false, false));
        let rolls = rolls_for(&everything, true, &CreatedThisBoot { dispatcher: true, broker: false });
        assert_eq!((rolls.dispatcher, rolls.broker), (false, true));
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
    fn apiserver_endpoint_is_read_from_the_kubernetes_endpoint_slice() {
        // The shape `kubectl get endpointslices -o json` returns for
        // the `kubernetes` Service on kind: the node's address and the
        // apiserver's real port, which is what a packet carries once
        // kube-proxy has rewritten the ClusterIP.
        let listing = serde_json::json!({"items": [{
            "endpoints": [{"addresses": ["172.19.0.2"], "conditions": {"ready": true}}],
            "ports": [{"name": "https", "port": 6443, "protocol": "TCP"}]
        }]});
        let endpoint = apiserver_endpoint_from_slices(&listing).unwrap();
        assert_eq!(endpoint.addresses, vec!["172.19.0.2".to_string()]);
        assert_eq!(endpoint.port, 6443);
        // A control plane with several apiservers lists every one.
        let listing = serde_json::json!({"items": [{
            "endpoints": [{"addresses": ["10.0.0.1"]}, {"addresses": ["10.0.0.2"]}],
            "ports": [{"port": 443}]
        }]});
        assert_eq!(apiserver_endpoint_from_slices(&listing).unwrap().addresses.len(), 2);
        // No address or no port is a refusal, never a rule that admits nothing.
        assert!(apiserver_endpoint_from_slices(&serde_json::json!({"items": []})).is_err());
    }

    /// The rendered `to:` entries sit at the manifests' indentation
    /// and end on a newline, so the `ports:` line that follows the
    /// placeholder stays a line of its own.
    #[test]
    fn apiserver_peers_render_one_ip_block_per_range() {
        let yaml = apiserver_peers_yaml(&["172.19.0.0/16".to_string(), "10.0.0.2/32".to_string()]);
        assert_eq!(
            yaml,
            "        - ipBlock:\n            cidr: 172.19.0.0/16\n\
             \x20       - ipBlock:\n            cidr: 10.0.0.2/32\n"
        );
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

