//! `weft daemon start|stop|status|restart|logs`. Owns the kind
//! cluster lifecycle and the dispatcher deployment inside it.
//!
//! Local dev and cloud deploy share this shape: dispatcher runs as
//! a Pod, listener as a Pod, worker as a Pod, infra as Pods. The
//! only difference is that `start` locally uses `kind` to host the
//! cluster and `kind load docker-image` to fill the image cache
//! without a registry push. In cloud the same manifests get applied
//! to a managed cluster and images come from a real registry.

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
/// for tenant `local` run). Cloud adds more user namespaces, one
/// per tenant; OSS sticks to a single one.
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
    pub storage_image: String,
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
            .unwrap_or_else(|_| "wm-local".into());
        let dispatcher_image = std::env::var("WEFT_DISPATCHER_IMAGE")
            .unwrap_or_else(|_| "weft-dispatcher:local".into());
        let listener_image = std::env::var("WEFT_LISTENER_IMAGE")
            .unwrap_or_else(|_| "weft-listener:local".into());
        let broker_image = std::env::var("WEFT_BROKER_IMAGE")
            .unwrap_or_else(|_| "weft-broker:local".into());
        let supervisor_image = std::env::var("WEFT_SUPERVISOR_IMAGE")
            .unwrap_or_else(|_| "weft-infra-supervisor:local".into());
        let storage_image = std::env::var("WEFT_STORAGE_IMAGE")
            .unwrap_or_else(|_| "weft-storage:local".into());
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
        // Raw here; validated where they're consumed (the manifest
        // apply, `apply_static_manifests`), not at config load: a
        // malformed CIDR should fail the apply loudly, not break
        // unrelated commands (stop / logs / rm) that read this config
        // but never touch the CIDRs.
        let service_cidr = std::env::var("WEFT_CLUSTER_SERVICE_CIDR")
            .unwrap_or_else(|_| "10.96.0.0/12".into());
        let pod_cidr = std::env::var("WEFT_CLUSTER_POD_CIDR")
            .unwrap_or_else(|_| "10.244.0.0/16".into());
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
            storage_image,
            dispatcher_port,
            ingress_port,
            gateway_port,
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
/// - Kind (local dev): default to `http://127.0.0.1:<ingress_port>`
///   (the daemon forwards the cluster ingress there) and set
///   WEFT_LOCAL_DEV=1 so the dispatcher accepts the loopback host.
///   An operator may still override WEFT_DISPATCHER_PUBLIC_BASE_URL.
/// - K8s (real cluster): the operator MUST set
///   WEFT_DISPATCHER_PUBLIC_BASE_URL to the external ingress host;
///   WEFT_LOCAL_DEV is empty, so a loopback there fails loud.
fn manifest_template_vars(cfg: &ClusterConfig) -> Result<Vec<(&'static str, String)>> {
    check_cidr(&cfg.service_cidr, true)
        .map_err(|e| anyhow::anyhow!("WEFT_CLUSTER_SERVICE_CIDR='{}': {e}", cfg.service_cidr))?;
    check_cidr(&cfg.pod_cidr, false)
        .map_err(|e| anyhow::anyhow!("WEFT_CLUSTER_POD_CIDR='{}': {e}", cfg.pod_cidr))?;
    let apiserver_ip = apiserver_clusterip(&cfg.service_cidr)
        .map_err(|e| anyhow::anyhow!("WEFT_CLUSTER_SERVICE_CIDR='{}': {e}", cfg.service_cidr))?;

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

    Ok(vec![
        ("WEFT_CLUSTER_SERVICE_CIDR", cfg.service_cidr.clone()),
        ("WEFT_CLUSTER_POD_CIDR", cfg.pod_cidr.clone()),
        ("WEFT_APISERVER_CLUSTERIP", apiserver_ip),
        ("WEFT_DISPATCHER_PUBLIC_BASE_URL", public_base_url),
        ("WEFT_LOCAL_DEV", local_dev),
        ("GATEWAY_HOST", gateway_host),
        ("WEFT_GATEWAY_BASE_URL", gateway_base_url),
        ("WEFT_CALLER_TOKEN_SECRET", caller_token_secret),
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
    Start { rebuild: bool },
    Stop,
    Status,
    Restart { rebuild: bool },
    Logs { tail: usize, follow: bool },
}

pub async fn run(ctx: Ctx, action: DaemonAction) -> Result<()> {
    match action {
        DaemonAction::Start { rebuild } => start(&ctx, rebuild).await,
        DaemonAction::Stop => stop().await,
        DaemonAction::Status => status(&ctx).await,
        DaemonAction::Restart { rebuild } => restart(&ctx, rebuild).await,
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
        storage: storage_built,
    } = built;

    // The Envoy Gateway controller is infrastructure both `start` and
    // `restart` must guarantee BEFORE applying gateway.yaml (its CRs need
    // the CRDs). Idempotent: a no-op once installed. (A restart against a
    // cluster created before this feature existed installs it now.)
    if cfg.backend == ClusterBackend::Kind {
        ensure_envoy_gateway().await?;
    }

    // Re-apply k8s manifests on every restart. NetworkPolicy /
    // ClusterRole / SA-label tweaks land via the manifest files in
    // deploy/k8s; without re-applying them on restart, a manifest
    // change picked up only by a fresh `daemon start`. Apply is
    // idempotent: unchanged manifests are no-ops at the
    // kube-apiserver layer (resourceVersion match).
    let manifests_changed = apply_static_manifests(cfg).await?;

    if dispatcher_built || listener_built || broker_built || supervisor_built || storage_built || manifests_changed {
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
                images::kind_load_force(&cfg.cluster_name, &cfg.storage_image),
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
            if broker_built {
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
        // Storage boxes are per-tenant, dispatcher-created at runtime
        // (not static manifests), so the kind-load above doesn't reach
        // an already-running box pod; roll it explicitly like the
        // supervisor / listener.
        let storage_rollout = async {
            if storage_built {
                roll_role_deployments("storage", "storage").await?;
            }
            Ok::<(), anyhow::Error>(())
        };
        tokio::try_join!(
            listener_rollout,
            broker_rollout,
            supervisor_rollout,
            storage_rollout
        )?;
        println!("daemon refreshed; new image / manifests rolled out");
    } else {
        println!("daemon already running with the latest images and manifests; nothing to do");
    }
    let _ = ctx;
    Ok(())
}

/// Which of the four system images were actually rebuilt by a
/// provisioning pass. Drives the "anything to roll out?" decision.
struct BuiltImages {
    dispatcher: bool,
    listener: bool,
    broker: bool,
    supervisor: bool,
    storage: bool,
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
    let (dispatcher, listener, broker, supervisor, storage, ()) = tokio::join!(
        images::ensure_system_image(&cfg.dispatcher_image, "dispatcher.Dockerfile", &["catalog"], rebuild),
        images::ensure_system_image(&cfg.listener_image, "listener.Dockerfile", &[], rebuild),
        images::ensure_system_image(&cfg.broker_image, "broker.Dockerfile", &[], rebuild),
        images::ensure_system_image(&cfg.supervisor_image, "infra-supervisor.Dockerfile", &[], rebuild),
        images::ensure_system_image(&cfg.storage_image, "storage.Dockerfile", &[], rebuild),
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
        storage: unwrap("storage", storage),
    };
    if !failures.is_empty() {
        anyhow::bail!("system image build failed:\n  {}", failures.join("\n  "));
    }
    Ok(built)
}

/// Apply every static manifest in `deploy/k8s`. Returns true iff
/// `kubectl apply` reported a change (non-`unchanged` line) on any
/// manifest, signalling that a pod rollout is warranted.
async fn apply_static_manifests(cfg: &ClusterConfig) -> Result<bool> {
    let repo_root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let manifests = repo_root.join("deploy/k8s");
    // broker + dispatcher carry ${...} placeholders (CIDRs, and for
    // the dispatcher the public base URL + local-dev flag), all
    // substituted from `template_vars`; the others have no
    // placeholders so the same applier is a no-op substitution for
    // them. `cluster-rbac.yaml`: ClusterRoles for the per-tenant
    // supervisor + listener pods, bound into project namespaces by
    // RoleBindings the dispatcher creates at register time.
    // Cluster-scoped; in the rolling-apply list so RBAC drift (e.g.
    // the supervisor's surface growing) stays in sync.
    let template_vars = manifest_template_vars(cfg)?;
    let mut any_changed = false;
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
        // without placeholders (broker + dispatcher are the only ones).
        any_changed |= kubectl_apply_changed(&manifests.join(name), &template_vars).await?;
    }
    Ok(any_changed)
}

pub fn data_dir() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    home.join(".local/share/weft")
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
        images::kind_load_force(&cfg.cluster_name, &cfg.storage_image).await?;
    }

    let repo_root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    let manifests = repo_root.join("deploy/k8s");
    kubectl_apply_file(&manifests.join("system-namespace.yaml")).await?;
    kubectl_apply_file(&manifests.join("db-namespace.yaml")).await?;
    // No static `wm-local` namespace: the dispatcher renders the
    // per-tenant bundle (Namespace + SAs + NetworkPolicies + the
    // supervisor pod) on first project register.
    kubectl_apply_file(&manifests.join("postgres.yaml")).await?;
    wait_for_deployment_ready_in_ns("weft-postgres", &cfg.db_namespace).await?;
    // broker + dispatcher carry cluster-specific placeholders (the
    // broker's TokenReview-egress NetworkPolicy CIDRs, the
    // dispatcher's CIDRs + public base URL + local-dev flag);
    // substitute them so a non-kind operator sets them once via env
    // instead of hand-editing manifests.
    let template_vars = manifest_template_vars(cfg)?;
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

async fn status(ctx: &Ctx) -> Result<()> {
    let cfg = cluster_config();
    // Every forward the daemon ACTUALLY runs must be alive to report "up".
    // We iterate the real set (`port_forwards`), not the superset of names:
    // the gateway forward only exists once the Gateway is programmed, so it
    // is simply absent from the set until then and never drags liveness down,
    // while a required forward (dispatcher, ingress) that is missing its pid
    // correctly reports "down" (no fail-open `None => true`).
    let pf_alive = port_forwards(&cfg).await.iter().all(|pf| {
        read_pid(&data_dir_pid_file(pf.name))
            .map(process_alive)
            .unwrap_or(false)
    });
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
/// caller to a specific worker pod. Same controller local (kind) and
/// cloud; only the public host + TLS differ (set via the gateway
/// manifest's `${GATEWAY_HOST}`, applied by `apply_static_manifests`).
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
/// These deployments live one-per-tenant across `wm-*` namespaces and
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

/// The one `kubectl apply` path. Reads the manifest, substitutes
/// `${VAR}` placeholders from `vars` (empty for manifests with no
/// placeholders), pipes the result to `kubectl apply -f -`, and
/// reports whether any resource changed (`created`/`configured`, vs
/// the `unchanged` no-op `restart` uses to decide whether to roll the
/// dispatcher pod). Always fails loud on a leftover `${...}` so a
/// typo'd or unpassed placeholder can never apply literally to the
/// cluster, regardless of which manifest it's in.
async fn kubectl_apply_changed(path: &Path, vars: &[(&str, String)]) -> Result<bool> {
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
        anyhow::bail!("kubectl apply ({}) failed: {stderr}", path.display());
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    print!("{stdout}");
    Ok(apply_output_reported_change(&stdout))
}

/// True iff a `kubectl apply` stdout reports at least one changed
/// resource. Lines look like `networkpolicy.../foo created` /
/// `configured` / `unchanged`; only `unchanged` is a no-op.
fn apply_output_reported_change(stdout: &str) -> bool {
    stdout.lines().any(|l| {
        let trimmed = l.trim();
        !trimmed.is_empty() && !trimmed.ends_with(" unchanged")
    })
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
    use super::{apiserver_clusterip, check_cidr};

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
}
