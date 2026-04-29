//! `weft daemon start|stop|status|restart|logs`. Owns the kind
//! cluster lifecycle and the dispatcher deployment inside it.
//!
//! Local dev and cloud deploy share this shape: dispatcher runs as
//! a Pod, listener as a Pod, worker as a Pod, sidecars as Pods. The
//! only difference is that `start` locally uses `kind` to host the
//! cluster and `kind load docker-image` to fill the image cache
//! without a registry push. In cloud the same manifests get applied
//! to a managed cluster and images come from a real registry.

use std::fs;
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
/// `default_user_namespace` (where workers, listeners, sidecars
/// for tenant `local` run). Cloud adds more user namespaces, one
/// per tenant; OSS sticks to a single one.
pub struct ClusterConfig {
    pub cluster_name: String,
    pub kube_context: String,
    pub system_namespace: String,
    pub default_user_namespace: String,
    pub dispatcher_image: String,
    pub listener_image: String,
    pub dispatcher_port: u16,
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
        let default_user_namespace = std::env::var("WEFT_DEFAULT_USER_NAMESPACE")
            .unwrap_or_else(|_| "wm-local".into());
        let dispatcher_image = std::env::var("WEFT_DISPATCHER_IMAGE")
            .unwrap_or_else(|_| "weft-dispatcher:local".into());
        let listener_image = std::env::var("WEFT_LISTENER_IMAGE")
            .unwrap_or_else(|_| "weft-listener:local".into());
        let dispatcher_port = std::env::var("WEFT_DISPATCHER_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(9999);
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
            default_user_namespace,
            dispatcher_image,
            listener_image,
            dispatcher_port,
            backend,
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
/// dropped WebSockets, no port-forward rebuild.
async fn restart(ctx: &Ctx, rebuild: bool) -> Result<()> {
    let cfg = cluster_config();
    require_binary("kubectl").await?;
    require_binary("docker").await?;

    let dispatcher_built =
        images::ensure_dispatcher_image(&cfg.dispatcher_image, rebuild).await?;
    let listener_built = images::ensure_listener_image(&cfg.listener_image, rebuild).await?;
    if dispatcher_built || listener_built {
        if cfg.backend == ClusterBackend::Kind {
            images::kind_load(&cfg.cluster_name, &cfg.dispatcher_image).await?;
            images::kind_load(&cfg.cluster_name, &cfg.listener_image).await?;
        }
        // Roll the dispatcher pod so it picks up the new image. The
        // port-forward is bound to a single Pod IP, so a Pod
        // recreate kills it; we refresh it after the rollout.
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
        kill_existing_port_forward();
        start_port_forward().await?;
        wait_for_http(&format!("http://127.0.0.1:{}/health", cfg.dispatcher_port)).await?;
        // Also roll every per-tenant listener Deployment when the
        // listener image rebuilt. The dispatcher creates these
        // dynamically (named `listener-<tenant>`) and never
        // restarts them on its own, so without this they stay on
        // the old image, breaking the dispatcher↔listener wire
        // contract whenever the FormField / WakeSignalKind types
        // shift between releases.
        if listener_built {
            roll_listener_deployments(cfg).await?;
        }
        println!("daemon refreshed; new image rolled out");
    } else {
        println!("daemon already running with the latest images; nothing to do");
    }
    let _ = ctx;
    Ok(())
}

pub fn data_dir() -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_default();
    home.join(".local/share/weft")
}

pub fn pid_file_path() -> PathBuf {
    data_dir().join("port-forward.pid")
}

pub fn log_file_path() -> PathBuf {
    data_dir().join("port-forward.log")
}

async fn start(ctx: &Ctx, rebuild: bool) -> Result<()> {
    let cfg = cluster_config();
    require_binary("kubectl").await?;
    require_binary("docker").await?;
    if cfg.backend == ClusterBackend::Kind {
        require_binary("kind").await?;
        ensure_cluster(cfg).await?;
        ensure_ingress_controller().await?;
    }

    images::ensure_dispatcher_image(&cfg.dispatcher_image, rebuild).await?;
    images::ensure_listener_image(&cfg.listener_image, rebuild).await?;
    if cfg.backend == ClusterBackend::Kind {
        images::kind_load(&cfg.cluster_name, &cfg.dispatcher_image).await?;
        images::kind_load(&cfg.cluster_name, &cfg.listener_image).await?;
    }

    let repo_root = images::repo_root()?;
    let manifests = repo_root.join("deploy/k8s");
    kubectl_apply_file(&manifests.join("system-namespace.yaml")).await?;
    kubectl_apply_file(&manifests.join("user-namespace.yaml")).await?;
    kubectl_apply_file(&manifests.join("postgres.yaml")).await?;
    wait_for_deployment_ready("weft-postgres").await?;
    ensure_internal_secret().await?;
    kubectl_apply_file(&manifests.join("dispatcher.yaml")).await?;
    kubectl_apply_file(&manifests.join("ingress.yaml")).await?;

    wait_for_statefulset_ready("weft-dispatcher").await?;
    start_port_forward().await?;
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
    kill_existing_port_forward();
    let _ = kubectl(&[
        "-n", &cfg.system_namespace, "scale", "statefulset/weft-dispatcher", "--replicas=0",
    ])
    .status()
    .await;
    println!("daemon stopped");
    Ok(())
}

/// Kill any running `kubectl port-forward` we previously spawned
/// for the dispatcher Service. Called on stop and before we
/// re-establish a port-forward after a Pod rollout. Idempotent.
fn kill_existing_port_forward() {
    let pf = pid_file_path();
    if let Some(pid) = read_pid(&pf) {
        let _ = signal_term(pid);
        let _ = fs::remove_file(&pf);
    }
}

async fn status(ctx: &Ctx) -> Result<()> {
    let cfg = cluster_config();
    let pf_alive = read_pid(&pid_file_path()).map(process_alive).unwrap_or(false);
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

/// Create `weft-internal-secret` with a random value if it doesn't
/// exist. Idempotent: subsequent runs see the existing Secret and
/// no-op. Not in dispatcher.yaml because `kubectl apply` would
/// reset the secret to a placeholder on every re-apply.
async fn ensure_internal_secret() -> Result<()> {
    let cfg = cluster_config();
    let out = kubectl(&[
        "-n",
        &cfg.system_namespace,
        "get",
        "secret",
        "weft-internal-secret",
        "-o",
        "name",
    ])
    .output()
    .await?;
    if out.status.success() && !out.stdout.is_empty() {
        return Ok(());
    }
    let fresh = uuid::Uuid::new_v4().simple().to_string();
    let status = kubectl(&[
        "-n",
        &cfg.system_namespace,
        "create",
        "secret",
        "generic",
        "weft-internal-secret",
        &format!("--from-literal=WEFT_INTERNAL_SECRET={fresh}"),
    ])
    .status()
    .await?;
    if !status.success() {
        anyhow::bail!("create internal secret failed");
    }
    Ok(())
}

async fn wait_for_deployment_ready(name: &str) -> Result<()> {
    let cfg = cluster_config();
    let status = kubectl(&[
        "-n", &cfg.system_namespace,
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
    let listeners: Vec<&str> = names
        .split_whitespace()
        .filter(|n| n.starts_with("listener-"))
        .collect();
    for name in &listeners {
        let status = kubectl(&[
            "-n",
            &cfg.default_user_namespace,
            "rollout",
            "restart",
            &format!("deployment/{name}"),
        ])
        .status()
        .await?;
        if !status.success() {
            tracing::warn!(
                target: "weft_cli::daemon",
                "rollout restart deployment/{name} failed"
            );
            continue;
        }
        // Block briefly on each rollout so subsequent register
        // calls hit the new Pod, not the old one mid-termination.
        let wait = kubectl(&[
            "-n",
            &cfg.default_user_namespace,
            "rollout",
            "status",
            &format!("deployment/{name}"),
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
    }
    if !listeners.is_empty() {
        println!("rolled {} listener deployment(s)", listeners.len());
    }
    Ok(())
}

async fn start_port_forward() -> Result<()> {
    let cfg = cluster_config();
    fs::create_dir_all(data_dir())?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file_path())?;
    let err = log.try_clone()?;
    let child = std::process::Command::new("kubectl")
        .args([
            "--context", &cfg.kube_context,
            "-n", &cfg.system_namespace,
            "port-forward", "svc/weft-dispatcher",
            &format!("{}:9999", cfg.dispatcher_port),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .context("spawn kubectl port-forward")?;
    fs::write(pid_file_path(), child.id().to_string())?;
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
    let status = kubectl(&["apply", "-f"]).arg(path).status().await?;
    if !status.success() {
        anyhow::bail!("kubectl apply -f {} failed", path.display());
    }
    Ok(())
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
