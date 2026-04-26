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

/// Cluster / namespace / image config the CLI talks to. Local
/// development defaults to a kind cluster named `weft-local` in
/// namespace `wm-local`; overrides via env var let the same
/// CLI target a managed cluster without recompiling.
pub struct ClusterConfig {
    pub cluster_name: String,
    pub kube_context: String,
    pub namespace: String,
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
        let namespace = std::env::var("WEFT_NAMESPACE")
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
            namespace,
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
        DaemonAction::Restart { rebuild } => {
            stop().await?;
            sleep(Duration::from_millis(400)).await;
            start(&ctx, rebuild).await
        }
        DaemonAction::Logs { tail, follow } => logs(tail, follow).await,
    }
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
    ensure_namespace().await?;

    images::ensure_dispatcher_image(&cfg.dispatcher_image, rebuild).await?;
    images::ensure_listener_image(&cfg.listener_image, rebuild).await?;
    if cfg.backend == ClusterBackend::Kind {
        images::kind_load(&cfg.cluster_name, &cfg.dispatcher_image).await?;
        images::kind_load(&cfg.cluster_name, &cfg.listener_image).await?;
    }

    let repo_root = images::repo_root()?;
    let manifests = repo_root.join("deploy/k8s");
    kubectl_apply_file(&manifests.join("namespace.yaml")).await?;
    kubectl_apply_file(&manifests.join("dispatcher.yaml")).await?;
    kubectl_apply_file(&manifests.join("ingress.yaml")).await?;

    wait_for_deployment_ready("weft-dispatcher").await?;
    start_port_forward().await?;
    wait_for_http(&format!("http://127.0.0.1:{}/health", cfg.dispatcher_port)).await?;

    let _ = ctx;
    let backend = match cfg.backend {
        ClusterBackend::Kind => "kind",
        ClusterBackend::K8s => "k8s",
    };
    println!(
        "daemon ready at http://127.0.0.1:{} ({} cluster '{}', namespace '{}')",
        cfg.dispatcher_port, backend, cfg.cluster_name, cfg.namespace,
    );
    Ok(())
}

async fn stop() -> Result<()> {
    let cfg = cluster_config();
    let pf = pid_file_path();
    if let Some(pid) = read_pid(&pf) {
        let _ = signal_term(pid);
        let _ = fs::remove_file(&pf);
    }
    let _ = kubectl(&[
        "-n", &cfg.namespace, "scale", "deployment/weft-dispatcher", "--replicas=0",
    ])
    .status()
    .await;
    println!("daemon stopped");
    Ok(())
}

async fn status(ctx: &Ctx) -> Result<()> {
    let cfg = cluster_config();
    let pf_alive = read_pid(&pid_file_path()).map(process_alive).unwrap_or(false);
    match ctx.client().get_json("/projects").await {
        Ok(v) => {
            let n = v.as_array().map(|a| a.len()).unwrap_or(0);
            println!(
                "daemon: running (cluster '{}', namespace '{}', port-forward {}); {} project(s)",
                cfg.cluster_name,
                cfg.namespace,
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
        "-n", &cfg.namespace, "logs", "deployment/weft-dispatcher", &tail_arg,
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
    let wait = kubectl(&[
        "wait",
        "--namespace",
        "ingress-nginx",
        "--for=condition=ready",
        "pod",
        "--selector=app.kubernetes.io/component=controller",
        "--timeout=180s",
    ])
    .status()
    .await?;
    if !wait.success() {
        anyhow::bail!("ingress controller failed to become ready");
    }
    Ok(())
}

async fn ensure_namespace() -> Result<()> {
    let repo_root = images::repo_root()?;
    let manifest = repo_root.join("deploy/k8s/namespace.yaml");
    kubectl_apply_file(&manifest).await
}

async fn wait_for_deployment_ready(name: &str) -> Result<()> {
    let cfg = cluster_config();
    let status = kubectl(&[
        "-n", &cfg.namespace,
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
            "-n", &cfg.namespace,
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
