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

const CLUSTER_NAME: &str = "weft-local";
const KUBE_CONTEXT: &str = "kind-weft-local";
const NAMESPACE: &str = "wm-local";
const DISPATCHER_IMAGE: &str = "weft-dispatcher:local";
const LISTENER_IMAGE: &str = "weft-listener:local";
const DISPATCHER_PORT: u16 = 9999;

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
    require_binary("kind").await?;
    require_binary("kubectl").await?;
    require_binary("docker").await?;

    ensure_cluster().await?;
    ensure_ingress_controller().await?;
    ensure_namespace().await?;

    images::ensure_dispatcher_image(DISPATCHER_IMAGE, rebuild).await?;
    images::ensure_listener_image(LISTENER_IMAGE, rebuild).await?;
    images::kind_load(CLUSTER_NAME, DISPATCHER_IMAGE).await?;
    images::kind_load(CLUSTER_NAME, LISTENER_IMAGE).await?;

    let repo_root = images::repo_root()?;
    let manifests = repo_root.join("deploy/k8s");
    kubectl_apply_file(&manifests.join("namespace.yaml")).await?;
    kubectl_apply_file(&manifests.join("dispatcher.yaml")).await?;
    kubectl_apply_file(&manifests.join("ingress.yaml")).await?;

    wait_for_deployment_ready("weft-dispatcher").await?;
    start_port_forward().await?;
    wait_for_http("http://127.0.0.1:9999/health").await?;

    let _ = ctx;
    println!(
        "daemon ready at http://127.0.0.1:{DISPATCHER_PORT} \
         (kind cluster '{CLUSTER_NAME}', namespace '{NAMESPACE}')"
    );
    Ok(())
}

async fn stop() -> Result<()> {
    let pf = pid_file_path();
    if let Some(pid) = read_pid(&pf) {
        let _ = signal_term(pid);
        let _ = fs::remove_file(&pf);
    }
    let _ = kubectl(&[
        "-n", NAMESPACE, "scale", "deployment/weft-dispatcher", "--replicas=0",
    ])
    .status()
    .await;
    println!("daemon stopped");
    Ok(())
}

async fn status(ctx: &Ctx) -> Result<()> {
    let pf_alive = read_pid(&pid_file_path()).map(process_alive).unwrap_or(false);
    match ctx.client().get_json("/projects").await {
        Ok(v) => {
            let n = v.as_array().map(|a| a.len()).unwrap_or(0);
            println!(
                "daemon: running (cluster '{CLUSTER_NAME}', namespace '{NAMESPACE}', port-forward {}); {} project(s)",
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
    let tail_arg = format!("--tail={tail}");
    let mut args: Vec<&str> = vec![
        "-n", NAMESPACE, "logs", "deployment/weft-dispatcher", &tail_arg,
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

async fn ensure_cluster() -> Result<()> {
    let out = Command::new("kind").args(["get", "clusters"]).output().await?;
    let list = String::from_utf8_lossy(&out.stdout);
    if list.lines().any(|n| n == CLUSTER_NAME) {
        return Ok(());
    }
    println!("creating kind cluster '{CLUSTER_NAME}' (first run)");
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
        .args(["create", "cluster", "--name", CLUSTER_NAME, "--config"])
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
    let status = kubectl(&[
        "-n", NAMESPACE,
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
    fs::create_dir_all(data_dir())?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file_path())?;
    let err = log.try_clone()?;
    let child = std::process::Command::new("kubectl")
        .args([
            "--context", KUBE_CONTEXT,
            "-n", NAMESPACE,
            "port-forward", "svc/weft-dispatcher",
            &format!("{DISPATCHER_PORT}:9999"),
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

/// Build a kubectl Command pinned to the kind context. Every
/// kubectl call in this module goes through this so the user's
/// current-context (which might be GKE) never interferes.
fn kubectl(args: &[&str]) -> Command {
    let mut cmd = Command::new("kubectl");
    cmd.arg("--context").arg(KUBE_CONTEXT);
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
