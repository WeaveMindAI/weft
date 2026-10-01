//! The agent beside every infra unit (`weft-runtime unit-agent`).
//!
//! `serve` holds the unit's network and answers weft's readiness and
//! liveness checks from inside it (`weft_platform_traits::unit_agent`).
//! `own` hands a unit's disks to its group before its containers start
//! (a unit's `fsGroup`). `host` runs on a cloud machine given one unit: it
//! runs the unit on the machine's Docker (the laptop's own code,
//! `weft_platform_local::LocalInfraHost`) and answers weft about it.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context as _;
use axum::routing::post;
use axum::{Json, Router};
use weft_core::infra::ProbeKind;
use weft_platform_traits::unit_agent::{ProbeAnswer, ProbeRequest, PROBE_PATH};
use weft_core::ports::UNIT_AGENT;

/// Serve checks until the container is stopped.
pub async fn serve() -> anyhow::Result<()> {
    let app = Router::new().route(PROBE_PATH, post(probe));
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", UNIT_AGENT))
        .await
        .with_context(|| format!("bind the unit agent on port {UNIT_AGENT}"))?;
    axum::serve(listener, app).await.context("serve the unit agent")
}

async fn probe(Json(req): Json<ProbeRequest>) -> Json<ProbeAnswer> {
    let timeout = Duration::from_secs(u64::from(req.timeout_seconds.max(1)));
    Json(run_probe(&req.probe, timeout).await)
}

/// Run one network check against the unit's own `127.0.0.1`.
pub async fn run_probe(probe: &ProbeKind, timeout: Duration) -> ProbeAnswer {
    let failed = |why: String| ProbeAnswer { ok: false, why: Some(why) };
    match probe {
        ProbeKind::Tcp { port } => match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(("127.0.0.1", *port))).await {
            Ok(Ok(_)) => ProbeAnswer { ok: true, why: None },
            Ok(Err(e)) => failed(format!("port {port}: {e}")),
            Err(_) => failed(format!("port {port}: no answer within {}s", timeout.as_secs())),
        },
        ProbeKind::Http { path, port } => {
            let client = reqwest::Client::builder()
                .timeout(timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("reqwest client");
            match client.get(format!("http://127.0.0.1:{port}{path}")).send().await {
                Ok(r) if r.status().is_success() || r.status().is_redirection() => ProbeAnswer { ok: true, why: None },
                Ok(r) => failed(format!("GET {path} on {port} answered {}", r.status().as_u16())),
                Err(e) => failed(format!("GET {path} on {port}: {e}")),
            }
        }
        ProbeKind::Exec { .. } => failed("an exec check runs inside its own container, never through the agent".into()),
    }
}

/// Give every file under `paths` to group `gid`, readable and writable by
/// it (directories searchable), the way a unit's `fsGroup` asks.
pub fn own(gid: u32, paths: &[PathBuf]) -> anyhow::Result<()> {
    for path in paths {
        own_tree(gid, path).with_context(|| format!("hand {} to group {gid}", path.display()))?;
    }
    Ok(())
}

#[cfg(unix)]
fn own_tree(gid: u32, path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::symlink_metadata(path)?;
    std::os::unix::fs::lchown(path, None, Some(gid))?;
    if meta.file_type().is_symlink() {
        return Ok(());
    }
    let mode = meta.permissions().mode();
    // Group read and write, and search on directories (what `g+rwX` does).
    let wanted = mode | 0o060 | if meta.is_dir() || mode & 0o100 != 0 { 0o010 } else { 0 } | if meta.is_dir() { 0o2000 } else { 0 };
    if wanted != mode {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(wanted))?;
    }
    if meta.is_dir() {
        for entry in std::fs::read_dir(path)? {
            own_tree(gid, &entry?.path())?;
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn own_tree(_gid: u32, _path: &Path) -> anyhow::Result<()> {
    anyhow::bail!("the unit agent runs in a Linux container")
}

// ---------- the host agent ----------

const METADATA: &str = "http://metadata.google.internal/computeMetadata/v1/instance";

/// Where the host agent keeps its Docker login and scratch files.
const HOST_STATE: &str = "/var/lib/weft";

async fn metadata(path: &str) -> anyhow::Result<String> {
    let resp = reqwest::Client::new()
        .get(format!("{METADATA}/{path}"))
        .header("Metadata-Flavor", "Google")
        .send()
        .await?
        .error_for_status()
        .with_context(|| format!("read {path} from the machine's metadata"))?;
    Ok(resp.text().await?)
}

async fn attribute(key: &str) -> anyhow::Result<String> {
    metadata(&format!("attributes/{key}")).await
}

#[derive(Clone)]
struct Host {
    local: std::sync::Arc<weft_platform_local::LocalInfraHost>,
    tokens: std::sync::Arc<weft_platform_gcp::MetadataTokens>,
    registry: String,
    /// One apply at a time.
    applying: std::sync::Arc<tokio::sync::Mutex<()>>,
}

impl Host {
    async fn assignment() -> anyhow::Result<weft_platform_traits::unit_agent::UnitAssignment> {
        let raw = attribute(weft_platform_gcp::infra_host::MD_UNIT).await?;
        serde_json::from_str(&raw).context("the machine's unit is not valid")
    }

    /// Log the machine's Docker into the install's registry with a fresh
    /// token, so the unit's own images pull.
    async fn login(&self) -> anyhow::Result<()> {
        use base64::Engine as _;
        let token = self.tokens.access_token().await?;
        let dir = Path::new(HOST_STATE).join("docker");
        std::fs::create_dir_all(&dir)?;
        let auth = base64::engine::general_purpose::STANDARD.encode(format!("oauth2accesstoken:{token}"));
        let config = serde_json::json!({ "auths": { self.registry.clone(): { "auth": auth } } });
        std::fs::write(dir.join("config.json"), serde_json::to_vec(&config)?)?;
        Ok(())
    }

    async fn apply(&self) -> anyhow::Result<()> {
        let _one = self.applying.lock().await;
        self.login().await?;
        let a = Self::assignment().await?;
        use weft_platform_traits::InfraHost as _;
        self.local.apply_unit(&a.node, &a.unit).await
    }
}

/// Run the machine's unit and answer weft about it.
pub async fn host() -> anyhow::Result<()> {
    use weft_platform_gcp::infra_host::{MD_CORE_ACCOUNT, MD_GCP_PROJECT, MD_GPU, MD_RUNTIME_IMAGE};
    use weft_platform_local::{DiskBacking, GpuAccess, LocalInfraHostConfig, Publish};
    use weft_platform_traits::unit_agent::{HOST_APPLY, HOST_LOGS, HOST_OBSERVE, HOST_RESTART};

    // The Docker CLI this process runs reads its login from here.
    std::env::set_var("DOCKER_CONFIG", Path::new(HOST_STATE).join("docker"));
    let image = attribute(MD_RUNTIME_IMAGE).await?;
    let registry = image.split('/').next().unwrap_or_default().to_string();
    let gpu = if attribute(MD_GPU).await? == "yes" { GpuAccess::CosDriver } else { GpuAccess::None };
    let local = std::sync::Arc::new(weft_platform_local::LocalInfraHost::new(
        std::sync::Arc::new(weft_platform_local::DockerCli),
        LocalInfraHostConfig {
            agent_image: image,
            scratch_dir: Path::new(HOST_STATE).join("run"),
            gpu,
            disks: DiskBacking::MountedUnder("/mnt/disks".into()),
            publish: Publish::AllPorts,
            // A machine runs one unit of one install.
            install: weft_core::infra::Install::default_install(),
        },
    ));
    let host = Host {
        local,
        tokens: std::sync::Arc::new(weft_platform_gcp::MetadataTokens::new()),
        registry,
        applying: std::sync::Arc::default(),
    };
    let ip = metadata("network-interfaces/0/ip").await?;
    let door = crate::guard::CoreOnly {
        identity: std::sync::Arc::new(weft_platform_gcp::GoogleIdentity::new(
            attribute(MD_GCP_PROJECT).await?,
            attribute(MD_CORE_ACCOUNT).await?,
            None,
        )),
        audiences: std::sync::Arc::new(vec![format!("http://{}:{UNIT_AGENT}", ip.trim())]),
    };

    // The unit comes up with the machine; a failure is reported on the
    // next look (`observe`) and the next apply tries again.
    if let Err(e) = host.apply().await {
        tracing::error!(target: "weft_runtime::unit_agent", error = %format!("{e:#}"), "could not bring the unit up");
    }

    let err = |e: anyhow::Error| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"));
    let app = Router::new()
        .route(HOST_APPLY, post({
            let host = host.clone();
            move || async move { host.apply().await.map_err(err) }
        }))
        .route(HOST_RESTART, post({
            let host = host.clone();
            move || async move {
                use weft_platform_traits::InfraHost as _;
                let a = Host::assignment().await.map_err(err)?;
                host.local.restart_unit(&a.node.node, &a.unit).await.map_err(err)
            }
        }))
        .route(HOST_OBSERVE, axum::routing::get({
            let host = host.clone();
            move || async move {
                use weft_platform_traits::InfraHost as _;
                let a = Host::assignment().await.map_err(err)?;
                host.local.observe(&a.node.node.tenant, a.node.node.project).await.map(Json).map_err(err)
            }
        }))
        .route(HOST_LOGS, axum::routing::get({
            let host = host.clone();
            move |axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>| async move {
                use weft_platform_traits::InfraHost as _;
                let from = q.get("from").ok_or_else(|| err(anyhow::anyhow!("the logs query names no `from`")))?;
                let from: weft_core::infra::wire::LogsFrom =
                    serde_json::from_str(from).map_err(|e| err(anyhow::anyhow!("the logs query's `from`: {e}")))?;
                let a = Host::assignment().await.map_err(err)?;
                host.local.logs(&a.node.node, &a.unit, &from).await.map(Json).map_err(err)
            }
        }));
    let app = door.guard(app);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", UNIT_AGENT)).await.context("bind the host agent")?;
    axum::serve(listener, app).await.context("serve the host agent")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_tcp_check_passes_on_a_listening_port_and_fails_on_a_closed_one() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(run_probe(&ProbeKind::Tcp { port }, Duration::from_secs(1)).await.ok);
        drop(listener);
        let answer = run_probe(&ProbeKind::Tcp { port }, Duration::from_secs(1)).await;
        assert!(!answer.ok);
        assert!(answer.why.unwrap().contains(&port.to_string()));
    }

    #[tokio::test]
    async fn an_http_check_reads_the_status() {
        let app = Router::new()
            .route("/ok", axum::routing::get(|| async { "fine" }))
            .route("/down", axum::routing::get(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        assert!(run_probe(&ProbeKind::Http { path: "/ok".into(), port }, Duration::from_secs(2)).await.ok);
        let down = run_probe(&ProbeKind::Http { path: "/down".into(), port }, Duration::from_secs(2)).await;
        assert_eq!(down.why.as_deref(), Some(format!("GET /down on {port} answered 503").as_str()));
    }

    #[cfg(unix)]
    #[test]
    fn own_makes_a_tree_group_writable() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let dir = std::env::temp_dir().join(format!("weft-own-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub/f"), b"x").unwrap();
        std::fs::set_permissions(dir.join("sub/f"), std::fs::Permissions::from_mode(0o600)).unwrap();
        // The test's own group: changing to it needs no privilege.
        let gid = std::fs::metadata(&dir).unwrap().gid();
        own(gid, std::slice::from_ref(&dir)).unwrap();
        let f = std::fs::metadata(dir.join("sub/f")).unwrap();
        assert_eq!(f.permissions().mode() & 0o070, 0o060);
        assert_eq!(std::fs::metadata(dir.join("sub")).unwrap().permissions().mode() & 0o070, 0o070);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
