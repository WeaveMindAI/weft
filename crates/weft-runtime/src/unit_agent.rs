//! The agent beside every infra unit (`weft-runtime unit-agent`).
//!
//! `serve` holds the unit's network, answers weft's readiness and
//! liveness checks from inside it, and passes on to the broker what the
//! unit's containers say changed of their copy's values
//! (`weft_platform_traits::unit_agent`).
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
use weft_platform_traits::unit_agent::{ProbeAnswer, ProbeRequest, AGENT_BROKER_URL_ENV, AGENT_IDENTITY_ENV, PROBE_PATH, VALUES_PATH};
use weft_core::ports::UNIT_AGENT;

/// Serve checks, and pass on what the unit's containers push, until the
/// container is stopped.
pub async fn serve() -> anyhow::Result<()> {
    let broker_url = std::env::var(AGENT_BROKER_URL_ENV).with_context(|| format!("{AGENT_BROKER_URL_ENV} is required: where this agent reaches the broker"))?;
    let tokens = weft_platform_gcp::identity_from_env(AGENT_IDENTITY_ENV)?;
    let link = weft_broker_client::BrokerLink::new(broker_url, weft_broker_client::TokenSource::worker(tokens, weft_platform_traits::identity::mint_replica_id("unit-agent")));
    let broker = weft_broker_client::BrokerInfraClient::new(link);
    let app = Router::new().route(PROBE_PATH, post(probe)).route(VALUES_PATH, post(push)).with_state(broker);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", UNIT_AGENT))
        .await
        .with_context(|| format!("bind the unit agent on port {UNIT_AGENT}"))?;
    axum::serve(listener, app).await.context("serve the unit agent")
}

/// A container says what changed: passed on to the broker as this copy,
/// and answered with what the broker said, so the container knows whether
/// it landed.
async fn push(
    axum::extract::State(broker): axum::extract::State<std::sync::Arc<weft_broker_client::BrokerInfraClient>>,
    Json(values): Json<weft_core::infra::bake::PushedValues>,
) -> (axum::http::StatusCode, String) {
    match broker.push_values(&values).await {
        Ok(()) => (axum::http::StatusCode::OK, "{}".into()),
        Err(e) => match e.downcast_ref::<weft_broker_client::BrokerRefused>() {
            Some(refused) => (axum::http::StatusCode::from_u16(refused.status.as_u16()).unwrap_or(axum::http::StatusCode::BAD_GATEWAY), refused.body.clone()),
            None => (axum::http::StatusCode::BAD_GATEWAY, format!("the broker could not be reached: {e:#}")),
        },
    }
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
            let client = weft_core::net::http_client()
                .timeout(timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("reqwest client");
            match client.get(format!("http://127.0.0.1:{port}{path}")).send().await {
                Ok(r) if r.status().is_success() || r.status().is_redirection() => ProbeAnswer { ok: true, why: None },
                Ok(r) => {
                    let status = r.status().as_u16();
                    // The body rides the same client timeout; one that does
                    // not arrive is a check that said nothing.
                    let body = body_start(r).await;
                    match what_it_said(&body) {
                        Some(said) => failed(format!("GET {path} on {port} answered {status}: {said}")),
                        None => failed(format!("GET {path} on {port} answered {status}")),
                    }
                }
                Err(e) => failed(format!("GET {path} on {port}: {e}")),
            }
        }
        ProbeKind::Exec { .. } => failed("an exec check runs inside its own container, never through the agent".into()),
    }
}

/// The longest stretch of a failing check's body that its reason carries.
const SAID_MAX_CHARS: usize = 300;

/// How much of a failing check's body is read: enough for any JSON
/// `error` a container means to tell, never a whole page a misbehaving one
/// streams back.
const BODY_MAX_BYTES: usize = 4096;

/// The first [`BODY_MAX_BYTES`] of a response's body, read lossily as
/// text; what arrived before an error when the body breaks off.
async fn body_start(mut response: reqwest::Response) -> String {
    let mut body = Vec::new();
    while body.len() < BODY_MAX_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => body.extend_from_slice(&chunk[..chunk.len().min(BODY_MAX_BYTES - body.len())]),
            Ok(None) | Err(_) => break,
        }
    }
    String::from_utf8_lossy(&body).into_owned()
}

/// What a failing HTTP check's body says, so a container that knows why
/// it is not ready can tell it: the string `error` field of a JSON body,
/// else the body's text, trimmed and cut to [`SAID_MAX_CHARS`]. `None`
/// when it says nothing.
fn what_it_said(body: &str) -> Option<String> {
    let error = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("error").and_then(serde_json::Value::as_str).map(str::to_owned));
    let said = error.as_deref().unwrap_or(body).trim();
    if said.is_empty() {
        return None;
    }
    Some(said.chars().take(SAID_MAX_CHARS).collect())
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
    let resp = weft_core::net::http_client()
        .build()?
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
    /// Held for the whole of an apply, so one runs at a time and `observe`
    /// can tell one is running; holds why the last one failed, until one
    /// succeeds.
    applying: std::sync::Arc<tokio::sync::Mutex<Option<String>>>,
    /// Applies asked for that have not taken `applying` yet: a look counts
    /// them as running, so none falls between one apply and the next.
    queued: std::sync::Arc<std::sync::atomic::AtomicUsize>,
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

    /// Apply the machine's unit in a task of its own, one apply at a time.
    /// It counts as running from before this returns (`queued`, then
    /// `applying`), so a look right after already sees the unit starting
    /// rather than what ran before. A failure is the unit's state on every
    /// look until an apply succeeds, or until the unit runs whole as asked
    /// anyway (a failed boot apply over containers Docker brought back).
    fn apply_in_background(&self) {
        use std::sync::atomic::Ordering;
        self.queued.fetch_add(1, Ordering::SeqCst);
        let host = self.clone();
        tokio::spawn(async move {
            let mut last_failure = host.applying.clone().lock_owned().await;
            host.queued.fetch_sub(1, Ordering::SeqCst);
            let applied = async {
                host.login().await?;
                let a = Self::assignment().await?;
                use weft_platform_traits::InfraHost as _;
                host.local.apply_unit(&a.node, &a.unit).await
            };
            // A panic is a failure too, never one the next look forgets.
            let applied = match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(applied)).await {
                Ok(applied) => applied,
                Err(_) => Err(anyhow::anyhow!("the apply panicked; the agent's log has where")),
            };
            if let Err(e) = &applied {
                tracing::error!(target: "weft_runtime::unit_agent", error = %format!("{e:#}"), "could not bring the unit up");
            }
            *last_failure = applied.err().map(|e| format!("{e:#}"));
        });
    }
}

impl Host {
    /// How the machine's unit stands, as weft is told it (`observe`).
    async fn seen(&self) -> anyhow::Result<Vec<weft_platform_traits::UnitObservation>> {
        use weft_platform_traits::InfraHost as _;
        let a = Self::assignment().await?;
        let mut seen = self.local.observe(&a.node.node.tenant, a.node.node.project).await?;
        // While an apply runs, that is the unit's state, whatever
        // containers are there: one still running from before would read
        // as a ready unit that is not the one asked for. After one failed,
        // so is the failure, unless the unit runs whole as asked anyway
        // (Docker brought its containers back).
        let ours = |o: &weft_platform_traits::UnitObservation| o.copy_id == a.node.node.copy_id && o.unit == a.unit;
        let wanted = a.node.unit(&a.unit).map(|u| u.hash.clone()).unwrap_or_default();
        let queued = self.queued.load(std::sync::atomic::Ordering::SeqCst) > 0;
        let idle = self.applying.try_lock().ok().filter(|_| !queued).map(|last_failure| last_failure.clone());
        let state = match idle {
            Some(Some(_)) if self.local.runs_whole(&a.node, &a.unit).await? => None,
            Some(last_failure) => last_failure.map(|why| weft_platform_traits::UnitRunState::Failed { why }),
            None if !seen.iter().any(ours) => Some(weft_platform_traits::UnitRunState::Starting { step: "its images are downloading".into() }),
            None => Some(weft_platform_traits::UnitRunState::Starting { step: "its new version is starting".into() }),
        };
        if let Some(state) = state {
            // A restart of its containers stays what it was: the apply
            // says how the unit stands, not what happened to it before.
            let restart = seen.iter().find(|o| ours(o)).and_then(|o| o.restart.clone());
            seen.retain(|o| !ours(o));
            seen.push(weft_platform_traits::UnitObservation { copy_id: a.node.node.copy_id.clone(), unit: a.unit.clone(), hash: wanted, state, restart });
        }
        Ok(seen)
    }

    /// Watch how the unit stands, here on its machine where looking costs
    /// nothing, and ask for a look at the project's health each time it
    /// changes (a container stopped, a check started failing or passing
    /// again, an apply ended, a container came back on its own: that one
    /// can come and go between two looks, so it is told by when it
    /// stopped, which stays until the next one). The supervisor looks at
    /// infra that runs fine only when told to, so an install whose infra runs fine
    /// wakes nothing; the death of the whole machine is told by the cloud
    /// itself. The first look after the agent starts always asks. A
    /// request that fails is asked again at the next looks, [`ASK_TRIES`]
    /// times in all, and then not until the unit changes again: each try
    /// wakes the broker's database.
    async fn watch(self, broker: std::sync::Arc<weft_broker_client::BrokerInfraClient>) {
        let mut told: Option<Vec<Stand>> = None;
        let mut tries = 0;
        loop {
            match self.seen().await.map(|seen| Self::stands(&seen)) {
                Ok(stands) if told.as_ref() == Some(&stands) => {}
                Ok(stands) => {
                    let asked = async {
                        let a = Self::assignment().await?;
                        broker.ask_for_a_look(a.node.node.project).await
                    };
                    tries += 1;
                    match asked.await {
                        Ok(()) => {
                            told = Some(stands);
                            tries = 0;
                        }
                        Err(e) if tries >= ASK_TRIES => {
                            tracing::error!(target: "weft_runtime::unit_agent", error = %format!("{e:#}"), tries, "could not ask for a look at the unit; giving up until it changes again");
                            told = Some(stands);
                            tries = 0;
                        }
                        Err(e) => tracing::warn!(target: "weft_runtime::unit_agent", error = %format!("{e:#}"), "could not ask for a look at the unit; asking again at the next look"),
                    }
                }
                Err(e) => tracing::warn!(target: "weft_runtime::unit_agent", error = %format!("{e:#}"), "could not see how the unit stands"),
            }
            tokio::time::sleep(WATCH_EVERY).await;
        }
    }

    /// What a look compares: each unit, the kind of state it is in
    /// (never the words, which may change between looks of one state),
    /// and when its latest restart stopped it.
    fn stands(seen: &[weft_platform_traits::UnitObservation]) -> Vec<Stand> {
        let mut stands: Vec<Stand> = seen
            .iter()
            .map(|o| (o.copy_id.clone(), o.unit.clone(), o.state.kind(), o.restart.as_ref().map(|r| r.stopped_at_unix)))
            .collect();
        stands.sort();
        stands
    }
}

/// One unit as a look compares it ([`Host::stands`]): its copy, its
/// name, the kind of its state, and when its latest restart stopped it.
type Stand = (String, String, &'static str, Option<i64>);

/// How often the agent looks at its unit (`Host::watch`). Looking is a
/// call to the machine's own Docker and the unit's checks, and costs
/// nothing off the machine.
const WATCH_EVERY: std::time::Duration = std::time::Duration::from_secs(10);

/// How many looks in a row the agent asks for a look at one state of its
/// unit before it gives up on that state (`Host::watch`).
const ASK_TRIES: u32 = 3;

/// Run the machine's unit and answer weft about it.
pub async fn host() -> anyhow::Result<()> {
    use weft_platform_gcp::infra_host::{AGENT_IMAGE_ENV, MD_CORE_ACCOUNT, MD_GCP_PROJECT, MD_GPU};
    use weft_platform_local::{DiskBacking, GpuAccess, LocalInfraHostConfig, Publish};
    use weft_platform_traits::unit_agent::{HOST_APPLY, HOST_LOGS, HOST_OBSERVE};

    // The Docker CLI this process runs reads its login from here.
    std::env::set_var("DOCKER_CONFIG", Path::new(HOST_STATE).join("docker"));
    // The image this container was made from, as the startup script that
    // made it says; never the machine's metadata, which an apply rewrites
    // while this container keeps running (and Docker brings it back as it
    // was). The unit's own agent runs the same image.
    let image = std::env::var(AGENT_IMAGE_ENV)
        .with_context(|| format!("{AGENT_IMAGE_ENV} is required: the image this agent runs, which the machine's startup script sets"))?;
    let registry = image.split('/').next().unwrap_or_default().to_string();
    let gpu = if attribute(MD_GPU).await? == "yes" { GpuAccess::CosDriver } else { GpuAccess::None };
    // Where the unit's agent pushes its values and this machine asks for
    // looks. Every machine weft makes is told; one that was not is from a
    // weft too old to run here, and applying its node again replaces it.
    let broker_url = attribute(weft_platform_gcp::infra_host::MD_BROKER_URL)
        .await
        .context("this machine was not told where the broker is; apply its infra node again to replace it")?;
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
            // The machine's own identity names the copy it runs.
            agent_broker: weft_platform_local::AgentBroker {
                url: broker_url.clone(),
                identity: weft_platform_local::AgentIdentity::Machine,
            },
        },
    ));
    let host = Host {
        local,
        tokens: std::sync::Arc::new(weft_platform_gcp::MetadataTokens::new()),
        registry,
        applying: std::sync::Arc::default(),
        queued: std::sync::Arc::default(),
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

    // The unit comes up with the machine, in the background: the agent
    // answers from the start, so weft can say what the machine is doing
    // while its images download (`observe`).
    host.apply_in_background();

    // Asking for a look goes through the broker, as the copy this machine
    // runs (its token names the machine).
    let machine = metadata("name").await?;
    let link = weft_broker_client::BrokerLink::new(
        broker_url,
        weft_broker_client::TokenSource::worker(host.tokens.clone(), format!("infra-{}", machine.trim())),
    );
    tokio::spawn(host.clone().watch(weft_broker_client::BrokerInfraClient::new(link)));

    let err = |e: anyhow::Error| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"));
    let app = Router::new()
        // Answered at once: pulling a unit's images takes longer than any
        // one call to the agent may, and a call cut short would drop the
        // apply half done. How it goes is read off `observe`.
        .route(HOST_APPLY, post({
            let host = host.clone();
            move || async move {
                host.apply_in_background();
                axum::http::StatusCode::ACCEPTED
            }
        }))
        .route(HOST_OBSERVE, axum::routing::get({
            let host = host.clone();
            move || async move { host.seen().await.map(Json).map_err(err) }
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
            .route("/down", axum::routing::get(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }))
            .route(
                "/broken",
                axum::routing::get(|| async { (axum::http::StatusCode::SERVICE_UNAVAILABLE, r#"{"error": "no extension"}"#) }),
            )
            .route("/long", axum::routing::get(|| async { (axum::http::StatusCode::SERVICE_UNAVAILABLE, "x".repeat(1 << 20)) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        assert!(run_probe(&ProbeKind::Http { path: "/ok".into(), port }, Duration::from_secs(2)).await.ok);
        let down = run_probe(&ProbeKind::Http { path: "/down".into(), port }, Duration::from_secs(2)).await;
        assert_eq!(down.why.as_deref(), Some(format!("GET /down on {port} answered 503").as_str()));
        let broken = run_probe(&ProbeKind::Http { path: "/broken".into(), port }, Duration::from_secs(2)).await;
        assert_eq!(broken.why.as_deref(), Some(format!("GET /broken on {port} answered 503: no extension").as_str()));
        let long = run_probe(&ProbeKind::Http { path: "/long".into(), port }, Duration::from_secs(2)).await;
        assert_eq!(long.why, Some(format!("GET /long on {port} answered 503: {}", "x".repeat(SAID_MAX_CHARS))));
    }

    /// A failing check's body is told by its JSON `error`, else by its
    /// trimmed text, cut on a character boundary, and not at all when
    /// it is empty.
    #[test]
    fn a_failing_check_tells_what_its_body_said() {
        assert_eq!(
            what_it_said(r#"{"error": "the database could not create the extension 'foo'", "ready": false}"#).as_deref(),
            Some("the database could not create the extension 'foo'")
        );
        assert_eq!(what_it_said("  still warming up\n").as_deref(), Some("still warming up"));
        assert_eq!(what_it_said(r#"{"ready": false}"#).as_deref(), Some(r#"{"ready": false}"#));
        assert_eq!(what_it_said(""), None);
        assert_eq!(what_it_said(" \n "), None);
        assert_eq!(what_it_said(r#"{"error": "  "}"#), None);
        let long = "é".repeat(400);
        let cut = what_it_said(&long).unwrap();
        assert_eq!(cut.chars().count(), SAID_MAX_CHARS);
        assert!(long.starts_with(&cut));
    }

    /// A restart changes what a look compares once, when the container
    /// stops, and not again while it runs back as it was; a new stop does.
    #[test]
    fn a_restart_is_told_once_by_when_it_stopped() {
        use weft_platform_traits::{RestartCause, UnitObservation, UnitRestart, UnitRunState};
        let seen = |state: UnitRunState, stopped: Option<i64>| {
            vec![UnitObservation {
                copy_id: "c".into(),
                unit: "db".into(),
                hash: "h".into(),
                state,
                restart: stopped.map(|at| UnitRestart { container: "pg".into(), stopped_at_unix: at, cause: RestartCause::OutOfMemory, count: 1 }),
            }]
        };
        let before = Host::stands(&seen(UnitRunState::Ready, None));
        // Back up before the look: the state reads the same, the stop does not.
        let restarted = Host::stands(&seen(UnitRunState::Ready, Some(100)));
        assert_ne!(before, restarted);
        assert_eq!(restarted, Host::stands(&seen(UnitRunState::Ready, Some(100))), "the same restart is not told again");
        assert_ne!(restarted, Host::stands(&seen(UnitRunState::Ready, Some(160))), "a new stop is");
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
