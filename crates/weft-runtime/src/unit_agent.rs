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
//! `process` is the agent of a unit that runs a process on the machine
//! (`weft_platform_local::process_units`): it runs the process, stamps
//! what it prints into the unit's log, and starts it again when it exits.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context as _;
use axum::routing::post;
use axum::{Json, Router};
use weft_core::infra::wire::{LogLine, Pipe};
use weft_platform_traits::unit_agent::{
    ProbeAnswer, ProbeRequest, ProcessAgentState, ProcessSetup, AGENT_BROKER_URL_ENV, AGENT_IDENTITY_ENV, PROBE_PATH, VALUES_PATH,
};
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
    Json(weft_platform_local::probe::run_probe(&req.probe, timeout).await)
}

/// How big a process unit's log grows before it rolls over to `.1`
/// (`crate::log_file`): the host reads both back from their end to follow
/// it, as far as a read needs.
pub const PROCESS_LOG_MAX_BYTES: u64 = 10 * 1024 * 1024;

/// Run a process unit's process, `command`, until this agent is told to
/// stop (`SIGTERM`), the way Docker runs a container with `--restart
/// unless-stopped`: started in this agent's folder with this agent's
/// environment (both the host's choice), every line it prints appended to
/// `log` with the instant it came and the pipe it came out of
/// (`LogLine::stamped`), and started again whenever it exits on its own,
/// after [`restart_delay`]. How it is doing is kept at `state`
/// ([`ProcessAgentState`]) for the host to read.
///
/// The process shares this agent's process group, which the host started
/// as a group of its own: a stop is passed on to the process, which gets
/// to wind down, and the host kills the whole group once its grace ends,
/// whatever the process started included.
///
/// The process never outlives this agent: on any way out of it (an error,
/// a panic) the process is killed as the agent lets go of it
/// (`kill_on_drop`), and on Linux the kernel kills it the moment the agent
/// dies, even killed outright (`SIGKILL`), which no code of the agent's
/// runs through ([`dies_with_the_agent`]). Left running, it would hold the
/// unit's port against the process of the agent the host starts next, and
/// nothing would ever stop it. On macOS, which has no such signal, the host
/// kills what a gone agent left before it starts the next one
/// (`weft_platform_local::processes::kill_leftovers`), as it does on Linux
/// for what the process itself started.
///
/// With `setup` (a `ProcessSetup` file), the needs it names are met first
/// ([`set_up`]), and the process runs with their program folders first on
/// its `PATH`.
pub async fn process(state: &Path, log: &Path, setup: Option<&Path>, command: &[String]) -> anyhow::Result<()> {
    use weft_platform_traits::unit_agent::{ProcessExit, ProcessRun};
    let (program, args) = command.split_first().context("a process unit's agent is given the command to run")?;
    let log = std::sync::Arc::new(crate::log_file::LogFile::open(log, PROCESS_LOG_MAX_BYTES).with_context(|| format!("open {}", log.display()))?);
    let mut stop = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).context("listen for the host's stop")?;
    let path = match setup {
        None => None,
        Some(setup) => match set_up(setup, state, &log, &mut stop).await? {
            Some(path) => Some(path),
            None => return Ok(()),
        },
    };
    let mut kept = ProcessAgentState::default();
    let mut delay = None;
    loop {
        let started = std::time::Instant::now();
        let spawned = process_command(program, args, path.as_deref()).spawn();
        let code = match spawned {
            // What a shell answers for a program it cannot run.
            Err(e) => {
                say(&log, &format!("could not start `{program}`: {e}"));
                127
            }
            Ok(mut child) => {
                let pid = child.id().context("the process ended before its pid could be read")?;
                kept.running = Some(ProcessRun { pid, started_at_unix: now_unix() });
                kept.write(state)?;
                stamp(child.stdout.take(), Pipe::Stdout, log.clone());
                stamp(child.stderr.take(), Pipe::Stderr, log.clone());
                tokio::select! {
                    status = child.wait() => exit_code(status?),
                    _ = stop.recv() => {
                        // SAFETY: kill(2) takes plain integers and touches no memory of ours.
                        unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
                        let code = exit_code(child.wait().await?);
                        kept.running = None;
                        kept.write(state)?;
                        say(&log, &format!("stopped; the process exited with code {code}"));
                        return Ok(());
                    }
                }
            }
        };
        kept.running = None;
        kept.restarts += 1;
        kept.last_exit = Some(ProcessExit { code, at_unix: now_unix() });
        kept.write(state)?;
        let wait = restart_delay(delay, started.elapsed());
        delay = Some(wait);
        say(&log, &format!("the process exited with code {code}; starting it again in {} ms", wait.as_millis()));
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            _ = stop.recv() => return Ok(()),
        }
    }
}

/// Meet the needs the `ProcessSetup` at `file` names, one after the
/// other, each step said in the agent's `state` (`setting_up`) and its
/// log: the `PATH` the process then runs with, their program folders
/// first. `None` when the host stopped the agent meanwhile, or once it
/// stopped it after a need could not be met, which the agent keeps in
/// its `state` (`setup_failed`) and waits there, its process never
/// started: an agent that exited would be started again, installing
/// again, for ever.
///
/// No deadline: a person waits on the install, as long as it takes. A
/// stop ends the agent at once; the install it ran is in its process
/// group, which the host kills with it, and the install left halfway is
/// one the next setup starts over (`weft_core::needs::setup`).
async fn set_up(file: &Path, state: &Path, log: &std::sync::Arc<crate::log_file::LogFile>, stop: &mut tokio::signal::unix::Signal) -> anyhow::Result<Option<String>> {
    let setup: ProcessSetup = serde_json::from_slice(&std::fs::read(file).with_context(|| format!("read {}", file.display()))?)
        .with_context(|| format!("{} is no process setup this weft reads", file.display()))?;
    let mut path = std::env::var("PATH").unwrap_or_default();
    let system = weft_core::needs::System::here();
    for need in setup.needs {
        let (root, given, system, state_file, said) = (setup.root.clone(), path.clone(), system.clone(), state.to_path_buf(), log.clone());
        let meeting = tokio::task::spawn_blocking(move || {
            need.meet(&root, &given, &system, &mut |step| {
                let words = step.words(&need.name);
                say(&said, &words);
                if let Err(e) = (ProcessAgentState { setting_up: Some(words), ..Default::default() }).write(&state_file) {
                    tracing::error!(target: "weft_runtime::unit_agent", error = %format!("{e:#}"), "could not say what the setup is doing");
                }
            })
        });
        let met = tokio::select! {
            met = meeting => met.context("a need's setup stopped")?,
            _ = stop.recv() => return Ok(None),
        };
        match met {
            Ok(bins) => path = weft_core::needs::setup::path_with(&bins, &path),
            Err(why) => {
                let why = format!(
                    "{why}\nthen start the infra again (`weft deps-install`, in the project's folder, installs what this \
                     machine lacks for it, asking for your password where a system package needs it)"
                );
                say(log, &why);
                ProcessAgentState { setup_failed: Some(why), ..Default::default() }.write(state)?;
                stop.recv().await;
                return Ok(None);
            }
        }
    }
    Ok(Some(path))
}

/// The command that starts a process unit's process: what it prints piped
/// to the agent, and killed whenever the agent lets go of it or dies. With
/// `path`, that is its `PATH`.
fn process_command(program: &str, args: &[String], path: Option<&str>) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(program);
    if let Some(path) = path {
        command.env("PATH", path);
    }
    command
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    dies_with_the_agent(command.as_std_mut());
    command
}

/// Have the kernel kill what `command` starts the moment the thread that
/// starts it ends (`PR_SET_PDEATHSIG`, which is about the thread, not the
/// process). That is the agent's whole life: [`process`] runs on the
/// agent's main thread (`main` awaits it, and `#[tokio::main]` polls that
/// on the main thread), which ends only as the agent does.
#[cfg(target_os = "linux")]
fn dies_with_the_agent(command: &mut std::process::Command) {
    use std::os::unix::process::CommandExt as _;
    // SAFETY: getpid(2) takes nothing and touches no memory of ours.
    let agent = unsafe { libc::getpid() };
    // SAFETY: what runs between fork and exec only calls prctl(2),
    // getppid(2) and _exit(2), all async-signal-safe, and allocates
    // nothing.
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL as libc::c_ulong, 0, 0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            // The agent died between the fork and the line above, so the
            // signal will never come: go now.
            if libc::getppid() != agent {
                libc::_exit(137);
            }
            Ok(())
        });
    }
}

/// macOS has no signal for a parent's death: the host kills what a gone
/// agent left before it starts the next one
/// (`weft_platform_local::processes::kill_leftovers`).
#[cfg(target_os = "macos")]
fn dies_with_the_agent(_command: &mut std::process::Command) {}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("a process unit's agent kills its process when it dies on Linux and macOS only; say how on this system before building it here");

/// The first wait before a process that exited is started again.
const FIRST_RESTART_DELAY: Duration = Duration::from_millis(100);
/// The longest one.
const LONGEST_RESTART_DELAY: Duration = Duration::from_secs(60);
/// How long a process must have run for its next exit to wait the first
/// delay again.
const RAN_LONG_ENOUGH: Duration = Duration::from_secs(10);

/// How long to wait before starting again a process that ran for `ran`,
/// when the wait before its last start was `previous` (`None`: it had not
/// exited before): Docker's for a container it restarts, twice the last
/// one from a tenth of a second up to a minute, back to a tenth once it
/// ran ten seconds.
fn restart_delay(previous: Option<Duration>, ran: Duration) -> Duration {
    match previous {
        Some(previous) if ran < RAN_LONG_ENOUGH => (previous * 2).min(LONGEST_RESTART_DELAY),
        _ => FIRST_RESTART_DELAY,
    }
}

/// The exit code a shell and Docker say for `status`: its own, or 128
/// plus the signal that ended it.
fn exit_code(status: std::process::ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt as _;
    status.code().or_else(|| status.signal().map(|signal| 128 + signal)).unwrap_or(-1)
}

fn now_unix() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("system clock past UNIX_EPOCH").as_secs() as i64
}

/// Append `line` to `log` stamped as one of the agent's own, on stderr.
fn say(log: &crate::log_file::LogFile, line: &str) {
    write_line(log, &LogLine::now(Pipe::Stderr, format!("weft: {line}")));
}

fn write_line(log: &crate::log_file::LogFile, line: &LogLine) {
    if let Err(e) = log.write_all(format!("{}\n", line.stamped()).as_bytes()) {
        tracing::error!(target: "weft_runtime::unit_agent", error = %e, "could not write the process's log");
    }
}

/// Copy what the process prints on `pipe` into `log`, one stamped line at
/// a time, until the pipe closes: when the process and every program it
/// started that kept the pipe are gone.
fn stamp(from: Option<impl tokio::io::AsyncRead + Unpin + Send + 'static>, pipe: Pipe, log: std::sync::Arc<crate::log_file::LogFile>) {
    use tokio::io::AsyncBufReadExt as _;
    let Some(from) = from else { return };
    tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(from);
        let mut raw = Vec::new();
        loop {
            raw.clear();
            match reader.read_until(b'\n', &mut raw).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let text = String::from_utf8_lossy(&raw);
                    write_line(&log, &LogLine::now(pipe, text.trim_end_matches(['\n', '\r'])));
                }
            }
        }
    });
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
            // Its agent's name is the machine's own.
            names: weft_platform_local::infra_names::Names::Machine,
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

    /// The wait before a restart doubles from a tenth of a second while
    /// the process keeps exiting soon after it starts, stops at a minute,
    /// and starts over once it ran ten seconds.
    #[test]
    fn a_process_that_keeps_exiting_waits_longer_each_time() {
        let quick = Duration::from_secs(1);
        assert_eq!(restart_delay(None, quick), Duration::from_millis(100));
        assert_eq!(restart_delay(Some(Duration::from_millis(100)), quick), Duration::from_millis(200));
        assert_eq!(restart_delay(Some(Duration::from_secs(40)), quick), Duration::from_secs(60));
        assert_eq!(restart_delay(Some(Duration::from_secs(60)), quick), Duration::from_secs(60));
        assert_eq!(restart_delay(Some(Duration::from_secs(60)), Duration::from_secs(10)), Duration::from_millis(100));
    }

    /// The agent runs its process, stamps what it prints into the log,
    /// keeps how it exited, starts it again, and on a stop passes the stop
    /// on and ends. Against a real `sh`, which every machine weft runs on
    /// has.
    #[tokio::test]
    async fn the_agent_runs_its_process_again_and_stops_when_told() {
        let dir = tempfile::tempdir().unwrap();
        let (state, log) = (dir.path().join("agent.json"), dir.path().join("process.log"));
        let command: Vec<String> = ["sh", "-c", "echo out; echo err >&2; exit 3"].map(String::from).to_vec();
        let agent = tokio::spawn({
            let (state, log) = (state.clone(), log.clone());
            async move { process(&state, &log, None, &command).await }
        });
        let restarted = async {
            loop {
                if ProcessAgentState::read(&state).unwrap().is_some_and(|s| s.restarts >= 2) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(10), restarted).await.expect("the process is started again");
        let kept = ProcessAgentState::read(&state).unwrap().unwrap();
        assert_eq!(kept.last_exit.map(|e| e.code), Some(3));
        let lines: Vec<LogLine> = std::fs::read_to_string(&log).unwrap().lines().map(|l| LogLine::read_stamped(l).unwrap()).collect();
        assert!(lines.iter().any(|l| l.pipe == Pipe::Stdout && l.text == "out"), "{lines:?}");
        assert!(lines.iter().any(|l| l.pipe == Pipe::Stderr && l.text == "err"), "{lines:?}");
        assert!(lines.iter().any(|l| l.text.starts_with("weft: the process exited with code 3; starting it again in")), "{lines:?}");
        // SAFETY: kill(2) takes plain integers and touches no memory of ours.
        unsafe { libc::kill(std::process::id() as libc::pid_t, libc::SIGTERM) };
        tokio::time::timeout(Duration::from_secs(10), agent).await.expect("the agent ends when told").unwrap().unwrap();
    }

    /// A need for `fakeprog`, which `script` installs under `root`.
    fn fake_need(script: &str) -> weft_core::needs::Need {
        weft_core::needs::Need {
            name: "Fake".into(),
            stage: weft_core::needs::NeedStage::Run,
            check: vec!["fakeprog".into(), "--version".into()],
            install: Default::default(),
            how: None,
            recipe: Some(weft_core::needs::Recipe {
                folder: "tools/fake".into(),
                script: [(weft_core::needs::Os::here().unwrap(), script.to_string())].into(),
                bin: vec!["bin".into()],
                prelude: String::new(),
            }),
        }
    }

    /// The agent waits for its state to satisfy `until`, then answers it.
    async fn state_when(state: &Path, until: impl Fn(&ProcessAgentState) -> bool) -> ProcessAgentState {
        let seen = async {
            loop {
                if let Some(kept) = ProcessAgentState::read(state).unwrap().filter(|s| until(s)) {
                    break kept;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        match tokio::time::timeout(Duration::from_secs(10), seen).await {
            Ok(kept) => kept,
            Err(_) => panic!("the agent never got there; its state: {:?}", ProcessAgentState::read(state)),
        }
    }

    /// A process whose need weft installs: the agent installs it into its
    /// folder, says so in its log, and runs the process with the need's
    /// programs on its PATH. One whose install fails never starts, the
    /// failure kept in the agent's state until it is stopped.
    #[tokio::test]
    async fn the_agent_installs_what_its_process_needs_first() {
        let dir = tempfile::tempdir().unwrap();
        let (state, log, setup) = (dir.path().join("agent.json"), dir.path().join("process.log"), dir.path().join("setup.json"));
        let script = "mkdir -p \"$WEFT_NEED_DIR/bin\"\nprintf '#!/bin/sh\\n[ \"$1\" = --version ] && exit 0\\necho from the installed program; exit 4\\n' > \"$WEFT_NEED_DIR/bin/fakeprog\"\nchmod +x \"$WEFT_NEED_DIR/bin/fakeprog\"";
        let written = |need| std::fs::write(&setup, serde_json::to_vec(&ProcessSetup { root: dir.path().join("root"), needs: vec![need] }).unwrap()).unwrap();
        written(fake_need(script));
        let command: Vec<String> = vec!["fakeprog".into()];
        let agent = tokio::spawn({
            let (state, log, setup) = (state.clone(), log.clone(), setup.clone());
            async move { process(&state, &log, Some(&setup), &command).await }
        });
        let kept = state_when(&state, |s| s.last_exit.is_some()).await;
        assert_eq!(kept.last_exit.map(|e| e.code), Some(4), "the installed program ran");
        assert_eq!(kept.setting_up, None);
        let printed = std::fs::read_to_string(&log).unwrap();
        assert!(printed.contains("weft: installing Fake (its log: "), "{printed}");
        assert!(printed.contains("from the installed program"), "{printed}");
        // SAFETY: kill(2) takes plain integers and touches no memory of ours.
        unsafe { libc::kill(std::process::id() as libc::pid_t, libc::SIGTERM) };
        tokio::time::timeout(Duration::from_secs(10), agent).await.expect("the agent ends when told").unwrap().unwrap();

        std::fs::remove_file(&state).unwrap();
        let mut failing = fake_need("echo 'the mirror is down'; exit 2");
        failing.recipe.as_mut().unwrap().folder = "tools/broken".into();
        written(failing);
        let command: Vec<String> = vec!["fakeprog".into()];
        let agent = tokio::spawn({
            let (state, log, setup) = (state.clone(), log.clone(), setup.clone());
            async move { process(&state, &log, Some(&setup), &command).await }
        });
        let kept = state_when(&state, |s| s.setup_failed.is_some()).await;
        let why = kept.setup_failed.unwrap();
        assert!(why.starts_with("installing Fake into ") && why.contains("the mirror is down"), "{why}");
        assert!(why.contains("then start the infra again"), "{why}");
        assert_eq!((kept.running, kept.restarts), (None, 0), "never started");
        // SAFETY: as above.
        unsafe { libc::kill(std::process::id() as libc::pid_t, libc::SIGTERM) };
        tokio::time::timeout(Duration::from_secs(10), agent).await.expect("a failed setup ends when told").unwrap().unwrap();
    }

    /// Whether the process `pid` has ended: gone, or a zombie its parent
    /// (this test process) has not reaped yet.
    #[cfg(target_os = "linux")]
    fn ended(pid: u32) -> bool {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => stat.rsplit_once(')').is_some_and(|(_, rest)| rest.trim_start().starts_with('Z')),
            Err(_) => true,
        }
    }

    /// An agent that goes away while its process runs (here its task is
    /// dropped, as an error or a panic in it would) takes the process
    /// with it.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_process_ends_when_its_agent_lets_go_of_it() {
        let dir = tempfile::tempdir().unwrap();
        let (state, log) = (dir.path().join("agent.json"), dir.path().join("process.log"));
        let command: Vec<String> = ["sleep", "300"].map(String::from).to_vec();
        let agent = tokio::spawn({
            let (state, log) = (state.clone(), log.clone());
            async move { process(&state, &log, None, &command).await }
        });
        let running = async {
            loop {
                if let Some(run) = ProcessAgentState::read(&state).unwrap().and_then(|s| s.running) {
                    break run.pid;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        let pid = tokio::time::timeout(Duration::from_secs(10), running).await.expect("the process starts");
        agent.abort();
        assert!(agent.await.unwrap_err().is_cancelled());
        let gone = async {
            while !ended(pid) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        let ended_in_time = tokio::time::timeout(Duration::from_secs(10), gone).await.is_ok();
        // SAFETY: kill(2) takes plain integers and touches no memory of ours.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        assert!(ended_in_time, "the process {pid} outlived its agent");
    }

    /// What an agent starts is killed by the kernel the moment the thread
    /// that started it ends, which no code of the agent's has to run for:
    /// the agent killed outright (`SIGKILL`) leaves nothing running.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_process_dies_with_what_started_it() {
        let mut child = std::thread::spawn(|| {
            let mut command = std::process::Command::new("sleep");
            command.arg("300");
            dies_with_the_agent(&mut command);
            command.spawn().unwrap()
        })
        .join()
        .unwrap();
        let asked = std::time::Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break Some(status);
            }
            if asked.elapsed() > Duration::from_secs(10) {
                break None;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let Some(status) = status else {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("the process outlived the thread that started it");
        };
        use std::os::unix::process::ExitStatusExt as _;
        assert_eq!(status.signal(), Some(libc::SIGKILL));
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
