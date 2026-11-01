//! The local Docker daemon, reached through the `docker` command.
//!
//! Every local implementation that runs containers (infra, images) talks to Docker
//! through [`Docker`], so the decisions (which container to start, with
//! what, when to stop it) are tested against a fake that records the
//! arguments, and only [`DockerCli`] runs anything.

use std::collections::BTreeMap;
use std::process::Stdio;

use async_trait::async_trait;

/// What one `docker` call printed and how it exited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DockerOutput {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

impl DockerOutput {
    pub fn success(&self) -> bool {
        self.code == 0
    }

    /// The output when the call succeeded; the call and what Docker said
    /// otherwise.
    pub fn ok(self, args: &[String]) -> anyhow::Result<String> {
        if self.success() {
            Ok(self.stdout)
        } else {
            anyhow::bail!("`docker {}` failed ({}): {}", args.join(" "), self.code, self.stderr.trim())
        }
    }
}

#[async_trait]
pub trait Docker: Send + Sync {
    /// Run `docker <args>` to completion.
    async fn exec(&self, args: &[String]) -> anyhow::Result<DockerOutput>;

    /// [`Self::exec`] with everything the call prints written to `log` as
    /// it prints it, stdout and stderr together, so the file can be
    /// followed while the call runs. Answers how it exited.
    async fn exec_logged(&self, args: &[String], log: &std::path::Path) -> anyhow::Result<i32> {
        let out = self.exec(args).await?;
        tokio::fs::write(log, format!("{}{}", out.stdout, out.stderr)).await.map_err(|e| anyhow::anyhow!("write {}: {e}", log.display()))?;
        Ok(out.code)
    }
}

/// The real daemon, through the `docker` command on the PATH.
pub struct DockerCli;

#[async_trait]
impl Docker for DockerCli {
    async fn exec(&self, args: &[String]) -> anyhow::Result<DockerOutput> {
        let out = tokio::process::Command::new("docker")
            .args(args)
            .env("DOCKER_BUILDKIT", "1")
            .stdin(Stdio::null())
            // A caller that stops waiting (a released build) stops the
            // command with it.
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|e| anyhow::anyhow!("run `docker`: {e} (is Docker installed and on the PATH?)"))?;
        Ok(DockerOutput {
            code: out.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }

    async fn exec_logged(&self, args: &[String], log: &std::path::Path) -> anyhow::Result<i32> {
        let file = std::fs::File::create(log).map_err(|e| anyhow::anyhow!("create {}: {e}", log.display()))?;
        let both = file.try_clone().map_err(|e| anyhow::anyhow!("open {} twice: {e}", log.display()))?;
        let status = tokio::process::Command::new("docker")
            .args(args)
            .env("DOCKER_BUILDKIT", "1")
            .stdin(Stdio::null())
            .stdout(file)
            .stderr(both)
            .kill_on_drop(true)
            .status()
            .await
            .map_err(|e| anyhow::anyhow!("run `docker`: {e} (is Docker installed and on the PATH?)"))?;
        Ok(status.code().unwrap_or(-1))
    }
}

/// Run `args` and hand back stdout, failing loudly with what Docker said.
pub async fn run(docker: &dyn Docker, args: Vec<String>) -> anyhow::Result<String> {
    docker.exec(&args).await?.ok(&args)
}

/// Every label weft puts on what it starts, so it finds its own
/// containers and volumes again and never touches anything else.
pub mod labels {
    /// On everything weft starts: which install it belongs to
    /// (`weft_core::infra::Install::label_value`), so one install never
    /// touches another's on a machine that holds several.
    pub const INSTALL: &str = weft_core::infra::INSTALL_LABEL;
    pub const PROJECT: &str = "weft.project";
    pub const TENANT: &str = "weft.tenant";
    /// What it is: one of [`super::roles`].
    pub const ROLE: &str = "weft.role";
    pub const IMAGE: &str = "weft.image";
    pub const NODE: &str = "weft.node";
    /// Which copy of an infra node (`NodeRef::copy_id`).
    pub const COPY: &str = "weft.copy";
    pub const UNIT: &str = "weft.unit";
    pub const UNIT_HASH: &str = "weft.unit-hash";
}

/// The values of [`labels::ROLE`]: what a container is for.
pub mod roles {
    /// Part of an infra node's unit.
    pub const INFRA: &str = "infra";
}

/// `--label k=v` for each label.
pub fn label_args(labels: &BTreeMap<&str, String>) -> Vec<String> {
    labels.iter().flat_map(|(k, v)| ["--label".to_string(), format!("{k}={v}")]).collect()
}

/// `--filter label=k=v` for each label.
pub fn filter_args(labels: &[(&str, &str)]) -> Vec<String> {
    labels.iter().flat_map(|(k, v)| ["--filter".to_string(), format!("label={k}={v}")]).collect()
}

/// The containers of `install` matching every label, as `docker ps` lists
/// them: one JSON object per line.
pub async fn containers(docker: &dyn Docker, install: &str, labels: &[(&str, &str)]) -> anyhow::Result<Vec<ContainerRow>> {
    let mut args = vec!["ps".to_string(), "--all".into(), "--no-trunc".into(), "--format".into(), "{{json .}}".into()];
    args.extend(filter_args(&[(labels::INSTALL, install)]));
    args.extend(filter_args(labels));
    let out = run(docker, args).await?;
    out.lines().filter(|l| !l.trim().is_empty()).map(ContainerRow::parse).collect()
}

/// The names of every container that mounts the volume `volume`, whoever
/// made it.
pub async fn containers_using(docker: &dyn Docker, volume: &str) -> anyhow::Result<Vec<String>> {
    let args = vec!["ps".to_string(), "--all".into(), "--filter".into(), format!("volume={volume}"), "--format".into(), "{{.Names}}".into()];
    let out = run(docker, args).await?;
    Ok(out.lines().map(str::trim).filter(|l| !l.is_empty()).map(str::to_string).collect())
}

/// When the container `name` last started (`State.StartedAt`, on the
/// daemon's clock): a restart moves it. `docker ps` prints only a
/// rounded "Up 2 minutes", so this asks `docker inspect`.
pub async fn started_at(docker: &dyn Docker, name: &str) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let out = run(docker, vec!["inspect".into(), "--format".into(), "{{json .State.StartedAt}}".into(), name.into()]).await?;
    parse_started_at(name, &out)
}

fn parse_started_at(name: &str, printed: &str) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let at: String = serde_json::from_str(printed.trim())
        .map_err(|e| anyhow::anyhow!("`docker inspect` printed no start time for {name} ({e}): {}", printed.trim()))?;
    Ok(chrono::DateTime::parse_from_rfc3339(&at)
        .map_err(|e| anyhow::anyhow!("`docker inspect` printed a start time for {name} that is not RFC 3339 ({e}): {at}"))?
        .to_utc())
}

/// How one container last stopped, as `docker inspect` says: what a
/// restart is read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerStop {
    pub name: String,
    /// How many times Docker's restart policy brought it back since it
    /// was last started by hand (a `docker start` or `docker restart`
    /// sets it back to 0).
    pub restart_count: u32,
    /// `running`, `restarting`, `exited`...
    pub status: String,
    /// Whether the kernel killed one of its processes for memory since it
    /// last started: Docker sets it on every such kill, whether the
    /// container's main process was the one killed (the container stops)
    /// or another one inside it was (it runs on, a Postgres backend for
    /// one), and clears it when the container starts again.
    pub oom_killed: bool,
    /// Its last exit code, cleared to 0 once it runs again, like
    /// `oom_killed`.
    pub exit_code: i64,
    /// When it last stopped; `None` when it never did. Kept while it
    /// runs again, so the same stop reads the same at every look.
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
    /// When it last started; `None` when it never did.
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// How each named container last stopped, in one `docker inspect`. A
/// container removed since it was listed is left out.
pub async fn container_stops(docker: &dyn Docker, names: &[String]) -> anyhow::Result<Vec<ContainerStop>> {
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let mut args = vec![
        "inspect".to_string(),
        "--format".into(),
        r#"{"name":{{json .Name}},"restartCount":{{.RestartCount}},"state":{{json .State}}}"#.into(),
    ];
    args.extend(names.iter().cloned());
    let out = docker.exec(&args).await?;
    // A failed inspect is fine only when what it says is that some of
    // them are gone; one that says nothing failed for another reason.
    let mut said = out.stderr.lines().map(str::trim).filter(|l| !l.is_empty()).peekable();
    let only_gone = said.peek().is_some() && said.all(|l| l.to_lowercase().contains("no such object"));
    if !out.success() && !only_gone {
        out.clone().ok(&args)?;
    }
    out.stdout.lines().filter(|l| !l.trim().is_empty()).map(parse_container_stop).collect()
}

fn parse_container_stop(line: &str) -> anyhow::Result<ContainerStop> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Printed {
        name: String,
        restart_count: u32,
        state: State,
    }
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct State {
        status: String,
        #[serde(rename = "OOMKilled")]
        oom_killed: bool,
        exit_code: i64,
        started_at: String,
        finished_at: String,
    }
    let p: Printed =
        serde_json::from_str(line).map_err(|e| anyhow::anyhow!("`docker inspect` printed a line that is not a container's state ({e}): {line}"))?;
    // Docker's zero time (year 1) is "never".
    let instant = |what: &str, raw: &str| -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
        let at = chrono::DateTime::parse_from_rfc3339(raw)
            .map_err(|e| anyhow::anyhow!("`docker inspect` printed a {what} time that is not RFC 3339 ({e}): {raw}"))?
            .to_utc();
        Ok((at.timestamp() > 0).then_some(at))
    };
    Ok(ContainerStop {
        name: p.name.trim_start_matches('/').to_string(),
        restart_count: p.restart_count,
        status: p.state.status,
        oom_killed: p.state.oom_killed,
        exit_code: p.state.exit_code,
        finished_at: instant("stop", &p.state.finished_at)?,
        started_at: instant("start", &p.state.started_at)?,
    })
}

/// The Docker daemon's own clock: the one its logs and events are
/// stamped with. A time to read up to is taken here, never from this
/// process's clock, so a daemon running behind it (another machine, a
/// desktop VM) is never asked about its future.
pub async fn daemon_now(docker: &dyn Docker) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let out = run(docker, vec!["info".into(), "--format".into(), "{{json .SystemTime}}".into()]).await?;
    let at: String = serde_json::from_str(out.trim())
        .map_err(|e| anyhow::anyhow!("`docker info` printed no system time ({e}): {}", out.trim()))?;
    Ok(chrono::DateTime::parse_from_rfc3339(&at)
        .map_err(|e| anyhow::anyhow!("`docker info` printed a system time that is not RFC 3339 ({e}): {at}"))?
        .to_utc())
}

/// `at` as `docker events --since/--until` reads it, to the nanosecond.
fn events_time(at: chrono::DateTime<chrono::Utc>) -> String {
    format!("{}.{:09}", at.timestamp(), at.timestamp_subsec_nanos())
}

/// Why the container `name` stopped at `at`, from what Docker still
/// remembers of its events around then. Docker keeps only its latest
/// events, daemon-wide, so a stop old enough reads as
/// [`RestartCause::Unknown`](weft_platform_traits::RestartCause::Unknown).
pub async fn stop_cause(docker: &dyn Docker, name: &str, at: chrono::DateTime<chrono::Utc>) -> anyhow::Result<weft_platform_traits::RestartCause> {
    // The `die` event lands a moment after the stop time inspect
    // records; the window is wide enough for a slow daemon, and the pick
    // is the `die` nearest the stop. It ends no later than the daemon's
    // now: `docker events` asked about a time still ahead waits for it,
    // streaming, and a look just after a stop would sit there.
    let until = (at + chrono::TimeDelta::seconds(10)).min(daemon_now(docker).await?);
    let args = vec![
        "events".to_string(),
        "--since".into(),
        events_time(at - chrono::TimeDelta::seconds(2)),
        "--until".into(),
        events_time(until),
        "--filter".into(),
        format!("container={name}"),
        "--filter".into(),
        "event=oom".into(),
        "--filter".into(),
        "event=die".into(),
        "--format".into(),
        r#"{{.TimeNano}} {{.Action}} {{index .Actor.Attributes "exitCode"}}"#.into(),
    ];
    let printed = run(docker, args).await?;
    read_stop_cause(&printed, at)
}

/// The cause of the stop at `at` in `docker events` lines
/// (`<nanos> <oom|die> [exit code]`): the `die` nearest `at`, killed for
/// memory when its process was killed (exit code [`KILLED`]) and an
/// `oom` came after the `die` before it. An `oom` alone is not enough: a
/// process inside the container killed for memory earlier in the same
/// run (which the container survived) leaves one too.
fn read_stop_cause(printed: &str, at: chrono::DateTime<chrono::Utc>) -> anyhow::Result<weft_platform_traits::RestartCause> {
    use weft_platform_traits::RestartCause;
    let mut dies: Vec<(i128, i64)> = Vec::new();
    let mut ooms: Vec<i128> = Vec::new();
    for line in printed.lines().map(str::trim).filter(|l| !l.is_empty()) {
        let mut parts = line.split_whitespace();
        let (Some(nanos), Some(action)) = (parts.next(), parts.next()) else {
            anyhow::bail!("`docker events` printed a line that is not '<time> <action>': {line}");
        };
        let nanos: i128 = nanos.parse().map_err(|e| anyhow::anyhow!("`docker events` printed a time that is not a number ({e}): {line}"))?;
        match action {
            "oom" => ooms.push(nanos),
            "die" => {
                let code = parts.next().ok_or_else(|| anyhow::anyhow!("`docker events` printed a die with no exit code: {line}"))?;
                dies.push((nanos, code.parse().map_err(|e| anyhow::anyhow!("`docker events` printed an exit code that is not a number ({e}): {line}"))?));
            }
            other => anyhow::bail!("`docker events` printed an event it was not asked for ({other}): {line}"),
        }
    }
    let at = i128::from(at.timestamp()) * 1_000_000_000 + i128::from(at.timestamp_subsec_nanos());
    let Some(&(died, code)) = dies.iter().min_by_key(|(t, _)| (t - at).abs()) else {
        return Ok(RestartCause::Unknown);
    };
    let before = dies.iter().map(|(t, _)| *t).filter(|t| *t < died).max().unwrap_or(i128::MIN);
    Ok(if code == KILLED && ooms.iter().any(|t| *t > before && *t <= died) { RestartCause::OutOfMemory } else { RestartCause::Exited { code } })
}

/// The exit code of a container whose main process was killed outright
/// (128 + SIGKILL), which is how the kernel ends one for memory.
pub const KILLED: i64 = 137;

/// Every time the kernel killed a process inside one of the containers
/// `names` for memory, at `since` or after and up to `until` (the
/// daemon's now, see [`daemon_now`]), each with the container's name,
/// from Docker's `oom` events: one call for all of them. Docker keeps
/// only its latest events, daemon-wide, so an old kill may be missing.
pub async fn oom_kills(
    docker: &dyn Docker,
    names: &[&str],
    since: chrono::DateTime<chrono::Utc>,
    until: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Vec<(String, chrono::DateTime<chrono::Utc>)>> {
    if names.is_empty() || since > until {
        return Ok(Vec::new());
    }
    let mut args = vec!["events".to_string(), "--since".into(), events_time(since), "--until".into(), events_time(until)];
    // Filters on the same key are or'd: one read covers every container.
    for name in names {
        args.extend(["--filter".to_string(), format!("container={name}")]);
    }
    args.extend(["--filter".to_string(), "event=oom".into(), "--format".into(), "{{.TimeNano}} {{.Actor.Attributes.name}}".into()]);
    read_oom_kills(&run(docker, args).await?)
}

/// `docker events` lines that are each `<nanos> <container name>`.
fn read_oom_kills(printed: &str) -> anyhow::Result<Vec<(String, chrono::DateTime<chrono::Utc>)>> {
    printed
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(|line| {
            let (nanos, name) = line
                .split_once(' ')
                .ok_or_else(|| anyhow::anyhow!("`docker events` printed a line that is not '<time> <container>': {line}"))?;
            let nanos: i64 = nanos.parse().map_err(|e| anyhow::anyhow!("`docker events` printed a time that is not a number ({e}): {line}"))?;
            Ok((name.to_string(), chrono::DateTime::from_timestamp_nanos(nanos)))
        })
        .collect()
}

/// One line of `docker ps --format '{{json .}}'`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerRow {
    pub name: String,
    /// `running`, `exited`, `created`, `restarting`, `paused`, `dead`.
    pub state: String,
    pub labels: BTreeMap<String, String>,
}

impl ContainerRow {
    pub fn parse(line: &str) -> anyhow::Result<Self> {
        let v: serde_json::Value =
            serde_json::from_str(line).map_err(|e| anyhow::anyhow!("`docker ps` printed a line that is not JSON ({e}): {line}"))?;
        let field = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or_default().to_string();
        let labels = field("Labels")
            .split(',')
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Ok(Self { name: field("Names"), state: field("State"), labels })
    }

    pub fn label(&self, key: &str) -> Option<&str> {
        self.labels.get(key).map(String::as_str)
    }
}

/// Remove the named containers, running or not. Removing one already gone,
/// or one Docker is already removing, is not an error.
pub async fn remove_containers(docker: &dyn Docker, names: &[String]) -> anyhow::Result<()> {
    if names.is_empty() {
        return Ok(());
    }
    let mut args = vec!["rm".to_string(), "--force".into()];
    args.extend(names.iter().cloned());
    let out = docker.exec(&args).await?;
    if out.success() || out.stderr.contains("No such container") || out.stderr.contains("is already in progress") {
        Ok(())
    } else {
        out.ok(&args).map(|_| ())
    }
}

/// `--add-host` naming the machine `host.docker.internal`: what a container
/// that calls weft's internal port (the agent beside an infra unit)
/// reaches it under. Docker Desktop knows the name on its own; a
/// Linux engine only through this.
pub fn host_gateway_args() -> [String; 2] {
    ["--add-host".into(), "host.docker.internal:host-gateway".into()]
}

/// The address the project's containers reach this machine at, when they
/// reach it somewhere other than loopback: what the engine resolves
/// `host-gateway` to, asked of the engine itself in a throwaway container
/// of `image` (the runtime's own image, which the install already has, so
/// nothing is pulled), since a daemon setting (`host-gateway-ip`) can move
/// it off the default bridge. Asked on every start of the runtime, so a
/// change to Docker's network is picked up by the next start.
///
/// Where it is an address of this machine, the engine runs on this
/// machine's own kernel (Linux) and a container's call comes in on that
/// bridge: weft's internal port listens there too, and only there beside
/// loopback, so the machine's network never reaches it. Where it is not,
/// the engine runs in a machine of its own (Docker Desktop and the like),
/// which brings a container's call to this machine's loopback: `None`.
/// Refused, naming the probe, when the engine names no address at all.
pub async fn containers_call_in_at(docker: &dyn Docker, image: &str) -> anyhow::Result<Option<std::net::IpAddr>> {
    let [flag, host] = host_gateway_args();
    let args: Vec<String> =
        ["run", "--rm", &flag, &host, "--entrypoint", "getent", image, "hosts", "host.docker.internal"].map(String::from).into();
    let answer = docker
        .exec(&args)
        .await?
        .ok(&args)
        .map_err(|e| e.context("ask Docker which address the project's containers reach this machine at"))?;
    containers_address(&answer, image, |address| std::net::TcpListener::bind((address, 0)).map(drop))
}

/// [`containers_call_in_at`]'s decision on what the probe printed
/// (`getent hosts`: `172.17.0.1  host.docker.internal`), with `bind`
/// trying the address (a bind succeeds only on an address this machine
/// has, and an address it does not have is the one refusal that means the
/// engine runs elsewhere; any other refusal is said).
fn containers_address(
    answer: &str,
    image: &str,
    bind: impl Fn(std::net::IpAddr) -> std::io::Result<()>,
) -> anyhow::Result<Option<std::net::IpAddr>> {
    let Some(address) = answer.split_whitespace().next().and_then(|word| word.parse().ok()) else {
        anyhow::bail!(
            "Docker named no address for this machine as its containers reach it (`host-gateway` gave {:?}), so they \
             could not reach weft's internal port; check that `docker run --rm --add-host \
             host.docker.internal:host-gateway --entrypoint getent {image} hosts host.docker.internal` prints an address",
            answer.trim()
        );
    };
    match bind(address) {
        Ok(()) => Ok(Some(address)),
        Err(e) if e.kind() == std::io::ErrorKind::AddrNotAvailable => Ok(None),
        Err(e) => anyhow::bail!(
            "Docker says its containers reach this machine at {address}, which is this machine's, but weft cannot listen \
             there ({e}), so they could not reach weft's internal port"
        ),
    }
}

/// The network every container weft starts joins, so an infra container
/// reaches another by name.
pub const NETWORK: &str = "weft";

/// Create [`NETWORK`] when it does not exist yet.
pub async fn ensure_network(docker: &dyn Docker) -> anyhow::Result<()> {
    let inspect = docker.exec(&["network".into(), "inspect".into(), NETWORK.into()]).await?;
    if inspect.success() {
        return Ok(());
    }
    let args = vec!["network".to_string(), "create".into(), NETWORK.into()];
    let out = docker.exec(&args).await?;
    if out.success() || out.stderr.contains("already exists") {
        Ok(())
    } else {
        out.ok(&args).map(|_| ())
    }
}

#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;
    use parking_lot::Mutex;

    /// Records every call and answers from a script: the first rule whose
    /// prefix the call's arguments start with answers it; no rule, an
    /// empty success.
    #[derive(Default)]
    pub struct FakeDocker {
        calls: Mutex<Vec<Vec<String>>>,
        rules: Mutex<Vec<(Vec<String>, DockerOutput)>>,
    }

    impl FakeDocker {
        pub fn new() -> Self {
            Self::default()
        }

        /// Answer calls starting with `prefix` with `stdout`, exit 0.
        pub fn answer(&self, prefix: &[&str], stdout: &str) {
            self.rules.lock().push((
                prefix.iter().map(|s| s.to_string()).collect(),
                DockerOutput { code: 0, stdout: stdout.into(), stderr: String::new() },
            ));
        }

        /// Answer calls starting with `prefix` with `out`, whatever it
        /// says.
        pub fn respond(&self, prefix: &[&str], out: DockerOutput) {
            self.rules.lock().push((prefix.iter().map(|s| s.to_string()).collect(), out));
        }

        /// Drop every answer given for calls starting with `prefix`: what
        /// the daemon says changes.
        pub fn forget(&self, prefix: &[&str]) {
            self.rules.lock().retain(|(p, _)| p.iter().map(String::as_str).ne(prefix.iter().copied()));
        }

        /// Fail calls starting with `prefix`, printing `stderr`.
        pub fn fail(&self, prefix: &[&str], stderr: &str) {
            self.rules.lock().push((
                prefix.iter().map(|s| s.to_string()).collect(),
                DockerOutput { code: 1, stdout: String::new(), stderr: stderr.into() },
            ));
        }

        pub fn calls(&self) -> Vec<Vec<String>> {
            self.calls.lock().clone()
        }

        /// The calls whose first argument is `verb`.
        pub fn calls_to(&self, verb: &str) -> Vec<Vec<String>> {
            self.calls.lock().iter().filter(|c| c.first().is_some_and(|v| v == verb)).cloned().collect()
        }
    }

    #[async_trait]
    impl Docker for FakeDocker {
        async fn exec(&self, args: &[String]) -> anyhow::Result<DockerOutput> {
            self.calls.lock().push(args.to_vec());
            let rules = self.rules.lock();
            let hit = rules.iter().find(|(prefix, _)| args.starts_with(prefix));
            Ok(hit.map(|(_, out)| out.clone()).unwrap_or(DockerOutput { code: 0, stdout: String::new(), stderr: String::new() }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The containers' address is used when this machine has it, left out
    /// when the engine runs elsewhere (the address is not this machine's),
    /// and refused when Docker names none or the address cannot be listened
    /// on for another reason.
    #[test]
    fn the_containers_address_is_this_machine_s_or_none() {
        let gateway: std::net::IpAddr = [172, 17, 0, 1].into();
        let answer = "172.17.0.1      host.docker.internal\n";
        assert_eq!(containers_address(answer, "img", |_| Ok(())).unwrap(), Some(gateway));
        let elsewhere = containers_address(answer, "img", |_| Err(std::io::ErrorKind::AddrNotAvailable.into())).unwrap();
        assert_eq!(elsewhere, None);
        let denied = containers_address(answer, "img", |_| Err(std::io::ErrorKind::PermissionDenied.into())).unwrap_err();
        assert!(denied.to_string().contains("cannot listen there"), "{denied}");
        for none in ["", "host.docker.internal"] {
            let refused = containers_address(none, "img", |_| Ok(())).unwrap_err();
            assert!(refused.to_string().contains("--entrypoint getent img hosts"), "{refused}");
        }
    }

    #[tokio::test]
    async fn a_container_already_going_counts_as_removed() {
        let docker = fake::FakeDocker::new();
        docker.fail(&["rm"], "Error response from daemon: removal of container weft-u-pg-1 is already in progress");
        remove_containers(&docker, &["weft-u-pg-1".into()]).await.unwrap();
    }

    #[test]
    fn a_ps_line_is_read_with_its_labels() {
        let row = ContainerRow::parse(r#"{"Names":"weft-u-pg-1","State":"running","Labels":"weft.project=p,weft.role=infra"}"#).unwrap();
        assert_eq!(row.name, "weft-u-pg-1");
        assert_eq!(row.state, "running");
        assert_eq!(row.label(labels::ROLE), Some(roles::INFRA));
        assert!(ContainerRow::parse("not json").is_err());
    }

    #[test]
    fn a_start_time_is_read_from_inspect_or_refused() {
        let at = parse_started_at("c-pg", "\"2024-01-01T00:00:05.123456789Z\"\n").unwrap();
        assert_eq!(at, chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:05.123456789Z").unwrap().to_utc());
        assert!(parse_started_at("c-pg", "\"yesterday\"").is_err());
        assert!(parse_started_at("c-pg", "").is_err());
    }

    fn at(rfc3339: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(rfc3339).unwrap().to_utc()
    }

    #[test]
    fn a_containers_stop_is_read_from_inspect() {
        let line = r#"{"name":"/weft-u-pg-1","restartCount":3,"state":{"Status":"restarting","OOMKilled":true,"ExitCode":137,"StartedAt":"2026-10-27T12:20:03.159420394Z","FinishedAt":"2026-10-27T12:20:06.351466883Z"}}"#;
        let stop = parse_container_stop(line).unwrap();
        assert_eq!(stop.name, "weft-u-pg-1");
        assert_eq!((stop.restart_count, stop.status.as_str(), stop.oom_killed, stop.exit_code), (3, "restarting", true, 137));
        assert_eq!(stop.finished_at, Some(at("2026-10-27T12:20:06.351466883Z")));
        assert_eq!(stop.started_at, Some(at("2026-10-27T12:20:03.159420394Z")));
        let never = r#"{"name":"/c","restartCount":0,"state":{"Status":"created","OOMKilled":false,"ExitCode":0,"StartedAt":"0001-01-01T00:00:00Z","FinishedAt":"0001-01-01T00:00:00Z"}}"#;
        let never = parse_container_stop(never).unwrap();
        assert_eq!((never.finished_at, never.started_at), (None, None), "Docker's zero time is never");
        assert!(parse_container_stop("not json").is_err());
    }

    #[tokio::test]
    async fn a_container_gone_since_it_was_listed_is_left_out_of_its_stops() {
        let docker = fake::FakeDocker::new();
        docker.respond(
            &["inspect"],
            DockerOutput {
                code: 1,
                stdout: r#"{"name":"/a","restartCount":0,"state":{"Status":"running","OOMKilled":false,"ExitCode":0,"StartedAt":"2026-10-27T12:20:03Z","FinishedAt":"0001-01-01T00:00:00Z"}}"#.into(),
                stderr: "Error: No such object: b\n".into(),
            },
        );
        let stops = container_stops(&docker, &["a".into(), "b".into()]).await.unwrap();
        assert_eq!(stops.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(), ["a"]);
        assert!(container_stops(&docker, &[]).await.unwrap().is_empty());
        assert_eq!(docker.calls_to("inspect").len(), 1, "no containers, no call");

        let broken = fake::FakeDocker::new();
        broken.fail(&["inspect"], "Cannot connect to the Docker daemon");
        assert!(container_stops(&broken, &["a".into()]).await.is_err());
        let silent = fake::FakeDocker::new();
        silent.fail(&["inspect"], "");
        assert!(container_stops(&silent, &["a".into()]).await.is_err(), "a failure that says nothing is not a container gone");
    }

    /// A stop's events are read up to the daemon's now at the latest:
    /// `docker events` asked about a time ahead waits for it.
    #[tokio::test]
    async fn a_stops_events_are_read_no_later_than_the_daemons_now() {
        let docker = fake::FakeDocker::new();
        docker.answer(&["info"], "\"2026-10-27T12:20:08.25Z\"\n");
        stop_cause(&docker, "c", at("2026-10-27T12:20:06Z")).await.unwrap();
        let call = &docker.calls_to("events")[0];
        let until = call.iter().position(|a| a == "--until").unwrap();
        assert_eq!(call[until + 1], "1793103608.250000000", "{call:?}");
        stop_cause(&docker, "c", at("2026-10-27T12:00:00Z")).await.unwrap();
        let call = &docker.calls_to("events")[1];
        assert_eq!(call[call.iter().position(|a| a == "--until").unwrap() + 1], "1793102410.000000000", "ten seconds after an old stop");
    }

    #[test]
    fn oom_kills_are_read_with_their_container() {
        assert!(read_oom_kills("").unwrap().is_empty());
        let two = "1793118867034183780 c-a\n1793118869105466292 c-b\n";
        let at = chrono::DateTime::from_timestamp_nanos;
        assert_eq!(read_oom_kills(two).unwrap(), [("c-a".to_string(), at(1793118867034183780)), ("c-b".to_string(), at(1793118869105466292))]);
        assert!(read_oom_kills("1793118867034183780\n").is_err());
        assert!(read_oom_kills("oom c-a\n").is_err());
    }

    /// One read covers every container asked about, and none is made
    /// when there is nothing to ask.
    #[tokio::test]
    async fn oom_kills_of_several_containers_are_one_read() {
        let docker = fake::FakeDocker::new();
        let at = |s: i64| chrono::DateTime::from_timestamp(s, 0).unwrap();
        oom_kills(&docker, &[], at(1), at(2)).await.unwrap();
        oom_kills(&docker, &["c-a"], at(3), at(2)).await.unwrap();
        assert!(docker.calls_to("events").is_empty());
        oom_kills(&docker, &["c-a", "c-b"], at(1), at(2)).await.unwrap();
        let calls = docker.calls_to("events");
        assert_eq!(calls.len(), 1);
        assert!(calls[0].contains(&"container=c-a".to_string()) && calls[0].contains(&"container=c-b".to_string()));
    }

    /// The `oom` lands between the stop and its `die`: the die nearest the
    /// stop is the one, and an `oom` before the die ahead of it belongs to
    /// an older stop.
    #[test]
    fn a_stops_cause_is_the_die_nearest_it_and_the_oom_before_that_die() {
        use weft_platform_traits::RestartCause;
        let stopped = at("2026-10-27T12:19:59.488814837Z");
        let oom_then_die = "1793103599495662458 oom \n1793103599742702568 die 137\n";
        assert_eq!(read_stop_cause(oom_then_die, stopped).unwrap(), RestartCause::OutOfMemory);
        assert_eq!(read_stop_cause("1793103599742702568 die 3\n", stopped).unwrap(), RestartCause::Exited { code: 3 });
        let older_oom = "1793103596000000000 oom \n1793103596200000000 die 137\n1793103599742702568 die 1\n";
        assert_eq!(read_stop_cause(older_oom, stopped).unwrap(), RestartCause::Exited { code: 1 });
        // A process inside it killed for memory earlier in the run, then
        // a crash of its own: the crash is the cause.
        let killed_inside_then_crashed = "1793103597000000000 oom \n1793103599742702568 die 1\n";
        assert_eq!(read_stop_cause(killed_inside_then_crashed, stopped).unwrap(), RestartCause::Exited { code: 1 });
        assert_eq!(read_stop_cause("", stopped).unwrap(), RestartCause::Unknown, "Docker forgot it");
        assert!(read_stop_cause("1793103599742702568 die\n", stopped).is_err());
        assert!(read_stop_cause("1793103599742702568 start\n", stopped).is_err());
    }

    #[test]
    fn labels_and_filters_are_spelled_as_docker_takes_them() {
        let mut l = BTreeMap::new();
        l.insert(labels::PROJECT, "p".to_string());
        assert_eq!(label_args(&l), vec!["--label", "weft.project=p"]);
        assert_eq!(filter_args(&[(labels::ROLE, "infra")]), vec!["--filter", "label=weft.role=infra"]);
    }
}
