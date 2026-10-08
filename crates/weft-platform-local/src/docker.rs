//! The local Docker daemon, reached through the `docker` command.
//!
//! Every local implementation (workers, infra, images) talks to Docker
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
    /// On a worker container: which life of the process that started it
    /// (`LocalRunner`), so a restarted process tells its own workers from
    /// the ones an earlier life left running.
    pub const LIFE: &str = "weft.life";
}

/// The values of [`labels::ROLE`]: what a container is for.
pub mod roles {
    /// Serves a project's program.
    pub const WORKER: &str = "worker";
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
/// or one Docker is already removing (a worker the idle sweep is taking
/// down), is not an error.
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

/// Stop the named containers the way a platform replaces a process: told
/// to stop (`SIGTERM`), given `grace_secs` to exit on their own, killed
/// after that. One already gone is no error.
pub async fn stop(docker: &dyn Docker, names: &[String], grace_secs: u64) -> anyhow::Result<()> {
    if names.is_empty() {
        return Ok(());
    }
    let mut args = vec!["stop".to_string(), "--time".into(), grace_secs.to_string()];
    args.extend(names.iter().cloned());
    let out = docker.exec(&args).await?;
    if out.success() || out.stderr.contains("No such container") {
        Ok(())
    } else {
        out.ok(&args).map(|_| ())
    }
}

/// Tell the named running containers to stop, as a platform tells a
/// process it is about to replace (`SIGTERM`), and leave them to exit in
/// their own time: a worker hands its durable runs back and lets its fast
/// runs end first. One already gone, or already stopped, is not an error.
pub async fn ask_to_stop(docker: &dyn Docker, names: &[String]) -> anyhow::Result<()> {
    if names.is_empty() {
        return Ok(());
    }
    let mut args = vec!["kill".to_string(), "--signal".into(), "TERM".into()];
    args.extend(names.iter().cloned());
    let out = docker.exec(&args).await?;
    if out.success() || out.stderr.contains("No such container") || out.stderr.contains("is not running") {
        Ok(())
    } else {
        out.ok(&args).map(|_| ())
    }
}

/// `--add-host` naming the machine `host.docker.internal`: what a container
/// that calls weft's internal port (a worker, the agent beside an infra
/// unit) reaches it under. Docker Desktop knows the name on its own; a
/// Linux engine only through this.
pub fn host_gateway_args() -> [String; 2] {
    ["--add-host".into(), "host.docker.internal:host-gateway".into()]
}

/// The network every container weft starts joins, so a worker reaches an
/// infra container by name and an infra container reaches another.
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

    #[tokio::test]
    async fn a_container_already_going_counts_as_removed() {
        let docker = fake::FakeDocker::new();
        docker.fail(&["rm"], "Error response from daemon: removal of container weft-w-1 is already in progress");
        remove_containers(&docker, &["weft-w-1".into()]).await.unwrap();
    }

    #[test]
    fn a_ps_line_is_read_with_its_labels() {
        let row = ContainerRow::parse(r#"{"Names":"weft-w-1","State":"running","Labels":"weft.project=p,weft.role=worker"}"#).unwrap();
        assert_eq!(row.name, "weft-w-1");
        assert_eq!(row.state, "running");
        assert_eq!(row.label(labels::ROLE), Some("worker"));
        assert!(ContainerRow::parse("not json").is_err());
    }

    #[test]
    fn labels_and_filters_are_spelled_as_docker_takes_them() {
        let mut l = BTreeMap::new();
        l.insert(labels::PROJECT, "p".to_string());
        assert_eq!(label_args(&l), vec!["--label", "weft.project=p"]);
        assert_eq!(filter_args(&[(labels::ROLE, "infra")]), vec!["--filter", "label=weft.role=infra"]);
    }
}
