//! Infra units on the local Docker daemon.
//!
//! A unit is a small group of containers sharing one network, the way
//! they would share one machine. The network belongs to the unit's agent
//! (`weft-runtime unit-agent`, see `weft_platform_traits::unit_agent`),
//! which starts first and holds it; every container of the unit joins it,
//! so they reach each other on `127.0.0.1`, and the agent answers the
//! unit's readiness checks from inside it. The agent's container is on
//! the install's Docker network under the unit's name, which is how a
//! worker or another unit reaches the unit's ports.
//!
//! Disks are Docker volumes named after the copy, kept across stop and
//! upgrade and removed by terminate (except those the node keeps on
//! terminate, which carry the copy's labels, so the copy still shows in
//! `copies` until the sweep of copies gone for good deletes them). Starting a unit that is not running
//! builds it again from its spec (containers are disposable; only disks
//! carry state), which is also what empties its scratch space.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use weft_core::infra::{
    Container, EndpointTarget, Expose, Image, NodeRef, ProbeKind, ResolvedNode, ResolvedUnit, VolumeKind,
};
use weft_core::infra::wire::{LogLine, LogMark, LogStream, LogsFrom, Pipe};
use weft_platform_traits::unit_agent::{ProbeAnswer, ProbeRequest, PROBE_PATH};
use weft_core::ports::UNIT_AGENT;
use weft_platform_traits::{EndpointAt, InfraHost, UnitObservation, UnitRunState};

use crate::docker::{self, labels, roles, ContainerRow, Docker};

/// Which part of a unit a container is.
const PART: &str = "weft.part";
const PART_AGENT: &str = "agent";
const PART_APP: &str = "app";
/// On a volume: its name in the spec, and whether it is a disk.
const VOLUME: &str = "weft.volume";
const VOLUME_KIND: &str = "weft.volume-kind";

#[derive(Clone)]
pub struct LocalInfraHostConfig {
    /// The image the unit agent runs from (the install's `weft-runtime`).
    pub agent_image: String,
    /// Where an environment file is written for the moment it takes to
    /// start a container (values stay off the command line).
    pub scratch_dir: PathBuf,
    /// How this machine hands a container its GPUs, if it has any.
    pub gpu: GpuAccess,
    /// What backs a unit's disks.
    pub disks: DiskBacking,
    /// Which of a unit's ports are published on the machine.
    pub publish: Publish,
    /// The install these units belong to.
    pub install: weft_core::infra::Install,
}

/// How a container reaches the machine's GPUs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GpuAccess {
    /// The machine has none.
    None,
    /// Docker's NVIDIA runtime (`--gpus all`).
    DockerGpus,
    /// The driver Container-Optimized OS installs (`cos-extensions install
    /// gpu`): its libraries and device nodes are handed in by hand.
    CosDriver,
}

/// What a unit's disks are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiskBacking {
    /// Docker volumes named after the copy (a laptop).
    Volumes,
    /// Directories under this one, one per disk, each a disk the machine
    /// mounted there (a cloud machine with persistent disks attached).
    MountedUnder(PathBuf),
}

/// Which ports of a unit the machine publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Publish {
    /// Every port, on loopback at a port Docker picks (a laptop): weft's
    /// runtime is a process on the machine, off the Docker network the
    /// workers reach the unit on, and it reaches the unit here. Loopback
    /// is the machine's own, which a local install trusts whole (its
    /// management API answers there without a key), so this opens
    /// nothing a local process could not already do.
    Loopback,
    /// Every port of every container, on every interface at the same
    /// number (a cloud machine running one unit: the workers and the
    /// install's network reach the unit at the machine's address, and the
    /// network's firewall keeps the internet out).
    AllPorts,
}

pub struct LocalInfraHost {
    docker: Arc<dyn Docker>,
    http: reqwest::Client,
    cfg: LocalInfraHostConfig,
    /// Consecutive failed liveness checks, by container name.
    liveness_failures: parking_lot::Mutex<HashMap<String, u32>>,
}

impl LocalInfraHost {
    pub fn new(docker: Arc<dyn Docker>, cfg: LocalInfraHostConfig) -> Self {
        Self {
            docker,
            http: reqwest::Client::builder().build().expect("reqwest client"),
            cfg,
            liveness_failures: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// `DockerGpus` when this machine's Docker offers the NVIDIA runtime,
    /// `None` otherwise.
    pub async fn detect_gpu(docker: &dyn Docker) -> anyhow::Result<GpuAccess> {
        let out = docker::run(docker, vec!["info".into(), "--format".into(), "{{json .Runtimes}}".into()]).await?;
        Ok(if out.contains("\"nvidia\"") { GpuAccess::DockerGpus } else { GpuAccess::None })
    }

    fn unit<'a>(node: &'a ResolvedNode, unit: &str) -> anyhow::Result<&'a ResolvedUnit> {
        node.unit(unit).ok_or_else(|| anyhow::anyhow!("node '{}' declares no unit '{unit}'", node.node.node))
    }

    async fn unit_containers(&self, node: &NodeRef, unit: &str) -> anyhow::Result<Vec<ContainerRow>> {
        docker::containers(
            self.docker.as_ref(),
            self.cfg.install.label_value(),
            &[(labels::ROLE, roles::INFRA), (labels::COPY, &node.copy_id), (labels::UNIT, unit)],
        )
        .await
    }

    async fn remove_unit_containers(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
        let names: Vec<String> = self.unit_containers(node, unit).await?.into_iter().map(|c| c.name).collect();
        docker::remove_containers(self.docker.as_ref(), &names).await
    }

    async fn volumes(&self, filter: &[(&str, &str)]) -> anyhow::Result<Vec<(String, BTreeMap<String, String>)>> {
        let mut args = vec!["volume".to_string(), "ls".into(), "--format".into(), "{{json .}}".into()];
        args.extend(docker::filter_args(&[(labels::INSTALL, self.cfg.install.label_value())]));
        args.extend(docker::filter_args(filter));
        let out = docker::run(self.docker.as_ref(), args).await?;
        out.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                let row = ContainerRow::parse(l)?;
                let v: serde_json::Value = serde_json::from_str(l)?;
                let name = v.get("Name").and_then(|n| n.as_str()).unwrap_or_default().to_string();
                Ok((name, row.labels))
            })
            .collect()
    }

    async fn remove_volumes(&self, names: &[String]) -> anyhow::Result<()> {
        if names.is_empty() {
            return Ok(());
        }
        let mut args = vec!["volume".to_string(), "rm".into(), "--force".into()];
        args.extend(names.iter().cloned());
        docker::run(self.docker.as_ref(), args).await.map(|_| ())
    }

    /// Build the unit from its spec: disks, fresh scratch space, the
    /// agent, the init containers one after another, then the containers.
    async fn create(&self, node: &ResolvedNode, unit: &ResolvedUnit) -> anyhow::Result<()> {
        let r = &node.node;
        let name = &unit.unit.name;
        docker::ensure_network(self.docker.as_ref()).await?;
        self.remove_unit_containers(r, name).await?;

        let mounted = mounted_volumes(unit);
        let mut scratch = Vec::new();
        for v in node.spec.volumes.iter().filter(|v| mounted.contains(v.name.as_str())) {
            match &v.kind {
                VolumeKind::Disk { .. } => match &self.cfg.disks {
                    DiskBacking::Volumes => {
                        docker::run(self.docker.as_ref(), volume_create_args(&self.cfg.install, r, name, &v.name, true)).await?;
                    }
                    DiskBacking::MountedUnder(root) => {
                        let at = root.join(&v.name);
                        anyhow::ensure!(at.is_dir(), "the disk '{}' of '{}' is not mounted at {}", v.name, r.node, at.display());
                    }
                },
                VolumeKind::Scratch { .. } => scratch.push(scratch_volume(r, name, &v.name)),
            }
        }
        self.remove_volumes(&scratch).await?;
        for v in node.spec.volumes.iter().filter(|v| mounted.contains(v.name.as_str())) {
            if matches!(v.kind, VolumeKind::Scratch { .. }) {
                docker::run(self.docker.as_ref(), volume_create_args(&self.cfg.install, r, name, &v.name, false)).await?;
            }
        }
        if let Some(gid) = unit.unit.fs_group {
            let owned: Vec<String> = mounted.iter().map(|v| volume_name(node, name, v, &self.cfg.disks)).collect();
            docker::run(self.docker.as_ref(), own_args(&self.cfg.agent_image, gid, &owned)).await?;
        }

        docker::run(self.docker.as_ref(), agent_args(node, unit, &self.cfg)).await?;
        for c in &unit.unit.init_containers {
            let env_file = self.env_file(r, name, c)?;
            let args = container_args(node, unit, c, &env_file, ContainerRun::Init, &self.cfg);
            let out = self.docker.exec(&args).await;
            let _ = std::fs::remove_file(&env_file);
            let out = out?;
            if !out.success() {
                anyhow::bail!(
                    "unit '{name}' of '{}': init container '{}' failed ({}):\n{}{}",
                    r.node,
                    c.name,
                    out.code,
                    out.stdout,
                    out.stderr
                );
            }
        }
        for c in &unit.unit.containers {
            let env_file = self.env_file(r, name, c)?;
            let started = docker::run(self.docker.as_ref(), container_args(node, unit, c, &env_file, ContainerRun::App, &self.cfg)).await;
            let _ = std::fs::remove_file(&env_file);
            started.map_err(|e| e.context(format!("start container '{}' of unit '{name}' of '{}'", c.name, r.node)))?;
        }
        Ok(())
    }

    fn env_file(&self, node: &NodeRef, unit: &str, c: &Container) -> anyhow::Result<PathBuf> {
        use std::io::Write as _;
        std::fs::create_dir_all(&self.cfg.scratch_dir)?;
        let path = self.cfg.scratch_dir.join(format!("{}.env", container_name(node, unit, &c.name)));
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
        let mut file = opts.open(&path).map_err(|e| anyhow::anyhow!("write {}: {e}", path.display()))?;
        for e in &c.env {
            anyhow::ensure!(
                !e.value.contains('\n'),
                "the environment variable {} of container '{}' holds a line break, which Docker cannot pass",
                e.name,
                c.name
            );
            writeln!(file, "{}={}", e.name, e.value)?;
        }
        Ok(path)
    }

    /// The agent's address, published on loopback.
    async fn agent_url(&self, agent: &str) -> anyhow::Result<String> {
        let printed = docker::run(self.docker.as_ref(), vec!["port".into(), agent.into(), format!("{UNIT_AGENT}/tcp")]).await?;
        loopback(&printed)
            .map(|a| format!("http://{a}"))
            .ok_or_else(|| anyhow::anyhow!("the agent {agent} publishes no loopback port: {printed:?}"))
    }

    /// Run one check of `container`: an exec inside it, or a network
    /// check through the unit's agent.
    async fn probe(&self, agent_url: &str, container: &str, probe: &weft_core::infra::Probe) -> anyhow::Result<ProbeAnswer> {
        match &probe.kind {
            ProbeKind::Exec { command } => {
                let mut args = vec!["exec".to_string(), container.to_string()];
                args.extend(command.iter().cloned());
                let out = self.docker.exec(&args).await?;
                Ok(ProbeAnswer {
                    ok: out.success(),
                    why: (!out.success()).then(|| format!("`{}` exited {}", command.join(" "), out.code)),
                })
            }
            kind => {
                let req = ProbeRequest { probe: kind.clone(), timeout_seconds: probe.timeout_seconds };
                let resp = self
                    .http
                    .post(format!("{agent_url}{PROBE_PATH}"))
                    .timeout(Duration::from_secs(u64::from(probe.timeout_seconds) + 5))
                    .json(&req)
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.json().await?)
            }
        }
    }

    /// How one unit is doing; `probes` are its containers' checks, by
    /// container.
    async fn unit_state(&self, probes: &HashMap<String, Probes>, rows: &[ContainerRow]) -> anyhow::Result<UnitRunState> {
        let agent = rows.iter().find(|c| c.label(PART) == Some(PART_AGENT));
        let apps: Vec<&ContainerRow> = rows.iter().filter(|c| c.label(PART) == Some(PART_APP)).collect();
        let Some(agent) = agent else {
            return Ok(UnitRunState::Failed { why: "its agent container is gone".into() });
        };
        if agent.state != "running" {
            return Ok(UnitRunState::Stopped);
        }
        if let Some(c) = apps.iter().find(|c| c.state == "restarting") {
            return Ok(UnitRunState::NotReady { why: format!("container {} keeps restarting", container_short(c)) });
        }
        if let Some(c) = apps.iter().find(|c| c.state != "running") {
            return Ok(match c.state.as_str() {
                "created" => UnitRunState::Starting,
                other => UnitRunState::Failed { why: format!("container {} is {other}", container_short(c)) },
            });
        }
        let agent_url = self.agent_url(&agent.name).await?;
        for c in &apps {
            let short = container_short(c);
            let Some((readiness, liveness)) = probes.get(short) else { continue };
            if let Some(live) = liveness {
                let answer = self.probe(&agent_url, &c.name, live).await?;
                let restart = {
                    let mut failures = self.liveness_failures.lock();
                    let n = failures.entry(c.name.clone()).or_insert(0);
                    *n = if answer.ok { 0 } else { *n + 1 };
                    let due = *n >= live.failure_threshold.max(1);
                    if due {
                        *n = 0;
                    }
                    due
                };
                if restart {
                    tracing::warn!(target: "weft_platform_local::infra", container = %c.name, "liveness check keeps failing; restarting it");
                    docker::run(self.docker.as_ref(), vec!["restart".into(), c.name.clone()]).await?;
                    return Ok(UnitRunState::NotReady { why: format!("container {short} failed its liveness check and was restarted") });
                }
            }
            if let Some(ready) = readiness {
                let answer = self.probe(&agent_url, &c.name, ready).await?;
                if !answer.ok {
                    return Ok(UnitRunState::NotReady {
                        why: format!("container {short}: {}", answer.why.unwrap_or_else(|| "not ready".into())),
                    });
                }
            }
        }
        Ok(UnitRunState::Ready)
    }
}

/// On an app container: its checks (`encode_probes`), so an observation
/// after a restart of this process still knows them.
const PROBES: &str = "weft.probes";

fn container_short(c: &ContainerRow) -> &str {
    c.label("weft.container").unwrap_or(&c.name)
}

/// `docker logs --timestamps` output, the container's stdout and stderr
/// merged back into the order they were written (docker hands them back
/// on its own two streams), each line tagged with its pipe. Every line
/// starts with its RFC 3339 instant; one that does not is a docker we do
/// not understand, and says so.
fn timestamped_lines(stdout: &str, stderr: &str) -> anyhow::Result<Vec<LogLine>> {
    let mut lines = Vec::new();
    for (pipe, raw) in stdout.lines().map(|l| (Pipe::Stdout, l)).chain(stderr.lines().map(|l| (Pipe::Stderr, l))) {
        let (at, text) = raw.split_once(' ').unwrap_or((raw, ""));
        let at = chrono::DateTime::parse_from_rfc3339(at)
            .map_err(|e| anyhow::anyhow!("docker logs printed a line with no timestamp ({e}): {raw}"))?;
        lines.push(LogLine { at: at.to_utc(), pipe, text: text.to_string() });
    }
    // Stable: lines of one stream keep their order at a shared instant.
    lines.sort_by_key(|l| l.at);
    Ok(lines)
}

/// The Docker daemon's own clock: the one `docker logs` stamps lines
/// with. A mark for a read that delivers nothing is taken here, never
/// from this process's clock, so a daemon running behind it (another
/// machine, a desktop VM) does not put fresh lines before the mark.
async fn daemon_now(docker: &dyn Docker) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let out = docker::run(docker, vec!["info".into(), "--format".into(), "{{json .SystemTime}}".into()]).await?;
    let at: String = serde_json::from_str(out.trim())
        .map_err(|e| anyhow::anyhow!("`docker info` printed no system time ({e}): {}", out.trim()))?;
    Ok(chrono::DateTime::parse_from_rfc3339(&at)
        .map_err(|e| anyhow::anyhow!("`docker info` printed a system time that is not RFC 3339 ({e}): {at}"))?
        .to_utc())
}

#[async_trait]
impl InfraHost for LocalInfraHost {
    fn check(&self, node: &ResolvedNode) -> Result<(), String> {
        for u in &node.units {
            if u.unit.machine.gpu.is_some() && self.cfg.gpu == GpuAccess::None {
                return Err(format!(
                    "unit '{}' of '{}' asks for a GPU, and this machine's Docker has none (no NVIDIA runtime); \
                     install the NVIDIA container toolkit and restart weft (`weft daemon start`; weft \
                     looks for the runtime when it starts), or remove the unit's machine.gpu",
                    u.unit.name, node.node.node
                ));
            }
        }
        Ok(())
    }

    fn notes(&self, node: &ResolvedNode) -> Vec<String> {
        // Docker's NVIDIA runtime cannot pick a GPU by kind, so a unit
        // asking for one gets every GPU the machine has (`--gpus all`,
        // see `container_args`). A Container-Optimized OS machine was
        // made with the kind asked, so it has nothing to say.
        if self.cfg.gpu != GpuAccess::DockerGpus {
            return Vec::new();
        }
        node.units
            .iter()
            .filter_map(|u| u.unit.machine.gpu.as_ref().map(|gpu| (u, gpu)))
            .map(|(u, gpu)| {
                format!(
                    "unit '{}' of '{}' asked for {} x {}; a local install cannot choose a GPU by kind, \
                     so it hands the container every GPU on this machine",
                    u.unit.name, node.node.node, gpu.count, gpu.kind
                )
            })
            .collect()
    }

    async fn apply_unit(&self, node: &ResolvedNode, unit: &str) -> anyhow::Result<()> {
        let resolved = Self::unit(node, unit)?;
        let rows = self.unit_containers(&node.node, unit).await?;
        let expected = resolved.unit.containers.len() + 1;
        let current = rows.len() == expected
            && rows.iter().all(|c| c.label(labels::UNIT_HASH) == Some(resolved.hash.as_str()) && c.state == "running");
        if current {
            return Ok(());
        }
        self.create(node, resolved).await
    }

    async fn stop_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
        let rows = self.unit_containers(node, unit).await?;
        let mut order: Vec<String> = rows.iter().filter(|c| c.label(PART) == Some(PART_APP)).map(|c| c.name.clone()).collect();
        order.extend(rows.iter().filter(|c| c.label(PART) == Some(PART_AGENT)).map(|c| c.name.clone()));
        if order.is_empty() {
            return Ok(());
        }
        let mut args = vec!["stop".to_string()];
        args.extend(order);
        docker::run(self.docker.as_ref(), args).await.map(|_| ())
    }

    async fn restart_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
        let rows = self.unit_containers(node, unit).await?;
        let agent_running = rows.iter().any(|c| c.label(PART) == Some(PART_AGENT) && c.state == "running");
        anyhow::ensure!(
            agent_running,
            "unit '{unit}' of '{}' is not running; a restart restarts a running unit, and starting one is an apply",
            node.node
        );
        let apps: Vec<String> = rows.iter().filter(|c| c.label(PART) == Some(PART_APP)).map(|c| c.name.clone()).collect();
        let mut args = vec!["restart".to_string()];
        args.extend(apps);
        docker::run(self.docker.as_ref(), args).await.map(|_| ())
    }

    async fn remove_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
        self.remove_unit_containers(node, unit).await?;
        let scratch: Vec<String> = self
            .volumes(&[(labels::COPY, &node.copy_id), (labels::UNIT, unit), (VOLUME_KIND, "scratch")])
            .await?
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        self.remove_volumes(&scratch).await
    }

    async fn terminate(&self, node: &NodeRef, keep_disks: &[String]) -> anyhow::Result<()> {
        let names: Vec<String> =
            docker::containers(self.docker.as_ref(), self.cfg.install.label_value(), &[(labels::ROLE, roles::INFRA), (labels::COPY, &node.copy_id)])
                .await?
                .into_iter()
                .map(|c| c.name)
                .collect();
        docker::remove_containers(self.docker.as_ref(), &names).await?;
        let gone: Vec<String> = self
            .volumes(&[(labels::COPY, &node.copy_id)])
            .await?
            .into_iter()
            .filter(|(_, l)| !(l.get(VOLUME_KIND).map(String::as_str) == Some("disk") && l.get(VOLUME).is_some_and(|v| keep_disks.contains(v))))
            .map(|(name, _)| name)
            .collect();
        self.remove_volumes(&gone).await
    }

    async fn observe(&self, _tenant: &str, project: uuid::Uuid) -> anyhow::Result<Vec<UnitObservation>> {
        let project_label = project.to_string();
        let rows = docker::containers(self.docker.as_ref(), self.cfg.install.label_value(), &[(labels::ROLE, roles::INFRA), (labels::PROJECT, &project_label)]).await?;
        let mut by_unit: BTreeMap<(String, String), Vec<ContainerRow>> = BTreeMap::new();
        for r in rows {
            let (Some(c), Some(u)) = (r.label(labels::COPY), r.label(labels::UNIT)) else { continue };
            by_unit.entry((c.to_string(), u.to_string())).or_default().push(r);
        }
        let mut out = Vec::with_capacity(by_unit.len());
        for ((copy_id, unit), rows) in by_unit {
            let hash = rows.iter().find_map(|r| r.label(labels::UNIT_HASH)).unwrap_or_default().to_string();
            let mut probes = HashMap::new();
            for r in &rows {
                if let Some(raw) = r.label(PROBES) {
                    probes.insert(container_short(r).to_string(), decode_probes(raw)?);
                }
            }
            let state = self.unit_state(&probes, &rows).await?;
            out.push(UnitObservation { copy_id, unit, hash, state });
        }
        Ok(out)
    }

    async fn copies(&self) -> anyhow::Result<Vec<NodeRef>> {
        // A running or stopped copy shows by its agents; a terminated one
        // that kept disks shows only by those disks.
        let agents = docker::containers(self.docker.as_ref(), self.cfg.install.label_value(), &[(labels::ROLE, roles::INFRA), (PART, PART_AGENT)])
            .await?
            .into_iter()
            .map(|r| r.labels);
        let disks = self.volumes(&[(labels::ROLE, roles::INFRA), (VOLUME_KIND, "disk")]).await?.into_iter().map(|(_, l)| l);
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for l in agents.chain(disks) {
            let (Some(tenant), Some(project), Some(node), Some(copy_id)) = (
                l.get(labels::TENANT),
                l.get(labels::PROJECT).and_then(|p| p.parse::<uuid::Uuid>().ok()),
                l.get(labels::NODE),
                l.get(labels::COPY),
            ) else {
                continue;
            };
            if seen.insert(copy_id.clone()) {
                out.push(NodeRef { tenant: tenant.clone(), project, node: node.clone(), copy_id: copy_id.clone() });
            }
        }
        Ok(out)
    }

    async fn endpoint(&self, node: &ResolvedNode, endpoint: &str) -> anyhow::Result<EndpointAt> {
        let ep = node
            .spec
            .endpoints
            .iter()
            .find(|e| e.name == endpoint)
            .ok_or_else(|| anyhow::anyhow!("node '{}' declares no endpoint '{endpoint}'", node.node.node))?;
        match &ep.target {
            EndpointTarget::External { url } => Ok(EndpointAt {
                url: url.clone(),
                install_url: url.clone(),
                same_network: matches!(ep.expose, Expose::SameNetwork).then(|| host_port(url).to_string()),
            }),
            EndpointTarget::Unit { unit, container, port } => {
                let port = port_number(node, unit, container, port)?;
                let agent = agent_name(&node.node, unit);
                let url = format!("http://{agent}:{port}");
                let on_machine = match self.cfg.publish {
                    Publish::Loopback => {
                        let printed = docker::run(self.docker.as_ref(), vec!["port".into(), agent.clone(), format!("{port}/tcp")]).await?;
                        loopback(&printed)
                            .ok_or_else(|| anyhow::anyhow!("{agent} publishes no loopback port for {port}: {printed:?}"))?
                            .to_string()
                    }
                    Publish::AllPorts => format!("{agent}:{port}"),
                };
                let install_url = match self.cfg.publish {
                    Publish::Loopback => format!("http://{on_machine}"),
                    Publish::AllPorts => url.clone(),
                };
                let same_network = matches!(ep.expose, Expose::SameNetwork).then_some(on_machine);
                Ok(EndpointAt { url, install_url, same_network })
            }
        }
    }

    async fn logs(&self, node: &NodeRef, unit: &str, from: &LogsFrom) -> anyhow::Result<Vec<LogStream>> {
        let rows = self.unit_containers(node, unit).await?;
        let mut out = Vec::new();
        for r in rows.iter().filter(|c| c.label(PART) == Some(PART_APP)) {
            let source = container_short(r).to_string();
            let mark = match from {
                LogsFrom::Tail(_) => None,
                LogsFrom::After(marks) => marks.get(&source).copied(),
            };
            // Taken before the read, on the daemon's clock: a line written
            // while the read runs is at or after it, so an empty read's
            // mark misses none. A marked container resumes from its mark.
            let start = match mark {
                Some(mark) => mark,
                None => LogMark::start(daemon_now(self.docker.as_ref()).await?),
            };
            let mut args = vec!["logs".to_string(), "--timestamps".into()];
            match (from, &mark) {
                (LogsFrom::Tail(n), _) => args.extend(["--tail".into(), n.to_string()]),
                // Inclusive of the mark's instant: `LogMark::unseen` drops
                // what was already delivered there.
                (LogsFrom::After(_), Some(mark)) => {
                    args.extend(["--since".into(), mark.at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)])
                }
                // No mark: a container that appeared since the follow
                // started (every read marks each stream it returned), so
                // all it wrote is new.
                (LogsFrom::After(_), None) => {}
            }
            args.push(r.name.clone());
            let logs = self.docker.exec(&args).await?;
            if !logs.success() {
                return Err(logs.ok(&args).expect_err("a call that did not succeed is an error"));
            }
            let lines = timestamped_lines(&logs.stdout, &logs.stderr)?;
            let lines = match mark {
                Some(mark) => mark.unseen(lines),
                None => lines,
            };
            let mark = start.advance(&lines);
            out.push(LogStream { source, lines, mark });
        }
        Ok(out)
    }
}

// ---------- names ----------

/// The unit's agent container: also the unit's name on the network.
pub fn agent_name(node: &NodeRef, unit: &str) -> String {
    format!("{}-{unit}", node.resource_base())
}

pub fn container_name(node: &NodeRef, unit: &str, container: &str) -> String {
    format!("{}-{unit}-{container}", node.resource_base())
}

fn disk_volume(node: &NodeRef, volume: &str) -> String {
    format!("{}-{volume}", node.resource_base())
}

fn scratch_volume(node: &NodeRef, unit: &str, volume: &str) -> String {
    format!("{}-{unit}-{volume}", node.resource_base())
}

/// What `docker --volume` names for `volume`: a Docker volume, or the
/// directory a mounted disk is at.
fn volume_name(node: &ResolvedNode, unit: &str, volume: &str, disks: &DiskBacking) -> String {
    let is_disk = node.spec.volumes.iter().any(|v| v.name == volume && matches!(v.kind, VolumeKind::Disk { .. }));
    match (is_disk, disks) {
        (true, DiskBacking::Volumes) => disk_volume(&node.node, volume),
        (true, DiskBacking::MountedUnder(root)) => root.join(volume).display().to_string(),
        (false, _) => scratch_volume(&node.node, unit, volume),
    }
}

// ---------- pure translators ----------

fn mounted_volumes(unit: &ResolvedUnit) -> BTreeSet<&str> {
    unit.unit
        .containers
        .iter()
        .chain(unit.unit.init_containers.iter())
        .flat_map(|c| c.mounts.iter().map(|m| m.volume.as_str()))
        .collect()
}

fn base_labels(install: &weft_core::infra::Install, node: &NodeRef, unit: &str) -> BTreeMap<&'static str, String> {
    let mut l = BTreeMap::new();
    l.insert(labels::INSTALL, install.label_value().to_string());
    l.insert(labels::ROLE, roles::INFRA.to_string());
    l.insert(labels::TENANT, node.tenant.clone());
    l.insert(labels::PROJECT, node.project.to_string());
    l.insert(labels::NODE, node.node.clone());
    l.insert(labels::COPY, node.copy_id.clone());
    l.insert(labels::UNIT, unit.to_string());
    l
}

fn volume_create_args(install: &weft_core::infra::Install, node: &NodeRef, unit: &str, volume: &str, disk: bool) -> Vec<String> {
    let mut l = base_labels(install, node, unit);
    l.insert(VOLUME, volume.to_string());
    l.insert(VOLUME_KIND, if disk { "disk" } else { "scratch" }.to_string());
    if disk {
        // A disk belongs to the copy, not to the unit that mounts it now.
        l.remove(labels::UNIT);
    }
    let name = if disk { disk_volume(node, volume) } else { scratch_volume(node, unit, volume) };
    let mut args = vec!["volume".to_string(), "create".into()];
    args.extend(docker::label_args(&l));
    args.push(name);
    args
}

/// Hand every mounted volume to group `gid` (a unit's `fsGroup`).
fn own_args(agent_image: &str, gid: u32, volumes: &[String]) -> Vec<String> {
    let mut args = vec!["run".to_string(), "--rm".into()];
    let mut paths = Vec::new();
    for (i, v) in volumes.iter().enumerate() {
        let path = format!("/weft-own/{i}");
        args.extend(["--volume".into(), format!("{v}:{path}")]);
        paths.push(path);
    }
    args.extend([agent_image.to_string(), "unit-agent".into(), "own".into(), gid.to_string()]);
    args.extend(paths);
    args
}

fn port_number(node: &ResolvedNode, unit: &str, container: &str, port: &str) -> anyhow::Result<u16> {
    node.unit(unit)
        .and_then(|u| u.unit.containers.iter().find(|c| c.name == container))
        .and_then(|c| c.ports.iter().find(|p| p.name == port))
        .map(|p| p.port)
        .ok_or_else(|| anyhow::anyhow!("node '{}': no port '{port}' on container '{container}' of unit '{unit}'", node.node.node))
}

/// Every port of every container of `unit`.
fn all_ports(unit: &ResolvedUnit) -> Vec<u16> {
    let mut ports: Vec<u16> = unit.unit.containers.iter().flat_map(|c| c.ports.iter().map(|p| p.port)).collect();
    ports.sort_unstable();
    ports.dedup();
    ports
}

/// `docker run` for the unit's agent: it owns the unit's network, joins
/// the install's under the unit's name, and publishes its own port on
/// loopback and the unit's ports as the machine publishes them.
fn agent_args(node: &ResolvedNode, unit: &ResolvedUnit, cfg: &LocalInfraHostConfig) -> Vec<String> {
    let name = agent_name(&node.node, &unit.unit.name);
    let mut l = base_labels(&cfg.install, &node.node, &unit.unit.name);
    l.insert(labels::UNIT_HASH, unit.hash.clone());
    l.insert(PART, PART_AGENT.to_string());
    let mut args = vec![
        "run".to_string(),
        "--detach".into(),
        "--name".into(),
        name.clone(),
        "--network".into(),
        docker::NETWORK.into(),
        "--network-alias".into(),
        name,
        "--restart".into(),
        "unless-stopped".into(),
        "--publish".into(),
        format!("127.0.0.1::{UNIT_AGENT}"),
    ];
    match cfg.publish {
        Publish::Loopback => {
            for port in all_ports(unit) {
                args.extend(["--publish".into(), format!("127.0.0.1::{port}")]);
            }
        }
        Publish::AllPorts => {
            for port in all_ports(unit) {
                args.extend(["--publish".into(), format!("{port}:{port}")]);
            }
        }
    }
    args.extend(docker::label_args(&l));
    args.extend([cfg.agent_image.clone(), "unit-agent".into(), "serve".into()]);
    args
}

enum ContainerRun {
    Init,
    App,
}

/// `docker run` for one container of the unit, on the agent's network.
fn container_args(
    node: &ResolvedNode,
    unit: &ResolvedUnit,
    c: &Container,
    env_file: &std::path::Path,
    run: ContainerRun,
    cfg: &LocalInfraHostConfig,
) -> Vec<String> {
    let unit_name = &unit.unit.name;
    let mut l = base_labels(&cfg.install, &node.node, unit_name);
    l.insert(labels::UNIT_HASH, unit.hash.clone());
    l.insert("weft.container", c.name.clone());
    let mut args = vec!["run".to_string()];
    match run {
        ContainerRun::Init => args.push("--rm".into()),
        ContainerRun::App => {
            l.insert(PART, PART_APP.to_string());
            if c.readiness.is_some() || c.liveness.is_some() {
                l.insert(PROBES, encode_probes(c.readiness.as_ref(), c.liveness.as_ref()));
            }
            args.extend(["--detach".into(), "--restart".into(), "unless-stopped".into()]);
        }
    }
    args.extend([
        "--name".into(),
        container_name(&node.node, unit_name, &c.name),
        "--network".into(),
        format!("container:{}", agent_name(&node.node, unit_name)),
        "--env-file".into(),
        env_file.display().to_string(),
    ]);
    if let Some(cpu) = c.limits.cpu.as_ref().or(unit.unit.machine.cpu.as_ref()) {
        args.extend(["--cpus".into(), crate::runner::docker_cpus(cpu)]);
    }
    if let Some(memory) = c.limits.memory.as_ref().or(unit.unit.machine.memory.as_ref()) {
        args.extend(["--memory".into(), crate::runner::docker_memory(memory)]);
    }
    if let Some(user) = &c.run_as {
        args.extend(["--user".into(), user.clone()]);
    }
    // The unit's shared group: every container is in it, whatever user
    // it runs as, so what one writes to a shared disk the others read.
    if let Some(gid) = unit.unit.fs_group {
        args.extend(["--group-add".into(), gid.to_string()]);
    }
    if let Some(gpu) = &unit.unit.machine.gpu {
        match cfg.gpu {
            GpuAccess::DockerGpus => args.extend(["--gpus".into(), "all".into()]),
            GpuAccess::CosDriver => {
                // As Container-Optimized OS documents it: the driver's
                // libraries and tools, and the device nodes.
                args.extend([
                    "--volume".into(),
                    "/var/lib/nvidia/lib64:/usr/local/nvidia/lib64".into(),
                    "--volume".into(),
                    "/var/lib/nvidia/bin:/usr/local/nvidia/bin".into(),
                    "--device".into(),
                    "/dev/nvidia-uvm:/dev/nvidia-uvm".into(),
                    "--device".into(),
                    "/dev/nvidiactl:/dev/nvidiactl".into(),
                ]);
                for i in 0..gpu.count {
                    args.extend(["--device".into(), format!("/dev/nvidia{i}:/dev/nvidia{i}")]);
                }
            }
            // Refused by `check` before anything starts.
            GpuAccess::None => {}
        }
    }
    for m in &c.mounts {
        args.extend(["--mount".into(), mount_arg(node, unit_name, m, &cfg.disks)]);
    }
    if let Some(first) = c.command.as_ref().and_then(|cmd| cmd.first()) {
        args.extend(["--entrypoint".into(), first.clone()]);
    }
    args.extend(docker::label_args(&l));
    let Image::Upstream { reference } = &c.image else {
        unreachable!("a resolved unit's images are all upstream refs (weft_core::infra::resolve)")
    };
    args.push(reference.clone());
    if let Some(cmd) = &c.command {
        args.extend(cmd.iter().skip(1).cloned());
    }
    args.extend(c.args.iter().cloned());
    args
}

/// `docker --mount` for one mount: a Docker volume (a sub-directory of it
/// through `volume-subpath`), or a mounted disk's directory.
fn mount_arg(node: &ResolvedNode, unit: &str, m: &weft_core::infra::Mount, disks: &DiskBacking) -> String {
    let source = volume_name(node, unit, &m.volume, disks);
    let is_bind = source.starts_with('/');
    let mut arg = match (is_bind, &m.sub_path) {
        (true, Some(sub)) => format!("type=bind,source={source}/{sub},target={}", m.path),
        (true, None) => format!("type=bind,source={source},target={}", m.path),
        (false, Some(sub)) => format!("type=volume,source={source},target={},volume-subpath={sub}", m.path),
        (false, None) => format!("type=volume,source={source},target={}", m.path),
    };
    if m.read_only {
        arg.push_str(",readonly");
    }
    arg
}

/// A container's readiness and liveness checks, as a label value.
fn encode_probes(readiness: Option<&weft_core::infra::Probe>, liveness: Option<&weft_core::infra::Probe>) -> String {
    // Hex: a label value cannot hold the commas `docker ps` separates
    // labels with.
    hex::encode(serde_json::to_vec(&(readiness, liveness)).expect("a probe serializes"))
}

type Probes = (Option<weft_core::infra::Probe>, Option<weft_core::infra::Probe>);

fn decode_probes(raw: &str) -> anyhow::Result<Probes> {
    let bytes = hex::decode(raw).map_err(|e| anyhow::anyhow!("a unit's probe label is not hex: {e}"))?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// `127.0.0.1:<port>` from what `docker port` printed.
fn loopback(printed: &str) -> Option<&str> {
    printed.lines().map(str::trim).find(|l| l.starts_with("127.0.0.1:"))
}

fn host_port(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    rest.split('/').next().unwrap_or(rest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::docker::fake::FakeDocker;

    #[test]
    fn docker_logs_merge_back_into_written_order() {
        let out = "2024-01-01T00:00:00.000000001Z first\n2024-01-01T00:00:00.000000003Z third\n";
        let err = "2024-01-01T00:00:00.000000002Z second line\n";
        let lines = timestamped_lines(out, err).unwrap();
        assert_eq!(lines.iter().map(|l| l.text.as_str()).collect::<Vec<_>>(), ["first", "second line", "third"]);
        assert_eq!(lines.iter().map(|l| l.pipe).collect::<Vec<_>>(), [Pipe::Stdout, Pipe::Stderr, Pipe::Stdout]);
        assert!(timestamped_lines("no stamp here\n", "").is_err());
    }
    use weft_core::infra::{
        resolve, ContainerPort, Endpoint, EnvEntry, InfraSpec, MachineShape, Mount, Probe, Protocol, Unit, Volume,
    };

    fn node_ref() -> NodeRef {
        NodeRef { tenant: "local".into(), project: uuid::Uuid::from_u128(5), node: "db".into(), copy_id: "wn-1".into() }
    }

    fn spec() -> InfraSpec {
        InfraSpec {
            units: vec![Unit {
                name: "main".into(),
                containers: vec![Container::new("pg", Image::Upstream { reference: "postgres:18".into() })
                    .with_command(vec!["docker-entrypoint.sh".into(), "postgres".into()])
                    .with_args(vec!["-c".into(), "fsync=off".into()])
                    .with_env(vec![EnvEntry::new("POSTGRES_PASSWORD", "s3cret")])
                    .with_ports(vec![ContainerPort { name: "sql".into(), port: 5432, protocol: Protocol::Tcp }])
                    .with_mounts(vec![Mount::new("data", "/var/lib/postgresql")])
                    .with_readiness(Probe::exec(vec!["pg_isready".into()]))],
                init_containers: vec![Container::new("init", Image::Upstream { reference: "busybox:1".into() })],
                machine: MachineShape { memory: Some("1Gi".into()), ..Default::default() },
                ..Default::default()
            }],
            volumes: vec![Volume { name: "data".into(), kind: VolumeKind::Disk { size: "10Gi".into(), class: None } }],
            endpoints: vec![Endpoint {
                name: "sql".into(),
                target: EndpointTarget::Unit { unit: "main".into(), container: "pg".into(), port: "sql".into() },
                expose: Expose::SameNetwork,
            }],
            keep_on_terminate: vec!["data".into()],
        }
    }

    fn resolved() -> ResolvedNode {
        resolve(&spec(), &node_ref(), &BTreeMap::new()).unwrap()
    }

    fn cfg(gpu: GpuAccess) -> LocalInfraHostConfig {
        let dir = std::env::temp_dir().join(format!("weft-infra-test-{}", uuid::Uuid::new_v4().simple()));
        LocalInfraHostConfig { agent_image: "weft-runtime:t".into(), scratch_dir: dir, gpu, disks: DiskBacking::Volumes, publish: Publish::Loopback, install: weft_core::infra::Install::default_install() }
    }

    fn host(docker: Arc<FakeDocker>, gpu: bool) -> LocalInfraHost {
        LocalInfraHost::new(docker, cfg(if gpu { GpuAccess::DockerGpus } else { GpuAccess::None }))
    }

    #[test]
    fn a_container_runs_on_its_agents_network_with_its_command_disks_and_limits() {
        let node = resolved();
        let unit = node.unit("main").unwrap();
        let c = &unit.unit.containers[0];
        let args = container_args(&node, unit, c, std::path::Path::new("/tmp/e.env"), ContainerRun::App, &cfg(GpuAccess::None));
        let agent = agent_name(&node.node, "main");
        assert!(args.windows(2).any(|w| w == ["--network".to_string(), format!("container:{agent}")]));
        assert!(args.windows(2).any(|w| w == ["--entrypoint", "docker-entrypoint.sh"]));
        assert!(args.windows(2).any(|w| w == ["--memory", "1g"]));
        assert!(args.windows(2).any(|w| w == ["--mount".to_string(), format!("type=volume,source={},target=/var/lib/postgresql", disk_volume(&node.node, "data"))]));
        let at = args.iter().position(|a| a == "postgres:18").unwrap();
        assert_eq!(args[at + 1..], ["postgres", "-c", "fsync=off"], "the command's rest, then the args");
        assert!(!args.iter().any(|a| a.contains("s3cret")), "values stay off the command line");
    }

    #[test]
    fn every_container_of_a_unit_with_a_shared_group_runs_in_it() {
        let mut s = spec();
        s.units[0].fs_group = Some(70);
        let node = resolve(&s, &node_ref(), &BTreeMap::new()).unwrap();
        let unit = node.unit("main").unwrap();
        for (c, run) in [(&unit.unit.containers[0], ContainerRun::App), (&unit.unit.init_containers[0], ContainerRun::Init)] {
            let args = container_args(&node, unit, c, std::path::Path::new("/e"), run, &cfg(GpuAccess::None));
            assert!(args.windows(2).any(|w| w == ["--group-add", "70"]), "{}", c.name);
        }
        let plain = container_args(&resolved(), resolved().unit("main").unwrap(), &unit.unit.containers[0], std::path::Path::new("/e"), ContainerRun::App, &cfg(GpuAccess::None));
        assert!(!plain.iter().any(|a| a == "--group-add"));
    }

    #[test]
    fn the_agent_publishes_its_port_and_the_units_on_loopback_or_as_is_on_a_cloud_machine() {
        let node = resolved();
        let args = agent_args(&node, node.unit("main").unwrap(), &cfg(GpuAccess::None));
        let published: Vec<&String> = args.windows(2).filter(|w| w[0] == "--publish").map(|w| &w[1]).collect();
        assert_eq!(published, [&format!("127.0.0.1::{UNIT_AGENT}"), &"127.0.0.1::5432".to_string()]);
        assert_eq!(args[args.len() - 3..], ["weft-runtime:t", "unit-agent", "serve"]);

        let machine = LocalInfraHostConfig { publish: Publish::AllPorts, ..cfg(GpuAccess::None) };
        let args = agent_args(&node, node.unit("main").unwrap(), &machine);
        let published: Vec<&String> = args.windows(2).filter(|w| w[0] == "--publish").map(|w| &w[1]).collect();
        assert_eq!(published, [&format!("127.0.0.1::{UNIT_AGENT}"), &"5432:5432".to_string()], "a cloud machine publishes every port as is");
    }

    #[test]
    fn a_gpu_this_machine_lacks_is_refused_before_anything_starts() {
        let mut s = spec();
        s.units[0].machine.gpu = Some(weft_core::infra::Gpu { kind: "nvidia-l4".into(), count: 1 });
        let node = resolve(&s, &node_ref(), &BTreeMap::new()).unwrap();
        let err = host(Arc::new(FakeDocker::new()), false).check(&node).unwrap_err();
        assert!(err.contains("machine.gpu"), "{err}");
        assert!(host(Arc::new(FakeDocker::new()), true).check(&node).is_ok());
    }

    #[test]
    fn a_gpu_kind_asked_on_docker_says_every_gpu_is_handed_over() {
        let mut s = spec();
        s.units[0].machine.gpu = Some(weft_core::infra::Gpu { kind: "nvidia-l4".into(), count: 1 });
        let node = resolve(&s, &node_ref(), &BTreeMap::new()).unwrap();
        let notes = host(Arc::new(FakeDocker::new()), true).notes(&node);
        assert_eq!(notes.len(), 1, "{notes:?}");
        assert!(notes[0].contains("nvidia-l4") && notes[0].contains("every GPU"), "{notes:?}");
        let cos = LocalInfraHost::new(Arc::new(FakeDocker::new()), cfg(GpuAccess::CosDriver));
        assert!(cos.notes(&node).is_empty(), "a machine made with the kind asked runs it as asked");
        assert!(host(Arc::new(FakeDocker::new()), true).notes(&resolve(&spec(), &node_ref(), &BTreeMap::new()).unwrap()).is_empty());
    }

    #[test]
    fn a_cos_machine_hands_in_its_driver_and_one_device_per_gpu_and_a_mounted_disk_by_path() {
        let mut s = spec();
        s.units[0].machine.gpu = Some(weft_core::infra::Gpu { kind: "nvidia-l4".into(), count: 2 });
        let node = resolve(&s, &node_ref(), &BTreeMap::new()).unwrap();
        let unit = node.unit("main").unwrap();
        let machine = LocalInfraHostConfig { gpu: GpuAccess::CosDriver, disks: DiskBacking::MountedUnder("/mnt/disks".into()), ..cfg(GpuAccess::None) };
        let args = container_args(&node, unit, &unit.unit.containers[0], std::path::Path::new("/e"), ContainerRun::App, &machine);
        assert!(args.windows(2).any(|w| w == ["--device", "/dev/nvidia1:/dev/nvidia1"]));
        assert!(!args.windows(2).any(|w| w == ["--device", "/dev/nvidia2:/dev/nvidia2"]));
        assert!(args.windows(2).any(|w| w == ["--volume", "/var/lib/nvidia/lib64:/usr/local/nvidia/lib64"]));
        assert!(args.windows(2).any(|w| w == ["--mount", "type=bind,source=/mnt/disks/data,target=/var/lib/postgresql"]));
    }

    #[tokio::test]
    async fn an_apply_builds_the_unit_in_order_and_skips_a_unit_already_running_as_asked() {
        let docker = Arc::new(FakeDocker::new());
        let h = host(docker.clone(), false);
        let node = resolved();
        h.apply_unit(&node, "main").await.unwrap();
        let runs: Vec<String> = docker.calls_to("run").iter().map(|c| c[c.iter().position(|a| a == "--name").unwrap() + 1].clone()).collect();
        assert_eq!(
            runs,
            [agent_name(&node.node, "main"), container_name(&node.node, "main", "init"), container_name(&node.node, "main", "pg")],
            "agent, then init containers, then the containers"
        );
        assert_eq!(docker.calls_to("volume").iter().filter(|c| c[1] == "create").count(), 1, "the disk");

        let hash = &node.unit("main").unwrap().hash;
        let row = |name: String, part: &str| {
            format!(r#"{{"Names":"{name}","State":"running","Labels":"weft.unit-hash={hash},weft.part={part}"}}"#)
        };
        docker.answer(
            &["ps"],
            &format!("{}\n{}\n", row(agent_name(&node.node, "main"), "agent"), row(container_name(&node.node, "main", "pg"), "app")),
        );
        let before = docker.calls_to("run").len();
        h.apply_unit(&node, "main").await.unwrap();
        assert_eq!(docker.calls_to("run").len(), before, "running as asked: nothing restarts");
    }

    /// An empty read marks its start on the daemon's clock, the one that
    /// stamps the lines, and a marked read resumes from its mark without
    /// asking the daemon's time.
    #[tokio::test]
    async fn an_empty_read_marks_the_daemons_time() {
        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["ps"], r#"{"Names":"c-pg","State":"running","Labels":"weft.part=app,weft.container=pg"}"#);
        docker.answer(&["info"], "\"2024-01-01T00:00:05.000000007Z\"\n");
        let h = host(docker.clone(), false);
        let streams = h.logs(&node_ref(), "main", &LogsFrom::Tail(0)).await.unwrap();
        let daemon = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:05.000000007Z").unwrap().to_utc();
        assert_eq!(streams[0].mark, LogMark::start(daemon));

        let marks = std::collections::BTreeMap::from([("pg".to_string(), streams[0].mark)]);
        let before = docker.calls_to("info").len();
        let again = h.logs(&node_ref(), "main", &LogsFrom::After(marks)).await.unwrap();
        assert_eq!(again[0].mark, streams[0].mark, "nothing new: the mark stays");
        assert_eq!(docker.calls_to("info").len(), before, "a marked read needs no clock");
    }

    #[tokio::test]
    async fn terminate_keeps_only_the_disks_it_is_told_to() {
        let docker = Arc::new(FakeDocker::new());
        docker.answer(
            &["volume", "ls"],
            concat!(
                r#"{"Name":"v-data","Labels":"weft.volume=data,weft.volume-kind=disk"}"#, "\n",
                r#"{"Name":"v-cache","Labels":"weft.volume=cache,weft.volume-kind=disk"}"#, "\n",
                r#"{"Name":"v-tmp","Labels":"weft.volume=tmp,weft.volume-kind=scratch"}"#, "\n",
            ),
        );
        host(docker.clone(), false).terminate(&node_ref(), &["data".into()]).await.unwrap();
        let rm = docker.calls().into_iter().find(|c| c.starts_with(&["volume".into(), "rm".into()])).unwrap();
        assert_eq!(rm[3..], ["v-cache", "v-tmp"]);
    }

    #[tokio::test]
    async fn a_copy_that_holds_only_kept_disks_is_still_listed() {
        let docker = Arc::new(FakeDocker::new());
        let p = uuid::Uuid::from_u128(1);
        docker.answer(
            &["ps"],
            &format!(r#"{{"Names":"a","State":"running","Labels":"weft.tenant=t,weft.project={p},weft.node=api,weft.copy=i-api"}}"#),
        );
        docker.answer(
            &["volume", "ls"],
            &format!(
                "{}\n{}\n",
                format_args!(r#"{{"Name":"v-api","Labels":"weft.tenant=t,weft.project={p},weft.node=api,weft.copy=i-api,weft.volume=data,weft.volume-kind=disk"}}"#),
                format_args!(r#"{{"Name":"v-db","Labels":"weft.tenant=t,weft.project={p},weft.node=db,weft.copy=i-db,weft.volume=data,weft.volume-kind=disk"}}"#),
            ),
        );
        let mut copies: Vec<(String, String)> =
            host(docker.clone(), false).copies().await.unwrap().into_iter().map(|c| (c.node, c.copy_id)).collect();
        copies.sort();
        assert_eq!(copies, [("api".to_string(), "i-api".to_string()), ("db".to_string(), "i-db".to_string())], "each copy once, the disk-only one included");
    }

    #[test]
    fn probes_survive_the_label_round_trip() {
        let p = Probe::http("/health", 8080);
        let raw = encode_probes(Some(&p), None);
        assert!(!raw.contains(','));
        assert_eq!(decode_probes(&raw).unwrap(), (Some(p), None));
    }
}
