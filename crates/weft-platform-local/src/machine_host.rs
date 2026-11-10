//! A local install's infra: container units on the Docker daemon
//! (`LocalInfraHost`), process units as processes on the machine
//! (`ProcessUnits`), behind the one `InfraHost` its supervisor drives.
//!
//! An apply names its unit in a resolved spec, which says what the unit
//! runs. Everything else names a unit alone (stop, remove, logs), and the
//! process host's registry answers where it lives: a unit it holds is a
//! process unit, any other is the Docker host's. A unit that changes kind
//! between two applies leaves nothing of its old kind behind: the apply
//! removes it from the other host first.

use std::sync::Arc;

use async_trait::async_trait;
use weft_core::infra::wire::{LogStream, LogsFrom};
use weft_core::infra::{EndpointTarget, NodeRef, ResolvedNode};
use weft_platform_traits::{EndpointAt, InfraHost, UnitObservation};

use crate::infra_host::LocalInfraHost;
use crate::process_units::ProcessUnits;

pub struct MachineInfraHost {
    containers: Arc<LocalInfraHost>,
    processes: Arc<ProcessUnits>,
}

impl MachineInfraHost {
    pub fn new(containers: Arc<LocalInfraHost>, processes: Arc<ProcessUnits>) -> Self {
        Self { containers, processes }
    }

    /// The host `unit` of `node` lives on, for a call that names no spec.
    async fn host_of(&self, node: &NodeRef, unit: &str) -> anyhow::Result<&dyn InfraHost> {
        let host: &dyn InfraHost = match self.processes.holds(node, unit).await? {
            true => self.processes.as_ref(),
            false => self.containers.as_ref(),
        };
        Ok(host)
    }
}

#[async_trait]
impl InfraHost for MachineInfraHost {
    fn check(&self, node: &ResolvedNode) -> Result<(), String> {
        node.units.iter().try_for_each(|u| match u.unit.process {
            Some(_) => self.processes.check_unit(node, u),
            None => self.containers.check_unit(node, u),
        })
    }

    fn notes(&self, node: &ResolvedNode) -> Vec<String> {
        self.containers.notes(node)
    }

    async fn apply_unit(&self, node: &ResolvedNode, unit: &str) -> anyhow::Result<()> {
        let runs_process = node.unit(unit).is_some_and(|u| u.unit.process.is_some());
        let held = self.processes.holds(&node.node, unit).await?;
        match (runs_process, held) {
            (true, true) => {}
            // Becoming a process unit: whatever containers it ran go.
            (true, false) => self.containers.remove_unit(&node.node, unit).await?,
            (false, true) => self.processes.remove_unit(&node.node, unit).await?,
            (false, false) => {}
        }
        match runs_process {
            true => self.processes.apply_unit(node, unit).await,
            false => self.containers.apply_unit(node, unit).await,
        }
    }

    async fn stop_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
        self.host_of(node, unit).await?.stop_unit(node, unit).await
    }

    async fn remove_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
        self.host_of(node, unit).await?.remove_unit(node, unit).await
    }

    async fn terminate(&self, node: &NodeRef, keep_disks: &[String]) -> anyhow::Result<()> {
        self.processes.terminate(node, keep_disks).await?;
        self.containers.terminate(node, keep_disks).await
    }

    async fn observe(&self, tenant: &str, project: uuid::Uuid) -> anyhow::Result<Vec<UnitObservation>> {
        // A unit either host cannot look at is that unit failed, in its
        // own observation; what fails here is a host that cannot name its
        // units at all (Docker not answering, the process registry
        // unreadable). That stays the whole look's error: answering the
        // other host's units alone would leave this one's unreported,
        // which the supervisor reads as not known yet and decides nothing
        // on, so the failure would never be said.
        let mut seen = self.containers.observe(tenant, project).await?;
        seen.extend(self.processes.observe(tenant, project).await?);
        Ok(seen)
    }

    async fn copies(&self) -> anyhow::Result<Vec<NodeRef>> {
        let mut copies = self.containers.copies().await?;
        for copy in self.processes.copies().await? {
            if !copies.iter().any(|c| c.copy_id == copy.copy_id) {
                copies.push(copy);
            }
        }
        Ok(copies)
    }

    async fn endpoint(&self, node: &ResolvedNode, endpoint: &str) -> anyhow::Result<EndpointAt> {
        let process = node.spec.endpoints.iter().any(|e| e.name == endpoint && matches!(e.target, EndpointTarget::Process { .. }));
        match process {
            true => self.processes.endpoint(node, endpoint).await,
            false => self.containers.endpoint(node, endpoint).await,
        }
    }

    async fn logs(&self, node: &NodeRef, unit: &str, from: &LogsFrom) -> anyhow::Result<Vec<LogStream>> {
        self.host_of(node, unit).await?.logs(node, unit, from).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    use crate::docker::fake::FakeDocker;
    use crate::infra_host::{AgentBroker, AgentIdentity, DiskBacking, GpuAccess, LocalInfraHostConfig, Publish};
    use crate::kept_ports::KeptPorts;
    use crate::process_units::ProcessUnitsConfig;
    use crate::processes::fake::{Behaviour, FakeProcesses};
    use weft_core::infra::{Container, Image, InfraSpec, Process, Unit};

    fn copy() -> NodeRef {
        NodeRef { tenant: "local".into(), project: uuid::Uuid::from_u128(1), node: "agent".into(), copy_id: "wn-agent-1".into() }
    }

    fn node(process: bool) -> ResolvedNode {
        let unit = match process {
            true => Unit { name: "server".into(), process: Some(Process::new(vec!["python3".into()])), ..Default::default() },
            false => Unit {
                name: "server".into(),
                containers: vec![Container::new("main", Image::Upstream { reference: "python:3".into() })],
                ..Default::default()
            },
        };
        weft_core::infra::resolve(&InfraSpec { units: vec![unit], ..Default::default() }, &copy(), &BTreeMap::new()).unwrap()
    }

    fn docker_cfg(dir: &std::path::Path, ports: Arc<KeptPorts>) -> LocalInfraHostConfig {
        LocalInfraHostConfig {
            agent_image: "weft-runtime:t".into(),
            scratch_dir: dir.join("run"),
            gpu: GpuAccess::None,
            disks: DiskBacking::Volumes,
            publish: Publish::Loopback(ports),
            install: weft_core::infra::Install::default_install(),
            names: crate::infra_names::Names::Machine,
            agent_broker: AgentBroker { url: "http://broker".into(), identity: AgentIdentity::Machine },
        }
    }

    struct Rig {
        dir: tempfile::TempDir,
        docker: Arc<FakeDocker>,
        processes: Arc<FakeProcesses>,
        host: MachineInfraHost,
    }

    fn rig() -> Rig {
        let dir = tempfile::tempdir().unwrap();
        crate::process_units::tests::program_in(&dir.path().join("bin"), "python3");
        let docker = Arc::new(FakeDocker::new());
        docker.fail(&["volume", "inspect"], "Error response from daemon: get x: no such volume");
        let processes = Arc::new(FakeProcesses::new());
        processes.behave(Behaviour::Runs);
        // One install's kept ports, for both hosts, as the runtime has it.
        let ports = Arc::new(KeptPorts::new(dir.path().to_path_buf(), processes.clone()));
        let containers = Arc::new(LocalInfraHost::new(docker.clone(), docker_cfg(dir.path(), ports.clone())));
        let units = Arc::new(ProcessUnits::new(
            processes.clone(),
            Arc::new(weft_platform_traits::FakeMachineNeeds::new()),
            weft_platform_traits::FakeClock::new(),
            ports,
            ProcessUnitsConfig {
                dir: dir.path().join("infra-processes"),
                agent_program: "/opt/weft/weft-runtime".into(),
                machine_env: [("PATH".to_string(), dir.path().join("bin").display().to_string())].into(),
                system: Err("unknown".into()),
                stop_grace: Duration::from_millis(100),
                needs_root: dir.path().join("needs"),
            },
        )
        .unwrap());
        Rig { dir, docker, processes, host: MachineInfraHost::new(containers, units) }
    }

    /// A process unit goes to the process host and a container unit to
    /// Docker, each refused by the other; a unit that turns into a process
    /// unit leaves no container behind, and a call that names no spec goes
    /// where the unit lives.
    #[tokio::test]
    async fn each_unit_goes_to_the_host_that_runs_it() {
        let r = rig();
        r.host.check(&node(true)).unwrap();
        r.host.check(&node(false)).unwrap();
        let ports = crate::kept_ports::fake::counting_in(r.dir.path(), 20000);
        let docker_alone = LocalInfraHost::new(Arc::new(FakeDocker::new()), docker_cfg(r.dir.path(), ports));
        let refused = docker_alone.check(&node(true)).unwrap_err();
        assert!(refused.contains("runs a process") && refused.contains("only a local install runs one"), "{refused}");
        assert!(r.host.processes.check(&node(false)).unwrap_err().contains("runs containers"));

        r.host.apply_unit(&node(true), "server").await.unwrap();
        assert_eq!(r.processes.spawns().len(), 1);
        assert!(r.docker.calls_to("ps").iter().any(|c| c.iter().any(|a| a == "label=weft.unit=server")), "its containers are looked for: {:?}", r.docker.calls());
        assert!(r.docker.calls_to("run").is_empty(), "no container starts");
        let looked = r.docker.calls().len();
        r.host.stop_unit(&copy(), "server").await.unwrap();
        assert_eq!(r.docker.calls().len(), looked, "the process host stops it");
        assert_eq!(r.processes.signals().len(), 2);

        r.host.apply_unit(&node(false), "server").await.unwrap();
        assert!(!r.host.processes.holds(&copy(), "server").await.unwrap(), "the process unit it was is gone");
        assert!(!r.docker.calls_to("run").is_empty(), "its containers start");
    }
}
