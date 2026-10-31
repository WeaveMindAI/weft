//! Computer: infra node running a machine a program works on, with a
//! screen, a mouse, a keyboard and a shell.
//!
//! - `provision_infra` declares one unit: the computer's container, a
//!   disk for its home folder (kept across stop, so a stopped computer
//!   keeps its work), the `api` endpoint the action nodes call, and, when
//!   the node is given SSH keys, an `ssh` endpoint open to the install's
//!   own network.
//! - `run` hands out the `computer` handle every action node resolves.

use async_trait::async_trait;

use weft::infra::{
    Container, ContainerPort, Endpoint, EndpointTarget, EnvEntry, Expose, Image, InfraSpec, MachineShape, Mount, Probe,
    Protocol, Unit, Volume, VolumeKind,
};
use weft::node::NodeOutput;
use weft::{InfraProvisionContext, Node, NodeManifest, RunContext, ValueBag, WeftResult};

#[derive(NodeManifest)]
pub struct ComputerNode;

#[cfg(feature = "node-tests")]
mod tests;

// SYNC: the computer's environment <-> catalog/computer/computer/images/computer/computer.py
//       (WEFT_COMPUTER_PORT, WEFT_COMPUTER_SCREEN, WEFT_COMPUTER_SSH_KEYS), images/computer/start.sh
//       (WEFT_COMPUTER_PACKAGES, WEFT_COMPUTER_SSH_KEYS)
const PORT: u16 = 8000;
// SYNC: SSH_PORT <-> catalog/computer/computer/images/computer/computer.py (SSH_PORT)
const SSH_PORT: u16 = 2222;
// The home folder the disk is mounted on: the image's own, where its
// user lives.
// SYNC: HOME <-> catalog/computer/computer/images/computer/Dockerfile (ENV HOME), images/computer/computer.py (HOME)
const HOME: &str = "/home/user";
// SYNC: HEALTH_PATH <-> catalog/computer/computer/images/computer/computer.py (HEALTH_PATH)
const HEALTH_PATH: &str = "/health";

#[async_trait]
impl Node for ComputerNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn provision_infra(&self, _ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec> {
        let screen: String = input.get("screen")?;
        let packages: Option<String> = input.opt("packages")?;
        let ssh_keys: Option<String> = input.opt("sshKeys")?;
        let disk: String = input.get("disk")?;
        let machine = MachineShape {
            cpu: input.opt("cpu")?,
            memory: input.opt("memory")?,
            kind: input.opt::<String>("machineType")?.filter(|t| !t.trim().is_empty()),
            gpu: None,
        };
        let mut env = vec![
            EnvEntry::new("WEFT_COMPUTER_PORT", PORT.to_string()),
            EnvEntry::new("WEFT_COMPUTER_SCREEN", screen),
        ];
        // One spelling per package list, so the same packages written
        // with other spacing are the same spec and re-apply nothing.
        let packages = packages.map(|p| p.split_whitespace().collect::<Vec<_>>().join(" ")).filter(|p| !p.is_empty());
        if let Some(packages) = packages {
            env.push(EnvEntry::new("WEFT_COMPUTER_PACKAGES", packages));
        }
        // One key per line, blank lines and stray spaces dropped, for the
        // same reason.
        let ssh_keys = ssh_keys
            .map(|keys| keys.lines().map(str::trim).filter(|key| !key.is_empty()).collect::<Vec<_>>().join("\n"))
            .filter(|keys| !keys.is_empty());
        let mut ports = vec![ContainerPort { name: "http".into(), port: PORT, protocol: Protocol::Tcp }];
        let mut endpoints = vec![Endpoint {
            name: "api".into(),
            target: EndpointTarget::Unit { unit: "computer".into(), container: "computer".into(), port: "http".into() },
            expose: Expose::Project,
        }];
        if let Some(keys) = ssh_keys {
            env.push(EnvEntry::new("WEFT_COMPUTER_SSH_KEYS", keys));
            ports.push(ContainerPort { name: "ssh".into(), port: SSH_PORT, protocol: Protocol::Tcp });
            // The install's own network and no further: getting in still
            // takes one of the keys, and nothing on the internet reaches it.
            endpoints.push(Endpoint {
                name: "ssh".into(),
                target: EndpointTarget::Unit { unit: "computer".into(), container: "computer".into(), port: "ssh".into() },
                expose: Expose::SameNetwork,
            });
        }
        Ok(InfraSpec {
            units: vec![Unit {
                name: "computer".into(),
                containers: vec![Container::new("computer", Image::Local { name: "computer".into() })
                    .with_env(env)
                    .with_ports(ports)
                    .with_mounts(vec![Mount::new("home", HOME)])
                    // No liveness probe: installing `packages` at start
                    // takes as long as the packages take, and a probe
                    // would restart the container in the middle of it.
                    .with_readiness(Probe::http(HEALTH_PATH, PORT).with_initial_delay(2))],
                machine,
                ..Default::default()
            }],
            volumes: vec![Volume { name: "home".into(), kind: VolumeKind::Disk { size: disk, class: None } }],
            endpoints,
            keep_on_terminate: Vec::new(),
            health_rules: None,
            notes: Vec::new(),
        })
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let api = ctx.endpoint("api").await?;
        ctx.pulse_downstream(NodeOutput::new().set("computer", api.infra_handle())).await
    }
}
