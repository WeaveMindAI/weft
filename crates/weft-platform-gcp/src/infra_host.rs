//! Infra units on Compute Engine: one machine per unit.
//!
//! The machine runs Container-Optimized OS. Its startup script mounts the
//! unit's disks (persistent disks weft creates and attaches, formatted on
//! first use), installs the GPU driver when the unit has GPUs, and starts
//! the host agent (`weft-runtime unit-agent host`), which reads the unit
//! from the machine's metadata and runs it on the machine's Docker the
//! same way a laptop runs it. Weft asks the host agent how the unit is
//! doing and to apply a changed unit, signed as the install's core
//! account.
//!
//! The machine has no public address; the workers and the install reach
//! the unit's ports at the machine's address on the install's network,
//! and the network's firewall keeps the rest out. It runs as the project's
//! own service account, never weft's: what a unit's containers can ask the
//! metadata server for is that project's identity and nothing more.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use weft_core::infra::wire::{LogStream, LogsFrom};
use weft_core::infra::{Container, EndpointTarget, Expose, Limits, NodeRef, ResolvedNode, ResolvedUnit, VolumeKind};
use weft_platform_traits::config::GcpPlatform;
use weft_platform_traits::unit_agent::{UnitAssignment, HOST_APPLY, HOST_LOGS, HOST_OBSERVE, HOST_RESTART};
use weft_core::ports::UNIT_AGENT;
use weft_platform_traits::{EndpointAt, InfraHost, UnitObservation, UnitRunState};

use crate::accounts::Access;
use crate::api::Google;
use crate::names;

/// The metadata keys the startup script and the host agent read.
// SYNC: metadata keys <-> crates/weft-runtime/src/unit_agent.rs (host)
pub const MD_UNIT: &str = "weft-unit";
pub const MD_DISKS: &str = "weft-disks";
pub const MD_GPU: &str = "weft-gpu";
pub const MD_RUNTIME_IMAGE: &str = "weft-runtime-image";
pub const MD_CORE_ACCOUNT: &str = "weft-core-account";
pub const MD_GCP_PROJECT: &str = "weft-gcp-project";

/// The boot image of every infra machine.
const BOOT_IMAGE: &str = "projects/cos-cloud/global/images/family/cos-stable";

/// The boot disk of every infra machine, in GB.
const BOOT_DISK_GB: u64 = 20;

/// How long one call to a host agent may take.
const AGENT_CALL: Duration = Duration::from_secs(30);

const STARTUP_SCRIPT: &str = r#"#!/bin/bash
# Written by weft (crates/weft-platform-gcp/src/infra_host.rs). Runs on
# every boot: mount the unit's disks, install the GPU driver when asked,
# start the host agent.
set -eu
# Container-Optimized OS drops every incoming connection but ssh. The
# ports that may reach this machine are the network's firewall rules
# (deploy/terraform/gcp/network.tf), so the machine accepts what those
# let through: the host agent's port and the ports its units declare.
iptables -w -A INPUT -p tcp -j ACCEPT
iptables -w -A INPUT -p udp -j ACCEPT
md() { curl -sf -H 'Metadata-Flavor: Google' "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1" || true; }
for disk in $(md weft-disks); do
  dev="/dev/disk/by-id/google-$disk"
  if ! blkid "$dev" >/dev/null 2>&1; then mkfs.ext4 -F -m 0 "$dev"; fi
  mkdir -p "/mnt/disks/$disk"
  mountpoint -q "/mnt/disks/$disk" || mount -o discard,defaults "$dev" "/mnt/disks/$disk"
done
if [ "$(md weft-gpu)" = "yes" ]; then
  cos-extensions install gpu
  mount --bind /var/lib/nvidia /var/lib/nvidia
  mount -o remount,exec /var/lib/nvidia
fi
image="$(md weft-runtime-image)"
export HOME=/var/lib/weft/home
mkdir -p "$HOME"
docker-credential-gcr configure-docker --registries="${image%%/*}"
docker rm -f weft-host-agent >/dev/null 2>&1 || true
docker run -d --name weft-host-agent --restart always --network host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /mnt/disks:/mnt/disks -v /var/lib/weft:/var/lib/weft \
  "$image" unit-agent host
"#;

pub struct ComputeInfraHost {
    google: Google,
    gcp: GcpPlatform,
    install: weft_core::infra::Install,
}

impl ComputeInfraHost {
    pub fn new(google: Google, gcp: GcpPlatform, install: weft_core::infra::Install) -> Self {
        Self { google, gcp, install }
    }

    fn zone_base(&self) -> String {
        format!("https://compute.googleapis.com/compute/v1/projects/{}/zones/{}", self.gcp.project, self.gcp.zone)
    }

    fn instance_url(&self, name: &str) -> String {
        format!("{}/instances/{name}", self.zone_base())
    }

    async fn wait(&self, op: Value) -> anyhow::Result<Value> {
        self.google.wait(&self.zone_base(), op).await
    }

    /// Every machine of this install labeled with each of `labels`.
    async fn machines(&self, labels: &[(&str, &str)]) -> anyhow::Result<Vec<Value>> {
        let mut filter = vec![format!("labels.{}={}", weft_core::infra::INSTALL_LABEL, self.install.label_value())];
        filter.extend(labels.iter().map(|(k, v)| format!("labels.{k}={v}")));
        self.list("instances", &filter.join(" AND ")).await
    }

    /// Every `kind` (`instances`, `disks`) in the zone matching `filter`,
    /// page after page.
    async fn list(&self, kind: &str, filter: &str) -> anyhow::Result<Vec<Value>> {
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        loop {
            let mut query = vec![("maxResults", "200".to_string()), ("filter", filter.to_string())];
            if let Some(token) = page.take() {
                query.push(("pageToken", token));
            }
            let listed = self.google.get_query(&format!("{}/{kind}", self.zone_base()), &query).await?;
            out.extend(listed.get("items").and_then(Value::as_array).into_iter().flatten().cloned());
            page = listed.get("nextPageToken").and_then(Value::as_str).map(str::to_string);
            if page.is_none() {
                return Ok(out);
            }
        }
    }

    async fn ensure_disks(&self, node: &ResolvedNode, unit: &ResolvedUnit) -> anyhow::Result<Vec<String>> {
        let base = node.node.resource_base();
        let mut attached = Vec::new();
        for v in &node.spec.volumes {
            let VolumeKind::Disk { size, class } = &v.kind else { continue };
            if !mounts(unit, &v.name) {
                continue;
            }
            let name = names::unit_disk(&base, &v.name);
            let url = format!("{}/disks/{name}", self.zone_base());
            if self.google.get_opt(&url).await?.is_none() {
                let body = json!({
                    "name": name,
                    "sizeGb": size_gb(size)?.to_string(),
                    "type": format!("zones/{}/diskTypes/{}", self.gcp.zone, class.as_deref().unwrap_or("pd-balanced")),
                    "labels": labels(&self.install, &node.node, &unit.unit.name, None),
                    // The copy the disk belongs to, whole: a label value
                    // cannot hold a node's spelling (`one.db`), and a
                    // disk a terminate kept is how `copies` still finds
                    // the copy once its machines are gone.
                    "description": serde_json::to_string(&node.node)?,
                });
                let op = self.google.post(&format!("{}/disks", self.zone_base()), &body).await?;
                self.wait(op).await?;
            }
            attached.push(v.name.clone());
        }
        Ok(attached)
    }

    fn instance_body(&self, node: &ResolvedNode, unit: &ResolvedUnit, disks: &[String], account: &str) -> anyhow::Result<Value> {
        let shape = machine_shape(&self.gcp.zone, unit)?;
        let base = node.node.resource_base();
        let assignment = serde_json::to_string(&UnitAssignment { node: node.clone(), unit: unit.unit.name.clone() })?;
        let mut attached = vec![json!({
            "boot": true,
            "autoDelete": true,
            "initializeParams": { "sourceImage": BOOT_IMAGE, "diskSizeGb": BOOT_DISK_GB.to_string() },
        })];
        for d in disks {
            attached.push(json!({
                "source": format!("zones/{}/disks/{}", self.gcp.zone, names::unit_disk(&base, d)),
                "deviceName": d,
                // A disk outlives its machine: terminate deletes it.
                "autoDelete": false,
            }));
        }
        let mut body = json!({
            "name": names::unit_machine(&base, &unit.unit.name),
            "machineType": format!("zones/{}/machineTypes/{}", self.gcp.zone, shape.machine_type),
            "labels": labels(&self.install, &node.node, &unit.unit.name, Some(&unit.hash)),
            "disks": attached,
            "networkInterfaces": [{ "network": self.gcp.network, "subnetwork": self.gcp.subnet }],
            "tags": { "items": [self.gcp.infra_network_tag] },
            "serviceAccounts": [{ "email": account, "scopes": ["https://www.googleapis.com/auth/cloud-platform"] }],
            "metadata": { "items": [
                { "key": "startup-script", "value": STARTUP_SCRIPT },
                { "key": MD_UNIT, "value": assignment },
                { "key": MD_DISKS, "value": disks.join(" ") },
                { "key": MD_GPU, "value": if shape.gpus > 0 { "yes" } else { "no" } },
                { "key": MD_RUNTIME_IMAGE, "value": self.gcp.runtime_image },
                { "key": MD_CORE_ACCOUNT, "value": self.gcp.core_service_account },
                { "key": MD_GCP_PROJECT, "value": self.gcp.project },
            ]},
        });
        if let Some(kind) = &shape.accelerator {
            body["guestAccelerators"] = json!([{ "acceleratorType": format!("zones/{}/acceleratorTypes/{kind}", self.gcp.zone), "acceleratorCount": shape.gpus }]);
        }
        if shape.gpus > 0 {
            // A machine with a GPU cannot move to another host live, whether
            // the GPU is attached (N1) or comes with the machine type (G2).
            body["scheduling"] = json!({ "onHostMaintenance": "TERMINATE", "automaticRestart": true });
        }
        Ok(body)
    }

    /// Create the machine for `unit`. Right after its account is new,
    /// Compute Engine may not know the account yet; that is waited out.
    async fn create_machine(&self, node: &ResolvedNode, unit: &ResolvedUnit, disks: &[String], account: &str) -> anyhow::Result<()> {
        let body = self.instance_body(node, unit, disks, account)?;
        let url = format!("{}/instances", self.zone_base());
        crate::accounts::until_account_is_known(|| async {
            let op = self.google.post(&url, &body).await?;
            self.wait(op).await
        })
        .await?;
        Ok(())
    }

    /// Call the host agent of a running machine, signed as weft.
    async fn agent(&self, machine: &Value, method: reqwest::Method, path: &str) -> anyhow::Result<reqwest::Response> {
        let ip = internal_ip(machine).ok_or_else(|| anyhow::anyhow!("the machine {} has no address yet", name_of(machine)))?;
        let base = format!("http://{ip}:{UNIT_AGENT}");
        let token = self.google.tokens().id_token(&base).await?;
        let resp = self
            .google
            .http()
            .request(method, format!("{base}{path}"))
            .bearer_auth(token)
            .timeout(AGENT_CALL)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            anyhow::bail!("the host agent of {} answered {status}: {}", name_of(machine), resp.text().await.unwrap_or_default());
        }
        Ok(resp)
    }

    async fn machine(&self, node: &NodeRef, unit: &str) -> anyhow::Result<Option<Value>> {
        self.google.get_opt(&self.instance_url(&names::unit_machine(&node.resource_base(), unit))).await
    }

    async fn set_assignment(&self, machine: &Value, node: &ResolvedNode, unit: &ResolvedUnit) -> anyhow::Result<()> {
        let fingerprint = machine.pointer("/metadata/fingerprint").and_then(Value::as_str).unwrap_or_default();
        let mut items: Vec<Value> = machine.pointer("/metadata/items").and_then(Value::as_array).cloned().unwrap_or_default();
        let assignment = serde_json::to_string(&UnitAssignment { node: node.clone(), unit: unit.unit.name.clone() })?;
        items.retain(|i| i.get("key").and_then(Value::as_str) != Some(MD_UNIT));
        items.push(json!({ "key": MD_UNIT, "value": assignment }));
        let op = self
            .google
            .post(&format!("{}/setMetadata", self.instance_url(name_of(machine))), &json!({ "fingerprint": fingerprint, "items": items }))
            .await?;
        self.wait(op).await?;
        let op = self
            .google
            .post(
                &format!("{}/setLabels", self.instance_url(name_of(machine))),
                &json!({
                    "labelFingerprint": machine.get("labelFingerprint").and_then(Value::as_str).unwrap_or_default(),
                    "labels": labels(&self.install, &node.node, &unit.unit.name, Some(&unit.hash)),
                }),
            )
            .await?;
        self.wait(op).await?;
        Ok(())
    }
}

/// What machine a unit gets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shape {
    pub machine_type: String,
    /// The GPU kind attached to the machine, when the machine type does
    /// not already come with it (N1 does not; G2 does, so `None`).
    pub accelerator: Option<String>,
    pub gpus: u32,
}

/// The machine for `unit`. A `machine.type` is used as is. Otherwise the
/// numbers are `machine.cpu` and `machine.memory`, each unset one being
/// the sum of the containers' own limits (or the largest init
/// container's, which runs alone), and the machine is the cheapest that
/// holds them: one of the shared-core E2 types, or an E2 custom shape
/// (an even number of CPUs from 2, memory in 256 MB steps between half a
/// GB and 8 GB per CPU). For GPUs it is the family the accelerator
/// attaches to (N1 for the `nvidia-tesla-*` kinds, G2 for `nvidia-l4`),
/// at the cheapest type that holds its CPUs and memory.
pub fn machine_shape(_zone: &str, unit: &ResolvedUnit) -> anyhow::Result<Shape> {
    let m = &unit.unit.machine;
    if let Some(kind) = &m.kind {
        anyhow::ensure!(
            !kind.is_empty() && kind.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "unit '{}' asks for machine type '{kind}'; a Compute Engine machine type is lowercase letters, digits and dashes (e2-micro, n2-standard-8)",
            unit.unit.name
        );
        let gpus = m.gpu.as_ref().map_or(0, |g| g.count);
        // These families come with their GPUs; any other takes them
        // attached.
        let built_in = ["a2-", "a3-", "a4-", "g2-", "g4-"].iter().any(|f| kind.starts_with(f));
        let accelerator = m.gpu.as_ref().filter(|_| !built_in).map(|g| g.kind.clone());
        return Ok(Shape { machine_type: kind.clone(), accelerator, gpus });
    }
    let cpus = match &m.cpu {
        Some(c) => parse_cpus(c)?,
        None => containers_need(unit, |l| l.cpu.as_deref(), parse_cpus)?,
    };
    let memory_mb = match &m.memory {
        Some(mem) => parse_mb(mem)?,
        None => containers_need(unit, |l| l.memory.as_deref(), |raw| parse_mb(raw).map(f64::from))?.ceil() as u32,
    };
    if let Some(gpu) = &m.gpu {
        anyhow::ensure!(gpu.count >= 1, "unit '{}' asks for 0 GPUs", unit.unit.name);
        return gpu_shape(&unit.unit.name, &gpu.kind, gpu.count, cpus, memory_mb);
    }
    // The shared-core types: the share of a CPU each sustains, and its
    // memory in MB. Each is cheaper than any custom shape that holds as
    // much.
    const SHARED: [(&str, f64, u32); 3] = [("e2-micro", 0.25, 1024), ("e2-small", 0.5, 2048), ("e2-medium", 1.0, 4096)];
    if let Some((name, ..)) = SHARED.iter().find(|&&(_, c, mb)| cpus <= c && memory_mb <= mb) {
        return Ok(Shape { machine_type: (*name).into(), accelerator: None, gpus: 0 });
    }
    let cpus = (cpus.ceil() as u32).max(2).div_ceil(2) * 2;
    let floor = cpus * 512;
    let ceiling = cpus * 8 * 1024;
    let mb = memory_mb.max(floor).div_ceil(256) * 256;
    anyhow::ensure!(
        mb <= ceiling,
        "unit '{}' asks for {} MB on {cpus} CPUs; an E2 machine holds at most 8 GB per CPU, so raise machine.cpu",
        unit.unit.name,
        memory_mb
    );
    Ok(Shape { machine_type: format!("e2-custom-{cpus}-{mb}"), accelerator: None, gpus: 0 })
}

/// What `unit`'s containers' own limits add up to, read by `pick` and
/// `parse`: the containers run side by side, so theirs are summed, and
/// an init container runs alone, so only the largest counts. A
/// container with no limit adds nothing.
fn containers_need(unit: &ResolvedUnit, pick: fn(&Limits) -> Option<&str>, parse: fn(&str) -> anyhow::Result<f64>) -> anyhow::Result<f64> {
    let each = |cs: &[Container]| cs.iter().filter_map(|c| pick(&c.limits)).map(parse).collect::<anyhow::Result<Vec<f64>>>();
    let side_by_side: f64 = each(&unit.unit.containers)?.into_iter().sum();
    let alone = each(&unit.unit.init_containers)?.into_iter().fold(0.0, f64::max);
    Ok(side_by_side.max(alone))
}

// The GPU machines Compute Engine offers, as its GPU machine types page
// lists them (docs.cloud.google.com/compute/docs/gpus). The page no longer
// lists the P100, so weft does not attach it.

/// The most vCPUs and memory (GB) an N1 machine may have with `count`
/// GPUs of `kind` attached, or `None` when that pairing does not exist.
fn n1_limits(kind: &str, count: u32) -> Option<(u32, u32)> {
    Some(match (kind, count) {
        ("nvidia-tesla-t4", 1 | 2) => (48, 312),
        ("nvidia-tesla-t4", 4) => (96, 624),
        ("nvidia-tesla-p4", 1) => (24, 156),
        ("nvidia-tesla-p4", 2) => (48, 312),
        ("nvidia-tesla-p4", 4) => (96, 624),
        ("nvidia-tesla-v100", 1) => (12, 78),
        ("nvidia-tesla-v100", 2) => (24, 156),
        ("nvidia-tesla-v100", 4) => (48, 312),
        ("nvidia-tesla-v100", 8) => (96, 624),
        _ => return None,
    })
}

/// The predefined N1 types a GPU attaches to: name, vCPUs, memory in MB.
fn n1_types() -> impl Iterator<Item = (String, u32, u32)> {
    const SIZES: [u32; 8] = [1, 2, 4, 8, 16, 32, 64, 96];
    // Memory per vCPU in MB: standard 3.75 GB, highmem 6.5 GB.
    let family = |name: &'static str, per_cpu_mb: u32, from: u32| {
        SIZES.into_iter().filter(move |&n| n >= from).map(move |n| (format!("n1-{name}-{n}"), n, n * per_cpu_mb))
    };
    family("standard", 3840, 1).chain(family("highmem", 6656, 2))
}

/// The G2 types (each with its own L4 GPUs): name, GPUs, vCPUs, memory GB.
const G2_TYPES: [(&str, u32, u32, u32); 8] = [
    ("g2-standard-4", 1, 4, 16),
    ("g2-standard-8", 1, 8, 32),
    ("g2-standard-12", 1, 12, 48),
    ("g2-standard-16", 1, 16, 64),
    ("g2-standard-24", 2, 24, 96),
    ("g2-standard-32", 1, 32, 128),
    ("g2-standard-48", 4, 48, 192),
    ("g2-standard-96", 8, 96, 384),
];

/// The cheapest machine carrying `count` GPUs of `kind` whose CPUs and
/// memory hold the request, refused naming the largest there is when
/// none does. "Cheapest" weighs one vCPU as 7.5 GB of memory, about
/// their price ratio on N1 and G2.
fn gpu_shape(unit: &str, kind: &str, count: u32, cpus: f64, memory_mb: u32) -> anyhow::Result<Shape> {
    let cpus = (cpus.ceil() as u32).max(1);
    let cost = |c: u32, mb: u32| u64::from(c) * 7680 + u64::from(mb);
    let fits = |c: u32, mb: u32| c >= cpus && mb >= memory_mb;
    let asked = format!("unit '{unit}' asks for {count} {kind} with {cpus} CPUs and {memory_mb} MB");
    if kind.starts_with("nvidia-tesla-") {
        let Some((max_cpus, max_gb)) = n1_limits(kind, count) else {
            anyhow::bail!("{asked}; Compute Engine attaches nvidia-tesla-t4 and -p4 in 1, 2 or 4, and nvidia-tesla-v100 in 1, 2, 4 or 8");
        };
        let best = n1_types()
            .filter(|&(_, c, mb)| c <= max_cpus && mb <= max_gb * 1024 && fits(c, mb))
            .min_by_key(|&(_, c, mb)| cost(c, mb));
        let Some((machine_type, _, _)) = best else {
            anyhow::bail!("{asked}; an N1 machine with {count} of them holds at most {max_cpus} CPUs and {max_gb} GB, so lower machine.cpu or machine.memory, or ask for more GPUs");
        };
        return Ok(Shape { machine_type, accelerator: Some(kind.to_string()), gpus: count });
    }
    if kind == "nvidia-l4" {
        let best = G2_TYPES
            .iter()
            .filter(|&&(_, g, c, gb)| g == count && fits(c, gb * 1024))
            .min_by_key(|&&(_, _, c, gb)| cost(c, gb * 1024));
        let Some(&(machine_type, ..)) = best else {
            anyhow::bail!("{asked}; a G2 machine has 1, 2, 4 or 8 of them, with 1 up to 32 CPUs and 128 GB, 2 with 24 CPUs and 96 GB, 4 with 48 CPUs and 192 GB, 8 with 96 CPUs and 384 GB");
        };
        return Ok(Shape { machine_type: machine_type.into(), accelerator: None, gpus: count });
    }
    anyhow::bail!("unit '{unit}' asks for GPU kind '{kind}'; the kinds weft attaches on Compute Engine are nvidia-l4, nvidia-tesla-t4, nvidia-tesla-p4 and nvidia-tesla-v100")
}

fn parse_cpus(raw: &str) -> anyhow::Result<f64> {
    let raw = raw.trim();
    match raw.strip_suffix('m') {
        Some(milli) => Ok(milli.parse::<f64>()? / 1000.0),
        None => raw.parse::<f64>().map_err(|_| anyhow::anyhow!("'{raw}' is not a CPU count")),
    }
}

fn parse_mb(raw: &str) -> anyhow::Result<u32> {
    let raw = raw.trim();
    // In MiB: a gigabyte (10^9 bytes) is about 953.7 MiB.
    for (suffix, per) in [("Gi", 1024.0), ("Mi", 1.0), ("G", 1e9 / 1048576.0), ("M", 1e6 / 1048576.0)] {
        if let Some(n) = raw.strip_suffix(suffix) {
            return Ok((n.parse::<f64>().map_err(|_| anyhow::anyhow!("'{raw}' is not a memory size"))? * per).ceil() as u32);
        }
    }
    anyhow::bail!("'{raw}' is not a memory size (write it like 512Mi or 4Gi)")
}

/// A disk size in whole GB (`10Gi`, `500Mi` rounds up to 1).
pub fn size_gb(raw: &str) -> anyhow::Result<u64> {
    Ok(u64::from(parse_mb(raw)?).div_ceil(1024).max(1))
}

fn mounts(unit: &ResolvedUnit, volume: &str) -> bool {
    unit.unit.containers.iter().chain(unit.unit.init_containers.iter()).any(|c| c.mounts.iter().any(|m| m.volume == volume))
}

/// Labels on everything of `unit` of `node` (a label value holds at most
/// 63 lowercase letters, digits, `-` and `_`).
fn labels(install: &weft_core::infra::Install, node: &NodeRef, unit: &str, hash: Option<&str>) -> Value {
    let mut l = json!({
        weft_core::infra::INSTALL_LABEL: install.label_value(),
        "weft-project": node.project.simple().to_string(),
        "weft-copy": node.resource_base(),
        "weft-unit": unit,
    });
    if let Some(h) = hash {
        l["weft-unit-hash"] = json!(&h[..h.len().min(63)]);
    }
    l
}

/// The copy a disk belongs to, from its description. Every disk of an
/// install is created with one, so a disk without it is refused loudly.
fn disk_copy(disk: &Value) -> anyhow::Result<NodeRef> {
    let raw = disk
        .get("description")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("disk '{}' carries no description naming its copy", name_of(disk)))?;
    serde_json::from_str(raw).map_err(|e| anyhow::anyhow!("disk '{}': its description is not a copy: {e}", name_of(disk)))
}

fn name_of(machine: &Value) -> &str {
    machine.get("name").and_then(Value::as_str).unwrap_or_default()
}

fn internal_ip(machine: &Value) -> Option<&str> {
    machine.pointer("/networkInterfaces/0/networkIP").and_then(Value::as_str)
}

fn label<'a>(machine: &'a Value, key: &str) -> Option<&'a str> {
    machine.get("labels").and_then(|l| l.get(key)).and_then(Value::as_str)
}

/// The unit a machine was given, from its metadata. None for a machine
/// that carries no assignment (one of the install's own); an assignment
/// that does not read is an error naming the machine, never a silent
/// skip, and each caller says what that error stops.
fn assignment(machine: &Value) -> anyhow::Result<Option<UnitAssignment>> {
    let Some(item) = machine
        .pointer("/metadata/items")
        .and_then(Value::as_array)
        .and_then(|items| items.iter().find(|i| i.get("key").and_then(Value::as_str) == Some(MD_UNIT)))
    else {
        return Ok(None);
    };
    let raw = item
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("machine '{}': its '{MD_UNIT}' metadata has no text value", name_of(machine)))?;
    serde_json::from_str(raw)
        .map(Some)
        .map_err(|e| anyhow::anyhow!("machine '{}': its '{MD_UNIT}' metadata is not a unit assignment: {e}", name_of(machine)))
}

#[async_trait]
impl InfraHost for ComputeInfraHost {
    fn check(&self, node: &ResolvedNode) -> Result<(), String> {
        for u in &node.units {
            machine_shape(&self.gcp.zone, u).map_err(|e| format!("{e:#}"))?;
        }
        Ok(())
    }

    async fn apply_unit(&self, node: &ResolvedNode, unit: &str) -> anyhow::Result<()> {
        let resolved = node.unit(unit).ok_or_else(|| anyhow::anyhow!("node '{}' declares no unit '{unit}'", node.node.node))?;
        let account = crate::accounts::ensure_project_account(&self.google, &self.gcp, node.node.project, &[Access::ImageRegistry, Access::Logging]).await?;
        let disks = self.ensure_disks(node, resolved).await?;
        let Some(machine) = self.machine(&node.node, unit).await? else {
            return self.create_machine(node, resolved, &disks, &account).await;
        };
        let want = machine_shape(&self.gcp.zone, resolved)?;
        let has_type = machine.get("machineType").and_then(Value::as_str).unwrap_or_default();
        let same_disks = {
            let attached: Vec<&str> = machine
                .get("disks")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|d| d.get("deviceName").and_then(Value::as_str))
                .collect();
            disks.iter().all(|d| attached.contains(&d.as_str()))
        };
        let has_gpus = machine
            .pointer("/guestAccelerators/0")
            .map(|a| {
                let kind = a.get("acceleratorType").and_then(Value::as_str).unwrap_or_default();
                (kind.rsplit('/').next().unwrap_or_default().to_string(), a.get("acceleratorCount").and_then(Value::as_u64).unwrap_or(0) as u32)
            });
        // Only an attached GPU is compared: a G2 machine's GPUs come with its
        // type, which is compared already, and what Compute Engine reports
        // for them is not what weft set.
        let gpus_differ = want.accelerator.is_some() && has_gpus != want.accelerator.clone().map(|k| (k, want.gpus));
        if !has_type.ends_with(&format!("/{}", want.machine_type)) || !same_disks || gpus_differ {
            // A new shape, a new disk or an accelerator is a new machine;
            // its disks carry over (they are never deleted with it).
            if let Some(op) = self.google.delete(&self.instance_url(name_of(&machine))).await? {
                self.wait(op).await?;
            }
            return self.create_machine(node, resolved, &disks, &account).await;
        }
        if label(&machine, "weft-unit-hash") != Some(&resolved.hash[..resolved.hash.len().min(63)]) {
            self.set_assignment(&machine, node, resolved).await?;
        }
        match machine.get("status").and_then(Value::as_str) {
            Some("RUNNING") => {
                self.agent(&machine, reqwest::Method::POST, HOST_APPLY).await?;
            }
            _ => {
                // Its agent applies the unit as it boots.
                let op = self.google.post(&format!("{}/start", self.instance_url(name_of(&machine))), &json!({})).await?;
                self.wait(op).await?;
            }
        }
        Ok(())
    }

    async fn stop_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
        let Some(machine) = self.machine(node, unit).await? else { return Ok(()) };
        if matches!(machine.get("status").and_then(Value::as_str), Some("TERMINATED" | "STOPPING" | "SUSPENDED")) {
            return Ok(());
        }
        let op = self.google.post(&format!("{}/stop", self.instance_url(name_of(&machine))), &json!({})).await?;
        self.wait(op).await.map(|_| ())
    }

    async fn restart_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
        let machine = self
            .machine(node, unit)
            .await?
            .ok_or_else(|| anyhow::anyhow!("unit '{unit}' of '{}' has no machine", node.node))?;
        self.agent(&machine, reqwest::Method::POST, HOST_RESTART).await.map(|_| ())
    }

    async fn remove_unit(&self, node: &NodeRef, unit: &str) -> anyhow::Result<()> {
        if let Some(op) = self.google.delete(&self.instance_url(&names::unit_machine(&node.resource_base(), unit))).await? {
            self.wait(op).await?;
        }
        Ok(())
    }

    async fn terminate(&self, node: &NodeRef, keep_disks: &[String]) -> anyhow::Result<()> {
        let copy = node.resource_base();
        for m in self.machines(&[("weft-copy", &copy)]).await? {
            if let Some(op) = self.google.delete(&self.instance_url(name_of(&m))).await? {
                self.wait(op).await?;
            }
        }
        let keep: Vec<String> = keep_disks.iter().map(|d| names::unit_disk(&copy, d)).collect();
        for d in self.list("disks", &format!("labels.weft-copy={copy}")).await? {
            let name = name_of(&d);
            if keep.iter().any(|k| k == name) {
                continue;
            }
            if let Some(op) = self.google.delete(&format!("{}/disks/{name}", self.zone_base())).await? {
                self.wait(op).await?;
            }
        }
        Ok(())
    }

    async fn observe(&self, _tenant: &str, project: uuid::Uuid) -> anyhow::Result<Vec<UnitObservation>> {
        let mut out = Vec::new();
        for m in self.machines(&[("weft-project", &project.simple().to_string())]).await? {
            // Every machine labeled with a project is one of its units.
            let a = assignment(&m)?.ok_or_else(|| {
                anyhow::anyhow!("machine '{}' is labeled for project {project} but carries no '{MD_UNIT}' metadata", name_of(&m))
            })?;
            let hash = a.node.unit(&a.unit).map(|u| u.hash.clone()).unwrap_or_default();
            let at = |state| UnitObservation { copy_id: a.node.node.copy_id.clone(), unit: a.unit.clone(), hash: hash.clone(), state };
            match m.get("status").and_then(Value::as_str) {
                Some("RUNNING") => match self.agent(&m, reqwest::Method::GET, HOST_OBSERVE).await {
                    Ok(resp) => out.extend(resp.json::<Vec<UnitObservation>>().await?),
                    // Booting: the agent is not up yet.
                    Err(e) => out.push(at(UnitRunState::NotReady { why: format!("its machine's agent does not answer yet: {e:#}") })),
                },
                Some("PROVISIONING" | "STAGING") => out.push(at(UnitRunState::Starting)),
                Some("REPAIRING") => out.push(at(UnitRunState::NotReady { why: "Compute Engine is repairing its machine".into() })),
                _ => out.push(at(UnitRunState::Stopped)),
            }
        }
        Ok(out)
    }

    async fn copies(&self) -> anyhow::Result<Vec<NodeRef>> {
        // A machine or disk that cannot be read as part of a copy cannot be
        // judged, so it is left alone and logged as an error naming it, so a
        // person can look; failing the whole listing would stop every
        // other copy's sweep. (`observe`, which asks about one project,
        // fails loudly instead.)
        let mut seen = BTreeMap::new();
        for m in self.machines(&[]).await? {
            match assignment(&m) {
                Ok(Some(a)) => {
                    seen.insert(a.node.node.copy_id.clone(), a.node.node);
                }
                Ok(None) => {}
                Err(e) => tracing::error!(machine = %name_of(&m), error = %e, "a machine's unit assignment does not read; leaving it alone"),
            }
        }
        // A terminated copy that kept disks has no machine left; its
        // disks name it. Only a copy's disks carry `weft-copy` (the
        // install's own machine disks do not).
        let install = format!("labels.{}={}", weft_core::infra::INSTALL_LABEL, self.install.label_value());
        for d in self.list("disks", &install).await?.iter().filter(|d| label(d, "weft-copy").is_some()) {
            match disk_copy(d) {
                Ok(copy) => {
                    seen.entry(copy.copy_id.clone()).or_insert(copy);
                }
                Err(e) => tracing::error!(disk = %name_of(d), error = %e, "a copy's disk names no copy; leaving it alone"),
            }
        }
        Ok(seen.into_values().collect())
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
                same_network: matches!(ep.expose, Expose::SameNetwork)
                    .then(|| url.split_once("://").map_or(url.as_str(), |(_, r)| r).split('/').next().unwrap_or_default().to_string()),
            }),
            EndpointTarget::Unit { unit, container, port } => {
                let number = node
                    .unit(unit)
                    .and_then(|u| u.unit.containers.iter().find(|c| &c.name == container))
                    .and_then(|c| c.ports.iter().find(|p| &p.name == port))
                    .map(|p| p.port)
                    .ok_or_else(|| anyhow::anyhow!("node '{}': no port '{port}' on '{container}' of '{unit}'", node.node.node))?;
                let machine = self
                    .machine(&node.node, unit)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("unit '{unit}' of '{}' has no machine yet", node.node.node))?;
                let ip = internal_ip(&machine).ok_or_else(|| anyhow::anyhow!("the machine of '{unit}' has no address yet"))?;
                Ok(EndpointAt {
                    url: format!("http://{ip}:{number}"),
                    install_url: format!("http://{ip}:{number}"),
                    same_network: matches!(ep.expose, Expose::SameNetwork).then(|| format!("{ip}:{number}")),
                })
            }
        }
    }

    async fn logs(&self, node: &NodeRef, unit: &str, from: &LogsFrom) -> anyhow::Result<Vec<LogStream>> {
        let machine = self
            .machine(node, unit)
            .await?
            .ok_or_else(|| anyhow::anyhow!("unit '{unit}' of '{}' has no machine", node.node))?;
        let from: String = url::form_urlencoded::byte_serialize(serde_json::to_string(from)?.as_bytes()).collect();
        Ok(self.agent(&machine, reqwest::Method::GET, &format!("{HOST_LOGS}?from={from}")).await?.json().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::infra::{Gpu, MachineShape, Unit};

    fn unit(machine: MachineShape) -> ResolvedUnit {
        ResolvedUnit { unit: Unit { name: "main".into(), machine, ..Default::default() }, hash: "h".into() }
    }

    #[test]
    fn a_unit_gets_the_cheapest_e2_machine_that_holds_it() {
        let sized = |cpu: &str, memory: &str| {
            machine_shape("z", &unit(MachineShape { cpu: Some(cpu.into()), memory: Some(memory.into()), ..Default::default() }))
                .unwrap()
                .machine_type
        };
        assert_eq!(machine_shape("z", &unit(MachineShape::default())).unwrap().machine_type, "e2-micro");
        assert_eq!(sized("0.25", "1Gi"), "e2-micro");
        assert_eq!(sized("250m", "1536Mi"), "e2-small");
        assert_eq!(sized("1", "4Gi"), "e2-medium");
        assert_eq!(sized("1", "5Gi"), "e2-custom-2-5120");
        assert_eq!(sized("3", "6Gi"), "e2-custom-4-6144");
        let err = machine_shape("z", &unit(MachineShape { cpu: Some("2".into()), memory: Some("64Gi".into()), ..Default::default() })).unwrap_err();
        assert!(format!("{err}").contains("machine.cpu"));
    }

    #[test]
    fn a_gpu_unit_gets_the_family_its_accelerator_attaches_to() {
        let t4 = machine_shape("z", &unit(MachineShape { cpu: Some("4".into()), gpu: Some(Gpu { kind: "nvidia-tesla-t4".into(), count: 1 }), ..Default::default() })).unwrap();
        assert_eq!(t4, Shape { machine_type: "n1-standard-4".into(), accelerator: Some("nvidia-tesla-t4".into()), gpus: 1 });
        let l4 = machine_shape("z", &unit(MachineShape { gpu: Some(Gpu { kind: "nvidia-l4".into(), count: 2 }), ..Default::default() })).unwrap();
        assert_eq!(l4.machine_type, "g2-standard-24");
        assert!(machine_shape("z", &unit(MachineShape { gpu: Some(Gpu { kind: "amd-mi300".into(), count: 1 }), ..Default::default() })).is_err());
    }

    #[test]
    fn a_gpu_unit_gets_the_cheapest_machine_that_holds_its_memory_or_is_refused() {
        let gpu = |kind: &str, count: u32, cpu: &str, memory: &str| {
            machine_shape(
                "z",
                &unit(MachineShape { cpu: Some(cpu.into()), memory: Some(memory.into()), gpu: Some(Gpu { kind: kind.into(), count }), kind: None }),
            )
        };
        assert_eq!(gpu("nvidia-tesla-t4", 1, "2", "32Gi").unwrap().machine_type, "n1-highmem-8", "32 GB on a T4 is not a 7.5 GB n1-standard-2");
        assert_eq!(gpu("nvidia-tesla-t4", 1, "2", "12Gi").unwrap().machine_type, "n1-highmem-2");
        assert_eq!(gpu("nvidia-tesla-t4", 1, "3", "1Gi").unwrap().machine_type, "n1-standard-4");
        assert_eq!(gpu("nvidia-tesla-v100", 8, "96", "300Gi").unwrap().machine_type, "n1-standard-96");
        let over = gpu("nvidia-tesla-v100", 1, "13", "1Gi").unwrap_err().to_string();
        assert!(over.contains("at most 12 CPUs"), "{over}");
        assert!(gpu("nvidia-tesla-t4", 1, "2", "400Gi").is_err());
        assert!(gpu("nvidia-tesla-t4", 3, "2", "1Gi").is_err());
        assert!(gpu("nvidia-tesla-p100", 1, "2", "1Gi").is_err());
        assert_eq!(gpu("nvidia-l4", 1, "2", "40Gi").unwrap().machine_type, "g2-standard-12");
        assert_eq!(gpu("nvidia-l4", 1, "20", "1Gi").unwrap().machine_type, "g2-standard-32");
        assert!(gpu("nvidia-l4", 2, "32", "1Gi").is_err());
    }

    #[test]
    fn unset_numbers_are_the_containers_own_limits() {
        use weft_core::infra::Image;
        let c = |cpu: &str, memory: &str| {
            Container::new("c", Image::Local { name: "c".into() })
                .with_limits(Limits { cpu: Some(cpu.into()), memory: Some(memory.into()) })
        };
        let u = |containers, init_containers, machine| ResolvedUnit {
            unit: Unit { name: "main".into(), containers, init_containers, machine, ..Default::default() },
            hash: "h".into(),
        };
        let two = vec![c("0.2", "512Mi"), c("0.1", "64Mi")];
        assert_eq!(machine_shape("z", &u(two.clone(), vec![], MachineShape::default())).unwrap().machine_type, "e2-small");
        // An init container runs alone, so it counts against the sum, not into it.
        assert_eq!(machine_shape("z", &u(two.clone(), vec![c("0.1", "3Gi")], MachineShape::default())).unwrap().machine_type, "e2-medium");
        // The unit's own numbers win over its containers'.
        let set = MachineShape { cpu: Some("0.25".into()), memory: Some("1Gi".into()), ..Default::default() };
        assert_eq!(machine_shape("z", &u(vec![c("2", "2Gi")], vec![], set)).unwrap().machine_type, "e2-micro");
    }

    #[test]
    fn a_named_machine_type_is_used_as_is() {
        let named = |kind: &str, gpu: Option<Gpu>| machine_shape("z", &unit(MachineShape { kind: Some(kind.into()), gpu, ..Default::default() }));
        assert_eq!(named("n2-highmem-8", None).unwrap(), Shape { machine_type: "n2-highmem-8".into(), accelerator: None, gpus: 0 });
        let t4 = named("n1-standard-8", Some(Gpu { kind: "nvidia-tesla-t4".into(), count: 2 })).unwrap();
        assert_eq!((t4.accelerator.as_deref(), t4.gpus), (Some("nvidia-tesla-t4"), 2));
        let g2 = named("g2-standard-8", Some(Gpu { kind: "nvidia-l4".into(), count: 1 })).unwrap();
        assert_eq!((g2.accelerator, g2.gpus), (None, 1));
        assert!(named("N2 Standard", None).is_err());
    }

    #[test]
    fn disk_sizes_round_up_to_whole_gb() {
        assert_eq!(size_gb("10Gi").unwrap(), 10);
        assert_eq!(size_gb("500Mi").unwrap(), 1);
        assert!(size_gb("ten").is_err());
    }

    #[test]
    fn a_disk_names_its_copy_by_its_description() {
        let copy = NodeRef { tenant: "t".into(), project: uuid::Uuid::from_u128(1), node: "one.db".into(), copy_id: "wn-1".into() };
        let disk = json!({ "name": "d", "description": serde_json::to_string(&copy).unwrap() });
        assert_eq!(disk_copy(&disk).unwrap(), copy);
        let err = disk_copy(&json!({ "name": "d" })).unwrap_err().to_string();
        assert!(err.contains("carries no description"), "{err}");
    }
}
