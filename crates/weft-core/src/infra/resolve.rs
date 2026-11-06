//! From what a node asked for to what a platform runs.
//!
//! [`resolve`] checks an [`InfraSpec`] (every name a unit, a container, a
//! port or a volume is referred by exists; a disk is mounted by one unit
//! only; a public path is a plain path; a unit runs containers or one
//! process, and a process only what a process takes) and swaps every locally built image
//! name for the ref its version build registered. What comes out is a
//! [`ResolvedNode`]: the same spec with nothing left to look up, plus one
//! hash per unit, which is how an apply tells a unit that changed (replace
//! it) from one that did not (leave it running). Pure: the same inputs
//! give the same output, on every platform.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::types::{Endpoint, EndpointTarget, Expose, Image, InfraSpec, MachineShape, Process, Unit, VolumeKind};

/// Which copy of which infra node of which project: what a platform names
/// everything it runs for the node by.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeRef {
    pub tenant: String,
    pub project: uuid::Uuid,
    /// The node as the program spells it (`db`, `one.db`).
    pub node: String,
    /// Which copy this is, from [`NodeRef::copy_id`]: the same
    /// for the same (project, node, instance) forever, so every apply of
    /// the copy, including the first one after a terminate, finds its
    /// disks under the same names.
    pub copy_id: String,
}

impl NodeRef {
    /// The id of the copy of `node` that `instance` gets (`None`: the shared
    /// copy). Derived, never minted: a disk listed in `keepOnTerminate`
    /// outlives the copy's row, and the next apply can only find it again
    /// if it names the copy the same way. Two copies with one id never
    /// run side by side: an apply over a copy still being terminated
    /// finishes that terminate before it creates anything.
    pub fn copy_id(project: uuid::Uuid, node: &str, instance: Option<&crate::instance::InstanceId>) -> String {
        let pid = super::name_segment(&project.to_string()).chars().take(8).collect::<String>();
        let nid = super::name_segment(node).chars().take(20).collect::<String>();
        let key = format!("{project}|{node}|{}", instance.map(|m| m.as_str()).unwrap_or(""));
        // An instance id is not name-safe and may be long, so it enters as
        // the digest only. 40 bits: no collision across one project's
        // copies of one node.
        let digest = Sha256::digest(key.as_bytes());
        let hex: String = digest.iter().take(5).map(|b| format!("{b:02x}")).collect();
        format!("wn-{pid}-{nid}-{hex}")
    }

    /// Whether this is an instance's copy rather than the shared one: the
    /// shared copy's id is the one derived with no instance.
    pub fn is_instance_copy(&self) -> bool {
        self.copy_id != Self::copy_id(self.project, &self.node, None)
    }

    /// A short name that is the same for the same copy and safe as a
    /// resource name on every platform: lowercase letters, digits and `-`,
    /// starting with a letter, at most 40 characters, so a platform may
    /// add a unit or volume name after it and stay under 63.
    // SYNC: the `wi-` prefix <-> deploy/terraform/gcp/machines.tf (the machine events' filter)
    pub fn resource_base(&self) -> String {
        self.resource_base_within(None)
    }

    /// [`NodeRef::resource_base`] for a resource whose names other
    /// installs share (a laptop's Docker daemon, which every install on it
    /// uses): `scope`, the install, goes in after `wi-`, and the readable
    /// part shortens so the name stays within the same 40 characters.
    /// `None` is the plain one.
    pub fn resource_base_within(&self, scope: Option<&str>) -> String {
        let digest = Sha256::digest(format!("{}|{}|{}", self.project, self.node, self.copy_id).as_bytes());
        let hex: String = digest.iter().take(6).map(|b| format!("{b:02x}")).collect();
        let prefix = match scope {
            Some(scope) => format!("wi-{scope}-"),
            None => "wi-".to_string(),
        };
        let room = 40usize.saturating_sub(prefix.len() + hex.len() + 1).min(20);
        let mut readable: String = super::name_segment(&self.node).chars().take(room).collect();
        while readable.ends_with('-') {
            readable.pop();
        }
        if readable.is_empty() {
            format!("{prefix}{hex}")
        } else {
            format!("{prefix}{readable}-{hex}")
        }
    }
}

/// An infra node ready to run: every image resolved, every reference
/// checked.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedNode {
    pub node: NodeRef,
    pub units: Vec<ResolvedUnit>,
    pub spec: InfraSpec,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedUnit {
    /// The unit, every container's image an `Image::Upstream` ref.
    pub unit: Unit,
    /// Changes whenever anything the unit runs with changes: its own
    /// definition, the disks it mounts, the endpoints it serves.
    pub hash: String,
}

impl ResolvedNode {
    pub fn unit(&self, name: &str) -> Option<&ResolvedUnit> {
        self.units.iter().find(|u| u.unit.name == name)
    }

    /// Changes whenever anything the copy runs with changes: any unit's
    /// hash, the set of units, the endpoints (an endpoint to an outside
    /// address included), the disks terminate keeps and the health rules
    /// the node writes. What an apply compares with the copy's last one to
    /// skip a copy already running as asked, so a change to any of them
    /// stamps the copy's row again.
    pub fn hash(&self) -> String {
        let units: BTreeMap<&str, &str> = self.units.iter().map(|u| (u.unit.name.as_str(), u.hash.as_str())).collect();
        let mut doc = serde_json::json!({
            "units": units,
            "endpoints": self.spec.endpoints,
            "keep_on_terminate": self.spec.keep_on_terminate,
        });
        // Only when written, so a copy whose node writes none keeps the
        // hash it had before rules could be written.
        if let Some(rules) = &self.spec.health_rules {
            doc["health_rules"] = serde_json::json!(rules);
        }
        let mut hasher = Sha256::new();
        hasher.update(b"weft-infra-node-v1\n");
        hasher.update(canonical(&doc).to_string().as_bytes());
        hasher.finalize().iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResolveError {
    /// The tag map has no entry for a local image the spec names. It is
    /// filled by the version build, so on a project whose infra was never
    /// started it is empty whatever the node's metadata declares.
    #[error("infra node '{node}' has no image built yet for '{name}': run `weft infra start` \
             (or, for a node marked `@per_instance`, `weft activate`), which builds the node's \
             images and registers them")]
    MissingLocalImage { node: String, name: String },
    #[error("infra node '{node}': {what} '{name}' is declared twice")]
    Duplicate { node: String, what: &'static str, name: String },
    #[error("infra node '{node}': {what} name '{name}' may only hold lowercase letters, digits \
             and '-', start with a letter, and be at most {max} characters (it becomes part \
             of the names the platform gives what it runs)")]
    BadName { node: String, what: &'static str, name: String, max: usize },
    #[error("infra node '{node}': endpoint '{endpoint}' names {what} '{name}', which is not declared")]
    EndpointTargetMissing { node: String, endpoint: String, what: &'static str, name: String },
    #[error("infra node '{node}': unit '{unit}' mounts volume '{volume}', which is not declared")]
    MountMissing { node: String, unit: String, volume: String },
    #[error("infra node '{node}': disk '{volume}' is mounted by units '{first}' and '{second}'; a \
             disk attaches to one machine, so give each unit a disk of its own")]
    DiskShared { node: String, volume: String, first: String, second: String },
    #[error("infra node '{node}': unit '{unit}' runs no container and no process; give it containers, or one process")]
    EmptyUnit { node: String, unit: String },
    #[error("infra node '{node}': unit '{unit}' runs a process and containers; a unit runs one or the other, so \
             give the process a unit of its own")]
    UnitRunsBoth { node: String, unit: String },
    #[error("infra node '{node}': unit '{unit}' runs a process and sets its {what}, which a process does not take: it \
             runs as you, on the machine weft runs on, sharing it with everything else you run; remove its {what}")]
    ProcessTakesNo { node: String, unit: String, what: &'static str },
    #[error("infra node '{node}': unit '{unit}' runs a process with an empty command; name the program to run first")]
    ProcessCommandEmpty { node: String, unit: String },
    #[error("infra node '{node}': unit '{unit}' writes the file '{path}', which is not a plain path inside its folder. \
             It must be relative, and every segment between slashes must be non-empty and not '.' or '..'")]
    ProcessFilePath { node: String, unit: String, path: String },
    #[error("infra node '{node}': the process of unit '{unit}' sets {name} to a value with a line break, which an \
             environment variable does not carry; write text of several lines into one of its files instead")]
    ProcessEnvLineBreak { node: String, unit: String, name: String },
    #[error("infra node '{node}': the process of unit '{unit}' gets port '{port}' in the variable '{env}', which is \
             no variable name: use uppercase letters, digits and '_', not starting with a digit")]
    ProcessPortEnvInvalid { node: String, unit: String, port: String, env: String },
    #[error("infra node '{node}': the readiness check of unit '{unit}' reads port '{port}', which its process does not \
             declare")]
    ProcessReadinessPortMissing { node: String, unit: String, port: String },
    #[error("infra node '{node}': the process of unit '{unit}' needs '{need}' at build time, and a process is never \
             built; give the need `stage = \"run\"` (the default), or drop it")]
    ProcessBuildNeed { node: String, unit: String, need: String },
    #[error("infra node '{node}': the process of unit '{unit}' needs '{need}', whose recipe {why}")]
    ProcessNeedRecipe { node: String, unit: String, need: String, why: String },
    #[error("infra node '{node}': container '{container}' sets {name}, which weft sets itself on \
             every container (where to push the values that changed); read it instead of setting it")]
    ReservedEnv { node: String, container: String, name: &'static str },
    #[error("infra node '{node}': container '{container}' sets {name} to a value with a line break, \
             which a platform cannot pass in an environment variable; hand text of several lines over \
             as the container's arguments instead (one line each)")]
    EnvLineBreak { node: String, container: String, name: String },
    #[error("infra node '{node}': endpoint '{endpoint}' is public at path '{path}', which is not \
             a plain path. It must start with '/', and every segment between slashes must be \
             non-empty, not '.' or '..', and use only letters, digits, '-', '_', '.' and '~'")]
    PublicPathInvalid { node: String, endpoint: String, path: String },
    #[error("infra node '{node}': endpoint '{endpoint}' points at '{url}', which is not an \
             http(s) or tcp address")]
    ExternalUrlInvalid { node: String, endpoint: String, url: String },
    #[error("infra node '{node}': endpoint '{endpoint}' points at an outside address, so it \
             cannot be exposed: whoever runs that service decides who reaches it")]
    ExternalExposed { node: String, endpoint: String },
    #[error("infra node '{node}': endpoint '{endpoint}' lives inside another node's endpoint at '{path}', which is \
             not a plain path under it. It must start with '/', name at least one segment, and every segment between \
             slashes must be non-empty, not '.' or '..', and use only letters, digits, '-', '_', '.' and '~'")]
    WithinPathInvalid { node: String, endpoint: String, path: String },
    #[error("infra node '{node}': endpoint '{endpoint}' lives inside another node's endpoint, so it cannot be \
             exposed: open the endpoint it lives inside instead")]
    WithinExposed { node: String, endpoint: String },
    #[error("infra node '{node}': keep_on_terminate names '{volume}', which is not a declared disk")]
    KeptVolumeMissing { node: String, volume: String },
    #[error("infra node '{node}': health rule '{rule}' reads unit '{unit}', which is not declared; \
             name one of the node's units, or leave `unit` out to read any of them")]
    HealthRuleUnitMissing { node: String, rule: String, unit: String },
}

const MAX_UNIT: usize = 12;
const MAX_VOLUME: usize = 12;

fn plain(node: &str) -> String {
    crate::project::plain_id(node)
}

fn check_name(node: &str, what: &'static str, name: &str, max: usize) -> Result<(), ResolveError> {
    let ok = !name.is_empty()
        && name.len() <= max
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !name.ends_with('-');
    if ok {
        Ok(())
    } else {
        Err(ResolveError::BadName { node: plain(node), what, name: name.to_string(), max })
    }
}

fn unique<'a>(node: &str, what: &'static str, names: impl Iterator<Item = &'a str>) -> Result<(), ResolveError> {
    let mut seen = HashSet::new();
    for n in names {
        if !seen.insert(n) {
            return Err(ResolveError::Duplicate { node: plain(node), what, name: n.to_string() });
        }
    }
    Ok(())
}

/// Resolve one image to the ref the platform runs: an upstream ref as
/// written, a local name through the version's tag map.
pub fn resolve_image(image: &Image, node: &str, tags: &BTreeMap<String, String>) -> Result<String, ResolveError> {
    match image {
        Image::Upstream { reference } => Ok(reference.clone()),
        Image::Local { name } => tags
            .get(name)
            .cloned()
            .ok_or_else(|| ResolveError::MissingLocalImage { node: plain(node), name: name.clone() }),
    }
}

/// Every image ref a unit's containers resolve to: what an apply of the
/// unit puts on the platform, and so what image reclamation must keep
/// while the unit runs.
pub fn unit_image_refs(unit: &Unit, node: &str, tags: &BTreeMap<String, String>) -> Result<BTreeSet<String>, ResolveError> {
    unit.containers
        .iter()
        .chain(unit.init_containers.iter())
        .map(|c| resolve_image(&c.image, node, tags))
        .collect()
}

fn plain_public_path(path: &str) -> bool {
    match path.strip_prefix('/') {
        None => false,
        Some("") => true,
        Some(body) => body.strip_suffix('/').unwrap_or(body).split('/').all(|seg| {
            !seg.is_empty()
                && seg != "."
                && seg != ".."
                && seg.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~'))
        }),
    }
}

fn check_endpoint(node: &str, spec: &InfraSpec, ep: &Endpoint) -> Result<(), ResolveError> {
    let missing = |what, name: &str| ResolveError::EndpointTargetMissing {
        node: plain(node),
        endpoint: ep.name.clone(),
        what,
        name: name.to_string(),
    };
    match &ep.target {
        EndpointTarget::Unit { unit, container, port } => {
            let u = spec.units.iter().find(|u| &u.name == unit).ok_or_else(|| missing("unit", unit))?;
            let c = u.containers.iter().find(|c| &c.name == container).ok_or_else(|| missing("container", container))?;
            c.ports.iter().find(|p| &p.name == port).ok_or_else(|| missing("port", port))?;
        }
        EndpointTarget::Process { unit, port } => {
            let u = spec.units.iter().find(|u| &u.name == unit).ok_or_else(|| missing("unit", unit))?;
            let p = u.process.as_ref().ok_or_else(|| missing("the process of unit", unit))?;
            p.ports.iter().find(|p| &p.name == port).ok_or_else(|| missing("process port", port))?;
        }
        EndpointTarget::Within { path, .. } => {
            if !plain_public_path(path) || path == "/" {
                return Err(ResolveError::WithinPathInvalid { node: plain(node), endpoint: ep.name.clone(), path: path.clone() });
            }
            if ep.expose != Expose::Project {
                return Err(ResolveError::WithinExposed { node: plain(node), endpoint: ep.name.clone() });
            }
        }
        EndpointTarget::External { url } => {
            let scheme_ok = ["http://", "https://", "tcp://"].iter().any(|s| url.starts_with(s) && url.len() > s.len());
            if !scheme_ok {
                return Err(ResolveError::ExternalUrlInvalid { node: plain(node), endpoint: ep.name.clone(), url: url.clone() });
            }
            if ep.expose != Expose::Project {
                return Err(ResolveError::ExternalExposed { node: plain(node), endpoint: ep.name.clone() });
            }
        }
    }
    if let Expose::Public { path } = &ep.expose {
        if !plain_public_path(path) {
            return Err(ResolveError::PublicPathInvalid { node: plain(node), endpoint: ep.name.clone(), path: path.clone() });
        }
    }
    Ok(())
}

/// The variable every container of an infra unit finds the address it
/// pushes changed values to in (`weft_platform_traits::unit_agent::VALUES_PATH`
/// on the agent beside it): set by the host, never by a spec.
// SYNC: VALUES_URL_ENV <-> catalog/postgres/database/images/credential/bootstrap.py (WEFT_VALUES_URL)
pub const VALUES_URL_ENV: &str = "WEFT_VALUES_URL";

/// Check `spec` and resolve its images for the copy `node`.
pub fn resolve(spec: &InfraSpec, node: &NodeRef, tags: &BTreeMap<String, String>) -> Result<ResolvedNode, ResolveError> {
    let id = node.node.as_str();
    unique(id, "unit", spec.units.iter().map(|u| u.name.as_str()))?;
    unique(id, "volume", spec.volumes.iter().map(|v| v.name.as_str()))?;
    unique(id, "endpoint", spec.endpoints.iter().map(|e| e.name.as_str()))?;
    for u in &spec.units {
        check_name(id, "unit", &u.name, MAX_UNIT)?;
        if let Some(process) = &u.process {
            check_process(id, u, process)?;
            continue;
        }
        if u.containers.is_empty() {
            return Err(ResolveError::EmptyUnit { node: plain(id), unit: u.name.clone() });
        }
        unique(id, "container", u.containers.iter().chain(u.init_containers.iter()).map(|c| c.name.as_str()))?;
        if let Some(c) = u.containers.iter().chain(u.init_containers.iter()).find(|c| c.env.iter().any(|e| e.name == VALUES_URL_ENV)) {
            return Err(ResolveError::ReservedEnv { node: plain(id), container: c.name.clone(), name: VALUES_URL_ENV });
        }
        for c in u.containers.iter().chain(u.init_containers.iter()) {
            if let Some(e) = c.env.iter().find(|e| e.value.contains(['\n', '\r'])) {
                return Err(ResolveError::EnvLineBreak { node: plain(id), container: c.name.clone(), name: e.name.clone() });
            }
        }
    }
    for v in &spec.volumes {
        check_name(id, "volume", &v.name, MAX_VOLUME)?;
    }
    for ep in &spec.endpoints {
        check_endpoint(id, spec, ep)?;
    }
    let rules = spec.health_rules.as_deref().unwrap_or_default();
    unique(id, "health rule", rules.iter().map(|r| r.name.as_str()))?;
    for rule in rules {
        if let Some(unit) = rule.when.units_named().into_iter().find(|unit| !spec.units.iter().any(|u| u.name == *unit)) {
            return Err(ResolveError::HealthRuleUnitMissing { node: plain(id), rule: rule.name.clone(), unit: unit.to_string() });
        }
    }
    for kept in &spec.keep_on_terminate {
        let is_disk = spec.volumes.iter().any(|v| &v.name == kept && matches!(v.kind, VolumeKind::Disk { .. }));
        if !is_disk {
            return Err(ResolveError::KeptVolumeMissing { node: plain(id), volume: kept.clone() });
        }
    }
    // Every mount names a declared volume, and a disk is mounted by one
    // unit only.
    let mut disk_owner: BTreeMap<&str, &str> = BTreeMap::new();
    for u in &spec.units {
        for c in u.containers.iter().chain(u.init_containers.iter()) {
            for m in &c.mounts {
                let v = spec.volumes.iter().find(|v| v.name == m.volume).ok_or_else(|| ResolveError::MountMissing {
                    node: plain(id),
                    unit: u.name.clone(),
                    volume: m.volume.clone(),
                })?;
                if matches!(v.kind, VolumeKind::Disk { .. }) {
                    match disk_owner.get(v.name.as_str()) {
                        Some(owner) if *owner != u.name => {
                            return Err(ResolveError::DiskShared {
                                node: plain(id),
                                volume: v.name.clone(),
                                first: owner.to_string(),
                                second: u.name.clone(),
                            })
                        }
                        _ => {
                            disk_owner.insert(&v.name, &u.name);
                        }
                    }
                }
            }
        }
    }

    let mut units = Vec::with_capacity(spec.units.len());
    for u in &spec.units {
        let mut resolved = u.clone();
        for c in resolved.containers.iter_mut().chain(resolved.init_containers.iter_mut()) {
            c.image = Image::Upstream { reference: resolve_image(&c.image, id, tags)? };
        }
        let hash = unit_hash(&resolved, spec);
        units.push(ResolvedUnit { unit: resolved, hash });
    }
    Ok(ResolvedNode { node: node.clone(), units, spec: spec.clone() })
}

/// Check the unit `unit` of node `node`, which runs `process`: nothing of
/// a container unit set, every file a plain path in its folder, every
/// variable one an environment carries, every port named once and found
/// in a variable of its own, and only run-time needs.
fn check_process(node: &str, unit: &Unit, process: &Process) -> Result<(), ResolveError> {
    let (node, name) = (plain(node), unit.name.clone());
    if !unit.containers.is_empty() || !unit.init_containers.is_empty() {
        return Err(ResolveError::UnitRunsBoth { node, unit: name });
    }
    if unit.machine != MachineShape::default() {
        return Err(ResolveError::ProcessTakesNo { node, unit: name, what: "machine" });
    }
    if unit.fs_group.is_some() {
        return Err(ResolveError::ProcessTakesNo { node, unit: name, what: "fsGroup" });
    }
    if process.command.first().is_none_or(|program| program.is_empty()) {
        return Err(ResolveError::ProcessCommandEmpty { node, unit: name });
    }
    if let Some(path) = process.files.keys().find(|path| !plain_relative_path(path)) {
        return Err(ResolveError::ProcessFilePath { node, unit: name, path: path.clone() });
    }
    if let Some(e) = process.env.iter().find(|e| e.value.contains(['\n', '\r'])) {
        return Err(ResolveError::ProcessEnvLineBreak { node, unit: name, name: e.name.clone() });
    }
    unique(&node, "process port", process.ports.iter().map(|p| p.name.as_str()))?;
    if let Some(p) = process.ports.iter().find(|p| !variable_name(&p.env)) {
        return Err(ResolveError::ProcessPortEnvInvalid { node, unit: name, port: p.name.clone(), env: p.env.clone() });
    }
    // A port's variable is the host's to set: one the spec sets too, or
    // a second port's, would leave the process reading the wrong number.
    unique(&node, "environment variable", process.env.iter().map(|e| e.name.as_str()).chain(process.ports.iter().map(|p| p.env.as_str())))?;
    if let Some(readiness) = &process.readiness {
        if !process.ports.iter().any(|p| p.name == readiness.port) {
            return Err(ResolveError::ProcessReadinessPortMissing { node, unit: name, port: readiness.port.clone() });
        }
    }
    if let Some(need) = process.needs.iter().find(|n| n.stage != crate::needs::NeedStage::Run) {
        return Err(ResolveError::ProcessBuildNeed { node, unit: name, need: need.name.clone() });
    }
    for need in &process.needs {
        if let Some(why) = need.recipe.as_ref().and_then(|r| r.refusal()) {
            return Err(ResolveError::ProcessNeedRecipe { node, unit: name, need: need.name.clone(), why });
        }
    }
    Ok(())
}

/// Whether `path` names a file inside a folder: relative, and no segment
/// empty, `.` or `..`.
fn plain_relative_path(path: &str) -> bool {
    !path.is_empty() && !path.starts_with('/') && path.split('/').all(|seg| !seg.is_empty() && seg != "." && seg != "..")
}

/// Whether `name` is an environment variable's name: uppercase letters,
/// digits and `_`, not starting with a digit.
fn variable_name(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_uppercase() || c == '_') && name.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// The unit's own definition plus every volume it mounts and every
/// endpoint that targets it, canonically serialized and hashed. A process
/// unit's definition holds its files, so a changed script is a changed
/// unit.
fn unit_hash(unit: &Unit, spec: &InfraSpec) -> String {
    let mounted: BTreeSet<&str> = unit
        .containers
        .iter()
        .chain(unit.init_containers.iter())
        .flat_map(|c| c.mounts.iter().map(|m| m.volume.as_str()))
        .collect();
    let volumes: Vec<_> = spec.volumes.iter().filter(|v| mounted.contains(v.name.as_str())).collect();
    let endpoints: Vec<_> = spec
        .endpoints
        .iter()
        .filter(|e| e.target.unit() == Some(unit.name.as_str()))
        .collect();
    let doc = serde_json::json!({ "unit": unit, "volumes": volumes, "endpoints": endpoints });
    let mut hasher = Sha256::new();
    hasher.update(b"weft-infra-unit-v1\n");
    hasher.update(canonical(&doc).to_string().as_bytes());
    hasher.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn canonical(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            serde_json::Value::Object(keys.into_iter().map(|k| (k.clone(), canonical(&map[k]))).collect())
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(items.iter().map(canonical).collect()),
        other => other.clone(),
    }
}

/// The front-door path of a public endpoint declaring `path` (no trailing
/// slash; the prefix alone for a declared `/`). Under `/infra/<project>/
/// <copy_id>`, so no endpoint can shadow a weft route or another node's.
// SYNC: public infra path <-> crates/weft-dispatcher/src/infra_door.rs (the route)
pub fn public_path(project: uuid::Uuid, copy_id: &str, path: &str) -> String {
    format!("/infra/{project}/{copy_id}{}", path.trim_end_matches('/'))
}

/// The full outside address of a public endpoint: the install's public
/// base joined with its [`public_path`]. Handed to whoever calls in, so
/// built from the configured base, never from a request's host.
pub fn public_url(front_door: &str, public_path: &str) -> String {
    format!("{}{public_path}", front_door.trim_end_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::types::*;

    #[test]
    fn a_copy_keeps_its_id_and_each_instance_gets_its_own() {
        let p = uuid::Uuid::from_u128(7);
        let alice = crate::instance::InstanceId::new("alice").unwrap();
        let bob = crate::instance::InstanceId::new("bob").unwrap();
        let shared = NodeRef::copy_id(p, "node_one", None);
        assert_eq!(shared, NodeRef::copy_id(p, "node_one", None));
        assert_ne!(shared, NodeRef::copy_id(p, "node_one", Some(&alice)));
        assert_ne!(NodeRef::copy_id(p, "node_one", Some(&alice)), NodeRef::copy_id(p, "node_one", Some(&bob)));
        assert_ne!(shared, NodeRef::copy_id(uuid::Uuid::from_u128(8), "node_one", None));
        assert!(shared.starts_with("wn-") && shared.len() <= 50);
        assert!(shared.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
    }

    fn node() -> NodeRef {
        NodeRef { tenant: "local".into(), project: uuid::Uuid::from_u128(1), node: "db".into(), copy_id: "i-1".into() }
    }

    fn spec() -> InfraSpec {
        InfraSpec {
            units: vec![Unit {
                name: "db".into(),
                containers: vec![
                    Container::new("pg", Image::Upstream { reference: "postgres:18".into() })
                        .with_ports(vec![ContainerPort { name: "sql".into(), port: 5432, protocol: Protocol::Tcp }])
                        .with_mounts(vec![Mount::new("store", "/data")]),
                    Container::new("cred", Image::Local { name: "credential".into() }),
                ],
                ..Default::default()
            }],
            volumes: vec![Volume { name: "store".into(), kind: VolumeKind::Disk { size: "1Gi".into(), class: None } }],
            endpoints: vec![Endpoint {
                name: "sql".into(),
                target: EndpointTarget::Unit { unit: "db".into(), container: "pg".into(), port: "sql".into() },
                expose: Expose::SameNetwork,
            }],
            keep_on_terminate: Vec::new(),
            health_rules: None,
            notes: Vec::new(),
        }
    }

    fn tags() -> BTreeMap<String, String> {
        BTreeMap::from([("credential".to_string(), "reg/weft-infra-credential:ab".to_string())])
    }

    #[test]
    fn local_images_resolve_to_their_built_ref() {
        let r = resolve(&spec(), &node(), &tags()).unwrap();
        let images: Vec<_> = r.units[0].unit.containers.iter().map(|c| c.image.clone()).collect();
        assert_eq!(images[1], Image::Upstream { reference: "reg/weft-infra-credential:ab".into() });
        let e = resolve(&spec(), &node(), &BTreeMap::new()).unwrap_err();
        assert!(matches!(e, ResolveError::MissingLocalImage { .. }), "{e}");
    }

    /// No platform passes a line break in an environment variable, so a
    /// spec that sets one is refused here, naming it, rather than when a
    /// platform gets to start the container.
    #[test]
    fn an_environment_value_with_a_line_break_is_refused() {
        let mut broken = spec();
        broken.units[0].containers[1].env.push(EnvEntry::new("SETTINGS", "a = 1\nb = 2"));
        let e = resolve(&broken, &node(), &tags()).unwrap_err();
        assert!(matches!(&e, ResolveError::EnvLineBreak { container, name, .. } if container == "cred" && name == "SETTINGS"), "{e}");
    }

    #[test]
    fn the_unit_hash_follows_what_the_unit_runs_with() {
        let a = resolve(&spec(), &node(), &tags()).unwrap().units[0].hash.clone();
        assert_eq!(a, resolve(&spec(), &node(), &tags()).unwrap().units[0].hash.clone(), "stable");
        let mut bigger = spec();
        bigger.volumes[0].kind = VolumeKind::Disk { size: "2Gi".into(), class: None };
        assert_ne!(a, resolve(&bigger, &node(), &tags()).unwrap().units[0].hash, "a mounted disk counts");
        let mut retagged = tags();
        retagged.insert("credential".into(), "reg/weft-infra-credential:cd".into());
        assert_ne!(a, resolve(&spec(), &node(), &retagged).unwrap().units[0].hash, "a new image counts");
    }

    #[test]
    fn the_node_hash_follows_its_units_endpoints_and_kept_disks() {
        let a = resolve(&spec(), &node(), &tags()).unwrap().hash();
        assert_eq!(a, resolve(&spec(), &node(), &tags()).unwrap().hash(), "stable");
        let mut retagged = tags();
        retagged.insert("credential".into(), "reg/weft-infra-credential:cd".into());
        assert_ne!(a, resolve(&spec(), &node(), &retagged).unwrap().hash(), "a unit that changed counts");
        let mut outside = spec();
        outside.endpoints.push(Endpoint {
            name: "ext".into(),
            target: EndpointTarget::External { url: "tcp://10.0.0.9:5432".into() },
            expose: Expose::Project,
        });
        assert_ne!(a, resolve(&outside, &node(), &tags()).unwrap().hash(), "an endpoint no unit serves counts");
        let mut kept = spec();
        kept.keep_on_terminate = vec!["store".into()];
        assert_ne!(a, resolve(&kept, &node(), &tags()).unwrap().hash(), "what terminate keeps counts");
        let mut ruled = spec();
        ruled.health_rules = Some(crate::infra::health::built_in_rules());
        assert_ne!(a, resolve(&ruled, &node(), &tags()).unwrap().hash(), "the rules the node writes count");
    }

    /// A health rule naming a unit the node does not declare is refused
    /// when the spec is checked, naming the rule and the unit; so is a
    /// second rule of the same name. A rule naming a declared unit, or
    /// none, passes.
    #[test]
    fn a_health_rule_reads_only_the_nodes_own_units() {
        use crate::infra::health::{HealthAction, HealthCondition, HealthRule, DEFAULT_RULE_TIMEOUT_SECONDS};
        let rule = |name: &str, unit: Option<&str>| HealthRule {
            name: name.into(),
            when: HealthCondition::Not {
                condition: Box::new(HealthCondition::UnitFlaky { unit: unit.map(str::to_string) }),
            },
            action: HealthAction::BringBack,
            timeout_seconds: DEFAULT_RULE_TIMEOUT_SECONDS,
        };
        let mut s = spec();
        s.health_rules = Some(vec![rule("back", Some("db")), rule("any", None)]);
        resolve(&s, &node(), &tags()).unwrap();
        s.health_rules = Some(vec![rule("back", Some("cache"))]);
        let e = resolve(&s, &node(), &tags()).unwrap_err();
        assert_eq!(e, ResolveError::HealthRuleUnitMissing { node: "db".into(), rule: "back".into(), unit: "cache".into() });
        assert!(e.to_string().contains("health rule 'back' reads unit 'cache'"), "{e}");
        s.health_rules = Some(vec![rule("back", None), rule("back", None)]);
        assert!(matches!(resolve(&s, &node(), &tags()).unwrap_err(), ResolveError::Duplicate { what: "health rule", .. }));
    }

    #[test]
    fn broken_references_are_refused_by_name() {
        let mut s = spec();
        s.endpoints[0].target = EndpointTarget::Unit { unit: "db".into(), container: "pg".into(), port: "http".into() };
        assert!(resolve(&s, &node(), &tags()).unwrap_err().to_string().contains("port 'http'"));
        let mut s = spec();
        s.units[0].containers[0].mounts[0].volume = "nope".into();
        assert!(matches!(resolve(&s, &node(), &tags()).unwrap_err(), ResolveError::MountMissing { .. }));
        let mut s = spec();
        let mut second = s.units[0].clone();
        second.name = "replica".into();
        s.units.push(second);
        assert!(matches!(resolve(&s, &node(), &tags()).unwrap_err(), ResolveError::DiskShared { .. }));
        let mut s = spec();
        s.units[0].name = "Bad_Name".into();
        assert!(matches!(resolve(&s, &node(), &tags()).unwrap_err(), ResolveError::BadName { .. }));
        let mut s = spec();
        s.keep_on_terminate = vec!["ghost".into()];
        assert!(matches!(resolve(&s, &node(), &tags()).unwrap_err(), ResolveError::KeptVolumeMissing { .. }));
    }

    fn process_spec() -> InfraSpec {
        InfraSpec {
            units: vec![Unit {
                name: "server".into(),
                process: Some(
                    Process::new(vec!["python3".into(), "server.py".into()])
                        .with_file("server.py", "serve()\n")
                        .with_env(vec![EnvEntry::new("MODE", "prod")])
                        .with_ports(vec![ProcessPort::new("http", "PORT")])
                        .with_readiness(ProcessReadiness::http("http", "/health")),
                ),
                ..Default::default()
            }],
            endpoints: vec![Endpoint {
                name: "api".into(),
                target: EndpointTarget::Process { unit: "server".into(), port: "http".into() },
                expose: Expose::Public { path: "/agent".into() },
            }],
            ..Default::default()
        }
    }

    /// A process unit resolves as written (it has no image to resolve),
    /// and its hash follows its files and the endpoints to its ports.
    #[test]
    fn a_process_unit_resolves_and_its_files_are_in_its_hash() {
        let r = resolve(&process_spec(), &node(), &BTreeMap::new()).unwrap();
        assert_eq!(r.units[0].unit, process_spec().units[0]);
        let mut edited = process_spec();
        edited.units[0].process.as_mut().unwrap().files.insert("server.py".into(), "serve(2)\n".into());
        assert_ne!(r.units[0].hash, resolve(&edited, &node(), &BTreeMap::new()).unwrap().units[0].hash, "a changed script counts");
        let mut opened = process_spec();
        opened.endpoints[0].expose = Expose::SameNetwork;
        assert_ne!(r.units[0].hash, resolve(&opened, &node(), &BTreeMap::new()).unwrap().units[0].hash, "an endpoint to it counts");
        assert!(unit_image_refs(&r.units[0].unit, "db", &BTreeMap::new()).unwrap().is_empty(), "no image to keep");
    }

    /// Everything a process unit cannot be is refused by name: a unit
    /// running a process and containers, or neither; what only a container
    /// takes; a file outside its folder; a port variable that is no
    /// variable, or one the spec sets too; a readiness check on a port it
    /// does not have; a need at build time; an endpoint asking a process
    /// for a container.
    #[test]
    fn what_a_process_unit_cannot_be_is_refused_by_name() {
        let refused = |change: &dyn Fn(&mut InfraSpec)| {
            let mut s = process_spec();
            change(&mut s);
            resolve(&s, &node(), &BTreeMap::new()).unwrap_err()
        };
        fn process(s: &mut InfraSpec) -> &mut Process {
            s.units[0].process.as_mut().unwrap()
        }
        let both = refused(&|s| s.units[0].containers = spec().units[0].containers.clone());
        assert_eq!(both, ResolveError::UnitRunsBoth { node: "db".into(), unit: "server".into() });
        let neither = refused(&|s| s.units[0].process = None);
        assert!(matches!(&neither, ResolveError::EmptyUnit { unit, .. } if unit == "server"), "{neither}");
        assert!(neither.to_string().contains("no container and no process"), "{neither}");
        let gpu = refused(&|s| s.units[0].machine.gpu = Some(Gpu { kind: "nvidia-l4".into(), count: 1 }));
        assert_eq!(gpu, ResolveError::ProcessTakesNo { node: "db".into(), unit: "server".into(), what: "machine" });
        assert!(matches!(refused(&|s| s.units[0].fs_group = Some(1)), ResolveError::ProcessTakesNo { what: "fsGroup", .. }));
        assert!(matches!(refused(&|s| process(s).command = Vec::new()), ResolveError::ProcessCommandEmpty { .. }));
        for path in ["/etc/passwd", "../out", "a//b", "./x", ""] {
            let e = refused(&|s| {
                process(s).files.insert(path.into(), "x".into());
            });
            assert_eq!(e, ResolveError::ProcessFilePath { node: "db".into(), unit: "server".into(), path: path.into() }, "{path}");
        }
        let broken = refused(&|s| process(s).env.push(EnvEntry::new("SETTINGS", "a\nb")));
        assert!(matches!(&broken, ResolveError::ProcessEnvLineBreak { name, .. } if name == "SETTINGS"), "{broken}");
        let lower = refused(&|s| process(s).ports[0].env = "port".into());
        assert!(matches!(&lower, ResolveError::ProcessPortEnvInvalid { env, .. } if env == "port"), "{lower}");
        let clash = refused(&|s| process(s).env.push(EnvEntry::new("PORT", "1")));
        assert_eq!(clash, ResolveError::Duplicate { node: "db".into(), what: "environment variable", name: "PORT".into() });
        let twice = refused(&|s| process(s).ports.push(ProcessPort::new("http", "OTHER_PORT")));
        assert!(matches!(twice, ResolveError::Duplicate { what: "process port", .. }));
        let unread = refused(&|s| process(s).readiness = Some(ProcessReadiness::http("admin", "/")));
        assert!(matches!(&unread, ResolveError::ProcessReadinessPortMissing { port, .. } if port == "admin"), "{unread}");
        let build = refused(&|s| {
            process(s).needs.push(crate::needs::Need {
                name: "cc".into(),
                stage: crate::needs::NeedStage::Build,
                check: vec!["cc".into()],
                install: BTreeMap::new(),
                how: None,
                recipe: None,
            })
        });
        assert!(matches!(&build, ResolveError::ProcessBuildNeed { need, .. } if need == "cc"), "{build}");
        let outside = refused(&|s| {
            let recipe = crate::needs::Recipe { folder: "../etc".into(), script: BTreeMap::new(), bin: Vec::new(), prelude: String::new() };
            process(s).needs.push(crate::needs::Need {
                name: "tool".into(),
                stage: crate::needs::NeedStage::Run,
                check: vec!["tool".into()],
                install: BTreeMap::new(),
                how: None,
                recipe: Some(recipe),
            })
        });
        assert!(matches!(&outside, ResolveError::ProcessNeedRecipe { need, .. } if need == "tool"), "{outside}");
        let no_port = refused(&|s| s.endpoints[0].target = EndpointTarget::Process { unit: "server".into(), port: "ssh".into() });
        assert!(no_port.to_string().contains("process port 'ssh'"), "{no_port}");
        let container = refused(&|s| s.endpoints[0].target = EndpointTarget::Unit { unit: "server".into(), container: "main".into(), port: "http".into() });
        assert!(container.to_string().contains("container 'main'"), "{container}");
        // And a process target on a container unit.
        let mut s = spec();
        s.endpoints[0].target = EndpointTarget::Process { unit: "db".into(), port: "sql".into() };
        let e = resolve(&s, &node(), &tags()).unwrap_err();
        assert!(e.to_string().contains("the process of unit 'db'"), "{e}");
    }

    /// A node with no unit and one endpoint inside another node's
    /// resolves; the endpoint's path is a plain path under it, and it is
    /// never exposed.
    #[test]
    fn an_endpoint_inside_another_nodes_is_checked_and_never_exposed() {
        let computer = crate::infra::InfraHandle::new("box", "api", None);
        let mut s = InfraSpec {
            endpoints: vec![Endpoint {
                name: "api".into(),
                target: EndpointTarget::Within { infra: computer, path: "/apps/claude".into() },
                expose: Expose::Project,
            }],
            ..Default::default()
        };
        let r = resolve(&s, &node(), &BTreeMap::new()).unwrap();
        assert!(r.units.is_empty());
        let wire = serde_json::to_value(&s.endpoints[0].target).unwrap();
        assert_eq!(wire["kind"], "within");
        assert_eq!(wire["path"], "/apps/claude");
        assert_eq!(serde_json::from_value::<EndpointTarget>(wire).unwrap(), s.endpoints[0].target);
        for bad in ["apps", "/", "/apps/../x", "/a//b"] {
            let EndpointTarget::Within { path, .. } = &mut s.endpoints[0].target else { unreachable!() };
            *path = bad.into();
            assert!(matches!(resolve(&s, &node(), &BTreeMap::new()).unwrap_err(), ResolveError::WithinPathInvalid { .. }), "{bad}");
        }
        let EndpointTarget::Within { path, .. } = &mut s.endpoints[0].target else { unreachable!() };
        *path = "/apps/claude".into();
        s.endpoints[0].expose = Expose::SameNetwork;
        assert_eq!(resolve(&s, &node(), &BTreeMap::new()).unwrap_err(), ResolveError::WithinExposed { node: "db".into(), endpoint: "api".into() });
    }

    /// A record keeps an address as the plain string rows always held,
    /// and an endpoint inside another as the handle and path, followed one
    /// hop at every read: an inner copy not running says so naming it, and
    /// one inside yet another is refused.
    #[test]
    fn a_record_inside_another_endpoint_is_followed_one_hop() {
        let url = EndpointRecord::Url("http://127.0.0.1:9".into());
        assert_eq!(serde_json::to_value(&url).unwrap(), serde_json::json!("http://127.0.0.1:9"));
        assert_eq!(url.within(), None);
        let computer = crate::infra::InfraHandle::new("box", "api", None);
        let within = EndpointRecord::Within(WithinRecord { within: computer.clone(), path: "/apps/claude".into() });
        let wire = serde_json::to_value(&within).unwrap();
        assert_eq!(wire["path"], "/apps/claude");
        assert_eq!(serde_json::from_value::<EndpointRecord>(wire).unwrap(), within);
        assert_eq!(serde_json::from_value::<EndpointRecord>(serde_json::json!("http://x")).unwrap(), EndpointRecord::Url("http://x".into()));
        assert_eq!(within.within(), Some(&computer));
        assert_eq!(within.summary(), "inside endpoint 'api' of 'box' at /apps/claude");

        assert_eq!(url.address(None).unwrap(), "http://127.0.0.1:9");
        let inner = EndpointRecord::Url("http://127.0.0.1:5000/".into());
        assert_eq!(within.clone().address(Some(inner)).unwrap(), "http://127.0.0.1:5000/apps/claude");
        let down = within.clone().address(None).unwrap_err();
        assert_eq!(down, "the infra node 'box' this endpoint runs inside is not running: start it with `weft infra start`");
        let chained = within.clone().address(Some(within)).unwrap_err();
        assert!(chained.contains("followed one step only"), "{chained}");
    }

    #[test]
    fn an_outside_address_is_checked_and_never_exposed() {
        let mut s = spec();
        s.endpoints.push(Endpoint { name: "ext".into(), target: EndpointTarget::External { url: "tcp://10.0.0.9:5432".into() }, expose: Expose::Project });
        resolve(&s, &node(), &tags()).unwrap();
        s.endpoints[1].expose = Expose::SameNetwork;
        assert!(matches!(resolve(&s, &node(), &tags()).unwrap_err(), ResolveError::ExternalExposed { .. }));
        s.endpoints[1] = Endpoint { name: "ext".into(), target: EndpointTarget::External { url: "10.0.0.9".into() }, expose: Expose::Project };
        assert!(matches!(resolve(&s, &node(), &tags()).unwrap_err(), ResolveError::ExternalUrlInvalid { .. }));
    }

    #[test]
    fn public_paths_are_plain_and_live_under_the_node() {
        for ok in ["/", "/hooks", "/a/b.c~d/"] {
            assert!(plain_public_path(ok), "{ok}");
        }
        for bad in ["", "hooks", "//", "/a/../b", "/a%2Fb", "/a//b"] {
            assert!(!plain_public_path(bad), "{bad}");
        }
        let p = public_path(uuid::Uuid::from_u128(1), "i-1", "/hooks/");
        assert_eq!(p, "/infra/00000000-0000-0000-0000-000000000001/i-1/hooks");
        assert_eq!(public_url("https://x.example/", &p), format!("https://x.example{p}"));
    }

    #[test]
    fn a_resource_base_is_safe_and_names_the_copy() {
        let base = node().resource_base();
        assert!(base.len() <= 40, "{base}");
        assert!(base.starts_with("wi-db-"), "{base}");
        assert!(base.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
        let mut other = node();
        other.copy_id = "i-2".into();
        assert_ne!(base, other.resource_base());
    }

    #[test]
    fn a_scoped_resource_base_carries_its_scope_and_stays_as_short() {
        let mut long = node();
        long.node = "a-node-with-a-rather-long-name".into();
        assert_eq!(long.resource_base_within(None), long.resource_base());
        let one = long.resource_base_within(Some("resumab8d646"));
        let two = long.resource_base_within(Some("default"));
        assert!(one.starts_with("wi-resumab8d646-a-node"), "{one}");
        assert!(one.len() <= 40 && two.len() <= 40, "{one} {two}");
        assert_ne!(one, two);
        // The same copy under any scope: the same digest at the end.
        let tail = |s: &str| s.rsplit('-').next().unwrap().to_string();
        assert_eq!(tail(&one), tail(&long.resource_base()));
        // A scope never makes a name that starts the plain one's.
        assert!(!one.starts_with(&format!("{}-", long.resource_base())));
    }
}
