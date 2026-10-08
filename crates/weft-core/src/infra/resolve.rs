//! From what a node asked for to what a platform runs.
//!
//! [`resolve`] checks an [`InfraSpec`] (every name a unit, a container, a
//! port or a volume is referred by exists; a disk is mounted by one unit
//! only; a public path is a plain path) and swaps every locally built image
//! name for the ref its version build registered. What comes out is a
//! [`ResolvedNode`]: the same spec with nothing left to look up, plus one
//! hash per unit, which is how an apply tells a unit that changed (replace
//! it) from one that did not (leave it running). Pure: the same inputs
//! give the same output, on every platform.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::types::{Endpoint, EndpointTarget, Expose, Image, InfraSpec, Unit, VolumeKind};

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
        let digest = Sha256::digest(format!("{}|{}|{}", self.project, self.node, self.copy_id).as_bytes());
        let hex: String = digest.iter().take(6).map(|b| format!("{b:02x}")).collect();
        let mut readable: String = super::name_segment(&self.node).chars().take(20).collect();
        while readable.ends_with('-') {
            readable.pop();
        }
        if readable.is_empty() {
            format!("wi-{hex}")
        } else {
            format!("wi-{readable}-{hex}")
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
    /// address included) and the disks terminate keeps. What an apply
    /// compares with the copy's last one to skip a copy already running
    /// as asked.
    pub fn hash(&self) -> String {
        let units: BTreeMap<&str, &str> = self.units.iter().map(|u| (u.unit.name.as_str(), u.hash.as_str())).collect();
        let doc = serde_json::json!({
            "units": units,
            "endpoints": self.spec.endpoints,
            "keep_on_terminate": self.spec.keep_on_terminate,
        });
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
    #[error("infra node '{node}': unit '{unit}' runs no container")]
    EmptyUnit { node: String, unit: String },
    #[error("infra node '{node}': container '{container}' sets {name}, which weft sets itself on \
             every container (where to push the values that changed); read it instead of setting it")]
    ReservedEnv { node: String, container: String, name: &'static str },
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
    #[error("infra node '{node}': keep_on_terminate names '{volume}', which is not a declared disk")]
    KeptVolumeMissing { node: String, volume: String },
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
        if u.containers.is_empty() {
            return Err(ResolveError::EmptyUnit { node: plain(id), unit: u.name.clone() });
        }
        unique(id, "container", u.containers.iter().chain(u.init_containers.iter()).map(|c| c.name.as_str()))?;
        if let Some(c) = u.containers.iter().chain(u.init_containers.iter()).find(|c| c.env.iter().any(|e| e.name == VALUES_URL_ENV)) {
            return Err(ResolveError::ReservedEnv { node: plain(id), container: c.name.clone(), name: VALUES_URL_ENV });
        }
    }
    for v in &spec.volumes {
        check_name(id, "volume", &v.name, MAX_VOLUME)?;
    }
    for ep in &spec.endpoints {
        check_endpoint(id, spec, ep)?;
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

/// The unit's own definition plus every volume it mounts and every
/// endpoint that targets it, canonically serialized and hashed.
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
        .filter(|e| matches!(&e.target, EndpointTarget::Unit { unit: u, .. } if *u == unit.name))
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
}
