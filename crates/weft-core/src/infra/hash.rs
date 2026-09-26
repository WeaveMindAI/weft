//! Compute the `applied_spec_hash` that drives the supervisor's
//! skip-vs-replace decision on the next apply for the same node.
//!
//! We hash the COMPILED manifests, exactly what gets applied. Anything
//! that changes what a unit runs changes the hash: the author's spec,
//! a rebuilt local image (its resolved tag is in the manifest), and a
//! change to how weft compiles a unit (a new pod field such as the DNS
//! settings, a label, a probe default). Hashing the typed spec instead
//! missed that last one, so a compile change never reached a unit
//! whose spec was untouched.
//!
//! The same spec for the same instance compiles to the same manifests:
//! the compile context (tenant, project, node, instance id, namespace,
//! install) is stable across applies of one row, since the supervisor
//! reuses the prior instance id.
//!
//! Determinism: object keys are sorted here, recursively, before
//! hashing. `serde_json` cannot be trusted to do it: a dependency
//! (`minillmlib`) turns on its `preserve_order` feature, which makes
//! every `Map` keep insertion order in any binary that links it.
//! Arrays keep the compiler's order, which follows the spec.

use sha2::{Digest, Sha256};
use serde_json::Value;

/// Stable hash of a node's compiled manifests (the output of
/// [`super::compile`]) for skip-vs-replace decisions.
pub fn hash_manifests(manifests: &[Value]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"weft-infra-compiled-v1\n");
    for manifest in manifests {
        let bytes = canonical(manifest).to_string();
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes.as_bytes());
    }
    hex(&hasher.finalize())
}

/// `value` with every object's keys in sorted order, whatever order
/// they were inserted in. Inserting sorted keys into a fresh `Map`
/// serializes sorted under both `Map` backings.
fn canonical(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            Value::Object(
                keys.into_iter()
                    .map(|k| (k.clone(), canonical(&map[k])))
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical).collect()),
        other => other.clone(),
    }
}

fn hex(bytes: &[u8]) -> String {
    static HEX: &[u8] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::compile::tests::ctx;
    use crate::infra::types::*;
    use crate::infra::compile;
    use serde_json::json;

    fn spec() -> InfraSpec {
        InfraSpec {
            units: vec![Unit {
                name: "u".into(),
                kind: UnitKind::Deployment,
                containers: vec![Container::new(
                    "c",
                    Image::Upstream {
                        reference: "nginx:1.27".into(),
                    },
                )],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn hash(spec: &InfraSpec) -> String {
        hash_manifests(&compile(spec, &ctx()).unwrap())
    }

    #[test]
    fn hash_stable_across_calls() {
        let h1 = hash(&spec());
        assert_eq!(h1, hash(&spec()));
        assert_eq!(h1.len(), 64);
    }

    #[test]
    fn hash_changes_with_spec() {
        let mut s2 = spec();
        s2.units[0].containers[0].image = Image::Upstream {
            reference: "nginx:1.28".into(),
        };
        assert_ne!(hash(&spec()), hash(&s2));
    }

    /// `on_upgrade` lives per-Unit and MUST reach the hash: changing
    /// the upgrade strategy is a change the supervisor has to re-apply.
    #[test]
    fn hash_changes_with_on_upgrade() {
        let mut s2 = spec();
        s2.units[0].on_upgrade = UpgradeBehavior::Recreate;
        assert_ne!(hash(&spec()), hash(&s2));
    }

    /// A change only to how weft compiles a unit (the spec untouched)
    /// changes the hash, so it rolls out on the next apply.
    #[test]
    fn hash_changes_with_compile_output_alone() {
        let manifests = compile(&spec(), &ctx()).unwrap();
        let mut recompiled = manifests.clone();
        let deployment = recompiled
            .iter_mut()
            .find(|m| m["kind"] == "Deployment")
            .unwrap();
        deployment["spec"]["template"]["spec"]["dnsConfig"] =
            json!({ "options": [{ "name": "ndots", "value": "2" }] });
        assert_ne!(hash_manifests(&manifests), hash_manifests(&recompiled));
    }

    /// Key order inside a manifest cannot leak into the hash: objects
    /// hash sorted whatever order they were built in.
    #[test]
    fn object_key_order_does_not_matter() {
        let mut a = serde_json::Map::new();
        a.insert("zone".into(), json!(1));
        a.insert("arch".into(), json!(2));
        let mut b = serde_json::Map::new();
        b.insert("arch".into(), json!(2));
        b.insert("zone".into(), json!(1));
        assert_eq!(
            hash_manifests(&[Value::Object(a)]),
            hash_manifests(&[Value::Object(b)])
        );
    }
}
