//! Compute the `applied_spec_hash` that drives the supervisor's
//! skip-vs-replace decision on the next apply for the same node.
//!
//! We hash the TYPED `InfraSpec` (post-validation), not the compiled
//! manifest bytes. Hashing the typed spec means:
//!   - changes to `compile.rs` output (annotations, labels, k8s field
//!     ordering) don't invalidate every existing hash;
//!   - the hash is stable as long as the user-authored spec is stable;
//!   - HashMap-iteration order can't leak in because every map field
//!     on `InfraSpec` is a `BTreeMap` (or a typed Vec).
//!
//! Determinism: `serde_json::to_string` walks structs in
//! struct-field declaration order and BTreeMaps in key order. With
//! the workspace's default `serde_json` features (no
//! `preserve_order`), object keys also serialize in BTreeMap order.
//! Any new HashMap added to `InfraSpec` MUST switch to `BTreeMap` or
//! this guarantee breaks.

use anyhow::Result;
use sha2::{Digest, Sha256};

use super::types::InfraSpec;

/// Stable hash of an `InfraSpec` for skip-vs-replace decisions.
///
/// `image_tags` is mixed in so a tag rebuild (same spec, fresh
/// `weft-infra-foo:abc` → `weft-infra-foo:xyz`) produces a different
/// hash and triggers a re-apply.
pub fn hash_spec(
    spec: &InfraSpec,
    image_tags: &std::collections::BTreeMap<String, String>,
) -> Result<String> {
    let spec_json = serde_json::to_string(spec)?;
    let tags_json = serde_json::to_string(image_tags)?;
    let mut hasher = Sha256::new();
    hasher.update(b"weft-infra-typed-v1\n");
    hasher.update(&(spec_json.len() as u64).to_le_bytes());
    hasher.update(spec_json.as_bytes());
    hasher.update(&(tags_json.len() as u64).to_le_bytes());
    hasher.update(tags_json.as_bytes());
    Ok(hex(&hasher.finalize()))
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
    use crate::infra::types::*;
    use std::collections::BTreeMap;

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

    #[test]
    fn hash_stable_across_calls() {
        let s = spec();
        let tags = BTreeMap::new();
        let h1 = hash_spec(&s, &tags).unwrap();
        let h2 = hash_spec(&s, &tags).unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
    }

    #[test]
    fn hash_changes_with_spec() {
        let s1 = spec();
        let mut s2 = spec();
        s2.units[0].containers[0].image = Image::Upstream {
            reference: "nginx:1.28".into(),
        };
        let tags = BTreeMap::new();
        assert_ne!(
            hash_spec(&s1, &tags).unwrap(),
            hash_spec(&s2, &tags).unwrap()
        );
    }

    #[test]
    fn hash_changes_with_image_tags() {
        let s = spec();
        let mut t1 = BTreeMap::new();
        t1.insert("bridge".to_string(), "weft-infra-bridge:abc".to_string());
        let mut t2 = BTreeMap::new();
        t2.insert("bridge".to_string(), "weft-infra-bridge:xyz".to_string());
        assert_ne!(
            hash_spec(&s, &t1).unwrap(),
            hash_spec(&s, &t2).unwrap()
        );
    }

    /// `on_upgrade` lives per-Unit and MUST contribute to the hash:
    /// changing the upgrade strategy is a spec change the supervisor
    /// has to re-apply. A `#[serde(skip)]` or field reorder that
    /// dropped it would silently break drift detection on upgrade.
    #[test]
    fn hash_changes_with_on_upgrade() {
        let s1 = spec(); // default Rolling
        let mut s2 = spec();
        s2.units[0].on_upgrade = UpgradeBehavior::Recreate;
        let tags = BTreeMap::new();
        assert_ne!(
            hash_spec(&s1, &tags).unwrap(),
            hash_spec(&s2, &tags).unwrap()
        );
    }

    /// The hash's determinism rests on the spec serializing with map
    /// keys in a STABLE (sorted) order. That holds because every
    /// spec-reachable map is a `BTreeMap` and serde_json (without the
    /// `preserve_order` feature) emits object keys sorted. This pins
    /// it at the serialization layer the hash actually uses: build a
    /// spec with an out-of-order multi-key map and assert the emitted
    /// JSON has those keys in sorted order. A regression to `HashMap`
    /// on the field, or serde_json gaining `preserve_order`, would
    /// flip the order and fail this, surfacing the nondeterminism
    /// instead of letting it cause silent rebuild loops.
    ///
    /// (The prior version inserted into a BTreeMap in two orders and
    /// asserted equal hashes: tautological, since BTreeMap sorts on
    /// insert regardless. This checks the real failure mode.)
    #[test]
    fn spec_serializes_map_keys_in_sorted_order() {
        let mut s = spec();
        let mut sel = BTreeMap::new();
        // Insert deliberately out of lexicographic order.
        sel.insert("zone".to_string(), "eu".to_string());
        sel.insert("arch".to_string(), "amd64".to_string());
        sel.insert("tier".to_string(), "infra".to_string());
        s.units[0].pod_options.node_selector = Some(sel);

        // Serialize via the same path `hash_spec` uses.
        let json = serde_json::to_string(&s).unwrap();
        let a = json.find("\"arch\"").expect("arch key present");
        let t = json.find("\"tier\"").expect("tier key present");
        let z = json.find("\"zone\"").expect("zone key present");
        assert!(
            a < t && t < z,
            "node_selector keys must serialize sorted (arch<tier<zone); \
             got arch@{a}, tier@{t}, zone@{z}. A HashMap field or serde_json \
             preserve_order would break hash determinism."
        );
    }
}
