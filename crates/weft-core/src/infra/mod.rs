//! Infra typed surface + pure helpers shared by every party that
//! needs to reason about an `InfraSpec`:
//!
//! - **supervisor** hashes, compiles, decides skip / fresh / replace
//!   and applies when it claims an `Apply` lifecycle command (the
//!   engine only ships the spec and waits);
//! - **tests** round-trip specs through compile to pin manifest
//!   shapes.
//!
//! The dispatcher does NOT call into the compile or apply path. It
//! routes lifecycle commands and writes their outcomes; supervisor
//! does the actual cluster work.

mod compile;
mod hash;
mod instance;
pub mod types;

pub use instance::{Instance, INSTANCE_ENV, MAX_INSTANCE_NAME};

pub use compile::{
    compile, door_service_name, tenant_public_path, tenant_public_url, unit_image_refs, CompileContext, CompileError,
};
pub use hash::hash_spec;
pub use types::*;

/// The whole node-port range this runtime's clusters use, inclusive.
///
/// Narrow on purpose. Kubernetes allocates a node port itself, from
/// whatever range the apiserver was given, and guarantees it is unique
/// across the cluster. Weft's part is only to make sure every port it
/// could pick is one the machine can actually reach, and on a local
/// install that means publishing the range on loopback: 36 ports is a
/// handful of port mappings, where Kubernetes' own default of 2768
/// would be absurd.
///
/// So nothing here allocates. Asking the apiserver for a specific
/// number would need a cluster-wide view of what is taken, and the one
/// component that applies these manifests is deliberately scoped to
/// the namespaces it owns. Letting the apiserver choose keeps that
/// scoping intact and removes the bookkeeping entirely.
///
/// The runtime's own three front doors pin 30097-30099 inside it.
/// SYNC: NODE_PORTS <-> crates/weft-cli/src/commands/daemon.rs (the
///       kind node's apiserver range and its loopback publications),
///       and MappedPort's three pinned ports, which must sit inside it
pub const NODE_PORTS: std::ops::RangeInclusive<u16> = 30064..=30099;

/// The label `weft.dev/node` on every Kubernetes object an infra node
/// owns: what a selector reads to find the node's pods (`weft infra
/// logs --node`, the health loop).
pub const NODE_LABEL: &str = "weft.dev/node";

/// The annotation carrying the node's id as it is, for a person
/// reading `kubectl describe`. A label cannot hold it: a node inside
/// an included file has an id like `@src:setup.store`, and a label
/// value allows only letters, digits, `-`, `_` and `.`, at most 63 of
/// them.
pub const NODE_ID_ANNOTATION: &str = "weft.dev/node-id";

/// The label value that stands for a node id: the id's characters a
/// label may hold, lowercased, runs of anything else collapsed to one
/// `-`, cut to 40, then `-` and 8 hex digits of the id's SHA-256, so
/// two ids that clean to the same text (`a.b` and `a-b`) never share
/// a value and a long id never overruns the 63 the label allows.
/// Every writer and every reader of [`NODE_LABEL`] goes through here:
/// the manifests, the selectors, and the health loop mapping a pod
/// back to its node.
pub fn node_label_value(node_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(node_id.as_bytes());
    let short: String = digest.iter().take(4).map(|b| format!("{b:02x}")).collect();
    let mut prefix: String = name_segment(node_id).chars().take(40).collect();
    while prefix.ends_with('-') {
        prefix.pop();
    }
    if prefix.is_empty() {
        return format!("n-{short}");
    }
    format!("{prefix}-{short}")
}

/// A string as a Kubernetes name segment may hold it: lowercase
/// letters and digits, runs of anything else collapsed to one `-`,
/// none at either end. The caller bounds the length (a name is at
/// most 63 characters, and most names carry a suffix).
pub fn name_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_dash = false;
    for c in s.chars() {
        let lc = c.to_ascii_lowercase();
        if lc.is_ascii_alphanumeric() {
            out.push(lc);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    out.trim_matches('-').to_string()
}

#[cfg(test)]
mod label_tests {
    use super::*;

    fn is_label_value(s: &str) -> bool {
        s.len() <= 63
            && s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
            && s.starts_with(|c: char| c.is_ascii_alphanumeric())
            && s.ends_with(|c: char| c.is_ascii_alphanumeric())
    }

    #[test]
    fn name_segment_keeps_only_what_a_name_may_hold() {
        assert_eq!(name_segment("Foo_Bar-123"), "foo-bar-123");
        assert_eq!(name_segment("a/b/c"), "a-b-c");
        assert_eq!(name_segment("--leading--"), "leading");
        assert_eq!(name_segment("@src:setup.store"), "src-setup-store");
    }

    /// An id from an included file is legal on a label, reads as the
    /// id it stands for, and cannot collide with an id that cleans to
    /// the same text.
    #[test]
    fn a_node_label_value_is_legal_readable_and_unique() {
        let inside = node_label_value("@src:setup.store");
        assert!(is_label_value(&inside), "{inside}");
        assert!(inside.starts_with("src-setup-store-"), "{inside}");
        assert_ne!(node_label_value("a.b"), node_label_value("a-b"));
        assert_eq!(node_label_value("store"), node_label_value("store"));
        let long = node_label_value(&"@src:very:deep:path:".repeat(5));
        assert!(is_label_value(&long), "{long}");
        let odd = node_label_value("@@@");
        assert!(is_label_value(&odd), "{odd}");
    }
}

#[cfg(test)]
mod node_port_tests {
    use super::NODE_PORTS;

    /// The runtime's own three front doors are pinned numbers, and the
    /// apiserver is told to allocate only inside this range, so a
    /// pinned port outside it could never be granted.
    #[test]
    fn the_runtime_s_own_ports_sit_inside_the_range() {
        for pinned in [30097u16, 30098, 30099] {
            assert!(NODE_PORTS.contains(&pinned), "{pinned} is outside {NODE_PORTS:?}");
        }
        // Every one of them is published on the machine, so the range
        // has to stay small enough for that to be sane.
        assert!(NODE_PORTS.clone().count() <= 64, "too many ports to publish");
    }
}
