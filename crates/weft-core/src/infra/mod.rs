//! Infra typed surface + pure helpers shared by every party that
//! needs to reason about an `InfraSpec`:
//!
//! - **engine** compiles + hashes a freshly-provisioned spec so it
//!   can decide skip / fresh / replace before enqueuing a lifecycle
//!   command;
//! - **supervisor** compiles + applies (kubectl) when it claims an
//!   `Apply` lifecycle command;
//! - **tests** round-trip specs through compile to pin manifest
//!   shapes.
//!
//! The dispatcher does NOT call into the compile or apply path. It
//! routes lifecycle commands and writes their outcomes; supervisor
//! does the actual kubectl work.

mod compile;
mod hash;
pub mod types;

/// The fixed namespaces the runtime deploys into. Constants, not env
/// knobs: every manifest under deploy/k8s hardcodes them, so an
/// override in one reader would apply into one namespace and wait on
/// another. ONE definition for every Rust reader (CLI, broker, e2e);
/// the manifests restate the strings because YAML cannot import them.
// SYNC: namespace values <-> deploy/k8s/system-namespace.yaml,
//       deploy/k8s/db-namespace.yaml, deploy/k8s/dispatcher.yaml,
//       deploy/k8s/broker.yaml, deploy/k8s/ingress.yaml,
//       deploy/k8s/postgres.yaml, deploy/k8s/public-tunnel.yaml,
//       setup.sh (the --release db port-forward, the Running banner),
//       scripts/run-node-tests.sh (the cluster-up probe),
//       scripts/run-e2e.sh (the cluster-up probe + db port-forward),
//       docs/src/connections/public-address.md (the tunnel target URL)
pub const SYSTEM_NAMESPACE: &str = "weft-system";
pub const DB_NAMESPACE: &str = "weft-db";

pub use compile::{compile, unit_image_refs, CompileContext, CompileError};
pub use hash::hash_spec;
pub use types::*;
