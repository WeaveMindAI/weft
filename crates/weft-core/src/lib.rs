// Runtime modules: the execution context, buses, storage streaming, caller
// connections, cancellation, timer signals, and signed routing tokens. Pull
// tokio/futures/bytes (no wasm32 support), so they are gated behind the
// `runtime` feature (default on). The browser WASM parse build turns `runtime`
// off and compiles only the pure type layer below.
#[cfg(feature = "runtime")]
pub mod bus;
#[cfg(feature = "runtime")]
pub mod caller;
pub mod access;
#[cfg(feature = "runtime")]
pub mod caller_token;
#[cfg(feature = "runtime")]
pub mod cancellation;
#[cfg(feature = "runtime")]
pub mod context;
pub mod error;
pub mod exec;
pub mod infra;
pub mod frames;
#[cfg(feature = "runtime")]
pub mod net;
pub mod node;
pub mod primitive;
pub mod project;
pub mod pulse;
pub mod running_policy;
// The predicate submodule is wire-pure and always available (the
// `SignalSpec` wire type in `primitive` carries `Vec<Predicate>`); the
// rest of the module (kind registry, typed kinds) is runtime-only and
// gated inside the module.
pub mod signal;
#[cfg(feature = "runtime")]
pub mod signed_token;
// Mostly pure (key grammar, wire types, marker builders: the compiler needs
// them, so the WASM parse build compiles them too); only its byte-stream
// aliases are runtime-gated, inside the module.
pub mod storage;
pub mod tag;
pub mod wait;
pub mod weft_type;

// Hosts the `stress_test!` macro (`#[macro_export]`). The macro is
// available to every workspace crate's tests; its expansion uses
// `futures::FutureExt::catch_unwind`, so consuming crates need
// `futures` as a dev-dependency. The module itself compiles
// unconditionally so the macro export is visible to downstream
// crates (a `#[cfg(test)]` gate here would only export the macro
// when weft-core's own tests build, which would defeat the purpose).
mod test_support;

/// Truncate a user-supplied string to at most `max_bytes` bytes,
/// walking back to a UTF-8 char boundary (a raw byte slice would
/// panic mid-character), and append a "[truncated, original N bytes]"
/// suffix. Each caller picks the cap its own channel needs (Postgres
/// NOTIFY producers, error previews, log fields); the shared helper
/// exists so no call site re-grows the boundary-panic bug.
pub fn truncate_user_string(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}... [truncated, original {} bytes]", &s[..end], s.len())
}

// Re-export `inventory` so the `register_signal_kind!` macro
// expanded in third-party crates (or other workspace crates) can
// reach the same crate version without adding a direct dep.
pub use async_trait;
pub use inventory;

// Re-export `serde_json` so code the `NodeManifest` derive GENERATES
// into node crates (the typed-inputs struct: structural inputs fall
// back to `Value`) reaches the same crate version without every node
// package declaring the dep.
pub use serde_json;

// Re-export `reqwest_middleware` so node code can NAME the client type
// `ctx.client` / `OpenedConnection::client` hand back (e.g. a helper fn
// taking `&weft::reqwest_middleware::ClientWithMiddleware`) without
// every node package declaring the dep.
#[cfg(feature = "runtime")]
pub use reqwest_middleware;

#[cfg(feature = "runtime")]
pub use bus::{
    BusCursor, BusEntry, BusEntryKind, BusHandle, BusInner, BusLookupError, BusMode, BusOptions,
    BusLiveness, BusParticipant, BusRegistry, CursorError, RegisterError, SendError, WaitError,
    WaitId,
};
#[cfg(feature = "runtime")]
pub use cancellation::CancellationFlag;
#[cfg(feature = "runtime")]
pub use context::{
    ContextHandle, EndpointHandle, EndpointMethod, ExecutionContext, Phase, ValueBag,
    StorageHandle,
};
pub use access::spec::{AccessSpec, AppRegistration};
pub use access::{Access, CredentialOwner};
pub use error::{node_error, NodeErrExt, WeftError, WeftResult};
pub use infra::{
    AccessMode, AutoscaleBehavior, AutoscaleMetric, AutoscaleSpec, ConfigSource,
    Container, ContainerPort, ContainerSecurityContext, EgressRule, Endpoint, EnvEntry, Expose,
    HttpHeader, Image, IngressRule, InfraProvisionContext, InfraSpec, Lifecycle,
    Mount, NetworkAccess, PodOptions, PodSecurityContext, PreStopHook, Probe, ProbeKind, Protocol,
    ProvisionContextError, Resources, ScalingPolicy, StopBehavior, TerminateBehavior, Toleration,
    Unit, UnitHealth, UnitKind, UpgradeBehavior, Volume, VolumeKind,
};
pub use frames::{LoopFrames, LoopIteration};
pub use node::{
    Condition, FormFieldPort, FormFieldSpec, InputSpec, MetadataCatalog,
    NodeFeatures, NodeManifest, NodeMetadata, NodeOutput, OutputSpec, RuleDiagnostic, RuleSeverity,
    Widget,
    ValidationLevel, ValidationRule,
};
// The `NodeManifest` DERIVE (same name as the trait, macro namespace):
// `#[derive(NodeManifest)]` on a node struct embeds the metadata.json
// sitting next to the node's source file.
pub use weft_node_derive::NodeManifest;
// The runtime node interface (`Node` + the runtime `NodeCatalog`) is gated:
// the parse/validate path uses only `MetadataCatalog` above.
#[cfg(feature = "runtime")]
pub use node::{Node, NodeCatalog};
pub use primitive::{
    AwaitedEntry, AwaitedEntryKind, ExecutionSnapshot, KickedNode, SignalAuth,
    SignalRouting, SignalSpec, SignalSurface, SuspensionInfo,
};
pub use project::{
    has_infra, Edge, EdgeIndex, GroupBoundary, GroupBoundaryRole, GroupDefinition, GroupKind,
    InputDefinition, NodeDefinition, PortDefinition, Position, ProjectDefinition,
};
pub use pulse::Pulse;
pub use running_policy::RunningPolicy;
#[cfg(feature = "runtime")]
pub use storage::{ByteRange, ByteStream, KeepTtl, StorageScope, StoredFileMeta, StoredFile};
pub use weft_type::{WeftPrimitive, WeftType};

/// The identity of ONE execution. A color IS an execution: every
/// execution is minted exactly one color at `ExecutionStarted`, every
/// journal event / pulse / node firing carries it, and when the
/// execution terminates the color is spent forever (a re-run is a new
/// execution with a NEW color). So "per color" always means "per
/// execution", never "per project" or "per anything reused across
/// runs". At most one worker drives a given color at a time (the
/// one-worker-per-color invariant keeps that color's journal a single
/// coherent stream); there is nothing to retry "across colors".
pub type Color = uuid::Uuid;

#[cfg(test)]
mod helper_tests {
    use super::*;

    #[test]
    fn truncate_user_string_short_passes_through() {
        assert_eq!(truncate_user_string("abc", 10), "abc");
    }

    #[test]
    fn truncate_user_string_walks_back_to_char_boundary() {
        // "é" is 2 bytes; cap at 3 lands mid-character and must walk
        // back instead of panicking.
        let s = "aéé";
        let out = truncate_user_string(s, 3);
        assert!(out.starts_with("aé"), "boundary-safe prefix: {out}");
        assert!(out.contains("[truncated, original 5 bytes]"), "suffix names size: {out}");
    }

}
