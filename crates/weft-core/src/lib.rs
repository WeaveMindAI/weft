pub mod bus;
pub mod caller;
pub mod caller_token;
pub mod cancellation;
pub mod context;
pub mod error;
pub mod exec;
pub mod infra;
pub mod frames;
pub mod node;
pub mod primitive;
pub mod project;
pub mod pulse;
pub mod running_policy;
pub mod signal;
pub mod signed_token;
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

/// Serialize a `[u8; 8]` as a 16-char lowercase hex string at the
/// wire boundary. Used as `#[serde(with = "weft_core::hex_array8")]`
/// on bus payload-hash-prefix fields so the journal and dispatcher
/// event types ship hex strings instead of byte-array literals.
pub mod hex_array8 {
    use serde::{Deserialize, Deserializer, Serializer};

    const HEX_TBL: &[u8; 16] = b"0123456789abcdef";

    pub fn serialize<S: Serializer>(bytes: &[u8; 8], s: S) -> Result<S::Ok, S::Error> {
        let mut hex = [0u8; 16];
        for (i, b) in bytes.iter().enumerate() {
            hex[i * 2] = HEX_TBL[(b >> 4) as usize];
            hex[i * 2 + 1] = HEX_TBL[(b & 0xf) as usize];
        }
        s.serialize_str(std::str::from_utf8(&hex).expect("ascii hex"))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 8], D::Error> {
        let s = String::deserialize(d)?;
        // ASCII check first: byte-indexed slicing below would PANIC
        // (not error) on a 16-byte string containing a multi-byte
        // char straddling an odd offset, and this deserializer exists
        // exactly to guard the wire boundary with errors.
        if s.len() != 16 || !s.is_ascii() {
            return Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(&s),
                &"16 ascii hex chars",
            ));
        }
        let mut out = [0u8; 8];
        for i in 0..8 {
            out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|_| serde::de::Error::custom("invalid hex digit"))?;
        }
        Ok(out)
    }
}

// Re-export `inventory` so the `register_signal_kind!` macro
// expanded in third-party crates (or other workspace crates) can
// reach the same crate version without adding a direct dep.
pub use inventory;

pub use bus::{
    BusCursor, BusEntry, BusEntryKind, BusHandle, BusInner, BusLookupError, BusMode, BusOptions,
    BusLiveness, BusParticipant, BusRegistry, CursorError, RegisterError, SendError, WaitError,
    WaitId,
};
pub use cancellation::CancellationFlag;
pub use context::{
    ContextHandle, EndpointHandle, EndpointMethod, ExecutionContext, InputBag, Phase,
    StorageHandle,
};
pub use error::{WeftError, WeftResult};
pub use infra::{
    Access, AccessMode, AutoscaleBehavior, AutoscaleMetric, AutoscaleSpec, ConfigSource,
    Container, ContainerPort, ContainerSecurityContext, EgressRule, Endpoint, EnvEntry, Expose,
    HttpHeader, Image, IngressRule, InfraProvisionContext, InfraSpec, Lifecycle,
    Mount, PodOptions, PodSecurityContext, PreStopHook, Probe, ProbeKind, Protocol,
    ProvisionContextError, Resources, ScalingPolicy, StopBehavior, TerminateBehavior, Toleration,
    Unit, UnitHealth, UnitKind, UpgradeBehavior, Volume, VolumeKind,
};
pub use frames::{LoopFrames, LoopIteration};
pub use node::{
    Condition, FieldDef, FormFieldPort, FormFieldSpec, MetadataCatalog, Node, NodeCatalog,
    NodeFeatures, NodeMetadata, NodeOutput, PortDef, RuleDiagnostic, RuleSeverity,
    ValidationLevel, ValidationRule,
};
pub use primitive::{
    AwaitedEntry, AwaitedEntryKind, CostReport, ExecutionSnapshot, KickedNode, SignalAuth,
    SignalRouting, SignalSpec, SignalSurface, SuspensionInfo,
};
pub use project::{
    has_infra, Edge, EdgeIndex, GroupBoundary, GroupBoundaryRole, GroupDefinition, GroupKind,
    NodeDefinition, PortDefinition, Position, ProjectDefinition,
};
pub use pulse::Pulse;
pub use running_policy::RunningPolicy;
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

    #[test]
    fn hex_array8_round_trips() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct H(#[serde(with = "crate::hex_array8")] [u8; 8]);
        let h = H([0x00, 0x11, 0xab, 0xcd, 0xef, 0x01, 0x99, 0xff]);
        let json = serde_json::to_string(&h).unwrap();
        assert_eq!(json, "\"0011abcdef0199ff\"");
        let back: H = serde_json::from_str(&json).unwrap();
        assert_eq!(back.0, h.0);
    }

    #[test]
    fn hex_array8_rejects_malformed_without_panicking() {
        #[derive(serde::Deserialize)]
        struct H(#[serde(with = "crate::hex_array8")] [u8; 8]);
        // 16 BYTES but containing a multi-byte char straddling an odd
        // offset: byte-indexed slicing would panic; must Err instead.
        let weird = format!("\"0{}{}\"", '\u{00e9}', "0123456789012".get(..13).unwrap());
        let r: Result<H, _> = serde_json::from_str(&weird);
        assert!(r.is_err(), "non-ascii 16-byte string must error");
        let r: Result<H, _> = serde_json::from_str("\"zz11abcdef0199ff\"");
        assert!(r.is_err(), "non-hex digits must error");
        let r: Result<H, _> = serde_json::from_str("\"0011\"");
        assert!(r.is_err(), "wrong length must error");
    }
}
