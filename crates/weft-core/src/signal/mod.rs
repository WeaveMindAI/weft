//! Wake-signal kinds.
//!
//! A wake signal is "something the listener listens for on behalf of
//! a node." Each kind (timer, form, sse_subscribe, api_endpoint, ...) is
//! a plain data struct in this module. Node code constructs one and passes
//! it directly to `ctx.register_signal(...)` (entry trigger) or
//! `ctx.await_signal(...)` (mid-execution resume). The framework
//! projects the typed kind onto the internal `SignalSpec` wire
//! shape via the `Signal` trait.
//!
//! ## Adding a new kind
//!
//! 1. Create `weft-core/src/signal/<name>.rs`. Define the data struct,
//!    derive `Serialize` + `Deserialize`, `impl Signal for ...`,
//!    end the file with `register_signal_kind!(...)`.
//! 2. Add `pub mod <name>;` to this file.
//! 3. Create `weft-listener/src/kinds/<name>.rs` with the handler
//!    impl + registration.
//!
//! No central enum, no match dispatch. The framework discovers kinds
//! at startup via the `inventory` registry.

pub mod auth;
pub mod timer;
pub mod form;
pub mod sse_subscribe;
pub mod poll_endpoint;
pub mod socket_listen;
pub mod live_connection;

pub use auth::PublicEntryAuth;
pub use timer::{Timer, TimerSpec};
pub use form::{Form, FormSchema, FormField};
pub use sse_subscribe::SseSubscribe;
pub use poll_endpoint::PollEndpoint;
pub use socket_listen::{SocketFrame, SocketListen};
pub use live_connection::{
    protocol_for_tag, ApiEndpoint, Backpressure, DataType, ErrorMode, JournalMode,
    LiveConnectionConfig, LiveSocket, Protocol,
};

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::primitive::SignalSpec;

/// Trait every wake-signal kind implements. Carries:
///   - `TAG`: discriminant string used by the wire shape and the
///     listener-side handler registry.
///   - `validate`: per-kind config rules (cron parses, path
///     well-formed, etc). Called at register time.
///   - `consumer_kind`: optional kind-level metadata. Default
///     `None`. Kinds with a `consumer_kind` field (`Form` for
///     human-in-the-loop) override to surface it.
///
/// `is_resume` is NOT on the trait. Whether a registration is a
/// fresh entry or a paused-firing resume is decided by which
/// `ExecutionContext` method the author called.
pub trait Signal: Serialize + DeserializeOwned + Sized {
    /// Kind tag stored on the wire (`"timer"`, `"api_endpoint"`, ...).
    /// Used to route incoming specs to the right handler in the
    /// listener.
    const TAG: &'static str;

    /// Validate the kind's configuration. Override to surface
    /// kind-specific rules (cron expression parses, URL is http(s),
    /// etc.). Default: no-op.
    fn validate(&self) -> Result<(), String> {
        Ok(())
    }

    /// Optional consumer label for token-scoped enumeration. Set by
    /// kinds whose suspensions are processed by external consumers
    /// (browser extension, Slack bot). Default `None` = not
    /// consumer-listable. Kinds with a `consumer_kind` field
    /// (`Form`) override this to expose it.
    fn consumer_kind(&self) -> Option<&str> {
        None
    }
}

/// Project a typed kind into the wire-shape `SignalSpec`. The
/// entry-vs-resume distinction is dispatcher-flow metadata, NOT
/// part of the spec; it rides on the register request.
pub fn to_spec<K: Signal>(kind: K) -> SignalSpec {
    // Charset: consumer_kind round-trips through SQL `ANY($1)` and
    // URL paths, so it must match the tag charset. Catalog authors
    // get an immediate panic if they emit a bad value.
    let consumer_kind = kind.consumer_kind().map(|s| {
        crate::tag::validate_tag(s).unwrap_or_else(|e| {
            panic!("signal kind '{}' returned invalid consumer_kind '{s}': {e}", K::TAG)
        });
        s.to_string()
    });
    SignalSpec {
        kind: K::TAG.to_string(),
        config: serde_json::to_value(&kind).expect("kind serialization is infallible"),
        consumer_kind,
    }
}

/// Inventory entry registering a kind's tag + a JSON-driven validator.
/// Each `weft-core/src/signal/<name>.rs` file submits one of these.
/// The framework iterates the registry to validate any `SignalSpec`
/// without knowing the typed struct.
pub struct SignalKindEntry {
    pub tag: &'static str,
    /// Parse `config` as the typed kind and call `validate`. Returns
    /// the typed kind's error message verbatim, or "unknown kind"
    /// when the tag isn't registered.
    pub validate_json: fn(&Value) -> Result<(), String>,
}

inventory::collect!(SignalKindEntry);

/// Find a registered kind by tag, or None. Internal: callers go
/// through `validate_spec`.
fn lookup(tag: &str) -> Option<&'static SignalKindEntry> {
    inventory::iter::<SignalKindEntry>
        .into_iter()
        .find(|e| e.tag == tag)
}

/// Validate a wire-shape `SignalSpec`. Looks up the kind by tag,
/// runs its `validate_json`. Returns Err for both unknown tags and
/// kind-reported failures.
pub fn validate_spec(spec: &SignalSpec) -> Result<(), String> {
    let entry = lookup(&spec.kind)
        .ok_or_else(|| format!("unknown signal kind: '{}'", spec.kind))?;
    (entry.validate_json)(&spec.config)
}

/// Macro called at the bottom of each `weft-core/src/signal/<name>.rs`
/// file to register the kind in the inventory. The macro expands to
/// an `inventory::submit!` block that pulls the typed `validate` out
/// via `serde_json::from_value` and surfaces failures uniformly.
#[macro_export]
macro_rules! register_signal_kind {
    ($ty:ty) => {
        $crate::inventory::submit! {
            $crate::signal::SignalKindEntry {
                tag: <$ty as $crate::signal::Signal>::TAG,
                validate_json: |config: &::serde_json::Value| -> ::core::result::Result<(), ::std::string::String> {
                    let typed: $ty = ::serde_json::from_value(config.clone())
                        .map_err(|e| format!(
                            "kind '{}' config does not deserialize: {e}",
                            <$ty as $crate::signal::Signal>::TAG
                        ))?;
                    <$ty as $crate::signal::Signal>::validate(&typed)
                },
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every kind shipped in weft-core must register itself via the
    /// `register_signal_kind!` macro. A missing registration would
    /// surface as a runtime "unknown signal kind" error in production,
    /// so we guard at compile/test time. Update the expected list
    /// when adding a kind.
    #[test]
    fn every_kind_registers() {
        let mut tags: Vec<&'static str> = inventory::iter::<SignalKindEntry>
            .into_iter()
            .map(|e| e.tag)
            .collect();
        tags.sort_unstable();
        assert_eq!(
            tags,
            vec![
                "api_endpoint",
                "form",
                "live_socket",
                "poll_endpoint",
                "socket_listen",
                "sse_subscribe",
                "timer",
            ],
            "kinds shipped in weft-core must all register; add the new tag here when adding a kind"
        );
    }

    #[test]
    fn validate_spec_routes_to_kind() {
        let spec = SignalSpec {
            kind: "api_endpoint".into(),
            config: serde_json::json!({ "path": "/leading-slash", "auth": { "kind": "none" } }),
            consumer_kind: None,
        };
        let err = validate_spec(&spec).expect_err("leading slash should fail");
        assert!(err.contains("must not start with"), "got: {err}");
    }

    #[test]
    fn unknown_kind_is_rejected() {
        let spec = SignalSpec {
            kind: "no-such-kind".into(),
            config: serde_json::Value::Null,
            consumer_kind: None,
        };
        let err = validate_spec(&spec).expect_err("unknown kind should fail");
        assert!(err.contains("unknown signal kind"), "got: {err}");
    }
}
