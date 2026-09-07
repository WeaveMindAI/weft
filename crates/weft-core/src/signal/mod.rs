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

// `predicate` is wire-pure (serde types + a pure evaluator) and stays
// available without the `runtime` feature: `SignalSpec` (an ungated
// wire type) carries `Vec<Predicate>`. Every other submodule is a
// typed runtime kind (cron/chrono/inventory deps), runtime-gated.
pub mod predicate;
pub use predicate::{Predicate, PredicateOp};

#[cfg(feature = "runtime")]
pub mod auth;
#[cfg(feature = "runtime")]
pub mod timer;
#[cfg(feature = "runtime")]
pub mod form;
#[cfg(feature = "runtime")]
pub mod sse_subscribe;
#[cfg(feature = "runtime")]
pub mod poll_endpoint;
#[cfg(feature = "runtime")]
pub mod socket_listen;
#[cfg(feature = "runtime")]
pub mod stream_listen;
#[cfg(feature = "runtime")]
pub mod provider_events;
#[cfg(feature = "runtime")]
pub mod live_connection;

#[cfg(feature = "runtime")]
pub use auth::PublicEntryAuth;
#[cfg(feature = "runtime")]
pub use provider_events::{EventScope, ProviderEvents};
#[cfg(feature = "runtime")]
pub use timer::{Timer, TimerSpec};
#[cfg(feature = "runtime")]
pub use form::{Form, FormSchema, FormField};
#[cfg(feature = "runtime")]
pub use sse_subscribe::SseSubscribe;
#[cfg(feature = "runtime")]
pub use poll_endpoint::{CursorParam, DeltaMode, PollDelta, PollEndpoint, PollFormat, PollMethod};
#[cfg(feature = "runtime")]
pub use socket_listen::{SocketFrame, SocketListen};
#[cfg(feature = "runtime")]
pub use stream_listen::{Framing, LengthCounts, ScriptStep, StreamListen, StreamReply};
#[cfg(feature = "runtime")]
pub use live_connection::{
    protocol_for_tag, ApiEndpoint, Backpressure, DataType, ErrorMode, JournalMode,
    LiveConnectionConfig, LiveSocket, Protocol,
};

#[cfg(feature = "runtime")]
use serde::{de::DeserializeOwned, Serialize};
#[cfg(feature = "runtime")]
use serde_json::Value;

#[cfg(feature = "runtime")]
use crate::primitive::{AccessRef, SignalSpec};

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
#[cfg(feature = "runtime")]
pub trait Signal: Serialize + DeserializeOwned + Sized {
    /// Kind tag stored on the wire (`"timer"`, `"api_endpoint"`, ...).
    /// Used to route incoming specs to the right handler in the
    /// listener.
    const TAG: &'static str;

    /// Does this kind mean nothing without a connection? Checked by
    /// [`validate_spec`] against [`SignalSpec::access`], which lives
    /// on the spec and so cannot be checked from the kind's own
    /// config blob. Default `false`: most kinds address a public URL
    /// and take a connection only when the author wires one.
    const REQUIRES_ACCESS: bool = false;

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

    /// The connection this signal acts as, when the kind takes one.
    /// Lifted onto [`SignalSpec::access`] by [`to_spec`], so a kind
    /// stores the author's `Access` in its own field and the shared
    /// listener plumbing does the resolving. Default `None`.
    fn access(&self) -> Option<AccessRef> {
        None
    }

    /// The pre-fire filter this signal carries, lifted onto
    /// [`SignalSpec::match_predicates`] by [`to_spec`]. Default: none
    /// (fire on everything the kind produces).
    fn match_predicates(&self) -> &[Predicate] {
        &[]
    }

    /// The stored file this signal shows under `field`, for the
    /// signal-token files door: a consumer that lists the signal asks
    /// the door for a fresh link to it. A kind whose consumer payload
    /// carries files (a form's image field) overrides this to name the
    /// file its config holds for that field; the stored file itself
    /// never reaches a consumer. `Ok(None)` when this field holds no
    /// stored file; `Err` when it holds one the kind cannot read, so a
    /// broken reference never reads as "there is no file here".
    /// Default: no kind carries files.
    fn stored_file(&self, _field: &str) -> Result<Option<crate::storage::StoredFile>, String> {
        Ok(None)
    }
}

/// Project a typed kind into the wire-shape `SignalSpec`. The
/// entry-vs-resume distinction is dispatcher-flow metadata, NOT
/// part of the spec; it rides on the register request.
#[cfg(feature = "runtime")]
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
        access: kind.access(),
        match_predicates: kind.match_predicates().to_vec(),
        config: serde_json::to_value(&kind).expect("kind serialization is infallible"),
        consumer_kind,
    }
}

/// Inventory entry registering a kind's tag + a JSON-driven validator.
/// Each `weft-core/src/signal/<name>.rs` file submits one of these.
/// The framework iterates the registry to validate any `SignalSpec`
/// without knowing the typed struct.
#[cfg(feature = "runtime")]
pub struct SignalKindEntry {
    pub tag: &'static str,
    /// Parse `config` as the typed kind and call `validate`. Returns
    /// the typed kind's error message verbatim, or "unknown kind"
    /// when the tag isn't registered.
    pub validate_json: fn(&Value) -> Result<(), String>,
    /// [`Signal::requires_access`], reachable from a tag alone so
    /// `validate_spec` can check the spec-level connection.
    pub requires_access: bool,
    /// Parse `config` as the typed kind and ask [`Signal::stored_file`],
    /// so the files door can answer for any kind from the wire shape.
    /// `Err` when the stored config no longer matches the kind's shape,
    /// which is the same failure the listener reports when it renders.
    pub stored_file_json: fn(&Value, &str) -> Result<Option<crate::storage::StoredFile>, String>,
}

#[cfg(feature = "runtime")]
inventory::collect!(SignalKindEntry);

/// Find a registered kind by tag, or None. Internal: callers go
/// through `validate_spec`.
#[cfg(feature = "runtime")]
fn lookup(tag: &str) -> Option<&'static SignalKindEntry> {
    inventory::iter::<SignalKindEntry>
        .into_iter()
        .find(|e| e.tag == tag)
}

/// The stored file a wire-shape `SignalSpec` shows under `field`, by
/// its kind's own rule ([`Signal::stored_file`]). `Ok(None)` when the
/// kind carries no files or this field holds none; `Err` when the kind
/// is not registered here or its stored config no longer parses, so a
/// consumer is told what is actually wrong instead of "no such file".
#[cfg(feature = "runtime")]
pub fn stored_file(spec: &SignalSpec, field: &str) -> Result<Option<crate::storage::StoredFile>, String> {
    let entry = lookup(&spec.kind).ok_or_else(|| format!("unknown signal kind '{}'", spec.kind))?;
    (entry.stored_file_json)(&spec.config, field)
}

/// Validate a wire-shape `SignalSpec`: the spec-level fields every
/// kind shares (the pre-fire filter), then the kind's own config.
/// Returns Err for unknown tags, malformed filters, and
/// kind-reported failures alike.
#[cfg(feature = "runtime")]
pub fn validate_spec(spec: &SignalSpec) -> Result<(), String> {
    let entry = lookup(&spec.kind)
        .ok_or_else(|| format!("unknown signal kind: '{}'", spec.kind))?;
    // Registration time is the only moment a bad filter can be
    // refused where someone is watching; per event it would be a
    // silent never-fires.
    predicate::validate(&spec.match_predicates)?;
    if entry.requires_access && spec.access.is_none() {
        return Err(format!(
            "signal kind '{}' acts as a connection, but none was set; build the kind with \
             the access value the node's input carries",
            spec.kind
        ));
    }
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
                requires_access: <$ty as $crate::signal::Signal>::REQUIRES_ACCESS,
                stored_file_json: |config: &::serde_json::Value, field: &str| {
                    let typed: $ty = ::serde_json::from_value(config.clone())
                        .map_err(|e| format!(
                            "kind '{}' config does not deserialize: {e} (the stored config \
                             does not match the current shape; re-register the signal by \
                             re-running the project)",
                            <$ty as $crate::signal::Signal>::TAG
                        ))?;
                    <$ty as $crate::signal::Signal>::stored_file(&typed, field)
                },
            }
        }
    };
}

#[cfg(all(test, feature = "runtime"))]
mod tests {
    use super::*;

    /// Every kind shipped in weft-core must register itself via the
    /// `register_signal_kind!` macro. A missing registration would
    /// surface as a runtime "unknown signal kind" error in production,
    /// so we guard at compile/test time. Update the expected list
    /// when adding a kind.
    /// The files door asks a kind for the file behind a field through
    /// the inventory, so it needs no knowledge of any kind.
    #[test]
    fn a_kind_names_the_stored_file_behind_a_field() {
        let spec = to_spec(Form {
            form_type: "any".into(),
            schema: FormSchema {
                fields: vec![FormField {
                    field_type: "display_image".into(),
                    key: "pic".into(),
                    label: String::new(),
                    render: crate::node::FormFieldRender {
                        component: "image".into(),
                        source: None,
                        multiple: false,
                        prefilled: false,
                    },
                    value: Some(serde_json::json!({ "__weft_image__": {
                        "key": "t/project/p/cat", "mimeType": "image/png", "sizeBytes": 7, "filename": "cat.png"
                    } })),
                    config: Default::default(),
                }],
            },
            title: None,
            description: None,
            consumer_kind: None,
        });
        assert_eq!(
            stored_file(&spec, "pic").unwrap().map(|f| f.key),
            Some("t/project/p/cat".to_string())
        );
        assert_eq!(stored_file(&spec, "nope").unwrap(), None, "an unknown field holds no file");
        let timer = SignalSpec {
            kind: "timer".into(),
            config: serde_json::to_value(Timer { spec: TimerSpec::After { duration_ms: 1000 } }).unwrap(),
            ..spec.clone()
        };
        assert_eq!(
            stored_file(&timer, "pic").unwrap(),
            None,
            "a kind with no files answers none"
        );
        // A kind nobody registered, and a config that no longer parses,
        // both say what is wrong instead of "no such file".
        let unknown = SignalSpec { kind: "nothing".into(), ..spec.clone() };
        assert!(stored_file(&unknown, "pic").unwrap_err().contains("unknown signal kind"));
        let stale = SignalSpec { config: serde_json::json!({ "form_type": 7 }), ..spec };
        assert!(stored_file(&stale, "pic").unwrap_err().contains("does not deserialize"));
    }

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
                "provider_events",
                "socket_listen",
                "sse_subscribe",
                "stream_listen",
                "timer",
            ],
            "kinds shipped in weft-core must all register; add the new tag here when adding a kind"
        );
    }

    #[test]
    fn validate_spec_routes_to_kind() {
        let spec = SignalSpec::of_kind(
            "api_endpoint",
            serde_json::json!({ "path": "/leading-slash", "auth": { "kind": "none" } }),
        );
        let err = validate_spec(&spec).expect_err("leading slash should fail");
        assert!(err.contains("must not start with"), "got: {err}");
    }

    #[test]
    fn unknown_kind_is_rejected() {
        let spec = SignalSpec::of_kind("no-such-kind", serde_json::Value::Null);
        let err = validate_spec(&spec).expect_err("unknown kind should fail");
        assert!(err.contains("unknown signal kind"), "got: {err}");
    }
}
