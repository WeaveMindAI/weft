//! Per-kind logic. One module per `Signal` impl in
//! `weft_core::signal`. Each module:
//!
//!   - declares a unit struct (`WebhookHandler`, `TimerHandler`, ...)
//!     implementing `KindHandler`.
//!   - parses the spec's opaque `config` blob into the kind's typed
//!     struct from `weft_core::signal`.
//!   - owns its own background task (timer schedule, SSE connect),
//!     `process`, `render`, `compute_routing`, and any /action
//!     handlers.
//!   - registers itself with the inventory at the bottom of the file.
//!
//! Adding a new kind = create `kinds/<name>.rs` (handler) + matching
//! file in `weft_core::signal`. The framework discovers it via the
//! inventory at startup; no central match, no enum.
//!
//! Top-level helpers in this file (`register_spec`, `process`,
//! `render`, `compute_routing`, etc.) look up the kind by tag and
//! delegate. They never know about specific kinds.
//!
//! Conventions enforced by the dispatch helpers below:
//!   - Stateful kinds (Timer, SSE) decline external fires by
//!     returning `ProcessTarget::Drop`. Their fires enqueue a
//!     `FireSignal` task via the broker, which the dispatcher's
//!     picker routes through the standard `/process` flow.
//!   - Resume signals (`is_resume = true`) always route to
//!     `ProcessTarget::Resume`; the kind's `process` impl is only
//!     consulted for entry-mode (`is_resume = false`).

pub mod sse;
pub mod timer;
pub mod webhook;
pub mod form;

use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::task::JoinHandle;
use weft_core::primitive::{SignalRouting, SignalSpec};

use crate::config::ListenerConfig;
use crate::fire_sink::FireSignalSink;
use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::{RegisteredSignal, Registry, TaskGuard};

/// Per-kind handler. One unit struct per kind, registered with the
/// inventory below. Methods take typed config blobs from the spec;
/// each handler parses what it needs and ignores the rest.
pub trait KindHandler: Send + Sync {
    /// Kind tag, matched against `SignalSpec.kind`. Must match the
    /// `Signal::TAG` constant on the corresponding data struct in
    /// `weft_core::signal`.
    fn tag(&self) -> &'static str;

    /// Compute the public routing (URL surface + auth gate config)
    /// for this signal. Called once at register time; the dispatcher
    /// stores the result on the signal row and the public router
    /// dispatches by `surface_kind` + `mount_path`. Returns Err if the
    /// spec's config blob fails to deserialize into the kind's typed
    /// shape; the caller surfaces that as a 400 to whoever submitted
    /// the register.
    fn compute_routing(
        &self,
        token: &str,
        spec: &SignalSpec,
        secret_cache: &Arc<DashMap<String, String>>,
    ) -> Result<SignalRouting>;

    /// Compute the initial opaque state to persist on the signal row
    /// at register time. Default: empty object. Kinds that need to
    /// survive a listener restart return values keyed by their own
    /// schema (Timer: `{"next_fire_at_unix": <abs unix>}` for After,
    /// `{}` for Cron/At since those are wall-clock-absolute).
    ///
    /// **Persistence policy**: `signal_insert`'s UPSERT runs
    /// `kind_state = EXCLUDED.kind_state` on conflict (entry-row
    /// re-register on reactivate). Whatever this method returns
    /// REPLACES the previously-persisted state. For Timer's
    /// After-schedule pinning this is intended (reactivate is a
    /// fresh schedule). For a hypothetical future kind that wants
    /// to preserve a cursor across reactivates (e.g. SSE
    /// "last-event-id"), this method must read the current row's
    /// state via the broker and merge before returning. Today no
    /// kind needs that; the contract is "register-time overwrite".
    fn compute_initial_state(&self, _spec: &SignalSpec) -> Result<Value> {
        Ok(Value::Object(serde_json::Map::new()))
    }

    /// Spawn any long-running task this kind needs (timer schedule,
    /// SSE subscriber). Returns `Ok(None)` for passive kinds that
    /// wait for an external HTTP fire (Webhook, Form). `Err` on
    /// malformed spec so register surfaces a 400.
    ///
    /// `kind_state` is the opaque blob persisted on the row at
    /// register time (or read back from the row on rehydrate).
    /// Kinds interpret it however they need. Default is empty `{}`.
    fn spawn_task(
        &self,
        token: &str,
        spec: &SignalSpec,
        kind_state: &Value,
        sink: FireSignalSink,
        config: Arc<ListenerConfig>,
    ) -> Result<Option<JoinHandle<()>>>;

    /// Decide how a fire's payload routes for an entry-mode signal.
    /// `is_resume` signals never reach this method: top-level
    /// `process` short-circuits to `ProcessTarget::Resume` first.
    fn process_entry(
        &self,
        sig: &RegisteredSignal,
        payload: Value,
    ) -> ProcessOutcome;

    /// Render the consumer-facing payload for this signal. Returns
    /// `Ok(None)` for kinds with no consumer surface (raw Webhook,
    /// Timer, SSE) and `Err` for malformed specs (so the caller
    /// surfaces a 400 instead of silently rendering empty).
    fn render(&self, token: &str, sig: &RegisteredSignal) -> Result<Option<Value>>;

    /// Handle a kind-specific /action (e.g. `regenerate_api_key`).
    /// Default: no actions defined.
    fn handle_action(
        &self,
        _token: &str,
        action: &str,
        _payload: Value,
        _sig: &RegisteredSignal,
        _secret_cache: &Arc<DashMap<String, String>>,
    ) -> Result<(Value, Option<SignalRouting>)> {
        anyhow::bail!("kind '{}' has no action '{}'", self.tag(), action)
    }
}

inventory::collect!(&'static dyn KindHandler);

/// Look up a registered handler by tag. Iterates the inventory once;
/// callers should not hold the result across kind additions (none in
/// production today; future hot-reload would require a different
/// design anyway).
pub fn lookup(tag: &str) -> Option<&'static dyn KindHandler> {
    inventory::iter::<&'static dyn KindHandler>
        .into_iter()
        .find(|h| h.tag() == tag)
        .copied()
}

fn handler_or_err(tag: &str) -> Result<&'static dyn KindHandler> {
    lookup(tag).ok_or_else(|| anyhow::anyhow!("unknown signal kind: '{tag}'"))
}

// ----- Public listener entrypoints (HTTP-driven) ---------------------

/// What the routing and kind_state come from. `Mint` is the register
/// path: compute routing fresh (may mint a secret into the cache) and
/// compute the initial kind_state. `Restore` is the rehydrate path:
/// both values came back from the durable row, never recompute (a
/// fresh `compute_routing` would mint a new API key and silently
/// invalidate the user's existing one; a fresh `compute_initial_state`
/// would reset a Timer's clock).
pub enum RoutingSource {
    Mint {
        secret_cache: Arc<DashMap<String, String>>,
    },
    Restore {
        routing: SignalRouting,
        kind_state: Value,
    },
}

/// Register a signal in the in-RAM registry. Single path for both
/// register (fresh registration from the worker) and rehydrate (boot
/// or post-deactivate reconciliation). Returns the routing and
/// kind_state for the dispatcher to persist on the signal row; on
/// the Restore path the returned values are the same ones that came
/// in.
pub async fn register_in_registry(
    token: String,
    spec: SignalSpec,
    node_id: String,
    is_resume: bool,
    color: Option<String>,
    source: RoutingSource,
    registry: Arc<Registry>,
    sink: FireSignalSink,
    config: Arc<ListenerConfig>,
) -> Result<(SignalRouting, Value)> {
    let handler = handler_or_err(&spec.kind)?;
    let (routing, kind_state_owned) = match source {
        RoutingSource::Mint { secret_cache } => {
            let r = handler.compute_routing(&token, &spec, &secret_cache)?;
            let s = handler.compute_initial_state(&spec)?;
            (r, s)
        }
        RoutingSource::Restore { routing, kind_state } => (routing, kind_state),
    };
    let task = handler
        .spawn_task(&token, &spec, &kind_state_owned, sink.clone(), config.clone())?
        .map(|h| Arc::new(TaskGuard::new(h)));
    registry.insert(
        token,
        RegisteredSignal {
            spec,
            node_id,
            is_resume,
            color,
            task,
            routing: routing.clone(),
        },
    );
    Ok((routing, kind_state_owned))
}

/// Process one stateless fire. Resume signals route to the
/// suspended color regardless of kind; entry signals delegate to
/// the kind's `process_entry`. Unknown tokens return Drop.
pub async fn process(
    token: &str,
    payload: Value,
    registry: Arc<Registry>,
) -> Result<ProcessOutcome> {
    let Some(signal) = registry.get(token) else {
        return Ok(ProcessOutcome {
            value: payload,
            target: ProcessTarget::Drop {
                reason: Some("unknown token".into()),
            },
        });
    };

    if signal.is_resume {
        let Some(color) = signal.color.clone() else {
            tracing::warn!(
                target: "weft_listener::kinds",
                %token,
                "is_resume signal has no color; dropping"
            );
            return Ok(ProcessOutcome {
                value: payload,
                target: ProcessTarget::Drop {
                    reason: Some("is_resume signal missing color".into()),
                },
            });
        };
        return Ok(ProcessOutcome {
            value: payload,
            target: ProcessTarget::Resume { color },
        });
    }

    let handler = handler_or_err(&signal.spec.kind)?;
    Ok(handler.process_entry(&signal, payload))
}

/// Render the consumer-facing payload for a registered signal.
pub fn render(token: &str, registry: Arc<Registry>) -> Result<Option<Value>> {
    let signal = registry
        .get(token)
        .ok_or_else(|| anyhow::anyhow!("unknown token: {token}"))?;
    let handler = handler_or_err(&signal.spec.kind)?;
    handler.render(token, &signal)
}

/// Display payload returned to the inspector. Pulls plaintext
/// from `secret_cache` if the listener still holds one (which is
/// only true for the same Pod that minted it; restart loses it).
pub fn compute_display(
    token: &str,
    sig: &RegisteredSignal,
    secret_cache: &Arc<DashMap<String, String>>,
) -> Value {
    let secret = secret_cache.get(token).map(|v| v.clone());
    serde_json::json!({
        "surface": sig.routing.surface,
        "auth": sig.routing.auth,
        "secret": secret,
        "kind": sig.spec.kind,
        "config": sig.spec.config,
    })
}

/// Dispatch an /action. Looks up the kind's handler and delegates.
pub fn handle_action(
    token: &str,
    action_kind: &str,
    payload: Value,
    registry: &Arc<Registry>,
    secret_cache: &Arc<DashMap<String, String>>,
) -> Result<(Value, Option<SignalRouting>)> {
    let sig = registry
        .get(token)
        .ok_or_else(|| anyhow::anyhow!("unknown token: {token}"))?;
    let handler = handler_or_err(&sig.spec.kind)?;
    handler.handle_action(token, action_kind, payload, &sig, secret_cache)
}

// ----- Helpers for kind impls ----------------------------------------

/// Mint an opaque plaintext key. Kinds use this when generating an
/// api-key gate (Webhook OptionalApiKey / regenerate_api_key).
pub fn mint_api_key() -> String {
    let bytes: [u8; 32] = rand::random();
    hex::encode(bytes)
}

pub fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    hex::encode(h.finalize())
}

pub fn default_api_key_header() -> &'static str {
    "X-Api-Key"
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every kind shipped in weft-core must have a matching listener
    /// handler. A mismatch surfaces at runtime as "unknown signal
    /// kind"; guard at test time. Add the new tag here when shipping
    /// a new kind.
    #[test]
    fn every_core_kind_has_a_handler() {
        let mut tags: Vec<&'static str> = inventory::iter::<&'static dyn KindHandler>
            .into_iter()
            .map(|h| h.tag())
            .collect();
        tags.sort_unstable();
        assert_eq!(
            tags,
            vec!["form", "sse", "timer", "webhook"],
            "listener handlers must cover every core kind; update this list when adding a kind"
        );
    }
}
