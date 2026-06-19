//! Live caller connection handler, shared by both live-caller kinds
//! (`ApiEndpoint`, `LiveSocket`). A PASSIVE PublicEntry kind: the listener
//! only registers the in-RAM entry and returns the routing shape (surface +
//! auth gate). It owns NO background task (`spawn_task` returns `None`) and
//! is NOT driven through `process_entry` (held connections are not the
//! read-body-return model).
//!
//! The connection itself is held by the worker, routed there through the
//! gateway by the dispatcher's control handshake; the listener's role is
//! purely registration + the api-key gate. The two kinds differ only by
//! protocol (which the dispatcher derives from the tag), so ONE handler impl
//! serves both, registered once per tag.

use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use serde_json::Value;
use tokio::task::JoinHandle;
use weft_core::primitive::{SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{ApiEndpoint, LiveConnectionConfig, LiveSocket, Signal};

use crate::config::ListenerConfig;
use crate::fire_sink::FireSignalSink;
use crate::protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use super::{public_entry_auth_to_routing, KindHandler};

/// One handler instance per live-caller tag. The behavior is identical
/// across tags; only `tag` differs (the dispatcher recovers the protocol
/// from it).
pub struct LiveCallerHandler {
    tag: &'static str,
}

impl KindHandler for LiveCallerHandler {
    fn tag(&self) -> &'static str {
        self.tag
    }

    fn compute_routing(
        &self,
        token: &str,
        spec: &SignalSpec,
        secret_cache: &Arc<DashMap<String, String>>,
    ) -> Result<SignalRouting> {
        let parsed = parse(spec)?;
        let surface = SignalSurface::PublicEntry { path: parsed.path };
        // Auth is the shared api-key gate. Protocol + connection policies
        // are NOT routing concerns; they ride the spec config the dispatcher
        // parses at handshake time.
        Ok(public_entry_auth_to_routing(token, surface, &parsed.auth, secret_cache))
    }

    fn spawn_task(
        &self,
        _token: &str,
        _spec: &SignalSpec,
        _kind_state: &Value,
        _sink: FireSignalSink,
        _config: Arc<ListenerConfig>,
    ) -> Result<Option<JoinHandle<()>>> {
        // Passive: the worker holds the connection, not the listener.
        Ok(None)
    }

    fn process_entry(
        &self,
        _sig: &RegisteredSignal,
        payload: Value,
    ) -> ProcessOutcome {
        // A live connection is never fired through the stateless
        // read-body-return path; the control handshake on the dispatcher
        // drives it. Drop loud rather than silently spawning a caller-less
        // run.
        ProcessOutcome {
            value: payload,
            target: ProcessTarget::Drop {
                reason: Some(
                    "live-caller kinds are driven by the dispatcher control \
                     handshake, not the stateless fire path"
                        .into(),
                ),
            },
        }
    }

    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }
}

/// Parse the spec's opaque config into the shared body. Fails loudly on
/// malformed config so register surfaces a 400.
fn parse(spec: &SignalSpec) -> Result<LiveConnectionConfig> {
    serde_json::from_value(spec.config.clone())
        .map_err(|e| anyhow::anyhow!("malformed live-caller spec: {e}"))
}

inventory::submit!(&LiveCallerHandler { tag: ApiEndpoint::TAG } as &dyn KindHandler);
inventory::submit!(&LiveCallerHandler { tag: LiveSocket::TAG } as &dyn KindHandler);

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::primitive::{SignalAuth, SignalSurface};
    use weft_core::signal::PublicEntryAuth;

    fn spec(path: &str, auth: PublicEntryAuth) -> SignalSpec {
        weft_core::signal::to_spec(LiveSocket {
            common: LiveConnectionConfig {
                path: path.into(),
                auth,
                suspend: Default::default(),
                connect_timeout_secs: 30,
                heartbeat_interval_secs: 25,
                max_inbound_bytes: 1024,
                max_session_secs: 0,
                data_type: Default::default(),
                backpressure: Default::default(),
                error_mode: Default::default(),
                journal_mode: Default::default(),
                window: None,
            },
        })
    }

    fn handler() -> LiveCallerHandler {
        LiveCallerHandler { tag: LiveSocket::TAG }
    }

    #[test]
    fn no_auth_yields_public_entry_with_path() {
        let cache = Arc::new(DashMap::new());
        let r = handler()
            .compute_routing("tok", &spec("chat", PublicEntryAuth::None), &cache)
            .expect("routing ok");
        assert!(matches!(
            r.surface,
            SignalSurface::PublicEntry { ref path } if path == "chat"
        ));
        assert!(matches!(r.auth, SignalAuth::None));
        assert!(cache.is_empty(), "no key minted");
    }

    #[test]
    fn api_key_mints_key_via_shared_helper() {
        let cache = Arc::new(DashMap::new());
        let r = handler()
            .compute_routing("tok-1", &spec("chat", PublicEntryAuth::OptionalApiKey), &cache)
            .expect("routing ok");
        assert!(matches!(r.auth, SignalAuth::ApiKey));
        assert!(cache.get("tok-1").is_some(), "plaintext stored in cache");
    }

    #[test]
    fn fire_path_drops_loud() {
        let sig = RegisteredSignal {
            spec: spec("chat", PublicEntryAuth::None),
            node_id: "n".into(),
            is_resume: false,
            color: None,
            task: None,
            routing: SignalRouting {
                surface: SignalSurface::PublicEntry { path: "chat".into() },
                auth: SignalAuth::None,
                auth_config: Value::Null,
            },
        };
        let out = handler().process_entry(&sig, Value::Null);
        assert!(matches!(out.target, ProcessTarget::Drop { .. }));
    }
}
