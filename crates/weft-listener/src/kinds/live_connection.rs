//! Live caller connection handler, shared by both live-caller kinds
//! (`Route`, `Socket`). A PASSIVE PublicEntry kind: the listener only
//! registers the in-RAM entry and returns the routing shape (the route
//! pattern + methods, and the auth gate: open, or a connection the caller
//! is verified against). It owns NO background task (`spawn_task` returns
//! `None`) and is NOT driven through `process_entry` (held connections are
//! not the read-body-return model).
//!
//! The connection itself is held by the worker, routed there through the
//! gateway by the dispatcher's control handshake; the listener's role is
//! purely registration. The two kinds differ only by protocol (which the
//! dispatcher derives from the tag), so ONE handler impl serves both,
//! registered once per tag.

use anyhow::Result;
use serde_json::Value;
use tokio::task::JoinHandle;
use weft_core::primitive::{SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::{LiveConnectionConfig, Route, Signal, Socket};

use async_trait::async_trait;

use weft_core::signal::listener_protocol::{ProcessOutcome, ProcessTarget};
use crate::registry::RegisteredSignal;

use super::{KindHandler, SpawnCtx};

/// One handler instance per live-caller tag. The behavior is identical
/// across tags; only `tag` differs (the dispatcher recovers the protocol
/// from it).
pub struct LiveCallerHandler {
    tag: &'static str,
}

#[async_trait]
impl KindHandler for LiveCallerHandler {
    fn tag(&self) -> &'static str {
        self.tag
    }

    fn compute_routing(&self, spec: &SignalSpec) -> Result<SignalRouting> {
        let parsed = parse(spec)?;
        let surface = SignalSurface::PublicEntry { path: parsed.path, methods: parsed.methods };
        // Protocol + connection policies are NOT routing concerns; they
        // ride the spec config the dispatcher parses at handshake time.
        Ok(SignalRouting::public_entry(surface, &parsed.auth))
    }

    async fn spawn_task(
        &self,
        _spec: &SignalSpec,
        _kind_state: &Value,
        _ctx: SpawnCtx,
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
                     handshake (/connect/...), not the stateless fire path"
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

inventory::submit!(&LiveCallerHandler { tag: Route::TAG } as &dyn KindHandler);
inventory::submit!(&LiveCallerHandler { tag: Socket::TAG } as &dyn KindHandler);

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::primitive::{SignalAuth, SignalSurface};
    use weft_core::signal::PublicEntryAuth;

    fn spec(path: &str, methods: &[&str], auth: PublicEntryAuth) -> SignalSpec {
        weft_core::signal::to_spec(Route {
            common: LiveConnectionConfig {
                path: path.into(),
                methods: methods.iter().map(|m| m.to_string()).collect(),
                auth,
                suspend: Default::default(),
                connect_timeout_secs: 30,
                heartbeat_interval_secs: 25,
                max_inbound_bytes: 1024,
                max_session_secs: 0,
                caller_silence_secs: weft_core::signal::DEFAULT_CALLER_SILENCE_SECS,
                data_type: Default::default(),
                backpressure: Default::default(),
                error_mode: Default::default(),
                journal_mode: Default::default(),
                journal_window_secs: None,
                window: None,
            },
        })
    }

    fn handler() -> LiveCallerHandler {
        LiveCallerHandler { tag: Route::TAG }
    }

    #[test]
    fn an_open_route_yields_a_public_entry_with_pattern_and_methods() {
        let r = handler()
            .compute_routing(&spec("chat/{room}", &["POST"], PublicEntryAuth::None))
            .expect("routing ok");
        assert!(matches!(
            r.surface,
            SignalSurface::PublicEntry { ref path, ref methods }
                if path == "chat/{room}" && methods == &["POST".to_string()]
        ));
        assert!(matches!(r.auth, SignalAuth::None));
        assert!(r.auth_config.is_null());
    }

    #[test]
    fn a_gated_route_names_its_connection_and_nothing_secret() {
        let auth = PublicEntryAuth::Connection {
            access_id: "acc-1".into(),
            service: "api_key_auth".into(),
        };
        let r = handler()
            .compute_routing(&spec("chat", &[], auth))
            .expect("routing ok");
        assert!(matches!(r.auth, SignalAuth::Connection));
        assert_eq!(
            r.auth_config,
            serde_json::json!({ "access_id": "acc-1", "service": "api_key_auth" })
        );
    }

    #[test]
    fn fire_path_drops_loud() {
        let sig = RegisteredSignal {
            spec: spec("chat", &[], PublicEntryAuth::None),
            node_id: "n".into(),
            tenant_id: "t".into(),
            is_resume: false,
            color: None,
            placement_generation: 0,
            task: None,
            routing: SignalRouting {
                surface: SignalSurface::PublicEntry { path: "chat".into(), methods: Vec::new() },
                auth: SignalAuth::None,
                auth_config: Value::Null,
            },
            serving: Default::default(),
        };
        let out = handler().process_entry(&sig, Value::Null);
        assert!(matches!(out.target, ProcessTarget::Drop { .. }));
    }
}
