//! Per-kind register/unregister logic. One module per
//! `WakeSignalKind` variant. Each knows how to:
//!   - set up any long-running task (timer schedule, SSE connect).
//!   - compute the user-facing URL (if any).
//!   - tear down its task on unregister (handled by dropping the
//!     `TaskGuard` stored in the registry entry).
//!
//! Adding a new kind = adding a module here + a branch in
//! `register_spec`. No dispatcher changes required.

pub mod sse;
pub mod timer;
pub mod webhook_form;

use std::sync::Arc;

use anyhow::Result;
use weft_core::primitive::{WakeSignalKind, WakeSignalSpec};

use crate::config::ListenerConfig;
use crate::registry::{RegisteredSignal, Registry};
use crate::relay::FireRelayer;

/// Register a signal by kind. Returns the user-facing URL (if the
/// kind has one) so the dispatcher can relay it back to whoever
/// requested the registration.
pub async fn register_spec(
    token: String,
    spec: WakeSignalSpec,
    node_id: String,
    registry: Arc<Registry>,
    relay: Arc<FireRelayer>,
    config: Arc<ListenerConfig>,
) -> Result<Option<String>> {
    match &spec.kind {
        WakeSignalKind::Webhook { .. } | WakeSignalKind::Form { .. } => {
            let url = webhook_form::user_url(&token, path_for(&spec), &config);
            registry.insert(
                token,
                RegisteredSignal { spec, node_id, task: None },
            );
            Ok(Some(url))
        }
        WakeSignalKind::Timer { spec: timer_spec } => {
            let handle = timer::spawn(token.clone(), timer_spec.clone(), relay.clone());
            registry.insert(
                token,
                RegisteredSignal {
                    spec,
                    node_id,
                    task: Some(Arc::new(crate::registry::TaskGuard::new(handle))),
                },
            );
            Ok(None)
        }
        WakeSignalKind::Sse { url, event_name } => {
            let handle = sse::spawn(
                token.clone(),
                url.clone(),
                event_name.clone(),
                relay.clone(),
            );
            registry.insert(
                token,
                RegisteredSignal {
                    spec,
                    node_id,
                    task: Some(Arc::new(crate::registry::TaskGuard::new(handle))),
                },
            );
            Ok(None)
        }
        WakeSignalKind::Socket { .. } => {
            // Phase B. Reject cleanly so the dispatcher surfaces a
            // useful error to the node author.
            anyhow::bail!("Socket wake signals are not implemented yet")
        }
    }
}

fn path_for(spec: &WakeSignalSpec) -> &str {
    match &spec.kind {
        WakeSignalKind::Webhook { path, .. } => path.as_str(),
        _ => "",
    }
}
