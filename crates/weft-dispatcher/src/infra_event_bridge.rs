//! Bridge between `infra_event` rows (written by the supervisor)
//! and the dispatcher's `EventBus` SSE fanout.
//!
//! Every row is announced as it commits, naming its project and its id
//! (`INFRA_EVENT_CHANNEL`). Every dispatcher process hears it, and the
//! ones where somebody follows that project read the row and publish it to
//! their own subscribers. The bridge is SSE-only: control-plane actions
//! (deactivate / reactivate) flow through `infra_lifecycle_command` rows
//! that `lifecycle_claimer` picks up, so a lost announcement only leaves
//! a screen behind until the next event or a reload, while the action
//! itself has its own queue.

use weft_task_store::pg_signal::Heard;

use crate::events::DispatcherEvent;
use crate::infra_event::InfraEvent;
use crate::state::DispatcherState;

/// The channel every `infra_event` row is announced on when it commits,
/// from the `infra_event_notify_on_insert` trigger in
/// `infra_event::GROUP`: `"<project id> <row id>"`.
pub const INFRA_EVENT_CHANNEL: &str = "weft_infra_event";

/// Publish the infra events of the projects followed on this process, for
/// the process's whole life.
pub async fn run(state: DispatcherState) {
    let mut heard = state.signals.subscribe();
    loop {
        match heard.next().await {
            Ok(Heard::Signal { channel, payload }) if channel == INFRA_EVENT_CHANNEL => {
                if let Err(e) = publish(&state, &payload).await {
                    tracing::warn!(target: "weft_dispatcher::infra_event_bridge", %payload, error = %format!("{e:#}"), "could not publish an infra event");
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(target: "weft_dispatcher::infra_event_bridge", error = %e, "the infra event bridge stopped hearing new events");
                return;
            }
        }
    }
}

/// Publish the row an announcement names, when its project is followed
/// here.
async fn publish(state: &DispatcherState, payload: &str) -> anyhow::Result<()> {
    let (project, id) = payload.split_once(' ').ok_or_else(|| anyhow::anyhow!("an infra event announcement is '<project> <id>'"))?;
    let project_id: uuid::Uuid = project.parse()?;
    if !state.events.watched(project_id).await {
        return Ok(());
    }
    let Some(row) = crate::infra_event::read(&state.pg_pool, id.parse()?).await? else { return Ok(()) };
    if let Some(event) = to_dispatcher_event(&row) {
        state.events.publish_local(crate::events::IdentifiedEvent::transient(event)).await;
    }
    Ok(())
}

/// Pure mapping from a fetched `infra_event` row to the
/// `DispatcherEvent` variant the SSE bus carries. Some kinds
/// (Notify) don't translate into a typed dispatcher event today;
/// the bridge drops those.
pub(crate) fn to_dispatcher_event(
    ev: &crate::infra_event::InfraEventRow,
) -> Option<DispatcherEvent> {
    let pid = ev.project_id;
    // Status-change kinds all need a node_id. Project-wide kinds
    // (ProtocolConfigError) don't. The pattern-match drives both.
    match &ev.event {
        InfraEvent::Flaky(p) => Some(DispatcherEvent::InfraFlaky {
            project_id: pid,
            node_id: require_node_id(ev)?,
            // User-string field: cap at 4 KB before NOTIFY fan-out.
            reason: weft_core::truncate_user_string(&p.reason, 4096),
        }),
        InfraEvent::Recovered => Some(DispatcherEvent::InfraRecovered {
            project_id: pid,
            node_id: require_node_id(ev)?,
        }),
        InfraEvent::Failed(_) => Some(DispatcherEvent::InfraStatusChanged {
            project_id: pid,
            node_id: require_node_id(ev)?,
            status: "failed".to_string(),
        }),
        InfraEvent::Started(_) => Some(DispatcherEvent::InfraStatusChanged {
            project_id: pid,
            node_id: require_node_id(ev)?,
            status: "running".to_string(),
        }),
        InfraEvent::Stopped => Some(DispatcherEvent::InfraStatusChanged {
            project_id: pid,
            node_id: require_node_id(ev)?,
            status: "stopped".to_string(),
        }),
        InfraEvent::Terminated => Some(DispatcherEvent::InfraTerminated {
            project_id: pid,
            node_id: require_node_id(ev)?,
        }),
        InfraEvent::Notify(_) => None,
        InfraEvent::ProtocolConfigError(p) => Some(DispatcherEvent::InfraConfigError {
            project_id: pid,
            // User-string field: cap before NOTIFY fan-out so a
            // multi-kB serde error from a deeply-nested protocol
            // config can't blow the 7800-byte channel cap.
            error: weft_core::truncate_user_string(&p.error, 4096),
        }),
    }
}

/// Pull `node_id` from a row that's supposed to carry one. If the
/// supervisor wrote a NULL `node_id` for a node-scoped kind (bug on
/// the writer side), log and skip the row rather than fabricating an
/// empty string on the wire.
fn require_node_id(ev: &crate::infra_event::InfraEventRow) -> Option<String> {
    match ev.node_id.clone() {
        // `node_id` is user-authored (from the project definition) and
        // unbounded; bound it here, the single choke point feeding every
        // node-scoped infra DispatcherEvent, so a long id can't push a
        // publish-path NOTIFY payload over the 8000-byte cap and make
        // sibling processes silently miss the event.
        Some(s) if !s.is_empty() => Some(weft_core::truncate_user_string(&s, 4096)),
        _ => {
            tracing::warn!(
                target: "weft_dispatcher::infra_event_bridge",
                event_id = ev.id,
                project_id = %ev.project_id,
                "infra_event row missing node_id for node-scoped kind; dropping SSE publish"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra_event::InfraEventRow;
    use weft_broker_client::protocol::{
        FailedPayload, FlakyPayload, NotifyPayload, ProtocolConfigErrorPayload, StartedPayload,
    };

    fn row(event: InfraEvent, node_id: Option<&str>) -> InfraEventRow {
        InfraEventRow {
            id: 1,
            tenant_id: "t".into(),
            project_id: uuid::Uuid::nil(),
            node_id: node_id.map(|s| s.to_string()),
            event,
            at_unix: 0,
        }
    }

    #[test]
    fn flaky_maps_with_reason_from_payload() {
        let r = row(
            InfraEvent::Flaky(FlakyPayload { reason: "crashloop".into() }),
            Some("n1"),
        );
        let de = to_dispatcher_event(&r).expect("event");
        match de {
            DispatcherEvent::InfraFlaky { project_id, node_id, reason } => {
                assert_eq!(project_id, uuid::Uuid::nil());
                assert_eq!(node_id, "n1");
                assert_eq!(reason, "crashloop");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn recovered_maps_to_infrarecovered() {
        let r = row(InfraEvent::Recovered, Some("n1"));
        assert!(matches!(
            to_dispatcher_event(&r).unwrap(),
            DispatcherEvent::InfraRecovered { .. }
        ));
    }

    #[test]
    fn started_maps_to_status_running() {
        let r = row(
            InfraEvent::Started(StartedPayload {
                copy_id: "inst1".into(),
                mode: weft_broker_client::protocol::StartMode::Fresh,
            }),
            Some("n1"),
        );
        match to_dispatcher_event(&r).unwrap() {
            DispatcherEvent::InfraStatusChanged { status, .. } => assert_eq!(status, "running"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn stopped_maps_to_status_stopped() {
        let r = row(InfraEvent::Stopped, Some("n1"));
        match to_dispatcher_event(&r).unwrap() {
            DispatcherEvent::InfraStatusChanged { status, .. } => assert_eq!(status, "stopped"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn failed_maps_to_status_failed() {
        let r = row(
            InfraEvent::Failed(FailedPayload {
                stage: weft_broker_client::protocol::FailureStage::Apply,
                message: "apply rejected".into(),
            }),
            Some("n1"),
        );
        match to_dispatcher_event(&r).unwrap() {
            DispatcherEvent::InfraStatusChanged { status, .. } => assert_eq!(status, "failed"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn terminated_maps_to_infraterminated() {
        let r = row(InfraEvent::Terminated, Some("n1"));
        assert!(matches!(
            to_dispatcher_event(&r).unwrap(),
            DispatcherEvent::InfraTerminated { .. }
        ));
    }

    #[test]
    fn notify_does_not_map() {
        let r = row(
            InfraEvent::Notify(NotifyPayload {
                protocol: "p".into(),
                channel: "ops".into(),
            }),
            Some("n1"),
        );
        assert!(to_dispatcher_event(&r).is_none());
    }

    #[test]
    fn missing_node_id_skips_publish() {
        let r = row(InfraEvent::Stopped, None);
        assert!(to_dispatcher_event(&r).is_none());
    }

    #[test]
    fn protocol_config_error_maps_to_config_error_event() {
        let r = row(
            InfraEvent::ProtocolConfigError(ProtocolConfigErrorPayload {
                error: "bad json: expected ',' at line 4".into(),
            }),
            None,
        );
        match to_dispatcher_event(&r).unwrap() {
            DispatcherEvent::InfraConfigError { error, .. } => {
                assert!(error.contains("expected ','"));
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
