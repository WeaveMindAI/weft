//! Bridge between `infra_event` rows (written by the supervisor)
//! and the dispatcher's `EventBus` SSE fanout.
//!
//! Mirrors `journal_bridge`: drain a cursor, publish each event for
//! SSE consumers, advance the cursor. The bridge is SSE-only.
//! Control-plane actions (deactivate / reactivate) flow through
//! `infra_lifecycle_command` rows that `lifecycle_claimer` picks up,
//! so there's no at-least-once retry burden here: missing an SSE
//! publish is cosmetic (clients reconnect and re-poll), losing a
//! control-plane action is not, and that path has its own queue.
//!
//! Multi-pod concurrency: the cursor read AND advance happen in one
//! transaction that takes `FOR UPDATE` on the `dispatcher_cursor`
//! row, so only one dispatcher Pod drains a given batch. Other
//! Pods' polls wait on the row lock and then see an advanced cursor.

use crate::events::DispatcherEvent;
use crate::infra_event::{self, InfraEvent};
use crate::pg_wake::{self, DrainStep};
use crate::state::DispatcherState;

const CURSOR_KEY: &str = "infra_event_bridge";

/// Per-iteration row limit. A burst of >FETCH_LIMIT events is handled
/// by the drain loop's "loop until empty" semantics: the body
/// returns `DrainStep::More` when it filled the batch, and the
/// runner re-invokes immediately.
const FETCH_LIMIT: i64 = 500;

/// Postgres NOTIFY channel writers (broker's `supervisor_event_record`)
/// kick on every infra_event insert. The bridge listens; the
/// safety poll catches missed wakes.
pub const INFRA_EVENT_CHANNEL: &str = "weft_infra_event";

pub async fn migrate(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO dispatcher_cursor (key, last_id) VALUES ($1, 0) \
         ON CONFLICT (key) DO NOTHING",
    )
    .bind(CURSOR_KEY)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn run(state: DispatcherState) {
    pg_wake::run(
        state.pg_pool.clone(),
        INFRA_EVENT_CHANNEL,
        "weft_dispatcher::infra_event_bridge",
        || async { drain(&state).await },
    )
    .await;
}

async fn drain(state: &DispatcherState) -> anyhow::Result<DrainStep> {
    use sqlx::Row;
    let mut tx = state.pg_pool.begin().await?;
    // Lock the cursor row so only one Pod processes a given batch.
    // Other Pods' SELECTs block here until we commit; they then see
    // the advanced cursor and find no new rows.
    let row = sqlx::query(
        "SELECT last_id FROM dispatcher_cursor WHERE key = $1 FOR UPDATE",
    )
    .bind(CURSOR_KEY)
    .fetch_one(&mut *tx)
    .await?;
    let cursor: i64 = row.try_get("last_id")?;

    let rows = infra_event::fetch_since_tx(&mut tx, cursor, FETCH_LIMIT).await?;
    if rows.is_empty() {
        // No work; commit to release the lock and park.
        tx.commit().await?;
        return Ok(DrainStep::Done);
    }
    let saturated = (rows.len() as i64) >= FETCH_LIMIT;
    let mut max_id = cursor;
    let events: Vec<_> = rows
        .into_iter()
        .filter_map(|ev| {
            max_id = max_id.max(ev.id);
            to_dispatcher_event(&ev)
        })
        .collect();

    // Publish BEFORE the cursor advances. SSE consumers are
    // idempotent (they de-dupe by (project_id, color, step) on the
    // client), so a crash AFTER publish + BEFORE commit just
    // re-publishes the same events on the next pod's drain. A crash
    // AFTER commit + BEFORE publish would silently drop the events:
    // the cursor moved past them, no replay ever happens, the SSE
    // client never sees them. Publish-before-commit makes the
    // failure mode "duplicate" (recoverable, idempotent) instead of
    // "lost" (unrecoverable).
    for de in events {
        state.events.publish(de).await;
    }

    sqlx::query("UPDATE dispatcher_cursor SET last_id = $1 WHERE key = $2")
        .bind(max_id)
        .bind(CURSOR_KEY)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    // If we filled the batch, more rows likely remain, so tell the
    // runner to invoke us again without waiting for the next NOTIFY.
    // Otherwise park on the next wake.
    Ok(if saturated { DrainStep::More } else { DrainStep::Done })
}

/// Pure mapping from a fetched `infra_event` row to the
/// `DispatcherEvent` variant the SSE bus carries. Some kinds
/// (Notify) don't translate into a typed dispatcher event today;
/// the bridge drops those.
pub(crate) fn to_dispatcher_event(
    ev: &crate::infra_event::InfraEventRow,
) -> Option<DispatcherEvent> {
    let pid = ev.project_id.clone();
    // Status-change kinds all need a node_id. Project-wide kinds
    // (ProtocolConfigError) don't. The pattern-match drives both.
    match &ev.event {
        InfraEvent::Flaky(p) => Some(DispatcherEvent::InfraFlaky {
            project_id: pid,
            node_id: require_node_id(ev)?,
            // User-string field: cap at 4 KB before NOTIFY fan-out.
            reason: weft_core::truncate_user_string(
                &p.reason
                    .clone()
                    .unwrap_or_else(|| format!("desired={} ready={}", p.desired, p.ready)),
                4096,
            ),
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
        // sibling pods silently miss the event.
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
            project_id: "p".into(),
            node_id: node_id.map(|s| s.to_string()),
            event,
            at_unix: 0,
        }
    }

    #[test]
    fn flaky_maps_with_reason_from_payload() {
        let r = row(
            InfraEvent::Flaky(FlakyPayload {
                desired: 3,
                ready: 1,
                reason: Some("crashloop".into()),
            }),
            Some("n1"),
        );
        let de = to_dispatcher_event(&r).expect("event");
        match de {
            DispatcherEvent::InfraFlaky { project_id, node_id, reason } => {
                assert_eq!(project_id, "p");
                assert_eq!(node_id, "n1");
                assert_eq!(reason, "crashloop");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn flaky_without_reason_uses_desired_ready_summary() {
        let r = row(
            InfraEvent::Flaky(FlakyPayload {
                desired: 2,
                ready: 0,
                reason: None,
            }),
            Some("n1"),
        );
        match to_dispatcher_event(&r).unwrap() {
            DispatcherEvent::InfraFlaky { reason, .. } => {
                assert_eq!(reason, "desired=2 ready=0");
            }
            _ => panic!("wrong variant"),
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
                instance_id: "inst1".into(),
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
                message: "kubectl rejected".into(),
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
