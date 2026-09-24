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
//! Multi-pod concurrency: a drain holds a session advisory lock on its
//! own connection, taken with `pg_try_advisory_lock`, so only one
//! dispatcher Pod drains at a time and the others skip rather than wait
//! (the holder's own wake covers whatever they heard). No transaction
//! stays open across the publishes: an open one would hold back the
//! settled horizon every cursor reads against (`crate::settled`).

use crate::events::DispatcherEvent;
use crate::infra_event::{self, InfraEvent};
use crate::pg_wake::{self, DrainStep, WakeOn};
use crate::settled::{Position, SettledReader};
use crate::state::DispatcherState;

const CURSOR_KEY: &str = "infra_event_bridge";

/// Per-iteration row limit. A burst of >FETCH_LIMIT events is handled
/// by the drain loop's "loop until empty" semantics: the body
/// returns `DrainStep::More` when it filled the batch, and the
/// runner re-invokes immediately.
const FETCH_LIMIT: i64 = 500;

/// The channel every `infra_event` row notifies on when it commits, from
/// the `infra_event_notify_on_insert` trigger in `infra_event::GROUP`.
/// The bridge listens; the safety tick catches a lost notification.
pub const INFRA_EVENT_CHANNEL: &str = "weft_infra_event";

/// Seed this bridge's cursor row in `dispatcher_cursor` (the table is
/// `journal_bridge::GROUP`'s; seeds run after every group's DDL, so
/// list order carries no constraint). Creates no table of its own, so
/// `tables` is empty. The seed row's key literal is `CURSOR_KEY`
/// (static DDL cannot bind).
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "infra_event_bridge_cursor",
    tables: &[],
    ddl: &[],
    seed: &[
        "INSERT INTO dispatcher_cursor (key, last_id) VALUES ('infra_event_bridge', 0) \
         ON CONFLICT (key) DO NOTHING",
    ],
};

const ON_INFRA_EVENT: &[WakeOn] = &[WakeOn::any(INFRA_EVENT_CHANNEL)];

/// The advisory lock key only one Pod's drain holds at a time.
// SYNC: 'infra_event_bridge' <-> CURSOR_KEY (the lock is keyed like the cursor row)
const DRAIN_LOCK_SQL: &str = "hashtextextended('infra_event_bridge', 0)";

pub async fn run(state: DispatcherState) {
    let reader = tokio::sync::Mutex::new(SettledReader::new("infra_event_bridge"));
    pg_wake::run(
        state.signals.subscribe(),
        ON_INFRA_EVENT,
        pg_wake::SAFETY_POLL_INTERVAL,
        "weft_dispatcher::infra_event_bridge",
        || async { drain(&state, &mut *reader.lock().await).await },
    )
    .await;
}

async fn drain(state: &DispatcherState, reader: &mut SettledReader) -> anyhow::Result<DrainStep> {
    let mut conn = state.pg_pool.acquire().await?;
    let locked: bool = sqlx::query_scalar(&format!("SELECT pg_try_advisory_lock({DRAIN_LOCK_SQL})"))
        .fetch_one(&mut *conn)
        .await?;
    if !locked {
        // A sibling is draining; its own wake covers what this one heard.
        return Ok(DrainStep::Done);
    }
    let step = drain_locked(state, reader, &mut conn).await;
    let unlocked = sqlx::query(&format!("SELECT pg_advisory_unlock({DRAIN_LOCK_SQL})"))
        .execute(&mut *conn)
        .await;
    if step.is_err() || unlocked.is_err() {
        // Never hand a connection that may still hold the lock back to
        // the pool: closing it is what releases the lock for certain.
        conn.close_on_drop();
    }
    let step = step?;
    unlocked?;
    Ok(step)
}

async fn drain_locked(
    state: &DispatcherState,
    reader: &mut SettledReader,
    conn: &mut sqlx::PgConnection,
) -> anyhow::Result<DrainStep> {
    let (xid, id): (i64, i64) = sqlx::query_as(
        "SELECT last_xid::text::bigint, last_id FROM dispatcher_cursor WHERE key = $1",
    )
    .bind(CURSOR_KEY)
    .fetch_one(&mut *conn)
    .await?;
    let batch = reader
        .read(&mut *conn, "infra_event", infra_event::READ_COLUMNS, Position { xid, id }, FETCH_LIMIT)
        .await?;
    let Some(last) = batch.rows.last().map(Position::of).transpose()? else {
        return Ok(batch.next);
    };
    let rows = infra_event::parse_rows(batch.rows)?;

    // Publish BEFORE the cursor advances. SSE consumers are idempotent
    // (they de-dupe by (project_id, color, step) on the client), so a
    // crash after publish and before the advance just re-publishes the
    // same events on the next drain, while a crash after an advance and
    // before publish would drop them for good. The lock is a session
    // lock, so a crash anywhere releases it with the connection.
    for de in rows.iter().filter_map(to_dispatcher_event) {
        state.events.publish(de).await;
    }

    sqlx::query("UPDATE dispatcher_cursor SET last_xid = $1::text::xid8, last_id = $2 WHERE key = $3")
        .bind(last.xid.to_string())
        .bind(last.id)
        .bind(CURSOR_KEY)
        .execute(&mut *conn)
        .await?;
    Ok(batch.next)
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
            project_id: uuid::Uuid::nil(),
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
                assert_eq!(project_id, uuid::Uuid::nil());
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
