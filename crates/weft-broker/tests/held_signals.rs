//! Layer-3 contract tests for `held_signals`, the statements the listener
//! depends on, against a REAL Postgres: the rule "a listener holds the
//! rows of a trigger that takes work, and never a wiped one's or a
//! hibernation's past its window" lives in the SQL's filter, and
//! "of two listeners woken for the same moment exactly one acts" lives in
//! the write's WHERE, so only the statements themselves can prove them.
//!
//! Gated behind `db-tests` (off by default) so a plain `cargo test` needs no PG.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;

use weft_broker::held_signals::{hold, judge_held_fire, let_go, set_holds, signal_held, signals_held, still_held_by, write_kind_state, HeldFire};
use weft_broker_client::protocol::{HeldNow, HeldServing, ProjectStatus};

async fn schema(pool: &PgPool) {
    weft_dispatcher::app::apply_core_schema(pool).await.expect("core schema");
}

async fn project(pool: &PgPool, id: uuid::Uuid) {
    sqlx::query(
        "INSERT INTO project (id, name, tenant_id, status, project_json, updated_at) \
         VALUES ($1, $2, 'local', 'registered', '{}', 0)",
    )
    .bind(id)
    .bind(id.to_string())
    .execute(pool)
    .await
    .expect("project row");
}

/// One entry signal governed by its own trigger's activation, in
/// `status`; the node id is the token, since a project holds one entry
/// row per node.
async fn signal(pool: &PgPool, token: &str, project_id: uuid::Uuid, status: ProjectStatus) {
    sqlx::query(
        "INSERT INTO trigger_activation \
           (project_id, trigger, status, accepting_fires, fires_visible_to_consumers, updated_at) \
         VALUES ($1, $2, $3, TRUE, TRUE, 0)",
    )
    .bind(project_id)
    .bind(token)
    .bind(status.as_str())
    .execute(pool)
    .await
    .expect("activation row");
    sqlx::query(
        "INSERT INTO signal (token, tenant_id, project_id, node_id, is_resume, spec_json, created_at, activation_trigger) \
         VALUES ($1, 'local', $2, $1, FALSE, '{}', 0, $1)",
    )
    .bind(token)
    .bind(project_id)
    .execute(pool)
    .await
    .expect("signal row");
}

const ACTIVE: uuid::Uuid = uuid::Uuid::from_u128(0xa);
const ACTIVATING: uuid::Uuid = uuid::Uuid::from_u128(0xb);
const PARKED: uuid::Uuid = uuid::Uuid::from_u128(0xc);
const DEACTIVATING: uuid::Uuid = uuid::Uuid::from_u128(0xd);
const WIPED: uuid::Uuid = uuid::Uuid::from_u128(0xe);
const HIBERNATING: uuid::Uuid = uuid::Uuid::from_u128(0xf);

/// Take the activation of `token` off work as a wipe leaves it: inactive
/// and refusing, with no grace window.
async fn wipe(pool: &PgPool, token: &str) {
    sqlx::query("UPDATE trigger_activation SET status = 'inactive', accepting_fires = FALSE, fires_deadline_unix = NULL WHERE trigger = $1")
        .bind(token)
        .execute(pool)
        .await
        .expect("wipe");
}

/// The listener holds the rows of every trigger that takes work (on, being
/// set up, draining, parked, or hibernating within its grace window: what
/// those hear waits for the trigger to be back), and never a wiped one's or
/// a hibernation's past its window.
#[sqlx::test]
async fn the_listener_holds_what_takes_work_and_never_what_refuses_it(pool: PgPool) {
    schema(&pool).await;
    for id in [ACTIVE, ACTIVATING, PARKED, DEACTIVATING, WIPED, HIBERNATING] {
        project(&pool, id).await;
    }
    signal(&pool, "active", ACTIVE, ProjectStatus::Active).await;
    signal(&pool, "activating", ACTIVATING, ProjectStatus::Activating).await;
    signal(&pool, "parked", PARKED, ProjectStatus::Inactive).await;
    signal(&pool, "deactivating", DEACTIVATING, ProjectStatus::Deactivating).await;
    signal(&pool, "wiped", WIPED, ProjectStatus::Inactive).await;
    wipe(&pool, "wiped").await;
    for (token, deadline) in [("in_grace", i64::MAX / 2), ("past_grace", 1)] {
        signal(&pool, token, HIBERNATING, ProjectStatus::Inactive).await;
        sqlx::query("UPDATE trigger_activation SET fires_visible_to_consumers = FALSE, fires_deadline_unix = $2 WHERE trigger = $1")
            .bind(token)
            .bind(deadline)
            .execute(&pool)
            .await
            .unwrap();
    }

    let mut tokens: Vec<String> = signals_held(&pool, None).await.expect("query").into_iter().map(|r| r.token).collect();
    tokens.sort();
    assert_eq!(tokens, vec!["activating", "active", "deactivating", "in_grace", "parked"]);

    // An activation's rehydrate reads its own project's rows only.
    let one: Vec<String> =
        signals_held(&pool, Some(ACTIVATING)).await.expect("query").into_iter().map(|r| r.token).collect();
    assert_eq!(one, vec!["activating".to_string()]);
    assert!(signals_held(&pool, Some(WIPED)).await.expect("query").is_empty(), "scoped, still only held rows");

    // One by token follows the same rule.
    assert_eq!(signal_held(&pool, "parked").await.unwrap().map(|r| r.token).as_deref(), Some("parked"));
    assert!(signal_held(&pool, "wiped").await.unwrap().is_none(), "a wiped row never comes back");
    assert!(signal_held(&pool, "past_grace").await.unwrap().is_none(), "nor one past its grace window");
    assert!(signal_held(&pool, "nothing").await.unwrap().is_none());
}

/// A run's wait comes back naming the run it answers: its `execution_id`
/// is a real id on the wire, never text the listener has to parse.
#[sqlx::test]
async fn a_held_wait_names_its_run(pool: PgPool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    let run = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO signal (token, tenant_id, project_id, execution_id, node_id, is_resume, spec_json, created_at) \
         VALUES ('wait', 'local', $1, $2, 'hold', TRUE, '{}', 0)",
    )
    .bind(ACTIVE)
    .bind(run)
    .execute(&pool)
    .await
    .expect("wait row");
    let held = signal_held(&pool, "wait").await.expect("query").expect("held");
    assert!(held.is_resume);
    assert_eq!(held.execution_id, Some(run));
    assert_eq!(signals_held(&pool, Some(ACTIVE)).await.expect("query")[0].execution_id, Some(run));
}

#[sqlx::test]
async fn a_kind_state_claim_has_one_winner(pool: PgPool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    signal(&pool, "tick", ACTIVE, ProjectStatus::Active).await;
    let state = |n: i64| serde_json::json!({ "n": n });
    let at = signal_held(&pool, "tick").await.unwrap().unwrap().kind_state_seq;

    // Two listeners woken for the same moment both read `at` and claim
    // from it: exactly one wins, and the row moves one past.
    let first = write_kind_state(&pool, "tick", &state(1), at).await.unwrap();
    let second = write_kind_state(&pool, "tick", &state(1), at).await.unwrap();
    assert!(first && !second);
    // A claim from a stale read loses.
    assert!(!write_kind_state(&pool, "tick", &state(9), at).await.unwrap());

    let row = signal_held(&pool, "tick").await.unwrap().unwrap();
    assert_eq!((row.kind_state, row.kind_state_seq), (state(1), at + 1));
}

/// A held signal: a row a holder takes.
async fn held_signal(pool: &PgPool, token: &str, project_id: uuid::Uuid, status: ProjectStatus) {
    signal(pool, token, project_id, status).await;
    set_holds(pool, token, true).await.expect("mark held");
}

fn holding(tokens: &[&str]) -> Vec<HeldNow> {
    tokens.iter().map(|t| HeldNow { token: t.to_string(), serving: None }).collect()
}

fn tokens(rows: &[weft_broker_client::protocol::SignalRowWire]) -> Vec<String> {
    let mut out: Vec<String> = rows.iter().map(|r| r.token.clone()).collect();
    out.sort();
    out
}

/// Two holders share the held signals: each takes only what no live
/// holder claims, up to its room, and never a wiped project's or one that
/// holds nothing.
#[sqlx::test]
async fn holders_take_disjoint_shares_within_their_room(pool: PgPool) {
    schema(&pool).await;
    for id in [ACTIVE, WIPED] {
        project(&pool, id).await;
    }
    for t in ["a", "b", "c"] {
        held_signal(&pool, t, ACTIVE, ProjectStatus::Active).await;
    }
    held_signal(&pool, "wiped", WIPED, ProjectStatus::Inactive).await;
    wipe(&pool, "wiped").await;
    signal(&pool, "form", ACTIVE, ProjectStatus::Active).await;

    let one = hold(&pool, "h1", &[], Some(2), &[], 30).await.unwrap();
    assert_eq!(one.taken.len(), 2, "its room");
    assert!(one.taken.iter().all(|r| r.holds));
    let two = hold(&pool, "h2", &[], Some(5), &[], 30).await.unwrap();
    let mut all = tokens(&one.taken);
    all.extend(tokens(&two.taken));
    all.sort();
    assert_eq!(all, vec!["a", "b", "c"], "every live held signal once, never the wiped one or the form");
    assert!(hold(&pool, "h3", &[], None, &[], 30).await.unwrap().taken.is_empty(), "nothing left");
}

/// A holder keeps what it claims while it renews, says what each
/// connection does, loses what another claim took after its own lapsed,
/// and gives up its claims when it stops.
#[sqlx::test]
async fn a_claim_lives_while_renewed_and_lapses_to_another_holder(pool: PgPool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "sse", ACTIVE, ProjectStatus::Active).await;

    assert_eq!(tokens(&hold(&pool, "h1", &[], None, &[], 30).await.unwrap().taken), vec!["sse"]);
    let listening = HeldServing { status: "listening".into(), transport: None };
    let renewed = hold(&pool, "h1", &[HeldNow { token: "sse".into(), serving: Some(listening.clone()) }], None, &[], 30).await.unwrap();
    assert_eq!(renewed.kept, vec!["sse"]);
    assert_eq!(signal_held(&pool, "sse").await.unwrap().unwrap().serving, Some(listening), "the display reads what it said");
    assert!(hold(&pool, "h2", &[], None, &[], 30).await.unwrap().taken.is_empty(), "a live claim is never taken");

    // h1 dies: its claim lapses, h2 takes it, and h1 coming back finds it
    // is no longer its own.
    sqlx::query("UPDATE signal SET held_until = 0 WHERE token = 'sse'").execute(&pool).await.unwrap();
    assert!(signal_held(&pool, "sse").await.unwrap().unwrap().serving.is_none(), "a dead holder's word is not shown");
    assert_eq!(tokens(&hold(&pool, "h2", &[], None, &[], 30).await.unwrap().taken), vec!["sse"]);
    let lost = hold(&pool, "h1", &holding(&["sse"]), None, &[], 30).await.unwrap();
    assert!(lost.kept.is_empty() && lost.ended.is_empty(), "lost to another holder, not ended: {lost:?}");
    let fire = |held_by: Option<&str>| weft_task_store::kinds::FireSignalPayload {
        token: "sse".into(),
        execution_id: weft_core::ExecutionId::from_u128(1),
        value: serde_json::json!({}),
        held_by: held_by.map(str::to_string),
    };
    assert_eq!(judge_held_fire(&pool, Some("h2"), &fire(Some("h2"))).await.unwrap(), HeldFire::Taken, "the new holder's fires are taken");
    assert_eq!(judge_held_fire(&pool, Some("h1"), &fire(Some("h1"))).await.unwrap(), HeldFire::NoLongerHeld, "the old one's are refused before it looks again");
    assert_eq!(judge_held_fire(&pool, Some("h1"), &fire(Some("h2"))).await.unwrap(), HeldFire::NotItsSender, "nobody fires in another's name");
    assert_eq!(judge_held_fire(&pool, None, &fire(None)).await.unwrap(), HeldFire::Taken, "a fire no holder sends is not judged here");

    // h2 stops: the next holder takes it at once.
    let_go(&pool, "h2").await.unwrap();
    assert_eq!(tokens(&hold(&pool, "h3", &[], None, &[], 30).await.unwrap().taken), vec!["sse"]);
}

/// A row that stops holding (its kind decides otherwise now) lets its
/// holder go, and a held row the holder holds is never taken by it twice.
#[sqlx::test]
async fn a_row_that_stops_holding_lets_its_holder_go(pool: PgPool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "sse", ACTIVE, ProjectStatus::Active).await;
    hold(&pool, "h1", &[], None, &[], 30).await.unwrap();
    assert!(hold(&pool, "h1", &holding(&["sse"]), None, &[], 30).await.unwrap().taken.is_empty(), "held, so not taken again");
    set_holds(&pool, "sse", false).await.unwrap();
    assert!(!still_held_by(&pool, "sse", "h1").await.unwrap(), "a row served another way takes no held fire");
    let after = hold(&pool, "h1", &holding(&["sse"]), None, &[], 30).await.unwrap();
    assert!(after.kept.is_empty() && after.ended.is_empty(), "the row lives on, served another way: {after:?}");
}

/// Of what a holder holds, a row that is gone or whose activation takes no
/// work any more has ended; a row still held by it is kept (a parked one
/// included), and none is two of these.
#[sqlx::test]
async fn a_gone_or_wiped_row_has_ended(pool: PgPool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    for t in ["kept", "parked", "gone", "wiped"] {
        held_signal(&pool, t, ACTIVE, ProjectStatus::Active).await;
    }
    hold(&pool, "h1", &[], None, &[], 30).await.unwrap();
    sqlx::query("DELETE FROM signal WHERE token = 'gone'").execute(&pool).await.unwrap();
    sqlx::query("UPDATE trigger_activation SET status = 'inactive' WHERE trigger = 'parked'").execute(&pool).await.unwrap();
    wipe(&pool, "wiped").await;
    let look = hold(&pool, "h1", &holding(&["kept", "parked", "gone", "wiped"]), None, &[], 30).await.unwrap();
    let mut kept = look.kept.clone();
    kept.sort();
    assert_eq!(kept, vec!["kept", "parked"]);
    let mut ended = look.ended.clone();
    ended.sort();
    assert_eq!(ended, vec!["gone", "wiped"]);
    assert!(look.taken.is_empty());
}

/// A holder that restarted under its own name takes its claims back at
/// once, without waiting for them to lapse.
#[sqlx::test]
async fn a_holder_back_under_its_name_takes_its_claims_back(pool: PgPool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "sse", ACTIVE, ProjectStatus::Active).await;
    hold(&pool, "local", &[], None, &[], 30).await.unwrap();
    assert_eq!(tokens(&hold(&pool, "local", &[], None, &[], 30).await.unwrap().taken), vec!["sse"]);
    assert!(hold(&pool, "other", &[], None, &[], 30).await.unwrap().taken.is_empty());
}
