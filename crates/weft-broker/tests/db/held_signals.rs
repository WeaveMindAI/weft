//! Layer-3 contract tests for `held_signals`, the statements the listener
//! depends on, against a REAL Postgres: the rule "a listener holds the
//! rows of a trigger that takes work, and never one whose door refuses
//! or whose window is over" lives in the SQL's filter, and
//! "of two listeners woken for the same moment exactly one acts" lives in
//! the write's WHERE, so only the statements themselves can prove them.

use weft_task_store::db::{Client as _, Pool, Session};

use weft_broker::held_signals::{
    give_up, hold, judge_held_fire, release, report_load, set_holds, signal_held, signals_held, still_held_by, HandTo, Reported,
    write_kind_state, HeldFire,
};
use weft_broker_client::protocol::{HeldNow, HeldServing, ProjectStatus};

async fn schema(pool: &Pool) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
}

async fn project(pool: &Pool, id: uuid::Uuid) {
    pool.execute(
        "INSERT INTO project (id, name, tenant_id, status, project_json, updated_at, project_json_version, resync_version, running_build_inputs_version) \
         VALUES ($1, $2, 'local', 'registered', '{}', 0, $3, 1, 1)",
        &[&id, &id.to_string(), &weft_core::ProjectDefinition::STORED.version()],
    )
    .await
    .expect("project row");
}

/// One entry signal governed by its own trigger's activation, in
/// `status`; the node id is the token, since a project holds one entry
/// row per node.
async fn signal(pool: &Pool, token: &str, project_id: uuid::Uuid, status: ProjectStatus) {
    pool.execute(
        "INSERT INTO trigger_activation \
           (project_id, trigger, status, accepting_fires, fires_visible_to_consumers, updated_at, activation_program_version) \
         VALUES ($1, $2, $3, TRUE, TRUE, 0, 1)",
        &[&project_id, &token, &status.as_str()],
    )
    .await
    .expect("activation row");
    pool.execute(
        "INSERT INTO signal (token, tenant_id, project_id, node_id, is_resume, spec_json, created_at, activation_trigger, \
         spec_json_version, program_json_version) \
         VALUES ($1, 'local', $2, $1, FALSE, '{}', 0, $1, $3, 1)",
        &[&token, &project_id, &weft_core::primitive::SignalSpec::STORED.version()],
    )
    .await
    .expect("signal row");
}

const ACTIVE: uuid::Uuid = uuid::Uuid::from_u128(0xa);
const ACTIVATING: uuid::Uuid = uuid::Uuid::from_u128(0xb);
const TAKING: uuid::Uuid = uuid::Uuid::from_u128(0xc);
const REFUSING: uuid::Uuid = uuid::Uuid::from_u128(0xe);
const HIDING: uuid::Uuid = uuid::Uuid::from_u128(0xf);

/// Take the activation of `token` down with its door refusing, and no
/// window.
async fn refuse(pool: &Pool, token: &str) {
    pool.execute("UPDATE trigger_activation SET status = 'inactive', accepting_fires = FALSE, fires_deadline_unix = NULL WHERE trigger = $1", &[&token])
        .await
        .expect("refuse");
}

/// The listener holds the rows of every trigger that takes work (on, being
/// set up, or down with its door taking or hiding what arrives within its
/// window: what those hear waits for the trigger to be back), and never one
/// whose door refuses or whose window is over.
#[weft_db_macros::test]
async fn the_listener_holds_what_takes_work_and_never_what_refuses_it(pool: Pool) {
    schema(&pool).await;
    for id in [ACTIVE, ACTIVATING, TAKING, REFUSING, HIDING] {
        project(&pool, id).await;
    }
    signal(&pool, "active", ACTIVE, ProjectStatus::Active).await;
    signal(&pool, "activating", ACTIVATING, ProjectStatus::Activating).await;
    signal(&pool, "taking", TAKING, ProjectStatus::Inactive).await;
    signal(&pool, "refusing", REFUSING, ProjectStatus::Inactive).await;
    refuse(&pool, "refusing").await;
    for (token, deadline) in [("in_window", i64::MAX / 2), ("past_window", 1)] {
        signal(&pool, token, HIDING, ProjectStatus::Inactive).await;
        pool.execute(
            "UPDATE trigger_activation SET fires_visible_to_consumers = FALSE, fires_deadline_unix = $2 WHERE trigger = $1",
            &[&token, &deadline],
        )
        .await
        .unwrap();
    }

    let mut tokens: Vec<String> = signals_held(&pool, None).await.expect("query").into_iter().map(|r| r.token).collect();
    tokens.sort();
    assert_eq!(tokens, vec!["activating", "active", "in_window", "taking"]);

    // An activation's rehydrate reads its own project's rows only.
    let one: Vec<String> =
        signals_held(&pool, Some(ACTIVATING)).await.expect("query").into_iter().map(|r| r.token).collect();
    assert_eq!(one, vec!["activating".to_string()]);
    assert!(signals_held(&pool, Some(REFUSING)).await.expect("query").is_empty(), "scoped, still only held rows");

    // One by token follows the same rule.
    assert_eq!(signal_held(&pool, "taking").await.unwrap().map(|r| r.token).as_deref(), Some("taking"));
    assert!(signal_held(&pool, "refusing").await.unwrap().is_none(), "a refusing door's row never comes back");
    assert!(signal_held(&pool, "past_window").await.unwrap().is_none(), "nor one past its window");
    assert!(signal_held(&pool, "nothing").await.unwrap().is_none());
}

/// A run's wait comes back naming the run it answers: its `run_id`
/// is a real id on the wire, never text the listener has to parse.
#[weft_db_macros::test]
async fn a_held_wait_names_its_run(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    let run = uuid::Uuid::now_v7();
    pool.execute(
        "INSERT INTO signal (token, tenant_id, project_id, run_id, node_id, is_resume, spec_json, created_at, \
         spec_json_version, program_json_version) \
         VALUES ('wait', 'local', $1, $2, 'hold', TRUE, '{}', 0, $3, 1)",
        &[&ACTIVE, &run, &weft_core::primitive::SignalSpec::STORED.version()],
    )
    .await
    .expect("wait row");
    let held = signal_held(&pool, "wait").await.expect("query").expect("held");
    assert!(held.is_resume);
    assert_eq!(held.run_id, Some(run));
    assert_eq!(signals_held(&pool, Some(ACTIVE)).await.expect("query")[0].run_id, Some(run));
}

#[weft_db_macros::test]
async fn a_kind_state_claim_has_one_winner(pool: Pool) {
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

/// What a kind keeps of an outside service's answer (a polled body
/// holding NUL characters) is written and read back exactly, and so is
/// what its holder says about the connection.
#[weft_db_macros::test]
async fn a_kind_state_holding_nul_reads_back_exactly(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    signal(&pool, "tick", ACTIVE, ProjectStatus::Active).await;
    let polled = serde_json::json!({ "delta": ["row\u{0}one"], "last_failure": "502: \u{0}" });
    let at = signal_held(&pool, "tick").await.unwrap().unwrap().kind_state_seq;
    assert!(write_kind_state(&pool, "tick", &polled, at).await.unwrap());
    let row = signal_held(&pool, "tick").await.unwrap().unwrap();
    assert_eq!(row.kind_state, polled);
}

/// A held signal: a row a holder takes.
async fn held_signal(pool: &Pool, token: &str, project_id: uuid::Uuid, status: ProjectStatus) {
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
/// holder claims, up to its room, and never a refusing trigger's or one
/// that holds nothing.
#[weft_db_macros::test]
async fn holders_take_disjoint_shares_within_their_room(pool: Pool) {
    schema(&pool).await;
    for id in [ACTIVE, REFUSING] {
        project(&pool, id).await;
    }
    for t in ["a", "b", "c"] {
        held_signal(&pool, t, ACTIVE, ProjectStatus::Active).await;
    }
    held_signal(&pool, "refusing", REFUSING, ProjectStatus::Inactive).await;
    refuse(&pool, "refusing").await;
    signal(&pool, "form", ACTIVE, ProjectStatus::Active).await;

    let one = hold(&pool, "h1", &[], Some(2), &[]).await.unwrap();
    assert_eq!(one.taken.len(), 2, "its room");
    assert!(one.taken.iter().all(|r| r.holds));
    let two = hold(&pool, "h2", &[], Some(5), &[]).await.unwrap();
    let mut all = tokens(&one.taken);
    all.extend(tokens(&two.taken));
    all.sort();
    assert_eq!(all, vec!["a", "b", "c"], "every live held signal once, never the refusing one or the form");
    assert!(hold(&pool, "h3", &[], None, &[]).await.unwrap().taken.is_empty(), "nothing left");
}

/// A holder keeps what it claims for as long as it lives, with no renewal,
/// says what each connection does, loses it when its check goes off (it
/// stopped pushing it: it died) to the next holder that looks, and gives
/// up its claims when it stops.
#[weft_db_macros::test]
async fn a_claim_lives_until_let_go_or_the_check(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "sse", ACTIVE, ProjectStatus::Active).await;

    assert_eq!(tokens(&hold(&pool, "h1", &[], None, &[]).await.unwrap().taken), vec!["sse"]);
    let listening = HeldServing { status: "listening".into(), transport: None };
    let kept = hold(&pool, "h1", &[HeldNow { token: "sse".into(), serving: Some(listening.clone()) }], None, &[]).await.unwrap();
    assert_eq!(kept.kept, vec!["sse"]);
    assert_eq!(signal_held(&pool, "sse").await.unwrap().unwrap().serving, Some(listening), "the display reads what it said");
    assert!(hold(&pool, "h2", &[], None, &[]).await.unwrap().taken.is_empty(), "a claim is never taken while its holder holds it");

    // h1 dies: its check frees the claim, h2 takes it, and h1 coming back
    // finds it is no longer its own.
    assert_eq!(release(&pool, "h1").await.unwrap(), 1, "the check frees what h1 held");
    assert!(signal_held(&pool, "sse").await.unwrap().unwrap().serving.is_none(), "a freed claim's word is not shown");
    assert_eq!(tokens(&hold(&pool, "h2", &[], None, &[]).await.unwrap().taken), vec!["sse"]);
    let lost = hold(&pool, "h1", &holding(&["sse"]), None, &[]).await.unwrap();
    assert!(lost.kept.is_empty() && lost.ended.is_empty(), "lost to another holder, not ended: {lost:?}");
    let fire = |held_by: Option<&str>| weft_task_store::kinds::FireSignalPayload {
        token: "sse".into(),
        run_id: weft_core::RunId::from_u128(1),
        value: serde_json::json!({}),
        held_by: held_by.map(str::to_string),
    };
    assert_eq!(judge_held_fire(&pool, Some("h2"), &fire(Some("h2"))).await.unwrap(), HeldFire::Taken, "the new holder's fires are taken");
    assert_eq!(judge_held_fire(&pool, Some("h1"), &fire(Some("h1"))).await.unwrap(), HeldFire::NoLongerHeld, "the old one's are refused before it looks again");
    assert_eq!(judge_held_fire(&pool, Some("h1"), &fire(Some("h2"))).await.unwrap(), HeldFire::NotItsSender, "nobody fires in another's name");
    assert_eq!(judge_held_fire(&pool, None, &fire(None)).await.unwrap(), HeldFire::Taken, "a fire no holder sends is not judged here");

    // h2 stops: the next holder takes it at once.
    release(&pool, "h2").await.unwrap();
    assert_eq!(tokens(&hold(&pool, "h3", &[], None, &[]).await.unwrap().taken), vec!["sse"]);
}

/// A row that stops holding (its kind decides otherwise now) lets its
/// holder go, and a held row the holder holds is never taken by it twice.
#[weft_db_macros::test]
async fn a_row_that_stops_holding_lets_its_holder_go(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "sse", ACTIVE, ProjectStatus::Active).await;
    hold(&pool, "h1", &[], None, &[]).await.unwrap();
    assert!(hold(&pool, "h1", &holding(&["sse"]), None, &[]).await.unwrap().taken.is_empty(), "held, so not taken again");
    set_holds(&pool, "sse", false).await.unwrap();
    assert!(!still_held_by(&pool, "sse", "h1").await.unwrap(), "a row served another way takes no held fire");
    let after = hold(&pool, "h1", &holding(&["sse"]), None, &[]).await.unwrap();
    assert!(after.kept.is_empty() && after.ended.is_empty(), "the row lives on, served another way: {after:?}");
}

/// Of what a holder holds, a row that is gone or whose activation takes no
/// work any more has ended; a row still held by it is kept (one down with
/// its door taking included), and none is two of these.
#[weft_db_macros::test]
async fn a_gone_or_refusing_row_has_ended(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    for t in ["kept", "taking", "gone", "refusing"] {
        held_signal(&pool, t, ACTIVE, ProjectStatus::Active).await;
    }
    hold(&pool, "h1", &[], None, &[]).await.unwrap();
    pool.execute("DELETE FROM signal WHERE token = 'gone'", &[]).await.unwrap();
    pool.execute("UPDATE trigger_activation SET status = 'inactive' WHERE trigger = 'taking'", &[]).await.unwrap();
    refuse(&pool, "refusing").await;
    let look = hold(&pool, "h1", &holding(&["kept", "taking", "gone", "refusing"]), None, &[]).await.unwrap();
    let mut kept = look.kept.clone();
    kept.sort();
    assert_eq!(kept, vec!["kept", "taking"]);
    let mut ended = look.ended.clone();
    ended.sort();
    assert_eq!(ended, vec!["gone", "refusing"]);
    assert!(look.taken.is_empty());
}

/// A holder that restarted under its own name takes its claims back at
/// once, without waiting for them to lapse.
#[weft_db_macros::test]
async fn a_holder_back_under_its_name_takes_its_claims_back(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "sse", ACTIVE, ProjectStatus::Active).await;
    hold(&pool, "local", &[], None, &[]).await.unwrap();
    assert_eq!(tokens(&hold(&pool, "local", &[], None, &[]).await.unwrap().taken), vec!["sse"]);
    assert!(hold(&pool, "other", &[], None, &[]).await.unwrap().taken.is_empty());
}

/// A dead holder's check frees every claim it held, and each one freed is
/// announced (`signal_held_on_release`), so the holders' pool is sized
/// again and the other holders look.
#[weft_db_macros::test]
async fn the_check_releases_a_dead_holders_rows_and_announces(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    for t in ["a", "b"] {
        held_signal(&pool, t, ACTIVE, ProjectStatus::Active).await;
    }
    assert_eq!(hold(&pool, "h1", &[], None, &[]).await.unwrap().taken.len(), 2);
    let mut heard = Session::connect(pool.config()).await.unwrap();
    heard.listen(["weft_held_signals"]).await.unwrap();
    assert_eq!(release(&pool, "h1").await.unwrap(), 2);
    tokio::time::timeout(std::time::Duration::from_secs(10), heard.notified())
        .await
        .expect("announced in time")
        .unwrap()
        .expect("the session is open");
    assert_eq!(release(&pool, "h1").await.unwrap(), 0, "a second check frees nothing");
}

/// A row registered again under its holder (the registration lets its
/// holder go) is taken back by that holder in the same look that finds it
/// no longer kept, so the new registration is served at once.
#[weft_db_macros::test]
async fn a_rewrite_under_a_holder_is_taken_back_in_the_same_look(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "sse", ACTIVE, ProjectStatus::Active).await;
    hold(&pool, "h1", &[], None, &[]).await.unwrap();
    pool.execute("UPDATE signal SET held_by = NULL, serving = NULL, generation = nextval('signal_generation_seq') WHERE token = 'sse'", &[])
        .await
        .unwrap();
    let look = hold(&pool, "h1", &holding(&["sse"]), None, &[]).await.unwrap();
    assert!(look.kept.is_empty(), "the old registration is not kept: {look:?}");
    assert_eq!(tokens(&look.taken), vec!["sse"], "and the new one is taken in the same look");
}

/// A signal given up on is shown with why, never taken, only for the
/// registration it names, and a decision made again clears it.
#[weft_db_macros::test]
async fn a_given_up_row_is_shown_and_never_taken(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "sse", ACTIVE, ProjectStatus::Active).await;
    let row = hold(&pool, "h1", &[], None, &[]).await.unwrap().taken.remove(0);
    assert!(!give_up(&pool, "sse", row.generation, "why", Some("h2")).await.unwrap(), "only its holder gives a held one up");
    assert!(!give_up(&pool, "sse", row.generation - 1, "why", Some("h1")).await.unwrap(), "an older registration is not this one");
    assert!(give_up(&pool, "sse", row.generation, "why", Some("h1")).await.unwrap());
    assert!(!still_held_by(&pool, "sse", "h1").await.unwrap(), "let go with it");
    let shown = signal_held(&pool, "sse").await.unwrap().expect("still shown");
    assert_eq!(shown.held_failed.as_deref(), Some("why"));
    assert!(hold(&pool, "h2", &[], None, &[]).await.unwrap().taken.is_empty(), "never taken");
    assert!(!give_up(&pool, "sse", row.generation, "again", Some("h1")).await.unwrap(), "given up once");
    set_holds(&pool, "sse", true).await.unwrap();
    assert!(signal_held(&pool, "sse").await.unwrap().unwrap().held_failed.is_none(), "deciding again clears it");
    assert_eq!(tokens(&hold(&pool, "h2", &[], None, &[]).await.unwrap().taken), vec!["sse"]);
}

/// A caller with no replica (the listener) gives up a held signal only
/// while no holder claims it.
#[weft_db_macros::test]
async fn a_caller_with_no_replica_gives_up_only_an_unclaimed_held_row(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "claimed", ACTIVE, ProjectStatus::Active).await;
    let claimed = hold(&pool, "h1", &[], None, &[]).await.unwrap().taken.remove(0);
    held_signal(&pool, "free", ACTIVE, ProjectStatus::Active).await;
    let free: i64 = pool.scalar("SELECT generation FROM signal WHERE token = 'free'", &[]).await.unwrap();
    assert!(!give_up(&pool, "claimed", claimed.generation, "why", None).await.unwrap(), "a claimed one is its holder's");
    assert!(still_held_by(&pool, "claimed", "h1").await.unwrap());
    assert!(give_up(&pool, "free", free, "why", None).await.unwrap(), "nobody claims this one");
    assert_eq!(signal_held(&pool, "free").await.unwrap().unwrap().held_failed.as_deref(), Some("why"));
}

/// A look that says nothing new and finds nothing to take writes nothing.
#[weft_db_macros::test]
async fn a_look_that_says_nothing_writes_nothing(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "sse", ACTIVE, ProjectStatus::Active).await;
    hold(&pool, "h1", &[], None, &[]).await.unwrap();
    let version = || async {
        pool.scalar::<String>("SELECT xmin::text FROM signal WHERE token = 'sse'", &[]).await.unwrap()
    };
    let before = version().await;
    let look = hold(&pool, "h1", &holding(&["sse"]), None, &[]).await.unwrap();
    assert_eq!(look.kept, vec!["sse"]);
    assert_eq!(version().await, before, "the row was not written");
}

/// What `replica` said of its load, whether that is measured on what it
/// holds, and the room it takes, as the sizing reads them.
async fn load_of(pool: &Pool, replica: &str) -> Option<(i32, bool, i64)> {
    pool.opt("SELECT cpu_percent, settled, room FROM holder_load WHERE replica = $1", &[&replica]).await.unwrap()
}

async fn version_of(pool: &Pool, replica: &str) -> String {
    pool.scalar::<String>("SELECT xmin::text FROM holder_load WHERE replica = $1", &[&replica]).await.unwrap()
}

/// What `report_load` said for a load written: how many it handed over
/// and to which spare.
fn said(handed: u64, to: Option<&str>) -> Reported {
    Reported::Said { handed, to: to.map(str::to_string) }
}

/// A holder handing signals over, with no spare up, keeps the load it
/// said, marked behind what it holds, and frees only the claims it hands
/// over, announcing them, so another holder takes them while it keeps the
/// rest; a report that repeats the row writes nothing, and one that hands
/// over nothing it still claims writes nothing at all.
#[weft_db_macros::test]
async fn a_holder_handing_over_lets_go_of_only_what_it_hands_over(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    for t in ["a", "b", "c"] {
        held_signal(&pool, t, ACTIVE, ProjectStatus::Active).await;
    }
    assert_eq!(hold(&pool, "h1", &[], None, &[]).await.unwrap().taken.len(), 3);
    // The row already says what the report says, so the only thing left to
    // announce is the claim let go.
    pool.execute("INSERT INTO holder_load (replica, cpu_percent, settled, room) VALUES ('h1', 60, false, 2)", &[]).await.unwrap();
    let mut heard = Session::connect(pool.config()).await.unwrap();
    heard.listen(["weft_held_signals"]).await.unwrap();
    let untouched = version_of(&pool, "h1").await;
    assert_eq!(report_load(&pool, "h1", 55, 2, &["c".to_string()], HandTo::Spare).await.unwrap(), Reported::NoSpare);
    assert!(still_held_by(&pool, "c", "h1").await.unwrap(), "only over the target, with no spare: nothing moves");
    assert_eq!((version_of(&pool, "h1").await, load_of(&pool, "h1").await), (untouched, Some((60, false, 2))), "and nothing is written");
    assert_eq!(report_load(&pool, "h1", 60, 2, &["c".to_string()], HandTo::SpareOrAny).await.unwrap(), said(1, None));
    tokio::time::timeout(std::time::Duration::from_secs(10), heard.notified())
        .await
        .expect("announced in time")
        .unwrap()
        .expect("the session is open");
    assert_eq!(load_of(&pool, "h1").await, Some((60, false, 2)));
    assert!(still_held_by(&pool, "a", "h1").await.unwrap() && still_held_by(&pool, "b", "h1").await.unwrap(), "the rest stays");
    assert_eq!(tokens(&hold(&pool, "h2", &[], None, &[]).await.unwrap().taken), vec!["c"], "what it handed over is free");

    let before = version_of(&pool, "h1").await;
    assert_eq!(report_load(&pool, "h1", 20, 10, &[], HandTo::SpareOrAny).await.unwrap(), said(0, None));
    assert_eq!(load_of(&pool, "h1").await, Some((20, true, 10)), "said with nothing handed over: measured on what it holds");
    let said_once = version_of(&pool, "h1").await;
    assert_ne!(said_once, before);
    assert_eq!(report_load(&pool, "h1", 20, 10, &[], HandTo::SpareOrAny).await.unwrap(), said(0, None));
    assert_eq!(version_of(&pool, "h1").await, said_once, "the same report again writes nothing");
    assert_eq!(report_load(&pool, "h1", 90, 1, &["c".to_string()], HandTo::SpareOrAny).await.unwrap(), Reported::NothingHeld, "c is h2's now");
    assert_eq!((version_of(&pool, "h1").await, load_of(&pool, "h1").await), (said_once, Some((20, true, 10))), "nothing written");

    // A holder its check took for dead says no load for what it no
    // longer claims.
    release(&pool, "h1").await.unwrap();
    assert_eq!(report_load(&pool, "h1", 90, 1, &["a".to_string()], HandTo::SpareOrAny).await.unwrap(), Reported::NothingHeld);
    assert_eq!(load_of(&pool, "h1").await, None);
}

/// A look that takes something marks the load its holder said as behind
/// what it holds, since it was measured on less, keeping the number; one
/// that takes nothing leaves it, and each look says
/// the load kept when it is measured on what the holder holds.
#[weft_db_macros::test]
async fn a_look_that_takes_leaves_the_load_said_behind(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "a", ACTIVE, ProjectStatus::Active).await;
    assert_eq!(hold(&pool, "h1", &[], None, &[]).await.unwrap().load, None, "nothing said yet");
    report_load(&pool, "h1", 30, 200, &[], HandTo::SpareOrAny).await.unwrap();
    let look = hold(&pool, "h1", &holding(&["a"]), None, &[]).await.unwrap();
    assert_eq!(look.load, Some(30), "a look that takes nothing keeps it");
    held_signal(&pool, "b", ACTIVE, ProjectStatus::Active).await;
    let look = hold(&pool, "h1", &holding(&["a"]), None, &[]).await.unwrap();
    assert_eq!((tokens(&look.taken), look.load), (vec!["b".to_string()], None));
    assert_eq!(load_of(&pool, "h1").await, Some((30, false, 200)), "the number and the room stay, for the sizing to grow on");
    let look = hold(&pool, "h1", &holding(&["a", "b"]), None, &[]).await.unwrap();
    assert_eq!(look.load, None, "behind until it says its load again");
    report_load(&pool, "h1", 35, 200, &[], HandTo::SpareOrAny).await.unwrap();
    assert_eq!(hold(&pool, "h1", &holding(&["a", "b"]), None, &[]).await.unwrap().load, Some(35), "said again: measured on what it holds");
}

/// A holder that said its load and claims no held signal is a spare: a
/// look says one is up, and a hand-over goes to it alone, announced, with
/// its load marked behind what it now holds; its next look takes and
/// brings up what was handed to it. A spare is handed work once: a second
/// holder handing over only to a spare finds none and writes nothing. A
/// holder that never said its load, or whose claims all ended, is judged
/// by what it holds now.
#[weft_db_macros::test]
async fn a_hand_over_goes_to_one_spare(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    for t in ["a1", "a2", "b1", "b2"] {
        held_signal(&pool, t, ACTIVE, ProjectStatus::Active).await;
    }
    hold(&pool, "h1", &[], Some(2), &[]).await.unwrap();
    hold(&pool, "h2", &[], Some(2), &[]).await.unwrap();
    let mine = |h: &'static str| {
        let pool = pool.clone();
        async move { pool.scalars::<String>("SELECT token FROM signal WHERE held_by = $1 ORDER BY token", &[&h]).await.unwrap() }
    };
    let (one, two) = (mine("h1").await, mine("h2").await);
    assert_eq!((one.len(), two.len()), (2, 2));
    hold(&pool, "h3", &[], Some(10), &[]).await.unwrap();
    assert_eq!(hold(&pool, "h1", &holding(&[one[0].as_str(), one[1].as_str()]), Some(2), &[]).await.unwrap().spare, None, "h3 said no load: it is no spare yet");
    report_load(&pool, "h3", 1, 1, &[], HandTo::SpareOrAny).await.unwrap();
    assert_eq!(hold(&pool, "h1", &holding(&[one[0].as_str(), one[1].as_str()]), Some(2), &[]).await.unwrap().spare, Some(1), "h3 said its load and holds nothing: it takes one");
    assert_eq!(
        report_load(&pool, "h1", 25, 0, &one, HandTo::Spare).await.unwrap(),
        Reported::NoSpare,
        "two do not fit a spare with room for one: what it could not take would never come up"
    );
    assert_eq!(load_of(&pool, "h3").await, Some((1, true, 1)), "nothing written on the spare either");
    report_load(&pool, "h3", 1, 10, &[], HandTo::SpareOrAny).await.unwrap();

    let mut heard = Session::connect(pool.config()).await.unwrap();
    heard.listen(["weft_held_signals"]).await.unwrap();
    assert_eq!(report_load(&pool, "h1", 50, 1, std::slice::from_ref(&one[1]), HandTo::Spare).await.unwrap(), said(1, Some("h3")));
    tokio::time::timeout(std::time::Duration::from_secs(10), heard.notified())
        .await
        .expect("a claim moving to the spare is announced")
        .unwrap()
        .expect("the session is open");
    assert!(still_held_by(&pool, &one[1], "h3").await.unwrap(), "handed to the spare, not freed");
    assert_eq!(load_of(&pool, "h3").await, Some((1, false, 10)), "the spare holds work it has not measured");
    assert_eq!(
        report_load(&pool, "h2", 50, 1, std::slice::from_ref(&two[1]), HandTo::Spare).await.unwrap(),
        Reported::NoSpare,
        "h3 holds work now"
    );
    assert!(still_held_by(&pool, &two[1], "h2").await.unwrap());
    assert_eq!(load_of(&pool, "h2").await, None, "the refused holder's load is not written");
    let look = hold(&pool, "h3", &[], Some(10), &[]).await.unwrap();
    assert_eq!(tokens(&look.taken), vec![one[1].clone()], "the spare's look brings up what was handed to it");
    assert_eq!(look.spare, None);

    // A holder whose last claim ended (its activation stopped listening)
    // holds nothing: it is a spare again.
    refuse(&pool, &one[1]).await;
    assert!(still_held_by(&pool, &one[1], "h3").await.unwrap(), "the claim stays on the row");
    assert_eq!(hold(&pool, "h2", &holding(&[two[0].as_str(), two[1].as_str()]), Some(2), &[]).await.unwrap().spare, Some(10));
}

/// A look takes back every claim of its own it did not say it keeps (one
/// it stopped for a hand-over the broker refused, one handed to it as a
/// spare), whatever its room, so none is ever left claimed and down; its
/// room bounds only the free ones it takes.
#[weft_db_macros::test]
async fn a_look_takes_back_all_of_its_own_claims(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "old", ACTIVE, ProjectStatus::Active).await;
    for t in ["mine1", "mine2", "mine3"] {
        held_signal(&pool, t, ACTIVE, ProjectStatus::Active).await;
        pool.execute("UPDATE signal SET held_by = 'h1', created_at = 10 WHERE token = $1", &[&t]).await.unwrap();
    }
    let look = hold(&pool, "h1", &holding(&["mine1"]), Some(2), &[]).await.unwrap();
    assert_eq!(tokens(&look.taken), vec!["mine2", "mine3"], "all of its own, past its room of two");
    assert!(!still_held_by(&pool, "old", "h1").await.unwrap(), "and no free one, though it is older");
    let look = hold(&pool, "h1", &holding(&["mine1", "mine2", "mine3"]), Some(5), &[]).await.unwrap();
    assert_eq!(tokens(&look.taken), vec!["old"], "room to spare: the free one too");
    let look = hold(&pool, "h2", &[], None, &[]).await.unwrap();
    assert!(look.taken.is_empty(), "with no room given, every free one is taken: none is left");
    held_signal(&pool, "new", ACTIVE, ProjectStatus::Active).await;
    assert_eq!(tokens(&hold(&pool, "h2", &[], None, &[]).await.unwrap().taken), vec!["new"], "with no room given, it takes what is free");
}

/// A look writes the room it is taken with onto its holder's load row when
/// it differs, so a report whose answer was lost (the holder kept its old
/// room) leaves the room the sizing counts right; a look with the same
/// room writes nothing.
#[weft_db_macros::test]
async fn a_look_brings_the_room_said_up_to_date(pool: Pool) {
    schema(&pool).await;
    project(&pool, ACTIVE).await;
    held_signal(&pool, "a", ACTIVE, ProjectStatus::Active).await;
    hold(&pool, "h1", &[], Some(10), &[]).await.unwrap();
    assert_eq!(load_of(&pool, "h1").await, None, "no row: nothing to bring up to date");
    pool.execute("INSERT INTO holder_load (replica, cpu_percent, settled, room) VALUES ('h1', 40, true, 1)", &[]).await.unwrap();
    let look = hold(&pool, "h1", &holding(&["a"]), Some(10), &[]).await.unwrap();
    assert_eq!(look.load, Some(40), "it took nothing, so the load stays");
    assert_eq!(load_of(&pool, "h1").await, Some((40, true, 10)));
    let before = version_of(&pool, "h1").await;
    hold(&pool, "h1", &holding(&["a"]), Some(10), &[]).await.unwrap();
    hold(&pool, "h1", &holding(&["a"]), None, &[]).await.unwrap();
    assert_eq!(version_of(&pool, "h1").await, before, "the same room, or none, writes nothing");
}

/// A holder letting go of everything (it stops), or its check going off
/// (it died), forgets what it said, so a holder gone never keeps the pool
/// sized for it; that is announced too.
#[weft_db_macros::test]
async fn a_full_release_forgets_the_load_said(pool: Pool) {
    schema(&pool).await;
    report_load(&pool, "h1", 95, 1, &[], HandTo::SpareOrAny).await.unwrap();
    let mut heard = Session::connect(pool.config()).await.unwrap();
    heard.listen(["weft_held_signals"]).await.unwrap();
    assert_eq!(release(&pool, "h1").await.unwrap(), 0, "it held nothing");
    tokio::time::timeout(std::time::Duration::from_secs(10), heard.notified())
        .await
        .expect("announced in time")
        .unwrap()
        .expect("the session is open");
    assert_eq!(load_of(&pool, "h1").await, None);
}
