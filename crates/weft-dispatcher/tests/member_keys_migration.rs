//! The proof that a database written before the rename of member to
//! instance still reads after it. The released renames moved columns;
//! the JSON stored IN them kept the old keys, so the hand-written
//! `20261001T200000_*_rename_member_keys_in_json.sql` files rewrite them.
//!
//! The test stops the migration history right before the rename, writes
//! rows exactly as the old build serialized them (each literal below is
//! the shape the type had then), plays the rest of the history, and reads
//! every row back with today's types.
//!
//! Gated behind `db-tests` (off by default): `scripts/run-db-tests.sh`
//! runs it.
#![cfg(feature = "db-tests")]

use serde_json::{json, Value};
use sqlx::PgPool;

use weft_broker_client::protocol::{InfraLifecycleVerb, LifecycleSpec, StartedPayload, TakeDownReach};
use weft_core::access::wire::GrantSummary;
use weft_core::activation::ActivationScope;
use weft_core::instance::{InstanceId, PerInstance};
use weft_core::program::{CostRecord, InfraCopy, InstanceHoldings, PaidBy, ProgramCall, ProgramCallOutcome, ProgramCallPayload};
use weft_core::project::ProjectDefinition;
use weft_core::run_spec::RunSpec;
use weft_core::CredentialOwner;
use weft_dispatcher::api::signal::ParkedFire;
use weft_dispatcher::journal::TriggerBake;
use weft_journal::ExecEvent;
use weft_task_store::kinds::{LiveArrivalPayload, LiveArrivalResult};
use weft_task_store::schema_guard::{replay_migrations, replay_origins};
use weft_task_store::{ExecutionPayload, RecordCostPayload};

/// The first release of the rename: everything before it is the database
/// the old build wrote into.
const RENAME: &str = "20260930T115937";

const PROJECT: &str = "9dfba28c-45c3-4ccc-aa77-985947728672";
const RUN: &str = "e4366595-1cfd-446c-b70a-5f1af995f80a";
const HASH: &str = "184f70b50b3e1c3e4d6913b16155136a05a602a7e595659772d6eaf64dcdd8c3";

fn ada() -> InstanceId {
    InstanceId::new("ada").unwrap()
}

/// A connection as the old `GrantSummary` serialized it: its member, and
/// an owner that was a member's credential.
fn old_grant() -> Value {
    json!({
        "id": "6f1c0e4a-2b7d-4d6e-9a51-0c3f6a8b2e11", "service": "google", "project_id": PROJECT,
        "member": "ada", "identity": "ada@example.com", "label": null, "scopes": ["email"],
        "permissions_verified": true, "value_names": [], "owner": { "member": "ada" },
        "door": "own", "expires_at": null, "has_credential": true
    })
}

/// A birth as the old `ExecEvent::ExecutionStarted` serialized it.
fn old_birth() -> Value {
    json!({
        "kind": "execution_started", "execution_id": RUN, "project_id": PROJECT,
        "entry_node": "n", "phase": "fire", "definition_hash": null,
        "program": null, "source_version": null, "at_unix": 1,
        "member": "ada", "member_values": { "b": { "in": "hello" } }
    })
}

/// A cost record as the old `CostRecord` serialized it.
fn old_cost() -> Value {
    json!({ "run": RUN, "member": "ada", "node": "n", "service": "openrouter", "model": null,
            "amount_usd": 0.5, "paid_by": { "member": "ada" }, "at_unix": 1 })
}

fn old_spec() -> Value {
    json!({ "mode": "park", "graceMinutes": 5, "runningPolicy": "wait" })
}

/// A queued program call with no parked answer.
fn call_task(call: Value) -> (&'static str, Value, Value) {
    ("program_call", json!({ "by": RUN, "stop_self": "keep", "call": call }), Value::Null)
}

fn assert_cost(cost: &CostRecord) {
    assert_eq!((cost.instance.as_ref(), &cost.paid_by), (Some(&ada()), &CredentialOwner::Instance(ada())));
}

async fn insert(pool: &PgPool, sql: &str, binds: &[Value]) {
    let mut query = sqlx::query(sql);
    for bind in binds {
        query = query.bind(match bind {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        });
    }
    query.execute(pool).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn assert_birth(event: &ExecEvent) {
    let ExecEvent::ExecutionStarted { instance, instance_values, .. } = event else { panic!("a birth, got {event:?}") };
    assert_eq!(instance.as_ref(), Some(&ada()));
    assert_eq!(instance_values.get("b").and_then(|p| p.get("in")), Some(&json!("hello")));
}

fn assert_grant(grant: &GrantSummary) {
    assert_eq!(grant.instance.as_ref(), Some(&ada()));
    assert_eq!(grant.owner, CredentialOwner::Instance(ada()));
}

#[sqlx::test]
async fn rows_written_before_the_rename_read_after_it(pool: PgPool) {
    let groups = weft_dispatcher::app::ALL_GROUPS;
    replay_origins(&pool, groups).await.expect("origins");
    replay_migrations(&pool, groups, |m| !m.draft && m.id < RENAME).await.expect("history before the rename");

    assert!(serde_json::from_value::<ExecEvent>(old_birth()).is_err(), "the old birth does not read today");

    // The journal: a birth, a cost on the member's credential, and three
    // program calls journaled for a resume (one infra copy, the holdings
    // under the journal name that moved, the member's connections).
    let journal = [
        old_birth(),
        json!({
            "kind": "cost_reported", "execution_id": RUN, "node_id": "n", "frames": [],
            "cost_id": "c1", "service": "openrouter", "model": null, "amount_usd": 0.5,
            "billed": false, "origin": { "member": "ada" }, "metadata": {}, "at_unix": 1
        }),
        json!({
            "kind": "run_output", "execution_id": RUN, "node_id": "n", "frames": [], "call_index": 0,
            "name": "weft.infra.status",
            "value": { "node": "blender", "member": "ada", "status": "running" }, "at_unix": 1
        }),
        json!({
            "kind": "run_output", "execution_id": RUN, "node_id": "n", "frames": [], "call_index": 1,
            "name": "weft.members.list",
            "value": [{ "member": "ada", "values": 1, "connections": 1, "tokens": 0,
                        "copies": [{ "node": "blender", "member": "ada", "status": "running" }], "triggers": [] }],
            "at_unix": 1
        }),
        json!({
            "kind": "run_output", "execution_id": RUN, "node_id": "n", "frames": [], "call_index": 2,
            "name": "weft.connections.list", "value": [old_grant()], "at_unix": 1
        }),
        json!({
            "kind": "run_output", "execution_id": RUN, "node_id": "n", "frames": [], "call_index": 3,
            "name": "weft.costs.list", "value": [old_cost()], "at_unix": 1
        }),
        json!({
            "kind": "run_output", "execution_id": RUN, "node_id": "n", "frames": [], "call_index": 4,
            "name": "weft.infra.copies", "value": [{ "node": "blender", "member": "ada", "status": "running" }], "at_unix": 1
        }),
    ];
    for row in &journal {
        insert(
            &pool,
            "INSERT INTO exec_event (execution_id, kind, payload_json, created_at) VALUES ($1, $2, $3, 1)",
            &[json!(RUN), row["kind"].clone(), json!(row.to_string())],
        )
        .await;
    }

    insert(
        &pool,
        "INSERT INTO trigger_bake (project_id, program_hash, bake_json) VALUES ($1::uuid, $2, $3)",
        &[json!(PROJECT), json!(HASH), json!(json!({
            "project_id": PROJECT, "member": "ada", "source_version": "v1",
            "program": { "definition_hash": HASH, "binary_hash": HASH, "implementations": {} },
            "execution_id": RUN, "captured": {}, "at_unix": 1
        }).to_string())],
    )
    .await;

    insert(
        &pool,
        "INSERT INTO signal (token, tenant_id, project_id, node_id, is_resume, spec_json, created_at, parked_fires) \
         VALUES ('t1', 'local', $1::uuid, 'n', false, '{}', 1, $2::jsonb)",
        &[json!(PROJECT), json!([{ "id": "f1", "payload": {}, "received_at_unix": 1, "member_gap": "b.in" }])],
    )
    .await;

    // Queued work, as the old payloads and answers serialized.
    let tasks = [
        ("record_cost", json!({
            "execution_id": RUN, "node_id": "n", "frames": [], "service": "openrouter", "model": null,
            "amount_usd": 0.5, "billed": false, "origin": { "member": "ada" }, "metadata": {}
        }), Value::Null),
        ("live_arrival", json!({ "token": "tok", "instance": "worker-a", "method": "GET", "query": {}, "headers": [] }),
         json!({ "outcome": "born", "execution_id": RUN, "instance": "worker-a" })),
        ("program_call", json!({ "by": RUN, "stop_self": "keep", "call": { "call": "infra_status", "node": "blender", "member": "ada" } }),
         json!({ "value": { "node": "blender", "member": "ada", "status": "running" }, "stops_asker": false })),
        ("program_call", json!({ "by": RUN, "stop_self": "keep", "call": { "call": "members_list" } }),
         json!({ "value": journal[3]["value"], "stops_asker": false })),
        ("program_call", json!({ "by": RUN, "stop_self": "keep", "call": { "call": "connections_list", "member": "ada" } }),
         json!({ "value": [old_grant()], "stops_asker": false })),
        ("program_call", json!({ "by": RUN, "stop_self": "keep", "call": { "call": "costs_list",
            "filter": { "member": "ada", "paid_by": "member" } } }),
         json!({ "value": [old_cost()], "stops_asker": false })),
        ("execute", json!({
            "project_id": PROJECT, "execution_id": RUN, "definition_hash": HASH,
            "unrecorded_birth": [old_birth()], "run_class": "short"
        }), Value::Null),
        // Every other call that named a member: on the call, in its scope,
        // in a run filter with no `paid_by`, and the copies answer.
        call_task(json!({ "call": "trigger_activate", "scope": { "triggers": ["t"], "member": "ada" } })),
        call_task(json!({ "call": "trigger_deactivate", "scope": { "member": "ada" }, "spec": old_spec() })),
        call_task(json!({ "call": "runs_clean", "filter": { "member": "ada" }, "running": "cancel" })),
        call_task(json!({ "call": "values_get", "member": "ada" })),
        call_task(json!({ "call": "values_change", "member": "ada", "set": [], "clear": [] })),
        call_task(json!({ "call": "values_forget", "member": "ada" })),
        call_task(json!({ "call": "tokens_revoke", "member": "ada", "id": null })),
        call_task(json!({ "call": "infra_start", "node": "blender", "member": "ada" })),
        call_task(json!({ "call": "infra_stop", "node": "blender", "member": "ada", "spec": old_spec() })),
        call_task(json!({ "call": "infra_terminate", "node": "blender", "member": "ada", "spec": old_spec(), "disks": "keep_listed" })),
        ("program_call", json!({ "by": RUN, "stop_self": "keep", "call": { "call": "infra_copies", "node": "blender" } }),
         json!({ "value": [{ "node": "blender", "member": "ada", "status": "running" }], "stops_asker": false })),
        ("program_call", json!({ "by": RUN, "stop_self": "keep", "call": { "call": "infra_status", "node": "blender", "member": null } }),
         json!({ "value": null, "stops_asker": false })),
    ];
    for (id, (kind, payload, result)) in tasks.iter().enumerate() {
        insert(
            &pool,
            "INSERT INTO task (id, kind, status, target, tenant_id, payload, result, created_at_unix) \
             VALUES ($1::uuid, $2, 'completed', 'dispatcher', 'local', $3::jsonb, $4::jsonb, 1)",
            &[json!(uuid::Uuid::from_u128(id as u128 + 1).to_string()), json!(kind), payload.clone(), result.clone()],
        )
        .await;
    }

    insert(&pool, "INSERT INTO project_version (id, project_id, manifest, created_at) VALUES ('v1', $1::uuid, '{}', 1)", &[json!(PROJECT)]).await;
    insert(
        &pool,
        "INSERT INTO version_run (execution_id, project_id, version_id, spec, definition_hash, created_at) \
         VALUES ($1::uuid, $2::uuid, 'v1', $3::jsonb, $4, 1)",
        &[json!(RUN), json!(PROJECT), json!({ "name": "for-ada", "member": "ada" }), json!(HASH)],
    )
    .await;

    // A health take-down and a recovery, each naming the member's broken copy.
    for (verb, spec) in [
        ("deactivate", json!({ "spec": { "mode": "park", "graceMinutes": 5, "runningPolicy": "wait" },
                               "reach": { "kind": "readers_of", "broken": [{ "node_id": "blender", "member": "ada" }] } })),
        ("reactivate", json!({ "still_broken": [{ "node_id": "blender", "member": "ada" }] })),
    ] {
        insert(
            &pool,
            "INSERT INTO infra_lifecycle_command (tenant_id, project_id, verb, issued_by_instance, issued_at_unix, spec_json) \
             VALUES ('local', $1::uuid, $2, 'd', 1, $3::jsonb)",
            &[json!(PROJECT), json!(verb), spec],
        )
        .await;
    }

    insert(
        &pool,
        "INSERT INTO infra_event (tenant_id, project_id, kind, payload, at_unix) VALUES ('local', $1::uuid, 'started', $2::jsonb, 1)",
        &[json!(PROJECT), json!({ "instance_id": "wn-abc-blender-1", "mode": "fresh" })],
    )
    .await;

    // A project with a node each member fills (with the service and rules
    // it carried for them), and a route and a socket whose live wire was a
    // bare `true`.
    let old_project = json!({
        "id": PROJECT, "edges": [],
        "nodes": [
            { "id": "b", "nodeType": "Text", "label": null, "position": { "x": 0, "y": 0 },
              "config": { "in": { "__weft_member_filled__": {} } }, "perMember": "marked",
              "memberService": { "service": "google", "acquisition": { "kind": "static", "fields": [{ "name": "key" }] } },
              "memberRules": { "rules": [] } },
            { "id": "r", "nodeType": "Route", "label": null, "position": { "x": 0, "y": 0 },
              "features": { "isTrigger": true, "liveConnection": true } },
            { "id": "s", "nodeType": "Socket", "label": null, "position": { "x": 0, "y": 0 },
              "features": { "isTrigger": true, "liveConnection": true } }
        ]
    });
    assert!(serde_json::from_value::<ProjectDefinition>(old_project.clone()).is_err(), "the old definition does not read today");
    insert(
        &pool,
        "INSERT INTO project_definition (project_id, definition_hash, project_json, recorded_at_unix) VALUES ($1::uuid, $2, $3, 1)",
        &[json!(PROJECT), json!(HASH), json!(old_project.to_string())],
    )
    .await;
    insert(
        &pool,
        "INSERT INTO project (id, name, status, project_json, updated_at, tenant_id) VALUES ($1::uuid, 'p', 'inactive', $2, 1, 'local')",
        &[json!(PROJECT), json!(old_project.to_string())],
    )
    .await;

    insert(
        &pool,
        "INSERT INTO access_connect_result (state, tenant_id, result_json) VALUES ('s1', 'local', $1::jsonb)",
        &[json!({ "grant": old_grant() })],
    )
    .await;

    replay_migrations(&pool, groups, |m| !m.draft && m.id >= RENAME).await.expect("the rename and everything after");

    // Every row now reads with today's types, carrying what it carried.
    let rows: Vec<(String,)> = sqlx::query_as("SELECT payload_json FROM exec_event ORDER BY id").fetch_all(&pool).await.unwrap();
    let events: Vec<ExecEvent> = rows.iter().map(|(t,)| serde_json::from_str(t).expect("an event reads")).collect();
    assert_birth(&events[0]);
    let ExecEvent::CostReported { origin, .. } = &events[1] else { panic!("a cost") };
    assert_eq!(origin, &CredentialOwner::Instance(ada()));
    let ExecEvent::RunOutput { value, .. } = &events[2] else { panic!("an answer") };
    let copy: Option<InfraCopy> = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(copy.and_then(|c| c.instance), Some(ada()));
    let ExecEvent::RunOutput { name, value, .. } = &events[3] else { panic!("an answer") };
    assert_eq!(name, "weft.instances.list");
    let held: Vec<InstanceHoldings> = serde_json::from_value(value.clone()).unwrap();
    assert_eq!((&held[0].instance, held[0].copies[0].instance.as_ref()), (&ada(), Some(&ada())));
    let ExecEvent::RunOutput { value, .. } = &events[4] else { panic!("an answer") };
    assert_grant(&serde_json::from_value::<Vec<GrantSummary>>(value.clone()).unwrap()[0]);
    let ExecEvent::RunOutput { value, .. } = &events[5] else { panic!("an answer") };
    assert_cost(&serde_json::from_value::<Vec<CostRecord>>(value.clone()).unwrap()[0]);
    let ExecEvent::RunOutput { value, .. } = &events[6] else { panic!("an answer") };
    assert_eq!(serde_json::from_value::<Vec<InfraCopy>>(value.clone()).unwrap()[0].instance, Some(ada()));

    let (bake,): (String,) = sqlx::query_as("SELECT bake_json FROM trigger_bake").fetch_one(&pool).await.unwrap();
    assert_eq!(serde_json::from_str::<TriggerBake>(&bake).unwrap().instance, Some(ada()));

    let (parked,): (Value,) = sqlx::query_as("SELECT parked_fires FROM signal").fetch_one(&pool).await.unwrap();
    assert_eq!(serde_json::from_value::<Vec<ParkedFire>>(parked).unwrap()[0].instance_gap.as_deref(), Some("b.in"));

    let tasks: Vec<(Value, Option<Value>)> =
        sqlx::query_as("SELECT payload, result FROM task ORDER BY id").fetch_all(&pool).await.unwrap();
    let call = |i: usize| serde_json::from_value::<ProgramCallPayload>(tasks[i].0.clone()).unwrap().call;
    let answer = |i: usize| serde_json::from_value::<ProgramCallOutcome>(tasks[i].1.clone().unwrap()).unwrap().value;
    assert_eq!(serde_json::from_value::<RecordCostPayload>(tasks[0].0.clone()).unwrap().origin, CredentialOwner::Instance(ada()));
    assert_eq!(serde_json::from_value::<LiveArrivalPayload>(tasks[1].0.clone()).unwrap().replica, "worker-a");
    assert_eq!(
        serde_json::from_value::<LiveArrivalResult>(tasks[1].1.clone().unwrap()).unwrap(),
        LiveArrivalResult::Born { execution_id: RUN.into(), replica: "worker-a".into() }
    );
    assert_eq!(call(2), ProgramCall::InfraStatus { node: "blender".into(), instance: Some(ada()) });
    assert_eq!(serde_json::from_value::<Option<InfraCopy>>(answer(2)).unwrap().and_then(|c| c.instance), Some(ada()));
    assert_eq!(call(3), ProgramCall::InstancesList);
    assert_eq!(serde_json::from_value::<Vec<InstanceHoldings>>(answer(3)).unwrap()[0].instance, ada());
    assert_eq!(call(4), ProgramCall::ConnectionsList { instance: ada() });
    assert_grant(&serde_json::from_value::<Vec<GrantSummary>>(answer(4)).unwrap()[0]);
    let ProgramCall::CostsList { filter } = call(5) else { panic!("a costs call") };
    assert_eq!((filter.instance, filter.paid_by), (Some(ada()), Some(PaidBy::Instance)));
    assert_cost(&serde_json::from_value::<Vec<CostRecord>>(answer(5)).unwrap()[0]);
    let execute: ExecutionPayload = serde_json::from_value(tasks[6].0.clone()).unwrap();
    assert_birth(&serde_json::from_value(execute.unrecorded_birth.unwrap()[0].clone()).unwrap());
    let scoped = ActivationScope { triggers: vec!["t".into()], instance: Some(ada()) };
    assert_eq!(call(7), ProgramCall::TriggerActivate { scope: scoped });
    let ProgramCall::TriggerDeactivate { scope, .. } = call(8) else { panic!("a deactivate") };
    assert_eq!(scope.instance, Some(ada()));
    let ProgramCall::RunsClean { filter, .. } = call(9) else { panic!("a clean") };
    assert_eq!(filter.instance, Some(ada()));
    assert_eq!(call(10), ProgramCall::ValuesGet { instance: ada() });
    let ProgramCall::ValuesChange { instance, .. } = call(11) else { panic!("a values change") };
    assert_eq!(instance, ada());
    assert_eq!(call(12), ProgramCall::ValuesForget { instance: ada() });
    assert_eq!(call(13), ProgramCall::TokensRevoke { instance: ada(), id: None });
    assert_eq!(call(14), ProgramCall::InfraStart { node: "blender".into(), instance: Some(ada()) });
    let ProgramCall::InfraStop { instance, .. } = call(15) else { panic!("a stop") };
    assert_eq!(instance, Some(ada()));
    let ProgramCall::InfraTerminate { instance, .. } = call(16) else { panic!("a terminate") };
    assert_eq!(instance, Some(ada()));
    assert_eq!(call(17), ProgramCall::InfraCopies { node: "blender".into() });
    assert_eq!(serde_json::from_value::<Vec<InfraCopy>>(answer(17)).unwrap()[0].instance, Some(ada()));
    assert_eq!(call(18), ProgramCall::InfraStatus { node: "blender".into(), instance: None });
    assert_eq!(serde_json::from_value::<Option<InfraCopy>>(answer(18)).unwrap(), None);

    let (spec,): (Value,) = sqlx::query_as("SELECT spec FROM version_run").fetch_one(&pool).await.unwrap();
    assert_eq!(serde_json::from_value::<RunSpec>(spec).unwrap().instance, Some(ada()));

    let commands: Vec<(String, Value)> =
        sqlx::query_as("SELECT verb, spec_json FROM infra_lifecycle_command ORDER BY id").fetch_all(&pool).await.unwrap();
    let LifecycleSpec::Deactivate(take_down) = LifecycleSpec::from_row_columns(InfraLifecycleVerb::Deactivate, Some(commands[0].1.clone())).unwrap()
    else {
        panic!("a take-down")
    };
    let TakeDownReach::ReadersOf { broken } = take_down.reach else { panic!("aimed at broken copies") };
    assert_eq!(broken[0].instance, Some(ada()));
    let LifecycleSpec::Reactivate(restore) = LifecycleSpec::from_row_columns(InfraLifecycleVerb::Reactivate, Some(commands[1].1.clone())).unwrap()
    else {
        panic!("a recovery")
    };
    assert_eq!(restore.still_broken[0].instance, Some(ada()));

    let (started,): (Value,) = sqlx::query_as("SELECT payload FROM infra_event").fetch_one(&pool).await.unwrap();
    assert_eq!(serde_json::from_value::<StartedPayload>(started).unwrap().copy_id, "wn-abc-blender-1");

    for table in ["project_definition", "project"] {
        let (definition,): (String,) =
            sqlx::query_as(&format!("SELECT project_json FROM {table}")).fetch_one(&pool).await.unwrap();
        let definition: ProjectDefinition = serde_json::from_str(&definition).unwrap_or_else(|e| panic!("{table} reads: {e}"));
        let filled = definition.nodes.iter().find(|n| n.id == "b").unwrap();
        assert_eq!(filled.per_instance, Some(PerInstance::Marked), "{table}");
        assert!(weft_core::instance::as_instance_filled(&filled.config["in"]).is_some(), "{table}");
        assert_eq!(filled.instance_service.as_ref().map(|s| s.service.as_str()), Some("google"), "{table}");
        assert!(filled.instance_rules.is_some(), "{table}");
        let wire = |id: &str| definition.nodes.iter().find(|n| n.id == id).unwrap().features.live_connection;
        assert_eq!(wire("r"), Some(weft_core::node::LiveWire::Http), "{table}");
        assert_eq!(wire("s"), Some(weft_core::node::LiveWire::Websocket), "{table}");
    }

    let (connect,): (Value,) = sqlx::query_as("SELECT result_json FROM access_connect_result").fetch_one(&pool).await.unwrap();
    assert_grant(&serde_json::from_value(connect["grant"].clone()).unwrap());
}
