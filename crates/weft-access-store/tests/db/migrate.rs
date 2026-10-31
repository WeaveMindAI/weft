//! Layer-3 tests for copying connections to another install
//! (`weft_access_store::migrate`), against a real Postgres. One database
//! plays both installs: the source's rows are exported, then removed, and
//! the batch is imported as another tenant, which is what the target sees.

use std::collections::BTreeSet;

use serde_json::json;
use weft_task_store::db::{Client as _, Pool};

use weft_access_store::migrate::{begin_import, export, import, make_transfer_key};
use weft_access_store::{connect_direct, list_grants, resolve_for_worker, GrantOwnerScope, GrantUser};
use weft_core::access::spec::Door;
use weft_core::access::wire::ConnectDirect;
use weft_core::migrate::{Outcome, Selection};
use weft_core::AccessSpec;

const SOURCE: &str = "local";
const TARGET: &str = "cloud-tenant";
const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(0xc0de);

/// A pasted-key service with both doors and no test call, so nothing
/// outside the test is reached.
fn keyed() -> AccessSpec {
    serde_json::from_value(json!({
        "service": "keyed",
        "doors": ["shared", "own"],
        "acquisition": { "kind": "static", "fields": [{ "name": "token" }] },
        "auth": [{ "kind": "header", "name": "Authorization", "value": "Bearer {token}" }],
    }))
    .unwrap()
}

async fn connect(pool: &Pool, door: Door, token: &str, project_id: Option<uuid::Uuid>) -> uuid::Uuid {
    connect_direct(
        pool,
        SOURCE,
        ConnectDirect {
            paste: false,
            spec: keyed(),
            door,
            registration: None,
            values: [("token".to_string(), token.to_string())].into_iter().collect(),
            label: Some(format!("key {token}")),
            permissions: Vec::new(),
            project_id,
            instance: None,
        },
    )
    .await
    .unwrap()
    .grant
    .id
}

/// A batch `selection` picks on the source, sealed to a fresh key of
/// `tenant`'s on the target: the key's id and the batch.
async fn sealed_for(pool: &Pool, tenant: &str, selection: &Selection) -> (weft_core::migrate::ExportedConnections, uuid::Uuid, String) {
    let key = make_transfer_key(pool, tenant).await.unwrap();
    let exported = export(pool, SOURCE, selection, Some(&key.public_key)).await.unwrap();
    let sealed = exported.sealed.clone().expect("something to copy");
    (exported, key.key_id, sealed)
}

/// Import a sealed batch as `tenant`, as the broker does.
async fn import_as(
    pool: &Pool,
    tenant: &str,
    (key_id, sealed): &(uuid::Uuid, String),
    shared_keys: &[&str],
) -> anyhow::Result<weft_core::migrate::ImportedConnections> {
    let opened = begin_import(pool, tenant, *key_id, sealed).await?;
    import(opened, tenant, &shared_keys.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>()).await
}

fn outcome(imported: &weft_core::migrate::ImportedConnections, id: uuid::Uuid) -> Outcome {
    imported.connections.iter().find(|c| c.id == id).unwrap().outcome.clone()
}

/// A connection copies with its id, its birth and its secret, as the
/// target's tenant; the same copy again is already there; one changed
/// since is left alone, naming how to replace it; one spending the
/// install's own key needs the target to hold it.
#[weft_db_macros::test]
async fn a_connection_copies_with_its_id_and_its_secret(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let own = connect(&pool, Door::Own, "tok-1", Some(PROJECT)).await;
    let shared = connect(&pool, Door::Shared, "unused", None).await;
    let born: chrono::DateTime<chrono::Utc> = pool.scalar("SELECT created_at FROM access_grant WHERE id = $1", &[&own]).await.unwrap();
    pool.execute(
        "INSERT INTO install_pick (tenant_id, project_id, node, field, grant_id) VALUES ($1, $2, 'bot', 'access', $3)",
        &[&SOURCE, &PROJECT, &own],
    )
    .await
    .unwrap();

    // One batch per import below, each sealed to a key of its own, made
    // while the source still holds the rows.
    let all = Selection { all: true, ..Default::default() };
    let (exported, first_key, first) = sealed_for(&pool, TARGET, &all).await;
    let second = { let (_, k, b) = sealed_for(&pool, TARGET, &all).await; (k, b) };
    let third = { let (_, k, b) = sealed_for(&pool, TARGET, &all).await; (k, b) };
    let elsewhere = { let (_, k, b) = sealed_for(&pool, "someone-else", &all).await; (k, b) };
    assert_eq!(exported.connections.iter().map(|c| c.id).collect::<Vec<_>>(), [own, shared]);
    assert_eq!(exported.picks.len(), 1, "the source's pick is answered, to be set again on the target");
    assert_eq!(exported.picks[0].grant_id, own);

    // The target does not have them yet.
    pool.execute("DELETE FROM access_grant", &[]).await.unwrap();
    let imported = import_as(&pool, TARGET, &(first_key, first.clone()), &[]).await.unwrap();
    assert_eq!(outcome(&imported, own), Outcome::Copied);
    let Outcome::Skipped { why } = outcome(&imported, shared) else { panic!("a shared-key connection needs the key on the target") };
    assert!(why.contains("holds none"), "{why}");
    assert!(import_as(&pool, TARGET, &(first_key, first), &[]).await.is_err(), "a key opens one batch");

    let resolved = resolve_for_worker(&pool, TARGET, GrantUser::Author, own, "keyed", &[], &[]).await.unwrap();
    assert_eq!(resolved.values.get("token").map(String::as_str), Some("tok-1"), "the secret arrived whole");
    let (tenant, created_at, project): (String, chrono::DateTime<chrono::Utc>, Option<uuid::Uuid>) =
        pool.one("SELECT tenant_id, created_at, project_id FROM access_grant WHERE id = $1", &[&own]).await.unwrap();
    assert_eq!((tenant.as_str(), created_at, project), (TARGET, born, Some(PROJECT)));

    // The same batch again: already there, and the shared one copies once
    // the target holds its key.
    let imported = import_as(&pool, TARGET, &second, &["keyed"]).await.unwrap();
    assert_eq!(outcome(&imported, own), Outcome::AlreadyThere);
    assert_eq!(outcome(&imported, shared), Outcome::Copied);

    // Changed on the target since: left alone.
    pool.execute("UPDATE access_grant SET label = 'renamed' WHERE id = $1", &[&own]).await.unwrap();
    let imported = import_as(&pool, TARGET, &third, &["keyed"]).await.unwrap();
    let Outcome::Skipped { why } = outcome(&imported, own) else { panic!("a changed copy is not overwritten") };
    assert!(why.contains(&format!("weft connect --forget {own}")), "{why}");

    // And an id another account here holds is never taken over.
    let imported = import_as(&pool, "someone-else", &elsewhere, &["keyed"]).await.unwrap();
    assert!(imported.connections.iter().all(|c| matches!(&c.outcome, Outcome::Skipped { why } if why.contains("another account"))));
    assert!(list_grants(&pool, "someone-else", None, GrantOwnerScope::Author).await.unwrap().is_empty());
}

/// A service with one connection per account keeps one row for it: a
/// copy of an account the target already holds under another id is left
/// alone, naming the one it has.
#[weft_db_macros::test]
async fn an_account_the_target_has_already_is_not_copied_twice(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let id = connect(&pool, Door::Own, "tok-1", None).await;
    let app = json!({ "label": "app", "client_id": "cid", "client_secret": "s" });
    let exclusive = json!({ "grants": "exclusive" });
    pool.execute(
        "UPDATE access_grant SET client_id = 'cid', registration_sealed = $1, spec = (spec::jsonb || $2::text::jsonb)::text, exclusive = TRUE",
        &[&weft_access_store::seal_json(&app).unwrap(), &exclusive.to_string()],
    )
    .await
    .unwrap();
    let all = Selection { all: true, ..Default::default() };
    let (_, key, sealed) = sealed_for(&pool, TARGET, &all).await;
    // The target holds the same account through the same app, under
    // another id.
    let theirs = uuid::Uuid::new_v4();
    pool.execute("UPDATE access_grant SET id = $1, tenant_id = $2", &[&theirs, &TARGET]).await.unwrap();
    let imported = import_as(&pool, TARGET, &(key, sealed), &[]).await.unwrap();
    let Outcome::Skipped { why } = outcome(&imported, id) else { panic!("a second row for one account is refused") };
    assert!(why.contains(&theirs.to_string()), "{why}");
}

/// A batch that does not open leaves the key, so the import can be run
/// again; an import that fails part way writes nothing.
#[weft_db_macros::test]
async fn a_failed_import_keeps_its_key_and_writes_nothing(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    connect(&pool, Door::Own, "tok-1", None).await;
    let all = Selection { all: true, ..Default::default() };
    let (_, key, sealed) = sealed_for(&pool, TARGET, &all).await;
    let (_, _, sealed_elsewhere) = sealed_for(&pool, TARGET, &all).await;
    assert!(import_as(&pool, TARGET, &(key, sealed_elsewhere), &[]).await.is_err(), "sealed to another key");
    pool.execute("DELETE FROM access_grant", &[]).await.unwrap();
    let imported = import_as(&pool, TARGET, &(key, sealed), &[]).await.unwrap();
    assert_eq!(imported.connections.len(), 1, "the key was still there");
}

/// Only what the selection picks travels, a dry run seals nothing, and a
/// connection a node published stays behind, saying why.
#[weft_db_macros::test]
async fn an_export_holds_to_the_selection(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    let first = connect(&pool, Door::Own, "tok-1", Some(PROJECT)).await;
    let second = connect(&pool, Door::Own, "tok-2", None).await;
    pool.execute("UPDATE access_grant SET published_by_node = 'db' WHERE id = $1", &[&second]).await.unwrap();

    let by_project = Selection { projects: vec![PROJECT], ..Default::default() };
    let listed = export(&pool, SOURCE, &by_project, None).await.unwrap();
    assert_eq!(listed.connections.iter().map(|c| c.id).collect::<Vec<_>>(), [first]);
    assert_eq!(listed.sealed, None, "a dry run seals nothing");

    let by_id = Selection { ids: vec![second.to_string()], ..Default::default() };
    let key = make_transfer_key(&pool, TARGET).await.unwrap();
    let listed = export(&pool, SOURCE, &by_id, Some(&key.public_key)).await.unwrap();
    assert!(listed.connections[0].skipped.as_deref().is_some_and(|why| why.contains("node 'db' published it")));
    assert_eq!(listed.sealed, None, "nothing left to seal");

    let since_tomorrow = Selection { since: Some(chrono::Utc::now() + chrono::Duration::days(1)), ..Default::default() };
    assert!(export(&pool, SOURCE, &since_tomorrow, None).await.unwrap().connections.is_empty());
    assert!(export(&pool, SOURCE, &Selection::default(), None).await.is_err(), "nothing named, nothing exported");
}

/// A transfer key opens a batch only for its tenant, within its time, and
/// keys past their time go as the next one is made.
#[weft_db_macros::test]
async fn a_transfer_key_is_its_tenants_and_expires(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&weft_access_store::GROUP]).await.unwrap();
    connect(&pool, Door::Own, "tok-1", None).await;
    let all = Selection { all: true, ..Default::default() };
    let (_, key, sealed) = sealed_for(&pool, TARGET, &all).await;
    let refused = import_as(&pool, "someone-else", &(key, sealed.clone()), &[]).await.unwrap_err().to_string();
    assert!(refused.contains("run the migrate again"), "another tenant's key is no key: {refused}");
    pool.execute("UPDATE access_transfer_key SET created_at = now() - interval '1 hour'", &[]).await.unwrap();
    assert!(import_as(&pool, TARGET, &(key, sealed), &[]).await.is_err(), "past its time");
    make_transfer_key(&pool, TARGET).await.unwrap();
    let left: i64 = pool.scalar("SELECT count(*) FROM access_transfer_key", &[]).await.unwrap();
    assert_eq!(left, 1, "a key past its time goes as the next one is made");
}
