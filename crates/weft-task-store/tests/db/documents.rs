//! Carrying a column of documents to today's shape at boot
//! (`weft_task_store::documents`), against a real Postgres: a database
//! from before the column was registered, one interrupted midway (rows at
//! several versions), one a newer weft carried, a fresh one, a step that
//! moves a value out to a column, and the boot with nothing to do.

use serde_json::{json, Value};
use weft_core::stored::{Kind, Op};
use weft_task_store::db::{Client as _, Json, Pool, Transaction};
use weft_task_store::documents::{Encoding, Fill, Rederived};
use weft_task_store::{apply_documents, Document};

/// Version 2 renames `a` to `b`; version 3 adds `c`. `AddKey` refuses a
/// key already there, so a row that took a step twice fails loudly.
const STEPS: &[&[Op]] = &[
    &[Op::RenameKey { at: "", from: "a", to: "b" }],
    &[Op::AddKey { at: "", key: "c", value: "true" }],
];

fn note_rederived<'c>(tx: &'c Transaction<'c>) -> Rederived<'c> {
    Box::pin(async move {
        tx.execute("INSERT INTO probe_rederived DEFAULT VALUES", &[]).await?;
        Ok(())
    })
}

/// The probe's documents, read as any JSON.
const PROBE: Kind<Value> = Kind::new("probe", STEPS);

static JSON_DOC: Document = Document::of(&PROBE, "probe_doc", "body", Encoding::Json, &[], Some(note_rederived));

static ZSTD_DOC: Document = Document::of(&PROBE, "probe_packed", "body", Encoding::Zstd { level: 1 }, &[], None);

/// Version 2 moves `owner` out of the JSON into the row's `owner` column:
/// a fill, then a step dropping the key.
const MOVING: Kind<Value> = Kind::new("moving probe", &[&[Op::DropKey { at: "", key: "owner" }]]);

static MOVING_DOC: Document = Document::of(
    &MOVING,
    "probe_moving",
    "body",
    Encoding::Json,
    &[Fill { from: 1, sql: "UPDATE probe_moving SET owner = body->>'owner' WHERE body_version = 1" }],
    None,
);

/// Version 2 only moves `n` out into a table of its own: nothing in the
/// JSON changes, on a table with no primary key.
const EMPTIED: Kind<Value> = Kind::new("emptied probe", &[&[]]);

static EMPTIED_DOC: Document = Document::of(
    &EMPTIED,
    "probe_keyless",
    "body",
    Encoding::Json,
    &[Fill {
        from: 1,
        sql: "INSERT INTO probe_moved (name, n) SELECT name, (body->>'n')::int FROM probe_keyless \
              WHERE body_version = 1 ON CONFLICT DO NOTHING",
    }],
    None,
);

/// The schema guard's own tables, which the ledger of carried columns is
/// one of: there on every database the boot has built.
async fn ledger(pool: &Pool) {
    weft_task_store::apply_groups(pool, &[]).await.unwrap();
}

/// A two-column key, so the walk in key order is exercised past one
/// batch (500 rows).
async fn tables(pool: &Pool, rows: i32) {
    ledger(pool).await;
    pool.batch_execute(
        "CREATE TABLE probe_doc (owner TEXT NOT NULL, n INTEGER NOT NULL, body JSONB, \
             body_version INTEGER NOT NULL DEFAULT 1, PRIMARY KEY (owner, n));
         CREATE TABLE probe_packed (id BIGINT PRIMARY KEY, body BYTEA NOT NULL, body_version INTEGER NOT NULL DEFAULT 1);
         CREATE TABLE probe_rederived (at TIMESTAMPTZ NOT NULL DEFAULT now())",
    )
    .await
    .unwrap();
    for n in 0..rows {
        let owner = if n % 2 == 0 { "even" } else { "odd" };
        pool.execute("INSERT INTO probe_doc (owner, n, body) VALUES ($1, $2, $3::text::jsonb)", &[&owner, &n, &json!({ "a": n }).to_string()])
            .await
            .unwrap();
    }
    // A row whose column is NULL holds no document and is left alone.
    pool.execute("INSERT INTO probe_doc (owner, n, body) VALUES ('none', 0, NULL)", &[]).await.unwrap();
}

async fn bodies(pool: &Pool) -> Vec<(Value, i32)> {
    let rows: Vec<(Json<Value>, i32)> =
        pool.all("SELECT body, body_version FROM probe_doc WHERE body IS NOT NULL ORDER BY owner, n", &[]).await.unwrap();
    rows.into_iter().map(|(Json(body), version)| (body, version)).collect()
}

async fn recorded(pool: &Pool, table: &str) -> i32 {
    pool.scalar("SELECT version FROM weft_document_version WHERE table_name = $1", &[&table]).await.unwrap()
}

async fn rederived(pool: &Pool) -> i64 {
    pool.scalar("SELECT count(*) FROM probe_rederived", &[]).await.unwrap()
}

/// A column with rows and no record was written before it was registered:
/// every row is at version 1 and takes every step, once, in batches, and
/// says it is at version 3; what derives from it is derived again once.
#[weft_db_macros::test]
async fn rows_from_before_the_column_was_registered_take_every_step_once(pool: Pool) {
    tables(&pool, 1203).await;
    apply_documents(&pool, &[&JSON_DOC]).await.unwrap();
    let all = bodies(&pool).await;
    assert_eq!(all.len(), 1203);
    assert!(
        all.iter().all(|(b, v)| *v == 3 && b.get("a").is_none() && b.get("b").is_some() && b["c"] == json!(true)),
        "{:?}",
        &all[..3]
    );
    assert_eq!(recorded(&pool, "probe_doc").await, 3);
    assert_eq!(rederived(&pool).await, 1);
    let null: (Option<Json<Value>>, i32) =
        pool.one("SELECT body, body_version FROM probe_doc WHERE owner = 'none'", &[]).await.unwrap();
    assert_eq!(null, (None, 1));
    // At today's version a second boot changes nothing.
    apply_documents(&pool, &[&JSON_DOC]).await.unwrap();
    assert_eq!(bodies(&pool).await, all);
    assert_eq!(rederived(&pool).await, 1);
}

/// A boot that stopped midway left rows at every version (each batch
/// committed with its rows' versions): each row takes only the steps after
/// its own version, and none takes one twice (`AddKey` would refuse it).
#[weft_db_macros::test]
async fn rows_at_several_versions_each_take_only_the_steps_after_their_own(pool: Pool) {
    tables(&pool, 10).await;
    pool.execute("UPDATE probe_doc SET body = jsonb_build_object('b', body->'a'), body_version = 2 WHERE n >= 3", &[])
        .await
        .unwrap();
    pool.execute("UPDATE probe_doc SET body = body || '{\"c\": true}', body_version = 3 WHERE n >= 6", &[]).await.unwrap();
    apply_documents(&pool, &[&JSON_DOC]).await.unwrap();
    let all = bodies(&pool).await;
    assert!(all.iter().all(|(b, v)| *v == 3 && b["c"] == json!(true) && b.get("b").is_some()), "{all:?}");
    assert_eq!(recorded(&pool, "probe_doc").await, 3);
}

/// A column a newer weft carried is refused, naming both versions.
#[weft_db_macros::test]
async fn a_column_a_newer_weft_carried_is_refused(pool: Pool) {
    tables(&pool, 1).await;
    apply_documents(&pool, &[&JSON_DOC]).await.unwrap();
    pool.execute("UPDATE weft_document_version SET version = 7", &[]).await.unwrap();
    let err = format!("{:#}", apply_documents(&pool, &[&JSON_DOC]).await.unwrap_err());
    assert!(
        err.contains("probe_doc.body were carried to version 7 by a newer weft, and this weft reads up to version 3"),
        "{err}"
    );
}

/// An empty column has nothing to carry: it is recorded at today's
/// version, and its rows from then on are written in today's shape.
#[weft_db_macros::test]
async fn an_empty_column_is_recorded_at_todays_version(pool: Pool) {
    tables(&pool, 0).await;
    pool.execute("DELETE FROM probe_doc", &[]).await.unwrap();
    apply_documents(&pool, &[&JSON_DOC]).await.unwrap();
    assert_eq!(recorded(&pool, "probe_doc").await, 3);
}

/// With every column at today's version a boot only reads: it does not
/// wait on the schema's lock, held here by another session.
#[weft_db_macros::test]
async fn a_boot_with_nothing_to_carry_takes_no_lock(pool: Pool) {
    tables(&pool, 3).await;
    apply_documents(&pool, &[&JSON_DOC]).await.unwrap();
    let holder = pool.get().await.unwrap();
    holder.execute("SELECT pg_advisory_lock(hashtextextended('weft:schema-migrate', 0))", &[]).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), apply_documents(&pool, &[&JSON_DOC]))
        .await
        .expect("the boot did not wait on the lock")
        .unwrap();
}

/// A compressed column is read, carried and written back compressed.
#[weft_db_macros::test]
async fn a_compressed_column_is_carried_compressed(pool: Pool) {
    tables(&pool, 0).await;
    for id in 0..3i64 {
        let packed = zstd::bulk::compress(&serde_json::to_vec(&json!({ "a": id })).unwrap(), 1).unwrap();
        pool.execute("INSERT INTO probe_packed (id, body) VALUES ($1, $2)", &[&id, &packed]).await.unwrap();
    }
    apply_documents(&pool, &[&ZSTD_DOC]).await.unwrap();
    let rows: Vec<(Vec<u8>, i32)> =
        pool.all("SELECT body, body_version FROM probe_packed ORDER BY id", &[]).await.unwrap();
    for (id, (row, version)) in rows.iter().enumerate() {
        let doc: Value = serde_json::from_slice(&zstd::stream::decode_all(&row[..]).unwrap()).unwrap();
        assert_eq!((doc, *version), (json!({ "b": id, "c": true }), 3));
    }
}

/// A value a step takes out of the JSON is first written where it lives
/// now, by the step's fill, from the JSON as it was.
#[weft_db_macros::test]
async fn a_fill_moves_a_value_out_before_its_step_drops_it(pool: Pool) {
    ledger(&pool).await;
    pool.batch_execute(
        "CREATE TABLE probe_moving (id INTEGER PRIMARY KEY, owner TEXT, body JSONB NOT NULL, \
             body_version INTEGER NOT NULL DEFAULT 1);
         INSERT INTO probe_moving (id, body) VALUES (1, '{\"owner\": \"ann\", \"x\": 1}'), (2, '{\"owner\": \"bob\"}')",
    )
    .await
    .unwrap();
    apply_documents(&pool, &[&MOVING_DOC]).await.unwrap();
    let rows: Vec<(Option<String>, Value, i32)> = pool
        .all::<(Option<String>, Json<Value>, i32)>("SELECT owner, body, body_version FROM probe_moving ORDER BY id", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|(owner, Json(body), version)| (owner, body, version))
        .collect();
    assert_eq!(
        rows,
        vec![(Some("ann".into()), json!({ "x": 1 }), 2), (Some("bob".into()), json!({}), 2)]
    );
}

/// A step whose ops change nothing only moves values out: it is one
/// statement, so it needs no key to walk the rows by.
#[weft_db_macros::test]
async fn a_step_that_only_moves_values_out_needs_no_key(pool: Pool) {
    ledger(&pool).await;
    pool.batch_execute(
        "CREATE TABLE probe_keyless (name TEXT NOT NULL, body JSONB NOT NULL, body_version INTEGER NOT NULL DEFAULT 1);
         CREATE TABLE probe_moved (name TEXT PRIMARY KEY, n INTEGER NOT NULL);
         INSERT INTO probe_keyless (name, body) VALUES ('a', '{\"n\": 1}'), ('b', '{\"n\": 2}')",
    )
    .await
    .unwrap();
    apply_documents(&pool, &[&EMPTIED_DOC]).await.unwrap();
    let moved: Vec<(String, i32)> = pool.all("SELECT name, n FROM probe_moved ORDER BY name", &[]).await.unwrap();
    assert_eq!(moved, vec![("a".into(), 1), ("b".into(), 2)]);
    let versions: Vec<i32> = pool.scalars("SELECT body_version FROM probe_keyless", &[]).await.unwrap();
    assert_eq!(versions, vec![2, 2]);
}

/// A column recorded below today is carried from the version it is
/// recorded at: the steps before it ran, and a fill before them may read
/// what is gone since.
#[weft_db_macros::test]
async fn a_column_recorded_below_today_is_carried_from_there(pool: Pool) {
    /// The fill from version 1 reads a column the table no longer has: run
    /// again, it would fail the boot.
    static RECORDED_AT_TWO: Document = Document::of(
        &PROBE,
        "probe_doc",
        "body",
        Encoding::Json,
        &[Fill { from: 1, sql: "UPDATE probe_doc SET n = gone_since FROM probe_doc_gone WHERE body_version = 1" }],
        None,
    );
    tables(&pool, 0).await;
    pool.execute("INSERT INTO probe_doc (owner, n, body, body_version) VALUES ('two', 1, '{\"b\": 1}', 2)", &[]).await.unwrap();
    pool.execute("INSERT INTO weft_document_version (table_name, column_name, version) VALUES ('probe_doc', 'body', 2)", &[])
        .await
        .unwrap();
    apply_documents(&pool, &[&RECORDED_AT_TWO]).await.unwrap();
    let rows: Vec<(String, Value, i32)> = pool
        .all::<(String, Json<Value>, i32)>("SELECT owner, body, body_version FROM probe_doc WHERE body IS NOT NULL ORDER BY owner", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|(owner, Json(body), version)| (owner, body, version))
        .collect();
    assert_eq!(rows, vec![("two".into(), json!({ "b": 1, "c": true }), 3)]);
    assert_eq!(recorded(&pool, "probe_doc").await, 3);
}

/// A column the code reads, missing from its table, is a mistake, said as
/// one, naming the way to move a column that goes.
#[weft_db_macros::test]
async fn a_column_the_table_does_not_have_is_refused(pool: Pool) {
    static READ: Document = Document::of(&PROBE, "probe_doc", "units", Encoding::Json, &[], None);
    tables(&pool, 1).await;
    let err = format!("{:#}", apply_documents(&pool, &[&READ]).await.unwrap_err());
    assert!(err.contains("probe_doc.units") && err.contains("the code still reads it") && err.contains("DataStep"), "{err}");
}

/// A version column has no default: a write that forgets it fails on the
/// NOT NULL instead of being labelled with a version it may not hold.
#[weft_db_macros::test]
async fn a_write_naming_no_version_is_refused(pool: Pool) {
    static VERSIONED: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
        name: "probe_versioned",
        tables: &["probe_versioned"],
        ddl: &["CREATE TABLE IF NOT EXISTS probe_versioned (id INT PRIMARY KEY, body JSONB, body_version INTEGER NOT NULL)"],
        seed: &[],
        data_steps: &[],
    };
    static VERSIONED_DOC: Document = Document::of(&PROBE, "probe_versioned", "body", Encoding::Json, &[], None);
    weft_task_store::apply_schema(&pool, &[&VERSIONED], &[&VERSIONED_DOC]).await.unwrap();
    let forgot = pool
        .execute("INSERT INTO probe_versioned (id, body) VALUES (1, '{}')", &[])
        .await
        .expect_err("a write naming no version is refused");
    assert_eq!(forgot.code(), Some("23502"), "{forgot}");
}
