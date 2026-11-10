//! The boot's data step that fills `run_cost` from the records an older
//! weft left (`weft_journal::costs::carry_from_records`) reads the records
//! a page at a time, each page bounded by the size of the rows it holds as
//! well as by their count.

use weft_core::CredentialOwner;
use weft_journal::events::RunEvent;
use weft_task_store::db::{Client as _, Pool};
use weft_task_store::pages::PageCaps;

/// How many statements wrote costs during one carry in `caps`: a page
/// writes its costs in one, which a trigger counts here.
async fn pages(pool: &Pool, caps: PageCaps) -> i64 {
    pool.execute("DELETE FROM probe_page", &[]).await.unwrap();
    let mut conn = pool.get().await.unwrap();
    let tx = conn.transaction().await.unwrap();
    weft_journal::costs::carry_from_records_in(&tx, caps).await.expect("the data step");
    tx.commit().await.unwrap();
    pool.scalar("SELECT count(*) FROM probe_page", &[]).await.unwrap()
}

#[weft_db_macros::test]
async fn the_cost_carry_reads_the_records_in_pages_bounded_by_their_size(pool: Pool) {
    weft_runtime::schema::apply(&pool).await.unwrap();
    pool.batch_execute(
        "CREATE TABLE probe_page (seq BIGSERIAL PRIMARY KEY);
         CREATE FUNCTION probe_note_page() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
             INSERT INTO probe_page DEFAULT VALUES; RETURN NULL;
         END $$;
         CREATE TRIGGER probe_note_page AFTER INSERT ON run_cost FOR EACH STATEMENT EXECUTE FUNCTION probe_note_page()",
    )
    .await
    .unwrap();
    // Five rows of one run's record, each reporting a cost. The run itself
    // is not there, so no cost lands: a page's statement still runs.
    let run_id = weft_core::new_run_id();
    for seq in 0..5i32 {
        let events = [RunEvent::CostReported {
            run_id,
            node_id: "llm".into(),
            frames: Vec::new(),
            cost_id: format!("c{seq}"),
            service: "openai".into(),
            model: None,
            amount_usd: Some(1.0),
            billed: false,
            origin: CredentialOwner::Platform,
            metadata: serde_json::Value::Null,
            charge: None,
            at_unix: 1,
        }];
        pool.execute(
            "INSERT INTO run_log (run_id, seq, events, events_version, writer, written_at) VALUES ($1, $2, $3, $4, 'probe', 0)",
            &[&run_id, &seq, &weft_journal::stored::encode(&events), &weft_journal::stored::STORED_EVENTS.version()],
        )
        .await
        .unwrap();
    }

    assert_eq!(pages(&pool, weft_journal::costs::CARRY_PAGE).await, 1, "five small rows fit one page");
    assert_eq!(pages(&pool, PageCaps { rows: 200, bytes: 1 }).await, 5, "a byte cap under one row: one row a page");
    assert_eq!(pages(&pool, PageCaps { rows: 2, bytes: i64::MAX }).await, 3, "the row cap still holds");
}
