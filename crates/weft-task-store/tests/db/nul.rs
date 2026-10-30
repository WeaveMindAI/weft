//! Layer-3 tests for the two rules every statement follows about the NUL
//! character (`weft_task_store::db`'s `text` module), against a REAL
//! Postgres, which is what refuses it: text loses it on the way in,
//! whatever the statement, and a value kept as JSON text reads back with
//! it.

use serde_json::json;

use weft_task_store::db::{Client as _, JsonText, Pool};

/// Text a statement binds, alone or in an array, goes in without its NUL
/// characters, including where Postgres reads it as text itself.
#[weft_db_macros::test]
async fn text_holding_nul_is_written_without_it(pool: Pool) {
    pool.batch_execute("CREATE TABLE probe (id INT PRIMARY KEY, words TEXT, tags TEXT[], short VARCHAR(20))").await.unwrap();
    let tags: Vec<&str> = vec!["a\u{0}b", "clean"];
    pool.execute("INSERT INTO probe VALUES (1, $1, $2, $3)", &[&"page\u{0}text", &tags, &"v\u{0}c"]).await.unwrap();
    let (words, tags, short): (String, Vec<String>, String) =
        pool.one("SELECT words, tags, short FROM probe WHERE id = 1", &[]).await.unwrap();
    assert_eq!((words.as_str(), tags, short.as_str()), ("pagetext", vec!["ab".to_string(), "clean".to_string()], "vc"));
    let indexed: String = pool.scalar("SELECT to_tsvector('simple', $1::text)::text", &[&"scraped\u{0}words"]).await.unwrap();
    assert_eq!(indexed, "'scrapedwords':1");
}

/// A value kept as JSON text reads back exactly, NUL characters
/// included.
#[weft_db_macros::test]
async fn json_text_keeps_nul_exactly(pool: Pool) {
    pool.batch_execute("CREATE TABLE probe (id INT PRIMARY KEY, doc TEXT NOT NULL)").await.unwrap();
    let doc = json!({ "page": "a\u{0}b", "n": 1 });
    pool.execute("INSERT INTO probe VALUES (1, $1)", &[&JsonText(&doc)]).await.unwrap();
    let back: JsonText<serde_json::Value> = pool.scalar("SELECT doc FROM probe WHERE id = 1", &[]).await.unwrap();
    assert_eq!(back.0, doc);
}
