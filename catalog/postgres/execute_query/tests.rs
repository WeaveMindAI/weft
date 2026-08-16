//! PostgresExecuteQuery self-tests: the pure parameter/identifier
//! plumbing (layer 1). The query path itself needs a real database
//! and is exercised end to end there.

use serde_json::json;

use weft::{NodeTest, WeftResult};

use super::super::postgres::{params_of, quote_ident};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("params_map_every_json_shape", params_map),
        NodeTest::basic("identifiers_quote_and_refuse_empty", idents),
    ]
}

fn params_map() -> WeftResult<()> {
    let params = params_of(&[
        json!(null),
        json!(true),
        json!(4.5),
        json!("text"),
        json!({ "a": 1 }),
        json!([1, 2]),
    ]);
    assert_eq!(params.len(), 6, "every JSON shape binds as a parameter");
    Ok(())
}

fn idents() -> WeftResult<()> {
    assert_eq!(quote_ident("users")?, "\"users\"");
    assert_eq!(quote_ident("we\"ird")?, "\"we\"\"ird\"");
    assert!(quote_ident("  ").is_err(), "an empty identifier refuses");
    Ok(())
}
