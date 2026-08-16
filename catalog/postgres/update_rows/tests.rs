//! PostgresUpdateRows self-tests: the built SQL and the placeholder
//! renumbering (layer 1).

use weft::{NodeTest, WeftResult};

use super::{offset_placeholders, update_sql};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("builds_a_quoted_update_with_condition", builds),
        NodeTest::basic("condition_placeholders_renumber_after_the_set", offsets),
    ]
}

fn builds() -> WeftResult<()> {
    let sql = update_sql("users", &["stage".into()], "email = $2")?;
    assert_eq!(sql, "UPDATE \"users\" SET \"stage\" = $1 WHERE email = $2 RETURNING *");
    assert!(update_sql("users", &[], "true").is_err(), "no columns refuses");
    assert!(update_sql("users", &["a".into()], "  ").is_err(), "an empty where refuses");
    Ok(())
}

fn offsets() -> WeftResult<()> {
    assert_eq!(offset_placeholders("email = $1 AND age > $2", 3), "email = $4 AND age > $5");
    assert_eq!(offset_placeholders("no placeholders", 3), "no placeholders");
    assert_eq!(offset_placeholders("cost > $10", 2), "cost > $12");
    Ok(())
}
