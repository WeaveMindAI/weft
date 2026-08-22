//! PostgresUpdateRows self-tests: the built SQL (layer 1).

use weft::{NodeTest, WeftResult};

use super::update_sql;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("builds_a_quoted_update_with_condition", builds),
        NodeTest::basic("the_condition_is_never_rewritten", condition_untouched),
        NodeTest::basic(
            "a_trailing_comment_cannot_eat_the_returning",
            a_trailing_comment_cannot_eat_the_returning,
        ),
    ]
}

fn builds() -> WeftResult<()> {
    let sql = update_sql("users", &["stage".into()], "email = $1", 2)?;
    assert_eq!(sql, "UPDATE \"users\" SET \"stage\" = $2 WHERE (\nemail = $1\n) RETURNING *");
    // Two SET columns after two where parameters: the SET numbering
    // continues from where the condition stopped.
    let sql = update_sql("users", &["a".into(), "b".into()], "x = $1 AND y = $2", 3)?;
    assert_eq!(
        sql,
        "UPDATE \"users\" SET \"a\" = $3, \"b\" = $4 WHERE (\nx = $1 AND y = $2\n) RETURNING *"
    );
    assert!(update_sql("users", &[], "true", 1).is_err(), "no columns refuses");
    assert!(update_sql("users", &["a".into()], "  ", 2).is_err(), "an empty where refuses");
    Ok(())
}

/// A condition ending in a line comment must not be able to reach the
/// `RETURNING *` that follows it. Unwrapped, `-- keep` comments the
/// RETURNING out: the update still happens and the node reports zero
/// changed rows, which is the worst answer available.
fn a_trailing_comment_cannot_eat_the_returning() -> WeftResult<()> {
    let sql = update_sql("users", &["stage".into()], "email = $1 -- keep", 2)?;
    let (_, after_comment) = sql.split_once("-- keep").expect("the comment survives");
    assert!(
        after_comment.contains('\n'),
        "the comment must end at a line break, before RETURNING: {sql}"
    );
    assert!(sql.trim_end().ends_with("RETURNING *"), "{sql}");
    Ok(())
}

/// The condition reaches Postgres byte for byte. These are the shapes
/// that used to be rewritten by hand and silently corrupted: text in
/// a quoted string, in a dollar-quoted block, and in a comment all
/// contain the same characters a placeholder does, and only a real
/// Postgres parser can tell them apart. Not rewriting is how that is
/// avoided rather than approximated.
fn condition_untouched() -> WeftResult<()> {
    for condition in [
        "note = 'costs $1 total' AND id = $1",
        "body = $$a $1 b$$ AND id = $1",
        "body = $tag$ $1 $tag$ AND id = $1",
        "body = E'it\\'s $1' AND id = $1",
        "\"user's name\" = $1",
        "id = $1 -- don't touch $2",
    ] {
        let sql = update_sql("t", &["c".into()], condition, 2)?;
        assert!(sql.contains(condition), "the condition was rewritten: {sql}");
    }
    Ok(())
}
