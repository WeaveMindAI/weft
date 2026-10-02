//! PostgresUpdateRows self-tests: the built SQL (layer 1).

use weft::{NodeTest, WeftResult};

use super::update_sql;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("builds_a_quoted_update_with_condition", builds),
        NodeTest::basic("only_real_placeholders_are_ports", only_real_placeholders),
        NodeTest::basic(
            "a_trailing_comment_cannot_eat_the_returning",
            a_trailing_comment_cannot_eat_the_returning,
        ),
    ]
}

fn builds() -> WeftResult<()> {
    let (sql, names) = update_sql("users", &["stage".into()], "email = $email")?;
    assert_eq!(sql, "UPDATE \"users\" SET \"stage\" = $2 WHERE (\nemail = $1\n) RETURNING *");
    assert_eq!(names, vec!["email".to_string()]);
    // Two SET columns after two where ports, one of them read twice:
    // each port binds once, and the SET numbering continues after them.
    let (sql, names) =
        update_sql("users", &["a".into(), "b".into()], "x = $x AND (y = $y OR x2 = $x)")?;
    assert_eq!(
        sql,
        "UPDATE \"users\" SET \"a\" = $3, \"b\" = $4 WHERE (\nx = $1 AND (y = $2 OR x2 = $1)\n) \
         RETURNING *"
    );
    assert_eq!(names, vec!["x".to_string(), "y".to_string()]);
    let (sql, names) = update_sql("users", &["a".into()], "true")?;
    assert_eq!(sql, "UPDATE \"users\" SET \"a\" = $1 WHERE (\ntrue\n) RETURNING *");
    assert!(names.is_empty());
    assert!(update_sql("users", &[], "true").is_err(), "no columns refuses");
    assert!(update_sql("users", &["a".into()], "  ").is_err(), "an empty where refuses");
    assert!(update_sql("users", &["a".into()], "id = $1").is_err(), "a positional $1 refuses");
    assert!(
        update_sql("users", &["a".into()], "true; DROP TABLE users").is_err(),
        "a second statement refuses"
    );
    Ok(())
}

/// A condition ending in a line comment must not be able to reach the
/// `RETURNING *` that follows it. Unwrapped, `-- keep` comments the
/// RETURNING out: the update still happens and the node reports zero
/// changed rows, which is the worst answer available.
fn a_trailing_comment_cannot_eat_the_returning() -> WeftResult<()> {
    let (sql, _) = update_sql("users", &["stage".into()], "email = $email -- keep")?;
    let (_, after_comment) = sql.split_once("-- keep").expect("the comment survives");
    assert!(
        after_comment.contains('\n'),
        "the comment must end at a line break, before RETURNING: {sql}"
    );
    assert!(sql.trim_end().ends_with("RETURNING *"), "{sql}");
    Ok(())
}

/// A `$` inside a string, a dollar-quoted body, a quoted identifier or
/// a comment is SQL, not a port: those stay byte for byte, and only the
/// real `$id` becomes a parameter.
fn only_real_placeholders() -> WeftResult<()> {
    for (condition, kept) in [
        ("note = 'costs $id total' AND id = $id", "note = 'costs $id total' AND id = $1"),
        ("body = $$a $id b$$ AND id = $id", "body = $$a $id b$$ AND id = $1"),
        ("body = $tag$ $id $tag$ AND id = $id", "body = $tag$ $id $tag$ AND id = $1"),
        ("body = E'it\\'s $id' AND id = $id", "body = E'it\\'s $id' AND id = $1"),
        ("\"user's $id\" = $id", "\"user's $id\" = $1"),
        ("id = $id -- don't touch $other", "id = $1 -- don't touch $other"),
    ] {
        let (sql, names) = update_sql("t", &["c".into()], condition)?;
        assert!(sql.contains(kept), "{condition:?} became {sql}");
        assert_eq!(names, vec!["id".to_string()], "{condition:?}");
    }
    Ok(())
}
