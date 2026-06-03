//! Parsing the interior of a MARKER token (`@directive(args)`).
//!
//! A MARKER is one opaque token to the lexer/parser; its directive name and
//! arguments are extracted here, in ONE place, so every consumer (the parser's
//! classifier, the lowering's `@require_one_of`/`@include`/`@file` handlers)
//! agrees on the grammar. The directive name is matched EXACTLY (so
//! `@includes_other` is not mistaken for `@include`). The arg-paren must follow
//! the name IMMEDIATELY (no space): the lexer only folds `(...)` into the marker
//! token when it directly abuts `@name` (see `marker_len`), so `@name (args)`
//! with a space is a bare `@name` marker plus separate tokens. The consumers
//! reflect that same rule (no whitespace handling here), and the lowering fails
//! loud on a directive that needs args but, because of the space, has none.

/// The directive name: the identifier after `@`, up to the first `(` or
/// whitespace. `@include("x")` -> `include`, `@includes_other(...)` ->
/// `includes_other`. Stopping at whitespace too means a bare `@name` whose
/// `(args)` split off across a space (the lexer never folds a spaced paren into
/// the token) still yields the clean name `name`, so the consumer matches the
/// directive and then fails loud on the now-missing args.
pub fn directive(marker_text: &str) -> &str {
    let after_at = marker_text.trim_start().trim_start_matches('@');
    let end = after_at
        .find(|c: char| c == '(' || c.is_whitespace())
        .unwrap_or(after_at.len());
    &after_at[..end]
}

/// The raw argument string between the outermost `(` and the last `)`, or None
/// if the marker has no balanced parens. The paren abuts the name (the lexer
/// guarantees no interior whitespace before it).
pub fn args_raw(marker_text: &str) -> Option<&str> {
    let open = marker_text.find('(')?;
    let close = marker_text.rfind(')')?;
    if close > open {
        Some(marker_text[open + 1..close].trim())
    } else {
        None
    }
}

/// The comma-separated argument list (trimmed, empties dropped). For
/// `@require_one_of(a, b)` -> `["a", "b"]`. None if there are no parens.
pub fn args_list(marker_text: &str) -> Option<Vec<String>> {
    args_raw(marker_text).map(|body| {
        body.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    })
}

/// The error for a `@require_one_of` whose `(...)` never closes (`@require_one_of(a, b`
/// or a paren that runs past the line, since a marker is single-line).
pub const REQUIRE_ONE_OF_UNCLOSED: &str = "@require_one_of missing closing parenthesis";

/// The error for a `@require_one_of` with NO port list at all: the common typo
/// `@require_one_of (a, b)` (a space splits `(a, b)` off the marker, since the
/// lexer only folds `(...)` into the token when it abuts `@name`), a bare
/// `@require_one_of`, or an empty `()`.
pub const REQUIRE_ONE_OF_NEEDS_ARGS: &str =
    "@require_one_of needs a parenthesized port list directly after the name, e.g. `@require_one_of(a, b)` (no space before `(`)";

/// Validate a `@require_one_of` marker's args: `Ok(ports)` for a non-empty list,
/// `Err` with the precise message otherwise. The caller has already matched
/// `directive(...) == "require_one_of"`. The single validity gate so every
/// `@require_one_of` site fails loud identically (never a silent drop), and the
/// two malformations (unbalanced parens vs no/empty port list) keep distinct,
/// actionable messages.
pub fn require_one_of_ports(marker_text: &str) -> Result<Vec<String>, &'static str> {
    // A `(` with no balanced `)` is specifically an unclosed paren; report that
    // rather than the generic "needs a port list" (which would misdescribe it).
    if marker_text.contains('(') && args_raw(marker_text).is_none() {
        return Err(REQUIRE_ONE_OF_UNCLOSED);
    }
    match args_list(marker_text) {
        Some(ports) if !ports.is_empty() => Ok(ports),
        _ => Err(REQUIRE_ONE_OF_NEEDS_ARGS),
    }
}
