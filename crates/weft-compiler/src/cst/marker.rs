//! Parsing the interior of a MARKER token (`@directive(args)`).
//!
//! A MARKER is one opaque token to the lexer/parser; its directive name and
//! arguments are extracted here, in ONE place, so every consumer (the parser's
//! classifier, the lowering's `@require_one_of`/`@include`/`@file` handlers)
//! agrees on the grammar. The directive name is matched EXACTLY (so
//! `@includes_other` is not mistaken for `@include`). The arg-paren must follow
//! the name IMMEDIATELY (no space): the lexer only folds `(...)` into the marker
//! token when it directly abuts `@name` (see the lexer's `marker_len`), so
//! `@name (args)` with a space is a bare `@name` marker plus separate tokens.
//! The consumers reflect that same rule (no whitespace handling here), and the
//! lowering fails loud on a directive that needs args but, because of the
//! space, has none.
//!
//! Token BOUNDARIES belong to the lexer (`marker_len`, `balanced_span`); this
//! module CLASSIFIES an already-bounded text: which directive, and which of
//! the four argument shapes it carries ([`MarkerArgs`]).

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

/// The four shapes a directive's argument list can be in, decided by ONE
/// balanced scan so no consumer re-derives the reason from string
/// searches (a `)` inside a quoted path once made "unclosed" read as
/// "trailing text").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerArgs<'a> {
    /// A well-formed list: the `(` abuts the name, the matching `)` is
    /// found (strings opaque), and nothing but whitespace follows it.
    Args(&'a str),
    /// No `(` abuts the name at all: a bare `@name`, or a spaced
    /// `@name (...)` (which is prose with a parenthetical, never an
    /// argument list; tolerating it once made ordinary sentences read a
    /// file off disk).
    NoList,
    /// A `(` abuts the name but its matching `)` never arrives.
    Unclosed,
    /// The list closes, but text follows it. Never treated as the
    /// marker alone: the surrounding words would be silently swallowed
    /// by whatever the marker resolves to.
    TrailingText,
}

/// Classify `marker_text`'s argument list. See [`MarkerArgs`].
pub fn args(marker_text: &str) -> MarkerArgs<'_> {
    let t = marker_text.trim_start();
    let after_at = t.strip_prefix('@').unwrap_or(t);
    let name_end = after_at
        .find(|c: char| c == '(' || c.is_whitespace())
        .unwrap_or(after_at.len());
    let Some(rest) = after_at[name_end..].strip_prefix('(') else {
        return MarkerArgs::NoList;
    };
    let bytes = rest.as_bytes();
    let mut depth = 1usize;
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if in_string => i += 1,
            b'"' => in_string = !in_string,
            b'(' if !in_string => depth += 1,
            b')' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    if !rest[i + 1..].trim().is_empty() {
                        return MarkerArgs::TrailingText;
                    }
                    return MarkerArgs::Args(rest[..i].trim());
                }
            }
            _ => {}
        }
        i += 1;
    }
    MarkerArgs::Unclosed
}

/// The raw argument string when the list is well-formed, else None.
/// [`args`] tells the shapes apart for consumers that word their errors.
pub fn args_raw(marker_text: &str) -> Option<&str> {
    match args(marker_text) {
        MarkerArgs::Args(body) => Some(body),
        _ => None,
    }
}

/// Whether a `(` directly abuts the directive name. The paren is what
/// STATES INTENT to be a marker: `@file("x"` (unclosed) is a broken
/// marker to report, while `@file the report` is a sentence to leave
/// alone. Consumers that tolerate prose branch on this before treating
/// a malformed list as an error.
pub fn has_abutting_args(marker_text: &str) -> bool {
    !matches!(args(marker_text), MarkerArgs::NoList)
}

/// The comma-separated argument list (trimmed, empties dropped). For
/// `@require_one_of(a, b)` -> `["a", "b"]`. None if the list is absent
/// or malformed.
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
/// malformations keep distinct, actionable messages.
pub fn require_one_of_ports(marker_text: &str) -> Result<Vec<String>, &'static str> {
    match args(marker_text) {
        MarkerArgs::Unclosed => Err(REQUIRE_ONE_OF_UNCLOSED),
        MarkerArgs::Args(body) if !body.is_empty() => {
            let ports: Vec<String> = body
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if ports.is_empty() {
                Err(REQUIRE_ONE_OF_NEEDS_ARGS)
            } else {
                Ok(ports)
            }
        }
        _ => Err(REQUIRE_ONE_OF_NEEDS_ARGS),
    }
}
