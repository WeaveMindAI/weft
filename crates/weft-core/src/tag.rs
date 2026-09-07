//! Tag validation. Tags are user-supplied strings, and the same string
//! rule serves two things that are otherwise unrelated: a NODE's tags
//! (`_tags` in its config, a compile-time label used for token-scoped
//! signal enumeration: a token with `allowed_tags = ["t1"]` sees only
//! signals tagged `t1`) and an EXECUTION's tags (`ctx.tag_execution`, a
//! run-time label a sibling run can `ctx.stop_tagged` on). One
//! validator, two homes; nothing else is shared.
//!
//! Charset is intentionally narrow: `[A-Za-z0-9_-]{1,64}`. Reasons:
//!   - URL-safe: tags appear in query params on listing routes.
//!   - Filter-safe: rules out anything that could even superficially
//!     look like a SQL fragment, even though we always use
//!     parameterized queries on TEXT[] columns.
//!   - Predictable: matches the AWS / GCP / Kubernetes label-value
//!     convention so users get the same constraints they expect.

use serde::{Deserialize, Serialize};

/// The reserved config key that carries a node's tags. The ONE definition; the
/// compiler (validation + reserved-key allow-list) and the runtime tag reader
/// reference this instead of re-spelling the literal.
/// SYNC: TAGS_CONFIG_KEY <-> packages/weft-graph/src/webview/lib/node-tags.ts (TAGS_CONFIG_KEY)
pub const TAGS_CONFIG_KEY: &str = "_tags";

/// Whether `ctx.stop_tagged` counts the calling execution among the
/// ones it stops. A named choice rather than a bare boolean, because
/// `stop_tagged("x", true)` at a call site says nothing about which way
/// `true` points.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopSelf {
    /// Stop the others and keep running. The debounce shape: a run tags
    /// itself with a sender's id, then stops every EARLIER run carrying
    /// it, so only the latest message is answered.
    Keep,
    /// Stop every run carrying the tag, this one included. The "we are
    /// all busted" shape: one run of an experiment finds the experiment
    /// is broken and takes the whole batch down with it.
    Include,
}

/// The most characters a tag may have. A node that turns arbitrary
/// text into a tag (`catalog/logic/steering.rs`) cuts to this.
pub const MAX_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagError {
    Empty,
    TooLong { len: usize },
    InvalidChar { tag: String, ch: char },
}

impl std::fmt::Display for TagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TagError::Empty => write!(f, "tag must not be empty"),
            TagError::TooLong { len } => write!(
                f,
                "tag is {len} chars; max is {MAX_LEN}"
            ),
            TagError::InvalidChar { tag, ch } => write!(
                f,
                "tag '{tag}' contains invalid character '{ch}'; allowed: A-Z a-z 0-9 _ -"
            ),
        }
    }
}

impl std::error::Error for TagError {}

/// Validate a single tag against the charset rule.
pub fn validate_tag(tag: &str) -> Result<(), TagError> {
    if tag.is_empty() {
        return Err(TagError::Empty);
    }
    if tag.len() > MAX_LEN {
        return Err(TagError::TooLong { len: tag.len() });
    }
    for ch in tag.chars() {
        if !is_allowed_char(ch) {
            return Err(TagError::InvalidChar {
                tag: tag.to_string(),
                ch,
            });
        }
    }
    Ok(())
}

/// Validate every tag in a list. Returns the first error found.
pub fn validate_tags(tags: &[String]) -> Result<(), TagError> {
    for t in tags {
        validate_tag(t)?;
    }
    Ok(())
}

fn is_allowed_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_alphanumeric_underscore_dash() {
        assert!(validate_tag("support").is_ok());
        assert!(validate_tag("team_alpha").is_ok());
        assert!(validate_tag("v1-stable").is_ok());
        assert!(validate_tag("a").is_ok());
        assert!(validate_tag(&"x".repeat(64)).is_ok());
    }

    #[test]
    fn rejects_invalid_inputs() {
        assert!(validate_tag("").is_err());
        assert!(validate_tag(&"x".repeat(65)).is_err());
        assert!(validate_tag("has space").is_err());
        assert!(validate_tag("tag;drop").is_err());
        assert!(validate_tag("co'mma").is_err());
        assert!(validate_tag("dot.tag").is_err());
        assert!(validate_tag("café").is_err());
    }
}
