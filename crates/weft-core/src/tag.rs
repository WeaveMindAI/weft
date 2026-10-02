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

/// The most characters a tag may have.
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

/// Hex characters of the fingerprint [`normalize_tag`] appends to a
/// value it had to change: sixty-four bits of the value's sha256, so two
/// values that only differ in the replaced characters do not meet on one
/// tag, while the readable part keeps most of the room.
const FINGERPRINT_LEN: usize = 16;

/// Any string as a valid tag, the one rule every execution-tag call
/// (`ctx.tag_execution`, `ctx.stop_tagged`, a runs query's tag filter)
/// applies to what it is handed. A value that already is a tag is kept
/// as it is, so a chat id stays readable and the same clean value is
/// the same tag. Anything else is rewritten: every other character
/// becomes `_`, the readable part is cut to leave room, and a short
/// fingerprint of the ORIGINAL value is appended
/// (`49151@s.whatsapp.net` becomes `49151_s_whatsapp_net-3f7a92c14b0e6d18`),
/// so two different values never share a tag however alike they look
/// once cleaned. An empty value fails: there is nothing to tag with.
pub fn normalize_tag(value: &str) -> Result<String, TagError> {
    if value.is_empty() {
        return Err(TagError::Empty);
    }
    if validate_tag(value).is_ok() {
        return Ok(value.to_string());
    }
    let readable: String = value
        .chars()
        .map(|c| if is_allowed_char(c) { c } else { '_' })
        .take(MAX_LEN - FINGERPRINT_LEN - 1)
        .collect();
    let digest = crate::project::hash::sha256_hex(value.as_bytes());
    Ok(format!("{readable}-{}", &digest[..FINGERPRINT_LEN]))
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

    /// A valid tag is kept; anything else is cleaned, fingerprinted by
    /// the original value (so two values that clean alike stay apart),
    /// cut to fit, and stable when normalized again. Empty fails.
    #[test]
    fn normalize_tag_keeps_valid_tags_and_fingerprints_the_rest() {
        assert_eq!(normalize_tag("user_7").unwrap(), "user_7");
        let max = "x".repeat(MAX_LEN);
        assert_eq!(normalize_tag(&max).unwrap(), max);
        let tag = normalize_tag("49151@s.whatsapp.net").unwrap();
        assert!(tag.starts_with("49151_s_whatsapp_net-"), "{tag}");
        assert_eq!(tag.len(), "49151_s_whatsapp_net-".len() + FINGERPRINT_LEN);
        assert!(validate_tag(&tag).is_ok());
        assert_eq!(normalize_tag("49151@s.whatsapp.net").unwrap(), tag, "deterministic");
        assert_ne!(normalize_tag("+33 6 12").unwrap(), normalize_tag("+33.6.12").unwrap());
        let long = "y".repeat(MAX_LEN + 1);
        let cut = normalize_tag(&long).unwrap();
        assert_eq!(cut.len(), MAX_LEN);
        assert_ne!(cut, normalize_tag(&format!("{long}y")).unwrap());
        assert_eq!(normalize_tag(&cut).unwrap(), cut, "normalizing twice changes nothing");
        assert_eq!(normalize_tag(""), Err(TagError::Empty));
    }
}
