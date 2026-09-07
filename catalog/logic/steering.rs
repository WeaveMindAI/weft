//! Shared by `TagRun` and `StopTagged`: how a steering node reads the
//! tags it was handed, and how any string becomes a tag the runtime
//! accepts. One reader and one sanitizer, so the two nodes can never
//! disagree on what counts as a tag, and the same value always lands
//! on the same tag from either side.

use sha2::{Digest, Sha256};
use weft::{node_bail, ExecutionContext, WeftResult};

/// Hex characters of the fingerprint appended to a value that had to
/// be changed: sixty-four bits of the value's sha256, so two values
/// that only differ in the characters the sanitizer replaced do not
/// meet on one tag, while the readable part keeps most of the room.
const FINGERPRINT_LEN: usize = 16;

/// The tags a steering node was handed: every created input (one tag
/// each, typed `String` by both manifests) plus the optional `tags`
/// list, created ports first, duplicates dropped, each turned into a
/// safe tag by [`safe_tag`]. Empty is a failure: a node with nothing to
/// tag or stop is a wiring mistake, never a silent no-op.
pub fn tags_from_inputs(ctx: &ExecutionContext) -> WeftResult<Vec<String>> {
    let mut raw: Vec<String> = Vec::new();
    for (name, value) in ctx.inputs.custom() {
        // `tags` is the declared list, read below; everything else in
        // the instance data is a created port.
        if name == "tags" {
            continue;
        }
        match value.as_str() {
            Some(tag) => raw.push(tag.to_string()),
            None => node_bail!("input '{name}' must carry a string tag, got {value}"),
        }
    }
    if let Some(list) = ctx.inputs.opt::<Vec<String>>("tags")? {
        raw.extend(list);
    }
    let mut tags = Vec::with_capacity(raw.len());
    for value in raw {
        tags.push(safe_tag(&value)?);
    }
    let mut seen = std::collections::HashSet::new();
    tags.retain(|t| seen.insert(t.clone()));
    if tags.is_empty() {
        node_bail!(
            "no tags: wire at least one input onto the node (`TagRun(sender: String)`) or a \
             `tags` list"
        );
    }
    Ok(tags)
}

/// Any string as a tag the runtime accepts. A value that already is
/// one (letters, digits, `_`, `-`, at most 64 characters) is kept as
/// it is, so a Telegram chat id stays readable and tagging twice with
/// the same clean value is the same tag. Anything else is rewritten:
/// every other character becomes `_`, the readable part is cut to
/// leave room, and a short fingerprint of the ORIGINAL value is
/// appended (`49151@s.whatsapp.net` becomes
/// `49151_s_whatsapp_net-3f7a92c14b0e6d18`), so two different values
/// never share a tag however alike they look once cleaned. An empty
/// value is a failure: there is nothing to tag with.
pub fn safe_tag(value: &str) -> WeftResult<String> {
    if value.is_empty() {
        node_bail!("a tag must not be empty");
    }
    if weft::tag::validate_tag(value).is_ok() {
        return Ok(value.to_string());
    }
    let readable: String = value
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .take(weft::tag::MAX_LEN - FINGERPRINT_LEN - 1)
        .collect();
    let digest = Sha256::digest(value.as_bytes());
    let fingerprint: String = digest.iter().map(|b| format!("{b:02x}")).collect::<String>()[..FINGERPRINT_LEN].to_string();
    Ok(format!("{readable}-{fingerprint}"))
}
