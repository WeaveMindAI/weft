//! What a node shows about itself while it runs: its **display**.
//!
//! A WhatsApp bridge shows a QR code to scan and then who is paired. A
//! database shows the credential it just minted. A webhook trigger
//! shows the address it is listening on and how its door checks a
//! caller. All of that is one shape, defined here once, so whoever reads
//! it (the editor's node panel, a website somebody built on the
//! program) draws the same four item types whatever produced them.
//!
//! Two things produce a display, and both serve it the same way, on a
//! `GET /live` returning [`LiveFeed`]:
//!
//!   - an **infra node's container**, which the node's author writes.
//!     The node opts in by naming the endpoint in
//!     `features.liveEndpoint`.
//!   - a **trigger's kind** in the listener. The kind owns the
//!     registration (the address, the auth, the key), so the kind is
//!     what has something to say; the node that declared the trigger
//!     never writes display code.
//!
//! An item of an INFRA node's display may carry one button. Pressing it
//! posts `{ "action": <actionKind>, "payload": ... }` back to the
//! container that served the feed, which is the only party that knows
//! what the press means. A trigger's display is read-only: nothing
//! about a registration is a reader's to change from a panel, and no
//! door carries a press to a listener, so the listener refuses to
//! serve a feed with a button rather than show one that cannot work.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What a token is granted when it is given one node's display:
/// `<project id>/<node>`, where the node is spelled the way a person
/// writes it (`test.whatsapp`).
///
/// One definition, because three places build this string (the CLI
/// minting it, the dispatcher matching it, the e2e rig standing in for
/// the CLI) and nothing else would catch them disagreeing: a changed
/// separator would compile everywhere and only fail in front of a
/// user.
///
/// Both halves are needed. That spelling is only a name inside one
/// project, and a project id says nothing about which of its displays
/// to open, one of which can be a credential.
pub fn display_grant(project_id: &uuid::Uuid, node: &str) -> String {
    format!("{project_id}/{node}")
}

/// Read a grant back: its project, and the node as the person wrote
/// it. `None` when the string is not a grant at all.
///
/// The FIRST slash separates the pair, which makes the parse total:
/// whatever follows is the node, so no grant reads two ways.
pub fn split_display_grant(grant: &str) -> Option<(uuid::Uuid, &str)> {
    let (project, node) = grant.split_once('/')?;
    if node.trim().is_empty() {
        return None;
    }
    Some((project.parse::<uuid::Uuid>().ok()?, node))
}

/// One `GET /live` answer: everything the node has to show, in order.
// SYNC: LiveFeed <-> crates/weft-e2e/src/display.rs, docs/src/nodes/infrastructure.md (the routes your container serves)
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct LiveFeed {
    #[serde(default)]
    pub items: Vec<LiveItem>,
}

impl LiveFeed {
    pub fn new(items: Vec<LiveItem>) -> Self {
        Self { items }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Read what a container answered on its `/live`.
    ///
    /// The ENVELOPE has to be right: an answer that is not an object
    /// with an `items` array is a container that is not serving a
    /// display, and that comes back as an error rather than as an
    /// empty panel, which would read as "nothing to report".
    ///
    /// One unreadable ITEM does not take the rest down. It becomes a
    /// line saying so, in its own place, so a container that grew a
    /// fifth item type still shows its other four to an older weft and
    /// the reader can see exactly what did not come through.
    pub fn from_answer(answer: &Value) -> Result<Self, String> {
        let raw = answer
            .get("items")
            .ok_or_else(|| format!("expected {{ \"items\": [...] }}, got {answer}"))?
            .as_array()
            .ok_or_else(|| "`items` is not an array".to_string())?;
        Ok(Self::new(
            raw.iter()
                .map(|item| {
                    match serde_json::from_value::<LiveItem>(item.clone()) {
                        Ok(read) => read.or_unreadable(),
                        Err(e) => LiveItem::text("Unreadable item", format!("{e}: {item}")),
                    }
                })
                .collect(),
        ))
    }
}

/// How one line of a display is drawn.
// SYNC: LiveItemKind <-> packages/weft-graph/src/protocol.ts LIVE_DATA_TYPES
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LiveItemKind {
    /// A copyable box of plain text.
    Text,
    /// Anything an `<img src>` takes, so a `data:` URI or a URL.
    Image,
    /// A number from 0 to 1, drawn as a bar.
    Progress,
    /// Text behind a `••••` mask until the reader clicks the eye. Copy
    /// hands over the real value either way. For an API key, a signed
    /// URL, anything that should not sit on somebody's screen.
    Secret,
}

impl LiveItemKind {
    /// The word the wire uses, which is the word a node's author wrote
    /// in `type`. Quoting anything else back at them (Rust's own
    /// spelling, say) names a kind they never typed.
    ///
    /// Read off the serialization rather than spelled out again here,
    /// so there is one place the wire words live.
    pub fn wire(self) -> String {
        match serde_json::to_value(self) {
            Ok(Value::String(word)) => word,
            other => unreachable!("a unit variant serializes to a string, got {other:?}"),
        }
    }
}

/// One line of a display.
// SYNC: LiveItem <-> packages/weft-graph/src/protocol.ts LiveDataItem, crates/weft-e2e/src/display.rs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LiveItem {
    #[serde(rename = "type")]
    pub kind: LiveItemKind,
    pub label: String,
    /// A string for `text`, `image` and `secret`; a number from 0 to 1
    /// for `progress`. A reader that gets the wrong one for the kind
    /// shows the label and says the value was unrenderable, rather
    /// than dropping the line quietly.
    pub data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<LiveAction>,
}

impl LiveItem {
    pub fn text(label: impl Into<String>, data: impl Into<String>) -> Self {
        Self::of(LiveItemKind::Text, label, Value::String(data.into()))
    }

    pub fn image(label: impl Into<String>, data: impl Into<String>) -> Self {
        Self::of(LiveItemKind::Image, label, Value::String(data.into()))
    }

    pub fn secret(label: impl Into<String>, data: impl Into<String>) -> Self {
        Self::of(LiveItemKind::Secret, label, Value::String(data.into()))
    }

    /// `fraction` is 0 to 1. Out-of-range values are kept as given:
    /// clamping here would hide a producer's bug behind a full bar.
    ///
    /// A fraction that is not a number at all (NaN, infinity) cannot
    /// ride on the wire, which carries only finite numbers, so it
    /// becomes the line that says so rather than a silent `null` the
    /// reader would drop.
    pub fn progress(label: impl Into<String>, fraction: f64) -> Self {
        let label = label.into();
        match serde_json::Number::from_f64(fraction) {
            Some(number) => Self::of(LiveItemKind::Progress, label, Value::Number(number)),
            None => Self::unreadable(&label, &format!("a 'progress' item carried {fraction}")),
        }
    }

    fn of(kind: LiveItemKind, label: impl Into<String>, data: Value) -> Self {
        Self { kind, label: label.into(), data, action: None }
    }

    /// Is this item's `data` the shape its kind draws?
    ///
    /// `data` is a `Value`, so a container can serve
    /// `{"type":"text","data":true}` and every serde check passes.
    /// Only the kind says what the payload has to be: a string for
    /// `text`, `image` and `secret`, a number for `progress`.
    pub fn data_matches_kind(&self) -> bool {
        match self.kind {
            LiveItemKind::Progress => self.data.is_number(),
            LiveItemKind::Text | LiveItemKind::Image | LiveItemKind::Secret => {
                self.data.is_string()
            }
        }
    }

    /// This item, or the line saying it could not be drawn.
    ///
    /// A reader draws four shapes and nothing else, so an item whose
    /// data does not fit its kind has to become visible text HERE, at
    /// the door. Handing it on lets the last renderer in the chain drop
    /// it, and a display quietly missing a line is the one failure
    /// nobody can see.
    ///
    /// The BUTTON survives: it is the node's, it still works, and the
    /// value being undrawable says nothing about the press.
    fn or_unreadable(self) -> Self {
        if self.data_matches_kind() {
            return self;
        }
        let said = format!("a '{}' item carried {}", self.kind.wire(), self.data);
        let mut line = Self::unreadable(&self.label, &said);
        line.action = self.action;
        line
    }

    /// The line that stands in for an item nothing can draw, named
    /// after the item it replaces so the reader knows which one it was.
    fn unreadable(label: &str, said: &str) -> Self {
        Self::text(format!("{label} (unreadable)"), said)
    }

    /// Put a button on this item.
    pub fn with_action(mut self, action: LiveAction) -> Self {
        self.action = Some(action);
        self
    }
}

/// A button on one item of an infra node's display. `action_kind` is
/// the name the container answers to on its own `/action`; nothing
/// between the two reads it. A listener kind never puts one on a
/// trigger's display (see the module doc).
// SYNC: LiveAction <-> packages/weft-graph/src/protocol.ts LiveDataItem.action, crates/weft-dispatcher/src/api/infra.rs InfraActionBody
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LiveAction {
    pub label: String,
    #[serde(rename = "actionKind")]
    pub action_kind: String,
    /// Sent with the press when the button carries data of its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    /// Ask this before pressing. Present means the press is worth a
    /// second thought (it detaches a paired phone, it invalidates a
    /// key somebody is using).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm: Option<String>,
}

impl LiveAction {
    pub fn new(label: impl Into<String>, action_kind: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            action_kind: action_kind.into(),
            payload: None,
            confirm: None,
        }
    }

    pub fn confirm(mut self, ask: impl Into<String>) -> Self {
        self.confirm = Some(ask.into());
        self
    }

    pub fn payload(mut self, payload: Value) -> Self {
        self.payload = Some(payload);
        self
    }
}

#[cfg(test)]
mod live_wire_tests {
    use super::*;

    /// Layer-2 wire shape. A container written in JavaScript and the
    /// listener written in Rust both serve this, and the editor reads
    /// both through one parser, so the JSON has to come out identical.
    #[test]
    fn a_feed_serializes_the_shape_every_container_already_serves() {
        let feed = LiveFeed::new(vec![
            LiveItem::image("Scan with WhatsApp", "data:image/png;base64,AAA"),
            LiveItem::text("Phone", "paired").with_action(
                LiveAction::new("Disconnect phone", "unpair")
                    .confirm("Detach the paired phone?"),
            ),
            LiveItem::progress("Restore", 0.25),
        ]);
        assert_eq!(
            serde_json::to_value(&feed).unwrap(),
            serde_json::json!({
                "items": [
                    { "type": "image", "label": "Scan with WhatsApp", "data": "data:image/png;base64,AAA" },
                    {
                        "type": "text",
                        "label": "Phone",
                        "data": "paired",
                        "action": {
                            "label": "Disconnect phone",
                            "actionKind": "unpair",
                            "confirm": "Detach the paired phone?"
                        }
                    },
                    { "type": "progress", "label": "Restore", "data": 0.25 }
                ]
            })
        );
    }

    #[test]
    fn the_wire_words_are_the_ones_every_reader_was_told() {
        // The contract a node's author writes against, and what the
        // TypeScript union lists. Renaming one silently would leave
        // every container serving a kind nothing draws.
        assert_eq!(LiveItemKind::Text.wire(), "text");
        assert_eq!(LiveItemKind::Image.wire(), "image");
        assert_eq!(LiveItemKind::Progress.wire(), "progress");
        assert_eq!(LiveItemKind::Secret.wire(), "secret");
    }

    #[test]
    fn a_fraction_that_is_not_a_number_says_so_instead_of_nulling() {
        // `Number::from_f64` refuses NaN and infinity, so the item
        // would otherwise carry `null` and be dropped by the renderer.
        let item = LiveItem::progress("Restore", f64::NAN);
        assert_eq!(item.label, "Restore (unreadable)");
        assert!(item.data.as_str().unwrap().contains("NaN"));
        assert!(item.data_matches_kind(), "the line it became is drawable");
        assert!(LiveItem::progress("Restore", 0.25).data_matches_kind());
    }

    #[test]
    fn an_unreadable_item_keeps_its_button() {
        // The value could not be drawn; the press still works, and it
        // is the node's, not ours to drop.
        let feed = LiveFeed::from_answer(&serde_json::json!({
            "items": [{
                "type": "progress", "label": "Restore", "data": "nearly there",
                "action": { "label": "Cancel restore", "actionKind": "cancel" }
            }]
        }))
        .expect("the envelope is right");
        assert_eq!(feed.items[0].label, "Restore (unreadable)");
        assert_eq!(
            feed.items[0].action,
            Some(LiveAction::new("Cancel restore", "cancel"))
        );
    }

    #[test]
    fn a_feed_reads_back_what_a_container_sent() {
        let sent = serde_json::json!({
            "items": [{ "type": "secret", "label": "API key", "data": "wft-abc" }]
        });
        let feed: LiveFeed = serde_json::from_value(sent).unwrap();
        assert_eq!(feed.items, vec![LiveItem::secret("API key", "wft-abc")]);
    }

    #[test]
    fn an_answer_with_no_items_reads_as_an_empty_feed() {
        let feed: LiveFeed = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(feed.is_empty());
    }

    #[test]
    fn an_answer_that_is_not_the_envelope_is_an_error() {
        let err = LiveFeed::from_answer(&serde_json::json!({ "status": "ok" }))
            .expect_err("not a display");
        assert!(err.contains("items"), "{err}");
        assert!(LiveFeed::from_answer(&serde_json::json!({ "items": 3 })).is_err());
    }

    #[test]
    fn an_item_whose_data_does_not_fit_its_kind_becomes_a_visible_line() {
        // Every serde check passes on this: `data` is a `Value`. Only
        // the kind knows a progress bar cannot draw a sentence, and a
        // reader that just dropped the line would leave a display
        // quietly short of what the container meant to show.
        let feed = LiveFeed::from_answer(&serde_json::json!({
            "items": [
                { "type": "progress", "label": "Restore", "data": "nearly there" },
                { "type": "text", "label": "Phone", "data": true },
                { "type": "text", "label": "Status", "data": "Connected" }
            ]
        }))
        .expect("the envelope is right");
        assert_eq!(feed.items.len(), 3);
        assert_eq!(feed.items[0].label, "Restore (unreadable)");
        assert_eq!(feed.items[1].label, "Phone (unreadable)");
        assert!(feed.items[0].data.as_str().unwrap().contains("nearly there"));
        assert_eq!(feed.items[2], LiveItem::text("Status", "Connected"));
        // And every line that comes out is one a reader can draw.
        assert!(feed.items.iter().all(|i| i.data_matches_kind()));
    }

    #[test]
    fn one_unreadable_item_does_not_take_the_others_down() {
        let feed = LiveFeed::from_answer(&serde_json::json!({
            "items": [
                { "type": "text", "label": "Status", "data": "Connected" },
                { "type": "hologram", "label": "New", "data": "x" },
                { "type": "text", "label": "Phone", "data": "+33" }
            ]
        }))
        .expect("the envelope is right");
        assert_eq!(feed.items.len(), 3);
        assert_eq!(feed.items[0].label, "Status");
        assert_eq!(feed.items[1].label, "Unreadable item");
        assert!(feed.items[1].data.as_str().unwrap().contains("hologram"));
        assert_eq!(feed.items[2].label, "Phone");
    }
}
