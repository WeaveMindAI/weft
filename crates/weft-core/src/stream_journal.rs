//! What a channel's messages look like once they are written down.
//!
//! Several things in the language carry a stream of messages between
//! parts of a program: a bus between nodes, a live caller conversation
//! between an outside client and a run, whatever comes next. They have
//! different transports and different lifecycles, and none of that
//! belongs here. What they must NOT differ on is how much of what they
//! carry ends up in the journal, because that is the author's answer to
//! "what did this program write down about my users", and an answer
//! that depends on which channel happened to be used is not an answer.
//!
//! So one policy, one trim rule, one set of defaults, applied by every
//! channel through [`record`].
//!
//! Two things this is NOT. It is not a limit on what a channel may
//! carry: a socket that can move ten megabytes still moves ten
//! megabytes, and what the transport supports is the transport's own
//! business (`max_inbound_bytes` and friends). And it is not a warning:
//! a big payload is a legitimate thing to send, so it is simply
//! recorded trimmed rather than complained about.

use std::time::Duration;

use serde_json::Value;

use crate::bus::WirePayload;

/// Content bigger than this is recorded trimmed, never whole.
///
/// A journal row is something a person reads in the inspector and
/// something the database stores for every run forever, so there is a
/// size past which keeping the rest buys nothing: nobody reads the
/// four-hundredth kilobyte of a message, and the row still has to say
/// truthfully how big the thing was, which it does through the byte
/// size that travels beside the content.
///
/// The number is the same as [`crate::storage::MAX_WIRE_VALUE_BYTES`]
/// and the two are NOT the same rule. That one refuses: a value over it
/// may not travel a wire at all, and the message points at stored files.
/// This one only trims what gets recorded, and refuses nothing. Kept as
/// its own constant so that changing what a wire may carry does not
/// silently change what the journal keeps.
pub const JOURNAL_TRIM_BYTES: usize = 100 * 1024;

/// The most one journal ROW may carry, across every message in its
/// window.
///
/// [`JOURNAL_TRIM_BYTES`] bounds one message; this bounds the row they
/// share, and without it the two multiply. A window is a slice of TIME,
/// so a busy second can put any number of messages in one row: seventy
/// hundred-kilobyte messages in a second is a seven-megabyte row, past
/// what the journal accepts. The write is refused, the window is kept
/// to retry, the retry is refused again, and the channel is wedged for
/// good over one second of traffic.
///
/// So a window closes on whichever comes first, its time or this size.
/// Nothing is lost when it closes early: the messages go in the next
/// row, offsets still run in order, and a reader sees the same stream
/// in more rows.
///
/// Well under the journal's own limit on a record, because a row also
/// carries its rollup, its offsets and its framing, and the point of a
/// bound is to be nowhere near the cliff.
pub const JOURNAL_ROW_BYTES: usize = 2 * 1024 * 1024;

/// How long a trimmed text field keeps, so a trimmed row still shows
/// the start of what was there rather than only its size.
const TRIM_FLOOR_BYTES: usize = 64;

/// The aggregation window every channel starts from: messages inside
/// one window become one journal row instead of a row each.
///
/// One second keeps a conversation-shaped channel reading per-message
/// while collapsing a frame-rate stream about fifty times.
pub const DEFAULT_JOURNAL_WINDOW: Duration = Duration::from_secs(1);

/// How one channel's messages reach the journal. Every channel carries
/// one of these and hands it to [`record`], so two channels cannot
/// drift on what "ephemeral" means or on where the trim falls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalPolicy {
    /// Messages appended inside one window become one journal row.
    pub window: Duration,
    /// Keep metadata only: how many messages, how many bytes, and when,
    /// never the content. The content then lives only in the channel's
    /// own in-memory window, so it is gone once that window rolls past
    /// and there is no database copy to fall back on.
    pub ephemeral: bool,
    /// Where content gets cut when it is kept at all.
    pub trim_bytes: usize,
    /// The most one journal ROW may carry, across all the messages in
    /// its window. See [`JOURNAL_ROW_BYTES`].
    pub row_bytes: usize,
}

impl Default for JournalPolicy {
    fn default() -> Self {
        Self {
            window: DEFAULT_JOURNAL_WINDOW,
            ephemeral: false,
            trim_bytes: JOURNAL_TRIM_BYTES,
            row_bytes: JOURNAL_ROW_BYTES,
        }
    }
}

impl JournalPolicy {
    /// The most a row will carry for one message of this true size:
    /// the whole thing, or the trim, whichever is smaller.
    pub fn kept_at_most(&self, byte_size: u64) -> usize {
        (byte_size as usize).min(self.trim_bytes)
    }

    /// Would adding a message of `next` bytes push a row already
    /// holding `held` past what one row may carry?
    ///
    /// Asked BEFORE the message joins the window, so the answer is
    /// "close this row and start the next one with it". A row that is
    /// still empty always takes the message, however big: one message
    /// alone is never split across rows, and the per-message trim has
    /// already bounded it.
    pub fn row_is_full(&self, held: usize, next: usize) -> bool {
        held > 0 && held.saturating_add(next) > self.row_bytes
    }
}

/// What one message contributes to a journal row.
#[derive(Debug, Clone, PartialEq)]
pub struct Recorded {
    /// The content to write down, or `None` when only metadata is kept
    /// (an ephemeral channel, or any payload of raw bytes).
    pub payload: Option<WirePayload>,
    /// The message's TRUE size, whatever was kept of it. A reader
    /// comparing this against the content it got is how the inspector
    /// knows to say "412 KB, showing the first 100".
    pub byte_size: u64,
    /// Content was cut to fit. False for an untouched payload AND for
    /// one that was dropped entirely, which `payload` already says.
    pub trimmed: bool,
}

impl Recorded {
    /// What this message adds to the weight of the row it joins: the
    /// content actually kept, or nothing when none was.
    pub fn kept_bytes(&self) -> usize {
        self.payload.as_ref().map(|p| p.byte_size() as usize).unwrap_or(0)
    }
}

/// Decide what the journal keeps of one message.
///
/// Raw bytes are always metadata only, whatever the policy says. Base64
/// in a journal row is a third bigger than the thing it describes and
/// unreadable to the person looking at it, so the size is the whole of
/// what is worth keeping. This is a rule rather than a default, because
/// a channel that could opt into recording media would fill a database
/// with it by accident.
pub fn record(payload: &WirePayload, policy: &JournalPolicy) -> Recorded {
    let byte_size = payload.byte_size();
    let json = match payload {
        WirePayload::Bytes(_) => {
            return Recorded { payload: None, byte_size, trimmed: false };
        }
        WirePayload::Json(v) => v,
    };
    if policy.ephemeral {
        return Recorded { payload: None, byte_size, trimmed: false };
    }
    if byte_size as usize <= policy.trim_bytes {
        return Recorded {
            payload: Some(payload.clone()),
            byte_size,
            trimmed: false,
        };
    }
    Recorded {
        payload: Some(WirePayload::Json(trim_json(json, policy.trim_bytes))),
        byte_size,
        trimmed: true,
    }
}

/// Cut a JSON value down to `limit`, keeping its SHAPE.
///
/// The keys stay, the structure stays, and the long text fields are the
/// ones that give way, because that is what a reader needs: a row
/// reading `{"user": "ada", "photo": "iVBORw0…[trimmed, 312144 bytes]"}`
/// still tells you who it was about, where a row replaced wholesale by
/// its own size tells you nothing.
///
/// Every text field is cut to the same length, chosen as large as still
/// fits, so which field gives way does not depend on the order the keys
/// happen to be in.
///
/// When the size is in the STRUCTURE rather than in any one field (a
/// hundred thousand tiny keys), no per-field cut can fit and the shape
/// is given up on: the value is written out as text and chopped at the
/// limit, ending mid-way through whatever it was in. The result is not
/// parseable JSON any more, and that is fine. A reader looking at a
/// journal row wants the first however-many-thousand characters of what
/// went past far more than it wants a well-formed husk, and far more
/// than it wants nothing at all.
fn trim_json(value: &Value, limit: usize) -> Value {
    // The largest per-field length that fits, by bisection. The
    // measurement is a serialization, so this is a handful of passes
    // over an already-oversized value, on the journaling path only.
    let mut low = TRIM_FLOOR_BYTES;
    let mut high = limit;
    let mut best: Option<Value> = None;
    while low <= high {
        let mid = low + (high - low) / 2;
        let cut = cut_strings(value, mid);
        if json_bytes(&cut) <= limit {
            best = Some(cut);
            low = mid + 1;
        } else {
            if mid == TRIM_FLOOR_BYTES {
                break;
            }
            high = mid - 1;
        }
    }
    best.unwrap_or_else(|| cut_whole(value, limit))
}

/// The last resort: the value as text, chopped at `limit`, carrying what
/// it really weighed. Used when no per-field cut fits, so the shape
/// cannot be kept whatever is done to the fields.
fn cut_whole(value: &Value, limit: usize) -> Value {
    let text = serde_json::to_string(value).expect("a serde_json::Value always serializes");
    let mut keep = limit.min(text.len());
    // Never split a character in half: a journal row has to survive
    // being read back as text.
    while keep > 0 && !text.is_char_boundary(keep) {
        keep -= 1;
    }
    Value::String(format!("{}… [trimmed, {} bytes]", &text[..keep], text.len()))
}

/// Every string longer than `cap` becomes its own first `cap` bytes
/// plus a note of what it really was. Recurses through objects and
/// arrays; every other kind of value is already small.
fn cut_strings(value: &Value, cap: usize) -> Value {
    match value {
        Value::String(s) if s.len() > cap => {
            let mut keep = cap;
            // Never split a character in half: a journal row has to
            // survive being read back as text.
            while keep > 0 && !s.is_char_boundary(keep) {
                keep -= 1;
            }
            Value::String(format!("{}… [trimmed, {} bytes]", &s[..keep], s.len()))
        }
        Value::Array(items) => {
            Value::Array(items.iter().map(|v| cut_strings(v, cap)).collect())
        }
        Value::Object(fields) => Value::Object(
            fields.iter().map(|(k, v)| (k.clone(), cut_strings(v, cap))).collect(),
        ),
        other => other.clone(),
    }
}

fn json_bytes(value: &Value) -> usize {
    serde_json::to_vec(value).expect("a serde_json::Value always serializes").len()
}

/// Which way one message of a caller conversation went.
///
/// A bus names its sender, because a bus has any number of them. A
/// conversation has exactly two parties and they never change, so the
/// direction is the whole of who said it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallerDirection {
    /// The outside caller sent it.
    Inbound,
    /// The program sent it.
    Outbound,
}

/// One message of a conversation, as the journal keeps it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WindowedCallerMessage {
    pub offset: u64,
    pub direction: CallerDirection,
    /// What was said, when the journal keeps it at all. Absent only when
    /// nothing was kept on purpose: an ephemeral conversation, or a
    /// payload of raw bytes. A message too big to write down whole is
    /// still here, cut down, and `trimmed` says so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<WirePayload>,
    /// The message's TRUE size, whatever was kept of it.
    pub payload_byte_size: u64,
    /// The payload here is a cut-down copy of a message too big to
    /// write down whole. Always alongside a payload: something is
    /// always kept, down to the first few thousand characters of the
    /// value written out as text.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub trimmed: bool,
    /// The last thing the program sends on this exchange.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub terminal: bool,
    pub at_unix: u64,
}

/// What one window carries per direction: the whole story an ephemeral
/// conversation journals, and the summary a journaled one carries
/// beside its messages.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CallerWindowTotal {
    pub direction: CallerDirection,
    pub count: u64,
    pub bytes: u64,
}

/// A window's aggregate: what one journal row carries for one
/// conversation.
#[derive(Debug, Clone, PartialEq)]
pub struct CallerWindowAggregate {
    pub first_offset: u64,
    pub last_offset: u64,
    /// Per-message detail. A message whose content was not kept still
    /// appears here, carrying its size, so the row says a message
    /// happened even when it does not say what it was.
    pub messages: Vec<WindowedCallerMessage>,
    pub totals: Vec<CallerWindowTotal>,
    /// The LAST message's time: the row describes when the conversation
    /// said things, not when the pump got around to writing them down.
    pub last_at_unix: u64,
}

/// Fold one window's messages into what a journal row carries. Pure;
/// whoever owns the clock decides when to call it. `None` on an empty
/// window, meaning there is nothing to write.
pub fn aggregate_caller_window(
    messages: Vec<WindowedCallerMessage>,
) -> Option<CallerWindowAggregate> {
    let first = messages.first()?;
    let first_offset = first.offset;
    let last = messages.last().expect("a non-empty window has a last message");
    let last_offset = last.offset;
    let last_at_unix = last.at_unix;
    let mut totals: Vec<CallerWindowTotal> = Vec::new();
    for m in &messages {
        match totals.iter_mut().find(|t| t.direction == m.direction) {
            Some(t) => {
                t.count += 1;
                t.bytes += m.payload_byte_size;
            }
            None => totals.push(CallerWindowTotal {
                direction: m.direction,
                count: 1,
                bytes: m.payload_byte_size,
            }),
        }
    }
    Some(CallerWindowAggregate {
        first_offset,
        last_offset,
        messages,
        totals,
        last_at_unix,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn json(v: Value) -> WirePayload {
        WirePayload::Json(v)
    }

    /// A message inside the limit is written exactly as it was sent.
    #[test]
    fn a_small_message_is_kept_whole() {
        let p = json(json!({"user": "ada", "text": "hello"}));
        let got = record(&p, &JournalPolicy::default());
        assert_eq!(got.payload, Some(p.clone()));
        assert!(!got.trimmed);
        assert_eq!(got.byte_size, p.byte_size());
    }

    /// The shape survives the cut and the small fields beside the big
    /// one come through untouched, which is the whole point of cutting
    /// fields rather than replacing the value.
    #[test]
    fn an_oversized_message_keeps_its_shape_and_its_small_fields() {
        let big = "x".repeat(400 * 1024);
        let p = json(json!({"user": "ada", "photo": big, "n": 7}));
        let got = record(&p, &JournalPolicy::default());
        assert!(got.trimmed);
        let kept = got.payload.expect("the shape survives");
        let v = kept.as_json().unwrap();
        assert_eq!(v["user"], json!("ada"), "a small field is untouched");
        assert_eq!(v["n"], json!(7), "a value that is not text is untouched");
        let photo = v["photo"].as_str().unwrap();
        assert!(photo.starts_with("xxx"), "the start of the field is kept: {photo:.40}");
        assert!(photo.contains("trimmed, 409600 bytes"), "it says what it really was: {photo:.80}");
        assert!(kept.byte_size() as usize <= JOURNAL_TRIM_BYTES, "and it fits");
        // The row still reports the TRUE size, not the trimmed one.
        assert_eq!(got.byte_size, p.byte_size());
    }

    /// Two big fields give way together rather than the first one
    /// taking the whole cut, so what survives does not depend on key
    /// order.
    #[test]
    fn several_big_fields_are_cut_to_the_same_length() {
        let p = json(json!({
            "a": "a".repeat(300 * 1024),
            "b": "b".repeat(300 * 1024),
        }));
        let got = record(&p, &JournalPolicy::default());
        let kept = got.payload.expect("the shape survives");
        let v = kept.as_json().unwrap();
        let a = v["a"].as_str().unwrap();
        let b = v["b"].as_str().unwrap();
        let a_kept = a.find('…').expect("a marker");
        let b_kept = b.find('…').expect("a marker");
        assert_eq!(a_kept, b_kept, "both cut to the same length");
        assert!(kept.byte_size() as usize <= JOURNAL_TRIM_BYTES);
    }

    /// Cutting reaches into nested structures, because that is where a
    /// real payload keeps its big field.
    #[test]
    fn cutting_reaches_a_nested_field() {
        let p = json(json!({"body": {"items": [{"blob": "z".repeat(400 * 1024)}]}}));
        let got = record(&p, &JournalPolicy::default());
        let kept = got.payload.expect("the shape survives");
        let v = kept.as_json().unwrap();
        let blob = v["body"]["items"][0]["blob"].as_str().unwrap();
        assert!(blob.contains("trimmed"), "{blob:.60}");
        assert!(kept.byte_size() as usize <= JOURNAL_TRIM_BYTES);
    }

    /// An ephemeral channel keeps the size and nothing else, and says
    /// so through an absent payload rather than an empty one.
    #[test]
    fn an_ephemeral_channel_keeps_only_the_size() {
        let p = json(json!({"user": "ada", "text": "hello"}));
        let policy = JournalPolicy { ephemeral: true, ..Default::default() };
        let got = record(&p, &policy);
        assert_eq!(got.payload, None);
        assert_eq!(got.byte_size, p.byte_size());
        assert!(!got.trimmed, "dropped is not trimmed; the payload says which");
    }

    /// Raw bytes are metadata only whatever the policy says, so a
    /// channel carrying media cannot fill the journal with base64 by
    /// setting one flag.
    #[test]
    fn raw_bytes_are_never_written_down() {
        let p = WirePayload::Bytes(bytes::Bytes::from(vec![7u8; 2048]));
        for ephemeral in [false, true] {
            let policy = JournalPolicy { ephemeral, ..Default::default() };
            let got = record(&p, &policy);
            assert_eq!(got.payload, None, "ephemeral={ephemeral}");
            assert_eq!(got.byte_size, 2048);
        }
    }

    /// A value whose size is in its structure rather than in any field
    /// has nothing worth keeping, and says so with the true size.
    #[test]
    fn a_value_made_of_countless_tiny_fields_is_chopped_as_text() {
        let fields: serde_json::Map<String, Value> =
            (0..40_000).map(|i| (format!("k{i}"), json!(i))).collect();
        let p = json(Value::Object(fields));
        let got = record(&p, &JournalPolicy::default());
        assert!(got.trimmed, "it was cut down");
        assert!(got.byte_size as usize > JOURNAL_TRIM_BYTES);
        let Some(WirePayload::Json(Value::String(text))) = got.payload else {
            panic!("no per-field cut fits a value this shape, so it is kept as text");
        };
        assert!(
            text.starts_with("{\"k0\":0,"),
            "and the text is the start of the real value, not a summary: {}",
            &text[..40.min(text.len())]
        );
        assert!(text.contains("[trimmed,"), "saying what it really weighed");
    }

    /// The per-message trim bounds one message; the row bound is what
    /// stops a busy window from multiplying it into a row the journal
    /// will refuse.
    #[test]
    fn a_row_fills_up_and_the_next_message_starts_another() {
        let p = JournalPolicy::default();
        let big = p.trim_bytes;
        let fits = p.row_bytes / big;
        assert!(fits > 1, "a row holds several trimmed messages");
        assert!(!p.row_is_full(big * (fits - 1), big), "there is still room for one more");
        assert!(p.row_is_full(p.row_bytes, big), "and none once the row is at its bound");
    }

    /// One message is never split across rows: the per-message trim has
    /// already bounded it, and half a message in each of two rows would
    /// be unreadable in both.
    #[test]
    fn an_empty_row_takes_a_message_of_any_size() {
        let p = JournalPolicy::default();
        assert!(!p.row_is_full(0, p.row_bytes * 100));
    }

    /// What a message can contribute to a row is what survives the
    /// trim, not what the caller sent.
    #[test]
    fn a_huge_message_counts_as_its_trimmed_size() {
        let p = JournalPolicy::default();
        assert_eq!(p.kept_at_most(50_000_000), p.trim_bytes);
        assert_eq!(p.kept_at_most(12), 12);
    }

    fn msg(offset: u64, direction: CallerDirection, bytes: u64) -> WindowedCallerMessage {
        WindowedCallerMessage {
            offset,
            direction,
            payload: Some(json(json!("x"))),
            payload_byte_size: bytes,
            trimmed: false,
            terminal: false,
            at_unix: 100 + offset,
        }
    }

    /// A window totals each direction on its own, so a conversation
    /// that talked a lot one way reads as what it was.
    #[test]
    fn a_window_totals_each_direction_separately() {
        use CallerDirection::*;
        let got = aggregate_caller_window(vec![
            msg(1, Inbound, 10),
            msg(2, Outbound, 40),
            msg(3, Outbound, 2),
        ])
        .expect("a window with messages");
        assert_eq!(got.first_offset, 1);
        assert_eq!(got.last_offset, 3);
        assert_eq!(got.last_at_unix, 103, "the row is stamped by the last message");
        let inbound = got.totals.iter().find(|t| t.direction == Inbound).unwrap();
        assert_eq!((inbound.count, inbound.bytes), (1, 10));
        let outbound = got.totals.iter().find(|t| t.direction == Outbound).unwrap();
        assert_eq!((outbound.count, outbound.bytes), (2, 42));
    }

    /// A message whose content was not kept still appears, so the row
    /// says a message happened even when it does not say what it was.
    #[test]
    fn a_message_with_no_content_still_counts() {
        let mut m = msg(1, CallerDirection::Inbound, 900);
        m.payload = None;
        let got = aggregate_caller_window(vec![m]).expect("a window with messages");
        assert_eq!(got.messages.len(), 1);
        assert_eq!(got.totals[0].bytes, 900, "the size is known even with nothing kept");
    }

    /// An empty window is nothing to write, not an empty row.
    #[test]
    fn an_empty_window_writes_nothing() {
        assert_eq!(aggregate_caller_window(Vec::new()), None);
    }

    /// Cutting a field of multi-byte characters leaves text that can
    /// still be read back.
    #[test]
    fn a_cut_never_splits_a_character() {
        let p = json(json!({"text": "é".repeat(200 * 1024)}));
        let got = record(&p, &JournalPolicy::default());
        let kept = got.payload.expect("the shape survives");
        let text = kept.as_json().unwrap()["text"].as_str().unwrap();
        assert!(text.starts_with('é'), "readable from the first character");
        assert!(text.contains("trimmed"));
    }
}
