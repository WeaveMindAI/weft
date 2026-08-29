//! Outbound event source (raw pipe): a persistent TCP (usually TLS)
//! connection the listener dials OUT and holds, for services that
//! speak a wire protocol OTHER than HTTP or WebSocket (IMAP, MQTT,
//! Redis, XMPP, ...). The listener never learns the protocol: the
//! spec declares the literal frames to send (a login dialogue, a
//! heartbeat), how the byte stream cuts into units ([`Framing`]),
//! and a pattern that fires the trigger. Everything protocol-shaped
//! beyond "something happened" (fetching the mail, decoding the
//! packet) is the fired node's concern, where code is unrestricted.
//!
//! The division of labor mirrors [`super::SocketListen`]: the
//! language owns "hold the pipe, run the declared dialogue, cut the
//! stream, fire on a match, reconnect on drop"; the SERVICE protocol
//! is carried as literal frames and patterns in the spec. Text frames
//! interpolate `{placeholders}` from the connection's resolved values
//! on every connect cycle, so credentials ride the dialogue without
//! ever sitting in the spec.

use serde::{Deserialize, Serialize};

use super::socket_listen::SocketFrame;
use super::Signal;
use crate::primitive::AccessRef;
use crate::signal::Predicate;

/// Default heartbeat cadence, matching the WebSocket kind's floor.
pub use super::socket_listen::DEFAULT_HEARTBEAT_SECS;

fn default_heartbeat_secs() -> u64 {
    DEFAULT_HEARTBEAT_SECS
}

fn default_tls() -> bool {
    true
}

fn is_true(b: &bool) -> bool {
    *b
}

/// How a raw byte stream cuts into units. Every wire protocol frames
/// one of exactly three ways, so this is a closed set: a separator
/// (text protocols), a fixed-size length field (most binary
/// protocols), or a variable-size length field (MQTT). A unit is the
/// WHOLE frame as received (header included), except the delimiter
/// case, where the separator itself is stripped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Framing {
    /// A unit ends at this byte sequence (IMAP/SMTP/Redis: `"\r\n"`).
    /// The separator is stripped from the unit.
    Delimiter { bytes: String },
    /// The frame carries its payload length in a fixed-size integer
    /// field: `offset` header bytes, then a `size`-byte length
    /// (big-endian unless `little_endian`), then the counted bytes,
    /// then `trailer` fixed bytes (AMQP's frame-end octet).
    LengthPrefix {
        offset: usize,
        size: usize,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        little_endian: bool,
        /// What the length value counts, see [`LengthCounts`].
        #[serde(default, skip_serializing_if = "is_default_counts")]
        counts: LengthCounts,
        #[serde(default, skip_serializing_if = "is_zero")]
        trailer: usize,
    },
    /// The frame carries its payload length as a base-128 varint
    /// (7 data bits per byte, high bit = continue) after `offset`
    /// header bytes (MQTT: `offset: 1`).
    VarintPrefix { offset: usize },
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

fn is_default_counts(c: &LengthCounts) -> bool {
    *c == LengthCounts::Payload
}

/// What a length-prefix frame's length value counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LengthCounts {
    /// The bytes after the length field (the common shape).
    #[default]
    Payload,
    /// The length field itself plus the payload (Postgres's wire).
    FromLengthField,
}

/// A frame-split cap: a peer claiming a unit larger than this is
/// misbehaving (or the framing declaration is wrong), and buffering
/// it would grow without bound. The split fails loudly instead.
pub const MAX_UNIT_BYTES: usize = 8 * 1024 * 1024;

impl Framing {
    /// Split the FIRST complete unit off `buf`: `Ok(Some((unit,
    /// consumed)))` hands back the unit's bytes and how many buffer
    /// bytes it used (delimiter included), `Ok(None)` means the unit
    /// is still arriving, `Err` means the stream cannot be framed as
    /// declared (oversized claim, malformed varint) and the
    /// connection must be dropped.
    pub fn split(&self, buf: &[u8]) -> Result<Option<(Vec<u8>, usize)>, String> {
        match self {
            Framing::Delimiter { bytes } => {
                let sep = bytes.as_bytes();
                match buf.windows(sep.len()).position(|w| w == sep) {
                    Some(at) if at > MAX_UNIT_BYTES => Err(oversized(at)),
                    Some(at) => Ok(Some((buf[..at].to_vec(), at + sep.len()))),
                    None if buf.len() > MAX_UNIT_BYTES => Err(oversized(buf.len())),
                    None => Ok(None),
                }
            }
            Framing::LengthPrefix { offset, size, little_endian, counts, trailer } => {
                let header = offset + size;
                if buf.len() < header {
                    return Ok(None);
                }
                let field = &buf[*offset..header];
                let mut value: u64 = 0;
                if *little_endian {
                    for &b in field.iter().rev() {
                        value = (value << 8) | b as u64;
                    }
                } else {
                    for &b in field {
                        value = (value << 8) | b as u64;
                    }
                }
                let after_field = match counts {
                    LengthCounts::Payload => value as usize,
                    LengthCounts::FromLengthField => (value as usize)
                        .checked_sub(*size)
                        .ok_or_else(|| {
                            format!(
                                "the frame's length field claims {value} bytes, less than \
                                 the {size}-byte field it counts from; the framing \
                                 declaration does not match this stream"
                            )
                        })?,
                };
                let total = header + after_field + trailer;
                if total > MAX_UNIT_BYTES {
                    return Err(oversized(total));
                }
                if buf.len() < total {
                    return Ok(None);
                }
                Ok(Some((buf[..total].to_vec(), total)))
            }
            Framing::VarintPrefix { offset } => {
                if buf.len() < *offset {
                    return Ok(None);
                }
                let mut value: u64 = 0;
                let mut shift = 0u32;
                for (i, &b) in buf[*offset..].iter().enumerate() {
                    value |= ((b & 0x7f) as u64) << shift;
                    shift += 7;
                    if b & 0x80 == 0 {
                        let total = offset + i + 1 + value as usize;
                        if total > MAX_UNIT_BYTES {
                            return Err(oversized(total));
                        }
                        if buf.len() < total {
                            return Ok(None);
                        }
                        return Ok(Some((buf[..total].to_vec(), total)));
                    }
                    if shift > 56 {
                        return Err(
                            "the varint length never terminates; the framing declaration \
                             does not match this stream"
                                .into(),
                        );
                    }
                }
                Ok(None)
            }
        }
    }
}

fn oversized(n: usize) -> String {
    format!(
        "a frame claims {n} bytes, over the {MAX_UNIT_BYTES}-byte cap; either the peer is \
         misbehaving or the framing declaration does not match this stream"
    )
}

/// One step of the connect dialogue: send a frame, then hold until a
/// unit matches `until` before the next step runs. The fire pattern
/// stays quiet until the whole dialogue completed, so protocol
/// chatter during sign-in never fires the trigger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptStep {
    pub send: SocketFrame,
    /// A byte-level regex; the step completes on the first matching
    /// unit.
    pub until: String,
}

/// A declarative acknowledgement: whenever a unit matches `when`,
/// send `frame` back. Active from the moment the pipe opens (some
/// protocols demand acks mid-dialogue).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamReply {
    /// A byte-level regex over the unit.
    pub when: String,
    pub frame: SocketFrame,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamListen {
    /// `host:port`, usually with `{placeholders}` resolved from the
    /// connection ("{imap_host}:{imap_port}").
    pub address: String,

    /// Wrap the pipe in TLS (the default; almost nothing worth
    /// listening to speaks plaintext across the internet).
    #[serde(default = "default_tls", skip_serializing_if = "is_true")]
    pub tls: bool,

    pub framing: Framing,

    /// The connect dialogue, run in order on every (re)connect.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub script: Vec<ScriptStep>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replies: Vec<StreamReply>,

    /// Resent every `heartbeat_secs` once the dialogue completed
    /// (IMAP's IDLE re-issue, MQTT's PINGREQ). `None` = the protocol
    /// keeps itself alive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<SocketFrame>,

    #[serde(default = "default_heartbeat_secs")]
    pub heartbeat_secs: u64,

    /// The byte-level regex that fires the trigger: every unit
    /// matching it (after the dialogue completed) becomes one fire,
    /// with the unit as payload (UTF-8 text as a string, anything
    /// else base64).
    pub fire: String,

    /// The connection whose values interpolate the address and
    /// frames. Lifted onto the spec by [`super::to_spec`].
    #[serde(skip)]
    pub access: Option<AccessRef>,
    /// Pre-fire filter, lifted onto the spec.
    #[serde(skip)]
    pub filters: Vec<Predicate>,
}

impl StreamListen {
    /// A dialed pipe with the fire pattern; everything else is added
    /// by the builder-style setters or field assignment.
    pub fn new(address: impl Into<String>, framing: Framing, fire: impl Into<String>) -> Self {
        StreamListen {
            address: address.into(),
            tls: true,
            framing,
            script: Vec::new(),
            replies: Vec::new(),
            heartbeat: None,
            heartbeat_secs: DEFAULT_HEARTBEAT_SECS,
            fire: fire.into(),
            access: None,
            filters: Vec::new(),
        }
    }

    /// Attach the connection whose values the address and frames
    /// interpolate.
    pub fn with_access(mut self, access: &crate::Access) -> Self {
        self.access = Some(AccessRef::from(access));
        self
    }

    /// Append one dialogue step.
    pub fn step(mut self, send: SocketFrame, until: impl Into<String>) -> Self {
        self.script.push(ScriptStep { send, until: until.into() });
        self
    }

    /// Set the keep-alive frame and cadence.
    pub fn with_heartbeat(mut self, frame: SocketFrame, secs: u64) -> Self {
        self.heartbeat = Some(frame);
        self.heartbeat_secs = secs;
        self
    }
}

/// A byte-level regex that must compile, refused at registration
/// where someone is watching (per event it would be a silent
/// never-fires or never-completes).
fn compiled(pattern: &str, what: &str) -> Result<(), String> {
    regex::bytes::Regex::new(pattern)
        .map(|_| ())
        .map_err(|e| format!("stream_listen {what} pattern does not compile: {e}"))
}

impl Signal for StreamListen {
    const TAG: &'static str = "stream_listen";

    fn validate(&self) -> Result<(), String> {
        if self.address.trim().is_empty() {
            return Err("stream_listen needs an address (host:port)".into());
        }
        match &self.framing {
            Framing::Delimiter { bytes } if bytes.is_empty() => {
                return Err("stream_listen delimiter framing needs a non-empty separator".into())
            }
            Framing::LengthPrefix { size, .. } if *size == 0 || *size > 8 => {
                return Err(
                    "stream_listen length-prefix framing needs a length field of 1..=8 bytes"
                        .into(),
                )
            }
            _ => {}
        }
        compiled(&self.fire, "fire")?;
        for (i, step) in self.script.iter().enumerate() {
            compiled(&step.until, &format!("script[{i}].until"))?;
        }
        for (i, reply) in self.replies.iter().enumerate() {
            compiled(&reply.when, &format!("replies[{i}].when"))?;
        }
        if self.heartbeat.is_some() && self.heartbeat_secs == 0 {
            return Err(
                "stream_listen.heartbeat_secs must be > 0 when a heartbeat frame is set: \
                 a zero interval would spin the heartbeat loop"
                    .into(),
            );
        }
        Ok(())
    }

    fn access(&self) -> Option<AccessRef> {
        self.access.clone()
    }

    fn match_predicates(&self) -> &[Predicate] {
        &self.filters
    }
}

crate::register_signal_kind!(StreamListen);

#[cfg(test)]
mod tests {
    use super::*;

    fn crlf() -> Framing {
        Framing::Delimiter { bytes: "\r\n".into() }
    }

    #[test]
    fn delimiter_framing_cuts_on_the_separator() {
        let f = crlf();
        // A partial line is still arriving.
        assert_eq!(f.split(b"* OK ready").unwrap(), None);
        // A complete line hands back the unit without the separator,
        // consuming it.
        let (unit, used) = f.split(b"* OK ready\r\nnext").unwrap().unwrap();
        assert_eq!(unit, b"* OK ready");
        assert_eq!(used, 12);
        // An empty line is a valid (empty) unit.
        assert_eq!(f.split(b"\r\nrest").unwrap().unwrap(), (Vec::new(), 2));
    }

    #[test]
    fn length_prefix_framing_reads_the_declared_field() {
        // 1 header byte, 2-byte big-endian length counting the payload.
        let f = Framing::LengthPrefix {
            offset: 1,
            size: 2,
            little_endian: false,
            counts: LengthCounts::Payload,
            trailer: 0,
        };
        assert_eq!(f.split(&[0x0a, 0x00]).unwrap(), None, "header incomplete");
        assert_eq!(f.split(&[0x0a, 0x00, 0x03, 1, 2]).unwrap(), None, "payload incomplete");
        let (unit, used) = f.split(&[0x0a, 0x00, 0x03, 1, 2, 3, 9]).unwrap().unwrap();
        assert_eq!(unit, vec![0x0a, 0x00, 0x03, 1, 2, 3], "the whole frame is the unit");
        assert_eq!(used, 6);

        // A trailer rides inside the unit (AMQP's frame-end octet).
        let with_trailer = Framing::LengthPrefix {
            offset: 0,
            size: 1,
            little_endian: false,
            counts: LengthCounts::Payload,
            trailer: 1,
        };
        let (unit, used) = with_trailer.split(&[2, 7, 8, 0xce, 99]).unwrap().unwrap();
        assert_eq!(unit, vec![2, 7, 8, 0xce]);
        assert_eq!(used, 4);
    }

    /// Postgres-shaped: the length field counts itself plus the
    /// payload, so the payload is value minus the field's size.
    #[test]
    fn from_length_field_counting_subtracts_the_field() {
        let f = Framing::LengthPrefix {
            offset: 1,
            size: 4,
            little_endian: false,
            counts: LengthCounts::FromLengthField,
            trailer: 0,
        };
        // 'R' + len 8 (4 field + 4 payload) + 4 payload bytes.
        let frame = [b'R', 0, 0, 0, 8, 0, 0, 0, 0];
        let (unit, used) = f.split(&frame).unwrap().unwrap();
        assert_eq!(unit, frame.to_vec());
        assert_eq!(used, 9);
        // A claim smaller than the field itself cannot be framed.
        assert!(f.split(&[b'R', 0, 0, 0, 2]).is_err());
    }

    #[test]
    fn varint_framing_reads_a_base128_length() {
        let f = Framing::VarintPrefix { offset: 1 };
        // MQTT-shaped: fixed header byte, varint 2, 2 payload bytes.
        let (unit, used) = f.split(&[0x30, 0x02, 0xaa, 0xbb, 0x99]).unwrap().unwrap();
        assert_eq!(unit, vec![0x30, 0x02, 0xaa, 0xbb]);
        assert_eq!(used, 4);
        // A two-byte varint: low 7 bits 0 with the continue bit, then
        // 1 in the next group = 128.
        let mut long = vec![0x30, 0x80, 0x01];
        long.extend(std::iter::repeat_n(0u8, 127));
        assert_eq!(f.split(&long).unwrap(), None, "127 of 128 payload bytes arrived");
        long.push(0);
        assert_eq!(f.split(&long).unwrap().unwrap().1, 3 + 128);
        // A varint that never terminates is a framing mismatch.
        assert!(f.split(&[0x30, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]).is_err());
    }

    #[test]
    fn oversized_claims_are_refused_not_buffered() {
        let f = Framing::LengthPrefix {
            offset: 0,
            size: 4,
            little_endian: false,
            counts: LengthCounts::Payload,
            trailer: 0,
        };
        assert!(f.split(&[0xff, 0xff, 0xff, 0xff]).is_err());
        let d = crlf();
        let long = vec![b'a'; MAX_UNIT_BYTES + 1];
        assert!(d.split(&long).is_err());
    }

    #[test]
    fn validation_refuses_the_silent_never_fires_shapes() {
        let ok = StreamListen::new("imap.example.com:993", crlf(), r"^\* \d+ EXISTS");
        ok.validate().expect("a well-formed spec validates");

        let mut bad = ok.clone();
        bad.fire = "(unclosed".into();
        assert!(bad.validate().unwrap_err().contains("fire"));

        let mut bad = ok.clone();
        bad.script.push(ScriptStep {
            send: SocketFrame::Text { body: "a1 LOGIN {user} {password}\r\n".into() },
            until: "[bad".into(),
        });
        assert!(bad.validate().unwrap_err().contains("script[0]"));

        let mut bad = ok.clone();
        bad.framing = Framing::Delimiter { bytes: String::new() };
        assert!(bad.validate().unwrap_err().contains("separator"));

        let mut bad = ok.clone();
        bad.framing = Framing::LengthPrefix {
            offset: 0,
            size: 9,
            little_endian: false,
            counts: LengthCounts::Payload,
            trailer: 0,
        };
        assert!(bad.validate().unwrap_err().contains("1..=8"));

        let mut bad = ok.clone();
        bad.heartbeat = Some(SocketFrame::Text { body: "NOOP\r\n".into() });
        bad.heartbeat_secs = 0;
        assert!(bad.validate().unwrap_err().contains("heartbeat_secs"));

        let mut bad = ok;
        bad.address = "  ".into();
        assert!(bad.validate().unwrap_err().contains("address"));
    }

    #[test]
    fn an_imap_shaped_config_round_trips() {
        let s = StreamListen::new("{imap_host}:{imap_port}", crlf(), r"^\* \d+ (EXISTS|RECENT)")
            .step(
                SocketFrame::Text { body: "a1 LOGIN {user} {password}\r\n".into() },
                "^a1 OK",
            )
            .step(SocketFrame::Text { body: "a2 SELECT INBOX\r\n".into() }, "^a2 OK")
            .step(SocketFrame::Text { body: "a3 IDLE\r\n".into() }, r"^\+")
            .with_heartbeat(
                SocketFrame::Text { body: "DONE\r\na9 IDLE\r\n".into() },
                1500,
            );
        s.validate().expect("valid");
        let spec = crate::signal::to_spec(s);
        assert_eq!(spec.kind, "stream_listen");
        let back: StreamListen = serde_json::from_value(spec.config).unwrap();
        assert_eq!(back.script.len(), 3);
        assert_eq!(back.heartbeat_secs, 1500);
        assert!(back.tls, "tls defaults on");
    }
}
