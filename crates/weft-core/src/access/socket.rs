//! The node-facing WebSocket surface of an opened connection.
//!
//! A node never hand-rolls a socket client, for the same reason it never
//! hand-rolls an HTTP client: the runtime signs the handshake (the
//! credential rides the handshake ONLY, never a frame), routes the
//! session, and measures it when the service's meter prices sessions.
//! `conn.socket(url)` hands back a [`ProviderSocket`]; the node writes
//! and reads frames and closes it when done (dropping it releases the
//! socket too, but without the clean close exchange).
//!
//! The transport itself is injected by the runtime ([`SocketDial`]),
//! which keeps this crate free of any WebSocket implementation: the
//! engine dials, taps, and books the cost; this is only the surface.

use crate::error::WeftResult;

/// One WebSocket frame's payload, as a node sends or receives it.
/// Control frames (ping/pong) are handled by the transport and never
/// surface here; a Close ends the stream (`recv` returns `None`).
#[derive(Debug, Clone, PartialEq)]
pub enum SocketMessage {
    Text(String),
    Binary(Vec<u8>),
}

impl SocketMessage {
    /// The payload bytes, whichever frame shape carries them.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            SocketMessage::Text(s) => s.as_bytes(),
            SocketMessage::Binary(b) => b,
        }
    }

    /// The frame as text, for a session that speaks a text protocol
    /// (JSON realtime APIs): a binary frame refuses loudly with its
    /// byte count, never flattened into an empty string that would
    /// report as a blank parse error downstream.
    pub fn into_text(self) -> WeftResult<String> {
        match self {
            SocketMessage::Text(text) => Ok(text),
            SocketMessage::Binary(bytes) => Err(crate::error::WeftError::NodeExecution(format!(
                "the session sent a {}-byte binary frame; this endpoint speaks text frames",
                bytes.len()
            ))),
        }
    }
}

/// The open session: write frames, read frames, close. Reading `None`
/// means the peer (or the runtime) closed the session; a session cut by
/// the runtime carries its reason in the close frame, surfaced as the
/// error of the NEXT operation where the transport can tell.
#[async_trait::async_trait]
pub trait SocketTransport: Send {
    async fn send(&mut self, msg: SocketMessage) -> WeftResult<()>;
    async fn recv(&mut self) -> WeftResult<Option<SocketMessage>>;
    /// Close the session cleanly: send the close frame and drain what
    /// the peer sends around it, so the observation covers both
    /// directions to the end. Dropping the session instead releases the
    /// socket and ends its measurement as interrupted, but sends no
    /// close frame.
    async fn close(&mut self) -> WeftResult<()>;
}

/// The open session a node drives. A thin owner around the runtime's
/// transport so node code names a concrete type, not a `dyn` box.
pub struct ProviderSocket {
    inner: Box<dyn SocketTransport>,
}

impl ProviderSocket {
    pub fn new(inner: Box<dyn SocketTransport>) -> Self {
        Self { inner }
    }

    /// Send one frame to the provider.
    pub async fn send(&mut self, msg: SocketMessage) -> WeftResult<()> {
        self.inner.send(msg).await
    }

    /// The next frame from the provider, or `None` when the session
    /// closed.
    pub async fn recv(&mut self) -> WeftResult<Option<SocketMessage>> {
        self.inner.recv().await
    }

    /// Close the session cleanly.
    pub async fn close(&mut self) -> WeftResult<()> {
        self.inner.close().await
    }
}

/// The runtime's dialer, injected into an [`super::OpenedConnection`] at
/// assembly: given the provider URL the node wrote, produce the signed,
/// routed, measured session.
#[async_trait::async_trait]
pub trait SocketDial: Send + Sync {
    async fn dial(&self, url: &str) -> WeftResult<ProviderSocket>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A text frame passes through; a binary frame refuses with its
    /// byte count instead of flattening to an empty string.
    #[test]
    fn into_text_refuses_binary_frames() {
        assert_eq!(SocketMessage::Text("{}".into()).into_text().unwrap(), "{}");
        let err = SocketMessage::Binary(vec![0, 1, 2]).into_text().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("3-byte binary frame"), "{msg}");
    }
}
