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
///
/// Both shapes hold shared bytes ([`bytes::Bytes`]), the same the
/// transport reads into and writes from, so a frame passes between the
/// node and the socket without being copied, either way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketMessage {
    Text(SocketText),
    Binary(bytes::Bytes),
}

impl SocketMessage {
    /// A text frame of `text` (a `String` moves in without a copy).
    pub fn text(text: impl Into<SocketText>) -> Self {
        SocketMessage::Text(text.into())
    }

    /// A binary frame of `bytes` (a `Vec<u8>` moves in without a copy).
    pub fn binary(bytes: impl Into<bytes::Bytes>) -> Self {
        SocketMessage::Binary(bytes.into())
    }

    /// The payload bytes, whichever frame shape carries them.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            SocketMessage::Text(text) => text.as_str().as_bytes(),
            SocketMessage::Binary(b) => b,
        }
    }

    /// The frame as text, for a session that speaks a text protocol
    /// (JSON realtime APIs): a binary frame refuses loudly with its
    /// byte count, never flattened into an empty string that would
    /// report as a blank parse error downstream.
    pub fn into_text(self) -> WeftResult<SocketText> {
        match self {
            SocketMessage::Text(text) => Ok(text),
            SocketMessage::Binary(bytes) => Err(crate::error::WeftError::NodeFailed(format!(
                "the session sent a {}-byte binary frame; this endpoint speaks text frames",
                bytes.len()
            ))),
        }
    }
}

/// A text frame's payload: UTF-8 held as shared bytes. Reads as a `&str`
/// (it derefs to one). Every way to make one either checks the bytes are
/// UTF-8 or starts from a string, so [`SocketText::as_str`] never checks
/// again.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct SocketText(bytes::Bytes);

impl SocketText {
    /// `bytes` as text, refused when they are not UTF-8.
    pub fn from_utf8(bytes: bytes::Bytes) -> Result<Self, std::str::Utf8Error> {
        std::str::from_utf8(&bytes)?;
        Ok(Self(bytes))
    }

    /// `bytes` as text, without checking them: for a transport whose own
    /// type already guarantees them UTF-8 (a WebSocket library's text
    /// frame), so a received frame is not scanned twice.
    ///
    /// # Safety
    ///
    /// `bytes` must be valid UTF-8.
    pub unsafe fn from_utf8_unchecked(bytes: bytes::Bytes) -> Self {
        Self(bytes)
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: every constructor checks the bytes are UTF-8, starts
        // from a string, or (`from_utf8_unchecked`) is given that duty by
        // its caller; the bytes never change after.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }

    /// The UTF-8 bytes, shared, for a transport to write.
    pub fn into_bytes(self) -> bytes::Bytes {
        self.0
    }
}

impl std::ops::Deref for SocketText {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for SocketText {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<String> for SocketText {
    fn from(text: String) -> Self {
        Self(bytes::Bytes::from(text))
    }
}

impl From<&str> for SocketText {
    fn from(text: &str) -> Self {
        Self(bytes::Bytes::copy_from_slice(text.as_bytes()))
    }
}

impl std::fmt::Debug for SocketText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_str(), f)
    }
}

impl std::fmt::Display for SocketText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<str> for SocketText {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for SocketText {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
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

    /// This session, its every send, read and close ending with the
    /// cancelled error the moment `flag` (its run's) is set.
    pub(crate) fn ends_with_run(self, flag: std::sync::Arc<crate::cancellation::CancellationFlag>) -> Self {
        Self::new(Box::new(EndsWithRun { inner: self.inner, flag }))
    }
}

/// A transport whose every operation races its run's cancellation
/// ([`ProviderSocket::ends_with_run`]).
struct EndsWithRun {
    inner: Box<dyn SocketTransport>,
    flag: std::sync::Arc<crate::cancellation::CancellationFlag>,
}

#[async_trait::async_trait]
impl SocketTransport for EndsWithRun {
    async fn send(&mut self, msg: SocketMessage) -> WeftResult<()> {
        self.flag.race(self.inner.send(msg)).await?
    }

    async fn recv(&mut self) -> WeftResult<Option<SocketMessage>> {
        self.flag.race(self.inner.recv()).await?
    }

    async fn close(&mut self) -> WeftResult<()> {
        self.flag.race(self.inner.close()).await?
    }
}

/// A dialer whose sessions end with their run
/// ([`ProviderSocket::ends_with_run`]), and whose dial does too.
pub(crate) struct EndsWithRunDial {
    pub(crate) inner: std::sync::Arc<dyn SocketDial>,
    pub(crate) flag: std::sync::Arc<crate::cancellation::CancellationFlag>,
}

#[async_trait::async_trait]
impl SocketDial for EndsWithRunDial {
    async fn dial(&self, url: &str) -> WeftResult<ProviderSocket> {
        let socket = self.flag.race(self.inner.dial(url)).await??;
        Ok(socket.ends_with_run(self.flag.clone()))
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
        assert_eq!(SocketMessage::text("{}").into_text().unwrap(), "{}");
        let err = SocketMessage::binary(vec![0, 1, 2]).into_text().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("3-byte binary frame"), "{msg}");
    }

    /// A session whose provider never speaks: every read waits.
    struct Silent;
    #[async_trait::async_trait]
    impl SocketTransport for Silent {
        async fn send(&mut self, _: SocketMessage) -> WeftResult<()> {
            Ok(())
        }
        async fn recv(&mut self) -> WeftResult<Option<SocketMessage>> {
            std::future::pending().await
        }
        async fn close(&mut self) -> WeftResult<()> {
            Ok(())
        }
    }

    /// A read of a session its run handed out ends with the cancelled
    /// error once the run is cancelled, and so does every call after.
    #[tokio::test]
    async fn a_cancelled_run_ends_a_socket_read() {
        let flag = crate::cancellation::CancellationFlag::new_arc();
        let mut socket = ProviderSocket::new(Box::new(Silent)).ends_with_run(flag.clone());
        socket.send(SocketMessage::text("hi")).await.unwrap();
        let reading = tokio::spawn(async move { socket.recv().await.map(|_| ()) });
        flag.cancel_because(crate::run::CancelCause::User);
        let ended = tokio::time::timeout(std::time::Duration::from_secs(5), reading).await.expect("the read ended").unwrap();
        assert!(matches!(ended, Err(crate::error::WeftError::Cancelled)), "{ended:?}");
    }

    /// A node's string or buffer becomes a frame without being copied,
    /// and bytes that are not UTF-8 never become text.
    #[test]
    fn a_frame_holds_what_it_was_given_without_a_copy() {
        let text = String::from("{\"type\":\"hello\"}");
        let at = text.as_ptr();
        let SocketMessage::Text(frame) = SocketMessage::text(text) else { panic!("a text frame") };
        assert_eq!(frame.as_str().as_ptr(), at);
        assert_eq!(frame.clone().into_bytes().as_ptr(), at, "the bytes a transport writes are the same");
        let buffer = vec![1u8, 2, 3];
        let at = buffer.as_ptr();
        assert_eq!(SocketMessage::binary(buffer).as_bytes().as_ptr(), at);
        assert!(SocketText::from_utf8(bytes::Bytes::from_static(&[0xff, 0xfe])).is_err());
        assert_eq!(SocketText::from_utf8(bytes::Bytes::from_static(b"ok")).unwrap(), "ok");
    }
}
