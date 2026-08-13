//! Access: authorized ability to call a third party.
//!
//! ONE node-facing concept: [`Access`], the reference an access node
//! emits and action nodes consume. It points at a stored connection
//! (the store holds everything secret: the credential material, the
//! granted permissions, the identity, which app was used, who owns the
//! credential). A consuming node opens it per firing
//! (`ctx.open(&access)` / the `ctx.client` sugar) and gets a signed-in
//! HTTP client; whether the calls are measured follows from whether a
//! meter is registered for the service, and whether they are billed
//! follows from who owns the stored credential. Neither is ever
//! declared on a node, and node code cannot tell.
//!
//! Every cost figure in the system is produced by a provider meter
//! (`weft-providers`), run by the runtime around the call; a node never
//! states a cost and has no way to.

#[cfg(feature = "runtime")]
pub mod client;
pub mod events;
#[cfg(feature = "runtime")]
pub mod socket;
pub mod spec;
#[cfg(feature = "runtime")]
pub mod verify;
mod value;

pub use value::{Access, ACCESS_MARKER_KEY};

use serde::{Deserialize, Serialize};

/// Lowercase hex of `bytes`: the one hex writer every access-side
/// digest and signature site shares.
pub fn hex_of(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

/// Decode a hex string; `None` on anything that is not hex.
pub fn hex_to_bytes(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Whose credential a resolved connection rides; decides whose money a
/// call on it spends. A fact of the STORED connection, answered by the
/// resolver, never asserted by a client.
// SYNC: CredentialOwner <-> packages/weft-graph/src/protocol.ts CredentialOwner
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialOwner {
    /// The user's own credential (their key, their signed-in account):
    /// calls spend their money, so a measured figure is informational.
    TheirOwn,
    /// The runtime's own credential: calls spend its credit, so a
    /// measured figure is a charge.
    Ours,
}

/// A connection opened for ONE firing: the signed-in client to call
/// the service with, plus what resolution learned. Produced by
/// `ctx.open(&access)`; released by the runtime when the firing's body
/// finishes (nothing node-facing closes it).
///
/// `Debug` redacts the resolved values so a credential can never leak
/// through an error string or a log line.
#[cfg(feature = "runtime")]
#[derive(Clone)]
pub struct OpenedConnection {
    service: String,
    /// Everything the connection stores that the runtime hands out
    /// (never a refresh token or an app secret).
    values: std::collections::BTreeMap<String, String>,
    auth: Vec<spec::AuthStep>,
    identity: Option<String>,
    owner: CredentialOwner,
    client: reqwest_middleware::ClientWithMiddleware,
    /// The runtime's WebSocket dialer for this connection (signed
    /// handshake, routing, session metering). Behind [`Self::socket`].
    socket: std::sync::Arc<dyn socket::SocketDial>,
}

#[cfg(feature = "runtime")]
impl OpenedConnection {
    /// Assembled by the runtime after a resolve; never by node code.
    pub fn assemble(
        service: impl Into<String>,
        values: std::collections::BTreeMap<String, String>,
        auth: Vec<spec::AuthStep>,
        identity: Option<String>,
        owner: CredentialOwner,
        client: reqwest_middleware::ClientWithMiddleware,
        socket: std::sync::Arc<dyn socket::SocketDial>,
    ) -> Self {
        Self { service: service.into(), values, auth, identity, owner, client, socket }
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    /// Display identity of the connected account, if recorded.
    pub fn identity(&self) -> Option<&str> {
        self.identity.as_deref()
    }

    pub fn owner(&self) -> CredentialOwner {
        self.owner
    }

    /// The signed-in HTTP client: use it directly, or hand it to any
    /// library that accepts an injected client. Behind it, the runtime
    /// applies the service's auth steps, routes the call (straight to
    /// the service, or through the runtime's relay when the resolved
    /// credential carries one), and, when a meter is registered for
    /// the service, measures what each call cost and records the
    /// figure on the execution's cost trail. The one rule for calls on
    /// a connection: never construct your own HTTP client for them; a
    /// call on a hand-rolled client is invisible to the cost trail.
    pub fn client(&self) -> &reqwest_middleware::ClientWithMiddleware {
        &self.client
    }

    /// Open a WebSocket session to the service's realtime API. Same
    /// rules as [`Self::client`]: the runtime signs the handshake (the
    /// credential rides the handshake only, never a frame), routes the
    /// session, and, when the service's meter prices sessions, measures
    /// what it cost and records the figure on the execution's cost
    /// trail. Never construct your own socket client for a provider; a
    /// hand-rolled session is invisible to the cost trail.
    pub async fn socket(&self, url: &str) -> crate::error::WeftResult<socket::ProviderSocket> {
        self.socket.dial(url).await
    }

    /// One stored value of the connection, by name (a host, a port,
    /// a username, a password the protocol library takes directly).
    /// Fails loudly naming the service and the value when the
    /// connection does not hold it. Never put a secret value in an
    /// output port, a log line, or an error message.
    pub fn value(&self, name: &str) -> crate::error::WeftResult<&str> {
        self.values.get(name).map(String::as_str).ok_or_else(|| {
            crate::error::WeftError::Input(format!(
                "the '{}' connection holds no value named '{name}'; reconnect it (the \
                 service's fields may have changed since it was made)",
                self.service
            ))
        })
    }

    /// One stored value by name, or `None` when the connection does
    /// not hold it. For a genuinely optional field (an alias to send
    /// as); use [`Self::value`] for one the node cannot work without.
    pub fn opt_value(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    /// The connection's single credential string, for the rare library
    /// that insists on a raw value instead of an injected client.
    ///
    /// DERIVED, not declared: it exists exactly when the service's
    /// resolved auth is ONE step whose template interpolates ONE
    /// stored value (a bearer key, a bot token). Anything else (two
    /// steps, a Basic pair, request signing) has no single string that
    /// means anything, and this fails loudly naming the service.
    /// Never put the value in an output port, a log line, or an error
    /// message.
    pub fn credential(&self) -> crate::error::WeftResult<&str> {
        spec::single_credential(&self.auth, &self.values).map_err(|why| {
            crate::error::WeftError::Input(format!(
                "the '{}' connection has no single credential string ({why}); use \
                 `.client()` instead, which signs every request itself",
                self.service
            ))
        })
    }
}

#[cfg(feature = "runtime")]
impl std::fmt::Debug for OpenedConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenedConnection")
            .field("service", &self.service)
            .field("owner", &self.owner)
            .field("values", &"<redacted>")
            .finish()
    }
}

/// Declare an access node: the pass-through that reads the sealed
/// connect handle from its `account` config input and emits it on
/// `access`. Every access node's BODY is this exact shape (the
/// interesting part is its `metadata.json`, the service recipe), so
/// the body is one declaration and can never drift:
///
/// ```ignore
/// weft::access_node!(SlackAccessNode);
/// ```
///
/// The input is always named `account`, the one name every access
/// node's metadata declares.
#[macro_export]
macro_rules! access_node {
    ($name:ident) => {
        #[derive($crate::NodeManifest)]
        pub struct $name;

        #[$crate::async_trait::async_trait]
        impl $crate::Node for $name {
            async fn run(&self, ctx: $crate::ExecutionContext) -> $crate::WeftResult<()> {
                let access: $crate::Access = ctx.inputs.get("account")?;
                ctx.pulse_downstream($crate::node::NodeOutput::new().set("access", access))
                    .await
            }
        }
    };
}

#[cfg(all(test, feature = "runtime"))]
mod tests {
    use super::spec::{AuthStep, SignKind, Template};
    use super::*;
    use std::collections::BTreeMap;

    struct NoDial;
    #[async_trait::async_trait]
    impl socket::SocketDial for NoDial {
        async fn dial(&self, _url: &str) -> crate::error::WeftResult<socket::ProviderSocket> {
            unreachable!("these tests never dial")
        }
    }

    fn opened(auth: Vec<AuthStep>, values: &[(&str, &str)]) -> OpenedConnection {
        let client = reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build();
        OpenedConnection::assemble(
            "svc",
            values.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<BTreeMap<_, _>>(),
            auth,
            None,
            CredentialOwner::TheirOwn,
            client,
            std::sync::Arc::new(NoDial),
        )
    }

    /// A named stored value is handed out as-is; a name the
    /// connection does not hold refuses loudly naming both sides.
    #[test]
    fn a_stored_value_is_read_by_name() {
        let conn = opened(vec![], &[("imap_host", "imap.acme.com"), ("user", "q")]);
        assert_eq!(conn.value("imap_host").unwrap(), "imap.acme.com");
        let err = conn.value("smtp_host").unwrap_err().to_string();
        assert!(err.contains("svc") && err.contains("smtp_host"), "{err}");
    }

    /// `credential()` is DERIVED: one step interpolating one stored
    /// value yields it; everything else refuses loudly naming the
    /// service. This derivation replaces any config flag, so every
    /// refusal case is pinned.
    #[test]
    fn credential_is_derived_from_the_auth_shape() {
        let bearer = opened(
            vec![AuthStep::Header {
                name: "Authorization".into(),
                value: Template::new("Bearer {token}"),
            }],
            &[("token", "sk-1")],
        );
        assert_eq!(bearer.credential().unwrap(), "sk-1");

        let query = opened(
            vec![AuthStep::Query { name: "key".into(), value: Template::new("{key}") }],
            &[("key", "k-9")],
        );
        assert_eq!(query.credential().unwrap(), "k-9");

        // Two steps: no single string.
        let two = opened(
            vec![
                AuthStep::Header { name: "A".into(), value: Template::new("{token}") },
                AuthStep::Query { name: "q".into(), value: Template::new("{token}") },
            ],
            &[("token", "t")],
        );
        assert!(two.credential().unwrap_err().to_string().contains("svc"));

        // A Basic pair: no single string.
        let basic = opened(
            vec![AuthStep::Basic {
                username: Template::new("{sid}"),
                password: Template::new("{secret}"),
            }],
            &[("sid", "a"), ("secret", "b")],
        );
        assert!(basic.credential().is_err());

        // A signing step: no single string.
        let signed = opened(
            vec![AuthStep::Sign {
                with: SignKind::SigV4 {
                    service: "s3".into(),
                    region: Template::new("{region}"),
                    access_key_id: Template::new("{access_key_id}"),
                    secret_access_key: Template::new("{secret_access_key}"),
                },
            }],
            &[("region", "r"), ("access_key_id", "i"), ("secret_access_key", "s")],
        );
        assert!(signed.credential().is_err());

        // One step interpolating two values: no single string.
        let twovals = opened(
            vec![AuthStep::Header { name: "A".into(), value: Template::new("{a}:{b}") }],
            &[("a", "1"), ("b", "2")],
        );
        assert!(twovals.credential().is_err());
    }

    #[test]
    fn debug_never_prints_a_resolved_value() {
        let c = opened(
            vec![AuthStep::Header {
                name: "Authorization".into(),
                value: Template::new("Bearer {token}"),
            }],
            &[("token", "sk-very-secret")],
        );
        let rendered = format!("{c:?}");
        assert!(!rendered.contains("sk-very-secret"), "{rendered}");
    }
}
