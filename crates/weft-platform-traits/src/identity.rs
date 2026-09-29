//! Who is calling: the identity every internal call carries.
//!
//! Every call between weft's own pieces (a worker asking the broker for a
//! task, the dispatcher asking the listener to process a fire, a wake
//! delivered to the listener) carries a bearer token, and the receiving
//! side turns it into a [`Principal`] before it does anything. There is
//! no trusted network: the same check runs on a laptop, where every role
//! shares one process, and on a cloud, where a role may be a service of
//! its own that the internet can knock on.
//!
//! Two sides, two traits. [`CallerIdentity`] answers "who sent this
//! token" for a server. [`IdentityTokens`] hands a client the token it
//! presents for a given audience (the base URL it is about to call). Each
//! platform implements both: the local one signs and checks its own
//! tokens with a key only the install holds; a cloud one presents and
//! verifies the cloud's own signed identities.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// What a verified caller is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Principal {
    /// One of weft's own roles (dispatcher, broker, listener,
    /// supervisor). Runs weft's code only, so it acts for any
    /// tenant; each endpoint still checks the resource it touches is
    /// real and takes the tenant from it.
    Core,
    /// A worker of exactly one project. Runs the user's compiled
    /// program, so it acts for that project and nothing else.
    Worker { tenant: String, project: uuid::Uuid },
}

/// Why a token was not accepted. Always a 401 to the caller: the
/// detail goes to the receiving side's log, never back on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityRefused {
    Missing,
    Malformed(String),
    BadSignature,
    Expired,
    /// Signed by the platform, but for somebody weft does not know
    /// (another service account, a project that no longer exists).
    Stranger(String),
    /// The check itself could not run (the platform's keys could not
    /// be fetched). The caller retries; nothing about the token was
    /// decided.
    Unavailable(String),
}

impl std::fmt::Display for IdentityRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => f.write_str("no bearer token"),
            Self::Malformed(why) => write!(f, "malformed token: {why}"),
            Self::BadSignature => f.write_str("the token's signature does not verify"),
            Self::Expired => f.write_str("the token has expired"),
            Self::Stranger(who) => write!(f, "the token names nobody this install knows: {who}"),
            Self::Unavailable(why) => write!(f, "the identity check could not run: {why}"),
        }
    }
}

impl std::error::Error for IdentityRefused {}

/// The server side: who sent this bearer token.
#[async_trait]
pub trait CallerIdentity: Send + Sync {
    /// Verify `bearer` (the token alone, without `Bearer `), presented to
    /// an endpoint reachable at any of `audiences` (the base URLs callers
    /// address it by: a worker and a role on the same machine may reach
    /// it at different ones).
    async fn verify(&self, bearer: &str, audiences: &[String]) -> Result<Principal, IdentityRefused>;
}

/// The client side: the token to present when calling `audience` (the
/// base URL of the role or worker being called).
#[async_trait]
pub trait IdentityTokens: Send + Sync {
    async fn token_for(&self, audience: &str) -> anyhow::Result<String>;
}

/// A fixed token, handed to a process at start (a local worker is given
/// its own). The audience does not change it: the local key signs a
/// principal, not an address.
pub struct FixedToken(pub String);

#[async_trait]
impl IdentityTokens for FixedToken {
    async fn token_for(&self, _audience: &str) -> anyhow::Result<String> {
        Ok(self.0.clone())
    }
}

/// The header a worker process names itself with on every call to the
/// broker: a random id it mints at boot. The principal says WHICH
/// project; this says which running copy of it, so a claim, the
/// ownership of the execution it drives, and the journal rows it writes all
/// name the same writer. It is self-asserted, and that is enough: it
/// only orders writers inside one project, all of which are the same
/// principal.
// SYNC: INSTANCE_HEADER <-> crates/weft-broker/src/auth.rs (read),
//       crates/weft-broker-client/src/client.rs (sent)
pub const INSTANCE_HEADER: &str = "x-weft-instance";

/// The header one of weft's own roles names itself with. A weft role is
/// trusted, so this only says which of its surfaces a call is for (a
/// listener may fire signals, a supervisor may write infra rows); the
/// token is what proves the caller is weft at all. On a platform where
/// every role shares one identity (one machine, one service account),
/// the token could not say which role anyway.
// SYNC: ROLE_HEADER <-> crates/weft-broker/src/auth.rs (read),
//       crates/weft-broker-client/src/token.rs (sent)
pub const ROLE_HEADER: &str = "x-weft-role";

/// A fresh instance id for this process.
pub fn mint_instance_id(role: &str) -> String {
    format!("{role}-{}", uuid::Uuid::new_v4().simple())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_principal_round_trips() {
        for p in [
            Principal::Core,
            Principal::Worker { tenant: "local".into(), project: uuid::Uuid::from_u128(7) },
        ] {
            let v = serde_json::to_value(&p).unwrap();
            assert_eq!(serde_json::from_value::<Principal>(v).unwrap(), p);
        }
        assert_eq!(serde_json::to_value(Principal::Core).unwrap(), serde_json::json!({ "kind": "core" }));
    }

    #[test]
    fn instance_ids_are_unique_and_name_their_role() {
        let a = mint_instance_id("worker");
        let b = mint_instance_id("worker");
        assert_ne!(a, b);
        assert!(a.starts_with("worker-"));
    }
}
