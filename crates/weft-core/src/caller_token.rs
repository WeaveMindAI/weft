//! The ticket a browser opens its socket with. A browser cannot put a
//! credential on a WebSocket's opening request, so it asks the route with
//! a plain request first: the worker's door checks that request (its
//! route, its gate, its instance, its limits) and answers with a URL that
//! carries a ticket saying what it approved. The socket opened at that URL,
//! on whichever copy of the project's workers it reaches, shows the ticket
//! and its run is born there, held to what the ticket says. A copy rejects
//! a ticket that is missing, expired, forged, or for another project or
//! route.
//!
//! The crypto + wire format live ONCE in [`crate::signed_token`] (HMAC-SHA256
//! over a base64url JSON payload, `v1.<payload>.<sig>`); this module is just
//! the ticket's CLAIMS plus thin typed wrappers. The storage download
//! capability is the same machinery with different claims.

use serde::{Deserialize, Serialize};

use crate::signed_token::{self, SignedClaims};

/// Caller-safe noun for this token's error strings (no secret leak).
const NOUN: &str = "socket ticket";

/// What a ticket grants: one socket to the route `route_token` of
/// `project_id`, opened until `exp` (unix seconds), as the caller the
/// door checked (`identity`, for whom `instance`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallerTokenClaims {
    pub project_id: uuid::Uuid,
    /// The route the ticket opens (its signal token).
    pub route_token: String,
    /// A fingerprint of the request the gate approved, present only
    /// when something was checked (an open route approves nothing, so
    /// there is nothing to hold the caller to).
    ///
    /// Without it the door says "this is Alice" and nothing about what
    /// Alice asked for, so a caller could pass the gate with one
    /// request and then open their socket with a different one. For a
    /// password-shaped check that costs nothing, since the answer really
    /// was only about who they are. For a SIGNATURE it costs everything:
    /// a signature's whole claim is about one exact request, and a
    /// ticket any request fits throws that claim away.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved: Option<RequestFingerprint>,
    /// Who the gate said the caller is (`None` on an open route).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<serde_json::Value>,
    /// The instance the run is for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<crate::instance::InstanceId>,
    /// Random, so two callers asking alike in the same second hold two
    /// tickets, never one.
    pub nonce: uuid::Uuid,
    pub exp: i64,
}

/// What the gate saw, small enough to sign and exact enough to hold a
/// caller to. The body is a hash rather than the body, so the door stays
/// a token rather than a copy of the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestFingerprint {
    pub method: String,
    pub path: String,
    /// The query string as the gate received it, hashed.
    pub query_sha256: String,
    /// The body as the gate received it, hashed. Empty body hashes like
    /// any other, so "sent nothing" and "sent something" are different
    /// fingerprints rather than both being absent.
    pub body_sha256: String,
}

impl RequestFingerprint {
    /// Take the fingerprint of a request. One function, called by the
    /// gate when it approves and by the worker when the caller arrives,
    /// so the two can never disagree about what they are comparing.
    pub fn of(method: &str, path: &str, query: &str, body: &[u8]) -> Self {
        Self {
            method: method.to_ascii_uppercase(),
            path: path.trim_start_matches('/').to_string(),
            query_sha256: sha256_hex(query.as_bytes()),
            body_sha256: sha256_hex(body),
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

impl SignedClaims for CallerTokenClaims {
    fn exp(&self) -> i64 {
        self.exp
    }
}

/// The query pair a ticket rides as, always the LAST of the socket URL's
/// query, so the caller's own query (which the gate may have hashed) is
/// what is left once it is taken off.
pub const TICKET_PARAM: &str = "wct";

/// A socket URL's query split into the caller's own query and the ticket
/// appended as its last pair. `None` when the last pair is no ticket.
pub fn split_ticket(raw_query: &str) -> Option<(&str, &str)> {
    fn ticket(pair: &str) -> Option<&str> {
        pair.strip_prefix(TICKET_PARAM)?.strip_prefix('=')
    }
    match raw_query.rsplit_once('&') {
        Some((callers, last)) => Some((callers, ticket(last)?)),
        None => Some(("", ticket(raw_query)?)),
    }
}

/// The URL a browser opens its socket at: `base` (the address and prefix
/// the caller reached the route at) and `raw_path`, `raw_query` as the
/// caller sent them (still percent-encoded), with the ticket appended as
/// the last pair.
pub fn socket_url(base: &str, raw_path: &str, raw_query: &str, ticket: &str) -> String {
    let base = base.trim_end_matches('/');
    let path = raw_path.trim_start_matches('/');
    let carried = if raw_query.is_empty() { String::new() } else { format!("{raw_query}&") };
    format!("{base}/{path}?{carried}{TICKET_PARAM}={ticket}")
}

/// A project's own secret, derived from the install's caller-ticket secret
/// (`WEFT_CALLER_TOKEN_SECRET`): what the project's workers hold
/// (`WEFT_PROJECT_SECRET`), and never the install's secret itself, since
/// the program's own code runs in those workers and could read anything
/// they hold. Everything a worker signs or checks is keyed from it, so a
/// worker of one project can neither reach another project's workers nor
/// mint a ticket another project would accept. The parts of weft that hold
/// the install's secret (the runners, the listener) derive the
/// same value for whichever project they talk to.
#[derive(Clone)]
pub struct ProjectSecret(Vec<u8>);

impl std::fmt::Debug for ProjectSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProjectSecret(..)")
    }
}

impl ProjectSecret {
    /// `project`'s secret under the install's secret.
    pub fn of(install_secret: &[u8], project_id: uuid::Uuid) -> Self {
        Self(signed_token::derive_key(install_secret, &format!("weft-project:{project_id}")))
    }

    /// The project's secret a worker was given (`WEFT_PROJECT_SECRET`).
    pub fn from_env() -> Result<Self, String> {
        let raw = std::env::var("WEFT_PROJECT_SECRET").map_err(|_| "WEFT_PROJECT_SECRET is required".to_string())?;
        Self::from_hex(&raw).map_err(|e| format!("WEFT_PROJECT_SECRET: {e}"))
    }

    /// Read back what [`Self::to_hex`] wrote. Anything else (an empty or
    /// short value would key tickets anybody can forge) is refused.
    pub fn from_hex(hex: &str) -> Result<Self, String> {
        let bytes = crate::access::hex_to_bytes(hex.trim()).ok_or_else(|| "the project secret is not hex".to_string())?;
        if bytes.len() != 32 {
            return Err(format!("the project secret is {} bytes; it is 32", bytes.len()));
        }
        Ok(Self(bytes))
    }

    pub fn to_hex(&self) -> String {
        crate::access::hex_of(&self.0)
    }

    /// The key weft's own calls to the project's workers carry, hex
    /// (`weft_platform_traits::WORKER_AUTH_HEADER`).
    pub fn worker_door_key(&self) -> String {
        crate::access::hex_of(&signed_token::derive_key(&self.0, "worker-door"))
    }

    /// Mint a socket ticket for one of the project's routes.
    pub fn mint_ticket(&self, claims: &CallerTokenClaims) -> String {
        signed_token::mint(&signed_token::derive_key(&self.0, "caller-ticket"), claims)
    }

    /// Validate a ticket and return its claims. Rejects on format,
    /// signature, and expiry, and a ticket another project minted fails the
    /// signature; reasons are caller-safe (no secret leak). The door also
    /// checks the route is the one the socket asks for.
    pub fn validate_ticket(&self, token: &str, now_unix: i64) -> Result<CallerTokenClaims, String> {
        signed_token::validate(&signed_token::derive_key(&self.0, "caller-ticket"), token, now_unix, NOUN)
    }

}

/// The key weft's own calls to `project`'s workers carry, for a part of weft
/// holding the install's secret.
pub fn worker_door_key(install_secret: &[u8], project_id: uuid::Uuid) -> String {
    ProjectSecret::of(install_secret, project_id).worker_door_key()
}

/// What a caller is told when their ticket is no good, for any reason
/// `validate` gives. Every reason has the same remedy, so the answer leads
/// with it and names the reason after. The one that actually happens to
/// people is expiry: a ticket is good for a couple of minutes.
pub fn refusal(why: &str) -> String {
    format!(
        "this connection ticket is no good ({why}). Ask for a new one at the \
         route's address and open the socket at the URL it answers: a ticket \
         lasts a couple of minutes."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    const SECRET: &[u8] = b"test-secret-32-bytes-aaaaaaaaaaa";

    fn claims(exp: i64) -> CallerTokenClaims {
        CallerTokenClaims {
            project_id: Uuid::nil(),
            route_token: "route-1".into(),
            approved: Some(RequestFingerprint::of("post", "/chat/room7", "a=1", b"{}")),
            identity: Some(serde_json::json!({ "sub": "alice" })),
            instance: None,
            nonce: Uuid::nil(),
            exp,
        }
    }

    /// The caller's own query rides along byte for byte, which is what
    /// the gate hashed; the ticket is the last pair, so a `wct` the
    /// caller sent stays theirs and cannot shadow the door's.
    #[test]
    fn the_callers_query_rides_along_untouched_and_cannot_shadow_the_ticket() {
        for callers in ["verbose=1&q=a%20b", "a=1&&b=2&", "wct=fake&a=1"] {
            let url = socket_url("https://gw/connect/t", "/users/42", callers, "t");
            assert_eq!(url, format!("https://gw/connect/t/users/42?{callers}&wct=t"));
            let query = url.split_once('?').unwrap().1;
            assert_eq!(split_ticket(query), Some((callers, "t")));
        }
        assert_eq!(socket_url("http://127.0.0.1:9000/", "/", "", "tok"), "http://127.0.0.1:9000/?wct=tok");
        assert_eq!(split_ticket("wct=v1.x.y&b=2"), None, "the ticket is always appended last");
        assert_eq!(split_ticket("a=1"), None);
    }

    /// The fingerprint is what holds a caller to the request the gate
    /// approved, so it has to answer the same for the same request and
    /// differently for anything else about it.
    #[test]
    fn a_fingerprint_pins_the_request_that_was_approved() {
        let approved = RequestFingerprint::of("POST", "orders", "n=1", b"{\"amount\":5}");
        // The same request, spelled the way the two sides happen to
        // spell it: the method's case and the path's leading slash are
        // not a difference.
        assert_eq!(approved, RequestFingerprint::of("post", "/orders", "n=1", b"{\"amount\":5}"));
        // Everything that IS a difference.
        assert_ne!(approved, RequestFingerprint::of("DELETE", "orders", "n=1", b"{\"amount\":5}"));
        assert_ne!(approved, RequestFingerprint::of("POST", "refunds", "n=1", b"{\"amount\":5}"));
        assert_ne!(approved, RequestFingerprint::of("POST", "orders", "n=2", b"{\"amount\":5}"));
        assert_ne!(
            approved,
            RequestFingerprint::of("POST", "orders", "n=1", b"{\"amount\":999999}")
        );
        // An empty body is a fingerprint of its own, so dropping the
        // body is a mismatch rather than an absence nobody notices.
        assert_ne!(approved, RequestFingerprint::of("POST", "orders", "n=1", b""));
    }

    fn project(n: u128) -> ProjectSecret {
        ProjectSecret::of(SECRET, Uuid::from_u128(n))
    }

    #[test]
    fn a_projects_worker_key_is_its_own_and_the_same_for_whoever_derives_it() {
        let a = worker_door_key(SECRET, Uuid::from_u128(1));
        assert_eq!(a, project(1).worker_door_key());
        assert_eq!(a, ProjectSecret::from_hex(&project(1).to_hex()).unwrap().worker_door_key());
        assert_ne!(a, worker_door_key(SECRET, Uuid::from_u128(2)));
        assert_ne!(a, worker_door_key(b"other-secret-bbbbbbbbbbbbbbbbbbbb", Uuid::from_u128(1)));
        assert_eq!(a.len(), 64);
    }

    /// A project's secret is not the install's, and nothing keyed from one
    /// project's opens another's.
    #[test]
    fn a_project_secret_signs_for_its_own_project_only() {
        assert_ne!(project(1).to_hex(), crate::access::hex_of(SECRET));
        let ticket = project(1).mint_ticket(&claims(1_000));
        assert!(project(2).validate_ticket(&ticket, 0).is_err());
        assert!(signed_token::validate::<CallerTokenClaims>(SECRET, &ticket, 0, NOUN).is_err());
    }

    #[test]
    fn mint_validate_round_trip() {
        let p = project(0);
        let tok = p.mint_ticket(&claims(1_000));
        let back = p.validate_ticket(&tok, 999).unwrap();
        assert_eq!(back, claims(1_000));
        // An open route approved no request.
        let bare = CallerTokenClaims { approved: None, ..claims(1_000) };
        assert_eq!(p.validate_ticket(&p.mint_ticket(&bare), 0).unwrap(), bare);
    }

    #[test]
    fn rejects_expired() {
        let p = project(0);
        let tok = p.mint_ticket(&claims(1_000));
        assert_eq!(
            p.validate_ticket(&tok, 1_000).unwrap_err(),
            "socket ticket expired"
        );
        assert!(p.validate_ticket(&tok, 2_000).is_err());
    }

    #[test]
    fn rejects_wrong_secret_and_tampering() {
        let p = project(0);
        let tok = p.mint_ticket(&claims(1_000));
        assert!(ProjectSecret::of(b"other-secret-bbbbbbbbbbbbbbbbbbbb", Uuid::from_u128(0)).validate_ticket(&tok, 0).is_err());
        // Tamper: re-mint for another project under a DIFFERENT secret, then
        // splice that forged payload onto the real token's signature. The sig
        // was computed over the original payload, so it cannot validate the
        // re-pointed one.
        let elsewhere = CallerTokenClaims { project_id: uuid::Uuid::from_u128(0xbad), ..claims(1_000) };
        let forged_full = ProjectSecret::of(b"attacker-secret-cccccccccccccccc", Uuid::from_u128(0)).mint_ticket(&elsewhere);
        let forged_payload = forged_full.split('.').nth(1).unwrap();
        let real_sig = tok.split('.').nth(2).unwrap();
        let forged = format!("v1.{forged_payload}.{real_sig}");
        assert!(
            p.validate_ticket(&forged, 0).is_err(),
            "a token re-pointed to another project must fail signature check"
        );
    }

    #[test]
    fn rejects_malformed() {
        for bad in ["", "v1", "v1.abc", "v2.a.b", "v1.!!.??"] {
            assert!(project(0).validate_ticket(bad, 0).is_err(), "should reject {bad:?}");
        }
    }
}
