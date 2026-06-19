//! Signed routing token for a live caller connection. The dispatcher
//! mints one at the control handshake (after auth + ensuring the worker
//! is up); the gateway forwards it to the worker; the worker verifies it
//! before attaching the connection to an execution. A worker rejects any
//! connection whose token is missing, expired, forged, or addressed to a
//! different pod, so a worker that stays cluster-private only ever serves
//! connections the dispatcher signed.
//!
//! The crypto + wire format live ONCE in [`crate::signed_token`] (HMAC-SHA256
//! over a base64url JSON payload, `v1.<payload>.<sig>`); this module is just
//! the routing-token CLAIMS plus thin typed wrappers. The storage download
//! capability is the same machinery with different claims. Kept here in core
//! (not the dispatcher) because both the dispatcher (mint) and the
//! engine/worker (verify) need it and both depend on core.

use serde::{Deserialize, Serialize};

use crate::signed_token::{self, SignedClaims};
use crate::Color;

/// Caller-safe noun for this token's error strings (no secret leak).
const NOUN: &str = "routing token";

/// What a routing token grants: the right to attach ONE live connection
/// to execution `color` of `project_id`, served by the worker pod
/// `pod_name`, until `exp` (unix seconds).
///
/// `pod_name` is the pin: a held connection lives on exactly one pod for
/// its life, and the worker rejects a token addressed to a different pod.
/// `color` binds the connection to the one execution so `ctx.caller()`
/// resolves to the right run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallerTokenClaims {
    pub color: Color,
    pub project_id: String,
    pub pod_name: String,
    pub exp: i64,
}

impl SignedClaims for CallerTokenClaims {
    fn exp(&self) -> i64 {
        self.exp
    }
}

/// Mint a signed routing token. `secret` is the cluster's dispatcher
/// signing key (same provisioning path as the broker / storage HMAC
/// secrets).
pub fn mint(
    secret: &[u8],
    color: Color,
    project_id: &str,
    pod_name: &str,
    expires_at_unix: i64,
) -> String {
    signed_token::mint(
        secret,
        &CallerTokenClaims {
            color,
            project_id: project_id.to_string(),
            pod_name: pod_name.to_string(),
            exp: expires_at_unix,
        },
    )
}

/// Validate a routing token and return its claims. Rejects on format,
/// signature, and expiry; reasons are caller-safe (no secret leak). The
/// worker additionally checks `claims.pod_name == own_pod` and
/// `claims.color` resolves to a live execution; those are policy checks
/// on top of this cryptographic validation, not part of it.
pub fn validate(secret: &[u8], token: &str, now_unix: i64) -> Result<CallerTokenClaims, String> {
    signed_token::validate(secret, token, now_unix, NOUN)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    const SECRET: &[u8] = b"test-secret-32-bytes-aaaaaaaaaaa";

    fn color() -> Color {
        Uuid::from_u128(0x1234)
    }

    #[test]
    fn mint_validate_round_trip() {
        let tok = mint(SECRET, color(), "proj-1", "pod-7", 1_000);
        let claims = validate(SECRET, &tok, 999).unwrap();
        assert_eq!(claims.color, color());
        assert_eq!(claims.project_id, "proj-1");
        assert_eq!(claims.pod_name, "pod-7");
        assert_eq!(claims.exp, 1_000);
    }

    #[test]
    fn rejects_expired() {
        let tok = mint(SECRET, color(), "p", "pod", 1_000);
        assert_eq!(
            validate(SECRET, &tok, 1_000).unwrap_err(),
            "routing token expired"
        );
        assert!(validate(SECRET, &tok, 2_000).is_err());
    }

    #[test]
    fn rejects_wrong_secret_and_tampering() {
        let tok = mint(SECRET, color(), "proj-1", "pod-7", 1_000);
        assert!(validate(b"other-secret-bbbbbbbbbbbbbbbbbbbb", &tok, 0).is_err());
        // Tamper: re-mint with a different pod under a DIFFERENT secret, then
        // splice that forged payload onto the real token's signature. The sig
        // was computed over the original payload, so it cannot validate the
        // re-pointed one.
        let forged_full = mint(b"attacker-secret-cccccccccccccccc", color(), "proj-1", "attacker-pod", 1_000);
        let forged_payload = forged_full.split('.').nth(1).unwrap();
        let real_sig = tok.split('.').nth(2).unwrap();
        let forged = format!("v1.{forged_payload}.{real_sig}");
        assert!(
            validate(SECRET, &forged, 0).is_err(),
            "a token re-pointed to another pod must fail signature check"
        );
    }

    #[test]
    fn rejects_malformed() {
        for bad in ["", "v1", "v1.abc", "v2.a.b", "v1.!!.??"] {
            assert!(validate(SECRET, bad, 0).is_err(), "should reject {bad:?}");
        }
    }
}
