//! Download capabilities: the box's OWN short-lived, single-file
//! bearer tokens. The box is the single capability authority: it
//! mints and validates with a box-local HMAC secret persisted on the
//! backing disks (pod restarts don't invalidate outstanding tokens;
//! the box is only torn down at zero bytes, when no token has
//! anything left to fetch). Pure functions; the caller passes `now`.
//!
//! The crypto + wire format live ONCE in [`weft_core::signed_token`]
//! (HMAC-SHA256 over a base64url JSON payload, `v1.<payload>.<sig>`); this
//! module is just the capability CLAIMS plus thin typed wrappers. The
//! live-caller routing token is the same machinery with different claims.

use serde::{Deserialize, Serialize};

use weft_core::signed_token::{self, SignedClaims};

/// Caller-safe noun for this token's error strings (no secret leak).
const NOUN: &str = "capability";

/// What a capability grants: ONE key, until `exp` (unix seconds).
///
/// SECURITY INVARIANT: a cap binds to a key STRING, so its safety
/// depends on a key never being reused across two different files. That
/// holds because every put mints a fresh `uuid::Uuid::new_v4()` id
/// (`service::put_file`), so a deleted file's key is never handed to a
/// later file: an outstanding cap for a gone file can only ever 404,
/// never serve a different file's bytes. If id minting ever stops being
/// globally-unique-forever, caps must additionally encode a
/// file-generation nonce or this guarantee breaks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityClaims {
    pub key: String,
    pub exp: i64,
}

impl SignedClaims for CapabilityClaims {
    fn exp(&self) -> i64 {
        self.exp
    }
}

pub fn mint(secret: &[u8], key: &str, expires_at_unix: i64) -> String {
    signed_token::mint(
        secret,
        &CapabilityClaims { key: key.to_string(), exp: expires_at_unix },
    )
}

/// Validate a capability and return its claims. Rejects on format,
/// signature, and expiry; reasons are caller-safe (no secret leak).
pub fn validate(secret: &[u8], capability: &str, now_unix: i64) -> Result<CapabilityClaims, String> {
    signed_token::validate(secret, capability, now_unix, NOUN)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"test-secret-32-bytes-aaaaaaaaaaa";

    #[test]
    fn mint_validate_round_trip() {
        let cap = mint(SECRET, "exec/c1/f1", 1_000);
        let claims = validate(SECRET, &cap, 999).unwrap();
        assert_eq!(claims.key, "exec/c1/f1");
        assert_eq!(claims.exp, 1_000);
    }

    #[test]
    fn rejects_expired() {
        let cap = mint(SECRET, "k", 1_000);
        assert_eq!(validate(SECRET, &cap, 1_000).unwrap_err(), "capability expired");
        assert!(validate(SECRET, &cap, 2_000).is_err());
    }

    #[test]
    fn rejects_wrong_secret_and_tampering() {
        let cap = mint(SECRET, "exec/c1/f1", 1_000);
        assert!(validate(b"other-secret", &cap, 0).is_err());
        // Tamper: mint a different key under a DIFFERENT secret, then splice
        // that forged payload onto the real cap's signature. The sig was
        // computed over the original payload, so it cannot validate the swap.
        let forged_full = mint(b"attacker-secret", "exec/c2/f9", 1_000);
        let forged_payload = forged_full.split('.').nth(1).unwrap();
        let real_sig = cap.split('.').nth(2).unwrap();
        let forged = format!("v1.{forged_payload}.{real_sig}");
        assert!(validate(SECRET, &forged, 0).is_err());
    }

    #[test]
    fn rejects_malformed() {
        for bad in ["", "v1", "v1.abc", "v2.a.b", "v1.!!.??"] {
            assert!(validate(SECRET, bad, 0).is_err(), "should reject {bad:?}");
        }
    }
}
