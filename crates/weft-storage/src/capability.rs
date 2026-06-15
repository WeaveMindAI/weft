//! Download capabilities: the box's OWN short-lived, single-file
//! bearer tokens. The box is the single capability authority: it
//! mints and validates with a box-local HMAC secret persisted on the
//! backing disks (pod restarts don't invalidate outstanding tokens;
//! the box is only torn down at zero bytes, when no token has
//! anything left to fetch). Pure functions; the caller passes `now`.
//!
//! Wire format: `v1.<payload-b64url>.<sig-b64url>` where payload is
//! the JSON claims and sig = HMAC-SHA256(secret, payload-b64url).

use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE_NO_PAD;

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

pub fn mint(secret: &[u8], key: &str, expires_at_unix: i64) -> String {
    let claims = CapabilityClaims { key: key.to_string(), exp: expires_at_unix };
    let payload = B64.encode(serde_json::to_vec(&claims).expect("claims serialize"));
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(payload.as_bytes());
    let sig = B64.encode(mac.finalize().into_bytes());
    format!("v1.{payload}.{sig}")
}

/// Validate a capability and return its claims. Rejects on format,
/// signature, and expiry; reasons are caller-safe (no secret leak).
pub fn validate(secret: &[u8], capability: &str, now_unix: i64) -> Result<CapabilityClaims, String> {
    let mut parts = capability.splitn(3, '.');
    let (Some("v1"), Some(payload), Some(sig)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err("malformed capability".into());
    };
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(payload.as_bytes());
    let sig_bytes = B64.decode(sig).map_err(|_| "malformed capability signature".to_string())?;
    // verify_slice is constant-time.
    mac.verify_slice(&sig_bytes).map_err(|_| "invalid capability signature".to_string())?;
    let claims: CapabilityClaims = serde_json::from_slice(
        &B64.decode(payload).map_err(|_| "malformed capability payload".to_string())?,
    )
    .map_err(|_| "malformed capability claims".to_string())?;
    if now_unix >= claims.exp {
        return Err("capability expired".into());
    }
    Ok(claims)
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
        // Tamper with the payload (swap the key) keeping the old sig.
        let parts: Vec<&str> = cap.split('.').collect();
        let forged_payload = B64.encode(
            serde_json::to_vec(&CapabilityClaims { key: "exec/c2/f9".into(), exp: 1_000 })
                .unwrap(),
        );
        let forged = format!("v1.{}.{}", forged_payload, parts[2]);
        assert!(validate(SECRET, &forged, 0).is_err());
    }

    #[test]
    fn rejects_malformed() {
        for bad in ["", "v1", "v1.abc", "v2.a.b", "v1.!!.??"] {
            assert!(validate(SECRET, bad, 0).is_err(), "should reject {bad:?}");
        }
    }
}
