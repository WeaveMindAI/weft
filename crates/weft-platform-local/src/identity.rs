//! Identities a local install signs itself.
//!
//! There is no cloud to vouch for anyone on a laptop, so the install holds
//! one key (`WEFT_IDENTITY_KEY`, 32 bytes, written when the install is
//! created) and signs a short token per principal with it: one for its own
//! roles, and one per worker it starts (handed to that worker's container
//! at start). Verifying is checking the signature and the expiry. A worker
//! never sees the key, so it cannot mint a token for anyone but itself.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use weft_core::signed_token::{self, SignedClaims};
use weft_platform_traits::identity::{CallerIdentity, IdentityRefused, IdentityTokens, Principal};

/// The noun the token's error strings use.
const NOUN: &str = "identity token";

/// How long a role's own token lasts before it is minted again.
const ROLE_TOKEN_LIFE_SECS: i64 = 3600;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct IdentityClaims {
    principal: Principal,
    exp: i64,
}

impl SignedClaims for IdentityClaims {
    fn exp(&self) -> i64 {
        self.exp
    }
}

/// Signs and verifies identities with the install's key.
pub struct LocalIdentity {
    key: Vec<u8>,
}

impl LocalIdentity {
    /// The install's key, hex, from `WEFT_IDENTITY_KEY`.
    pub fn from_env() -> anyhow::Result<Self> {
        let raw = std::env::var("WEFT_IDENTITY_KEY")
            .map_err(|_| anyhow::anyhow!("WEFT_IDENTITY_KEY is required: the key the install signs its own identities with"))?;
        Self::from_hex(&raw)
    }

    pub fn from_hex(raw: &str) -> anyhow::Result<Self> {
        let key = hex::decode(raw.trim()).map_err(|e| anyhow::anyhow!("WEFT_IDENTITY_KEY is not hex: {e}"))?;
        anyhow::ensure!(key.len() >= 32, "WEFT_IDENTITY_KEY must be at least 32 bytes");
        Ok(Self { key })
    }

    /// A token naming `principal`, good until `exp` (unix seconds).
    pub fn mint(&self, principal: Principal, exp: i64) -> String {
        signed_token::mint(&self.key, &IdentityClaims { principal, exp })
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock past UNIX_EPOCH")
            .as_secs() as i64
    }
}

#[async_trait]
impl CallerIdentity for LocalIdentity {
    /// The audience is not checked: a local token names a principal, and
    /// every address it could be presented at is this install's.
    async fn verify(&self, bearer: &str, _audiences: &[String]) -> Result<Principal, IdentityRefused> {
        let claims: IdentityClaims = signed_token::validate(&self.key, bearer, Self::now(), NOUN).map_err(|why| {
            if why.contains("expired") {
                IdentityRefused::Expired
            } else if why.contains("signature") {
                IdentityRefused::BadSignature
            } else {
                IdentityRefused::Malformed(why)
            }
        })?;
        Ok(claims.principal)
    }
}

/// The token a weft role presents: `Principal::Core`, minted fresh as it
/// nears its end.
#[async_trait]
impl IdentityTokens for LocalIdentity {
    async fn token_for(&self, _audience: &str) -> anyhow::Result<String> {
        Ok(self.mint(Principal::Core, Self::now() + ROLE_TOKEN_LIFE_SECS))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> LocalIdentity {
        LocalIdentity::from_hex(&"ab".repeat(32)).unwrap()
    }

    #[tokio::test]
    async fn a_minted_token_names_its_principal() {
        let id = identity();
        let worker = Principal::Worker { tenant: "t".into(), project: uuid::Uuid::from_u128(3) };
        let token = id.mint(worker.clone(), LocalIdentity::now() + 60);
        assert_eq!(id.verify(&token, &[]).await.unwrap(), worker);
        let core = id.token_for("http://x").await.unwrap();
        assert_eq!(id.verify(&core, &[]).await.unwrap(), Principal::Core);
    }

    #[tokio::test]
    async fn a_forged_or_stale_token_is_refused() {
        let id = identity();
        let other = LocalIdentity::from_hex(&"cd".repeat(32)).unwrap();
        let forged = other.mint(Principal::Core, LocalIdentity::now() + 60);
        assert_eq!(id.verify(&forged, &[]).await.unwrap_err(), IdentityRefused::BadSignature);
        let stale = id.mint(Principal::Core, LocalIdentity::now() - 1);
        assert_eq!(id.verify(&stale, &[]).await.unwrap_err(), IdentityRefused::Expired);
        assert!(matches!(id.verify("garbage", &[]).await.unwrap_err(), IdentityRefused::Malformed(_)));
    }

    #[test]
    fn a_short_key_is_refused() {
        assert!(LocalIdentity::from_hex("abcd").is_err());
        assert!(LocalIdentity::from_hex("zz").is_err());
    }
}
