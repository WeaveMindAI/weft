//! Who is calling, on Google Cloud: a Google-signed identity token,
//! checked against Google's published keys.
//!
//! weft's own roles run as the install's core service account, so a token
//! for that account is `Principal::Core`. A project's workers run as the
//! project's own account (`names::project_account_email`), whose name
//! holds the start of the project's id; the project it names is looked up
//! once and remembered.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Deserialize;
use weft_platform_traits::{CallerIdentity, IdentityRefused, Principal};

use crate::names;

const GOOGLE_CERTS: &str = "https://www.googleapis.com/oauth2/v3/certs";

/// How long Google's keys are kept when the answer says nothing.
const DEFAULT_KEYS_LIFE: Duration = Duration::from_secs(3600);

/// The soonest Google's keys are fetched again for a key id they do not
/// hold: a token signed with a key Google has just started using needs a
/// fresh set, but a stream of tokens naming a made-up key must not turn
/// into a stream of fetches.
const UNKNOWN_KEY_REFETCH: Duration = Duration::from_secs(30);

/// Google's keys as last fetched.
struct Keys {
    by_id: HashMap<String, jsonwebtoken::DecodingKey>,
    fetched: Instant,
    until: Instant,
}

impl Keys {
    /// Whether a token naming `kid` calls for fetching the keys again.
    fn stale_for(&self, kid: &str, now: Instant) -> bool {
        now >= self.until || (!self.by_id.contains_key(kid) && now >= self.fetched + UNKNOWN_KEY_REFETCH)
    }
}

#[derive(Debug, Deserialize)]
struct Claims {
    email: Option<String>,
    #[serde(default)]
    email_verified: bool,
}

#[derive(Debug, Deserialize)]
struct Jwk {
    kid: String,
    n: String,
    e: String,
}

pub struct GoogleIdentity {
    http: reqwest::Client,
    gcp_project: String,
    core_account: String,
    /// Where a worker's project is looked up; `None` on a process that
    /// only ever takes weft's own calls (a unit's agent).
    pool: Option<sqlx::PgPool>,
    keys: tokio::sync::Mutex<Option<Keys>>,
    workers: parking_lot::Mutex<HashMap<String, Principal>>,
}

impl GoogleIdentity {
    pub fn new(gcp_project: String, core_account: String, pool: Option<sqlx::PgPool>) -> Self {
        Self {
            http: reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("reqwest client"),
            gcp_project,
            core_account,
            pool,
            keys: tokio::sync::Mutex::new(None),
            workers: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    async fn key(&self, kid: &str) -> Result<jsonwebtoken::DecodingKey, IdentityRefused> {
        let mut cached = self.keys.lock().await;
        let now = Instant::now();
        if cached.as_ref().is_none_or(|k| k.stale_for(kid, now)) {
            let resp = self.http.get(GOOGLE_CERTS).send().await.map_err(|e| IdentityRefused::Unavailable(e.to_string()))?;
            let life = max_age(resp.headers().get("cache-control").and_then(|v| v.to_str().ok())).unwrap_or(DEFAULT_KEYS_LIFE);
            #[derive(Deserialize)]
            struct Set {
                keys: Vec<Jwk>,
            }
            let set: Set = resp.json().await.map_err(|e| IdentityRefused::Unavailable(e.to_string()))?;
            let mut keys = HashMap::new();
            for k in set.keys {
                let key = jsonwebtoken::DecodingKey::from_rsa_components(&k.n, &k.e)
                    .map_err(|e| IdentityRefused::Unavailable(format!("Google published a key weft cannot read: {e}")))?;
                keys.insert(k.kid, key);
            }
            *cached = Some(Keys { by_id: keys, fetched: now, until: now + life });
        }
        cached
            .as_ref()
            .and_then(|k| k.by_id.get(kid).cloned())
            .ok_or_else(|| IdentityRefused::Malformed(format!("signed with a key Google does not publish ({kid})")))
    }

    async fn worker(&self, email: &str, prefix: &str) -> Result<Principal, IdentityRefused> {
        if let Some(p) = self.workers.lock().get(email) {
            return Ok(p.clone());
        }
        let Some(pool) = &self.pool else {
            return Err(IdentityRefused::Stranger(email.to_string()));
        };
        let rows: Vec<(uuid::Uuid, String)> =
            sqlx::query_as("SELECT id, tenant_id FROM project WHERE replace(id::text, '-', '') LIKE $1 || '%'")
                .bind(prefix)
                .fetch_all(pool)
                .await
                .map_err(|e| IdentityRefused::Unavailable(e.to_string()))?;
        match rows.as_slice() {
            [(project, tenant)] => {
                let p = Principal::Worker { tenant: tenant.clone(), project: *project };
                self.workers.lock().insert(email.to_string(), p.clone());
                Ok(p)
            }
            [] => Err(IdentityRefused::Stranger(format!("{email} (no project of this install)"))),
            _ => Err(IdentityRefused::Stranger(format!("{email} names several projects"))),
        }
    }
}

/// `max-age` from a `Cache-Control` header.
fn max_age(header: Option<&str>) -> Option<Duration> {
    header?
        .split(',')
        .map(str::trim)
        .find_map(|d| d.strip_prefix("max-age="))
        .and_then(|v| v.parse().ok())
        .map(Duration::from_secs)
}

#[async_trait]
impl CallerIdentity for GoogleIdentity {
    async fn verify(&self, bearer: &str, audiences: &[String]) -> Result<Principal, IdentityRefused> {
        if bearer.is_empty() {
            return Err(IdentityRefused::Missing);
        }
        let header = jsonwebtoken::decode_header(bearer).map_err(|e| IdentityRefused::Malformed(e.to_string()))?;
        let kid = header.kid.ok_or_else(|| IdentityRefused::Malformed("no key id".into()))?;
        let key = self.key(&kid).await?;
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
        validation.set_audience(audiences);
        validation.set_issuer(&["https://accounts.google.com", "accounts.google.com"]);
        let data = jsonwebtoken::decode::<Claims>(bearer, &key, &validation).map_err(|e| {
            use jsonwebtoken::errors::ErrorKind;
            match e.kind() {
                ErrorKind::ExpiredSignature => IdentityRefused::Expired,
                ErrorKind::InvalidSignature => IdentityRefused::BadSignature,
                ErrorKind::InvalidAudience => IdentityRefused::Stranger("a token for another audience".into()),
                _ => IdentityRefused::Malformed(e.to_string()),
            }
        })?;
        let claims = data.claims;
        let email = claims.email.filter(|_| claims.email_verified).ok_or_else(|| IdentityRefused::Stranger("a token with no verified account".into()))?;
        if email == self.core_account {
            return Ok(Principal::Core);
        }
        match names::project_prefix_of(&email, &self.gcp_project) {
            Some(prefix) => self.worker(&email, &prefix).await,
            None => Err(IdentityRefused::Stranger(email)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_key_refetches_at_most_once_a_while() {
        let t = Instant::now();
        let keys = Keys { by_id: HashMap::new(), fetched: t, until: t + DEFAULT_KEYS_LIFE };
        assert!(!keys.stale_for("new", t + Duration::from_secs(1)));
        assert!(keys.stale_for("new", t + UNKNOWN_KEY_REFETCH));
        assert!(keys.stale_for("any", t + DEFAULT_KEYS_LIFE));
    }

    #[test]
    fn keys_live_as_long_as_google_says() {
        assert_eq!(max_age(Some("public, max-age=21600, must-revalidate")), Some(Duration::from_secs(21600)));
        assert_eq!(max_age(Some("no-store")), None);
        assert_eq!(max_age(None), None);
    }

    #[tokio::test]
    async fn an_empty_or_garbled_token_is_refused_before_any_key_is_fetched() {
        let id = GoogleIdentity::new("acme".into(), "core@acme.iam.gserviceaccount.com".into(), None);
        assert_eq!(id.verify("", &[]).await.unwrap_err(), IdentityRefused::Missing);
        assert!(matches!(id.verify("not-a-jwt", &[]).await.unwrap_err(), IdentityRefused::Malformed(_)));
    }
}
