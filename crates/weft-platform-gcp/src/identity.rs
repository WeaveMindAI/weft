//! Who is calling, on Google Cloud: a Google-signed identity token,
//! checked against Google's published keys.
//!
//! weft's own roles run as the install's core service account, so a token
//! for that account is `Principal::Core`. A project's workers run as the
//! project's own account (`names::project_account_email`), whose name
//! holds the start of the project's id; the project it names is looked up
//! once and remembered. A project's infra machines run as that account
//! too, and a machine's token also names the machine (Google's
//! `compute_engine` claims, asked for with `format=full`): its name starts
//! with the resource base of the copy it runs (`NodeRef::resource_base`),
//! so the token is that copy's agent, `Principal::InfraCopy`.

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
    /// Present on a Compute Engine machine's token.
    #[serde(default)]
    google: Option<GoogleClaims>,
}

#[derive(Debug, Deserialize)]
struct GoogleClaims {
    #[serde(default)]
    compute_engine: Option<ComputeEngine>,
}

#[derive(Debug, Deserialize)]
struct ComputeEngine {
    instance_name: String,
    project_id: String,
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

    /// The copy whose machine `machine` is, of the project `worker` names.
    async fn infra_copy(&self, worker: Principal, machine: &str) -> Result<Principal, IdentityRefused> {
        let (Principal::Worker { tenant, project }, Some(pool)) = (worker, &self.pool) else {
            return Err(IdentityRefused::Stranger(format!("machine {machine}")));
        };
        let copies: Vec<(String, String)> = sqlx::query_as("SELECT node_id, copy_id FROM infra_node WHERE project_id = $1")
            .bind(project)
            .fetch_all(pool)
            .await
            .map_err(|e| IdentityRefused::Unavailable(e.to_string()))?;
        copy_of_machine(&tenant, project, &copies, machine)
            .ok_or_else(|| IdentityRefused::Stranger(format!("machine {machine} runs no infra copy of project {project}")))
    }
}

/// Which of `copies` (node, copy id) of `project` the machine named
/// `machine` runs: its name is the copy's resource base and a unit. A
/// node can be named so that one copy's base starts with another's
/// (`a` and `a-<another copy's suffix>`), so the longest base that fits
/// is the machine's.
fn copy_of_machine(tenant: &str, project: uuid::Uuid, copies: &[(String, String)], machine: &str) -> Option<Principal> {
    copies
        .iter()
        .filter_map(|(node, copy_id)| {
            let base = weft_core::infra::NodeRef { tenant: tenant.to_string(), project, node: node.clone(), copy_id: copy_id.clone() }.resource_base();
            machine.strip_prefix(&base).is_some_and(|unit| unit.starts_with('-')).then_some((base.len(), copy_id))
        })
        .max_by_key(|(len, _)| *len)
        .map(|(_, copy_id)| Principal::InfraCopy { tenant: tenant.to_string(), project, copy_id: copy_id.clone() })
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
        let Some(prefix) = names::project_prefix_of(&email, &self.gcp_project) else {
            return Err(IdentityRefused::Stranger(email));
        };
        let worker = self.worker(&email, &prefix).await?;
        match claims.google.and_then(|g| g.compute_engine) {
            Some(machine) if machine.project_id == self.gcp_project => self.infra_copy(worker, &machine.instance_name).await,
            Some(machine) => Err(IdentityRefused::Stranger(format!("machine {} of another Google project", machine.instance_name))),
            None => Ok(worker),
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

    #[test]
    fn a_machine_is_the_copy_whose_resource_base_starts_its_name() {
        let project = uuid::Uuid::from_u128(9);
        let copies = vec![("db".to_string(), "wn-1".to_string()), ("cache".to_string(), "wn-2".to_string())];
        let base = weft_core::infra::NodeRef { tenant: "t".into(), project, node: "cache".into(), copy_id: "wn-2".into() }.resource_base();
        assert_eq!(
            copy_of_machine("t", project, &copies, &format!("{base}-main")),
            Some(Principal::InfraCopy { tenant: "t".into(), project, copy_id: "wn-2".into() })
        );
        assert_eq!(copy_of_machine("t", project, &copies, &format!("{base}main")), None, "a unit follows a dash");
        assert_eq!(copy_of_machine("t", project, &copies, "wi-other-000000000000-main"), None);
    }

    #[test]
    fn a_machine_token_carries_its_name() {
        let claims: Claims = serde_json::from_value(serde_json::json!({
            "email": "wp-x@acme.iam.gserviceaccount.com",
            "email_verified": true,
            "google": { "compute_engine": { "instance_name": "wi-db-1-main", "project_id": "acme", "zone": "z" } }
        }))
        .unwrap();
        assert_eq!(claims.google.and_then(|g| g.compute_engine).map(|m| m.instance_name).as_deref(), Some("wi-db-1-main"));
    }

    #[tokio::test]
    async fn an_empty_or_garbled_token_is_refused_before_any_key_is_fetched() {
        let id = GoogleIdentity::new("acme".into(), "core@acme.iam.gserviceaccount.com".into(), None);
        assert_eq!(id.verify("", &[]).await.unwrap_err(), IdentityRefused::Missing);
        assert!(matches!(id.verify("not-a-jwt", &[]).await.unwrap_err(), IdentityRefused::Malformed(_)));
    }
}
