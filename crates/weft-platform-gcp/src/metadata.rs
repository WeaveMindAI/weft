//! A process's own identity on Google Cloud, from the metadata server
//! every Compute Engine machine and Cloud Run instance has.
//!
//! Two kinds of token come from it: an identity token for a given
//! audience (what a call to a Cloud Run service or to a weft role
//! presents, and what `GoogleIdentity` verifies), and an access token
//! for Google's own APIs. Both are cached until shortly before they
//! expire.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use parking_lot::Mutex;
use weft_platform_traits::IdentityTokens;

const METADATA: &str = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default";

/// How long before a token's end it is fetched again.
const EARLY: Duration = Duration::from_secs(300);

/// A Google identity token is good for an hour.
const ID_TOKEN_LIFE: Duration = Duration::from_secs(3600);

pub struct MetadataTokens {
    http: reqwest::Client,
    ids: Mutex<HashMap<String, (String, Instant)>>,
    access: Mutex<Option<(String, Instant)>>,
}

impl Default for MetadataTokens {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataTokens {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("a default reqwest client builds"),
            ids: Mutex::new(HashMap::new()),
            access: Mutex::new(None),
        }
    }

    /// An identity token for `audience` (the base URL being called).
    pub async fn id_token(&self, audience: &str) -> anyhow::Result<String> {
        if let Some((token, until)) = self.ids.lock().get(audience) {
            if Instant::now() < *until {
                return Ok(token.clone());
            }
        }
        let token = self
            .http
            .get(format!("{METADATA}/identity"))
            .query(&[("audience", audience), ("format", "full")])
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .context("ask the metadata server for an identity token")?
            .error_for_status()
            .context("the metadata server refused an identity token")?
            .text()
            .await
            .context("read the identity token")?;
        self.ids.lock().insert(audience.to_string(), (token.clone(), Instant::now() + ID_TOKEN_LIFE - EARLY));
        Ok(token)
    }

    /// An access token for Google's APIs, as this process's service
    /// account.
    pub async fn access_token(&self) -> anyhow::Result<String> {
        if let Some((token, until)) = self.access.lock().as_ref() {
            if Instant::now() < *until {
                return Ok(token.clone());
            }
        }
        #[derive(serde::Deserialize)]
        struct Token {
            access_token: String,
            expires_in: u64,
        }
        let token: Token = self
            .http
            .get(format!("{METADATA}/token"))
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .context("ask the metadata server for an access token")?
            .error_for_status()
            .context("the metadata server refused an access token")?
            .json()
            .await
            .context("read the access token")?;
        let until = Instant::now() + Duration::from_secs(token.expires_in).saturating_sub(EARLY);
        *self.access.lock() = Some((token.access_token.clone(), until));
        Ok(token.access_token)
    }
}

#[async_trait]
impl IdentityTokens for MetadataTokens {
    async fn token_for(&self, audience: &str) -> anyhow::Result<String> {
        self.id_token(audience).await
    }
}
