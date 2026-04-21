//! Thin HTTP client against the dispatcher.

use anyhow::Context;

#[derive(Clone)]
pub struct DispatcherClient {
    base: String,
    http: reqwest::Client,
}

impl DispatcherClient {
    pub fn new(base: impl Into<String>) -> Self {
        Self { base: base.into(), http: reqwest::Client::new() }
    }

    pub async fn get_json(&self, path: &str) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}{}", self.base, path);
        let resp = self.http.get(&url).send().await.with_context(|| format!("GET {url}"))?;
        resp.error_for_status()?.json().await.context("parse response")
    }

    pub async fn post_json(&self, path: &str, body: &serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}{}", self.base, path);
        let resp = self.http.post(&url).json(body).send().await.with_context(|| format!("POST {url}"))?;
        resp.error_for_status()?.json().await.context("parse response")
    }

    pub async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let url = format!("{}{}", self.base, path);
        let resp = self.http.delete(&url).send().await.with_context(|| format!("DELETE {url}"))?;
        resp.error_for_status()?;
        Ok(())
    }

    pub async fn post_empty(&self, path: &str) -> anyhow::Result<()> {
        let url = format!("{}{}", self.base, path);
        let resp = self.http.post(&url).send().await.with_context(|| format!("POST {url}"))?;
        resp.error_for_status()?;
        Ok(())
    }
}

pub fn resolve_dispatcher_url(override_url: Option<&str>) -> String {
    if let Some(u) = override_url {
        return u.to_string();
    }
    // Phase A2: read from weft.toml if present. Scaffold defaults.
    "http://localhost:9999".to_string()
}
