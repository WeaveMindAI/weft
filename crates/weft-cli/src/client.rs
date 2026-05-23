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

    pub fn base(&self) -> &str {
        &self.base
    }

    /// The ONE place any client method turns an HTTP failure into an
    /// error. On non-2xx, surface the dispatcher's BODY text (its
    /// handlers return the reason as the body, e.g. "project is
    /// already activating; wait or weft deactivate") rather than
    /// reqwest's stock "HTTP status client error (...) for url (...)"
    /// line, which buries the reason behind URL noise. Falls back to
    /// the bare status only when the body is empty. Every verb routes
    /// through this so they all get the same message quality.
    async fn check(resp: reqwest::Response) -> anyhow::Result<reqwest::Response> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let body = resp.text().await.unwrap_or_default();
        let msg = body.trim();
        anyhow::bail!(if msg.is_empty() {
            format!("dispatcher returned {status}")
        } else {
            msg.to_string()
        });
    }

    pub async fn get_json(&self, path: &str) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}{}", self.base, path);
        let resp = self.http.get(&url).send().await.with_context(|| format!("GET {url}"))?;
        Self::check(resp).await?.json().await.context("parse response")
    }

    pub async fn post_json(&self, path: &str, body: &serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}{}", self.base, path);
        let resp = self.http.post(&url).json(body).send().await.with_context(|| format!("POST {url}"))?;
        Self::check(resp).await?.json().await.context("parse response")
    }

    pub async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let url = format!("{}{}", self.base, path);
        let resp = self.http.delete(&url).send().await.with_context(|| format!("DELETE {url}"))?;
        Self::check(resp).await?;
        Ok(())
    }

    pub async fn post_empty(&self, path: &str) -> anyhow::Result<()> {
        let url = format!("{}{}", self.base, path);
        let resp = self.http.post(&url).send().await.with_context(|| format!("POST {url}"))?;
        Self::check(resp).await?;
        Ok(())
    }

    /// POST with a JSON body, discard the response. For endpoints
    /// that return 204 No Content (idempotent state mutations).
    pub async fn post_with_body(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let url = format!("{}{}", self.base, path);
        let resp = self.http.post(&url).json(body).send().await.with_context(|| format!("POST {url}"))?;
        Self::check(resp).await?;
        Ok(())
    }
}

