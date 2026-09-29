//! Thin HTTP client against the dispatcher.

use anyhow::Context;

#[derive(Clone)]
pub struct DispatcherClient {
    base: String,
    /// The bearer every request carries: the operator key, when this
    /// person holds one for the install (see `crate::credentials`; the
    /// local install needs none), or a member token (`with_bearer`).
    operator_key: Option<String>,
    http: reqwest::Client,
}

impl DispatcherClient {
    pub fn new(base: impl Into<String>, operator_key: Option<String>) -> Self {
        Self { base: base.into(), operator_key, http: reqwest::Client::new() }
    }

    /// The same dispatcher, with every request carrying `token` as its
    /// bearer in place of the operator key: how the CLI speaks at a door
    /// that answers to a token rather than to the operator (the member
    /// door).
    pub fn with_bearer(&self, token: &str) -> Self {
        Self::new(self.base.clone(), Some(token.to_string()))
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    /// A live-updates (SSE) stream at `path` on this install, carrying
    /// the same bearer as every other request. The SSE library owns its
    /// own connection, so this is the one door out of this client that
    /// is not `send`, and it still cannot leave without the key.
    pub fn event_stream(&self, path: &str) -> anyhow::Result<eventsource_client::ClientBuilder> {
        let url = format!("{}{}", self.base, path);
        let builder = eventsource_client::ClientBuilder::for_url(&url).context("build sse client")?;
        match &self.operator_key {
            Some(key) => builder.header("Authorization", &format!("Bearer {key}")).context("build sse client"),
            None => Ok(builder),
        }
    }

    /// Every request to the install goes out here, so none can leave
    /// without the key: the inner `reqwest::Client` is touched nowhere
    /// else, and every public verb is a thin reading of this response.
    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> anyhow::Result<reqwest::Response> {
        let url = format!("{}{}", self.base, path);
        let mut builder = self.http.request(method.clone(), &url);
        if let Some(key) = &self.operator_key {
            builder = builder.bearer_auth(key);
        }
        if let Some(body) = body {
            builder = builder.json(body);
        }
        builder.send().await.with_context(|| format!("{method} {url}"))
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
        let resp = self.send(reqwest::Method::GET, path, None).await?;
        Self::check(resp).await?.json().await.context("parse response")
    }

    /// GET where "no such thing" is an answer, not an error: `None` on
    /// a 404 carrying the dispatcher's `x-weft-not-found` marker (the
    /// resource genuinely does not exist for this caller), `Some` on
    /// success. A headerless 404 (a version-skewed dispatcher missing
    /// the route) still fails loudly through `check`, so a missing
    /// route can never read as "not registered yet".
    pub async fn get_json_if_found(&self, path: &str) -> anyhow::Result<Option<serde_json::Value>> {
        let resp = self.send(reqwest::Method::GET, path, None).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND
            && resp.headers().contains_key("x-weft-not-found")
        {
            return Ok(None);
        }
        Ok(Some(Self::check(resp).await?.json().await.context("parse response")?))
    }

    pub async fn post_json(&self, path: &str, body: &serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let resp = self.send(reqwest::Method::POST, path, Some(body)).await?;
        Self::check(resp).await?.json().await.context("parse response")
    }

    pub async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let resp = self.send(reqwest::Method::DELETE, path, None).await?;
        Self::check(resp).await?;
        Ok(())
    }

    /// DELETE treating "already gone" as success: a 404 carrying the
    /// dispatcher's `x-weft-not-found` marker means the resource genuinely does
    /// not exist (as opposed to a wrong URL), which for a delete IS the desired
    /// end state. Makes a retried delete (whose first response was lost) land on
    /// success instead of a confusing 404. Any other error stays loud.
    pub async fn delete_idempotent(&self, path: &str) -> anyhow::Result<()> {
        let resp = self.send(reqwest::Method::DELETE, path, None).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND
            && resp.headers().contains_key("x-weft-not-found")
        {
            return Ok(());
        }
        Self::check(resp).await?;
        Ok(())
    }

    pub async fn post_empty(&self, path: &str) -> anyhow::Result<()> {
        let resp = self.send(reqwest::Method::POST, path, None).await?;
        Self::check(resp).await?;
        Ok(())
    }

    /// PUT with a JSON body, returning JSON.
    pub async fn put_json(&self, path: &str, body: &serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let resp = self.send(reqwest::Method::PUT, path, Some(body)).await?;
        Self::check(resp).await?.json().await.context("parse response")
    }

    /// PUT with a JSON body, discard the response (a 204).
    pub async fn put_with_body(&self, path: &str, body: &serde_json::Value) -> anyhow::Result<()> {
        let resp = self.send(reqwest::Method::PUT, path, Some(body)).await?;
        Self::check(resp).await?;
        Ok(())
    }

    /// DELETE returning JSON (a prune answers what it removed).
    pub async fn delete_json(&self, path: &str) -> anyhow::Result<serde_json::Value> {
        let resp = self.send(reqwest::Method::DELETE, path, None).await?;
        Self::check(resp).await?.json().await.context("parse response")
    }

    /// POST with a JSON body, returning JSON, or `Ok(Err(refusal))` when
    /// the dispatcher asks for the person's trigger-deactivation choice
    /// (a 428 carrying `TRIGGER_CHOICE_REQUIRED_HEADER`; the other 428,
    /// infra that is not running, stays an error), so the caller can ask
    /// for the choice or pass the refusal on. Every other failure is
    /// `check`'s.
    pub async fn post_json_or_choice_needed(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> anyhow::Result<Result<serde_json::Value, String>> {
        let resp = self.send(reqwest::Method::POST, path, Some(body)).await?;
        if resp.status() == reqwest::StatusCode::PRECONDITION_REQUIRED
            && resp.headers().contains_key(weft_core::TRIGGER_CHOICE_REQUIRED_HEADER)
        {
            return Ok(Err(resp.text().await.unwrap_or_default().trim().to_string()));
        }
        Ok(Ok(Self::check(resp).await?.json().await.context("parse response")?))
    }

    /// POST with a JSON body, handing back the status and the body
    /// text whatever the status: for a caller that reads a structured
    /// refusal (the run endpoint's 422 carries a JSON `Refusal`) and
    /// treats every other failure as `check` would.
    pub async fn post_json_status(&self, path: &str, body: &serde_json::Value) -> anyhow::Result<(u16, String)> {
        let resp = self.send(reqwest::Method::POST, path, Some(body)).await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        Ok((status.as_u16(), text))
    }

    /// DELETE carrying a JSON body and returning JSON (the storage
    /// files endpoint takes its key/prefix selector in the body).
    pub async fn delete_with_body(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let resp = self.send(reqwest::Method::DELETE, path, Some(body)).await?;
        Self::check(resp).await?.json().await.context("parse response")
    }

    /// POST with a JSON body, discard the response. For endpoints
    /// that return 204 No Content (idempotent state mutations).
    pub async fn post_with_body(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let resp = self.send(reqwest::Method::POST, path, Some(body)).await?;
        Self::check(resp).await?;
        Ok(())
    }
}

