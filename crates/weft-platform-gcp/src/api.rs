//! Calling Google's REST APIs as the runtime's own service account.
//!
//! Every Google API weft drives (Cloud Run, Compute Engine, Cloud Build,
//! Cloud Tasks, Cloud Storage, IAM) is JSON over HTTPS with an access
//! token, and the slow ones answer with an operation to wait on. This is
//! that, once.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use serde_json::Value;

use crate::metadata::MetadataTokens;

/// How often an operation still running is looked at again.
const OPERATION_POLL: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct Google {
    http: reqwest::Client,
    tokens: Arc<MetadataTokens>,
}

/// What an API answered when it answered with an error.
#[derive(Debug)]
pub struct ApiError {
    pub status: u16,
    pub body: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Google answered {}: {}", self.status, self.body.trim())
    }
}

impl std::error::Error for ApiError {}

/// Whether `e` is Google answering `status`.
pub fn is_status(e: &anyhow::Error, status: u16) -> bool {
    e.chain().any(|c| c.downcast_ref::<ApiError>().is_some_and(|a| a.status == status))
}

impl Google {
    pub fn new(tokens: Arc<MetadataTokens>) -> Self {
        Self {
            http: reqwest::Client::builder().timeout(Duration::from_secs(60)).build().expect("reqwest client"),
            tokens,
        }
    }

    pub fn tokens(&self) -> &Arc<MetadataTokens> {
        &self.tokens
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Send `req` as this process's account, answering Google's response
    /// whatever its status: for a caller that reads an answer that is not
    /// JSON (Cloud Storage's objects and XML) or treats a status itself.
    pub async fn send_raw(&self, req: reqwest::RequestBuilder) -> anyhow::Result<reqwest::Response> {
        let token = self.tokens.access_token().await?;
        Ok(req.bearer_auth(token).send().await?)
    }

    /// `resp` when it is a success; its status and Google's words, as an
    /// [`ApiError`], otherwise.
    pub async fn success(resp: reqwest::Response) -> anyhow::Result<reqwest::Response> {
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status().as_u16();
        Err(ApiError { status, body: resp.text().await.unwrap_or_default() }.into())
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> anyhow::Result<Value> {
        let resp = Self::success(self.send_raw(req).await?).await?;
        let body = resp.text().await?;
        if body.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&body).with_context(|| format!("Google answered something that is not JSON: {body}"))
    }

    pub async fn get(&self, url: &str) -> anyhow::Result<Value> {
        self.send(self.http.get(url)).await.with_context(|| format!("GET {url}"))
    }

    /// `GET` with `query` as the URL's query string, encoded.
    pub async fn get_query(&self, url: &str, query: &[(&str, String)]) -> anyhow::Result<Value> {
        self.send(self.http.get(url).query(query)).await.with_context(|| format!("GET {url}"))
    }

    /// `GET`, with a 404 read as "not there".
    pub async fn get_opt(&self, url: &str) -> anyhow::Result<Option<Value>> {
        match self.get(url).await {
            Ok(v) => Ok(Some(v)),
            Err(e) if is_status(&e, 404) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn post(&self, url: &str, body: &Value) -> anyhow::Result<Value> {
        self.send(self.http.post(url).json(body)).await.with_context(|| format!("POST {url}"))
    }

    pub async fn patch(&self, url: &str, body: &Value) -> anyhow::Result<Value> {
        self.send(self.http.patch(url).json(body)).await.with_context(|| format!("PATCH {url}"))
    }

    pub async fn put_bytes(&self, url: &str, content_type: &str, body: Vec<u8>) -> anyhow::Result<Value> {
        self.send(self.http.post(url).header("content-type", content_type).body(body))
            .await
            .with_context(|| format!("upload to {url}"))
    }

    /// `DELETE`, with a 404 read as "already gone".
    pub async fn delete(&self, url: &str) -> anyhow::Result<Option<Value>> {
        match self.send(self.http.delete(url)).await {
            Ok(v) => Ok(Some(v)),
            Err(e) if is_status(&e, 404) => Ok(None),
            Err(e) => Err(e.context(format!("DELETE {url}"))),
        }
    }

    /// Wait for a long-running operation to end, failing with its error.
    /// `op` is what the call that started it answered; `base` is the
    /// API's root the operation's name is under.
    pub async fn wait(&self, base: &str, op: Value) -> anyhow::Result<Value> {
        let mut op = op;
        loop {
            let done = op.get("done").and_then(Value::as_bool).unwrap_or(false)
                || op.get("status").and_then(Value::as_str) == Some("DONE");
            if done {
                if let Some(err) = op.get("error").filter(|e| !e.is_null()) {
                    anyhow::bail!("the operation failed: {err}");
                }
                return Ok(op);
            }
            let url = match (op.get("selfLink").and_then(Value::as_str), op.get("name").and_then(Value::as_str)) {
                (Some(link), _) => link.to_string(),
                (None, Some(name)) => format!("{}/{name}", base.trim_end_matches('/')),
                (None, None) => anyhow::bail!("an operation with no name to follow: {op}"),
            };
            tokio::time::sleep(OPERATION_POLL).await;
            op = self.get(&url).await?;
        }
    }
}
