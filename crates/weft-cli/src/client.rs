//! Thin HTTP client against the dispatcher.

use std::sync::{Arc, RwLock};

use anyhow::Context;
use weft_core::net::EmptyBody;

/// In an error's chain when the store refused because the upload is being
/// completed by another caller right now
/// ([`weft_core::storage::COMPLETING_HEADER`]): the file is about to land.
#[derive(Debug)]
pub struct StoreCompleting;

impl std::fmt::Display for StoreCompleting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the upload is being completed by another caller")
    }
}

impl std::error::Error for StoreCompleting {}

#[derive(Clone)]
pub struct DispatcherClient {
    base: String,
    /// The bearer every request carries: the operator key, when this
    /// person holds one for the install (see `crate::credentials`; the
    /// local install needs none), or an instance token (`with_bearer`).
    /// Shared by every clone, so `replace_bearer` reaches all of them:
    /// that is how a command outlives one instance token.
    bearer: Option<Arc<RwLock<String>>>,
    http: reqwest::Client,
}

impl DispatcherClient {
    pub fn new(base: impl Into<String>, operator_key: Option<String>) -> Self {
        Self { base: base.into(), bearer: operator_key.map(|key| Arc::new(RwLock::new(key))), http: reqwest::Client::new() }
    }

    /// The same dispatcher, with every request carrying `token` as its
    /// bearer in place of the operator key: how the CLI speaks at a door
    /// that answers to a token rather than to the operator (the
    /// instance door).
    pub fn with_bearer(&self, token: &str) -> Self {
        Self::new(self.base.clone(), Some(token.to_string()))
    }

    /// Swap the bearer this client and every clone of it carry from the
    /// next request on (a renewed instance token). A client built with
    /// no bearer has nothing to swap, which is a caller bug.
    pub fn replace_bearer(&self, token: &str) -> anyhow::Result<()> {
        let cell = self.bearer.as_ref().context("this client carries no bearer to replace")?;
        *cell.write().map_err(|_| anyhow::anyhow!("the bearer lock was poisoned"))? = token.to_string();
        Ok(())
    }

    fn current_bearer(&self) -> anyhow::Result<Option<String>> {
        self.bearer
            .as_ref()
            .map(|cell| cell.read().map(|key| key.clone()).map_err(|_| anyhow::anyhow!("the bearer lock was poisoned")))
            .transpose()
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
        let mut builder = eventsource_client::ClientBuilder::for_url(&url).context("build sse client")?;
        if let Some(commit) = CLI_COMMIT {
            builder = builder.header(weft_core::install::CLI_COMMIT_HEADER, commit).context("build sse client")?;
        }
        match self.current_bearer()? {
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
        if let Some(key) = self.current_bearer()? {
            builder = builder.bearer_auth(key);
        }
        if let Some(commit) = CLI_COMMIT {
            builder = builder.header(weft_core::install::CLI_COMMIT_HEADER, commit);
        }
        builder = match body {
            Some(body) => builder.json(body),
            None if [reqwest::Method::POST, reqwest::Method::PUT, reqwest::Method::PATCH].contains(&method) => builder.empty_body(),
            None => builder,
        };
        let resp = builder.send().await.with_context(|| format!("{method} {url}"))?;
        warn_once_on_version_note(&resp);
        Ok(resp)
    }

    /// The ONE place any client method turns an HTTP failure into an
    /// error. On non-2xx, surface the dispatcher's BODY text (its
    /// handlers return the reason as the body, e.g. "project is
    /// already activating; wait or weft deactivate") rather than
    /// reqwest's stock "HTTP status client error (...) for url (...)"
    /// line, which buries the reason behind URL noise. Falls back to
    /// the bare status only when the body is empty, and names an HTML
    /// page rather than printing it (see `failure_text`). Every verb
    /// routes through this so they all get the same message quality.
    async fn check(resp: reqwest::Response) -> anyhow::Result<reqwest::Response> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let completing = status == reqwest::StatusCode::CONFLICT
            && resp.headers().contains_key(weft_core::storage::COMPLETING_HEADER);
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = resp.text().await.with_context(|| format!("read the body of a {status} answer"))?;
        let msg = failure_text(status, content_type.as_deref(), &body);
        if completing {
            return Err(anyhow::Error::new(StoreCompleting).context(msg));
        }
        anyhow::bail!(msg)
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

    /// `delete_idempotent`, answering the body: `None` when it was already
    /// gone.
    pub async fn delete_idempotent_json(&self, path: &str) -> anyhow::Result<Option<serde_json::Value>> {
        let resp = self.send(reqwest::Method::DELETE, path, None).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND && resp.headers().contains_key("x-weft-not-found") {
            return Ok(None);
        }
        Ok(Some(Self::check(resp).await?.json().await.context("parse response")?))
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

/// The commit this CLI was built from (`build.rs`), when the build knew it.
const CLI_COMMIT: Option<&str> = option_env!("WEFT_CLI_COMMIT");

/// Print the install's version note (the CLI and the install run
/// different weft commits) on stderr, once per process however many
/// answers carry it. Never on stdout, which `--json` keeps for its output.
fn warn_once_on_version_note(resp: &reqwest::Response) {
    static WARNED: std::sync::Once = std::sync::Once::new();
    let Some(note) = resp.headers().get(weft_core::install::VERSION_NOTE_HEADER) else {
        return;
    };
    let note = String::from_utf8_lossy(note.as_bytes()).into_owned();
    WARNED.call_once(|| eprintln!("warning: {note}"));
}

/// The error a failed answer turns into. An HTML page did not come from
/// weft (which answers in text or JSON) but from something in front of it,
/// a load balancer or a proxy, so it is named with its title rather than
/// printed whole. Any other body is the refusal weft wrote.
fn failure_text(status: reqwest::StatusCode, content_type: Option<&str>, body: &str) -> String {
    let html = content_type.is_some_and(|t| t.trim_start().to_ascii_lowercase().starts_with("text/html"));
    if html {
        let title = html_title(body).map(|t| format!(" (\"{t}\")")).unwrap_or_default();
        return format!(
            "the install's address answered {status} with an HTML page{title}, which comes from something in front of weft, not from weft itself"
        );
    }
    refusal_text(body).unwrap_or_else(|| format!("dispatcher returned {status}"))
}

/// The text of an HTML page's `<title>`, with its whitespace folded;
/// `None` when it has none or it is empty.
fn html_title(body: &str) -> Option<String> {
    let lower = body.to_ascii_lowercase();
    let open = lower.find("<title")?;
    let start = open + lower[open..].find('>')? + 1;
    let end = start + lower[start..].find("</title")?;
    let title = body[start..end].split_whitespace().collect::<Vec<_>>().join(" ");
    (!title.is_empty()).then_some(title)
}

/// What a refusal's body says, as lines a person reads: a structured
/// refusal (`{"errors": [...]}`, the shape every validation answers
/// with) one error per line, any other body as it came. `None` when the
/// body is empty.
pub(crate) fn refusal_text(body: &str) -> Option<String> {
    let body = body.trim();
    if body.is_empty() {
        return None;
    }
    match serde_json::from_str::<weft_core::run_spec::Refusal>(body) {
        Ok(refusal) if !refusal.is_empty() => Some(refusal.to_string()),
        _ => Some(body.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{failure_text, refusal_text};

    #[test]
    fn a_structured_refusal_reads_as_its_errors() {
        assert_eq!(
            refusal_text(r#"{"errors":["'keys' has no connection picked","'db' is not running"]}"#).as_deref(),
            Some("'keys' has no connection picked\n'db' is not running")
        );
        assert_eq!(refusal_text("project is already activating").as_deref(), Some("project is already activating"));
        assert_eq!(refusal_text(r#"{"errors":[]}"#).as_deref(), Some(r#"{"errors":[]}"#));
        assert_eq!(refusal_text("  \n"), None);
    }

    #[test]
    fn an_html_page_is_named_by_its_title_and_never_printed() {
        let length_required = reqwest::StatusCode::LENGTH_REQUIRED;
        let page = "<!DOCTYPE html><html><head><TITLE>Error 411 (Length Required)!!1</TITLE><style>body{}</style></head><body><p>long page</p></body></html>";
        assert_eq!(
            failure_text(length_required, Some("text/html; charset=UTF-8"), page),
            "the install's address answered 411 Length Required with an HTML page (\"Error 411 (Length Required)!!1\"), \
             which comes from something in front of weft, not from weft itself"
        );
        assert_eq!(
            failure_text(reqwest::StatusCode::BAD_GATEWAY, Some("text/html"), "<html><title>\n </title>oops</html>"),
            "the install's address answered 502 Bad Gateway with an HTML page, which comes from something in front of weft, not from weft itself"
        );
        assert_eq!(
            failure_text(reqwest::StatusCode::CONFLICT, Some("text/plain; charset=utf-8"), "project is already activating"),
            "project is already activating"
        );
        assert_eq!(failure_text(reqwest::StatusCode::NOT_FOUND, None, ""), "dispatcher returned 404 Not Found");
    }
}
