//! Thin HTTP client against the dispatcher.

use std::sync::{Arc, RwLock};

use anyhow::Context;
use weft_core::net::EmptyBody;

/// In an error's chain when the install answered with a failing status:
/// it was reached, and it (or something in front of it) said no. Reads
/// as the reason the answer gave (see `failure_text`).
#[derive(Debug)]
pub struct Refused {
    status: reqwest::StatusCode,
    message: String,
    /// What the refusal's marker header says, when it carries one.
    marked: Option<Marked>,
}

/// What a refusal's marker header says about why the install said no,
/// beyond its status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Marked {
    /// The upload is being completed by another caller right now
    /// ([`weft_core::storage::COMPLETING_HEADER`]): the file is about to
    /// land.
    StoreCompleting,
    /// Another ask of the project is starting its image builds right now
    /// ([`weft_core::builds::BUILD_BUSY_HEADER`]) and lets go within
    /// moments.
    BuildBusy,
}

impl Refused {
    pub fn marked(&self) -> Option<Marked> {
        self.marked
    }

    /// The status the answer carried.
    pub fn status(&self) -> reqwest::StatusCode {
        self.status
    }
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Refused {}

/// The refusal in `e`'s chain, when the install answered one.
pub fn refusal(e: &anyhow::Error) -> Option<&Refused> {
    e.chain().find_map(|cause| cause.downcast_ref::<Refused>())
}

/// Whether `e` is a request that failed on its way rather than being
/// answered: its connection could not be made ([`InstallUnreachable`]),
/// or broke before the answer was whole. Nothing said no, so the same
/// request may get through later; whether sending it again is safe is the
/// caller's call. A status, however bad, is an answer ([`Refused`]), and so
/// is a TLS failure (a certificate refused, a handshake that is not TLS):
/// the address or the setup is wrong, and waiting will not change it.
/// The one reading of a failed connection, for requests to the install and
/// to the store alike.
pub fn connection_failed(e: &anyhow::Error) -> bool {
    e.chain().filter_map(|cause| cause.downcast_ref::<reqwest::Error>()).any(failed_on_the_way)
}

/// [`connection_failed`] for one request's error.
fn failed_on_the_way(cause: &reqwest::Error) -> bool {
    cause.status().is_none()
        && !tls_failure(cause)
        && (cause.is_connect() || cause.is_timeout() || cause.is_request() || cause.is_body() || cause.is_decode())
}

/// Whether the connection was never made: refused, or not made in time.
fn no_connection(cause: &reqwest::Error) -> bool {
    !tls_failure(cause) && (cause.is_connect() || cause.is_timeout())
}

/// Whether `cause` is TLS refusing the other end, which is how rustls
/// reports a refused certificate or a bad handshake.
fn tls_failure(cause: &reqwest::Error) -> bool {
    let mut source = std::error::Error::source(cause);
    while let Some(err) = source {
        if err.downcast_ref::<std::io::Error>().is_some_and(|io| io.kind() == std::io::ErrorKind::InvalidData) {
            return true;
        }
        source = err.source();
    }
    false
}

/// In an error's chain when a request could not reach the install: the
/// connection could not be made, or was not made in time. Nothing was
/// refused, so what the person can do depends on where the install is
/// (`crate::progress::error_detail`): start the daemon when it is this
/// machine's, check the network when it is a remote one.
#[derive(Debug)]
pub struct InstallUnreachable {
    /// The install's address.
    pub url: String,
    /// The target that named it (`--on <target>`), when one did.
    pub target: Option<String>,
    /// Why the request did not get through.
    pub cause: reqwest::Error,
}

impl InstallUnreachable {
    /// Whether the install is this machine's: its address is a loopback
    /// one, which only the local daemon answers at.
    pub fn is_local(&self) -> bool {
        is_loopback_address(&self.url)
    }

    /// How the install is named to a person: its target and address, or
    /// its address alone.
    pub fn named(&self) -> String {
        match &self.target {
            Some(target) => format!("the install `{target}` ({})", self.url),
            None => format!("the install at {}", self.url),
        }
    }
}

/// Whether `url` names this machine (a loopback host).
fn is_loopback_address(url: &str) -> bool {
    let host = reqwest::Url::parse(url).ok().and_then(|url| url.host().map(|host| host.to_owned()));
    match host {
        Some(url::Host::Domain(name)) => name.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

impl std::fmt::Display for InstallUnreachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} did not answer", self.named())
    }
}

impl std::error::Error for InstallUnreachable {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

/// How long making a connection to the install may take before the
/// request fails as unreachable. An install answers a connection in well
/// under a second on this machine and in a few seconds on a cloud that
/// is waking up; a path that is down never does, and without a limit
/// the request would hang on it for minutes.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// How long a connection may sit with nothing on it before the system
/// checks that the other end is still there, and how often after that.
/// A request held open on a quiet connection (a verb waiting on the
/// install's answer) keeps its path alive this way, and one whose path
/// died fails within a minute or so instead of waiting for ever. There is
/// no limit on a request's whole length: some verbs wait as long as the
/// person's own work runs.
const KEEPALIVE_IDLE: std::time::Duration = std::time::Duration::from_secs(30);
const KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
const KEEPALIVE_RETRIES: u32 = 3;

#[derive(Clone)]
pub struct DispatcherClient {
    base: String,
    /// The target this install was named by (`--on <target>`), for an
    /// error that has to say which install did not answer.
    target: Option<String>,
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
        // The one connection setup every request to the install uses,
        // live-update streams included.
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_keepalive(KEEPALIVE_IDLE)
            .tcp_keepalive_interval(KEEPALIVE_INTERVAL)
            .tcp_keepalive_retries(KEEPALIVE_RETRIES)
            .build()
            .expect("an HTTP client with only timeouts set always builds");
        Self { base: base.into(), target: None, bearer: operator_key.map(|key| Arc::new(RwLock::new(key))), http }
    }

    /// The same client, naming the install by `target` when it does not
    /// answer.
    pub fn for_target(mut self, target: Option<&str>) -> Self {
        self.target = target.map(str::to_string);
        self
    }

    /// The same dispatcher, with every request carrying `token` as its
    /// bearer in place of the operator key: how the CLI speaks at a door
    /// that answers to a token rather than to the operator (the
    /// instance door).
    pub fn with_bearer(&self, token: &str) -> Self {
        Self { bearer: Some(Arc::new(RwLock::new(token.to_string()))), ..self.clone() }
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

    /// The live updates (SSE) at `path` on this install, each event as it
    /// comes. Opened like every other request (`send`, `check`), so it
    /// carries the key, uses the same connection setup, and a refusal or
    /// an install out of reach reads the same as anywhere else. Once open,
    /// the install was reached: a stream that breaks afterwards is never
    /// "unreachable", since the install itself ends a subscriber that fell
    /// behind by cutting the stream, which reads the same as a dropped
    /// connection.
    pub async fn event_stream(
        &self,
        path: &str,
    ) -> anyhow::Result<impl futures::Stream<Item = anyhow::Result<eventsource_stream::Event>> + use<>> {
        use eventsource_stream::{EventStreamError, Eventsource};
        use futures::StreamExt;
        let resp = self.check(self.send(reqwest::Method::GET, path, None).await?).await?;
        let url = resp.url().clone();
        Ok(resp.bytes_stream().eventsource().map(move |event| {
            event.map_err(|e| match e {
                EventStreamError::Transport(cause) => anyhow::Error::new(cause).context(format!("live updates from {url} stopped")),
                other => anyhow::Error::new(other).context(format!("read live updates from {url}")),
            })
        }))
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
        let resp = builder.send().await.map_err(|cause| self.failed(cause, format!("{method} {url}")))?;
        warn_once_on_version_note(&resp);
        Ok(resp)
    }

    /// The error a request or the reading of its answer failed with,
    /// under `doing`: [`InstallUnreachable`] when no connection could be
    /// made, the cause as it came otherwise.
    fn failed(&self, cause: reqwest::Error, doing: String) -> anyhow::Error {
        if no_connection(&cause) {
            return anyhow::Error::new(InstallUnreachable { url: self.base.clone(), target: self.target.clone(), cause }).context(doing);
        }
        anyhow::Error::new(cause).context(doing)
    }

    /// The whole body of `resp`, as it came.
    async fn body(&self, resp: reqwest::Response) -> anyhow::Result<Vec<u8>> {
        let url = resp.url().clone();
        let body = resp.bytes().await.map_err(|cause| self.failed(cause, format!("read the answer from {url}")))?;
        Ok(body.into())
    }

    /// The body of `resp` as text.
    async fn text(&self, resp: reqwest::Response) -> anyhow::Result<String> {
        Ok(String::from_utf8_lossy(&self.body(resp).await?).into_owned())
    }

    /// A successful answer's JSON body, or `check`'s error.
    async fn answer<T: serde::de::DeserializeOwned>(&self, resp: reqwest::Response) -> anyhow::Result<T> {
        let resp = self.check(resp).await?;
        serde_json::from_slice(&self.body(resp).await?).context("parse response")
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
    /// The error is a [`Refused`], so a caller can tell which status said no.
    async fn check(&self, resp: reqwest::Response) -> anyhow::Result<reqwest::Response> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let marked = match status {
            reqwest::StatusCode::CONFLICT if resp.headers().contains_key(weft_core::storage::COMPLETING_HEADER) => {
                Some(Marked::StoreCompleting)
            }
            reqwest::StatusCode::CONFLICT if resp.headers().contains_key(weft_core::builds::BUILD_BUSY_HEADER) => Some(Marked::BuildBusy),
            _ => None,
        };
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = self.text(resp).await.with_context(|| format!("read the body of a {status} answer"))?;
        let message = failure_text(status, content_type.as_deref(), &body);
        Err(anyhow::Error::new(Refused { status, message, marked }))
    }

    pub async fn get_json(&self, path: &str) -> anyhow::Result<serde_json::Value> {
        let resp = self.send(reqwest::Method::GET, path, None).await?;
        self.answer(resp).await
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
        Ok(Some(self.answer(resp).await?))
    }

    pub async fn post_json(&self, path: &str, body: &serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let resp = self.send(reqwest::Method::POST, path, Some(body)).await?;
        self.answer(resp).await
    }

    pub async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let resp = self.send(reqwest::Method::DELETE, path, None).await?;
        self.check(resp).await?;
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
        self.check(resp).await?;
        Ok(())
    }

    /// `delete_idempotent`, answering the body: `None` when it was already
    /// gone.
    pub async fn delete_idempotent_json(&self, path: &str) -> anyhow::Result<Option<serde_json::Value>> {
        let resp = self.send(reqwest::Method::DELETE, path, None).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND && resp.headers().contains_key("x-weft-not-found") {
            return Ok(None);
        }
        Ok(Some(self.answer(resp).await?))
    }

    pub async fn post_empty(&self, path: &str) -> anyhow::Result<()> {
        let resp = self.send(reqwest::Method::POST, path, None).await?;
        self.check(resp).await?;
        Ok(())
    }

    /// PUT with a JSON body, returning JSON.
    pub async fn put_json(&self, path: &str, body: &serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let resp = self.send(reqwest::Method::PUT, path, Some(body)).await?;
        self.answer(resp).await
    }

    /// PUT with a JSON body, discard the response (a 204).
    pub async fn put_with_body(&self, path: &str, body: &serde_json::Value) -> anyhow::Result<()> {
        let resp = self.send(reqwest::Method::PUT, path, Some(body)).await?;
        self.check(resp).await?;
        Ok(())
    }

    /// DELETE returning JSON (a prune answers what it removed).
    pub async fn delete_json(&self, path: &str) -> anyhow::Result<serde_json::Value> {
        let resp = self.send(reqwest::Method::DELETE, path, None).await?;
        self.answer(resp).await
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
            return Ok(Err(self.text(resp).await?.trim().to_string()));
        }
        Ok(Ok(self.answer(resp).await?))
    }

    /// POST with a JSON body, handing back the status and the body
    /// text whatever the status: for a caller that reads a structured
    /// refusal (the run endpoint's 422 carries a JSON `Refusal`) and
    /// treats every other failure as `check` would.
    pub async fn post_json_status(&self, path: &str, body: &serde_json::Value) -> anyhow::Result<(u16, String)> {
        let resp = self.send(reqwest::Method::POST, path, Some(body)).await?;
        let status = resp.status();
        Ok((status.as_u16(), self.text(resp).await?))
    }

    /// POST with a JSON body, handing back the success status and the
    /// body text: for a route whose successes mean different things (a
    /// build answers 200 once the version registered, 202 while its images
    /// build). A failure is `check`'s.
    pub async fn post_json_success(&self, path: &str, body: &serde_json::Value) -> anyhow::Result<(u16, String)> {
        let resp = self.check(self.send(reqwest::Method::POST, path, Some(body)).await?).await?;
        let status = resp.status();
        Ok((status.as_u16(), self.text(resp).await?))
    }

    /// DELETE carrying a JSON body and returning JSON (the storage
    /// files endpoint takes its key/prefix selector in the body).
    pub async fn delete_with_body(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let resp = self.send(reqwest::Method::DELETE, path, Some(body)).await?;
        self.answer(resp).await
    }

    /// POST with a JSON body, discard the response. For endpoints
    /// that return 204 No Content (idempotent state mutations).
    pub async fn post_with_body(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let resp = self.send(reqwest::Method::POST, path, Some(body)).await?;
        self.check(resp).await?;
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

/// A stand-in install for tests: every connection gets the same scripted
/// end, so a test can make each way a request fails happen for real.
#[cfg(test)]
pub(crate) mod fake {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Clone, Copy)]
    pub enum Then {
        /// Write these bytes as the answer, then close.
        Answer(&'static str),
        /// Read the request, then reset the connection without a word.
        Reset,
    }

    /// The address of a stand-in install doing `then` to every request.
    pub async fn install(then: Then) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let Ok((mut conn, _)) = listener.accept().await else { return };
                let mut request = vec![0u8; 64 * 1024];
                let _ = conn.read(&mut request).await;
                match then {
                    Then::Answer(raw) => {
                        let _ = conn.write_all(raw.as_bytes()).await;
                        let _ = conn.shutdown().await;
                    }
                    Then::Reset => {
                        #[allow(deprecated)]
                        conn.set_linger(Some(std::time::Duration::ZERO)).unwrap();
                    }
                }
            }
        });
        base
    }

    /// The address of a port nothing listens on: a connection there is refused.
    pub fn closed() -> String {
        format!("http://{}", std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use super::fake::{closed, install, Then};
    use super::{connection_failed, failure_text, is_loopback_address, refusal, refusal_text, DispatcherClient, InstallUnreachable};

    fn unreachable(e: &anyhow::Error) -> bool {
        e.chain().any(|cause| cause.is::<InstallUnreachable>())
    }

    /// A connection that cannot be made is the install not answering.
    #[tokio::test]
    async fn a_refused_connection_is_unreachable() {
        let e = DispatcherClient::new(closed(), None).get_json("/install").await.unwrap_err();
        assert!(unreachable(&e), "{e:#}");
        assert!(connection_failed(&e));
    }

    /// A connection that breaks once the request is on it, before the
    /// answer or halfway through it, failed on its way whichever verb was
    /// reading, but never as "the install is not there", since it was.
    #[tokio::test]
    async fn a_connection_that_breaks_mid_request_failed_on_its_way() {
        let reset = DispatcherClient::new(install(Then::Reset).await, None);
        let e = reset.get_json("/install").await.unwrap_err();
        assert!(connection_failed(&e) && !unreachable(&e), "reset before the answer: {e:#}");

        let cut = DispatcherClient::new(
            install(Then::Answer("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 100\r\n\r\n{\"state\"")).await,
            None,
        );
        let e = cut.get_json("/install").await.unwrap_err();
        assert!(connection_failed(&e) && !unreachable(&e), "cut halfway through the answer: {e:#}");
        let e = cut.post_json_status("/run", &serde_json::json!({})).await.unwrap_err();
        assert!(connection_failed(&e), "a status read never swallows its body: {e:#}");
    }

    /// A status the install answered is a refusal carrying it, never a
    /// connection that failed.
    #[tokio::test]
    async fn an_answered_status_is_a_refusal() {
        let forbidden = DispatcherClient::new(
            install(Then::Answer("HTTP/1.1 403 Forbidden\r\ncontent-length: 14\r\n\r\nnot your build")).await,
            None,
        );
        let e = forbidden.get_json("/projects/p/builds/b").await.unwrap_err();
        assert!(!connection_failed(&e), "{e:#}");
        assert_eq!(e.to_string(), "not your build");
        assert_eq!(refusal(&e).map(|r| r.status()), Some(reqwest::StatusCode::FORBIDDEN));
    }

    /// Live updates go through the same door: a refusal to open them is
    /// the install's refusal, and a stream that cannot connect is unreachable.
    #[tokio::test]
    async fn live_updates_fail_like_every_other_request() {
        let missing = DispatcherClient::new(
            install(Then::Answer("HTTP/1.1 404 Not Found\r\ncontent-length: 10\r\n\r\nno such id")).await,
            None,
        );
        let e = missing.event_stream("/events/project/p").await.err().unwrap();
        assert_eq!(e.to_string(), "no such id");
        let e = DispatcherClient::new(closed(), None).event_stream("/events/project/p").await.err().unwrap();
        assert!(unreachable(&e), "{e:#}");
        // Opened, then cut before its end (how the install ends a
        // subscriber that fell behind): reached, so never "unreachable".
        let cut = DispatcherClient::new(
            install(Then::Answer("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n9\r\ndata: 1\n\n\r\n")).await,
            None,
        );
        let mut stream = std::pin::pin!(cut.event_stream("/events/project/p").await.unwrap());
        assert_eq!(stream.next().await.unwrap().unwrap().data, "1");
        let e = stream.next().await.unwrap().err().unwrap();
        assert!(!unreachable(&e), "{e:#}");
    }

    /// Only a loopback address is this machine's daemon.
    #[test]
    fn only_a_loopback_address_is_local() {
        for (url, local) in [
            ("http://127.0.0.1:9000", true),
            ("http://localhost:9000", true),
            ("http://[::1]:9000", true),
            ("https://weft.example.com", false),
            ("http://10.0.0.4:9000", false),
            ("not a url", false),
        ] {
            assert_eq!(is_loopback_address(url), local, "{url}");
        }
    }

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
