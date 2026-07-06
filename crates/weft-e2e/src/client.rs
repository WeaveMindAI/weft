//! The two ways the rig talks to a running weft system, both exactly as a
//! real user / outside party would:
//!
//!   - [`Dispatcher`]: a thin HTTP client over the dispatcher's public API
//!     (port 9999 by default). Used for reads (execution status / replay,
//!     storage, infra status) and outside-world pokes (firing signals, the
//!     live-caller handshake). This is the same contract the CLI and the
//!     VS Code extension speak.
//!   - [`cli`]: shells out to the installed `weft` binary in a project
//!     directory. Used for the lifecycle that must go through the real build
//!     path (build, register, activate, resync, infra, rm), because driving
//!     those by hand against the HTTP API would skip the compile + gate the
//!     CLI performs, which is precisely the behavior a Layer-4 test wants to
//!     exercise.
//!
//! Both fail loud: a non-2xx HTTP response or a non-zero CLI exit becomes an
//! `Err` carrying the body / stderr, never a silent empty result.

use std::path::Path;
use std::process::Output;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::de::DeserializeOwned;
use serde_json::Value;

/// Default dispatcher API base. The daemon port-forwards the dispatcher to
/// `127.0.0.1:9999`; override with `WEFT_DISPATCHER_URL` to match a
/// non-default `WEFT_HTTP_PORT`.
pub const DEFAULT_DISPATCHER_URL: &str = "http://127.0.0.1:9999";

/// Mints the bearer a request authenticates with. With no auth, the default is
/// `None` (no `Authorization` header). A harness that needs a token injects a
/// provider that signs a fresh Ed25519 JWT for the tenant under test: every call
/// carries a signed token, no CLI. Sync (JWT signing is sync) and called per
/// request so a short-lived token is always fresh. `Send + Sync` so a
/// `Dispatcher` stays cheaply cloneable + usable across the rig's spawned tasks.
pub trait AuthProvider: Send + Sync {
    /// The `Authorization` header VALUE for the next request (e.g.
    /// `"Bearer <jwt>"`), or `None` for an unauthenticated call.
    fn authorization(&self) -> Option<String>;
}

/// Thin HTTP client over the dispatcher's public API. Clone is cheap
/// (reqwest::Client is an Arc internally; the auth provider is an Arc).
#[derive(Clone)]
pub struct Dispatcher {
    base: String,
    http: reqwest::Client,
    /// Mints the per-request bearer. `None` => unauthenticated; `Some` => a
    /// harness signs a token per call.
    auth: Option<std::sync::Arc<dyn AuthProvider>>,
}

impl Dispatcher {
    /// Build an UNAUTHENTICATED client against `WEFT_DISPATCHER_URL` or the
    /// default (no login). The inner reqwest client carries no global request
    /// timeout: live-caller and long-running operations are driven through here
    /// and a deadline belongs on the specific call (the rig's wait loops), not on
    /// every request.
    pub fn from_env() -> Result<Self> {
        let base = std::env::var("WEFT_DISPATCHER_URL")
            .unwrap_or_else(|_| DEFAULT_DISPATCHER_URL.to_string());
        let http = reqwest::Client::builder()
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            http,
            auth: None,
        })
    }

    /// A clone of this client that authenticates every request via `auth`. A
    /// harness that needs tokens builds the base from env, then attaches a
    /// per-tenant signer; unauthenticated tests never call this.
    pub fn with_auth(mut self, auth: std::sync::Arc<dyn AuthProvider>) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Apply the auth header (if any) to a request builder. Every verb routes
    /// through here so an authed client signs uniformly and an unauthed one
    /// adds nothing.
    fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.auth.as_ref().and_then(|a| a.authorization()) {
            Some(value) => req.header("authorization", value),
            None => req,
        }
    }

    /// The base URL (no trailing slash), e.g. for building sub-URLs the rig
    /// needs to hit directly (the live-caller `/connect` path).
    pub fn base(&self) -> &str {
        &self.base
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base, path.trim_start_matches('/'))
    }

    /// GET `path` presenting a SIGNAL TOKEN as the bearer credential
    /// (`Authorization: Bearer <token>`), overriding the client's own auth for
    /// this one request: the token-scoped signal routes authenticate by the
    /// token itself, never by the tenant credential.
    pub async fn get_json_bearer<T: DeserializeOwned>(
        &self,
        path: &str,
        bearer: &str,
    ) -> Result<T> {
        let url = self.url(path);
        let resp = self
            .http
            .get(&url)
            .header("authorization", format!("Bearer {bearer}"))
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let body = read_ok(resp, "GET", &url).await?;
        serde_json::from_str(&body).with_context(|| format!("GET {url}: decode body: {body}"))
    }

    /// GET `path`, deserialize the JSON body into `T`. Errors on non-2xx with
    /// the response body in the message.
    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.url(path);
        let resp = self
            .authed(self.http.get(&url))
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let body = read_ok(resp, "GET", &url).await?;
        serde_json::from_str(&body).with_context(|| format!("GET {url}: decode body: {body}"))
    }

    /// POST `body` as JSON to `path`, deserialize the JSON response into `T`.
    pub async fn post_json<T: DeserializeOwned>(&self, path: &str, body: &Value) -> Result<T> {
        let url = self.url(path);
        let resp = self
            .authed(self.http.post(&url))
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let text = read_ok(resp, "POST", &url).await?;
        // 204 / empty bodies decode to unit-like T via serde_json::from_str on
        // "null"; callers that expect no body use `post_empty` instead.
        serde_json::from_str(&text).with_context(|| format!("POST {url}: decode body: {text}"))
    }

    /// POST `body` as JSON to `path`, expecting no useful response body
    /// (204 or an ignorable payload). Errors on non-2xx.
    pub async fn post_empty(&self, path: &str, body: &Value) -> Result<()> {
        let url = self.url(path);
        let resp = self
            .authed(self.http.post(&url))
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        read_ok(resp, "POST", &url).await?;
        Ok(())
    }

    /// DELETE `path`, requiring 2xx. Used by the suite's startup sweep to
    /// remove leftover projects via the real `DELETE /projects/{id}` path
    /// (the same forced cleanup `weft rm --force` performs).
    pub async fn delete(&self, path: &str) -> Result<()> {
        let url = self.url(path);
        let resp = self
            .authed(self.http.delete(&url))
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;
        read_ok(resp, "DELETE", &url).await?;
        Ok(())
    }

    /// POST `body` as JSON to `path` and return the raw status + body without
    /// requiring 2xx. Used by negative transition tests that assert a verb is
    /// REJECTED with a specific status (the reconciliation table's REJ cells),
    /// where a 2xx would be the failure.
    pub async fn post_raw(&self, path: &str, body: &Value) -> Result<(reqwest::StatusCode, String)> {
        let url = self.url(path);
        let resp = self
            .authed(self.http.post(&url))
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        Ok((status, text))
    }

    /// GET `path` and return the raw status + body without requiring 2xx. Used
    /// where the rig must assert on a specific status code (e.g. a 404 after
    /// teardown) rather than treat non-2xx as an error.
    pub async fn get_raw(&self, path: &str) -> Result<(reqwest::StatusCode, String)> {
        let url = self.url(path);
        let resp = self
            .authed(self.http.get(&url))
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        Ok((status, body))
    }

    /// GET an ABSOLUTE url (not under the dispatcher base) and return raw
    /// status + body. Used for the live-caller per-pod gateway URL and for
    /// storage capability URLs, which point at the gateway / storage box, not
    /// the dispatcher.
    pub async fn get_abs_raw(&self, url: &str) -> Result<(reqwest::StatusCode, Vec<u8>)> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        let bytes = resp.bytes().await.unwrap_or_default().to_vec();
        Ok((status, bytes))
    }
}

/// Read a response, returning its body text on 2xx and an `Err` carrying the
/// status + body otherwise. The single place every HTTP call funnels through
/// so the "fail loud with the server's message" rule holds uniformly.
async fn read_ok(resp: reqwest::Response, verb: &str, url: &str) -> Result<String> {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if status.is_success() {
        Ok(body)
    } else {
        bail!("{verb} {url} -> HTTP {status}: {body}")
    }
}

/// Run the installed `weft` CLI in `dir`, returning the captured output. The
/// CLI inherits the rig's environment (so `WEFT_DISPATCHER_URL` and friends
/// flow through). Fails loud if the binary cannot be spawned; the EXIT STATUS
/// is left to the caller (some verbs are expected to fail in negative tests).
pub async fn cli(dir: &Path, args: &[&str]) -> Result<CliOutput> {
    let mut cmd = tokio::process::Command::new("weft");
    cmd.current_dir(dir);
    cmd.args(args);
    // The rig is non-interactive by construction: a verb that wants a
    // prompt must receive its answer via flags. With an inherited stdin
    // (the developer's terminal) a prompt would READ from the terminal
    // while its text went to the captured pipe: an invisible hang. A
    // null stdin makes the CLI's prompt guard bail loudly instead.
    cmd.stdin(std::process::Stdio::null());
    let out: Output = cmd
        .output()
        .await
        .with_context(|| format!("spawn `weft {}` in {}", args.join(" "), dir.display()))?;
    Ok(CliOutput {
        status: out.status.code(),
        success: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        invocation: format!("weft {}", args.join(" ")),
    })
}

/// Run `weft` in `dir` and require a zero exit, returning stdout. Errors carry
/// the invocation, exit code, and BOTH streams so a failure is fully legible.
pub async fn cli_ok(dir: &Path, args: &[&str]) -> Result<String> {
    let out = cli(dir, args).await?;
    if out.success {
        Ok(out.stdout)
    } else {
        bail!(
            "`{}` failed (exit {:?})\n--- stdout ---\n{}\n--- stderr ---\n{}",
            out.invocation,
            out.status,
            out.stdout,
            out.stderr
        )
    }
}

/// Captured result of one `weft` invocation.
pub struct CliOutput {
    pub status: Option<i32>,
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    pub invocation: String,
}

/// Poll `f` until it returns `Ok(Some(v))`, yielding `v`; retry on `Ok(None)`;
/// propagate `Err` immediately. Times out after `deadline` with a message that
/// names `what`. This is the rig's single wait primitive: every "wait until the
/// system reaches state X" loop goes through here so the timeout / poll cadence
/// is consistent and a hang always surfaces as a clear error rather than a hung
/// test the harness has to kill. The deadline is for INTERNAL transitions the
/// rig controls (build done, pod running, run terminal), which legitimately
/// bound; it is not imposed on user-controlled long operations.
pub async fn poll_until<T, F, Fut>(
    what: &str,
    deadline: Duration,
    interval: Duration,
    mut f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>>>,
{
    let start = std::time::Instant::now();
    loop {
        if let Some(v) = f().await? {
            return Ok(v);
        }
        if start.elapsed() >= deadline {
            bail!(
                "timed out after {:?} waiting for: {what}",
                deadline
            );
        }
        tokio::time::sleep(interval).await;
    }
}
