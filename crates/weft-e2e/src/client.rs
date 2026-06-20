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

/// Thin HTTP client over the dispatcher's public API. Clone is cheap
/// (reqwest::Client is an Arc internally).
#[derive(Clone)]
pub struct Dispatcher {
    base: String,
    http: reqwest::Client,
}

impl Dispatcher {
    /// Build a client against `WEFT_DISPATCHER_URL` or the default. The inner
    /// reqwest client carries no global request timeout: live-caller and
    /// long-running operations are driven through here and a deadline belongs
    /// on the specific call (the rig's wait loops), not on every request.
    pub fn from_env() -> Result<Self> {
        let base = std::env::var("WEFT_DISPATCHER_URL")
            .unwrap_or_else(|_| DEFAULT_DISPATCHER_URL.to_string());
        let http = reqwest::Client::builder()
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            http,
        })
    }

    /// The base URL (no trailing slash), e.g. for building sub-URLs the rig
    /// needs to hit directly (the live-caller `/connect` path).
    pub fn base(&self) -> &str {
        &self.base
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base, path.trim_start_matches('/'))
    }

    /// GET `path`, deserialize the JSON body into `T`. Errors on non-2xx with
    /// the response body in the message.
    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.url(path);
        let resp = self
            .http
            .get(&url)
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
            .http
            .post(&url)
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
            .http
            .post(&url)
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
            .http
            .delete(&url)
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;
        read_ok(resp, "DELETE", &url).await?;
        Ok(())
    }

    /// GET `path` and return the raw status + body without requiring 2xx. Used
    /// where the rig must assert on a specific status code (e.g. a 404 after
    /// teardown) rather than treat non-2xx as an error.
    pub async fn get_raw(&self, path: &str) -> Result<(reqwest::StatusCode, String)> {
        let url = self.url(path);
        let resp = self
            .http
            .get(&url)
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
