//! The two ways the rig talks to a running weft system, both exactly as a
//! real user / outside party would:
//!
//!   - [`Dispatcher`]: a thin HTTP client over the dispatcher's public API
//!     (where the install under test, or a test's cell, answers). Used for
//!     reads (run status / replay, storage, infra status) and
//!     outside-world pokes (firing signals). This is the same contract the
//!     CLI and the VS Code extension speak.
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

/// Thin HTTP client over the dispatcher's public API. Clone is cheap
/// (reqwest::Client is an Arc internally).
#[derive(Clone)]
pub struct Dispatcher {
    base: String,
    http: reqwest::Client,
    /// The operator key every request sends (`Authorization: Bearer
    /// <key>`): the install's own key for an install on this machine
    /// ([`Dispatcher::for_install`]), or one a test holds for another (a
    /// cloud install, [`Dispatcher::with_key`]).
    key: String,
    /// Which install on this machine this dispatcher belongs to: the
    /// install under test, or a test cell ([`crate::cell::Cell`]). What reaches
    /// behind the API (the platform layer, the `weft` CLI) follows it to
    /// the right files and database.
    install: weft_core::infra::Install,
}

impl Dispatcher {
    /// Build a client for the install under test
    /// ([`crate::ensure::install_under_test`]), at the address its
    /// `ports.json` gives (the CLI's own resolution). The address always
    /// comes from that install, so one test never drives two installs. The
    /// inner reqwest client carries no global request timeout: live-caller
    /// and long-running operations are driven through here and a deadline
    /// belongs on the specific call (the rig's wait loops), not on every
    /// request.
    pub fn under_test() -> Result<Self> {
        let install = crate::ensure::install_under_test()?;
        let base = weft_core::ports::local_public_url_of(&install).map_err(anyhow::Error::msg)?;
        Self::for_install(&base, install)
    }

    /// A client for the dispatcher of `install`, answering at `base`, each
    /// call carrying the install's own key (read from its secrets, the way
    /// the CLI on this machine reads it).
    pub fn for_install(base: &str, install: weft_core::infra::Install) -> Result<Self> {
        let http = weft_core::net::http_client()
            .build()
            .context("build reqwest client")?;
        let key = install.secret(weft_core::signal_token::INSTALL_KEY_SECRET).map_err(anyhow::Error::msg)?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            http,
            key,
            install,
        })
    }

    /// The install this dispatcher belongs to.
    pub fn install(&self) -> &weft_core::infra::Install {
        &self.install
    }

    /// A clone of this client that sends operator key `key` instead (one
    /// for a cloud install).
    pub fn with_key(mut self, key: String) -> Self {
        self.key = key;
        self
    }

    /// Apply the auth header to a request builder. Every verb routes
    /// through here, so every call is signed the same way.
    fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        req.header("authorization", format!("Bearer {}", self.key))
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

    /// DELETE `path`, requiring 2xx. Used by the cleanup
    /// (`crate::cleanup`) to remove the projects failed runs kept via the
    /// real `DELETE /projects/{id}` path (the same forced cleanup
    /// `weft rm --force` performs).
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

    /// GET `path` presenting a SIGNAL TOKEN as bearer, returning the raw
    /// status + body without requiring 2xx. The pair to `get_json_bearer`,
    /// for the doors whose REFUSAL is the thing under test (a token that was
    /// never granted a node's display).
    pub async fn get_raw_bearer(
        &self,
        path: &str,
        bearer: &str,
    ) -> Result<(reqwest::StatusCode, String)> {
        let url = self.url(path);
        let resp = self
            .http
            .get(&url)
            .header("authorization", format!("Bearer {bearer}"))
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        let body = resp.text().await.with_context(|| format!("GET {url}: read the body"))?;
        Ok((status, body))
    }

    /// POST `path` presenting a SIGNAL TOKEN as bearer, returning the raw
    /// status + body without requiring 2xx. The pair to `get_raw_bearer`, for
    /// a door whose REFUSAL is the thing under test (pressing a button on a
    /// display that carries none).
    pub(crate) async fn post_raw_bearer(
        &self,
        path: &str,
        bearer: &str,
        body: &serde_json::Value,
    ) -> Result<(reqwest::StatusCode, String)> {
        let url = self.url(path);
        let resp = self
            .http
            .post(&url)
            .header("authorization", format!("Bearer {bearer}"))
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let body = resp.text().await.with_context(|| format!("POST {url}: read the body"))?;
        Ok((status, body))
    }

    /// GET `path` and return the raw status + body without requiring 2xx. Used
    /// where the rig must assert on a specific status code (e.g. a 404 after
    /// teardown) rather than treat non-2xx as an error.
    pub(crate) async fn get_raw(&self, path: &str) -> Result<(reqwest::StatusCode, String)> {
        let url = self.url(path);
        let resp = self
            .authed(self.http.get(&url))
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        let body = resp.text().await.with_context(|| format!("GET {url}: read the body"))?;
        Ok((status, body))
    }

    /// GET an ABSOLUTE url (not under the dispatcher base) and return raw
    /// status + body. Used for storage capability URLs, which the
    /// dispatcher hands out whole.
    pub(crate) async fn get_abs_raw(&self, url: &str) -> Result<(reqwest::StatusCode, Vec<u8>)> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        let bytes = resp.bytes().await.with_context(|| format!("GET {url}: read the body"))?.to_vec();
        Ok((status, bytes))
    }
}

/// Read a response, returning its body text on 2xx and an `Err` carrying the
/// status + body otherwise. The single place every HTTP call funnels through
/// so the "fail loud with the server's message" rule holds uniformly.
async fn read_ok(resp: reqwest::Response, verb: &str, url: &str) -> Result<String> {
    let status = resp.status();
    let body = resp.text().await.with_context(|| format!("{verb} {url} -> HTTP {status}: read the body"))?;
    if status.is_success() {
        Ok(body)
    } else {
        bail!("{verb} {url} -> HTTP {status}: {body}")
    }
}

/// A `weft` command acting on `install`: the install under test's own
/// binary, by its path, with `WEFT_INSTALL` naming `install`. Both are
/// explicit because the rig runs `weft` from temp folders, where the CLI
/// would otherwise pick the install from where it stands (outside any
/// checkout: the default install). A cell has no binary of its own: the
/// install under test's starts it, and its runtime is the one next to that
/// binary.
pub fn weft_command(install: &weft_core::infra::Install) -> Result<tokio::process::Command> {
    let under_test = crate::ensure::install_under_test()?;
    let bin = under_test.bin_dir().join("weft");
    if !bin.is_file() {
        bail!(
            "{} does not exist: the install under test ({}) is not set up; run ./setup.sh in {}",
            bin.display(),
            under_test.env_value(),
            crate::ensure::repo_root()?.display()
        );
    }
    let mut cmd = tokio::process::Command::new(bin);
    cmd.env(weft_core::infra::INSTALL_ENV, install.env_value());
    Ok(cmd)
}

/// Run the install under test's `weft` CLI in `dir` against `disp`'s
/// install ([`weft_command`]), returning the captured output. The CLI
/// inherits the rig's environment, with the dispatcher address and the
/// install set to `disp`'s, so a test on a cell never reaches another
/// install by accident. Fails loud if the binary cannot be spawned; the
/// EXIT STATUS is left to the caller (some verbs are expected to fail in
/// negative tests).
pub async fn cli(disp: &Dispatcher, dir: &Path, args: &[&str]) -> Result<CliOutput> {
    let mut cmd = weft_command(disp.install())?;
    cmd.current_dir(dir);
    cmd.args(args);
    cmd.env("WEFT_DISPATCHER_URL", disp.base());
    // The rig is non-interactive by construction: a verb that wants a
    // prompt must receive its answer via flags. With an inherited stdin
    // (the developer's terminal) a prompt would READ from the terminal
    // while its text went to the captured pipe: an invisible hang. A
    // null stdin makes the CLI's prompt guard bail loudly instead.
    cmd.stdin(std::process::Stdio::null());
    let start = std::time::Instant::now();
    let out: Output = cmd
        .output()
        .await
        .with_context(|| format!("spawn `weft {}` in {}", args.join(" "), dir.display()))?;
    eprintln!("[e2e] {:.1}s: weft {}", start.elapsed().as_secs_f64(), args.join(" "));
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
pub async fn cli_ok(disp: &Dispatcher, dir: &Path, args: &[&str]) -> Result<String> {
    let out = cli(disp, dir, args).await?;
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

/// The most one test may take: past it the runner stops the test and
/// counts it failed, saying only that it ran out of time.
// SYNC: TEST_LIMIT <-> scripts/run-e2e.sh TEST_LIMIT
pub const TEST_LIMIT: Duration = Duration::from_secs(300);

/// How long a wait may take in a test that began at `started` and still
/// needs `after` for what follows the wait, so a slow wait fails with its
/// own named [`poll_until`] timeout before the runner cuts the test with
/// nothing to say. Zero once the time is already spent (the poll then
/// checks once and fails naming the wait).
pub fn time_left(started: std::time::Instant, after: Duration) -> Duration {
    TEST_LIMIT.saturating_sub(started.elapsed()).saturating_sub(after)
}

/// Poll `f` until it returns `Ok(Some(v))`, yielding `v`; retry on `Ok(None)`;
/// propagate `Err` immediately. Times out after `deadline` with a message that
/// names `what`. This is the rig's single wait primitive: every "wait until the
/// system reaches state X" loop goes through here so the timeout / poll cadence
/// is consistent and a hang always surfaces as a clear error rather than a hung
/// test the harness has to kill. The deadline is for INTERNAL transitions the
/// rig controls (build done, unit running, run terminal), which legitimately
/// bound; it is not imposed on user-controlled long operations.
pub async fn poll_until<T, F, Fut>(
    what: &str,
    deadline: Duration,
    interval: Duration,
    f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>>>,
{
    poll_until_describing(what, deadline, interval, f, String::new).await
}

/// [`poll_until`] whose TIMEOUT message ends with `last_seen()`: what the
/// poll last observed (a status, a count), so a timeout says how far the
/// system got instead of leaving the reader to guess. Only the timeout
/// gets it: an `Err` from the closure (an HTTP failure, a run settling
/// wrong) propagates untouched, since decorating it with a "last seen"
/// would restate the error or, on a first-poll failure, invent an
/// observation that never happened.
pub async fn poll_until_describing<T, F, Fut>(
    what: &str,
    deadline: Duration,
    interval: Duration,
    mut f: F,
    last_seen: impl Fn() -> String,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>>>,
{
    let start = std::time::Instant::now();
    loop {
        if let Some(v) = f().await? {
            // One line per wait in the test's log: where a slow test spends
            // its time is read off these, and so is how far a failed one got.
            eprintln!("[e2e] waited {:.1}s for: {what}", start.elapsed().as_secs_f64());
            return Ok(v);
        }
        if start.elapsed() >= deadline {
            let seen = last_seen();
            let seen = if seen.is_empty() { seen } else { format!(" ({seen})") };
            bail!("timed out after {deadline:?} waiting for: {what}{seen}");
        }
        tokio::time::sleep(interval).await;
    }
}
