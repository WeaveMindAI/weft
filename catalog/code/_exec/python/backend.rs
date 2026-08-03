//! Code Node - Execute Python code (E2B sandbox in cloud, host python3 in local dev).
//!
//! **Inputs**: Each input port becomes a Python variable with the port's name.
//! For example, if you have input ports "data" and "config", your code can
//! directly reference `data` and `config` as variables. Unconnected ports are `None`.
//!
//! **Outputs**: Return a dict where keys are output port names.
//! For example: `return {"approved": result, "rejected": None}`
//! Only ports with non-None values will continue the flow.
//!
//! Two implicit output ports are always available: `stdout` and `stderr`. They
//! capture everything the user code printed during the run. They are always
//! emitted (empty string when the user printed nothing).
//!
//! **Dependencies**: Optional pip packages listed in config (one per line).
//! `code-interpreter-v1` already has numpy, pandas, scipy, scikit-learn,
//! matplotlib, pillow, requests, httpx, beautifulsoup4, lxml, pyyaml. Extras
//! are pip-installed at the start of the run.
//!
//! ## Execution backend
//!
//! In **cloud mode** (DEPLOYMENT_MODE=cloud) code runs inside an E2B Firecracker
//! microVM: no host filesystem, no host env, no host network. Egress to the
//! public internet is allowed so user code can hit third-party APIs. The
//! sandbox is terminated as soon as the run completes. E2B usage is billed.
//!
//! In **local mode** (the dev.sh default) code runs via the host's `python3`,
//! with no sandboxing at all. This is intended for the developer running their
//! own code on their own machine; same threat model as `python script.py`. The
//! `dependencies` field is ignored in this mode: the user installs their own
//! libs in their own Python environment.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};
use futures::StreamExt;
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use tokio::process::Command;
use tokio::sync::Semaphore;

const E2B_API_BASE: &str = "https://api.e2b.app";
const E2B_TEMPLATE: &str = "code-interpreter-v1";
const E2B_EXEC_PORT: u16 = 49999;
const SANDBOX_TIMEOUT_SECS: u64 = 60;
const REQUEST_TIMEOUT_SECS: u64 = 90;

// Concurrency guard for the Pro tier's 100-sandbox cap. We stay under 100 per
// replica (leaving headroom for multiple node-runner replicas). If this replica
// hits the cap, new callers wait for a slot rather than failing the node.
// Override with E2B_CONCURRENCY env var if you have a different tier / limit.
const E2B_DEFAULT_CONCURRENCY: usize = 50;

// Retry policy for the rare race where we somehow hit E2B's cap despite our
// own semaphore (multiple replicas, external callers, bursty retries).
const E2B_CREATE_MAX_RETRIES: u32 = 4;
const E2B_CREATE_RETRY_BASE_MS: u64 = 500;
const E2B_CREATE_RETRY_CAP_MS: u64 = 8_000;

// E2B pricing (April 2026):
//   $0.000028 / vCPU-second + $0.0000045 / GiB-second
// The `code-interpreter-v1` template is pinned to 2 vCPU / 2 GiB (verified by
// hitting the API). Custom sizes require the Pro tier, so we stay on defaults.
// We bill a flat 30s estimate per run, covering the average glue-code /
// API-call workload. Raw cost is reported to billing; margin is applied
// downstream by get_user_margin().
const E2B_VCPU_USD_PER_SEC: f64 = 0.000028;
const E2B_RAM_USD_PER_GIB_SEC: f64 = 0.0000045;
const E2B_DEFAULT_VCPUS: f64 = 2.0;
const E2B_DEFAULT_RAM_GIB: f64 = 2.0;
const E2B_ESTIMATED_RUN_SECS: f64 = 30.0;

/// Global per-replica concurrency gate for E2B sandbox creation. Enforced
/// across all concurrent ExecPython executions in a single node-runner process.
/// Sized from E2B_CONCURRENCY env var (falls back to E2B_DEFAULT_CONCURRENCY).
fn e2b_semaphore() -> &'static Semaphore {
    static SEM: std::sync::OnceLock<Semaphore> = std::sync::OnceLock::new();
    SEM.get_or_init(|| {
        let n = std::env::var("E2B_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(E2B_DEFAULT_CONCURRENCY);
        tracing::info!("ExecPython: E2B concurrency cap = {}", n);
        Semaphore::new(n)
    })
}

#[derive(Default)]
pub struct CodeNode;

#[async_trait]
impl Node for CodeNode {
    fn node_type(&self) -> &'static str {
        "ExecPython"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Code",
            inputs: vec![],
            outputs: vec![
                PortDef::new("stdout", "String", false),
                PortDef::new("stderr", "String", false),
            ],
            features: NodeFeatures {
                canAddInputPorts: true,
                canAddOutputPorts: true,
                ..Default::default()
            },
            fields: vec![
                FieldDef::code("code"),
                FieldDef::code("dependencies"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let code = ctx.config.get("code")
            .and_then(|v| v.as_str())
            .unwrap_or("return {}");

        let dependencies = ctx.config.get("dependencies")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let outcome = if is_local_deployment() {
            // Local dev: run with the host's python3, no sandbox, no API call.
            // The user is running their own code on their own machine; same
            // threat model as `python script.py`. Dependencies field is ignored:
            // users install their own libs in their own environment.
            tracing::warn!(
                "ExecPython: running user code on the host (DEPLOYMENT_MODE != cloud). \
                 This must NEVER happen in a multi-tenant deployment."
            );
            let wrapped = build_local_python(code, &ctx.input);
            match run_local_python(&wrapped).await {
                Ok(o) => o,
                Err(msg) => return NodeResult::failed(&msg),
            }
        } else {
            let api_key = match ctx.resolve_api_key(None, "e2b") {
                Some(r) => r.key,
                None => return NodeResult::failed(
                    "E2B_API_KEY is not configured on this node-runner. Code execution is unavailable."
                ),
            };
            let wrapped = build_e2b_python(code, dependencies, &ctx.input);
            let outcome = match run_in_e2b(&ctx.http_client, &api_key, &wrapped).await {
                Ok(o) => o,
                Err(msg) => return NodeResult::failed(&msg),
            };

            // Report estimated cost to billing. Actual sandbox lifetime is hard
            // to know per-call; the 30s estimate covers the typical glue workload.
            let cost_usd = E2B_ESTIMATED_RUN_SECS
                * (E2B_DEFAULT_VCPUS * E2B_VCPU_USD_PER_SEC
                    + E2B_DEFAULT_RAM_GIB * E2B_RAM_USD_PER_GIB_SEC);
            ctx.report_usage_cost(
                "e2b-code-interpreter",
                "code_exec",
                cost_usd,
                false,
                Some(serde_json::json!({
                    "estimatedDurationSecs": E2B_ESTIMATED_RUN_SECS,
                    "vcpus": E2B_DEFAULT_VCPUS,
                    "ramGib": E2B_DEFAULT_RAM_GIB,
                })),
            ).await;
            outcome
        };

        match outcome {
            ExecOutcome::Ok { ports, stdout, stderr } => {
                let mut output = ports;
                output.insert("stdout".into(), Value::String(stdout));
                output.insert("stderr".into(), Value::String(stderr));
                NodeResult::completed(Value::Object(output))
            }
            ExecOutcome::UserError(msg) => NodeResult::failed(&format!("Python error: {}", msg)),
        }
    }
}

/// True iff this node-runner is running in the local-dev deployment mode.
/// Anything other than the literal value "cloud" is treated as local: this
/// matches dev.sh which defaults DEPLOYMENT_MODE to "local" and the K8s manifests
/// which set it to "cloud" explicitly.
fn is_local_deployment() -> bool {
    std::env::var("DEPLOYMENT_MODE").as_deref() != Ok("cloud")
}

enum ExecOutcome {
    Ok {
        ports: serde_json::Map<String, Value>,
        stdout: String,
        stderr: String,
    },
    UserError(String),
}

/// Shared prelude: turns inputs into Python variables and wraps the user code
/// in a function so `return` works. `from X import *` lines are hoisted to
/// module scope because Python 3 disallows them inside functions.
fn build_user_block(user_code: &str, input: &Value) -> (String, String, String) {
    let inputs_json = serde_json::to_string(input).unwrap_or_else(|_| "{}".into());
    let (star_imports, clean_code) = extract_star_imports(user_code);
    let indented = indent_code(&clean_code, "    ");
    let var_setup = match input.as_object() {
        Some(obj) => obj.keys()
            .map(|k| {
                let safe = sanitize_identifier(k);
                format!("{} = __weft_inputs__.get({})", safe, python_str_literal(k))
            })
            .collect::<Vec<_>>()
            .join("\n"),
        None => String::new(),
    };
    let prelude = format!(
        "__weft_inputs__ = __weft_json__.loads({inputs})\n{vars}",
        inputs = python_str_literal(&inputs_json),
        vars = var_setup,
    );
    (star_imports, prelude, indented)
}

/// Build the full Python script that gets sent to E2B's /execute endpoint.
/// The function's return dict is rendered as the cell's last expression wrapped
/// in `IPython.display.JSON(...)`. The Jupyter kernel emits it as a `result`
/// event with the dict in its `json` field, so we never scrape stdout for it.
/// Optional pip deps are installed via `!pip install` at the top.
fn build_e2b_python(user_code: &str, deps_field: &str, input: &Value) -> String {
    let (star_imports, prelude, body) = build_user_block(user_code, input);
    let pip_line = pip_install_line(deps_field);

    format!(
        r#"
import json as __weft_json__
from IPython.display import JSON as __weft_JSON__

{pip}
{star_imports}

{prelude}

def __weft_user_code__():
{body}

__weft_result__ = __weft_user_code__()
if __weft_result__ is None:
    __weft_result__ = {{}}
if not isinstance(__weft_result__, dict):
    raise ValueError("Code must return a dict with output port names as keys, got: " + str(type(__weft_result__)))

__weft_JSON__(__weft_result__)
"#,
        pip = pip_line,
        star_imports = star_imports,
        prelude = prelude,
        body = body,
    )
}

/// Build the Python script for local execution (no Jupyter, no IPython).
/// Returns a script that prints exactly one line `WEFT_RESULT_JSON=<json>` to
/// stdout, surrounded by whatever the user code printed. The host parses that
/// marker line out. Dependencies are intentionally ignored: in local mode the
/// user runs against their own Python environment and installs their own libs.
fn build_local_python(user_code: &str, input: &Value) -> String {
    let (star_imports, prelude, body) = build_user_block(user_code, input);

    format!(
        r#"
import json as __weft_json__
import sys as __weft_sys__

{star_imports}

{prelude}

def __weft_user_code__():
{body}

__weft_result__ = __weft_user_code__()
if __weft_result__ is None:
    __weft_result__ = {{}}
if not isinstance(__weft_result__, dict):
    raise ValueError("Code must return a dict with output port names as keys, got: " + str(type(__weft_result__)))

# Leading \n is required so the host parser can anchor on `\n<marker>=` even
# when the user's last print() had end="" (no trailing newline). json.dumps
# default output is single-line, so the whole marker fits on one line.
__weft_sys__.stdout.write("\nWEFT_RESULT_JSON=" + __weft_json__.dumps(__weft_result__) + "\n")
__weft_sys__.stdout.flush()
"#,
        star_imports = star_imports,
        prelude = prelude,
        body = body,
    )
}

/// Build a `!pip install` line for any deps the user listed. Empty if none.
/// We don't whitelist or validate package names here: E2B is the trust boundary,
/// and pip installing a malicious package only harms the sandbox itself.
fn pip_install_line(deps_field: &str) -> String {
    let deps: Vec<&str> = deps_field
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();
    if deps.is_empty() {
        return String::new();
    }
    format!("!pip install --quiet {}", deps.join(" "))
}

/// Hoist `from X import *` lines from module scope (column 0) to the top of the
/// generated script, because Python 3 disallows them inside functions and we
/// wrap the user's code in one. Indented star-imports (inside a try/except,
/// a class, etc.) are left alone: Python will raise its own clear SyntaxError,
/// which is better than us silently ripping a line out of a block and breaking
/// control flow.
fn extract_star_imports(code: &str) -> (String, String) {
    let re = Regex::new(r"^from\s+\S+\s+import\s+\*\s*$").expect("valid regex");
    let mut star = Vec::new();
    let mut rest = Vec::new();
    for line in code.lines() {
        if re.is_match(line) {
            star.push(line.to_string());
        } else {
            rest.push(line.to_string());
        }
    }
    (star.join("\n"), rest.join("\n"))
}

fn sanitize_identifier(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_alphanumeric() || c == '_' {
            if i == 0 && c.is_ascii_digit() {
                out.push('_');
            }
            out.push(c);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() { "_input".into() } else { out }
}

fn indent_code(code: &str, indent: &str) -> String {
    if code.is_empty() {
        return format!("{}pass", indent);
    }
    code.lines()
        .map(|line| format!("{}{}", indent, line))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Render a Rust string as a Python string literal, escaped safely.
fn python_str_literal(s: &str) -> String {
    let escaped: String = s.chars().flat_map(|c| match c {
        '\\' => vec!['\\', '\\'],
        '"' => vec!['\\', '"'],
        '\n' => vec!['\\', 'n'],
        '\r' => vec!['\\', 'r'],
        '\t' => vec!['\\', 't'],
        c if (c as u32) < 0x20 => format!("\\x{:02x}", c as u32).chars().collect(),
        c => vec![c],
    }).collect();
    format!("\"{}\"", escaped)
}

/// Execute the wrapped script via the host's `python3` interpreter.
/// Used only when DEPLOYMENT_MODE != cloud. No sandboxing, no isolation: this
/// is intended for the developer running their own code on their own machine.
async fn run_local_python(code: &str) -> Result<ExecOutcome, String> {
    let output = Command::new("python3")
        .arg("-c")
        .arg(code)
        .output()
        .await
        .map_err(|e| format!("Failed to spawn local python3: {}", e))?;

    let stdout_full = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

    if !output.status.success() {
        let msg = if stderr.is_empty() {
            format!("python3 exited with status {}", output.status)
        } else {
            stderr.trim_end().to_string()
        };
        return Ok(ExecOutcome::UserError(msg));
    }

    // Wrapper always prefixes the marker with "\n" and json.dumps produces a
    // single line, so we can anchor on "\nWEFT_RESULT_JSON=" regardless of
    // whether the user's last print had a trailing newline. Using the LAST
    // marker occurrence means a user who prints the literal "WEFT_RESULT_JSON="
    // themselves cannot hijack us. Content AFTER the marker's terminating "\n"
    // (e.g. atexit / CPython shutdown prints) is preserved in the stdout port.
    const MARKER_ANCHOR: &str = "\nWEFT_RESULT_JSON=";
    let pos = stdout_full.rfind(MARKER_ANCHOR).ok_or_else(|| {
        "Local python3 finished without emitting a result marker. The wrapper may have been altered.".to_string()
    })?;
    let after = &stdout_full[pos + MARKER_ANCHOR.len()..];
    let (json_str, tail_after_marker) = match after.find('\n') {
        Some(nl) => (&after[..nl], &after[nl + 1..]),
        None => (after, ""),
    };
    let parsed: Value = serde_json::from_str(json_str)
        .map_err(|e| format!("Could not parse local result JSON: {} (raw: {})", e, json_str))?;
    let ports = parsed.as_object()
        .ok_or_else(|| format!("Local result was not a JSON object: {}", parsed))?
        .clone();

    let mut user_stdout = stdout_full[..pos].to_string();
    if !tail_after_marker.is_empty() {
        if !user_stdout.is_empty() && !user_stdout.ends_with('\n') {
            user_stdout.push('\n');
        }
        user_stdout.push_str(tail_after_marker);
    }

    Ok(ExecOutcome::Ok { ports, stdout: user_stdout, stderr })
}

/// Extracted sandbox handle. Owned by the caller; the caller is responsible
/// for calling `delete_sandbox` once it is done.
struct SandboxHandle {
    sandbox_id: String,
    envd_token: String,
    /// Only populated when E2B gates the per-sandbox domain with an extra layer.
    /// Today most sandboxes come back with null (no header needed).
    traffic_token: Option<String>,
}

async fn delete_sandbox(client: &reqwest::Client, api_key: &str, sandbox_id: &str) {
    let req = client
        .delete(format!("{}/sandboxes/{}", E2B_API_BASE, sandbox_id))
        .header("X-API-Key", api_key)
        .timeout(std::time::Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .send()
        .await;
    match req {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            tracing::warn!(target: "e2b", sandbox = %sandbox_id,
                "sandbox DELETE returned {}: {}. It will reap at its own timeout.", status, body);
        }
        Err(e) => {
            tracing::warn!(target: "e2b", sandbox = %sandbox_id,
                "sandbox DELETE failed: {}. It will reap at its own timeout.", e);
        }
    }
}

/// Call `POST /sandboxes` with exponential backoff on 429 Too Many Requests,
/// then parse the three fields we need. If the HTTP succeeds but the body is
/// missing any required field, we fire a best-effort DELETE so the sandbox
/// doesn't sit idle burning billable seconds until its own 60s timeout reaps it.
async fn create_sandbox_with_retry(
    client: &reqwest::Client,
    api_key: &str,
) -> Result<SandboxHandle, String> {
    let body = serde_json::json!({
        "templateID": E2B_TEMPLATE,
        "timeout": SANDBOX_TIMEOUT_SECS,
        "secure": true,
        "allow_internet_access": true,
        "autoPause": false,
    });

    let mut attempt: u32 = 0;
    let parsed: Value = loop {
        let resp = client
            .post(format!("{}/sandboxes", E2B_API_BASE))
            .header("X-API-Key", api_key)
            .header("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("E2B sandbox create failed: {}", e))?;

        if resp.status().is_success() {
            break resp.json().await
                .map_err(|e| format!("E2B create response was not JSON: {}", e))?;
        }

        let status = resp.status();
        // Honor Retry-After when present, clamped to our own retry cap so a
        // buggy upstream can't wedge us for hours. Otherwise exponential backoff.
        let retry_after = resp.headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .map(|secs| (secs * 1000).min(E2B_CREATE_RETRY_CAP_MS));
        let error_body = resp.text().await.unwrap_or_default();

        let retriable = status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE;

        if !retriable || attempt >= E2B_CREATE_MAX_RETRIES {
            return Err(format!(
                "E2B create returned {} after {} attempt(s): {}",
                status, attempt + 1, error_body
            ));
        }

        let backoff_ms = retry_after.unwrap_or_else(|| {
            let exp = E2B_CREATE_RETRY_BASE_MS.saturating_mul(1u64 << attempt);
            exp.min(E2B_CREATE_RETRY_CAP_MS)
        });
        tracing::warn!(
            target: "e2b",
            status = %status,
            attempt = attempt + 1,
            backoff_ms,
            "E2B capacity hit, backing off before retry"
        );
        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        attempt += 1;
    };

    // HTTP succeeded. If the body is missing a required field, we own a real
    // sandbox on E2B's side; clean it up before returning Err.
    let sandbox_id = match parsed.get("sandboxID").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return Err("E2B create response missing sandboxID".into()),
    };
    let envd_token = match parsed.get("envdAccessToken").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => {
            delete_sandbox(client, api_key, &sandbox_id).await;
            return Err("E2B create response missing envdAccessToken".into());
        }
    };
    let traffic_token = parsed.get("trafficAccessToken")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Ok(SandboxHandle { sandbox_id, envd_token, traffic_token })
}

async fn run_in_e2b(
    client: &reqwest::Client,
    api_key: &str,
    code: &str,
) -> Result<ExecOutcome, String> {
    // Block here until we have a concurrency slot. The slot is held for the
    // whole sandbox lifetime, including the exec stream and the DELETE call,
    // so we never count more live sandboxes than E2B allows.
    let _permit = e2b_semaphore()
        .acquire()
        .await
        .map_err(|_| "ExecPython semaphore closed (node-runner is shutting down)".to_string())?;

    let handle = create_sandbox_with_retry(client, api_key).await?;

    // Always tear down the sandbox, even if exec failed, so we don't pay for idle.
    let exec_result = exec_in_sandbox(
        client,
        &handle.sandbox_id,
        &handle.envd_token,
        handle.traffic_token.as_deref(),
        code,
    ).await;

    delete_sandbox(client, api_key, &handle.sandbox_id).await;

    exec_result
}

/// Typed E2B JSONL event. Known variants are strict; unknown tags map to
/// `Unknown` which we log and skip. That way a new E2B event type surfaces in
/// logs for us to react to, without taking down every live execution.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum E2bEvent {
    Stdout {
        #[serde(default)] text: Option<String>,
        #[serde(default)] timestamp: Option<String>,
    },
    Stderr {
        #[serde(default)] text: Option<String>,
        #[serde(default)] timestamp: Option<String>,
    },
    Result(ResultEvent),
    Error {
        #[serde(default)] name: Option<String>,
        #[serde(default)] value: Option<String>,
        #[serde(default)] traceback: Option<String>,
    },
    NumberOfExecutions { execution_count: i64 },
    EndOfExecution {},
    UnexpectedEndOfExecution {},
    Keepalive {},
    #[serde(other)]
    Unknown,
}

/// MIME-tagged display payload from a Jupyter `execute_result` (when
/// `is_main_result == true`) or `display_data` (when false). Every channel is
/// optional; populated ones reflect what the user code actually emitted.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultEvent {
    #[serde(default)] is_main_result: bool,
    #[serde(default)] text: Option<String>,
    #[serde(default)] html: Option<String>,
    #[serde(default)] markdown: Option<String>,
    #[serde(default)] svg: Option<String>,
    #[serde(default)] png: Option<String>,
    #[serde(default)] jpeg: Option<String>,
    #[serde(default)] pdf: Option<String>,
    #[serde(default)] latex: Option<String>,
    #[serde(default)] json: Option<Value>,
    #[serde(default)] javascript: Option<String>,
    #[serde(default)] data: Option<Value>,
    #[serde(default)] chart: Option<Value>,
    #[serde(default)] extra: Option<Value>,
}

/// Render every populated MIME channel of a non-main display_data event into
/// human-readable lines suitable for the stdout port. Binary channels are
/// summarized (size only) instead of dumped as base64.
fn render_display_data(r: &ResultEvent) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(t)  = &r.text       { out.push(format!("[text] {}", t)); }
    if let Some(h)  = &r.html       { out.push(format!("[html] {}", h)); }
    if let Some(m)  = &r.markdown   { out.push(format!("[markdown] {}", m)); }
    if let Some(s)  = &r.svg        { out.push(format!("[svg] {}", s)); }
    if let Some(p)  = &r.png        { out.push(format!("[png] {} bytes (base64)", p.len())); }
    if let Some(j)  = &r.jpeg       { out.push(format!("[jpeg] {} bytes (base64)", j.len())); }
    if let Some(p)  = &r.pdf        { out.push(format!("[pdf] {} bytes (base64)", p.len())); }
    if let Some(l)  = &r.latex      { out.push(format!("[latex] {}", l)); }
    if let Some(j)  = &r.json       { out.push(format!("[json] {}", j)); }
    if let Some(js) = &r.javascript { out.push(format!("[javascript] {}", js)); }
    if let Some(d)  = &r.data       { out.push(format!("[data] {}", d)); }
    if let Some(c)  = &r.chart      { out.push(format!("[chart] {}", c)); }
    if let Some(e)  = &r.extra {
        // Loud signal: extra is whatever MIME types we don't know about. If E2B
        // ever ships a new channel, we want it visible (not silently dropped).
        tracing::warn!(target: "e2b", "unknown MIME channels in result.extra: {}", e);
        out.push(format!("[extra] {}", e));
    }
    out
}

async fn exec_in_sandbox(
    client: &reqwest::Client,
    sandbox_id: &str,
    envd_token: &str,
    traffic_token: Option<&str>,
    code: &str,
) -> Result<ExecOutcome, String> {
    let url = format!("https://{}-{}.e2b.app/execute", E2B_EXEC_PORT, sandbox_id);

    let mut req = client
        .post(&url)
        .header("X-Access-Token", envd_token)
        .header("Content-Type", "application/json")
        .timeout(std::time::Duration::from_secs(REQUEST_TIMEOUT_SECS));
    if let Some(tok) = traffic_token {
        req = req.header("E2B-Traffic-Access-Token", tok);
    }
    let resp = req
        .json(&serde_json::json!({
            "code": code,
            "language": "python",
        }))
        .send()
        .await
        .map_err(|e| format!("E2B /execute request failed: {}", e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("E2B /execute returned {}: {}", status, body));
    }

    let mut stream = resp.bytes_stream();
    // Raw byte buffer so UTF-8 sequences that straddle TCP chunk boundaries
    // aren't corrupted by lossy per-chunk decoding. We split on the literal
    // `\n` byte (JSONL framing is ASCII), then decode each complete line as
    // UTF-8 once we have it whole.
    let mut buffer: Vec<u8> = Vec::new();
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut ports: Option<serde_json::Map<String, Value>> = None;
    let mut user_error: Option<String> = None;

    'outer: while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("E2B stream read error: {}", e))?;
        buffer.extend_from_slice(&chunk);

        while let Some(newline_pos) = buffer.iter().position(|&b| b == b'\n') {
            let line_bytes = buffer.drain(..=newline_pos).collect::<Vec<u8>>();
            let line_str = match std::str::from_utf8(&line_bytes[..line_bytes.len().saturating_sub(1)]) {
                Ok(s) => s.trim(),
                Err(e) => return Err(format!("E2B stream produced invalid UTF-8: {}", e)),
            };
            if line_str.is_empty() { continue; }

            let event: E2bEvent = serde_json::from_str(line_str)
                .map_err(|e| format!("E2B event JSON was malformed: {} (raw: {})", e, line_str))?;

            match event {
                E2bEvent::Stdout { text, timestamp } => {
                    if let Some(t) = text {
                        tracing::trace!(target: "e2b", ts = ?timestamp, "stdout: {:?}", t);
                        stdout.push_str(&t);
                    }
                }
                E2bEvent::Stderr { text, timestamp } => {
                    if let Some(t) = text {
                        tracing::trace!(target: "e2b", ts = ?timestamp, "stderr: {:?}", t);
                        stderr.push_str(&t);
                    }
                }
                E2bEvent::Result(r) => {
                    if r.is_main_result {
                        let obj = r.json.ok_or_else(|| {
                            "E2B result event for the main expression was missing the json field. \
                             User code must return a dict (the wrapper renders it via IPython.display.JSON)."
                                .to_string()
                        })?;
                        let map = obj.as_object().ok_or_else(|| {
                            format!("E2B result.json was not a JSON object: {}", obj)
                        })?.clone();
                        ports = Some(map);
                    } else {
                        // display_data event from inside the user code, e.g.
                        // `IPython.display.HTML(...)` or a matplotlib chart.
                        // Surface every populated MIME channel into stdout so
                        // the user sees what their code emitted.
                        for line in render_display_data(&r) {
                            stdout.push_str(&line);
                            stdout.push('\n');
                        }
                    }
                }
                E2bEvent::Error { name, value, traceback } => {
                    let summary = match (name.as_deref(), value.as_deref()) {
                        (Some(n), Some(v)) => format!("{}: {}", n, v),
                        (Some(n), None)    => n.to_string(),
                        (None, Some(v))    => v.to_string(),
                        (None, None)       => "Unknown Python error".to_string(),
                    };
                    let full = match traceback {
                        Some(tb) if !tb.is_empty() => format!("{}\n{}", summary, tb),
                        _ => summary,
                    };
                    user_error = Some(full);
                }
                E2bEvent::NumberOfExecutions { execution_count } => {
                    tracing::trace!(target: "e2b", count = execution_count, "execution count");
                }
                E2bEvent::EndOfExecution {} => break 'outer,
                E2bEvent::UnexpectedEndOfExecution {} => {
                    return Err("E2B kernel disconnected mid-execution".into());
                }
                E2bEvent::Keepalive {} => {}
                E2bEvent::Unknown => {
                    // E2B shipped a new event type we do not know about yet.
                    // Log loudly (so ops notices and ships a patch) but don't
                    // tear down the live execution for it.
                    tracing::warn!(target: "e2b", raw = %line_str, "unknown E2B event type");
                }
            }
        }
    }

    if let Some(err) = user_error {
        return Ok(ExecOutcome::UserError(err));
    }

    let ports = ports.ok_or_else(|| {
        "E2B did not emit a result event for the user code. The wrapper may have failed before \
         reaching IPython.display.JSON; check stderr in the node output."
            .to_string()
    })?;

    Ok(ExecOutcome::Ok { ports, stdout, stderr })
}

register_node!(CodeNode);
