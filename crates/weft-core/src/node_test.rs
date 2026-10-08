//! Per-node self-tests: the `Node::tests()` surface plus the two rigs
//! that run them.
//!
//! A node declares its tests in a `tests.rs` next to its `mod.rs`
//! (`pub fn tests() -> Vec<NodeTest>`), bridged through the node's
//! `impl Node` (`fn tests(&self) { tests::tests() }`). Three tiers:
//!
//! - **basic**: pure logic, no ctx, no I/O. A plain `fn() -> WeftResult<()>`.
//! - **fake**: the node's full `run` body against a [`FakeRig`]: canned
//!   provider responses, in-memory storage, canned signals. No
//!   credentials, no cost. The TOP tier for trigger and infra nodes:
//!   the live rig only drives a plain `run` body, so a live
//!   declaration on them is refused by the runner.
//! - **live**: the node's body against a [`LiveRig`] whose handle is
//!   the PRODUCTION runtime handle (real connection resolution,
//!   relaying, metering, billing). Needs a grant for the declared
//!   service and may spend money, so runners require an explicit
//!   opt-in before running one.
//!
//! Discovery is the node registry the running binary already has: walk
//! `NodeCatalog::all()`, ask each node for its list. No metadata
//! mirror, no inventory.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::access::{Access, CredentialOwner, OpenedConnection};
use crate::cancellation::CancellationFlag;
use crate::context::{ContextHandle, EndpointMethod, ExecutionContext, LogLevel, ValueBag};
use crate::error::{WeftError, WeftResult};
use crate::frames::LoopFrames;
use crate::node::{InputSpec, Node, NodeMetadata, NodeOutput};
use crate::primitive::{replay_await, replay_run, AwaitEnd, AwaitedEntry, AwaitedEntryKind, ReplayedAwait, SignalSpec};
use crate::weft_type::WeftType;

// ----- The test declaration ------------------------------------------

/// Which rig a test runs against. Serialized (lowercase) in the test
/// binary's `list` output and in run-result payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TestTier {
    Basic,
    Fake,
    Live,
}

impl TestTier {
    /// Stable lowercase tag, matching the serde form.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Basic => "basic",
            Self::Fake => "fake",
            Self::Live => "live",
        }
    }
}

/// The serializable face of one declared test: what `list` prints and
/// what UIs build their buttons from.
// SYNC: NodeTestInfo <-> weavemind/website/src/lib/graph/node-tests.ts (NodeTestInfo)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTestInfo {
    pub name: String,
    pub tier: TestTier,
    /// Live only: the service whose grant the test needs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// Live only: the test's declared parameters, in the same shape as
    /// node inputs, so a runner renders and collects them with the
    /// machinery it already has for inputs (widgets included). Each
    /// spec's `name` is the fixture name the test reads
    /// (`rig.fixture("NAME")`), prefilled locally from
    /// `WEFT_NODE_TEST_<NAME>`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fixtures: Vec<InputSpec>,
}

// ----- The runner's wire shapes ---------------------------------------
//
// One definition serves every consumer of the test binary's stdout
// (the engine runner writes them, the CLI and the dispatcher's process
// harvester read them), so the protocol cannot fork.

/// One node's `list` entry: its declared tests.
// SYNC: NodeTestsListing <-> weavemind/website/src/lib/graph/node-tests.ts (NodeTestsListing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTestsListing {
    #[serde(rename = "nodeType")]
    pub node_type: String,
    pub tests: Vec<NodeTestInfo>,
}

/// The `list` subcommand's whole output.
// SYNC: TestListing <-> weavemind/website/src/lib/graph/node-tests.ts (TestListing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestListing {
    pub nodes: Vec<NodeTestsListing>,
}

/// One finished run's report.
// SYNC: TestReport <-> weavemind/website/src/lib/graph/node-tests.ts (TestReport)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestReport {
    pub node: String,
    pub test: String,
    pub tier: TestTier,
    pub passed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Live only: the executions the run's cost is recorded
    /// under (the pinned pre-registered execution when one was supplied,
    /// throwaways otherwise).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub execution_ids: Vec<String>,
}

/// `POST /projects/{id}/node-tests/run`: run one live test on the
/// install, in the package's test image.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunNodeTestRequest {
    pub image_ref: String,
    pub node: String,
    pub test: String,
    #[serde(default)]
    pub live_connection: Option<String>,
    /// Live-test fixture variables (`WEFT_NODE_TEST_*`), handed to the
    /// test with the request.
    #[serde(default)]
    pub fixtures: BTreeMap<String, String>,
}

/// What starting a node-test run answers: the task to wait on.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunNodeTestResponse {
    pub task_id: String,
}

/// What a wait on a started node-test run answers
/// (`GET /projects/{id}/node-tests/runs/{task}`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeTestRunStatus {
    pub status: crate::task::TaskStatus,
    /// The runner's report, present once the task completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<TestReport>,
    /// The task's error, present when the RUN ITSELF failed (a failing
    /// test is a completed task whose report says `passed: false`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The `run-all` subcommand's whole output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunAllReport {
    pub tests: Vec<TestReport>,
    pub passed: bool,
}

/// The marker the runner prints in front of its JSON report line, so
/// readers find the report by identity instead of by position.
// A position-based protocol ("last non-empty line") broke whenever a
// log line landed after the report: process logs merge stdout and stderr
// into one stream, so a late tracing line from the runner's own
// teardown could shadow a report that was printed correctly.
pub const REPORT_SENTINEL: &str = "WEFT-TEST-REPORT ";

/// The runner's stdout protocol: exactly one JSON document, on the
/// last line starting with [`REPORT_SENTINEL`] (anything else is
/// stdout noise a node body or an interleaved log might have
/// printed). Every reader extracts through here so the protocol has
/// one definition.
pub fn report_line(stdout: &str) -> Option<&str> {
    stdout
        .lines()
        .rev()
        .map(str::trim)
        .find_map(|l| l.strip_prefix(REPORT_SENTINEL))
}

/// The in-flight cap for a `--parallel` flag: absent runs one at a
/// time, bare (`Some(0)`) runs everything at once, a number caps the
/// concurrency. One definition so the runner and every caller that
/// forwards the flag agree.
pub fn concurrency_limit(parallel: Option<usize>, total: usize) -> usize {
    match parallel {
        None => 1,
        Some(0) => total.max(1),
        Some(n) => n,
    }
}

enum TestFn {
    Basic(fn() -> WeftResult<()>),
    Fake(Box<dyn Fn(FakeRig) -> BoxFuture<'static, WeftResult<()>> + Send + Sync>),
    Live(Box<dyn Fn(LiveRig) -> BoxFuture<'static, WeftResult<()>> + Send + Sync>),
}

/// One declared test of one node. Built through the tier constructors
/// ([`Self::basic`] / [`Self::fake`] / [`Self::live`]); the tier and
/// the run function can never disagree because the constructor sets
/// both.
pub struct NodeTest {
    pub name: &'static str,
    pub tier: TestTier,
    /// Live only: the service whose grant this test needs. `None` on
    /// basic/fake.
    pub service: Option<&'static str>,
    /// Live only: the declared parameters this test reads through
    /// `rig.fixture`, as node-input specs (see
    /// [`NodeTestInfo::fixtures`]). Declared with [`Self::with_fixture`].
    pub fixtures: Vec<InputSpec>,
    run: TestFn,
}

/// How long one fake-tier test may run before the runner gives up on
/// it and fails it by name. Generous next to any honest fake (they do
/// no real I/O and finish in milliseconds), short next to a person
/// waiting on a suite that has silently stopped.
pub const FAKE_TEST_DEADLINE_SECS: u64 = 30;

impl NodeTest {
    /// A pure-logic test: no ctx, no I/O.
    pub fn basic(name: &'static str, run: fn() -> WeftResult<()>) -> Self {
        Self {
            name,
            tier: TestTier::Basic,
            service: None,
            fixtures: Vec::new(),
            run: TestFn::Basic(run),
        }
    }

    /// A fake-tier test: the body receives a fresh [`FakeRig`],
    /// declares canned responses/signals on it, runs the node through
    /// it, and asserts on the outcome + the request log.
    pub fn fake<F, Fut>(name: &'static str, run: F) -> Self
    where
        F: Fn(FakeRig) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = WeftResult<()>> + Send + 'static,
    {
        Self {
            name,
            tier: TestTier::Fake,
            service: None,
            fixtures: Vec::new(),
            run: TestFn::Fake(Box::new(move |rig| Box::pin(run(rig)))),
        }
    }

    /// A live-tier test: the body receives a [`LiveRig`] carrying the
    /// production handle and the resolved grant for `service`. May
    /// spend real money; runners gate it behind an explicit opt-in.
    pub fn live<F, Fut>(name: &'static str, service: &'static str, run: F) -> Self
    where
        F: Fn(LiveRig) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = WeftResult<()>> + Send + 'static,
    {
        Self {
            name,
            tier: TestTier::Live,
            service: Some(service),
            fixtures: Vec::new(),
            run: TestFn::Live(Box::new(move |rig| Box::pin(run(rig)))),
        }
    }

    /// Declare a parameter this live test reads through `rig.fixture`.
    /// One call per fixture the body reads; chain after [`Self::live`].
    /// Loud on any other tier (basic/fake tests read no fixtures, so a
    /// declaration there is an authoring error), and on a duplicate
    /// name.
    pub fn with_fixture(mut self, spec: InputSpec) -> Self {
        assert!(
            self.tier == TestTier::Live,
            "test '{}' is {}-tier; only live tests read fixtures, so only live tests \
             declare them",
            self.name,
            self.tier.as_str()
        );
        assert!(
            !self.fixtures.iter().any(|f| f.name == spec.name),
            "test '{}' declares fixture '{}' twice",
            self.name,
            spec.name
        );
        self.fixtures.push(spec);
        self
    }

    pub fn info(&self) -> NodeTestInfo {
        NodeTestInfo {
            name: self.name.to_string(),
            tier: self.tier,
            service: self.service.map(str::to_string),
            fixtures: self.fixtures.clone(),
        }
    }

    /// Run a basic test. Loud error on any other tier: the caller
    /// picked the wrong runner, not the test. A panicking assertion
    /// fails THIS test (an error carrying the panic message), never
    /// the whole runner.
    pub fn run_basic(&self) -> WeftResult<()> {
        match &self.run {
            TestFn::Basic(f) => {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
                    Ok(result) => result,
                    Err(payload) => Err(panic_error(self.name, payload)),
                }
            }
            _ => Err(WeftError::Config(format!(
                "test '{}' is {}-tier, not basic",
                self.name,
                self.tier.as_str()
            ))),
        }
    }

    /// Run a fake test against a fresh rig. Panics fail this one test,
    /// and so does hanging: a test that never returns fails by NAME
    /// after [`FAKE_TEST_DEADLINE_SECS`] instead of stalling the suite.
    ///
    /// The bound is here and not in each test because the shapes that
    /// hang are the rig's own (a cursor parked on a bus nothing closes,
    /// a caller nobody attaches), and without it the runner is killed
    /// from outside with no test named at all, which leaves bisecting
    /// by hand as the only way to find the culprit.
    pub async fn run_fake(&self) -> WeftResult<()> {
        self.run_fake_within(std::time::Duration::from_secs(FAKE_TEST_DEADLINE_SECS)).await
    }

    /// [`Self::run_fake`] with the bound named, so a test of the bound
    /// itself does not have to wait it out.
    pub async fn run_fake_within(&self, deadline: std::time::Duration) -> WeftResult<()> {
        match &self.run {
            TestFn::Fake(f) => {
                let body = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                    f(FakeRig::new()),
                ));
                match tokio::time::timeout(deadline, body).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(payload)) => Err(panic_error(self.name, payload)),
                    Err(_) => Err(WeftError::NodeExecution(format!(
                        "test '{}' did not finish within {}s. Something in \
                         it is waiting for what it never gets: a cursor reading a bus nothing \
                         closes, a caller nobody attaches, a signal nobody answers. Close the bus \
                         (or attach the caller) on every path the test takes",
                        self.name,
                        deadline.as_secs_f64(),
                    ))),
                }
            }
            _ => Err(WeftError::Config(format!(
                "test '{}' is {}-tier, not fake",
                self.name,
                self.tier.as_str()
            ))),
        }
    }

    /// Run a live test against the rig the runner composed (production
    /// handle + resolved grant). Panics fail this one test.
    pub async fn run_live(&self, rig: LiveRig) -> WeftResult<()> {
        match &self.run {
            TestFn::Live(f) => {
                match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(f(rig)))
                    .await
                {
                    Ok(result) => result,
                    Err(payload) => Err(panic_error(self.name, payload)),
                }
            }
            _ => Err(WeftError::Config(format!(
                "test '{}' is {}-tier, not live",
                self.name,
                self.tier.as_str()
            ))),
        }
    }
}

/// A plain text fixture spec for [`NodeTest::with_fixture`]: `name` is
/// the fixture name the test reads (`rig.fixture(name)`, prefilled
/// from `WEFT_NODE_TEST_<name>` locally), `label`/`description` are
/// what a runner shows when collecting the value.
pub fn fixture_spec(name: &str, label: &str, description: &str) -> InputSpec {
    InputSpec {
        name: name.to_string(),
        input_type: WeftType::Primitive(crate::weft_type::WeftPrimitive::String),
        required: true,
        accepts: None,
        widget: None,
        default: None,
        label: Some(label.to_string()),
        placeholder: None,
        description: Some(description.to_string()),
        requires_scopes: None,
        requires_values: None,
    }
}

/// A fixture spec cloned off one of the node's own inputs, renamed to
/// `fixture_name`: the test inherits the input's widget (a
/// remote-select picker, say) so a runner collects the fixture the
/// same way it collects that input. Loud when the manifest declares no
/// such input (an authoring error, caught the moment `tests()` runs).
pub fn fixture_spec_like(
    manifest: &NodeMetadata,
    input_name: &str,
    fixture_name: &str,
) -> InputSpec {
    let input = manifest
        .inputs
        .iter()
        .find(|i| i.name == input_name)
        .unwrap_or_else(|| {
            panic!(
                "node '{}' declares no input '{input_name}' to model fixture \
                 '{fixture_name}' on (declared: {:?})",
                manifest.node_type,
                manifest.inputs.iter().map(|i| i.name.as_str()).collect::<Vec<_>>()
            )
        });
    InputSpec {
        name: fixture_name.to_string(),
        // A fixture always needs a value; the node input's own default
        // is the node's business, not the test's.
        required: true,
        default: None,
        ..input.clone()
    }
}

/// Run a live test's `body`, then ALWAYS run `cleanup`, whatever the
/// body did: returned, `?`-errored, or panicked on an assertion. A live
/// test creates real resources in the connected account, and a bare
/// `?` or a failed `assert!` between the create and the delete would
/// otherwise leak the artifact on every failing run. Wrapping the two
/// halves here makes the leak impossible by construction: the delete is
/// not a statement the body can jump over.
///
/// The body's own outcome wins the report (its `?` error or panic is
/// what the author wants to see); a cleanup that then also fails is
/// appended so a leaked-and-uncleanable artifact is never silent. On a
/// clean body, a cleanup failure is the whole result.
///
/// Use it around the WHOLE create-to-assert span: create inside the
/// body, delete inside `cleanup`, and put assertions in the body so a
/// failed one still triggers cleanup.
pub async fn with_cleanup<T, B, Bf, C, Cf>(body: B, cleanup: C) -> WeftResult<T>
where
    B: FnOnce() -> Bf,
    Bf: std::future::Future<Output = WeftResult<T>>,
    C: FnOnce() -> Cf,
    Cf: std::future::Future<Output = WeftResult<()>>,
{
    let body_result =
        match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(body())).await {
            Ok(r) => r,
            Err(payload) => Err(panic_error("live body", payload)),
        };
    let cleanup_result = cleanup().await;
    match (body_result, cleanup_result) {
        (Err(body_err), Err(cleanup_err)) => Err(WeftError::NodeExecution(format!(
            "{body_err}; AND cleanup then failed, leaving an artifact behind: {cleanup_err}"
        ))),
        (Err(body_err), Ok(())) => Err(body_err),
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(cleanup_err)) => Err(cleanup_err),
    }
}

/// A panicked test body as a test failure, with the assertion's own
/// message.
fn panic_error(test: &str, payload: Box<dyn std::any::Any + Send>) -> WeftError {
    let message = if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    };
    WeftError::NodeExecution(format!("test '{test}' panicked: {message}"))
}

impl std::fmt::Debug for NodeTest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeTest")
            .field("name", &self.name)
            .field("tier", &self.tier)
            .field("service", &self.service)
            .field("fixtures", &self.fixtures.iter().map(|f| f.name.as_str()).collect::<Vec<_>>())
            .finish()
    }
}

// ----- The run outcome ------------------------------------------------

/// What one node run through a rig produced: the body's result plus
/// every emitted output, merged across `pulse_downstream` calls (each
/// port appears at most once; the rigs enforce the production
/// one-emission-per-port rule).
pub struct RunOutcome {
    /// The node body's own result. Tests that expect success `?` it
    /// via [`Self::ok`]; tests that expect a refusal match it.
    pub result: WeftResult<()>,
    /// Emitted output values by port.
    pub outputs: serde_json::Map<String, Value>,
    /// Ports the body closed explicitly (`ctx.close_port`).
    pub closed_ports: Vec<String>,
    /// The declared infrastructure, on a `run_provision_infra`
    /// outcome. `None` on the ctx-driven verbs (run / setup_trigger).
    pub infra_spec: Option<crate::infra::InfraSpec>,
}

impl RunOutcome {
    /// The outcome with its result unwrapped: `outcome.ok()?` is the
    /// standard first line of a happy-path test, erroring with the
    /// body's own failure when the run did not succeed.
    pub fn ok(self) -> WeftResult<Self> {
        match self.result {
            Ok(()) => Ok(Self { result: Ok(()), ..self }),
            Err(e) => Err(e),
        }
    }

    /// The message the body refused with: the mirror of [`Self::ok`],
    /// for a test whose whole point is that the node says no. A run
    /// that SUCCEEDED here is the failure, and says so.
    pub fn failure(self) -> WeftResult<String> {
        match self.result {
            Err(e) => Ok(e.to_string()),
            Ok(()) => Err(WeftError::NodeExecution(format!(
                "the node was expected to refuse, but it succeeded and emitted {:?}",
                self.outputs.keys().collect::<Vec<_>>()
            ))),
        }
    }

    /// The value emitted on `port`, loud when the body never emitted it.
    pub fn output(&self, port: &str) -> WeftResult<&Value> {
        self.outputs.get(port).ok_or_else(|| {
            WeftError::NodeExecution(format!(
                "the node emitted nothing on port '{port}' (emitted: {:?})",
                self.outputs.keys().collect::<Vec<_>>()
            ))
        })
    }

    /// The declared infrastructure, loud when this outcome did not
    /// come from a `run_provision_infra` call.
    pub fn infra_spec(&self) -> WeftResult<&crate::infra::InfraSpec> {
        self.infra_spec.as_ref().ok_or_else(|| {
            WeftError::NodeExecution(
                "this outcome carries no infra spec (only run_provision_infra produces one)"
                    .to_string(),
            )
        })
    }
}

// ----- The fake rig ---------------------------------------------------

/// One request the node sent through the rig's HTTP surface: a
/// connection client, `ctx.http()`, a connection-less
/// `ctx.client(None)`, or the rig's own URL fetches (put_from_url).
#[derive(Debug, Clone)]
pub struct SentRequest {
    /// Uppercase method (`"POST"`).
    pub method: String,
    /// URL path (`"/api/chat.postMessage"`), no query.
    pub path: String,
    /// Raw query string, if any.
    pub query: Option<String>,
    /// The request body as text, if the request carried a buffered body.
    pub body_text: Option<String>,
    /// The body parsed as JSON, when it parses.
    pub body: Option<Value>,
    /// True when the request carried a STREAMED body the rig cannot
    /// read (`body_text`/`body` are `None` then, which is not "no
    /// body"); an assertion on the body of a streamed request should
    /// check this first and say so.
    pub body_streamed: bool,
    /// The request's headers, names lowercased (`("content-type",
    /// "application/json")`). Non-UTF-8 values are lossily decoded.
    pub headers: Vec<(String, String)>,
}

impl SentRequest {
    /// The first value of a header, by case-insensitive name.
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers.iter().find(|(k, _)| *k == name).map(|(_, v)| v.as_str())
    }
}

/// What one canned route does when a request hits it.
#[derive(Clone)]
enum CannedRoute {
    /// Answer with this response.
    Respond(CannedResponse),
    /// Fail the call before any response, the way an unreachable server
    /// does ([`FakeRig::fail_connection`]).
    FailConnection,
}

#[derive(Clone)]
struct CannedResponse {
    status: u16,
    content_type: String,
    /// Response headers beyond the content type, as declared (a
    /// provider that answers with the created id in a header, an
    /// ETag, a `Location`).
    headers: Vec<(String, String)>,
    body: bytes::Bytes,
}

/// One canned route's CANONICAL key: method, path, and the declared
/// query as a decoded, order-normalized parameter MULTISET (`None` =
/// declared bare). Canonical at declaration time so the duplicate
/// guard and the matcher agree by construction: two declarations that
/// differ only in parameter order (or encoding) are the same route,
/// and repeated parameters stay distinct instead of collapsing.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct RouteKey {
    method: String,
    path: String,
    query: Option<Vec<(Vec<u8>, Vec<u8>)>>,
}

impl RouteKey {
    fn declared(method: &str, declared_path: &str) -> Self {
        let (path, query) = match declared_path.split_once('?') {
            Some((p, q)) => (p.to_string(), Some(query_params(q))),
            None => (declared_path.to_string(), None),
        };
        Self { method: method.to_ascii_uppercase(), path, query }
    }
}

/// A query string as its decoded, sorted parameter multiset. `+` and
/// percent-escapes decode first (via `percent_encoding`, the decoder
/// under the URL stack's form parsing; its lossy-UTF-8 pair iterator
/// would merge distinct binary tokens, so components decode here one
/// by one) so `q=a%20b` and `q=a+b` are one parameter; repeats are
/// kept (sorted), so `id=1&id=2` never collapses onto `id=2`.
/// Components stay BYTES end to end: an opaque binary token (`%FF%FE`
/// vs `%FE%FF`) compares byte-exact, so two genuinely different
/// queries can never merge; text rendering happens only in error
/// messages (see [`render_query`]).
fn query_params(query: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    fn decode(s: &str) -> Vec<u8> {
        percent_encoding::percent_decode(s.replace('+', " ").as_bytes()).collect()
    }
    let mut params: Vec<(Vec<u8>, Vec<u8>)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (decode(k), decode(v)),
            None => (decode(p), Vec::new()),
        })
        .collect();
    params.sort();
    params
}

/// Display-only rendering of a canonical parameter multiset for error
/// messages (lossy is fine here; matching never goes through this).
fn render_query(params: &[(Vec<u8>, Vec<u8>)]) -> String {
    params
        .iter()
        .map(|(k, v)| {
            format!("{}={}", String::from_utf8_lossy(k), String::from_utf8_lossy(v))
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// One stored file in the fake's in-memory storage.
struct StoredEntry {
    /// The wall it was stored inside, resolved (a `Instance { of: None }`
    /// names the run's instance): what `storage_list` filters on.
    scope: crate::storage::StorageScope,
    meta: crate::storage::StoredFileMeta,
    bytes: bytes::Bytes,
}

impl FakeState {
    /// `scope` with the run's own instance named: a `Instance { of: None }`
    /// in a run for no instance fails as it does in a real run.
    fn resolve_scope(&self, scope: &crate::storage::StorageScope) -> WeftResult<crate::storage::StorageScope> {
        match scope {
            crate::storage::StorageScope::Instance { of: None } => {
                let instance = self.instance.lock().unwrap().clone().ok_or_else(|| {
                    WeftError::NodeExecution(
                        "instance storage in a run for no instance: name the instance, or give the run one with rig.instance(..)".into(),
                    )
                })?;
                Ok(crate::storage::StorageScope::instance_of(instance))
            }
            other => Ok(other.clone()),
        }
    }

    /// The stored-file value an identified put in `scope` (resolved)
    /// already minted, if any.
    fn identified(&self, scope: &crate::storage::StorageScope, identity: &str) -> Option<Value> {
        let identities = self.identities.lock().unwrap();
        let (_, key) = identities.iter().find(|((s, i), _)| s == scope && i == identity)?;
        let storage = self.storage.lock().unwrap();
        let entry = storage.get(key).expect("an identified key is stored");
        Some(crate::storage::StoredFile::from(&entry.meta).to_value())
    }

    /// Every file stored in `scope`, sorted by key: what a list answers.
    fn files_in(&self, scope: &crate::storage::StorageScope) -> WeftResult<Vec<crate::storage::StoredFileMeta>> {
        let scope = self.resolve_scope(scope)?;
        let mut metas: Vec<_> = self
            .storage
            .lock()
            .unwrap()
            .values()
            .filter(|e| e.scope == scope)
            .map(|e| e.meta.clone())
            .collect();
        metas.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(metas)
    }

    /// A fresh key inside `scope` (already resolved).
    fn mint_storage_key(&self, scope: &crate::storage::StorageScope, filename: &str) -> String {
        let wall = match scope {
            crate::storage::StorageScope::Execution => "exec".to_string(),
            crate::storage::StorageScope::Project => "project".to_string(),
            crate::storage::StorageScope::Shared { name } => format!("shared/{name}"),
            crate::storage::StorageScope::Asset => "asset".to_string(),
            crate::storage::StorageScope::Instance { of } => {
                format!("instance/{}", of.as_ref().expect("a resolved instance scope names its instance"))
            }
        };
        format!("node-test/{wall}/{}-{filename}", self.next_storage_key.fetch_add(1, Ordering::SeqCst))
    }
}

/// Shared state behind the fake rig and its handle. Dumb by rule:
/// plain maps, append-only logs, zero provider logic.
struct FakeState {
    /// Canned provider responses keyed by their canonical
    /// [`RouteKey`]. `respond` declares them; the connection client's
    /// answering middleware matches an exact query parameter set
    /// first, then the bare path.
    routes: Mutex<HashMap<RouteKey, CannedRoute>>,
    /// Every request sent through the rig's HTTP surface, in order.
    requests: Mutex<Vec<SentRequest>>,
    /// Canned `await_signal` endings (an answer, or a wait given up),
    /// popped in order.
    signals: Mutex<VecDeque<AwaitEnd>>,
    /// The wake payload for the NEXT `run` (a firing trigger's
    /// `ctx.wake`). Taken (consumed) when a run starts.
    wake: Mutex<Option<Value>>,
    /// Which instance every run on this rig is for (`ctx.instance()`), set
    /// with `rig.instance(..)`. `None`: a run for no instance in particular.
    instance: Mutex<Option<crate::instance::InstanceId>>,
    /// The live caller the run is attached to (`attach_caller`), what
    /// `ctx.caller()` and its protocol-typed forms answer. `None` = a
    /// run with nobody on the line, which is what every node not behind
    /// a Route or Socket sees.
    caller: Mutex<Option<Arc<dyn crate::caller::CallerConnection>>>,
    /// Every `register_signal` call: (spec, port snapshot).
    registered_signals: Mutex<Vec<(SignalSpec, Value)>>,
    /// Every `await_signal` the node made, in order. What a node parks
    /// ON is as much of its behaviour as what it emits (a form is the
    /// thing a person reads), so a test can look at it.
    awaited_signals: Mutex<Vec<SignalSpec>>,
    /// Stored values the fake's opened connections answer
    /// (`conn.value(name)`), keyed by service then value name.
    connection_values: Mutex<BTreeMap<String, BTreeMap<String, String>>>,
    /// Services this run published a connection for, so
    /// `published_access` answers "already published" exactly when the
    /// node did publish, and answers None on the first run.
    published: Mutex<std::collections::BTreeSet<String>>,
    /// Declared granted permissions by service. A service with no
    /// declaration skips the required-permissions check entirely (a
    /// real granted set can also be unknowable); a declared set is
    /// checked against the access's required permissions.
    connection_permissions: Mutex<BTreeMap<String, Vec<String>>>,
    /// Declared-output-type overrides: what the compiler resolves for
    /// a `MustOverride` output port in a real graph.
    output_types: Mutex<HashMap<String, WeftType>>,
    /// Output ports the case wires downstream (`FakeRig::wire_output`),
    /// what the compiled graph supplies in production.
    wired_outputs: Mutex<HashSet<String>>,
    /// What the rig's runs share (`ctx.shared`), the way a worker's runs
    /// do: one rig is one worker, and nothing goes idle while it lives.
    shared: Arc<crate::shared::Shared>,
    /// Declared custom input ports, what the compiler merges onto a
    /// node with `canAddInputPorts` from the source's inline list.
    /// Custom input ports in the order the case declared them, which
    /// is the order the rig delivers them in (a compiled node's created
    /// ports come in written order; this stands in for it).
    input_types: Mutex<Vec<(String, WeftType)>>,
    /// Live buses opened during a run, keyed by their serialized
    /// marker, so the marker resolves back (in the node and in the
    /// test's post-run read).
    buses: Mutex<HashMap<String, crate::bus::BusHandle>>,
    /// In-memory storage, keyed by minted key. A key carries its scope
    /// (`node-test/<scope>/..`) and the entry its resolved scope, so a
    /// list sees only its own wall.
    storage: Mutex<HashMap<String, StoredEntry>>,
    /// `(resolved scope, identity)` of every identified put, to the key
    /// it minted: a second put of the same identity in the same scope
    /// answers that key and stores nothing, like the real service.
    identities: Mutex<Vec<((crate::storage::StorageScope, String), String)>>,
    /// Every change a run made to a stored file (`ctx.storage().edit` /
    /// `replace`), in order: what production journals for the inspector.
    file_edits: Mutex<Vec<crate::storage::FileEdit>>,
    /// Mint for storage keys.
    next_storage_key: AtomicU64,
    /// Every `ctx.log` line, in order.
    logs: Mutex<Vec<(LogLevel, String)>>,
    /// Every tag the node put on its execution, in call order, one
    /// entry per `ctx.tag_execution` call.
    execution_tags: Mutex<Vec<Vec<String>>>,
    /// Every `ctx.stop_tagged` the node asked for, in order. The fake
    /// stops nothing (there are no sibling runs here); it records the
    /// ask so a test can assert the node steered the right tag the
    /// right way.
    stops: Mutex<Vec<(String, crate::tag::StopSelf)>>,
    /// Every program call the node made (`ctx.infra(..)`,
    /// `ctx.trigger(..)`, `ctx.connections()`, ...), in order.
    program_calls: Mutex<Vec<(crate::program::ProgramCall, crate::tag::StopSelf)>>,
    /// Answers declared for program calls, by the call's journal name
    /// (`weft.infra.status`), each used once, in order.
    program_answers: Mutex<HashMap<String, VecDeque<Value>>>,
    /// Every instance token the node minted: the instance and its life.
    minted_tokens: Mutex<Vec<MintedToken>>,
    /// The infra endpoints the node can reach, by (place, endpoint
    /// name): its own (at `NODE_UNDER_TEST_ID`, declared by
    /// `declare_endpoint`) and the ones other infra nodes shared with it
    /// (declared by `declare_infra`). An undeclared one fails the way an
    /// unprovisioned one does in a real run.
    endpoints: Mutex<BTreeMap<EndpointKey, String>>,
    /// The outside address of a declared endpoint of the node's own.
    /// Declared by `declare_public_url`; absent reads as an endpoint
    /// that is not public.
    public_urls: Mutex<BTreeMap<EndpointKey, String>>,
    /// Canned answers, keyed by WHICH endpoint was called as well as
    /// the method and path, and popped in order.
    ///
    /// A queue rather than one standing answer, because the
    /// interesting nodes ask the same question twice and act on the
    /// answer CHANGING (a service that says no and then yes). Keyed by
    /// the endpoint because a node with two of them calling the wrong
    /// one is a real bug, and a fake that answered either identically
    /// would pass it.
    endpoint_answers: Mutex<BTreeMap<(EndpointKey, EndpointMethod, String), VecDeque<CannedAnswer>>>,
    /// Every endpoint call the node made, in order.
    endpoint_calls: Mutex<Vec<EndpointCall>>,
    /// The fake's journal of each body's replayable calls: every
    /// `ctx.run` step and every `ctx.await_signal`, in one sequence
    /// with one counter, exactly as production journals them (a body
    /// that awaits then runs journals the run at index 1). Keyed the
    /// way production keys its journal, per node body (a node's run
    /// and its trigger setup are two executions there, and two node
    /// types never share one), and it lives on the RIG, not on one
    /// run's handle, so a second `rig.run(..)` of the same node on the
    /// same rig is a resume: it replays what the first recorded, a
    /// step's value and a signal's payload alike, and refuses (through
    /// the same reading production uses) when the body's call order
    /// changed between the two. That is what lets a test prove a node's
    /// exactly-once step: a post that must not go out twice.
    journal: Mutex<BTreeMap<String, Vec<AwaitedEntry>>>,
    cancellation: Arc<CancellationFlag>,
    /// [`FakeRig::stop_after_calls`]: the number of outbound calls
    /// (HTTP requests and endpoint calls together) after which the
    /// rig presses stop. `None` until a test asks.
    stop_after_calls: Mutex<Option<usize>>,
}

/// An infra endpoint in the fake: the infra node's place, and the
/// endpoint's name.
type EndpointKey = (String, String);

/// The fake's key for an endpoint of the node's OWN infra.
fn own_endpoint(name: &str) -> EndpointKey {
    (NODE_UNDER_TEST_ID.to_string(), name.to_string())
}

/// One call a node made to an infra endpoint, as the fake recorded it.
#[derive(Debug, Clone, PartialEq)]
pub struct EndpointCall {
    /// The infra node the call went to: `NODE_UNDER_TEST_ID` for the
    /// node's own, else the place given to `declare_infra`.
    pub place: String,
    /// The endpoint the call went to, by the name the node resolved.
    pub endpoint: String,
    pub method: EndpointMethod,
    pub path: String,
    pub body: Option<Value>,
}

/// What the fake answers one endpoint call with: a body, or the
/// refusal a real service gives. Both are declarable, because a node
/// that only ever sees success is a node whose failure handling is
/// untested, and a real service answers 503 while it is still coming
/// up.
#[derive(Debug, Clone)]
enum CannedAnswer {
    Body(Value),
    Refusal { status: u16, body: String },
}

impl FakeState {
    /// The canned-answer client every fake-run HTTP surface hands out
    /// (connections, `ctx.http()`): the process-wide base client (the
    /// middleware short-circuits before any socket opens, but the
    /// shared pool keeps the one-base-client rule intact) wrapped in
    /// the answering middleware.
    fn canned_client(self: &Arc<Self>) -> reqwest_middleware::ClientWithMiddleware {
        reqwest_middleware::ClientBuilder::new(crate::access::client::base_client().clone())
            .with(CannedAnswerMiddleware { state: self.clone() })
            .build()
    }

    fn new() -> Arc<Self> {
        Arc::new(Self {
            routes: Mutex::new(HashMap::new()),
            requests: Mutex::new(Vec::new()),
            signals: Mutex::new(VecDeque::new()),
            wake: Mutex::new(None),
            instance: Mutex::new(None),
            caller: Mutex::new(None),
            registered_signals: Mutex::new(Vec::new()),
            awaited_signals: Mutex::new(Vec::new()),
            connection_values: Mutex::new(BTreeMap::new()),
            published: Mutex::new(std::collections::BTreeSet::new()),
            connection_permissions: Mutex::new(BTreeMap::new()),
            output_types: Mutex::new(HashMap::new()),
            wired_outputs: Mutex::new(HashSet::new()),
            shared: crate::shared::Shared::new(std::time::Duration::MAX),
            input_types: Mutex::new(Vec::new()),
            buses: Mutex::new(HashMap::new()),
            storage: Mutex::new(HashMap::new()),
            identities: Mutex::new(Vec::new()),
            file_edits: Mutex::new(Vec::new()),
            next_storage_key: AtomicU64::new(0),
            logs: Mutex::new(Vec::new()),
            execution_tags: Mutex::new(Vec::new()),
            stops: Mutex::new(Vec::new()),
            program_calls: Mutex::new(Vec::new()),
            program_answers: Mutex::new(HashMap::new()),
            minted_tokens: Mutex::new(Vec::new()),
            endpoints: Mutex::new(BTreeMap::new()),
            public_urls: Mutex::new(BTreeMap::new()),
            endpoint_answers: Mutex::new(BTreeMap::new()),
            endpoint_calls: Mutex::new(Vec::new()),
            journal: Mutex::new(BTreeMap::new()),
            cancellation: Arc::new(CancellationFlag::new()),
            stop_after_calls: Mutex::new(None),
        })
    }

    /// Press stop (cancel as a person would) once the node has made as
    /// many outbound calls as [`FakeRig::stop_after_calls`] asked for.
    /// Called after every recorded call, and when the test asks.
    fn press_stop_if_due(&self) {
        let Some(limit) = *self.stop_after_calls.lock().unwrap() else { return };
        let made = self.requests.lock().unwrap().len() + self.endpoint_calls.lock().unwrap().len();
        if made >= limit {
            self.cancellation.cancel_because(crate::exec::CancelCause::User);
        }
    }
}

/// One instance token a node minted through the rig
/// ([`FakeRig::minted_tokens`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MintedToken {
    pub instance: crate::instance::InstanceId,
    /// How many seconds it was made to live.
    pub expires_in_secs: u64,
    /// The id the run chose for it.
    pub id: uuid::Uuid,
}

/// The fake-tier harness handle. A test declares canned responses and
/// signals, runs the node, then asserts on the outcome and the request
/// log. Cheap to clone (shared state).
///
/// What the fake supports is exactly what a node body reaches through
/// its ctx seam: connection clients (answered from the canned routes,
/// every request recorded), in-memory storage, canned wake/await
/// signals, output capture, logs. Anything it does not support yet
/// fails loud naming the missing capability, never a silent no-op.
#[derive(Clone)]
pub struct FakeRig {
    state: Arc<FakeState>,
}

impl FakeRig {
    /// A fresh, empty rig. Built by the runner ([`NodeTest::run_fake`]);
    /// tests receive it, they don't construct it.
    pub fn new() -> Self {
        Self { state: FakeState::new() }
    }

    /// Declare a canned 200 response: any request the node sends on a
    /// rig-opened connection whose method matches and whose `path?query`
    /// (or bare path) equals `path` is answered with `body` as JSON.
    pub fn respond(&self, method: &str, path: &str, body: Value) {
        self.respond_status(method, path, 200, body);
    }

    /// [`Self::respond`] with an explicit status, for testing how the
    /// node handles a provider refusal.
    pub fn respond_status(&self, method: &str, path: &str, status: u16, body: Value) {
        self.respond_raw(
            method,
            path,
            status,
            "application/json",
            serde_json::to_vec(&body).expect("canned JSON serializes"),
        );
    }

    /// A canned response with raw bytes and an explicit content type,
    /// for providers that answer XML / CSV / binary. Each (method,
    /// path) is declared exactly once; a second declaration panics
    /// loudly instead of silently replacing the first (two pages of a
    /// paginated flow must differ in their query, never overwrite).
    pub fn respond_raw(
        &self,
        method: &str,
        path: &str,
        status: u16,
        content_type: &str,
        body: impl Into<bytes::Bytes>,
    ) {
        self.respond_with_headers(method, path, status, content_type, &[], body);
    }

    /// [`Self::respond_raw`] with response headers, for a provider whose
    /// answer lives partly outside the body: a create that hands the new
    /// id back in a header (`x-restli-id`), an `ETag`, a `Location`.
    /// `headers` are `(name, value)` pairs; the content type is its own
    /// argument and must not be repeated here.
    pub fn respond_with_headers(
        &self,
        method: &str,
        path: &str,
        status: u16,
        content_type: &str,
        headers: &[(&str, &str)],
        body: impl Into<bytes::Bytes>,
    ) {
        // Production's client follows redirects, so a node never sees
        // a 3xx; handing one out raw would exercise a path production
        // does not have.
        assert!(
            !(300..=399).contains(&status),
            "the fake rig does not model redirects; declare the final response's \
             route directly"
        );
        assert!(
            !headers.iter().any(|(name, _)| name.eq_ignore_ascii_case("content-type")),
            "the content type is the `content_type` argument; declaring it again as a \
             header would give the response two"
        );
        // Checked here, at declaration, naming the header and the route:
        // left to the response builder, a bad name surfaces only when a
        // request hits the route, as the node's own HTTP error, and a
        // route no request hits never reports it at all.
        for (name, value) in headers {
            assert!(
                http::header::HeaderName::from_bytes(name.as_bytes()).is_ok(),
                "'{name}' is not a valid header name (declared on {method} {path})"
            );
            // `from_bytes`, as the response builder and the real client
            // read one: a provider may send a latin-1 value, and a rig
            // that refused it would refuse what production serves.
            assert!(
                http::header::HeaderValue::from_bytes(value.as_bytes()).is_ok(),
                "the value of header '{name}' is not a valid header value (declared on {method} {path})"
            );
        }
        self.declare_route(method, path, CannedRoute::Respond(CannedResponse {
            status,
            content_type: content_type.to_string(),
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            body: body.into(),
        }));
    }

    /// Declare that a request matching (method, path) FAILS before any
    /// response, the way a call to a server that is down or unreachable
    /// does: the node's `send()` returns an error, with no status and no
    /// body. For testing how a node handles a provider it cannot reach
    /// at all, as opposed to one that answers with an error status
    /// ([`Self::respond_status`]). Matched and declared exactly like
    /// [`Self::respond`], and like it, once per route. The error is the
    /// rig's own, so `is_connect()` on it is false; test what the node
    /// does with a failed call, not how the error classifies itself.
    pub fn fail_connection(&self, method: &str, path: &str) {
        self.declare_route(method, path, CannedRoute::FailConnection);
    }

    fn declare_route(&self, method: &str, path: &str, route: CannedRoute) {
        // Keys are canonical (decoded, order-normalized query
        // multiset), so two spellings of one route collide here
        // instead of shadowing each other at match time.
        let key = RouteKey::declared(method, path);
        // The duplicate check releases the lock before asserting, so a
        // refused declaration never poisons the routes mutex.
        let already_declared = {
            let routes = self.state.routes.lock().unwrap();
            routes.contains_key(&key)
        };
        assert!(
            !already_declared,
            "a canned response for {method} {path} is already declared (query \
             parameter ORDER and encoding do not distinguish routes); each route \
             is declared once"
        );
        self.state.routes.lock().unwrap().insert(key, route);
    }

    /// Queue a canned payload for the node's next `ctx.await_signal`.
    /// Multiple calls queue in order; an `await_signal` on an empty
    /// queue fails loud ("the test declared no signal").
    pub fn signal(&self, payload: Value) {
        self.state.signals.lock().unwrap().push_back(AwaitEnd::Answered { value: payload });
    }

    /// Make the node's next `ctx.await_signal` fail the way a wait the
    /// run could not pause on fails once its hold runs out
    /// (`WeftError::WaitGaveUp`), queued in order with [`Self::signal`]:
    /// how a test checks what the node does with a wait given up.
    pub fn signal_given_up(&self) {
        let error = "the wait was given up (`rig.signal_given_up()`)".to_string();
        self.state.signals.lock().unwrap().push_back(AwaitEnd::GaveUp { error });
    }

    /// Make every run on this rig a run for this instance, what
    /// `ctx.instance()` answers, the way an instance's firing, instance token or
    /// `Weft-Instance` header would in production. A blank or malformed id
    /// panics: it is a mistake in the test.
    pub fn instance(&self, id: &str) {
        let id = crate::instance::InstanceId::new(id).unwrap_or_else(|why| panic!("rig.instance({id:?}): {why}"));
        *self.state.instance.lock().unwrap() = Some(id);
    }

    /// Set the wake payload (`ctx.wake`) for the NEXT run: what a
    /// firing trigger received from its provider. Consumed by that run.
    pub fn wake(&self, payload: Value) {
        *self.state.wake.lock().unwrap() = Some(payload);
    }

    /// Attach a scripted live caller for the next runs: what a Route or
    /// Socket trigger's execution carries. Build it with
    /// `FakeCallerConnection::connected(config)`, script its handshake,
    /// body and inbound messages, run the node, then assert on its
    /// recorded calls (`heads`, `chunks`, `close_reason`, `calls`).
    pub fn attach_caller(&self, conn: Arc<crate::caller::FakeCallerConnection>) {
        *self.state.caller.lock().unwrap() = Some(conn);
    }

    /// Mint a live bus the test feeds itself, and its marker to place
    /// on a `Bus` input: `inputs = json!({"bus": marker})`. The same
    /// registry the run's `ctx.bus` resolves markers through, so the
    /// node under test reads what the test sent. The producer side of
    /// what [`Self::bus`] reads back after a run.
    pub fn seed_bus(
        &self,
        opts: crate::bus::BusOptions,
    ) -> WeftResult<(crate::bus::BusHandle, Value)> {
        let handle = crate::bus::BusHandle::create_with_options(opts)
            .map_err(|e| WeftError::NodeExecution(format!("create bus: {e}")))?;
        let marker = handle.marker();
        self.state
            .buses
            .lock()
            .unwrap()
            .insert(marker.to_string(), handle.new_handle());
        Ok((handle, marker))
    }

    /// A connection marker for `service`, to place on an access input:
    /// `inputs = json!({"account": rig.access("slack"), ...})`. Opening
    /// it answers a client whose requests hit the canned routes.
    pub fn access(&self, service: &str) -> Value {
        Access::new("fake-connection", service, None).to_value()
    }

    /// Declare the resolved type of an output port, what the compiler
    /// supplies for a `MustOverride` declaration in a real graph
    /// (`ctx.output_type` reads it). Ports with a concrete metadata
    /// type need no declaration.
    pub fn output_type(&self, port: &str, ty: WeftType) {
        self.state.output_types.lock().unwrap().insert(port.to_string(), ty);
    }

    /// Wire output `port` downstream, what a wire out of it in the
    /// graph does in production (`ctx.is_output_wired` reads it). A
    /// rig run wires nothing by default, so a node that catches its
    /// failure into `error` fails loudly until the case wires it.
    pub fn wire_output(&self, port: &str) {
        self.state.wired_outputs.lock().unwrap().insert(port.to_string());
    }

    /// The port of a server on this machine that accepts every
    /// connection and hangs up at once, for a case that needs a server
    /// which is there and will not talk. A closed port is no substitute:
    /// on some machines (WSL among them) a dial to one hangs until its
    /// timeout instead of being refused.
    pub fn hang_up_server(&self) -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a loopback port");
        let port = listener.local_addr().expect("a bound port").port();
        std::thread::spawn(move || {
            for connection in listener.incoming() {
                drop(connection);
            }
        });
        port
    }

    /// Declare a custom input port with its type, what the compiler
    /// merges onto a node written `ExecPython(photo: Image)`
    /// (`ctx.declared_inputs` reads it). Ports the metadata declares
    /// need no declaration. Declared ports are delivered in declaration
    /// order, after the metadata's own, the way a compiled node holds
    /// its created ports in the order they were written.
    pub fn input_type(&self, port: &str, ty: WeftType) {
        let mut declared = self.state.input_types.lock().unwrap();
        match declared.iter_mut().find(|(name, _)| name == port) {
            Some(entry) => entry.1 = ty,
            None => declared.push((port.to_string(), ty)),
        }
    }

    /// What the node published as `service`'s connection, or `None` if
    /// it has not published one. For a test asserting on a node that
    /// opens something it runs itself.
    pub fn published_values(&self, service: &str) -> Option<BTreeMap<String, String>> {
        if !self.state.published.lock().unwrap().contains(service) {
            return None;
        }
        Some(
            self.state
                .connection_values
                .lock()
                .unwrap()
                .get(service)
                .cloned()
                .unwrap_or_default(),
        )
    }

    /// Declare that this node published `service`'s connection on an
    /// earlier run, holding these values. The second-run state: the
    /// node finds its own connection instead of asking the thing it
    /// runs for credentials again.
    pub fn published_connection(&self, service: &str, values: &[(&str, &str)]) {
        let values: BTreeMap<String, String> =
            values.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        self.state.connection_values.lock().unwrap().insert(service.to_string(), values);
        self.state.published.lock().unwrap().insert(service.to_string());
    }

    /// Declare that this node's own infrastructure answers on `name`
    /// at `url`. Without this, `ctx.endpoint(name)` fails the way it
    /// does when the infra is not running.
    pub fn declare_endpoint(&self, name: &str, url: &str) {
        self.declare_at(own_endpoint(name), url);
    }

    /// Declare that the infra node at `place` shares its endpoint
    /// `endpoint`, answering at `url`, and get the `Infra` handle it
    /// would emit, to place on the input the node reads it from. Answer
    /// its calls with [`Self::answer_infra`].
    pub fn declare_infra(&self, place: &str, endpoint: &str, url: &str) -> Value {
        assert_ne!(
            place, NODE_UNDER_TEST_ID,
            "'{place}' is the node under test; declare its own endpoints with declare_endpoint"
        );
        self.declare_at((place.to_string(), endpoint.to_string()), url);
        crate::infra::InfraHandle::new(place, endpoint, None).to_value()
    }

    fn declare_at(&self, key: EndpointKey, url: &str) {
        let mut endpoints = self.state.endpoints.lock().unwrap();
        // Two endpoints on one address would make a call ambiguous,
        // and the fake would answer the wrong one's canned reply
        // while the test went green. In a real project two endpoints
        // never share an address either.
        if let Some(((place, taken), _)) =
            endpoints.iter().find(|(taken, declared)| **taken != key && *declared == url)
        {
            panic!("endpoint '{taken}' of '{place}' is already declared at {url}; give '{}' its own", key.1);
        }
        endpoints.insert(key, url.to_string());
    }

    /// Declare that the endpoint `name` (already declared with
    /// `declare_endpoint`) is public and reached from outside at `url`:
    /// what `ctx.endpoint(name)?.public_url()` answers.
    pub fn declare_public_url(&self, name: &str, url: &str) {
        assert!(
            self.state.endpoints.lock().unwrap().contains_key(&own_endpoint(name)),
            "declare endpoint '{name}' with declare_endpoint before giving it a public url"
        );
        self.state.public_urls.lock().unwrap().insert(own_endpoint(name), url.to_string());
    }

    /// Declare what the NEXT call to `path` on the `endpoint` endpoint
    /// answers. Call it once per expected call, in order.
    ///
    /// A call with no answer left fails, which is the point: the test
    /// says exactly what the node is expected to ask, so an extra
    /// question it should not have needed to ask surfaces instead of
    /// being quietly answered.
    pub fn answer_endpoint(
        &self,
        endpoint: &str,
        method: EndpointMethod,
        path: &str,
        answer: Value,
    ) {
        self.queue_answer(own_endpoint(endpoint), method, path, CannedAnswer::Body(answer));
    }

    /// Declare what the NEXT call to `path` on the endpoint `endpoint`
    /// another infra node shared (see [`Self::declare_infra`]) answers.
    /// Call it once per expected call, in order, like `answer_endpoint`.
    pub fn answer_infra(
        &self,
        place: &str,
        endpoint: &str,
        method: EndpointMethod,
        path: &str,
        answer: Value,
    ) {
        self.queue_answer((place.to_string(), endpoint.to_string()), method, path, CannedAnswer::Body(answer));
    }

    /// Declare that the next call to `path` on `endpoint` is REFUSED
    /// with this status and body, the way a real service refuses one
    /// it is not ready for.
    pub fn refuse_endpoint(
        &self,
        endpoint: &str,
        method: EndpointMethod,
        path: &str,
        status: u16,
        body: &str,
    ) {
        self.queue_answer(
            own_endpoint(endpoint),
            method,
            path,
            CannedAnswer::Refusal { status, body: body.to_string() },
        );
    }

    fn queue_answer(
        &self,
        endpoint: EndpointKey,
        method: EndpointMethod,
        path: &str,
        answer: CannedAnswer,
    ) {
        self.state
            .endpoint_answers
            .lock()
            .unwrap()
            .entry((endpoint, method, path.to_string()))
            .or_default()
            .push_back(answer);
    }

    /// Press stop, the way a person does with `weft stop` or the
    /// editor's Stop button, once the node has made `calls` outbound
    /// calls (HTTP requests and endpoint calls, counted together). The
    /// call that reaches the count still gets its answer; the node sees
    /// the stop at its next `ctx.is_cancelled()` or `cancelled_err()`
    /// arm. `0` presses it before the run starts. The stop stays
    /// pressed for every later run on this rig, as a cancelled run's
    /// flag does.
    pub fn stop_after_calls(&self, calls: usize) {
        *self.state.stop_after_calls.lock().unwrap() = Some(calls);
        self.state.press_stop_if_due();
    }

    /// Every call the node made to an infra endpoint, its own or a
    /// shared one, in order.
    pub fn endpoint_calls(&self) -> Vec<EndpointCall> {
        self.state.endpoint_calls.lock().unwrap().clone()
    }

    /// Declare a stored value `service`'s opened connection answers
    /// (`conn.value(name)` / `conn.opt_value(name)`), e.g. an IMAP host.
    pub fn connection_value(&self, service: &str, name: &str, value: &str) {
        self.state
            .connection_values
            .lock()
            .unwrap()
            .entry(service.to_string())
            .or_default()
            .insert(name.to_string(), value.to_string());
    }

    /// Declare the permissions `service`'s connection has granted.
    /// Opening then checks the consuming input's required permissions
    /// against this set; with no declaration the check is skipped (a
    /// real granted set can also be unknowable, and required VALUES
    /// are always checked regardless).
    pub fn connection_permissions(&self, service: &str, granted: &[&str]) {
        self.state
            .connection_permissions
            .lock()
            .unwrap()
            .insert(service.to_string(), granted.iter().map(|s| s.to_string()).collect());
    }

    /// Run the node's `run` body: build the ctx from the node's own
    /// manifest (declared inputs/outputs) + `inputs` (a JSON object of
    /// input name -> value), capture everything it emits.
    pub async fn run(&self, node: &dyn Node, inputs: Value) -> RunOutcome {
        // `_feeds_alive` is LOAD-BEARING despite never being read: it
        // OWNS the run's generator feeds (the registry keeps only a
        // `Weak`, so these `Arc`s are what keep the markers
        // resolvable) and unregisters them when the body returns.
        // Binding it bare `_` would drop it immediately and every
        // `ctx.inputs.get::<Generator<T>>` read would fail "not live".
        let (handle, ctx, _feeds_alive) = match self.build_ctx(node, RUN_BODY, inputs) {
            Ok(triple) => triple,
            Err(e) => {
                return RunOutcome { result: Err(e), outputs: Default::default(), closed_ports: Vec::new(), infra_spec: None }
            }
        };
        let result = node.run(ctx).await;
        handle.into_outcome(result)
    }

    /// Run an infra node's `provision_infra` body with throwaway
    /// identities. The declared spec is behind
    /// [`RunOutcome::infra_spec`] for assertions.
    pub async fn run_provision_infra(&self, node: &dyn Node, inputs: Value) -> RunOutcome {
        let empty = || RunOutcome {
            result: Ok(()),
            outputs: Default::default(),
            closed_ports: Vec::new(),
            infra_spec: None,
        };
        // Keeps the run's generator feeds registered (see `run`).
        let (bag, _feeds_alive) = match manifest_input_bag(node.manifest(), &self.state.input_types.lock().unwrap(), inputs) {
            Ok(pair) => pair,
            Err(e) => return RunOutcome { result: Err(e), ..empty() },
        };
        let ictx = crate::infra::InfraProvisionContext::new(
            uuid::Uuid::new_v4(),
            NODE_UNDER_TEST_ID.to_string(),
            "node-test".to_string(),
        );
        match node.provision_infra(ictx, bag).await {
            Ok(spec) => RunOutcome { infra_spec: Some(spec), ..empty() },
            Err(e) => RunOutcome { result: Err(e), ..empty() },
        }
    }

    /// Run the node's `setup_trigger` body (a trigger registering its
    /// wake signal). Assert on [`Self::registered_signals`] after.
    pub async fn run_setup_trigger(&self, node: &dyn Node, inputs: Value) -> RunOutcome {
        // Keeps the run's generator feeds registered (see `run`).
        let (handle, ctx, _feeds_alive) = match self.build_ctx(node, SETUP_TRIGGER_BODY, inputs) {
            Ok(triple) => triple,
            Err(e) => {
                return RunOutcome { result: Err(e), outputs: Default::default(), closed_ports: Vec::new(), infra_spec: None }
            }
        };
        let result = node.setup_trigger(ctx).await;
        handle.into_outcome(result)
    }

    /// The journal a body of `node` replays and records: what
    /// production keys by (execution, node, frames), here one sequence per
    /// node TYPE and body kind, because a rig runs one instance of a
    /// type at a time (`NODE_UNDER_TEST_ID` is every node's id here).
    /// Two structs returning one manifest are one node to the rig.
    fn journal_key(node: &dyn Node, body: &str) -> String {
        format!("{}/{body}", node.manifest().node_type)
    }

    fn build_ctx(
        &self,
        node: &dyn Node,
        body: &str,
        inputs: Value,
    ) -> WeftResult<(CaptureBox, ExecutionContext, RegisteredFeeds)> {
        let manifest = node.manifest();
        let custom = self.state.input_types.lock().unwrap().clone();
        let (mut bag, feeds) = manifest_input_bag(manifest, &custom, inputs)?;
        // A case's declared created ports are part of what the node
        // declares, whether or not the case delivered on them. The
        // compiler supplies both halves in production; here the case does.
        for (port, _) in &custom {
            bag.declare_port(port.clone());
        }
        let wake = self.state.wake.lock().unwrap().take();
        // The same gate production applies before a trigger's body
        // runs: a firing payload has to be exactly what the node
        // declared it wakes with. A test that wakes a trigger with a
        // payload the node could never receive proves nothing, and
        // that is not hypothetical: every Slack trigger's test woke it
        // without the `type` field Slack puts on every event, so the
        // node passed its tests and failed on the first real message.
        if manifest.features.is_trigger {
            if let Some(payload) = &wake {
                crate::node::check_fire_payload(&manifest.fires_with, Some(payload))
                    .map_err(WeftError::NodeExecution)?;
            }
        }
        // Declared overrides play the compiler's role for MustOverride
        // ports; concrete metadata types stand as-is. The output map is
        // derived from the BAG (defaults applied), never the raw case
        // inputs: the node reads the bag, so a `portsFromConfig` field
        // with a manifest default must shape the outputs the same way
        // it shapes the run.
        let config = Value::Object(bag.object()?.clone());
        let mut outputs = declared_output_map(manifest, &config);
        for (port, ty) in self.state.output_types.lock().unwrap().iter() {
            outputs.insert(port.clone(), ty.clone());
        }
        // A resume loads the body's journal, exactly as the execution
        // driver seeds a firing's awaited sequence from the fold: in
        // call-index order, whatever order the records landed in (two
        // steps started together record in completion order, and the
        // fold sorts them the same way).
        let journal_key = Self::journal_key(node, body);
        let mut awaited_sequence =
            self.state.journal.lock().unwrap().get(&journal_key).cloned().unwrap_or_default();
        awaited_sequence.sort_by_key(|entry| entry.call_index);
        let handle = Arc::new(TestHandle {
            state: self.state.clone(),
            capture: Capture::new(
                outputs,
                self.state.wired_outputs.lock().unwrap().clone(),
                manifest.features.catch_errors,
            ),
            declared_inputs: declared_input_map(manifest, &custom),
            wake,
            publishes: manifest.publishes.clone(),
            has_generator_input: manifest.has_generator_input(),
            journal_key,
            awaited_sequence: Mutex::new(awaited_sequence.into()),
            next_call_index: AtomicU32::new(0),
        });
        let instance = self.state.instance.lock().unwrap().clone();
        let ctx = test_context(manifest, bag, instance, handle.clone());
        Ok((CaptureBox::Fake(handle), ctx, feeds))
    }

    /// Seed a stored file and get its stored-file value, to place on a
    /// file input: `inputs = json!({"file": rig.store_file("a.pdf",
    /// "application/pdf", bytes)})`. The same shape `ctx.storage`
    /// verbs mint during a run.
    pub fn store_file(&self, filename: &str, mime_type: &str, bytes: impl Into<Vec<u8>>) -> Value {
        self.store_file_in(&crate::storage::StorageScope::Execution, filename, mime_type, bytes)
    }

    /// [`Self::store_file`] inside `scope`: an instance's files
    /// (`StorageScope::instance_of(..)`), the project's, a shared space's.
    /// Only that scope's `storage_list` sees it. The run's own instance
    /// (`StorageScope::instance()`) needs `rig.instance(..)` first, and
    /// panics without one.
    pub fn store_file_in(
        &self,
        scope: &crate::storage::StorageScope,
        filename: &str,
        mime_type: &str,
        bytes: impl Into<Vec<u8>>,
    ) -> Value {
        let bytes: Vec<u8> = bytes.into();
        let scope = self.state.resolve_scope(scope).expect("store_file_in: the scope names no instance");
        let key = self.state.mint_storage_key(&scope, filename);
        let meta = crate::storage::StoredFileMeta {
            key: key.clone(),
            mime_type: mime_type.to_string(),
            size_bytes: bytes.len() as u64,
            filename: filename.to_string(),
            keep: false,
            expires_at_unix: None,
            keep_ttl_secs: None,
            created_at_unix: 0,
            version: crate::storage::FIRST_FILE_VERSION,
        };
        let stored = crate::storage::StoredFile::from(&meta);
        self.state
            .storage
            .lock()
            .unwrap()
            .insert(key, StoredEntry { scope, meta, bytes: bytes::Bytes::from(bytes) });
        stored.to_value()
    }

    /// The metadata the run stored under `key` (from an emitted
    /// stored-file value's `key` field), for asserting storage-side
    /// facts the wire value does not carry (the keep flag, the lifetime
    /// in `keep_ttl_secs`). Loud when nothing was stored under that key.
    pub fn stored_meta(&self, key: &str) -> WeftResult<crate::storage::StoredFileMeta> {
        self.state
            .storage
            .lock()
            .unwrap()
            .get(key)
            .map(|e| e.meta.clone())
            .ok_or_else(|| {
                WeftError::NodeExecution(format!("no stored file under key {key}"))
            })
    }

    /// Every file stored in `scope` (what `ctx.storage(scope).list()`
    /// answers), sorted by key: for asserting WHERE a node stored a file,
    /// or that it stored no second one.
    pub fn stored_files(&self, scope: &crate::storage::StorageScope) -> WeftResult<Vec<crate::storage::StoredFileMeta>> {
        self.state.files_in(scope)
    }

    /// Every change the runs made to a stored file, in order, as the
    /// inspector would show them (the file, the versions, the diff).
    pub fn file_edits(&self) -> Vec<crate::storage::FileEdit> {
        self.state.file_edits.lock().unwrap().clone()
    }

    /// The content stored under `key` (an emitted stored-file value's
    /// `key` field), for asserting what a node wrote. Loud when nothing
    /// is stored under that key.
    pub fn stored_bytes(&self, key: &str) -> WeftResult<bytes::Bytes> {
        self.state
            .storage
            .lock()
            .unwrap()
            .get(key)
            .map(|e| e.bytes.clone())
            .ok_or_else(|| WeftError::NodeExecution(format!("no stored file under key {key}")))
    }

    /// The live bus behind an emitted marker (`outcome.outputs["stream"]`),
    /// for reading what the run sent. Loud when the run opened no bus
    /// behind that marker.
    pub fn bus(&self, marker: &Value) -> WeftResult<crate::bus::BusHandle> {
        self.state
            .buses
            .lock()
            .unwrap()
            .get(&marker.to_string())
            .map(|h| h.new_handle())
            .ok_or_else(|| {
                WeftError::NodeExecution(
                    "no bus behind this marker (the run never opened one)".to_string(),
                )
            })
    }

    /// Every request sent through the rig's HTTP surface (connection
    /// clients, `ctx.http()`, connection-less `ctx.client(None)`, the
    /// rig's own URL fetches), in order.
    pub fn requests(&self) -> Vec<SentRequest> {
        self.state.requests.lock().unwrap().clone()
    }

    /// Assert a request matching (method, path) went through the rig's
    /// HTTP surface (any client the rig hands out), loud with the full
    /// log when none did.
    pub fn assert_sent(&self, method: &str, path: &str) {
        let requests = self.requests();
        let hit = requests
            .iter()
            .any(|r| r.method == method.to_ascii_uppercase() && r.path == path);
        assert!(
            hit,
            "no request matched {method} {path}; sent: {:?}",
            requests
                .iter()
                .map(|r| format!("{} {}", r.method, r.path))
                .collect::<Vec<_>>()
        );
    }

    /// Every `register_signal` the node made: (spec, port snapshot).
    pub fn registered_signals(&self) -> Vec<(SignalSpec, Value)> {
        self.state.registered_signals.lock().unwrap().clone()
    }

    /// Every signal the node PARKED on, in order. A form node's park is
    /// what a person ends up reading, so this is how a test looks at it.
    pub fn awaited_signals(&self) -> Vec<SignalSpec> {
        self.state.awaited_signals.lock().unwrap().clone()
    }

    /// Every `ctx.log` line, in order.
    pub fn logs(&self) -> Vec<(LogLevel, String)> {
        self.state.logs.lock().unwrap().clone()
    }

    /// Every replayable call recorded on this rig, in call order: each
    /// `ctx.run` step with its value and each `ctx.await_signal` with the
    /// payload it got, in call order with the call index each landed
    /// at, for `node`'s run body. A second `rig.run(..)` of the node on
    /// the same rig replays them instead of running the step's work or
    /// popping a new signal, the way a resumed execution replays its
    /// journal: run the node twice and this list still holds one entry
    /// per call, and the request log shows the provider was called
    /// once.
    pub fn recorded_steps(&self, node: &dyn Node) -> Vec<AwaitedEntry> {
        self.state.journal.lock().unwrap().get(&Self::journal_key(node, RUN_BODY)).cloned().unwrap_or_default()
    }

    /// Every tag the node put on its execution, one list per
    /// `ctx.tag_execution` call, in call order.
    pub fn execution_tags(&self) -> Vec<Vec<String>> {
        self.state.execution_tags.lock().unwrap().clone()
    }

    /// Every `ctx.stop_tagged` the node asked for, in order: the tag
    /// and whether it kept itself. Nothing was actually stopped (a
    /// fake run has no siblings); this is the record of the ask.
    pub fn stops(&self) -> Vec<(String, crate::tag::StopSelf)> {
        self.state.stops.lock().unwrap().clone()
    }

    /// Declare what the next program call named `name` answers (its
    /// journal name: `weft.infra.status`, `weft.infra.copies`,
    /// `weft.connections.list`, `weft.costs.list`, `weft.runs.clean`, ...).
    /// Answers are used once each, in order. A call that reads something
    /// with no answer declared fails naming this; a call that only acts
    /// (start, stop, activate, pick) answers without one.
    pub fn answer_program_call(&self, name: &str, value: Value) {
        self.state.program_answers.lock().unwrap().entry(name.to_string()).or_default().push_back(value);
    }

    /// Every program call the node made, in order, with the `StopSelf`
    /// it passed. Nothing was actually started, stopped or cleaned (a
    /// fake run has no project); this is the record of the asks.
    pub fn program_calls(&self) -> Vec<(crate::program::ProgramCall, crate::tag::StopSelf)> {
        self.state.program_calls.lock().unwrap().clone()
    }

    /// Every instance token the node minted, in order, one entry per
    /// `mint_instance_token` call.
    pub fn minted_tokens(&self) -> Vec<MintedToken> {
        self.state.minted_tokens.lock().unwrap().clone()
    }
}

impl Default for FakeRig {
    fn default() -> Self {
        Self::new()
    }
}

/// Build a node's input bag the way the engine would, from the node
/// TYPE's manifest (rather than a compiled `NodeDefinition`, which a
/// standalone test doesn't have): declared defaults fill absent
/// inputs, and the manifest's input names become the spec-name set so
/// `ctx.inputs.custom()` behaves. Tests hand values in engine shape
/// (an access input carries a marker built by `rig.access(..)`).
fn manifest_input_bag(
    manifest: &NodeMetadata,
    custom: &[(String, WeftType)],
    inputs: Value,
) -> WeftResult<(ValueBag, RegisteredFeeds)> {
    let Value::Object(mut delivered) = inputs else {
        return Err(WeftError::Input(format!(
            "test inputs must be a JSON object of input name -> value, got: {inputs}"
        )));
    };
    let mut feeds = RegisteredFeeds(Vec::new());
    for input in &manifest.inputs {
        if let Some(default) = &input.default {
            delivered
                .entry(input.name.clone())
                .or_insert_with(|| default.clone());
        }
        // A `Generator[T]` input takes its test value as a plain ARRAY
        // of items: the rig plays the engine's role, pre-loading a live
        // feed with the items (stream already finished) and placing the
        // handle marker in the bag, so the node's
        // `ctx.inputs.get::<Generator<T>>` pull loop runs unmodified.
        if input.input_type.as_generator().is_some() {
            if let Some(v) = delivered.get(&input.name) {
                let Some(items) = v.as_array().cloned() else {
                    return Err(WeftError::Input(format!(
                        "test input '{}' feeds a Generator port: pass the items as a JSON \
                         array (got: {v})",
                        input.name
                    )));
                };
                let feed = crate::generator::GeneratorFeed::new(
                    input.name.clone(),
                    crate::liveness::no_liveness(),
                    None,
                    Box::new(|_| {}),
                );
                for item in items {
                    feed.push(uuid::Uuid::new_v4(), std::sync::Arc::new(item))?;
                }
                feed.close(crate::generator::StreamEnd::Finished);
                let id = crate::generator::register_feed(&feed);
                feeds.0.push((id, feed));
                delivered.insert(input.name.clone(), crate::generator::generator_marker(id));
            }
        }
        // The same requiresScopes/requiresValues stamping the
        // production bag build applies, so the fake's open sees what
        // production's resolution would.
        crate::context::stamp_required_access(
            &input.name,
            input.requires_scopes.as_deref().unwrap_or_default(),
            input.requires_values.as_deref().unwrap_or_default(),
            &mut delivered,
        );
    }
    let spec_names = manifest.inputs.iter().map(|i| i.name.clone()).collect();
    // A rig run has no compiled node behind it, so the port order is the
    // manifest's declaration order, then the custom ports in the order
    // the case declared them (`rig.input_type`), standing in for the
    // compiler's written order, then any other delivered input. The
    // case's JSON object cannot carry the order itself: its keys are
    // sorted by name unless serde_json's `preserve_order` is on, and
    // turning that on for a test would change the map type of every
    // program built with the node.
    let mut order: Vec<String> = manifest.inputs.iter().map(|i| i.name.clone()).collect();
    for name in custom.iter().map(|(name, _)| name).chain(delivered.keys()) {
        if !order.iter().any(|n| n == name) {
            order.push(name.clone());
        }
    }
    let mut bag = ValueBag::inputs(delivered, spec_names, order);
    // The production bag reads the picker off the enrich-stamped widget;
    // a rig run has no enrich pass, so fill the same facts straight from
    // the manifest's recipe (the stamp's source).
    if let Some(spec) = &manifest.service {
        if let Some(input) = manifest.access_input() {
            bag.access_ports
                .insert(input.name.clone(), crate::context::AccessPort::from_recipe(spec));
        }
    }
    Ok((bag, feeds))
}

/// The generator-feed registrations one rig run installed. OWNS the
/// feeds (the registry keeps only a `Weak`, so these `Arc`s are what
/// keep the markers resolvable while the body runs) and unregisters
/// them on drop (every exit path, panics included), so the
/// process-wide feed registry never accumulates dead test feeds.
struct RegisteredFeeds(Vec<(uuid::Uuid, Arc<crate::generator::GeneratorFeed>)>);

impl Drop for RegisteredFeeds {
    fn drop(&mut self) {
        for (id, _) in self.0.drain(..) {
            crate::generator::unregister_feed(id);
        }
    }
}

/// The input ports a rig run declares: the metadata's own plus the
/// custom ones the test declared (`rig.input_type`).
fn declared_input_map(manifest: &NodeMetadata, custom: &[(String, WeftType)]) -> HashMap<String, WeftType> {
    let mut declared: HashMap<String, WeftType> = manifest
        .inputs
        .iter()
        .map(|i| (i.name.clone(), i.input_type.clone()))
        .collect();
    declared.extend(custom.iter().map(|(k, v)| (k.clone(), v.clone())));
    declared
}

/// Every output port a run may emit on: the manifest's own, plus the
/// ones this node DERIVES from its config (a form's fields, a switch's
/// cases). `config` is the case's inputs, which is where a rig run
/// finds that config. Without the derived half, a node whose ports come
/// from its config could never emit in a test.
fn declared_output_map(manifest: &NodeMetadata, config: &Value) -> HashMap<String, WeftType> {
    let mut declared: HashMap<String, WeftType> = manifest
        .outputs
        .iter()
        .map(|o| (o.name.clone(), o.port_type.clone()))
        .collect();
    if let Some(ports_from_config) = &manifest.ports_from_config {
        let (_, outputs) = crate::node::derive_config_ports(config.get(&ports_from_config.field), ports_from_config);
        for port in outputs {
            declared.insert(port.name, port.port_type);
        }
    }
    declared
}

/// One ExecutionContext for a rig run: throwaway identities, the
/// node's real type/label from its manifest.
fn test_context(
    manifest: &NodeMetadata,
    inputs: ValueBag,
    instance: Option<crate::instance::InstanceId>,
    handle: Arc<dyn ContextHandle>,
) -> ExecutionContext {
    ExecutionContext::new(
        uuid::Uuid::new_v4(),
        NODE_UNDER_TEST_ID.to_string(),
        manifest.node_type.clone(),
        None,
        crate::ExecutionId::new_v4(),
        LoopFrames::default(),
        instance,
        inputs,
        handle,
    )
    .marked_pure(manifest.features.pure)
}

/// The per-run output capture both rigs share: declared ports, the
/// one-emission-per-port rule (the production contract, enforced here
/// so a fake-passing node can't double-emit in production), and the
/// recorded values.
struct Capture {
    declared: HashMap<String, WeftType>,
    /// The output ports the case wired downstream.
    wired: HashSet<String>,
    /// The node declares `features.catchErrors`: a failed body goes to
    /// `error` when it is wired, decided as the engine decides it.
    catch_errors: bool,
    outputs: Mutex<serde_json::Map<String, Value>>,
    /// Items emitted on `Generator[T]` ports, in order. Folded into
    /// `outputs` as one JSON array per port at outcome time, so a test
    /// asserts the whole yielded sequence with the plain
    /// `outcome.output(port)` read.
    stream_outputs: Mutex<HashMap<String, Vec<Value>>>,
    closed_ports: Mutex<Vec<String>>,
}

impl Capture {
    fn new(declared: HashMap<String, WeftType>, wired: HashSet<String>, catch_errors: bool) -> Self {
        Self {
            declared,
            wired,
            catch_errors,
            outputs: Mutex::new(Default::default()),
            stream_outputs: Mutex::new(HashMap::new()),
            closed_ports: Mutex::new(Vec::new()),
        }
    }

    // `pulse_downstream` and `yield_downstream` capture identically:
    // the harness consumes every yield instantly, so a delivered yield
    // resolves at once and a plain one has nothing left un-taken.
    fn pulse(&self, output: NodeOutput) -> WeftResult<()> {
        let mut recorded = self.outputs.lock().unwrap();
        let mut streams = self.stream_outputs.lock().unwrap();
        let closed = self.closed_ports.lock().unwrap();
        // Gate order mirrors production's `pulse_downstream`: all
        // ports declared, then the runtime output-type check, then the
        // one-emission-per-port claim (generator ports accept repeats,
        // but never past a close), so a multi-fault emission errors on
        // the same gate here and there.
        for port in output.outputs.keys() {
            if !self.declared.contains_key(port) {
                return Err(WeftError::NodeExecution(format!(
                    "the node emitted on undeclared output port '{port}'; declare it in \
                     metadata.json's outputs list, or correct the port name in the body"
                )));
            }
        }
        // The same gate production applies: a value the port's
        // declared type does not accept fails THIS call and nothing
        // else. The port is left open, so a node that catches the
        // error can try the port again with something else, and
        // whatever the firing already sent stays sent. Generator ports
        // check each ITEM against the element type, again as
        // production does.
        for (port, value) in &output.outputs {
            let declared = self.declared[port].port_value_type();
            if !declared.accepts_runtime_value(value) {
                return Err(WeftError::NodeExecution(format!(
                    "the node emitted a value on port '{port}' that its declared type \
                     '{declared}' does not accept; nothing was sent and the port is still \
                     open, so this emission can be retried with a value that fits"
                )));
            }
        }
        for port in output.outputs.keys() {
            let is_generator = self.declared[port].as_generator().is_some();
            if closed.iter().any(|p| p == port) {
                return Err(WeftError::NodeExecution(if is_generator {
                    format!("the node yielded on stream port '{port}' after closing it")
                } else {
                    format!(
                        "the node touched output port '{port}' twice in one firing; each \
                         port can be emitted or closed at most once"
                    )
                }));
            }
            if !is_generator && recorded.contains_key(port) {
                return Err(WeftError::NodeExecution(format!(
                    "the node touched output port '{port}' twice in one firing; each port \
                     can be emitted or closed at most once"
                )));
            }
        }
        // No buffer-overrun check here, on purpose: production's cap
        // (`check_generator_buffer_cap`) counts UN-TAKEN items, and in
        // a node test the harness is the consumer and takes every
        // yield instantly, so that count is honestly always zero. A
        // lifetime counter would falsely fail any high-volume
        // fire-and-forget producer that runs fine behind a keeping-up
        // consumer. The overrun contract is pinned at the engine layer
        // instead (execution_driver_tests/stream.rs, deterministic
        // gated-consumer constructions).
        for (port, value) in output.outputs {
            if self.declared[&port].as_generator().is_some() {
                streams.entry(port).or_default().push(value);
            } else {
                recorded.insert(port, value);
            }
        }
        Ok(())
    }

    fn close_port(&self, port: &str) -> WeftResult<()> {
        if !self.declared.contains_key(port) {
            return Err(WeftError::NodeExecution(format!(
                "the node closed undeclared output port '{port}'"
            )));
        }
        let recorded = self.outputs.lock().unwrap();
        let mut closed = self.closed_ports.lock().unwrap();
        // A generator port closes AFTER its yields (that IS the early
        // end-of-stream verb); only a second close is a bug. Every
        // other port keeps the one-touch rule.
        let is_generator = self.declared[port].as_generator().is_some();
        if closed.iter().any(|p| p == port)
            || (!is_generator && recorded.contains_key(port))
        {
            return Err(WeftError::NodeExecution(format!(
                "the node touched output port '{port}' twice in one firing; each port can \
                 be emitted or closed at most once (a stream port may yield then close once)"
            )));
        }
        closed.push(port.to_string());
        Ok(())
    }

    /// Whether the body has touched (emitted or closed) any output
    /// port yet, mirroring production's port-claims "mentioned" set
    /// for the `await_signal` guard (a touch-then-durable-suspend
    /// would touch again on replay).
    fn has_mentioned_a_port(&self) -> bool {
        !self.outputs.lock().unwrap().is_empty()
            || !self.stream_outputs.lock().unwrap().is_empty()
            || !self.closed_ports.lock().unwrap().is_empty()
    }

    /// Same ARGUMENT validation production applies (a declared
    /// generator output, a cap above zero). The cap itself is not
    /// enforced in a node test: the harness consumes every yield
    /// instantly, so nothing is ever un-taken (see `pulse`).
    fn set_max_buffered_items(&self, port: &str, items: usize) -> WeftResult<()> {
        if self.declared.get(port).is_none_or(|t| t.as_generator().is_none()) {
            return Err(WeftError::NodeExecution(format!(
                "set_max_buffered_items on '{port}', which is not a Generator output of \
                 this node"
            )));
        }
        if items == 0 {
            return Err(WeftError::NodeExecution(
                "set_max_buffered_items(0): a cap of 0 could never accept even the first \
                 item"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn into_outcome(self, result: WeftResult<()>) -> RunOutcome {
        let mut outputs = self.outputs.into_inner().unwrap();
        // The runtime's catch, decided by the same function the engine
        // reads: a caught failure lands on `error` and the step ends
        // well, so a node's test of its caught path tests what runs.
        let result = match result {
            Err(error) => match crate::context::caught_failure(
                self.catch_errors,
                crate::context::catchable_message(&error),
                self.wired.contains(crate::context::ERROR_PORT),
            ) {
                Some(message) => {
                    outputs.insert(crate::context::ERROR_PORT.to_string(), Value::String(message));
                    Ok(())
                }
                None => Err(error),
            },
            ok => ok,
        };
        let mut streams = self.stream_outputs.into_inner().unwrap();
        // EVERY declared generator output lands in the outcome as one
        // array, a stream that yielded nothing included: a test asserts
        // the whole yielded sequence, and "no entry" would make an
        // empty stream indistinguishable from a port that isn't one.
        for (port, ty) in &self.declared {
            if ty.as_generator().is_some() {
                outputs.insert(port.clone(), Value::Array(streams.remove(port).unwrap_or_default()));
            }
        }
        RunOutcome {
            result,
            outputs,
            closed_ports: self.closed_ports.into_inner().unwrap(),
            infra_spec: None,
        }
    }
}

/// The capturing handle of a finished run, whichever rig produced it.
/// Exists to unwrap the `Arc` back into the owned capture (the run is
/// over; the ctx and every clone of the handle are dropped).
enum CaptureBox {
    Fake(Arc<TestHandle>),
    Live(Arc<CapturingHandle>),
}

impl CaptureBox {
    fn into_outcome(self, result: WeftResult<()>) -> RunOutcome {
        fn unwrap_capture<T>(handle: Arc<T>, capture: impl FnOnce(T) -> Capture) -> Capture {
            match Arc::try_unwrap(handle) {
                Ok(h) => capture(h),
                Err(_) => panic!(
                    "the node's body leaked its ExecutionContext past its own return (a \
                     spawned task holding ctx?); a node must not outlive its firing"
                ),
            }
        }
        match self {
            Self::Fake(handle) => unwrap_capture(handle, |h| h.capture).into_outcome(result),
            Self::Live(handle) => unwrap_capture(handle, |h| h.capture).into_outcome(result),
        }
    }
}

/// Error text for a capability the fake does not support yet. One
/// phrasing everywhere so the gap reads as the rig's, not the node's.
fn unsupported(what: &str) -> WeftError {
    WeftError::Config(format!(
        "the fake rig does not support {what} yet; add it to weft-core's node_test module \
         or cover this path with a live test"
    ))
}

/// The fake `ContextHandle`: routes every capability to the rig's
/// shared state. Shipped (not `#[cfg(test)]`): the per-package test
/// binary is a real artifact.
struct TestHandle {
    state: Arc<FakeState>,
    capture: Capture,
    declared_inputs: HashMap<String, WeftType>,
    wake: Option<Value>,
    /// The service this node declares it publishes a connection to
    /// (`publishes` in its metadata), so the fake answers exactly what
    /// production does: the service comes from the declaration, never
    /// from the body.
    publishes: Option<String>,
    /// Whether the node under test declares a Generator input, so the
    /// rig refuses `await_signal` exactly where an execution would.
    has_generator_input: bool,
    /// Which of the rig's journals this body records into.
    journal_key: String,
    /// What the body has left to replay: the journal it started from,
    /// popped in call order by the same reading production uses.
    awaited_sequence: Mutex<VecDeque<AwaitedEntry>>,
    /// The call counter for THIS run, advanced by `ctx.run` and
    /// `ctx.await_signal` alike, as production numbers them: one
    /// sequence across both, never keyed on a step's name.
    next_call_index: AtomicU32,
}

impl TestHandle {
    /// Append one replayable call to this body's journal on the rig.
    fn record(&self, entry: AwaitedEntry) {
        self.state.journal.lock().unwrap().entry(self.journal_key.clone()).or_default().push(entry);
    }

    /// The service the node under test declares it publishes, or the
    /// same refusal production gives a node that declares none.
    fn published_service(&self) -> WeftResult<String> {
        self.publishes.clone().ok_or_else(|| {
            WeftError::Config(
                "this node hands out a connection to a service it runs, but its metadata \
                 does not say which: add `\"publishes\": \"<service>\"` to it"
                    .to_string(),
            )
        })
    }
}

#[async_trait::async_trait]
impl ContextHandle for TestHandle {
    fn plain_http(&self) -> reqwest_middleware::ClientWithMiddleware {
        // `ctx.http()` and a connection-less `ctx.client(None)` answer
        // from the same canned routes as rig-opened connections: a
        // fake run can NEVER reach the real network.
        self.state.canned_client()
    }

    async fn await_signal(&self, spec: SignalSpec) -> WeftResult<Value> {
        // Same refusals as the production handle, in production's
        // order: a green node test on a body an execution would refuse
        // is the costly divergence.
        if self.capture.has_mentioned_a_port() {
            return Err(WeftError::NodeExecution(
                crate::context::emitted_then_await_signal_error(NODE_UNDER_TEST_ID),
            ));
        }
        if self.has_generator_input {
            return Err(WeftError::NodeExecution(
                crate::context::stream_consumer_await_signal_error(NODE_UNDER_TEST_ID),
            ));
        }
        // One counter with `ctx.run` and the one replay reading, as
        // production: a resume (a second run of this node on this rig)
        // replays the payload this index got the first time, and a
        // drifted body is refused with production's own words.
        let call_index = self.next_call_index.fetch_add(1, Ordering::SeqCst);
        match replay_await(&mut self.awaited_sequence.lock().unwrap(), call_index)? {
            Some(ReplayedAwait::Resolved(payload)) => return Ok(payload),
            Some(ReplayedAwait::Pending { token }) => {
                return Err(WeftError::NodeExecution(format!(
                    "the rig's journal holds an unanswered await ({token}) at call_index={call_index}, \
                     which it never records: every await it journals was answered when it was made"
                )));
            }
            None => {}
        }
        // Record the ATTEMPT before popping, so a test can see a park
        // that found no queued payload (otherwise "parked on nothing"
        // and "never awaited" would look the same).
        self.state.awaited_signals.lock().unwrap().push(spec.clone());
        let ended = self.state.signals.lock().unwrap().pop_front().ok_or_else(|| {
            WeftError::Config(format!(
                "the node awaited a '{}' signal but the test declared no payload for it; \
                 queue one with rig.signal(json!(..)) before rig.run(..)",
                spec.kind
            ))
        })?;
        self.record(AwaitedEntry {
            call_index,
            kind: AwaitedEntryKind::Await { token: format!("rig-{}-{call_index}", spec.kind), ended: Some(ended.clone()) },
        });
        match ended {
            AwaitEnd::Answered { value } => Ok(value),
            AwaitEnd::GaveUp { error } => Err(WeftError::WaitGaveUp(error)),
        }
    }

    async fn register_signal(&self, spec: SignalSpec, port_snapshot: Value) -> WeftResult<()> {
        self.state
            .registered_signals
            .lock()
            .unwrap()
            .push((spec, port_snapshot));
        Ok(())
    }

    fn own_infra(
        &self,
        name: &str,
        _instance: Option<&crate::instance::InstanceId>,
    ) -> WeftResult<crate::infra::InfraHandle> {
        // The node under test has one shared copy of its infra here.
        Ok(crate::infra::InfraHandle::new(NODE_UNDER_TEST_ID, name, None))
    }

    fn shared(&self) -> Arc<crate::shared::Shared> {
        self.state.shared.clone()
    }

    async fn endpoint_address(
        &self,
        infra: &crate::infra::InfraHandle,
    ) -> WeftResult<crate::infra::EndpointAddress> {
        let key = (infra.place().to_string(), infra.endpoint().to_string());
        let url = self.state.endpoints.lock().unwrap().get(&key).cloned().ok_or_else(|| {
            let (place, name) = &key;
            WeftError::Config(if place == NODE_UNDER_TEST_ID {
                format!(
                    "the node asked for its '{name}' endpoint but the test declared no address \
                     for it; declare one with rig.declare_endpoint(\"{name}\", \"http://..\") \
                     before rig.run(..)"
                )
            } else {
                format!(
                    "the node asked for the '{name}' endpoint of '{place}' but the test declared \
                     no address for it; declare one with rig.declare_infra(\"{place}\", \"{name}\", \
                     \"http://..\") before rig.run(..)"
                )
            })
        })?;
        let public_url = self.state.public_urls.lock().unwrap().get(&key).cloned();
        Ok(crate::infra::EndpointAddress { url, public_url })
    }

    async fn endpoint_call(
        &self,
        url: &str,
        method: EndpointMethod,
        path: &str,
        body: Option<Value>,
    ) -> WeftResult<Value> {
        // Which endpoint this URL belongs to. Production builds the
        // request from the base address, so a node calling the wrong
        // one of its endpoints reaches a different service; recording
        // the name is what lets a test see that.
        let key = self
            .state
            .endpoints
            .lock()
            .unwrap()
            .iter()
            .find(|(_, declared)| declared.as_str() == url)
            .map(|(key, _)| key.clone())
            .unwrap_or_else(|| (url.to_string(), url.to_string()));
        let (place, endpoint) = key.clone();
        self.state.endpoint_calls.lock().unwrap().push(EndpointCall {
            place: place.clone(),
            endpoint: endpoint.clone(),
            method,
            path: path.to_string(),
            body,
        });
        self.state.press_stop_if_due();
        let answer = self
            .state
            .endpoint_answers
            .lock()
            .unwrap()
            .get_mut(&(key, method, path.to_string()))
            .and_then(VecDeque::pop_front);
        match answer {
            Some(CannedAnswer::Body(v)) => Ok(v),
            // Worded exactly as production words it, so a node that
            // matches on a refusal is tested against the real text.
            // SYNC: refusal wording <-> crates/weft-engine/src/context.rs endpoint_call
            Some(CannedAnswer::Refusal { status, body }) => Err(WeftError::Runtime(
                anyhow::anyhow!("endpoint_call {url}{path} returned {status}: {body}"),
            )),
            None if place == NODE_UNDER_TEST_ID => Err(WeftError::Config(format!(
                "the node called {method:?} {path} on its '{endpoint}' endpoint but the \
                 test has no answer left for it; declare one with rig.answer_endpoint(..) \
                 per expected call, before rig.run(..)"
            ))),
            None => Err(WeftError::Config(format!(
                "the node called {method:?} {path} on the '{endpoint}' endpoint of '{place}' \
                 but the test has no answer left for it; declare one with rig.answer_infra(..) \
                 per expected call, before rig.run(..)"
            ))),
        }
    }

    async fn run_step(&self, name: &str) -> WeftResult<(u32, Option<Value>)> {
        // The rig's journal replayed by the one reading production
        // uses: a step an earlier run of this node recorded is replayed
        // by call index, name unread; an await there, or another index,
        // is the drift production refuses.
        let call_index = self.next_call_index.fetch_add(1, Ordering::SeqCst);
        let replayed = replay_run(&mut self.awaited_sequence.lock().unwrap(), name, call_index)?;
        Ok((call_index, replayed))
    }

    async fn run_record(&self, name: &str, call_index: u32, value: &Value) -> WeftResult<()> {
        // Appended in the order the records land, as production's
        // journal rows are; the index travels on the entry.
        self.record(AwaitedEntry {
            call_index,
            kind: AwaitedEntryKind::Run { name: name.to_string(), value: value.clone() },
        });
        Ok(())
    }

    /// Publishing in the fake rig writes the values into the same map
    /// `open_connection` reads, so a node that publishes and then opens
    /// what it published behaves as it does in production. The service
    /// comes from the node's own `publishes` declaration, exactly as it
    /// does in a real run, so a node that declares nothing fails here
    /// the same way.
    async fn publish_access(&self, values: BTreeMap<String, String>) -> WeftResult<Access> {
        let service = self.published_service()?;
        self.state
            .connection_values
            .lock()
            .unwrap()
            .insert(service.clone(), values);
        self.state.published.lock().unwrap().insert(service.clone());
        Ok(Access::new("fake-connection", service, None))
    }

    async fn published_access(&self) -> WeftResult<Option<Access>> {
        let service = self.published_service()?;
        Ok(self
            .state
            .published
            .lock()
            .unwrap()
            .contains(&service)
            .then(|| Access::new("fake-connection", service, None)))
    }

    async fn open_connection(
        &self,
        access: &Access,
        _window: std::time::Duration,
    ) -> WeftResult<OpenedConnection> {
        let service = access.service();
        let values = self
            .state
            .connection_values
            .lock()
            .unwrap()
            .get(service)
            .cloned()
            .unwrap_or_default();
        // The consuming input's required stored values are data the
        // node will read, so a missing one always fails loud, exactly
        // like production's resolution-time check.
        for name in access.required_values() {
            if !values.contains_key(name) {
                return Err(WeftError::Config(format!(
                    "the '{service}' connection stores no value '{name}' but this node's \
                     access input requires it; declare it with \
                     rig.connection_value(\"{service}\", \"{name}\", ...) before the run"
                )));
            }
        }
        // Required permissions are only checkable against a DECLARED
        // granted set; a service with no declaration skips the check.
        if let Some(granted) = self.state.connection_permissions.lock().unwrap().get(service) {
            for permission in access.required_permissions() {
                if !granted.contains(permission) {
                    return Err(WeftError::Config(format!(
                        "the '{service}' connection lacks permission '{permission}' but \
                         this node's access input requires it; grant it with \
                         rig.connection_permissions(\"{service}\", &[...]) before the run"
                    )));
                }
            }
        }
        let client = self.state.canned_client();
        Ok(OpenedConnection::assemble(
            service,
            values,
            Vec::new(),
            None,
            CredentialOwner::Author,
            client,
            Arc::new(NoFakeSocket),
        ))
    }

    async fn log(&self, level: LogLevel, message: String) -> WeftResult<()> {
        self.state.logs.lock().unwrap().push((level, message));
        Ok(())
    }

    async fn tag_execution(&self, tags: Vec<String>) -> WeftResult<()> {
        self.state.execution_tags.lock().unwrap().push(tags);
        Ok(())
    }

    async fn stop_tagged(&self, tag: String, stop_self: crate::tag::StopSelf) -> WeftResult<()> {
        self.state.stops.lock().unwrap().push((tag, stop_self));
        Ok(())
    }

    async fn program_call(&self, call: crate::program::ProgramCall, stop_self: crate::tag::StopSelf, _call_index: u32) -> WeftResult<Value> {
        let name = call.journal_name();
        // A call that only acts, left unanswered by the test, answers the
        // way the runtime answers one it carried out: a start is queued, a
        // copy taken down, a change of values set nothing up again.
        let unanswered = match call {
            crate::program::ProgramCall::InfraStart { .. } => serde_json::to_value(crate::program::InfraStartAnswer::Started),
            crate::program::ProgramCall::InfraStop { .. } | crate::program::ProgramCall::InfraTerminate { .. } => {
                serde_json::to_value(crate::program::InfraDownAnswer::TakenDown)
            }
            crate::program::ProgramCall::ValuesChange { .. } | crate::program::ProgramCall::ValuesForget { .. } => {
                serde_json::to_value(crate::instance_door::ValuesChanged::default())
            }
            _ => Ok(serde_json::json!({})),
        }
        .expect("a program call answer serializes");
        let acts_only = matches!(
            call,
            crate::program::ProgramCall::InfraStart { .. }
                | crate::program::ProgramCall::InfraStop { .. }
                | crate::program::ProgramCall::InfraTerminate { .. }
                | crate::program::ProgramCall::TriggerActivate { .. }
                | crate::program::ProgramCall::TriggerDeactivate { .. }
                | crate::program::ProgramCall::ValuesChange { .. }
                | crate::program::ProgramCall::ValuesForget { .. }
        );
        self.state.program_calls.lock().unwrap().push((call, stop_self));
        if let Some(answer) = self.state.program_answers.lock().unwrap().get_mut(name).and_then(VecDeque::pop_front) {
            return Ok(answer);
        }
        if acts_only {
            return Ok(unanswered);
        }
        Err(WeftError::Config(format!(
            "the node called {name}, and the test declared no answer for it: \
             rig.answer_program_call(\"{name}\", ..) before the run"
        )))
    }

    async fn mint_instance_token(
        &self,
        instance: &crate::instance::InstanceId,
        expires_in_secs: u64,
        _displays: bool,
        id: uuid::Uuid,
    ) -> WeftResult<crate::program::MintedInstanceToken> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs();
        let expires_at_unix = crate::signal_token::expiry_at(now, expires_in_secs).map_err(WeftError::Config)?;
        let mut minted = self.state.minted_tokens.lock().unwrap();
        minted.push(MintedToken { instance: instance.clone(), expires_in_secs, id });
        Ok(crate::program::MintedInstanceToken { id, token: format!("wft-test-token-{}", minted.len()), expires_at_unix })
    }

    fn cancellation(&self) -> Arc<CancellationFlag> {
        self.state.cancellation.clone()
    }

    fn declared_output_ports(&self) -> &HashMap<String, WeftType> {
        &self.capture.declared
    }

    fn declared_input_ports(&self) -> &HashMap<String, WeftType> {
        &self.declared_inputs
    }

    fn wired_output_ports(&self) -> &HashSet<String> {
        &self.capture.wired
    }

    fn catches_errors(&self) -> bool {
        self.capture.catch_errors
    }

    async fn pulse_downstream(&self, output: NodeOutput, _wait_delivered: bool) -> WeftResult<()> {
        // No downstream graph in a rig run: the harness is the
        // consumer and takes every yield instantly, so a delivery-
        // waiting emission resolves immediately and a plain one leaves
        // nothing un-taken.
        self.capture.pulse(output)
    }

    fn set_max_buffered_items(&self, port: &str, items: usize) -> WeftResult<()> {
        self.capture.set_max_buffered_items(port, items)
    }

    async fn close_port(&self, port: &str) -> WeftResult<()> {
        self.capture.close_port(port)
    }

    fn create_bus(
        &self,
        opts: crate::bus::BusOptions,
    ) -> WeftResult<(crate::bus::BusHandle, Value)> {
        // The REAL in-process bus, held in the rig's registry so the
        // marker resolves back (`ctx.bus`, and the test's own
        // `rig.bus` read after the run).
        let handle = crate::bus::BusHandle::create_with_options(opts)
            .map_err(|e| WeftError::NodeExecution(format!("create bus: {e}")))?;
        let marker = handle.marker();
        self.state
            .buses
            .lock()
            .unwrap()
            .insert(marker.to_string(), handle.new_handle());
        Ok((handle, marker))
    }

    fn bus(&self, marker: &Value) -> WeftResult<crate::bus::BusHandle> {
        self.state
            .buses
            .lock()
            .unwrap()
            .get(&marker.to_string())
            .map(|h| h.new_handle())
            .ok_or_else(|| {
                WeftError::NodeExecution(
                    "no bus behind this marker (the run never opened one)".to_string(),
                )
            })
    }

    async fn storage_put(
        &self,
        scope: &crate::storage::StorageScope,
        identity: Option<&str>,
        data: crate::storage::ByteStream,
        mime_type: &str,
        filename: &str,
        keep: Option<crate::storage::KeepTtl>,
        _declared_size: Option<u64>,
    ) -> WeftResult<Value> {
        let bytes = crate::storage::collect_stream(data)
            .await
            .map_err(|e| WeftError::NodeExecution(format!("fake storage put: {e}")))?;
        let scope = self.state.resolve_scope(scope)?;
        if keep.is_some() && scope == crate::storage::StorageScope::Asset {
            return Err(WeftError::NodeExecution(
                "an asset's lifetime follows the source that references it; it takes no keep".into(),
            ));
        }
        if let Some(stored) = identity.and_then(|identity| self.state.identified(&scope, identity)) {
            return Ok(stored);
        }
        let identity_key = identity.map(|i| (scope.clone(), i.to_string()));
        let key = self.state.mint_storage_key(&scope, filename);
        // As the store records it: the keep flag is the end-of-run
        // exemption only an execution file has; the lifetime applies in
        // every scope.
        let meta = crate::storage::StoredFileMeta {
            key: key.clone(),
            mime_type: mime_type.to_string(),
            size_bytes: bytes.len() as u64,
            filename: filename.to_string(),
            keep: keep.is_some() && scope == crate::storage::StorageScope::Execution,
            expires_at_unix: None,
            keep_ttl_secs: keep.and_then(crate::storage::KeepTtl::secs),
            created_at_unix: 0,
            version: crate::storage::FIRST_FILE_VERSION,
        };
        let stored = crate::storage::StoredFile::from(&meta);
        if let Some(identity_key) = identity_key {
            self.state.identities.lock().unwrap().push((identity_key, meta.key.clone()));
        }
        self.state
            .storage
            .lock()
            .unwrap()
            .insert(meta.key.clone(), StoredEntry { scope, meta, bytes });
        Ok(stored.to_value())
    }

    async fn storage_put_from_url(
        &self,
        scope: &crate::storage::StorageScope,
        identity: Option<&str>,
        url: &str,
        filename: Option<&str>,
        keep: Option<crate::storage::KeepTtl>,
    ) -> WeftResult<Value> {
        // An identified fetch the fake already holds costs no request,
        // like production: the canned route is not even consulted, so
        // a test can count requests to prove a second ask pulled nothing.
        let resolved = self.state.resolve_scope(scope)?;
        if let Some(stored) = identity.and_then(|identity| self.state.identified(&resolved, identity)) {
            return Ok(stored);
        }
        // Answered from the SAME canned routes every other fake call
        // uses, so a fetch stays offline: declare the URL's route with
        // `rig.respond*` and the "download" lands in fake storage.
        let resp = self
            .state
            .canned_client()
            .get(url)
            .send()
            .await
            .map_err(|e| WeftError::NodeExecution(format!("fake put_from_url: {e}")))?;
        if !resp.status().is_success() {
            return Err(WeftError::NodeExecution(format!(
                "fake put_from_url: the canned route answered {}",
                resp.status()
            )));
        }
        // Mime and filename derive through the same core helpers
        // production funnels a fetched URL through, so a fake-passing
        // node stores the same metadata production would.
        let mime = crate::storage::normalize_content_type(
            resp.headers().get("content-type").and_then(|v| v.to_str().ok()),
        );
        // The caller's name, then the one the canned route serves in
        // its Content-Disposition, then the URL: the same order as
        // production, so a test sees the name a node would.
        let name = filename
            .filter(|f| !f.is_empty())
            .map(str::to_string)
            .or_else(|| {
                resp.headers()
                    .get("content-disposition")
                    .and_then(|v| v.to_str().ok())
                    .and_then(crate::storage::filename_from_disposition)
            })
            .unwrap_or_else(|| crate::storage::filename_from_url(url));
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| WeftError::NodeExecution(format!("fake put_from_url body: {e}")))?;
        self.storage_put(scope, identity, crate::storage::bytes_stream(bytes), &mime, &name, keep, None)
            .await
    }

    async fn storage_get(
        &self,
        key: &str,
        range: Option<crate::storage::ByteRange>,
    ) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)> {
        if range.is_some() {
            return Err(unsupported("range reads"));
        }
        let store = self.state.storage.lock().unwrap();
        let entry = store.get(key).ok_or_else(|| {
            WeftError::NodeExecution(format!("fake storage holds no file at key '{key}'"))
        })?;
        Ok((entry.meta.clone(), crate::storage::bytes_stream(entry.bytes.clone())))
    }

    async fn storage_get_url(
        &self,
        _url: &str,
        _declared_mime: &str,
        _declared_filename: &str,
        _declared_size: u64,
        _range: Option<crate::storage::ByteRange>,
    ) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)> {
        Err(unsupported("url-backed file reads (they fetch a real URL; fake tests stay offline)"))
    }

    async fn storage_delete(&self, key: &str) -> WeftResult<()> {
        match self.state.storage.lock().unwrap().remove(key) {
            Some(_) => {
                self.state.identities.lock().unwrap().retain(|(_, minted)| minted != key);
                Ok(())
            }
            None => Err(WeftError::NodeExecution(format!(
                "fake storage holds no file at key '{key}'"
            ))),
        }
    }

    async fn storage_list(
        &self,
        scope: &crate::storage::StorageScope,
    ) -> WeftResult<Vec<crate::storage::StoredFileMeta>> {
        self.state.files_in(scope)
    }

    async fn storage_replace(
        &self,
        key: &str,
        expected_version: Option<u64>,
        data: crate::storage::ByteStream,
        _declared_size: Option<u64>,
    ) -> WeftResult<crate::storage::ReplaceOutcome> {
        let bytes = crate::storage::collect_stream(data)
            .await
            .map_err(|e| WeftError::NodeExecution(format!("fake storage replace: {e}")))?;
        let mut store = self.state.storage.lock().unwrap();
        let entry = store.get_mut(key).ok_or_else(|| {
            WeftError::NodeExecution(format!("fake storage holds no file at key '{key}'"))
        })?;
        if entry.scope == crate::storage::StorageScope::Asset {
            return Err(WeftError::NodeExecution(
                "an asset is managed by the pre-build asset sync; node code never replaces one".into(),
            ));
        }
        // The fake writes in one step, so no write is ever in flight
        // (never `Busy`); the version check is the store's.
        if expected_version.is_some_and(|v| v != entry.meta.version) {
            return Ok(crate::storage::ReplaceOutcome::Stale);
        }
        // Same key, scope, name, type and lifetime; new content, size
        // and version.
        entry.meta.size_bytes = bytes.len() as u64;
        entry.meta.version += 1;
        entry.bytes = bytes;
        Ok(crate::storage::ReplaceOutcome::Replaced(crate::storage::StoredFile::from(&entry.meta)))
    }

    async fn record_file_edit(&self, edit: crate::storage::FileEdit) -> WeftResult<()> {
        self.state.file_edits.lock().unwrap().push(edit);
        Ok(())
    }

    async fn storage_keep(&self, key: &str, ttl: crate::storage::KeepTtl) -> WeftResult<()> {
        match self.state.storage.lock().unwrap().get_mut(key) {
            Some(entry) => {
                match entry.scope {
                    crate::storage::StorageScope::Asset => {
                        return Err(WeftError::NodeExecution(
                            "an asset's lifetime follows the source that references it; it takes no keep".into(),
                        ))
                    }
                    crate::storage::StorageScope::Execution => entry.meta.keep = true,
                    _ => {}
                }
                entry.meta.keep_ttl_secs = ttl.secs();
                Ok(())
            }
            None => Err(WeftError::NodeExecution(format!(
                "fake storage holds no file at key '{key}'"
            ))),
        }
    }

    async fn storage_presign(&self, _key: &str, _ttl_secs: Option<u64>) -> WeftResult<String> {
        Err(unsupported("presigned URLs (there is no real bucket behind the fake)"))
    }

    async fn storage_public_link(
        &self,
        key: &str,
        _ttl_secs: Option<u64>,
        reach: crate::storage::LinkReach,
    ) -> WeftResult<Option<String>> {
        match reach {
            // The honest answer for a store nobody on the internet can
            // reach: no public link. Callers (externalize) fall back to
            // inline bytes.
            crate::storage::LinkReach::Internet => Ok(None),
            // A caller of the fake install: on the address its request
            // came in on (the attached caller's `base_url`), else a stable
            // fake address, so a test can assert the link a door hands out.
            crate::storage::LinkReach::Caller { base } => Ok(Some(format!(
                "{}/public/files/{key}",
                base.as_deref().unwrap_or(FAKE_CALLER_LINK_BASE)
            ))),
        }
    }

    fn wake_payload(&self) -> Option<&Value> {
        self.wake.as_ref()
    }

    fn caller_connection(&self) -> Option<Arc<dyn crate::caller::CallerConnection>> {
        self.state.caller.lock().unwrap().clone()
    }
}

/// Sockets have no canned form yet.
struct NoFakeSocket;

#[async_trait::async_trait]
impl crate::access::socket::SocketDial for NoFakeSocket {
    async fn dial(&self, _url: &str) -> WeftResult<crate::access::socket::ProviderSocket> {
        Err(unsupported("provider WebSocket sessions"))
    }
}

/// The middleware that IS the fake provider: matches the request
/// against the declared routes and answers directly, no network. Every
/// request is recorded first, matched or not.
struct CannedAnswerMiddleware {
    state: Arc<FakeState>,
}

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for CannedAnswerMiddleware {
    async fn handle(
        &self,
        req: reqwest::Request,
        _extensions: &mut http::Extensions,
        _next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let method = req.method().as_str().to_ascii_uppercase();
        let path = req.url().path().to_string();
        let query = req.url().query().map(str::to_string);
        let body_bytes = req.body().and_then(|b| b.as_bytes());
        let body_streamed = req.body().is_some() && body_bytes.is_none();
        let body_text = body_bytes.map(|b| String::from_utf8_lossy(b).into_owned());
        let body = body_text.as_deref().and_then(|t| serde_json::from_str(t).ok());
        let headers = req
            .headers()
            .iter()
            .map(|(k, v)| {
                (k.as_str().to_ascii_lowercase(), String::from_utf8_lossy(v.as_bytes()).into_owned())
            })
            .collect();
        self.state.requests.lock().unwrap().push(SentRequest {
            method: method.clone(),
            path: path.clone(),
            query: query.clone(),
            body_text,
            body,
            body_streamed,
            headers,
        });
        self.state.press_stop_if_due();

        // Matching runs on CANONICAL keys (decoded, order-normalized
        // query multisets), so parameter order and encoding never
        // matter: an exact query match wins; a query-less request
        // takes the bare declaration; a request WITH a query that
        // matched no queried declaration falls back to bare only when
        // no queried sibling shares the path (once pages are
        // distinguished by query, an unmatched query must fail loud,
        // never silently receive the wrong page).
        let routes = self.state.routes.lock().unwrap();
        let req_key = RouteKey {
            method: method.clone(),
            path: path.clone(),
            query: query.as_deref().map(query_params),
        };
        let bare_key = RouteKey { method: method.clone(), path: path.clone(), query: None };
        let exact = routes.get(&req_key).cloned();
        let bare = routes.get(&bare_key).cloned();
        let queried_siblings: Vec<String> = routes
            .keys()
            .filter(|k| k.method == method && k.path == path && k.query.is_some())
            .map(|k| format!("{path}?{}", render_query(k.query.as_ref().unwrap())))
            .collect();
        drop(routes);
        let canned = exact.or_else(|| {
            if req_key.query.is_some() && !queried_siblings.is_empty() {
                None
            } else {
                bare
            }
        });
        let Some(canned) = canned else {
            let declared = if queried_siblings.is_empty() {
                String::new()
            } else {
                format!("; declared on this path: {queried_siblings:?}")
            };
            return Err(reqwest_middleware::Error::Middleware(anyhow::anyhow!(
                "the fake rig has no canned response for {method} {path}{}{declared}; \
                 declare one with rig.respond(\"{method}\", \"{path}\", json!(..))",
                query.map(|q| format!("?{q}")).unwrap_or_default(),
            )));
        };
        let canned = match canned {
            CannedRoute::Respond(canned) => canned,
            CannedRoute::FailConnection => {
                return Err(reqwest_middleware::Error::Middleware(anyhow::anyhow!(
                    "error sending request for url ({}): connection refused (the rig's \
                     fail_connection for {method} {path})",
                    req.url()
                )));
            }
        };
        let mut response = http::Response::builder()
            .status(canned.status)
            .header(http::header::CONTENT_TYPE, canned.content_type.as_str());
        for (name, value) in &canned.headers {
            response = response.header(name.as_str(), value.as_str());
        }
        let response = response
            .body(canned.body.to_vec())
            .map_err(|e| {
                reqwest_middleware::Error::Middleware(anyhow::anyhow!(
                    "build canned response: {e}"
                ))
            })?;
        Ok(reqwest::Response::from(response))
    }
}

// ----- The live rig ---------------------------------------------------

/// Which actor a live-rig handle serves. STATED by the caller, never
/// inferred from the node-type string: the role decides the handle's
/// firing identity (the node under test vs the harness), which the
/// runtime's parked-body watchdog and error attribution key on, and a
/// node type that merely LOOKS like a harness sentinel must not be
/// misfiled.
pub enum HandleRole<'a> {
    /// The node body under test.
    NodeUnderTest { node_type: &'a str },
    /// A harness-side helper (a bus/storage/connect seed the TEST CODE
    /// drives, not the body).
    Harness { node_type: &'static str },
}

/// The two bodies a rig runs for a node, each with a journal of its
/// own on the rig, as each is its own execution in production.
const RUN_BODY: &str = "run";
const SETUP_TRIGGER_BODY: &str = "setup_trigger";

/// The firing id every [`HandleRole::NodeUnderTest`] handle runs as.
/// One definition: the runtime's parked-body watchdog keys its
/// in-flight set on exactly this id, and the rigs' error messages name
/// it, so a drifted copy would silently blind the watchdog.
pub const NODE_UNDER_TEST_ID: &str = "node-under-test";

/// The base the fake rig mints caller links under (`StorageHandle::
/// caller_link`): a stable address a test asserts against.
pub const FAKE_CALLER_LINK_BASE: &str = "https://fake-weft.test";

/// The firing id every [`HandleRole::Harness`] handle runs as, keeping
/// the test code's own waits out of the watchdog's picture.
pub const NODE_TEST_HARNESS_ID: &str = "node-test-harness";

/// Builds the production `ContextHandle` for one live-rig actor, from
/// its role, its declared output map, and whether it declares a
/// Generator input (so the production `await_signal` guard applies in
/// a live test exactly as in an execution). The runtime composes it
/// (broker clients, throwaway execution identity, no-op journal) and
/// hands it to the runner; weft-core only names the seam so
/// [`NodeTest`] can be declared without depending on the runtime
/// crate.
pub type LiveHandleFactory = Arc<
    dyn Fn(HandleRole<'_>, HashMap<String, WeftType>, bool) -> WeftResult<Arc<dyn ContextHandle>>
        + Send
        + Sync,
>;

/// The live-tier harness handle: same surface shape as [`FakeRig`],
/// but `run` goes through the PRODUCTION handle (real connection
/// resolution, relaying, metering, billing). The rig carries the
/// resolved grant for the test's declared service.
pub struct LiveRig {
    factory: LiveHandleFactory,
    access: Access,
    /// The `WEFT_NODE_TEST_*` values the run was handed, by full name.
    fixtures: std::collections::BTreeMap<String, String>,
    /// Output ports the case wires downstream, as [`FakeRig::wire_output`].
    wired_outputs: Mutex<HashSet<String>>,
}

impl LiveRig {
    /// Composed by the runtime's test runner, never by test code.
    pub fn new(factory: LiveHandleFactory, access: Access, fixtures: std::collections::BTreeMap<String, String>) -> Self {
        Self { factory, access, fixtures, wired_outputs: Mutex::new(HashSet::new()) }
    }

    /// Wire output `port` downstream for the runs that follow, as
    /// [`FakeRig::wire_output`] does on the fake tier.
    pub fn wire_output(&self, port: &str) {
        self.wired_outputs.lock().unwrap().insert(port.to_string());
    }

    /// The connection marker for the test's declared service, to place
    /// on the node's access input:
    /// `inputs = json!({"account": rig.access("exa"), ...})`. Takes the
    /// service like [`FakeRig::access`] (a fake test converts to live
    /// without editing its input block) and asserts it matches the
    /// test's DECLARED service: the runner resolved a grant for that
    /// one, and quietly handing it out under another name would sign
    /// the wrong provider's calls.
    pub fn access(&self, service: &str) -> Value {
        assert_eq!(
            service,
            self.access.service(),
            "this live test declared service '{}'; rig.access(\"{service}\") asks for a \
             grant the runner did not resolve",
            self.access.service(),
        );
        self.access.to_value()
    }

    /// A live fixture: a value the test cannot self-provision in the
    /// connected account (a chat id the tester's bot may message, a
    /// mailbox address). Answers `WEFT_NODE_TEST_<name>` as the run was
    /// handed it (the CLI forwards every such variable of its own
    /// environment with the test); a missing one fails the test naming
    /// exactly what to set.
    pub fn fixture(&self, name: &str) -> WeftResult<String> {
        let var = format!("WEFT_NODE_TEST_{name}");
        match self.fixtures.get(&var) {
            Some(v) if !v.is_empty() => Ok(v.clone()),
            _ => Err(crate::error::node_error(format!(
                "live fixture {var} is not set; add it to the environment (the repo \
                 .env for scripted runs) and re-run"
            ))),
        }
    }

    /// Seed a REAL stored file (the production storage behind the ctx)
    /// and get its stored-file value, to place on a file input. The
    /// live twin of [`FakeRig::store_file`].
    pub async fn store_file(&self,
        filename: &str,
        mime_type: &str,
        bytes: impl Into<Vec<u8>>,
    ) -> WeftResult<Value> {
        // A storage seed emits nothing, so it declares no outputs.
        let handle = (self.factory)(
            HandleRole::Harness { node_type: "NodeTestStorageSeed" },
            HashMap::new(),
            false,
        )?;
        handle
            .storage_put(
                &crate::storage::StorageScope::Execution,
                None,
                crate::storage::bytes_stream(bytes::Bytes::from(bytes.into())),
                mime_type,
                filename,
                None,
                None,
            )
            .await
    }

    /// Open the test's declared-service grant through the PRODUCTION
    /// handle and hand back the [`OpenedConnection`], so a test can
    /// make its own signed provider calls (`conn.client()`) for setup
    /// and teardown around the node under test: create a resource
    /// before running the node, delete what the node created after.
    /// The node itself is still driven through [`Self::run`]. Uses the
    /// default work window, exactly like a node's `ctx.open`; the
    /// runner's settle releases the lease with every other connection
    /// the run opened.
    pub async fn connect(&self) -> WeftResult<OpenedConnection> {
        // A setup/teardown connection emits nothing, so its handle
        // declares no outputs.
        let handle = (self.factory)(
            HandleRole::Harness { node_type: "NodeTestConnect" },
            HashMap::new(),
            false,
        )?;
        handle
            .open_connection(&self.access, crate::context::DEFAULT_PROVIDER_WINDOW)
            .await
    }

    /// Mint a REAL bus and get `(writer, marker)`, so a test can place
    /// the marker on a Bus-typed input and write frames into it from
    /// test code (register a name on the handle, `send`/`send_bytes`,
    /// then `close` so the node sees the stream end). The live twin of
    /// a producer node's `ctx.create_bus`: the marker resolves inside
    /// [`Self::run`] because every handle the rig mints shares one bus
    /// registry.
    pub fn bus(
        &self,
        opts: crate::bus::BusOptions,
    ) -> WeftResult<(crate::bus::BusHandle, Value)> {
        // A bus seed emits nothing, so its handle declares no outputs.
        let handle = (self.factory)(
            HandleRole::Harness { node_type: "NodeTestBusSeed" },
            HashMap::new(),
            false,
        )?;
        handle.create_bus(opts)
    }

    /// Run the node's `run` body against the production handle,
    /// capturing outputs at the ctx seam (there is no downstream graph
    /// in a node test, so pulses are recorded, not routed).
    pub async fn run(&self, node: &dyn Node, inputs: Value) -> RunOutcome {
        let manifest = node.manifest();
        // Keeps the run's generator feeds registered (see the fake
        // rig's `run` for why this binding must outlive the body).
        let (bag, _feeds_alive) = match manifest_input_bag(manifest, &[], inputs) {
            Ok(pair) => pair,
            Err(e) => {
                return RunOutcome { result: Err(e), outputs: Default::default(), closed_ports: Vec::new(), infra_spec: None }
            }
        };
        let has_generator_input = manifest.has_generator_input();
        // From the BAG, not the raw case inputs, for the same reason as
        // the fake rig: the node reads the defaulted bag, so the output
        // map must be derived from it too.
        let config = match bag.object() {
            Ok(obj) => Value::Object(obj.clone()),
            Err(e) => {
                return RunOutcome { result: Err(e), outputs: Default::default(), closed_ports: Vec::new(), infra_spec: None }
            }
        };
        let outputs_by_name = declared_output_map(manifest, &config);
        let inner = match (self.factory)(
            HandleRole::NodeUnderTest { node_type: &manifest.node_type },
            outputs_by_name.clone(),
            has_generator_input,
        ) {
            Ok(handle) => handle,
            Err(e) => {
                return RunOutcome { result: Err(e), outputs: Default::default(), closed_ports: Vec::new(), infra_spec: None }
            }
        };
        let handle = Arc::new(CapturingHandle {
            inner,
            capture: Capture::new(
                outputs_by_name,
                self.wired_outputs.lock().unwrap().clone(),
                manifest.features.catch_errors,
            ),
            declared_inputs: declared_input_map(manifest, &[]),
        });
        let ctx = test_context(manifest, bag, None, handle.clone());
        let result = node.run(ctx).await;
        CaptureBox::Live(handle).into_outcome(result)
    }
}

/// A `ContextHandle` that records emissions and delegates everything
/// else to the production handle. The live rig's capture layer: a node
/// test has no downstream graph, so `pulse_downstream` becomes a
/// record instead of a routed pulse, while every other capability
/// (connections, storage, logs, signals) is the real one.
struct CapturingHandle {
    inner: Arc<dyn ContextHandle>,
    capture: Capture,
    declared_inputs: HashMap<String, WeftType>,
}

#[async_trait::async_trait]
impl ContextHandle for CapturingHandle {
    async fn await_signal(&self, spec: SignalSpec) -> WeftResult<Value> {
        // Emissions are captured HERE, so the inner production
        // handle's own emitted-then-suspend guard never sees them;
        // apply it at the capture layer where the truth lives.
        if self.capture.has_mentioned_a_port() {
            return Err(WeftError::NodeExecution(
                crate::context::emitted_then_await_signal_error(NODE_UNDER_TEST_ID),
            ));
        }
        self.inner.await_signal(spec).await
    }

    async fn register_signal(&self, spec: SignalSpec, port_snapshot: Value) -> WeftResult<()> {
        self.inner.register_signal(spec, port_snapshot).await
    }

    fn own_infra(
        &self,
        name: &str,
        instance: Option<&crate::instance::InstanceId>,
    ) -> WeftResult<crate::infra::InfraHandle> {
        self.inner.own_infra(name, instance)
    }

    fn shared(&self) -> Arc<crate::shared::Shared> {
        self.inner.shared()
    }

    async fn endpoint_address(
        &self,
        infra: &crate::infra::InfraHandle,
    ) -> WeftResult<crate::infra::EndpointAddress> {
        self.inner.endpoint_address(infra).await
    }

    async fn endpoint_call(
        &self,
        url: &str,
        method: EndpointMethod,
        path: &str,
        body: Option<Value>,
    ) -> WeftResult<Value> {
        self.inner.endpoint_call(url, method, path, body).await
    }

    async fn run_step(&self, name: &str) -> WeftResult<(u32, Option<Value>)> {
        self.inner.run_step(name).await
    }

    async fn run_record(&self, name: &str, call_index: u32, value: &Value) -> WeftResult<()> {
        self.inner.run_record(name, call_index, value).await
    }

    async fn open_connection(
        &self,
        access: &Access,
        window: std::time::Duration,
    ) -> WeftResult<OpenedConnection> {
        self.inner.open_connection(access, window).await
    }

    async fn publish_access(&self, values: BTreeMap<String, String>) -> WeftResult<Access> {
        self.inner.publish_access(values).await
    }

    async fn published_access(&self) -> WeftResult<Option<Access>> {
        self.inner.published_access().await
    }

    // A delegating wrapper forwards EVERY trait method, including
    // defaulted ones: the default is only right for a leaf handle, and
    // silently taking it here would detach live-test plain HTTP from
    // whatever the production handle answers.
    fn plain_http(&self) -> reqwest_middleware::ClientWithMiddleware {
        self.inner.plain_http()
    }

    async fn log(&self, level: LogLevel, message: String) -> WeftResult<()> {
        self.inner.log(level, message).await
    }

    async fn tag_execution(&self, tags: Vec<String>) -> WeftResult<()> {
        self.inner.tag_execution(tags).await
    }

    async fn stop_tagged(&self, tag: String, stop_self: crate::tag::StopSelf) -> WeftResult<()> {
        self.inner.stop_tagged(tag, stop_self).await
    }

    async fn program_call(&self, call: crate::program::ProgramCall, stop_self: crate::tag::StopSelf, call_index: u32) -> WeftResult<Value> {
        self.inner.program_call(call, stop_self, call_index).await
    }

    async fn mint_instance_token(
        &self,
        instance: &crate::instance::InstanceId,
        expires_in_secs: u64,
        displays: bool,
        id: uuid::Uuid,
    ) -> WeftResult<crate::program::MintedInstanceToken> {
        self.inner.mint_instance_token(instance, expires_in_secs, displays, id).await
    }

    fn cancellation(&self) -> Arc<CancellationFlag> {
        self.inner.cancellation()
    }

    fn declared_output_ports(&self) -> &HashMap<String, WeftType> {
        &self.capture.declared
    }

    fn declared_input_ports(&self) -> &HashMap<String, WeftType> {
        &self.declared_inputs
    }

    fn wired_output_ports(&self) -> &HashSet<String> {
        &self.capture.wired
    }

    fn catches_errors(&self) -> bool {
        self.capture.catch_errors
    }

    async fn pulse_downstream(&self, output: NodeOutput, _wait_delivered: bool) -> WeftResult<()> {
        // Same stance as the fake handle: the harness takes every
        // yield instantly, so a delivery wait resolves immediately.
        self.capture.pulse(output)
    }

    fn set_max_buffered_items(&self, port: &str, items: usize) -> WeftResult<()> {
        self.capture.set_max_buffered_items(port, items)
    }

    async fn close_port(&self, port: &str) -> WeftResult<()> {
        self.capture.close_port(port)
    }

    fn create_bus(
        &self,
        opts: crate::bus::BusOptions,
    ) -> WeftResult<(crate::bus::BusHandle, Value)> {
        self.inner.create_bus(opts)
    }

    fn bus(&self, marker: &Value) -> WeftResult<crate::bus::BusHandle> {
        self.inner.bus(marker)
    }

    async fn storage_put(
        &self,
        scope: &crate::storage::StorageScope,
        identity: Option<&str>,
        data: crate::storage::ByteStream,
        mime_type: &str,
        filename: &str,
        keep: Option<crate::storage::KeepTtl>,
        declared_size: Option<u64>,
    ) -> WeftResult<Value> {
        self.inner
            .storage_put(scope, identity, data, mime_type, filename, keep, declared_size)
            .await
    }

    async fn storage_put_from_url(
        &self,
        scope: &crate::storage::StorageScope,
        identity: Option<&str>,
        url: &str,
        filename: Option<&str>,
        keep: Option<crate::storage::KeepTtl>,
    ) -> WeftResult<Value> {
        self.inner.storage_put_from_url(scope, identity, url, filename, keep).await
    }

    async fn storage_get(
        &self,
        key: &str,
        range: Option<crate::storage::ByteRange>,
    ) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)> {
        self.inner.storage_get(key, range).await
    }

    async fn storage_get_url(
        &self,
        url: &str,
        declared_mime: &str,
        declared_filename: &str,
        declared_size: u64,
        range: Option<crate::storage::ByteRange>,
    ) -> WeftResult<(crate::storage::StoredFileMeta, crate::storage::ByteStream)> {
        self.inner
            .storage_get_url(url, declared_mime, declared_filename, declared_size, range)
            .await
    }

    async fn storage_delete(&self, key: &str) -> WeftResult<()> {
        self.inner.storage_delete(key).await
    }

    async fn storage_list(
        &self,
        scope: &crate::storage::StorageScope,
    ) -> WeftResult<Vec<crate::storage::StoredFileMeta>> {
        self.inner.storage_list(scope).await
    }

    async fn storage_replace(
        &self,
        key: &str,
        expected_version: Option<u64>,
        data: crate::storage::ByteStream,
        declared_size: Option<u64>,
    ) -> WeftResult<crate::storage::ReplaceOutcome> {
        self.inner.storage_replace(key, expected_version, data, declared_size).await
    }

    async fn record_file_edit(&self, edit: crate::storage::FileEdit) -> WeftResult<()> {
        self.inner.record_file_edit(edit).await
    }

    async fn storage_keep(&self, key: &str, ttl: crate::storage::KeepTtl) -> WeftResult<()> {
        self.inner.storage_keep(key, ttl).await
    }

    async fn storage_presign(&self, key: &str, ttl_secs: Option<u64>) -> WeftResult<String> {
        self.inner.storage_presign(key, ttl_secs).await
    }

    async fn storage_public_link(
        &self,
        key: &str,
        ttl_secs: Option<u64>,
        reach: crate::storage::LinkReach,
    ) -> WeftResult<Option<String>> {
        self.inner.storage_public_link(key, ttl_secs, reach).await
    }

    fn wake_payload(&self) -> Option<&Value> {
        self.inner.wake_payload()
    }

    fn caller_connection(&self) -> Option<Arc<dyn crate::caller::CallerConnection>> {
        self.inner.caller_connection()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::node_error;
    use serde_json::json;

    /// A hand-built manifest (no metadata.json on disk for a test-only
    /// node), leaked for the 'static the trait wants.
    fn manifest() -> &'static NodeMetadata {
        static MANIFEST: std::sync::OnceLock<NodeMetadata> = std::sync::OnceLock::new();
        MANIFEST.get_or_init(|| {
            serde_json::from_value(json!({
                "type": "RigProbe",
                "label": "Rig probe",
                "description": "test-only node",
                "inputs": [
                    {"name": "account", "type": "Access", "required": false},
                    {"name": "text", "type": "String", "required": false,
                     "default": "default-text"}
                ],
                "outputs": [
                    {"name": "reply", "type": "JsonDict"},
                    {"name": "done", "type": "Boolean"}
                ]
            }))
            .expect("probe manifest")
        })
    }

    /// A scripted node exercising the rig's surfaces: opens the
    /// connection, posts, emits the reply + done.
    struct ProbeNode;

    impl crate::node::NodeManifest for ProbeNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }

    #[async_trait::async_trait]
    impl Node for ProbeNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let access: Access = ctx.inputs.get("account")?;
            let text: String = ctx.inputs.get("text")?;
            let client = ctx.client(&access).await?;
            let resp = crate::access::client::post_json(
                &client,
                "https://provider.example/api/send",
                &json!({"text": text}),
                "send the message",
            )
            .await?;
            ctx.pulse_downstream(NodeOutput::new().set("reply", resp).set("done", true))
                .await
        }
    }

    /// The happy path: canned route answers, request recorded with its
    /// body, outputs captured, defaults filled from the manifest.
    #[tokio::test]
    async fn fake_rig_answers_canned_routes_and_captures_outputs() {
        let rig = FakeRig::new();
        rig.respond("POST", "/api/send", json!({"ok": true, "id": "m1"}));
        let outcome = rig
            .run(&ProbeNode, json!({"account": rig.access("probe")}))
            .await
            .ok()
            .expect("probe run succeeds");
        assert_eq!(outcome.outputs["reply"], json!({"ok": true, "id": "m1"}));
        assert_eq!(outcome.outputs["done"], json!(true));

        rig.assert_sent("POST", "/api/send");
        let sent = rig.requests();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].body.as_ref().expect("json body"),
            &json!({"text": "default-text"}),
            "the manifest default filled the absent input"
        );
    }

    /// A node polling a long job: asks for the status until it is done,
    /// racing each wait against a stop.
    struct PollingNode;
    impl crate::node::NodeManifest for PollingNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }

    #[async_trait::async_trait]
    impl Node for PollingNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let cancelled = ctx.cancellation();
            loop {
                let status = crate::access::client::get_json(
                    &ctx.http(),
                    "https://provider.example/api/job",
                    "poll the job",
                )
                .await?;
                if status["done"] == json!(true) {
                    return ctx.pulse_downstream(NodeOutput::new().set("done", true)).await;
                }
                tokio::select! {
                    err = cancelled.cancelled_err() => return Err(err),
                    _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {}
                }
            }
        }
    }

    /// A stop pressed mid-poll ends the run cancelled after exactly the
    /// calls made before it; a stop pressed up front ends it before the
    /// first poll is answered past its select.
    #[tokio::test]
    async fn stop_after_calls_cancels_a_polling_node() {
        let rig = FakeRig::new();
        rig.respond("GET", "/api/job", json!({"done": false}));
        rig.stop_after_calls(3);
        let outcome = rig.run(&PollingNode, json!({})).await;
        assert!(
            matches!(outcome.result, Err(crate::error::WeftError::Cancelled)),
            "the run ended cancelled: {:?}",
            outcome.result
        );
        assert_eq!(rig.requests().len(), 3, "the node stopped polling at the stop");
        assert!(outcome.outputs.is_empty(), "a stopped run emits nothing");

        let rig = FakeRig::new();
        rig.respond("GET", "/api/job", json!({"done": false}));
        rig.stop_after_calls(0);
        let outcome = rig.run(&PollingNode, json!({})).await;
        assert!(matches!(outcome.result, Err(crate::error::WeftError::Cancelled)));
        assert_eq!(rig.requests().len(), 1, "one poll went out before the select saw the stop");
    }

    /// A node that steers its siblings: the fake records the tags it put
    /// on the run and every stop it asked for (with the self choice),
    /// stops nothing (there are no siblings here), and refuses a bad tag
    /// before recording anything.
    struct SteeringNode;
    impl crate::node::NodeManifest for SteeringNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for SteeringNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let text: String = ctx.inputs.get("text")?;
            ctx.tag_execution([text.as_str(), "batch_a"]).await?;
            ctx.stop_tagged(text.as_str(), crate::tag::StopSelf::Keep).await?;
            ctx.stop_tagged("batch_a", crate::tag::StopSelf::Include).await?;
            // An empty tag is refused at the ctx, before the handle
            // sees it (any other string is normalized into a tag). So
            // is an empty tag list: "at least one tag" is part of the
            // ctx contract, not only the broker's.
            let bad = ctx.tag_execution([""]).await;
            assert!(matches!(bad, Err(WeftError::Input(_))), "{bad:?}");
            let empty = ctx.tag_execution(Vec::<String>::new()).await;
            assert!(matches!(empty, Err(WeftError::Input(_))), "{empty:?}");
            ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
        }
    }

    #[tokio::test]
    async fn fake_rig_records_tags_and_stops() {
        let rig = FakeRig::new();
        rig.run(&SteeringNode, json!({"text": "user_7"}))
            .await
            .ok()
            .expect("steering run succeeds");
        assert_eq!(
            rig.execution_tags(),
            vec![vec!["user_7".to_string(), "batch_a".to_string()]],
            "the refused tag was never recorded"
        );
        assert_eq!(
            rig.stops(),
            vec![
                ("user_7".to_string(), crate::tag::StopSelf::Keep),
                ("batch_a".to_string(), crate::tag::StopSelf::Include),
            ]
        );
    }

    /// An undeclared route is a loud middleware error naming the fix,
    /// surfaced through the node's own error path.
    #[tokio::test]
    async fn fake_rig_fails_loud_on_an_undeclared_route() {
        let rig = FakeRig::new();
        let outcome = rig.run(&ProbeNode, json!({"account": rig.access("probe")})).await;
        let err = outcome.result.expect_err("no canned route declared").to_string();
        assert!(err.contains("no canned response for POST /api/send"), "{err}");
        assert_eq!(rig.requests().len(), 1, "the request was still recorded");
    }

    /// A route declared to fail at the connection answers no response at
    /// all: the node's send errors, and the request is still recorded.
    #[tokio::test]
    async fn fake_rig_fails_a_call_at_the_connection() {
        let rig = FakeRig::new();
        rig.fail_connection("POST", "/api/send");
        let outcome = rig.run(&ProbeNode, json!({"account": rig.access("probe")})).await;
        let err = outcome.result.expect_err("an unreachable server fails the call").to_string();
        assert!(err.contains("connection refused"), "{err}");
        assert_eq!(rig.requests().len(), 1, "the request was still recorded");
    }

    /// A node exercising the storage verbs a fake run supports beyond a
    /// plain put: an expiring project file, an in-place replace, and a
    /// caller link.
    struct StorageProbe;
    impl crate::node::NodeManifest for StorageProbe {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for StorageProbe {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let storage = ctx.storage(crate::storage::StorageScope::Project);
            let put = storage
                .put(&b"v1"[..], "text/plain", "a.txt", Some(crate::storage::KeepTtl::Secs { secs: 60 }))
                .await?;
            let handle = crate::storage::FileHandle::from_value(&put)?;
            let replaced = storage.replace(&handle, &b"version two"[..]).await?;
            let link = storage.caller_link(&handle, None).await?;
            ctx.pulse_downstream(
                NodeOutput::new().set("reply", json!({"put": put, "replaced": replaced, "link": link})),
            )
            .await
        }
    }

    #[tokio::test]
    async fn fake_rig_replaces_in_place_expires_anywhere_and_links_on_the_callers_address() {
        let rig = FakeRig::new();
        let conn = crate::caller::FakeCallerConnection::connected(crate::caller::CallerRuntimeConfig {
            protocol: crate::signal::Protocol::Http,
            data_type: crate::signal::DataType::Json,
            backpressure: crate::signal::Backpressure::Block,
            error_mode: crate::signal::ErrorMode::Surface,
            max_inbound_bytes: 1024,
            caller_silence_secs: crate::signal::DEFAULT_CALLER_SILENCE_SECS,
            max_session_secs: 0,
            outlives_caller: false,
            inbound_window: crate::caller::DEFAULT_INBOUND_WINDOW,
            journal: crate::stream_journal::JournalPolicy::default(),
        });
        conn.set_handshake(crate::caller::LiveRequest {
            method: "GET".into(),
            path: "file".into(),
            base_url: Some("http://127.0.0.1:14111".into()),
            ..Default::default()
        });
        rig.attach_caller(conn);
        let outcome = rig.run(&StorageProbe, json!({})).await.ok().expect("the storage probe runs");
        let reply = &outcome.outputs["reply"];
        let put = crate::storage::StoredFile::from_value(&reply["put"]).unwrap();
        let replaced = crate::storage::StoredFile::from_value(&reply["replaced"]).unwrap();
        assert_eq!(replaced.key, put.key, "a replace keeps the key");
        assert_eq!((put.size_bytes, replaced.size_bytes), (2, 11));
        assert_eq!(rig.stored_bytes(&put.key).unwrap().as_ref(), b"version two");
        let meta = rig.stored_meta(&put.key).unwrap();
        assert_eq!((meta.keep, meta.keep_ttl_secs), (false, Some(60)), "a project file expires, unflagged");
        assert_eq!(rig.stored_files(&crate::storage::StorageScope::Project).unwrap().len(), 1);
        assert_eq!(
            reply["link"],
            json!(format!("http://127.0.0.1:14111/public/files/{}", put.key)),
            "the link rides the address the caller came in on"
        );
    }

    /// A canned non-2xx status exercises the node's refusal handling.
    #[tokio::test]
    async fn fake_rig_serves_canned_error_statuses() {
        let rig = FakeRig::new();
        rig.respond_status("POST", "/api/send", 403, json!({"error": "forbidden"}));
        let outcome = rig.run(&ProbeNode, json!({"account": rig.access("probe")})).await;
        let err = outcome.result.expect_err("403 surfaces").to_string();
        assert!(err.contains("403"), "{err}");
    }

    /// A node whose provider answers with the created id in a HEADER
    /// and an empty body (LinkedIn's rest.li create is the shipped
    /// case): the happy path is only testable if the rig can say so.
    struct HeaderReadingNode;
    impl crate::node::NodeManifest for HeaderReadingNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for HeaderReadingNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let access: Access = ctx.inputs.get("account")?;
            let client = ctx.client(&access).await?;
            let resp = client
                .post("https://provider.example/api/create")
                .send()
                .await
                .map_err(node_error)?;
            let id = resp
                .headers()
                .get("x-restli-id")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
                .ok_or_else(|| node_error("the create answered without an id header"))?;
            ctx.pulse_downstream(NodeOutput::new().set("reply", json!({"id": id})).set("done", true))
                .await
        }
    }

    #[tokio::test]
    async fn a_canned_response_can_carry_headers() {
        let rig = FakeRig::new();
        rig.respond_with_headers(
            "POST",
            "/api/create",
            201,
            "application/json",
            &[("x-restli-id", "urn:li:share:42")],
            Vec::new(),
        );
        let outcome = rig
            .run(&HeaderReadingNode, json!({"account": rig.access("probe")}))
            .await
            .ok()
            .expect("the node reads the header");
        assert_eq!(outcome.outputs["reply"], json!({"id": "urn:li:share:42"}));
    }

    #[test]
    #[should_panic(expected = "content type")]
    fn a_content_type_header_is_refused_beside_the_content_type_argument() {
        let rig = FakeRig::new();
        rig.respond_with_headers(
            "GET",
            "/page",
            200,
            "application/json",
            &[("Content-Type", "text/plain")],
            Vec::new(),
        );
    }

    /// A node that publishes through `ctx.run`, the exactly-once step:
    /// the provider call is the step's work, and the emitted id is
    /// whatever the step answered.
    struct PublishingNode;
    impl crate::node::NodeManifest for PublishingNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for PublishingNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let access: Access = ctx.inputs.get("account")?;
            let client = ctx.client(&access).await?;
            let published = ctx
                .run("publish", || async {
                    crate::access::client::post_json(
                        &client,
                        "https://provider.example/api/send",
                        &json!({"text": "hello"}),
                        "publish the post",
                    )
                    .await
                })
                .await?;
            ctx.pulse_downstream(NodeOutput::new().set("reply", published).set("done", true))
                .await
        }
    }

    /// The rig's recorded steps are the journal: a second run on the
    /// same rig is a resume, and the provider is not called again. This
    /// is the property a node that posts on somebody's behalf exists to
    /// keep, and the one its tests could not prove before.
    #[tokio::test]
    async fn a_recorded_step_is_replayed_by_the_next_run_on_the_same_rig() {
        let rig = FakeRig::new();
        rig.respond("POST", "/api/send", json!({"id": "post-1"}));
        let first = rig
            .run(&PublishingNode, json!({"account": rig.access("probe")}))
            .await
            .ok()
            .expect("first run publishes");
        let second = rig
            .run(&PublishingNode, json!({"account": rig.access("probe")}))
            .await
            .ok()
            .expect("second run replays");

        assert_eq!(first.outputs["reply"], json!({"id": "post-1"}));
        assert_eq!(second.outputs["reply"], first.outputs["reply"], "the replay answers the same");
        assert_eq!(rig.requests().len(), 1, "the provider was called once across two runs");
        assert_eq!(
            rig.recorded_steps(&PublishingNode),
            vec![AwaitedEntry {
                call_index: 0,
                kind: AwaitedEntryKind::Run { name: "publish".into(), value: json!({"id": "post-1"}) },
            }]
        );
    }

    /// Two node types on one rig are two executions in production, with
    /// a journal each: the second never replays the first's steps.
    #[tokio::test]
    async fn another_node_on_the_same_rig_has_its_own_journal() {
        let rig = FakeRig::new();
        rig.respond("POST", "/api/send", json!({"id": "post-1"}));
        rig.signal(json!({"ok": true}));
        rig.run(&AskThenPublishNode { ask: true }, json!({"account": rig.access("probe")}))
            .await
            .ok()
            .expect("the asking node parks, is answered, publishes");
        let other = rig
            .run(&PublishingNode, json!({"account": rig.access("probe")}))
            .await
            .ok()
            .expect("a different node runs its own body, replaying nothing");
        assert_eq!(other.outputs["reply"], json!({"id": "post-1"}));
        assert_eq!(rig.requests().len(), 2, "each node made its own call");
        assert!(rig.recorded_steps(&PublishingNode).iter().all(|entry| matches!(entry.kind, AwaitedEntryKind::Run { .. })));
    }

    /// A body that parks on a signal and then publishes: production
    /// numbers the await and the run on one counter, and a resume
    /// replays both. The rig does the same, so the second run needs no
    /// second signal, calls the provider once, and a body whose call
    /// order changed is refused where production would refuse it.
    struct AskThenPublishNode {
        /// `false` is the same node with its await edited out: the
        /// drifted body a resume must refuse.
        ask: bool,
    }
    impl crate::node::NodeManifest for AskThenPublishNode {
        fn manifest(&self) -> &'static NodeMetadata {
            asking_manifest()
        }
    }
    fn asking_manifest() -> &'static NodeMetadata {
        static MANIFEST: std::sync::OnceLock<NodeMetadata> = std::sync::OnceLock::new();
        MANIFEST.get_or_init(|| {
            let mut m = manifest().clone();
            m.node_type = "RigAsker".into();
            m
        })
    }
    #[async_trait::async_trait]
    impl Node for AskThenPublishNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let access: Access = ctx.inputs.get("account")?;
            let client = ctx.client(&access).await?;
            let answer = if self.ask {
                ctx.await_signal(crate::signal::timer::Timer {
                    spec: crate::signal::timer::TimerSpec::After { duration_ms: 1 },
                })
                .await?
            } else {
                json!(true)
            };
            let published = ctx
                .run("publish", || async {
                    crate::access::client::post_json(
                        &client,
                        "https://provider.example/api/send",
                        &json!({"approved": answer}),
                        "publish the post",
                    )
                    .await
                })
                .await?;
            ctx.pulse_downstream(NodeOutput::new().set("reply", published).set("done", true))
                .await
        }
    }

    #[tokio::test]
    async fn an_await_and_a_run_share_one_index_and_both_replay() {
        let rig = FakeRig::new();
        rig.respond("POST", "/api/send", json!({"id": "post-1"}));
        rig.signal(json!({"ok": true}));
        rig.run(&AskThenPublishNode { ask: true }, json!({"account": rig.access("probe")}))
            .await
            .ok()
            .expect("first run parks, is answered, publishes");
        // No second signal queued: the resume replays the first answer.
        let second = rig
            .run(&AskThenPublishNode { ask: true }, json!({"account": rig.access("probe")}))
            .await
            .ok()
            .expect("second run replays the answer and the publish");
        assert_eq!(second.outputs["reply"], json!({"id": "post-1"}));
        assert_eq!(rig.requests().len(), 1, "one provider call across the two runs");
        let steps = rig.recorded_steps(&AskThenPublishNode { ask: true });
        assert_eq!(steps.len(), 2, "one entry per call, replays add none: {steps:?}");
        assert!(
            matches!(&steps[0].kind, AwaitedEntryKind::Await { ended: Some(AwaitEnd::Answered { value }), .. } if value == &json!({"ok": true}))
                && steps[0].call_index == 0,
            "the await is index 0, as production numbers it: {steps:?}"
        );
        assert_eq!(
            steps[1],
            AwaitedEntry {
                call_index: 1,
                kind: AwaitedEntryKind::Run { name: "publish".into(), value: json!({"id": "post-1"}) },
            },
            "and the run index 1"
        );

        // The same node with its await edited out runs where it awaited:
        // a drifted body, refused with production's own words.
        let err = rig
            .run(&AskThenPublishNode { ask: false }, json!({"account": rig.access("probe")}))
            .await
            .result
            .expect_err("a run where an await was journaled is a drifted body")
            .to_string();
        assert!(err.contains("journal has Await"), "{err}");
    }

    #[test]
    #[should_panic(expected = "not a valid header name")]
    fn a_bad_header_name_is_refused_at_declaration() {
        let rig = FakeRig::new();
        rig.respond_with_headers("GET", "/page", 200, "application/json", &[("no spaces allowed", "x")], Vec::new());
    }

    struct DoubleEmitNode;
    impl crate::node::NodeManifest for DoubleEmitNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for DoubleEmitNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("done", true)).await?;
            ctx.pulse_downstream(NodeOutput::new().set("done", false)).await
        }
    }

    struct UndeclaredPortNode;
    impl crate::node::NodeManifest for UndeclaredPortNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for UndeclaredPortNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("nope", 1)).await
        }
    }

    /// The rig enforces the production emission contract: one mention
    /// per port, declared ports only.
    #[tokio::test]
    async fn fake_rig_enforces_the_emission_contract() {
        let rig = FakeRig::new();
        let err = rig
            .run(&DoubleEmitNode, json!({}))
            .await
            .result
            .expect_err("double emit refused")
            .to_string();
        assert!(err.contains("twice"), "{err}");

        let err = rig
            .run(&UndeclaredPortNode, json!({}))
            .await
            .result
            .expect_err("undeclared port refused")
            .to_string();
        assert!(err.contains("undeclared output port 'nope'"), "{err}");
    }

    struct WrongTypeNode;
    impl crate::node::NodeManifest for WrongTypeNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for WrongTypeNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            // `done` is declared Boolean; a string must be refused.
            ctx.pulse_downstream(NodeOutput::new().set("done", "yes")).await
        }
    }

    /// The rig enforces production's runtime output-type check: a
    /// value the declared type does not accept fails the run loud.
    #[tokio::test]
    async fn fake_rig_enforces_the_output_type_check() {
        let rig = FakeRig::new();
        let err = rig
            .run(&WrongTypeNode, json!({}))
            .await
            .result
            .expect_err("wrong-typed emission refused")
            .to_string();
        assert!(err.contains("port 'done'"), "{err}");
        assert!(err.contains("does not accept"), "{err}");
    }

    /// A manifest whose access input requires stored values, for the
    /// connection-requirements gate below.
    fn requiring_manifest() -> &'static NodeMetadata {
        static MANIFEST: std::sync::OnceLock<NodeMetadata> = std::sync::OnceLock::new();
        MANIFEST.get_or_init(|| {
            serde_json::from_value(json!({
                "type": "RequiringProbe",
                "label": "Requiring probe",
                "description": "test-only node",
                "inputs": [
                    {"name": "account", "type": "Access", "required": true,
                     "requiresValues": ["smtp_host"],
                     "requiresScopes": ["mail.send"]}
                ],
                "outputs": [{"name": "done", "type": "Boolean"}]
            }))
            .expect("requiring manifest")
        })
    }

    struct RequiringNode;
    impl crate::node::NodeManifest for RequiringNode {
        fn manifest(&self) -> &'static NodeMetadata {
            requiring_manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for RequiringNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let access: Access = ctx.inputs.get("account")?;
            let _conn = ctx.open(&access).await?;
            ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
        }
    }

    /// Opening a connection checks the input's required stored values
    /// (always) and its required permissions (only once a granted set
    /// is declared for the service).
    #[tokio::test]
    async fn open_connection_enforces_required_values_and_permissions() {
        // Missing required value: loud, names the declaring call.
        let rig = FakeRig::new();
        let err = rig
            .run(&RequiringNode, json!({"account": rig.access("mail")}))
            .await
            .result
            .expect_err("missing value refused")
            .to_string();
        assert!(err.contains("no value 'smtp_host'"), "{err}");
        assert!(err.contains("rig.connection_value(\"mail\", \"smtp_host\""), "{err}");

        // Value present, no permissions declared: the permission check
        // is skipped and the run succeeds.
        let rig = FakeRig::new();
        rig.connection_value("mail", "smtp_host", "smtp.example.com");
        rig.run(&RequiringNode, json!({"account": rig.access("mail")}))
            .await
            .ok()
            .expect("no declared granted set skips the permission check");

        // A declared granted set missing the required permission: loud.
        let rig = FakeRig::new();
        rig.connection_value("mail", "smtp_host", "smtp.example.com");
        rig.connection_permissions("mail", &["mail.read"]);
        let err = rig
            .run(&RequiringNode, json!({"account": rig.access("mail")}))
            .await
            .result
            .expect_err("missing permission refused")
            .to_string();
        assert!(err.contains("lacks permission 'mail.send'"), "{err}");

        // The granted set covering the requirement passes.
        let rig = FakeRig::new();
        rig.connection_value("mail", "smtp_host", "smtp.example.com");
        rig.connection_permissions("mail", &["mail.read", "mail.send"]);
        rig.run(&RequiringNode, json!({"account": rig.access("mail")}))
            .await
            .ok()
            .expect("covered requirements pass");
    }

    struct AwaitingNode;
    impl crate::node::NodeManifest for AwaitingNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for AwaitingNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let answer = ctx
                .await_signal(crate::signal::timer::Timer {
                    spec: crate::signal::timer::TimerSpec::After { duration_ms: 1 },
                })
                .await?;
            ctx.pulse_downstream(NodeOutput::new().set("reply", answer)).await
        }
    }

    /// A canned signal resumes an awaiting node; an empty queue is a
    /// loud error naming the missing declaration.
    #[tokio::test]
    async fn canned_signals_resume_an_awaiting_node() {
        let rig = FakeRig::new();
        rig.signal(json!({"answer": 42}));
        let outcome = rig
            .run(&AwaitingNode, json!({}))
            .await
            .ok()
            .expect("resumes with the canned payload");
        assert_eq!(outcome.outputs["reply"], json!({"answer": 42}));

        let rig = FakeRig::new();
        let err = rig
            .run(&AwaitingNode, json!({}))
            .await
            .result
            .expect_err("empty queue is loud")
            .to_string();
        assert!(err.contains("declared no payload"), "{err}");
    }

    /// A stream-consuming test node: drains its `rows` stream and
    /// emits the gathered items, or (when `awaits`) calls
    /// `await_signal` instead, which the rig must refuse.
    struct StreamConsumerNode {
        awaits: bool,
    }
    impl crate::node::NodeManifest for StreamConsumerNode {
        fn manifest(&self) -> &'static NodeMetadata {
            static MANIFEST: std::sync::OnceLock<NodeMetadata> = std::sync::OnceLock::new();
            MANIFEST.get_or_init(|| {
                serde_json::from_value(json!({
                    "type": "RigStreamConsumer",
                    "label": "Rig stream consumer",
                    "description": "test-only stream consumer",
                    "inputs": [
                        {"name": "rows", "type": "Generator[Number]", "required": true}
                    ],
                    "outputs": [{"name": "reply", "type": "JsonDict"}]
                }))
                .expect("stream consumer manifest")
            })
        }
    }
    #[async_trait::async_trait]
    impl Node for StreamConsumerNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            if self.awaits {
                ctx.await_signal(crate::signal::timer::Timer {
                    spec: crate::signal::timer::TimerSpec::After { duration_ms: 1 },
                })
                .await?;
                return Ok(());
            }
            let rows = ctx.inputs.get::<crate::generator::Generator<f64>>("rows")?;
            let mut taken = Vec::new();
            while let Some(n) = rows.next().await? {
                taken.push(n);
            }
            ctx.pulse_downstream(NodeOutput::new().set("reply", json!({ "taken": taken })))
                .await
        }
    }

    /// The acceptance test for the rig's generator-input path: the
    /// seeded array becomes a LIVE feed the body's pull loop drains
    /// (the marker must stay resolvable for the whole run).
    #[tokio::test]
    async fn a_generator_input_feeds_the_bodys_pull_loop() {
        let rig = FakeRig::new();
        let outcome = rig
            .run(&StreamConsumerNode { awaits: false }, json!({"rows": [1, 2, 3]}))
            .await
            .ok()
            .expect("the consumer drains the seeded stream");
        assert_eq!(outcome.outputs["reply"], json!({"taken": [1.0, 2.0, 3.0]}));
    }

    /// A stream consumer's `await_signal` is refused with the exact
    /// production error, whatever the signal queue holds.
    #[tokio::test]
    async fn a_stream_consumers_await_signal_is_refused() {
        let rig = FakeRig::new();
        rig.signal(json!({"answer": 42}));
        let err = rig
            .run(&StreamConsumerNode { awaits: true }, json!({"rows": [1]}))
            .await
            .result
            .expect_err("a stream consumer cannot durably suspend")
            .to_string();
        assert!(
            err.contains(&crate::context::stream_consumer_await_signal_error(
                NODE_UNDER_TEST_ID
            )),
            "{err}"
        );
    }

    /// A body that emits then awaits is refused with the exact
    /// production error (a durable suspension would re-emit on replay).
    struct EmitThenAwaitNode;
    impl crate::node::NodeManifest for EmitThenAwaitNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for EmitThenAwaitNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("done", json!(true))).await?;
            ctx.await_signal(crate::signal::timer::Timer {
                spec: crate::signal::timer::TimerSpec::After { duration_ms: 1 },
            })
            .await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn an_emit_then_await_is_refused_like_production() {
        let rig = FakeRig::new();
        rig.signal(json!({"answer": 42}));
        let err = rig
            .run(&EmitThenAwaitNode, json!({}))
            .await
            .result
            .expect_err("emit-then-durable-suspend would re-emit on replay")
            .to_string();
        assert!(
            err.contains(&crate::context::emitted_then_await_signal_error(
                NODE_UNDER_TEST_ID
            )),
            "{err}"
        );
    }

    struct WakeEchoNode;
    impl crate::node::NodeManifest for WakeEchoNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for WakeEchoNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let event: Value = ctx.wake.get("event")?;
            ctx.pulse_downstream(NodeOutput::new().set("reply", event)).await
        }
    }

    /// `rig.wake` emulates a trigger firing: the payload lands on
    /// `ctx.wake` for exactly one run.
    #[tokio::test]
    async fn a_wake_payload_reaches_the_firing_trigger() {
        let rig = FakeRig::new();
        rig.wake(json!({"event": {"kind": "message"}}));
        let outcome = rig
            .run(&WakeEchoNode, json!({}))
            .await
            .ok()
            .expect("wake fields readable");
        assert_eq!(outcome.outputs["reply"], json!({"kind": "message"}));

        // Consumed: a second run has no wake payload.
        let outcome = rig.run(&WakeEchoNode, json!({})).await;
        assert!(outcome.result.is_err(), "no wake on the second run");
    }

    struct StorageRoundTripNode;
    impl crate::node::NodeManifest for StorageRoundTripNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for StorageRoundTripNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let storage = ctx.storage(crate::storage::StorageScope::Execution);
            let stored = storage.put(bytes::Bytes::from_static(b"hello"), "text/plain", "h.txt", None).await?;
            let handle = crate::storage::FileHandle::from_value(&stored)
                .map_err(|e| node_error(format!("parse stored value: {e}")))?;
            let (meta, bytes) = storage.get_bytes(&handle).await?;
            ctx.pulse_downstream(
                NodeOutput::new()
                    .set("reply", json!({
                        "text": String::from_utf8_lossy(&bytes),
                        "mime": meta.mime_type,
                    }))
                    .set("done", true),
            )
            .await
        }
    }

    /// In-memory storage round-trips bytes and metadata.
    #[tokio::test]
    async fn fake_storage_round_trips() {
        let rig = FakeRig::new();
        let outcome = rig
            .run(&StorageRoundTripNode, json!({}))
            .await
            .ok()
            .expect("round trip");
        assert_eq!(
            outcome.outputs["reply"],
            json!({"text": "hello", "mime": "text/plain"})
        );
    }

    /// The module's central promise: a panicking test body fails THAT
    /// test (an error carrying the assertion's own message), never the
    /// runner.
    #[tokio::test]
    async fn a_panicking_body_fails_its_own_test_only() {
        let basic = NodeTest::basic("panics", || panic!("left != right: 1 vs 2"));
        let err = basic.run_basic().expect_err("panic becomes a failure").to_string();
        assert!(err.contains("'panics' panicked"), "{err}");
        assert!(err.contains("left != right"), "the assertion message survives: {err}");

        let fake = NodeTest::fake("fake_panics", |_rig| async {
            assert_eq!(1, 2, "fake body assertion");
            Ok(())
        });
        let err = fake.run_fake().await.expect_err("async panic too").to_string();
        assert!(err.contains("panicked"), "{err}");
    }

    /// The other half of that promise: a test that never RETURNS fails
    /// by name too. Before this, the suite stalled and whoever killed
    /// it got no test name at all, so the only way to find the culprit
    /// was to run them one at a time.
    #[tokio::test]
    async fn a_hanging_body_fails_by_name_instead_of_stalling_the_suite() {
        let hangs = NodeTest::fake("hangs", |_rig| async {
            std::future::pending::<()>().await;
            Ok(())
        });
        let err = hangs
            .run_fake_within(std::time::Duration::from_millis(20))
            .await
            .expect_err("a hang is a failure")
            .to_string();
        assert!(err.contains("'hangs'"), "the culprit is named: {err}");
        assert!(err.contains("nothing closes"), "it says what usually causes it: {err}");
    }

    /// Running a test through the wrong tier's runner is a loud
    /// config error naming both tiers.
    #[tokio::test]
    async fn tier_mismatch_is_a_loud_error() {
        let fake = NodeTest::fake("f", |_rig| async { Ok(()) });
        let err = fake.run_basic().expect_err("fake is not basic").to_string();
        assert!(err.contains("fake-tier, not basic"), "{err}");

        let basic = NodeTest::basic("b", || Ok(()));
        let err = basic.run_fake().await.expect_err("basic is not fake").to_string();
        assert!(err.contains("basic-tier, not fake"), "{err}");
        let err = basic
            .run_live(LiveRig::new(
                Arc::new(|_, _, _| Err(WeftError::Config("unused".into()))),
                Access::new("c", "svc", None),
                Default::default(),
            ))
            .await
            .expect_err("basic is not live")
            .to_string();
        assert!(err.contains("basic-tier, not live"), "{err}");
    }

    /// Declaring the same (method, path) twice is a loud refusal, not
    /// a silent overwrite.
    #[test]
    #[should_panic(expected = "already declared")]
    fn duplicate_canned_routes_are_refused() {
        let rig = FakeRig::new();
        rig.respond("GET", "/page", json!({"n": 1}));
        rig.respond("GET", "/page", json!({"n": 2}));
    }

    /// A canned 3xx would hand the node a raw redirect production's
    /// client never surfaces (it follows them); refused at declaration.
    #[test]
    #[should_panic(expected = "does not model redirects")]
    fn canned_redirect_statuses_are_refused() {
        let rig = FakeRig::new();
        rig.respond_status("GET", "/page", 302, json!({}));
    }

    /// Canonicalization is byte-exact: two opaque binary tokens that
    /// both decode to invalid UTF-8 stay DISTINCT declarations (a
    /// text-lossy canonical form would merge them and panic here).
    #[test]
    fn binary_query_tokens_stay_distinct() {
        let rig = FakeRig::new();
        rig.respond("GET", "/page?cursor=%FF%FE", json!({"n": 1}));
        rig.respond("GET", "/page?cursor=%FE%FF", json!({"n": 2}));
    }

    /// A node using the PLAIN client (`ctx.http()`) is answered from
    /// the same canned routes: a fake run can never reach the real
    /// network.
    struct PlainHttpNode;
    impl crate::node::NodeManifest for PlainHttpNode {
        fn manifest(&self) -> &'static NodeMetadata {
            manifest()
        }
    }
    #[async_trait::async_trait]
    impl Node for PlainHttpNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let resp = ctx
                .http()
                .get("https://public.example/feed?page=2&size=10")
                .send()
                .await
                .map_err(|e| node_error(format!("plain fetch: {e}")))?;
            let body: Value = resp.json().await.map_err(|e| node_error(format!("json: {e}")))?;
            ctx.pulse_downstream(NodeOutput::new().set("reply", body).set("done", true)).await
        }
    }

    /// Plain HTTP is canned, and a queried declaration matches by
    /// query PARAMETER SET (order never matters); an unmatched query
    /// on a path with queried declarations fails loud instead of
    /// receiving the wrong page.
    #[tokio::test]
    async fn plain_http_is_canned_and_queries_match_as_sets() {
        let rig = FakeRig::new();
        // Declared in the opposite parameter order: still matches.
        rig.respond("GET", "/feed?size=10&page=2", json!({"page": 2}));
        let outcome = rig.run(&PlainHttpNode, json!({})).await.ok().expect("plain canned");
        assert_eq!(outcome.outputs["reply"], json!({"page": 2}));

        // A different query on the same path: loud, names the declared
        // pages, never answers the wrong one.
        let rig = FakeRig::new();
        rig.respond("GET", "/feed?size=10&page=3", json!({"page": 3}));
        let outcome = rig.run(&PlainHttpNode, json!({})).await;
        let err = outcome.result.expect_err("unmatched query is loud").to_string();
        assert!(err.contains("declared on this path"), "{err}");

        // With no canned route at all, the plain call is refused (it
        // must never fall through to the network).
        let rig = FakeRig::new();
        let outcome = rig.run(&PlainHttpNode, json!({})).await;
        let err = outcome.result.expect_err("no route, no network").to_string();
        assert!(err.contains("no canned response for GET /feed"), "{err}");
    }

    /// A node that reaches an endpoint another infra node shared: the
    /// `Infra` handle on its `service` input, one action through it.
    struct SharedInfraNode;
    impl crate::node::NodeManifest for SharedInfraNode {
        fn manifest(&self) -> &'static NodeMetadata {
            static MANIFEST: std::sync::OnceLock<NodeMetadata> = std::sync::OnceLock::new();
            MANIFEST.get_or_init(|| {
                serde_json::from_value(json!({
                    "type": "SharedInfraProbe",
                    "label": "Shared infra probe",
                    "description": "test-only node",
                    "inputs": [{"name": "service", "type": "Infra", "required": true}],
                    "outputs": [{"name": "reply", "type": "JsonDict"}]
                }))
                .expect("shared infra manifest")
            })
        }
    }
    #[async_trait::async_trait]
    impl Node for SharedInfraNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let service: crate::infra::InfraHandle = ctx.inputs.get("service")?;
            let reply = ctx.endpoint_of(&service).await?.action("ping", json!({"n": 1})).await?;
            ctx.pulse_downstream(NodeOutput::new().set("reply", reply)).await
        }
    }

    /// Another node's endpoint resolves through the same seam as the
    /// node's own, an action speaks the envelope, and the refusal a
    /// service answers with a 200 fails the node naming the action.
    #[tokio::test]
    async fn a_shared_infra_handle_resolves_and_speaks_the_action_envelope() {
        let rig = FakeRig::new();
        let service = rig.declare_infra("whatsapp", "api", "http://whatsapp:8090");
        rig.answer_infra("whatsapp", "api", EndpointMethod::Post, "/action", json!({"result": {"pong": 1}}));
        let outcome = rig.run(&SharedInfraNode, json!({"service": service.clone()})).await.ok().expect("resolves");
        assert_eq!(outcome.outputs["reply"], json!({"pong": 1}));
        let calls = rig.endpoint_calls();
        assert_eq!((calls[0].place.as_str(), calls[0].endpoint.as_str()), ("whatsapp", "api"));
        assert_eq!(calls[0].body, Some(json!({"action": "ping", "payload": {"n": 1}})));

        rig.answer_infra("whatsapp", "api", EndpointMethod::Post, "/action", json!({"result": {"error": "not paired"}}));
        let err = rig.run(&SharedInfraNode, json!({"service": service})).await.result.expect_err("refused");
        assert!(err.to_string().contains("ping: not paired"), "{err}");
    }

    /// A handle naming infra nobody declared fails the way an
    /// unprovisioned endpoint does, and a URL is not a handle at all.
    #[tokio::test]
    async fn an_unknown_handle_or_a_url_is_refused() {
        let rig = FakeRig::new();
        let stray = crate::infra::InfraHandle::new("elsewhere", "api", None).to_value();
        let err = rig.run(&SharedInfraNode, json!({"service": stray})).await.result.expect_err("unknown");
        assert!(err.to_string().contains("rig.declare_infra(\"elsewhere\""), "{err}");

        let err = rig
            .run(&SharedInfraNode, json!({"service": "http://whatsapp:8090"}))
            .await
            .result
            .expect_err("a url is not a handle");
        assert!(err.to_string().contains("infra"), "{err}");
    }
}
