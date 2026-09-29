//! The node-test binary's whole brain. Codegen emits a tiny `main.rs`
//! per package (`weft_engine::test_runner::main(&registry::PROJECT_CATALOG)`)
//! so every line of runner logic lives HERE, typechecked, instead of
//! in generated source.
//!
//! Subcommands (JSON on stdout, machine-consumed by `weft test-node`;
//! logs go to stderr):
//!
//!   list                       every node's declared tests
//!   run --node N --test T      one basic or fake test
//!   run-all [--tier ...]       every basic/fake test in the registry
//!   serve                      HTTP: `POST /_weft/test` runs one test
//!                              (live included) and answers its report.
//!                              How the install runs a test: the test
//!                              image is started the way a worker is,
//!                              with a worker's identity, so a live
//!                              test's connection resolution takes the
//!                              exact production path.
//!
//! Exit code: 0 = everything ran and passed, 1 = at least one test
//! failed, 2 = the runner itself could not do what was asked.

use std::process::ExitCode;

use clap::Parser;

use weft_core::node_test::{NodeTestsListing, RunAllReport, TestReport};
use weft_core::{NodeCatalog, TestTier};

use crate::test_rig::LiveTestRunner;

#[derive(Debug, Parser)]
#[command(name = "weft-node-tests", version)]
enum Args {
    /// Print every node's declared tests as JSON.
    List,
    /// Run one basic or fake test by node type + test name.
    Run {
        #[arg(long)]
        node: String,
        #[arg(long)]
        test: String,
    },
    /// Serve `POST /_weft/test` (one test per request, live included)
    /// and `GET /_weft/tests` as the install's test runner, until stopped.
    Serve {
        #[arg(long, env = "WEFT_BROKER_URL")]
        broker_url: String,
        #[arg(long, env = "WEFT_TENANT_ID")]
        tenant_id: String,
        /// The project the tests' cost and leases belong to.
        #[arg(long, env = "WEFT_PROJECT_ID")]
        project_id: uuid::Uuid,
        #[arg(long, env = "PORT", default_value = "8080")]
        port: u16,
    },
    /// Run every basic/fake test in the registry.
    RunAll {
        /// Tiers to run (repeatable). Default: basic and fake. Live is
        /// refused here: live runs are one at a time, explicitly.
        #[arg(long = "tier", value_enum)]
        tiers: Vec<TierArg>,
        /// Run tests concurrently: bare `--parallel` runs everything
        /// at once, `--parallel N` caps in-flight tests at N. Absent:
        /// one at a time. Free-tier tests share no state, so any
        /// interleaving is sound; the report keeps declaration order.
        #[arg(long, num_args = 0..=1, default_missing_value = "0")]
        parallel: Option<usize>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum TierArg {
    Basic,
    Fake,
}

impl TierArg {
    fn tier(self) -> TestTier {
        match self {
            Self::Basic => TestTier::Basic,
            Self::Fake => TestTier::Fake,
        }
    }
}

/// Entry point the generated per-package `main.rs` calls.
pub fn main(catalog: &'static dyn NodeCatalog) -> ExitCode {
    tracing_subscriber_init();
    let args = Args::parse();
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("start tokio runtime: {e}");
            return ExitCode::from(2);
        }
    };
    runtime.block_on(run(catalog, args))
}

/// Stderr-only logs, so stdout stays the one clean JSON channel. This
/// binary owns its whole process, so a subscriber already being
/// installed is a wiring bug; `init` panics loudly then instead of
/// silently dropping every log line.
fn tracing_subscriber_init() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "weft_engine=info,weft_core=info".into()),
        )
        .init();
}

/// A live test drives the node's plain `run` body through the
/// production rig; a trigger registers through `setup_trigger` and an
/// infra node provisions through `provision_infra`, neither of which
/// the live rig can drive. Fake is their top tier, so a live
/// declaration on such a node is a config error, refused before
/// anything runs (every subcommand, so it surfaces on the first
/// list/run after writing it).
fn refuse_misdeclared_live(catalog: &'static dyn NodeCatalog) -> Result<(), String> {
    for node_type in catalog.all() {
        let node = catalog.lookup(node_type).expect("catalog names only nodes it holds");
        let manifest = node.manifest();
        if !(manifest.features.is_trigger || manifest.requires_infra) {
            continue;
        }
        if let Some(t) = node.tests().iter().find(|t| t.tier == TestTier::Live) {
            return Err(format!(
                "node '{node_type}' declares live test '{}', but a {} node has no live \
                 tier (the live rig only drives a plain run body); declare it as a fake \
                 test instead",
                t.name,
                if manifest.features.is_trigger { "trigger" } else { "requires_infra" },
            ));
        }
    }
    Ok(())
}

async fn run(catalog: &'static dyn NodeCatalog, args: Args) -> ExitCode {
    if let Err(e) = refuse_misdeclared_live(catalog) {
        eprintln!("{e}");
        return ExitCode::from(2);
    }
    match args {
        Args::List => {
            let out = listing(catalog);
            // Same sentinel protocol as the run reports: every JSON
            // document this binary emits is marker-prefixed, so one
            // reader rule covers all three subcommands.
            println!(
                "{}{}",
                weft_core::node_test::REPORT_SENTINEL,
                serde_json::to_string(&out).expect("listing serializes")
            );
            ExitCode::SUCCESS
        }

        Args::Run { node, test } => {
            let request = TestRequest { node, test, live: None };
            match run_one(catalog, request, None).await {
                Ok(report) => {
                    let passed = report.passed;
                    println!(
                        "{}{}",
                        weft_core::node_test::REPORT_SENTINEL,
                        serde_json::to_string(&report).expect("report serializes")
                    );
                    if passed { ExitCode::SUCCESS } else { ExitCode::FAILURE }
                }
                Err(e) => {
                    eprintln!("{e}");
                    ExitCode::from(2)
                }
            }
        }

        Args::Serve { broker_url, tenant_id, project_id, port } => {
            let identity = match crate::worker::identity_from_env() {
                Ok(i) => i,
                Err(e) => {
                    eprintln!("{e:#}");
                    return ExitCode::from(2);
                }
            };
            let door = match crate::worker::WorkerDoor::from_env() {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("{e:#}");
                    return ExitCode::from(2);
                }
            };
            let live = LiveEnv { broker_url, tenant_id, project_id, identity };
            match serve(catalog, live, door, port).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("the test server stopped: {e:#}");
                    ExitCode::from(2)
                }
            }
        }

        Args::RunAll { tiers, parallel } => {
            let tiers: Vec<TestTier> = if tiers.is_empty() {
                vec![TestTier::Basic, TestTier::Fake]
            } else {
                tiers.into_iter().map(TierArg::tier).collect()
            };
            let mut selected: Vec<(&str, weft_core::node_test::NodeTest)> = Vec::new();
            let mut types = catalog.all();
            types.sort();
            for node_type in types {
                let node_impl = catalog
                    .lookup(node_type)
                    .expect("catalog names only nodes it holds");
                for declared in node_impl.tests() {
                    if tiers.contains(&declared.tier) {
                        selected.push((node_type, declared));
                    }
                }
            }
            let limit = weft_core::node_test::concurrency_limit(parallel, selected.len());
            // Free-tier tests are in-process and stateless across each
            // other, so they interleave freely; a sync basic body runs
            // on the blocking pool so it never starves the fake tests'
            // async I/O. The index restores declaration order in the
            // report whatever the completion order was.
            use futures::StreamExt as _;
            let mut reports: Vec<(usize, TestReport)> =
                futures::stream::iter(selected.into_iter().enumerate())
                    .map(|(i, (node_type, declared))| async move {
                        let (name, tier) = (declared.name, declared.tier);
                        let result = match tier {
                            TestTier::Basic => {
                                tokio::task::spawn_blocking(move || declared.run_basic())
                                    .await
                                    .unwrap_or_else(|e| {
                                        Err(weft_core::error::WeftError::NodeExecution(format!(
                                            "basic test task failed: {e}"
                                        )))
                                    })
                            }
                            TestTier::Fake => declared.run_fake().await,
                            TestTier::Live => unreachable!("run-all never selects live"),
                        };
                        (i, report_of(node_type, name, tier, result, Vec::new()))
                    })
                    .buffer_unordered(limit)
                    .collect()
                    .await;
            reports.sort_by_key(|(i, _)| *i);
            let reports: Vec<TestReport> = reports.into_iter().map(|(_, r)| r).collect();
            let all_passed = reports.iter().all(|r| r.passed);
            let out = RunAllReport { tests: reports, passed: all_passed };
            println!(
                "{}{}",
                weft_core::node_test::REPORT_SENTINEL,
                serde_json::to_string(&out).expect("report serializes")
            );
            if all_passed { ExitCode::SUCCESS } else { ExitCode::FAILURE }
        }
    }
}

/// Every node's declared tests.
fn listing(catalog: &'static dyn NodeCatalog) -> weft_core::node_test::TestListing {
    let mut types = catalog.all();
    types.sort();
    let nodes = types
        .into_iter()
        .map(|node_type| {
            let node = catalog.lookup(node_type).expect("catalog names only nodes it holds");
            NodeTestsListing { node_type: node_type.to_string(), tests: node.tests().iter().map(|t| t.info()).collect() }
        })
        .collect();
    weft_core::node_test::TestListing { nodes }
}

fn report_of(
    node: &str,
    test: &str,
    tier: TestTier,
    result: weft_core::error::WeftResult<()>,
    execution_ids: Vec<String>,
) -> TestReport {
    let error = result.err().map(|e| e.to_string());
    TestReport {
        node: node.to_string(),
        test: test.to_string(),
        tier,
        passed: error.is_none(),
        error,
        execution_ids,
    }
}


/// One test to run, as the install asks for it.
// SYNC: TestRequest <-> crates/weft-dispatcher/src/task_kinds/run_node_test.rs (sent)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TestRequest {
    pub node: String,
    pub test: String,
    /// Present for a live test.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live: Option<LiveRequest>,
}

/// What a live test needs besides the node and the test.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LiveRequest {
    /// The connection (grant) id resolving the test's declared service.
    pub connection: String,
    /// The execution the run's cost attributes to, registered by
    /// the install with `instance` as its driver.
    pub execution_id: uuid::Uuid,
    /// The instance id this run names itself with on the broker: the
    /// driver the install appointed for `execution_id`.
    pub instance: String,
    /// `WEFT_NODE_TEST_*` values the test reads (`LiveRig::fixture`).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub fixtures: std::collections::BTreeMap<String, String>,
}

/// The broker side a live test runs against: the serving process's own.
struct LiveEnv {
    broker_url: String,
    tenant_id: String,
    project_id: uuid::Uuid,
    identity: std::sync::Arc<dyn weft_platform_traits::IdentityTokens>,
}

/// Run one test. `Err` is a request that could not run at all (no such
/// node or test, a tier mismatch); a test that ran and failed is a
/// report with `passed: false`.
async fn run_one(catalog: &'static dyn NodeCatalog, request: TestRequest, live_env: Option<&LiveEnv>) -> Result<TestReport, String> {
    let TestRequest { node, test, live } = request;
    let node_impl = catalog.lookup(&node).ok_or_else(|| format!("no node type '{node}' in this package's registry"))?;
    let tests = node_impl.tests();
    let declared = tests.iter().find(|t| t.name == test).ok_or_else(|| {
        format!(
            "node '{node}' declares no test '{test}' (declared: {:?})",
            tests.iter().map(|t| t.name).collect::<Vec<_>>()
        )
    })?;
    match (declared.tier, live) {
        (TestTier::Basic, None) => Ok(report_of(&node, declared.name, declared.tier, declared.run_basic(), Vec::new())),
        (TestTier::Fake, None) => Ok(report_of(&node, declared.name, declared.tier, declared.run_fake().await, Vec::new())),
        (TestTier::Basic | TestTier::Fake, Some(_)) => {
            Err(format!("test '{test}' on node '{node}' is a free-tier test and takes no connection"))
        }
        (TestTier::Live, None) => Err(format!(
            "test '{test}' on node '{node}' is live: it runs inside the install, on the production \
             credential path (`weft test-node --live`)"
        )),
        (TestTier::Live, Some(live)) => {
            let env = live_env.ok_or_else(|| "a live test runs only in the install's test server".to_string())?;
            let service = declared.service.expect("NodeTest::live always carries its service");
            let token = weft_broker_client::TokenSource::worker(env.identity.clone(), live.instance.clone());
            let runner = LiveTestRunner::new(
                crate::EngineClients::from_broker(&env.broker_url, token),
                catalog,
                live.instance,
                env.tenant_id.clone(),
                env.project_id,
                Some(live.execution_id),
            );
            let rig = runner.rig(&live.connection, service, live.fixtures);
            let result = declared.run_live(rig).await;
            // Release leased connections + wait out in-flight cost records
            // BEFORE reporting: money first.
            let leaked = runner.settle().await;
            let execution_ids = vec![runner.execution_id().to_string()];
            let mut report = report_of(&node, declared.name, declared.tier, result, execution_ids);
            if !leaked.is_empty() {
                // A leaked lease fails the test even when the body passed;
                // the body's own error stays primary, the leak is appended
                // so an operator can release the named grant(s) by hand.
                report.passed = false;
                let leak = format!("leases not released: {}", leaked.join("; "));
                report.error = Some(match report.error {
                    Some(body) => format!("{body}; {leak}"),
                    None => leak,
                });
            }
            Ok(report)
        }
    }
}

/// Serve one test per `POST /_weft/test`, and the package's listing at
/// `GET /_weft/tests`, until the platform stops the process. A test's
/// answer is the report as JSON (200), or the reason the request could
/// not run (422).
async fn serve(catalog: &'static dyn NodeCatalog, live: LiveEnv, door: crate::worker::WorkerDoor, port: u16) -> anyhow::Result<()> {
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    #[derive(Clone)]
    struct Served {
        catalog: &'static dyn NodeCatalog,
        live: std::sync::Arc<LiveEnv>,
        door: crate::worker::WorkerDoor,
    }
    async fn test(State(s): State<Served>, headers: HeaderMap, axum::Json(req): axum::Json<TestRequest>) -> axum::response::Response {
        if !s.door.admits_headers(&headers) {
            return (StatusCode::UNAUTHORIZED, "this test server answers the install only").into_response();
        }
        match run_one(s.catalog, req, Some(&s.live)).await {
            Ok(report) => axum::Json(report).into_response(),
            Err(why) => (StatusCode::UNPROCESSABLE_ENTITY, why).into_response(),
        }
    }
    async fn tests(State(s): State<Served>, headers: HeaderMap) -> axum::response::Response {
        if !s.door.admits_headers(&headers) {
            return (StatusCode::UNAUTHORIZED, "this test server answers the install only").into_response();
        }
        axum::Json(listing(s.catalog)).into_response()
    }
    // SYNC: the test server's routes <-> crates/weft-dispatcher/src/task_kinds/run_node_test.rs
    let app = axum::Router::new()
        .route("/_weft/test", axum::routing::post(test))
        .route("/_weft/tests", axum::routing::get(tests))
        .route("/_weft/healthz", axum::routing::get(|| async { StatusCode::OK }))
        .with_state(Served { catalog, live: std::sync::Arc::new(live), door })
        .layer(axum::middleware::map_response(crate::worker::mark_worker_answer));
    let listener = tokio::net::TcpListener::bind(std::net::SocketAddr::from(([0, 0, 0, 0], port))).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
