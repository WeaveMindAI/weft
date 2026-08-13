//! The node-test binary's whole brain. Codegen emits a tiny `main.rs`
//! per package (`weft_engine::test_runner::main(&registry::PROJECT_CATALOG)`)
//! so every line of runner logic lives HERE, typechecked, instead of
//! in generated source.
//!
//! Subcommands (JSON on stdout, machine-consumed by `weft test-node`
//! and by pod-side runners; logs go to stderr):
//!
//!   list                       every node's declared tests
//!   run --node N --test T      one test; live needs --live-connection
//!                              plus the broker env a worker pod has
//!   run-all [--tier ...]       every basic/fake test in the registry
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
    /// Run one test by node type + test name.
    Run {
        #[arg(long)]
        node: String,
        #[arg(long)]
        test: String,
        /// Live only: the connection (grant) id resolving the test's
        /// declared service.
        #[arg(long)]
        live_connection: Option<String>,
        /// Live only: the pre-minted execution color this run's cost
        /// attributes to. When the runtime spawned this run it minted
        /// (and registered) the color; without one the runner mints a
        /// throwaway itself.
        #[arg(long)]
        color: Option<uuid::Uuid>,
        /// Live only: broker base URL (the production credential path).
        #[arg(long, env = "WEFT_BROKER_URL")]
        broker_url: Option<String>,
        /// Live only: path to the pod's projected SA token.
        #[arg(long, env = "WEFT_BROKER_TOKEN_PATH", default_value = "/var/run/weft/sa/token")]
        broker_token_path: String,
        /// Stamped on broker calls; the pod's own name when running in
        /// a cluster.
        #[arg(long, env = "WEFT_POD_NAME", default_value = "node-test")]
        pod_name: String,
        #[arg(long, env = "WEFT_TENANT_ID", default_value = "local")]
        tenant_id: String,
        #[arg(long, env = "WEFT_PROJECT_ID", default_value = "node-test")]
        project_id: String,
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
            let mut nodes: Vec<NodeTestsListing> = Vec::new();
            let mut types = catalog.all();
            types.sort();
            for node_type in types {
                let node = catalog
                    .lookup(node_type)
                    .expect("catalog names only nodes it holds");
                nodes.push(NodeTestsListing {
                    node_type: node_type.to_string(),
                    tests: node.tests().iter().map(|t| t.info()).collect(),
                });
            }
            let out = weft_core::node_test::TestListing { nodes };
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

        Args::Run {
            node,
            test,
            live_connection,
            color,
            broker_url,
            broker_token_path,
            pod_name,
            tenant_id,
            project_id,
        } => {
            let Some(node_impl) = catalog.lookup(&node) else {
                eprintln!("no node type '{node}' in this package's registry");
                return ExitCode::from(2);
            };
            let tests = node_impl.tests();
            let Some(declared) = tests.iter().find(|t| t.name == test) else {
                eprintln!(
                    "node '{node}' declares no test '{test}' (declared: {:?})",
                    tests.iter().map(|t| t.name).collect::<Vec<_>>()
                );
                return ExitCode::from(2);
            };
            let report = match declared.tier {
                TestTier::Basic | TestTier::Fake => {
                    if live_connection.is_some() || color.is_some() {
                        eprintln!(
                            "test '{test}' on node '{node}' is {}-tier and takes no \
                             connection or color",
                            match declared.tier {
                                TestTier::Basic => "basic",
                                TestTier::Fake => "fake",
                                TestTier::Live => unreachable!(),
                            }
                        );
                        return ExitCode::from(2);
                    }
                    let result = match declared.tier {
                        TestTier::Basic => declared.run_basic(),
                        TestTier::Fake => declared.run_fake().await,
                        TestTier::Live => unreachable!(),
                    };
                    report_of(&node, declared.name, declared.tier, result, Vec::new())
                }
                TestTier::Live => {
                    let (Some(connection), Some(broker_url)) = (live_connection, broker_url)
                    else {
                        eprintln!(
                            "a live test needs --live-connection <grant id> and a broker \
                             (--broker-url / WEFT_BROKER_URL): it runs the production \
                             credential path"
                        );
                        return ExitCode::from(2);
                    };
                    let service = declared
                        .service
                        .expect("NodeTest::live always carries its service");
                    let runner = LiveTestRunner::new(
                        crate::EngineClients::from_broker(
                            &broker_url,
                            std::path::Path::new(&broker_token_path),
                        ),
                        pod_name,
                        tenant_id,
                        project_id,
                        color,
                    );
                    let rig = runner.rig(&connection, service);
                    let result = declared.run_live(rig).await;
                    // Release leased connections + wait out in-flight
                    // cost records BEFORE reporting: money first.
                    let leaked = runner.settle().await;
                    let colors = vec![runner.color().to_string()];
                    let mut report =
                        report_of(&node, declared.name, declared.tier, result, colors);
                    if !leaked.is_empty() {
                        // A leaked lease fails the test even when the
                        // body passed; the body's own error stays
                        // primary, the leak is appended so an operator
                        // can release the named grant(s) by hand.
                        report.passed = false;
                        let leak = format!("leases not released: {}", leaked.join("; "));
                        report.error = Some(match report.error {
                            Some(body) => format!("{body}; {leak}"),
                            None => leak,
                        });
                    }
                    report
                }
            };
            let passed = report.passed;
            println!(
                "{}{}",
                weft_core::node_test::REPORT_SENTINEL,
                serde_json::to_string(&report).expect("report serializes")
            );
            if passed { ExitCode::SUCCESS } else { ExitCode::FAILURE }
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

fn report_of(
    node: &str,
    test: &str,
    tier: TestTier,
    result: weft_core::error::WeftResult<()>,
    colors: Vec<String>,
) -> TestReport {
    let error = result.err().map(|e| e.to_string());
    TestReport {
        node: node.to_string(),
        test: test.to_string(),
        tier,
        passed: error.is_none(),
        error,
        colors,
    }
}

