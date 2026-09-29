//! `run_node_test` task: run ONE node self-test on the project's test
//! server (the per-package node-test image, served like a worker by the
//! platform's `Runner`) and complete the task with its report.
//!
//! The dispatcher holds the task and calls the test server directly,
//! `POST /_weft/test`, and the server answers with the report once the
//! test ran. A LIVE run is a real journaled execution
//! (`ExecutionStarted` minted from the task id, `ExecutionCompleted`
//! once the report is in), driven by an instance named after the task,
//! so the test's connection resolution takes the exact production path
//! through the broker, including a credential source that answers a
//! relay instead of a raw key.
//!
//! Task semantics: a FAILING test is a SUCCESSFUL task (the report says
//! `passed: false`); the executor errors only when the run itself could
//! not happen (image missing, the server refusing the request).
//!
//! Idempotency under re-claim (a surrendered lease requeues the task; a
//! lapsed one is rescued by `claim_one`): the execution and the instance both
//! derive from the task id. A report is persisted on the TASK row
//! (`tasks::store_result_partial`) before anything else, so a re-claim
//! reads it back and returns it with no call. A live test is called only
//! on the task's FIRST claim (`task.attempts == 1`): a re-claim with no
//! recorded report cannot know whether the prior claim's call ran the
//! test (and spent money), so it fails loudly instead of ever spending
//! twice.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

/// Wire tag of this kind on the task table. String-keyed (the
/// `TaskKind` enum holds only the dispatcher's own core kinds; extra
/// kinds register by string).
pub const RUN_NODE_TEST_KIND: &str = "run_node_test";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunNodeTestPayload {
    pub project_id: uuid::Uuid,
    pub tenant: String,
    /// The per-package node-test image to run (content-addressed).
    /// Minted by the enqueuer, run verbatim.
    pub image_ref: String,
    /// Node type under test.
    pub node: String,
    /// Declared test name.
    pub test: String,
    /// Live tier: the connection (grant) id resolving the test's
    /// declared service. Absent for basic/fake runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_connection: Option<String>,
    /// Live-test fixture variables (`WEFT_NODE_TEST_*` only; a test reads
    /// them through `LiveRig::fixture`). Values a test cannot
    /// self-provision, like a chat id the tester's bot may message.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub fixtures: std::collections::BTreeMap<String, String>,
}

/// The test server's request, as `weft_engine::test_runner` reads it.
/// The dispatcher does not link the engine, so the shape is restated.
// SYNC: TestRequest <-> crates/weft-engine/src/test_runner.rs TestRequest
#[derive(Debug, Serialize)]
struct TestRequest<'a> {
    node: &'a str,
    test: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    live: Option<LiveTest<'a>>,
}

// SYNC: LiveTest <-> crates/weft-engine/src/test_runner.rs LiveRequest
#[derive(Debug, Serialize)]
struct LiveTest<'a> {
    connection: &'a str,
    execution_id: uuid::Uuid,
    instance: &'a str,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    fixtures: &'a std::collections::BTreeMap<String, String>,
}

pub struct RunNodeTestExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for RunNodeTestExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: RunNodeTestPayload = serde_json::from_value(task.payload.clone())?;
        for (name, value) in &payload.fixtures {
            require_fixture_safe(name, value)?;
        }
        // The instance the run names itself with, and on a live run the
        // execution: both the task's, so every claim lands on the same ones.
        let instance = format!("node-test-{}", task.id.simple());
        let execution_id: Option<weft_core::ExecutionId> = payload.live_connection.as_ref().map(|_| task.id);

        // Re-claim fast path: a prior claim already has the report on
        // the TASK row. Close the execution (deduped) and return it.
        if let Some(report) = weft_task_store::tasks::stored_result(&state.pg_pool, task.id).await? {
            close_execution_id(state, execution_id).await;
            return Ok(report);
        }
        if execution_id.is_some() && task.attempts > 1 {
            close_execution_id(state, execution_id).await;
            anyhow::bail!(
                "a prior claim of this live test task recorded no report; the test may or may \
                 not have run (and spent money), so it will not be re-run automatically. Re-run \
                 the test"
            );
        }

        // A LIVE run gets a REAL execution identity: journaled as a
        // genuine `ExecutionStarted` (event + `execution` seed in
        // one transaction, deduped), kind `node_test`, so cost
        // attribution, broker scoping, and the journal bridge's terminal
        // cleanup treat the execution like any execution, while the
        // project-lifecycle machinery reads only `kind = 'execution'`
        // executions and never touches it: THIS task owns the execution's whole
        // lifecycle. The instance is appointed its driver, since it
        // claims no task of its own.
        if let Some(execution_id) = execution_id {
            state
                .journal
                .record_event_dedup(
                    &weft_journal::ExecEvent::ExecutionStarted {
                        execution_id,
                        project_id: payload.project_id,
                        entry_node: format!("node-test:{}::{}", payload.node, payload.test),
                        phase: weft_core::context::Phase::Fire,
                        // A node test executes the task payload's
                        // content-addressed image, never a project
                        // definition; `None` makes a resume against
                        // this execution fail loudly as an unknown hash.
                        definition_hash: None,
                        program: None,
                        source_version: None,
                        run_kind: weft_core::exec::RunKind::NodeTest,
                        subgraph: None,
                        seed: None,
                        // No picks: a node test runs no program, and its
                        // connections are the ones the test itself hands
                        // the node, never the install's.
                        member: None,
                        fired_trigger: None,
                        member_values: Default::default(),
                        picks: Default::default(),
                        run_class: weft_core::run_class::RunClass::Short,
                        at_unix: crate::lease::now_unix() as u64,
                    },
                    &format!("node_test_started:{execution_id}"),
                )
                .await?;
            if let Err(e) =
                weft_task_store::tasks::bind_execution_id_owner(&state.pg_pool, &execution_id.to_string(), &instance).await
            {
                close_execution_id(state, Some(execution_id)).await;
                return Err(e);
            }
        }

        let report = call_test_server(state, &payload, &instance, execution_id).await;
        let report = match report {
            Ok(report) => report,
            Err(e) => {
                close_execution_id(state, execution_id).await;
                return Err(e);
            }
        };
        // Persist the report on the TASK row before closing the execution:
        // a re-claim returns it through the fast path at the top.
        weft_task_store::tasks::store_result_partial(&state.pg_pool, task.id, state.instance.as_str(), &report).await?;
        close_execution_id(state, execution_id).await;
        Ok(report)
    }
}

/// Call the project's test server at the payload's image for one test
/// and read its report.
async fn call_test_server(
    state: &DispatcherState,
    payload: &RunNodeTestPayload,
    instance: &str,
    execution_id: Option<weft_core::ExecutionId>,
) -> Result<Value> {
    let target = weft_platform_traits::WorkerTarget {
        tenant: payload.tenant.clone(),
        project: payload.project_id,
        image: payload.image_ref.clone(),
        settings: state.worker_defaults.clone(),
    };
    // A test the user asked for waits out its worker's start, which ends
    // on its own (ready, or failed naming why).
    let endpoint = state.runner.endpoint(&target, weft_platform_traits::Patience::ToTheEnd).await?;
    let request = TestRequest {
        node: &payload.node,
        test: &payload.test,
        live: payload.live_connection.as_deref().zip(execution_id).map(|(connection, execution_id)| LiveTest {
            connection,
            execution_id,
            instance,
            fixtures: &payload.fixtures,
        }),
    };
    let resp = state
        .http
        .post(format!("{}/_weft/test", endpoint.base_url.trim_end_matches('/')))
        .header(weft_platform_traits::WORKER_AUTH_HEADER, endpoint.auth_value())
        .json(&request)
        .send()
        .await
        .map_err(|e| {
            state.runner.call_ended(&target, weft_platform_traits::WorkerCall::no_answer(&e));
            anyhow::anyhow!("call the test server at {}: {e}", endpoint.base_url)
        })?;
    let status = resp.status();
    let call = weft_platform_traits::WorkerCall::answered(status, resp.headers());
    state.runner.call_ended(&target, call);
    if call.platform_refused() {
        anyhow::bail!("the platform refused weft's call to the test server at {} ({status}): weft's account may not invoke it", endpoint.base_url);
    }
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("the test server answered {status}: {body}");
    }
    let report = resp.json().await.map_err(|e| anyhow::anyhow!("read the test report: {e}"))?;
    drop(endpoint);
    Ok(report)
}

/// Write the execution's terminal `ExecutionCompleted` (deduped, so any
/// number of writers agree). A bare `execution` row with no
/// terminal journal event would read as a forever-non-terminal execution,
/// so every exit path of the executor funnels through here.
async fn close_execution_id(state: &DispatcherState, execution_id: Option<weft_core::ExecutionId>) {
    let Some(execution_id) = execution_id else { return };
    if let Err(e) = state
        .journal
        .record_event_dedup(
            &weft_journal::ExecEvent::ExecutionCompleted { execution_id, at_unix: crate::lease::now_unix() as u64 },
            &format!("node_test_execution_id_close:{execution_id}"),
        )
        .await
    {
        tracing::error!(
            target: "weft_dispatcher::run_node_test",
            %execution_id, error = %e,
            "node-test execution close failed; the execution stays open until a re-claim re-runs it"
        );
    }
}

/// A fixture pair: the name must be a `WEFT_NODE_TEST_*` identifier (the
/// reserved fixture namespace), the value printable single-line text.
fn require_fixture_safe(name: &str, value: &str) -> Result<()> {
    let name_ok = name.starts_with("WEFT_NODE_TEST_")
        && name.len() <= 200
        && name.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
    if !name_ok {
        anyhow::bail!(
            "run_node_test: fixture '{name}' is not a WEFT_NODE_TEST_* env identifier \
             (ascii uppercase/digits/underscores)"
        );
    }
    if value.is_empty() || value.len() > 4096 || value.bytes().any(|b| b.is_ascii_control()) {
        anyhow::bail!(
            "run_node_test: fixture '{name}' value must be non-empty single-line text \
             (no control characters, at most 4096 bytes)"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixtures_are_gated() {
        assert!(require_fixture_safe("WEFT_NODE_TEST_CHAT_ID", "123").is_ok());
        assert!(require_fixture_safe("WEFT_BROKER_URL", "x").is_err(), "only the reserved fixture namespace");
        assert!(require_fixture_safe("WEFT_NODE_TEST_lower", "x").is_err());
        assert!(require_fixture_safe("WEFT_NODE_TEST_X", "two\nlines").is_err());
        assert!(require_fixture_safe("WEFT_NODE_TEST_X", "").is_err());
    }

    /// The request the test server reads: a live run carries its
    /// connection, execution, instance and fixtures; a free-tier run none.
    #[test]
    fn the_test_request_reads_as_the_server_expects() {
        let fixtures = std::collections::BTreeMap::from([("WEFT_NODE_TEST_CHAT".to_string(), "1".to_string())]);
        let execution_id = uuid::Uuid::from_u128(7);
        let live = TestRequest {
            node: "SlackSendMessage",
            test: "posts",
            live: Some(LiveTest { connection: "c1", execution_id, instance: "node-test-x", fixtures: &fixtures }),
        };
        assert_eq!(
            serde_json::to_value(&live).unwrap(),
            serde_json::json!({
                "node": "SlackSendMessage", "test": "posts",
                "live": { "connection": "c1", "execution_id": execution_id, "instance": "node-test-x", "fixtures": { "WEFT_NODE_TEST_CHAT": "1" } }
            })
        );
        let free = TestRequest { node: "Text", test: "t", live: None };
        assert_eq!(serde_json::to_value(&free).unwrap(), serde_json::json!({ "node": "Text", "test": "t" }));
    }
}
