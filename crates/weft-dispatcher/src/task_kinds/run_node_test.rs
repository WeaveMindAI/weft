//! `run_node_test` task: run ONE node self-test on the project's test
//! server (the per-package node-test image, served like a worker by the
//! platform's `Runner`) and complete the task with its report.
//!
//! The dispatcher holds the task and calls the test server directly,
//! `POST /_weft/test`, and the server answers with the report once the
//! test ran. A LIVE run is a real recorded run, its id the task's: the
//! test server bears it, drives it under a replica named after the task
//! (its lease kept by the server's own ticks) and ends it once the report
//! is in (`weft_engine::test_rig::LiveTestRunner`), so the test's
//! connection resolution takes the exact production path through the
//! broker, including a credential source that answers a relay instead of
//! a raw key. A server that goes away mid-test leaves its run to the
//! lost-run sweep, which ends it.
//!
//! Task semantics: a FAILING test is a SUCCESSFUL task (the report says
//! `passed: false`); the executor errors only when the run itself could
//! not happen (image missing, the server refusing the request).
//!
//! Idempotency under re-claim (a surrendered lease requeues the task; a
//! lapsed one is rescued by `claim_one`): the run and the replica both
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
    replica: &'a str,
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
        // The replica the run names itself with, and on a live run the
        // execution: both the task's, so every claim lands on the same ones.
        let replica = format!("node-test-{}", task.id.simple());
        let execution_id: Option<weft_core::ExecutionId> = payload.live_connection.as_ref().map(|_| task.id);

        // Re-claim fast path: a prior claim already has the report on
        // the TASK row.
        if let Some(report) = weft_task_store::tasks::stored_result(&state.pg_pool, task.id).await? {
            return Ok(report);
        }
        if execution_id.is_some() && task.attempts > 1 {
            anyhow::bail!(
                "a prior claim of this live test task recorded no report; the test may or may \
                 not have run (and spent money), so it will not be re-run automatically. Re-run \
                 the test"
            );
        }

        let report = call_test_server(state, &payload, &replica, execution_id).await?;
        // Persist the report on the TASK row: a re-claim returns it through
        // the fast path at the top.
        weft_task_store::tasks::store_result_partial(&state.pg_pool, task.id, state.replica.as_str(), &report).await?;
        Ok(report)
    }
}

/// Call the project's test server at the payload's image for one test
/// and read its report.
async fn call_test_server(
    state: &DispatcherState,
    payload: &RunNodeTestPayload,
    replica: &str,
    execution_id: Option<weft_core::ExecutionId>,
) -> Result<Value> {
    let target = weft_platform_traits::WorkerTarget {
        tenant: payload.tenant.clone(),
        project: payload.project_id,
        image: payload.image_ref.clone(),
        binary_hash: None,
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
            replica,
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
    /// connection, execution, replica and fixtures; a free-tier run none.
    #[test]
    fn the_test_request_reads_as_the_server_expects() {
        let fixtures = std::collections::BTreeMap::from([("WEFT_NODE_TEST_CHAT".to_string(), "1".to_string())]);
        let execution_id = uuid::Uuid::from_u128(7);
        let live = TestRequest {
            node: "SlackSendMessage",
            test: "posts",
            live: Some(LiveTest { connection: "c1", execution_id, replica: "node-test-x", fixtures: &fixtures }),
        };
        assert_eq!(
            serde_json::to_value(&live).unwrap(),
            serde_json::json!({
                "node": "SlackSendMessage", "test": "posts",
                "live": { "connection": "c1", "execution_id": execution_id, "replica": "node-test-x", "fixtures": { "WEFT_NODE_TEST_CHAT": "1" } }
            })
        );
        let free = TestRequest { node: "Text", test: "t", live: None };
        assert_eq!(serde_json::to_value(&free).unwrap(), serde_json::json!({ "node": "Text", "test": "t" }));
    }
}
