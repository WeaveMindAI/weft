//! The two ai/llm RUNTIME mechanisms no node test can reach, against
//! the real service: LlmStream really streams (the journaled bus
//! deltas concatenate to the final response), and the tool loop closes
//! entirely in the graph (a forced tool call comes out on `toolCalls`,
//! ExecPython plays the tool, Chat Message appends the role-tool
//! result, and a prompt-less re-entry answers from it). Per-provider
//! wire correctness is node-tested in ai/llm, never here.
//!
//! Spends real money (fractions of a cent on the cheapest model).
//! Needs OPENROUTER_API_KEY in the environment the tests read
//! (repo-root `.env` or the shell), connected through the own-key door
//! exactly as the editor's "Your own" page does.
#![cfg(feature = "e2e")]

use weft_e2e::access::{catalog_spec, connect_direct, set_account};
use weft_e2e::{ensure, project::Project, run};

/// The one-word answer a Debug sink received, lowercased.
fn answer_of(settled: &run::SettledRun, node: &str) -> String {
    settled
        .input_of(node)
        .and_then(|input| input.get("data").and_then(|v| v.as_str()).map(str::to_string))
        .unwrap_or_default()
        .to_lowercase()
}

#[tokio::test]
async fn llm_stream_and_tool_loop_close_in_the_graph() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let key = std::env::var("OPENROUTER_API_KEY")
        .map_err(|_| anyhow::anyhow!("OPENROUTER_API_KEY must be set (repo-root .env or the shell)"))?;
    let conn = connect_direct(
        &disp,
        catalog_spec("ai/llm", "openrouter")?,
        "own",
        serde_json::json!({ "key": key }),
    )
    .await?;

    let mut project = Project::prepare("llm_stream_tool", disp).await?;
    set_account(&project, "prov", "connection", conn.handle())?;

    let mut settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // The stream really streamed: delta messages landed on the bus, and
    // their concatenation IS the final response (nothing dropped,
    // nothing reordered, nothing invented).
    let bus_id = settled.assert_bus_conversation(1, 1)?;
    let streamed: String = settled
        .bus_messages(&bus_id)
        .iter()
        .filter(|m| m.msg_kind == "delta")
        .filter_map(|m| m.value.as_ref().and_then(|v| v.as_str().map(str::to_string)))
        .collect();
    let full = answer_of(&settled, "out_stream");
    anyhow::ensure!(
        !streamed.is_empty() && streamed.to_lowercase() == full,
        "streamed deltas must concatenate to the response: deltas {streamed:?} vs response {full:?}"
    );

    // The tool loop closed: the forced call surfaced on toolCalls, the
    // role-tool result fed back, and the prompt-less second call
    // answered from it. The branch lives behind t1's toolCalls port
    // (null-propagation skips it wholesale if the model never calls the
    // tool), so pin that the second call actually RAN first: it makes a
    // skipped branch fail as "t2 never ran" instead of as an empty
    // answer that points at the model's wording.
    settled.assert_completed("t2")?;
    let tool_answer = answer_of(&settled, "out_tool");
    anyhow::ensure!(
        tool_answer.contains("biscuit"),
        "the tool round trip did not surface the tool's answer: {tool_answer:?}"
    );

    // The measured trail: exactly the three openrouter-served calls
    // (live, t1, t2), each with a resolved cost on the user's own key.
    settled.assert_measured("openrouter", "their-own", 3).await?;

    project.finish().await?;
    conn.finish().await
}
