//! Every ai/llm node in one run, against the real services.
//!
//! What it proves beyond the openrouter tests: all four provider nodes
//! work end to end (native Anthropic wire, direct OpenAI, and the
//! custom OpenAI-compatible path, exercised against OpenRouter's own
//! compatible endpoint on the same key), the shared LlmParams object
//! drives every provider identically, LlmStream really streams (the
//! journaled bus deltas concatenate to the final response), and the
//! tool loop closes entirely in the graph: a forced tool call comes out
//! on `toolCalls`, ExecPython plays the tool, Chat Message appends the
//! role-tool result, and a prompt-less re-entry answers from it.
//!
//! Spends real money (fractions of a cent on the cheapest models).
//! Needs three keys in the environment the tests read (repo-root `.env`
//! or the shell): OPENROUTER_API_KEY, ANTHROPIC_API_KEY,
//! OPENAI_API_KEY. Each is connected through the own-key door exactly
//! as the editor's "Your own" page does. Only the openrouter calls are
//! measured: anthropic / openai / custom_llm ship no meter (those APIs
//! report tokens per call, never a per-call dollar figure).
#![cfg(feature = "e2e")]

use weft_e2e::access::{catalog_spec, connect_direct, set_account, Connection};
use weft_e2e::client::Dispatcher;
use weft_e2e::{ensure, project::Project, run};

/// Own-key connection for one llm provider node's service, from an env
/// key. Fails loudly naming the variable to set.
async fn connect_own(disp: &Dispatcher, member: &str, env: &str) -> anyhow::Result<Connection> {
    let key = std::env::var(env)
        .map_err(|_| anyhow::anyhow!("{env} must be set (repo-root .env or the shell)"))?;
    connect_direct(
        disp,
        catalog_spec("ai/llm", member)?,
        "own",
        serde_json::json!({ "key": key }),
    )
    .await
}

/// The one-word answer a Debug sink received, lowercased.
fn answer_of(settled: &run::SettledRun, node: &str) -> String {
    settled
        .input_of(node)
        .and_then(|input| input.get("data").and_then(|v| v.as_str()).map(str::to_string))
        .unwrap_or_default()
        .to_lowercase()
}

#[tokio::test]
async fn every_llm_node_works_against_the_real_services() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let conn_or = connect_own(&disp, "openrouter", "OPENROUTER_API_KEY").await?;
    let conn_an = connect_own(&disp, "anthropic", "ANTHROPIC_API_KEY").await?;
    let conn_oa = connect_own(&disp, "openai", "OPENAI_API_KEY").await?;
    // The custom path is OpenRouter's own OpenAI-compatible endpoint, so
    // the same key connects it (as the custom_llm service).
    let conn_cu = connect_own(&disp, "custom", "OPENROUTER_API_KEY").await?;

    let mut project = Project::prepare("llm_suite", disp).await?;
    set_account(&project, "prov_or", "connection", conn_or.handle())?;
    set_account(&project, "prov_an", "connection", conn_an.handle())?;
    set_account(&project, "prov_oa", "connection", conn_oa.handle())?;
    set_account(&project, "prov_cu", "connection", conn_cu.handle())?;

    let mut settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // One exact-word answer per provider: a real completion came back on
    // every wire. Substring + lowercase: we assert the completion, not
    // the model's manners.
    for (sink, word) in [
        ("out_or", "blue"),
        ("out_an", "green"),
        ("out_oa", "yellow"),
        ("out_cu", "red"),
    ] {
        let answer = answer_of(&settled, sink);
        anyhow::ensure!(
            answer.contains(word),
            "{sink}: expected the answer to contain {word:?}, got {answer:?}"
        );
    }

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

    // The measured trail: exactly the four openrouter-served calls
    // (ask_or, live, t1, t2), each with a resolved cost on the user's
    // own key. The other services ship no meter, so no records.
    settled.assert_measured("openrouter", "their-own", 4).await?;

    project.finish().await?;
    conn_or.finish().await?;
    conn_an.finish().await?;
    conn_oa.finish().await?;
    conn_cu.finish().await
}
