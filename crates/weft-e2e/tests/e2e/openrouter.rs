//! A real LLM call, metered end to end.
//!
//! This is the whole paid-call path the way the outside world drives it: the
//! node asks the context for provider access, gets a metered HTTP client, and
//! streams a completion through it. The runtime measures what the call really
//! cost (the provider's meter, run around the call) and lands that figure on
//! the run's journal. What it proves that no unit test can: the provider
//! really answers, the meter really resolves a real number from the real
//! response, and that number reaches the cost trail.
//!
//! The graph is driven through the OpenRouterAccess node's connection
//! picker, exactly as a user picks a connection in the editor (the pick
//! lives on the ACCESS node, wired into the OpenRouterModel node that names
//! the model; the inference node just consumes the wired provider value).
//! The connection is a shared-door one, so the call is made on the
//! runtime's configured key (the shared-credentials `api_key` entry), which
//! the worker never holds directly.
//!
//! Spends real money (fractions of a cent on the cheapest model). Needs an
//! `api_key` entry for openrouter in the shared-credentials file the daemon
//! ships to the broker. Without it the test fails loudly at connect time,
//! naming the missing `api_key` entry and the file it belongs in.
#![cfg(feature = "e2e")]

use weft_e2e::access::{catalog_spec, connect_direct, set_account};
use weft_e2e::{ensure, project::Project, run};

/// Drive the openrouter fixture and assert a real blue-sky completion came
/// back with a resolved cost, spent on the runtime's credential (the cost
/// record says whose credential spent, so a silent fall-through to another
/// one fails here).
async fn assert_metered(project: &mut Project) -> anyhow::Result<()> {
    let mut settled = run::run_and_settle(project).await?;
    settled.completed()?;

    // The provider answered: the model was told to reply with one word, and
    // the sky is blue. Case/punctuation-insensitive: we are asserting that a
    // real completion came back, not the model's manners.
    let answer = settled
        .input_of("out")
        .and_then(|input| input.get("data").and_then(|v| v.as_str()).map(str::to_string))
        .unwrap_or_default();
    anyhow::ensure!(
        answer.to_lowercase().contains("blue"),
        "the completion did not come back as expected: {answer:?}"
    );

    // The call was measured: one cost record for the provider, resolved to a
    // real amount (the meter read a real usage figure off the real response).
    settled.assert_measured("openrouter", "platform", 1).await?;
    Ok(())
}

/// The shared-door path: a one-click connection on the runtime's own
/// credential, exactly as picking "Use ours" in the editor.
#[tokio::test]
async fn openrouter_node_measures_a_call_on_the_runtime_key() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let conn = connect_direct(
        &disp,
        catalog_spec("ai/llm", "openrouter_access")?,
        "shared",
        serde_json::json!({}),
    )
    .await?;
    let mut project = Project::prepare("openrouter", disp).await?;
    set_account(&project, "openrouter", conn.handle()).await?;
    assert_metered(&mut project).await?;
    project.finish().await?;
    conn.finish().await
}
