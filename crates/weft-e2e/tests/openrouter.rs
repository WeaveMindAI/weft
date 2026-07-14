//! A real LLM call on the DEPLOYMENT's key, through the broker's provider
//! proxy.
//!
//! This is the whole paid-call path end to end: the node asks the context for
//! provider access (and gets a stand-in plus the proxy's address, never the
//! key), provisions the cost, streams the completion through the proxy,
//! settles what it actually cost, and closes the access. What it proves that
//! no unit test can: the proxy really substitutes the deployment's key, the
//! provider really answers, and the cost really lands on the execution's
//! journal.
//!
//! Spends real money (fractions of a cent on the cheapest model). The
//! DEPLOYMENT must hold an OpenRouter key: set `OPENROUTER_API_KEY` in the
//! project's `.env` and `weft daemon restart`, which packs it into the
//! broker's secret. Without it the node fails loudly with "this deployment
//! has no key configured for 'openrouter'", which is exactly what this test
//! then reports.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn openrouter_node_spends_the_deployment_key_through_the_proxy() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("openrouter", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // The provider answered: the model was told to reply with one word, and
    // the sky is blue. Case/punctuation-insensitive: we are asserting that a
    // real completion came back through the proxy, not the model's manners.
    let answer = settled
        .input_of("out")
        .and_then(|input| input.get("data").and_then(|v| v.as_str()).map(str::to_string))
        .unwrap_or_default();
    anyhow::ensure!(
        answer.to_lowercase().contains("blue"),
        "the completion did not come back as expected: {answer:?}"
    );

    // The call was paid for: one settled cost for the provider, and it
    // resolved to a real amount (the proxy forwarded a request the provider
    // actually billed).
    settled.assert_paid("openrouter")?;

    project.finish().await
}
