//! A real LLM call, metered end to end.
//!
//! This is the whole paid-call path the way the outside world drives it: the
//! node asks the context for provider access, gets a metered HTTP client, and
//! streams a completion through it. The runtime measures what the call really
//! cost (the provider's meter, run around the call) and lands that figure on
//! the execution's journal. What it proves that no unit test can: the provider
//! really answers, the meter really resolves a real number from the real
//! response, and that number reaches the cost trail.
//!
//! Two scenarios drive the SAME node through its `apiKey` input, exactly as a
//! user picks "Credits" vs "Own key" in the editor:
//!   - `deployment`: `apiKey` unset, so the call is made on the deployment's
//!     configured key (`OPENROUTER_API_KEY` on the broker), which the worker
//!     never holds.
//!   - `byok`: a real key on `apiKey`, so the call is made on the user's own.
//! Both land a resolved cost, measured worker-side; the difference is only
//! whose key spent, which the node never sees.
//!
//! Spends real money (fractions of a cent on the cheapest model). Needs an
//! OpenRouter key: `OPENROUTER_API_KEY` in the environment the daemon was
//! started from (the CLI packs it into the broker's secret for the deployment
//! path, and this test reads it for the BYOK path). Without it the deployment
//! scenario fails loudly with "this deployment has no key configured for
//! 'openrouter'", which is exactly what this test then reports.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, project::Project, run};

/// Drive the openrouter fixture and assert a real blue-sky completion came
/// back with a resolved cost, spent on the expected key (`origin` is
/// `"deployment"` or `"user-provided"`; the cost record says whose key spent,
/// so a silent fall-through to the other key fails here).
async fn assert_metered(project: &mut Project, origin: &str) -> anyhow::Result<()> {
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
    settled.assert_measured("openrouter", origin).await?;
    Ok(())
}

/// The deployment path: the fixture leaves `apiKey` unset, so the runtime uses
/// the deployment's configured key.
#[tokio::test]
async fn openrouter_node_measures_a_call_on_the_deployment_key() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("openrouter", disp).await?;
    assert_metered(&mut project, "deployment").await?;
    project.finish().await
}

/// The BYOK path: the user's own key is set on the node's `apiKey` input, so
/// the runtime uses it instead of the deployment's.
#[tokio::test]
async fn openrouter_node_measures_a_call_on_the_users_own_key() -> anyhow::Result<()> {
    // `up` loads the repo-root `.env` (uncommitted), so OPENROUTER_API_KEY is
    // available here the same way the daemon's setup got it.
    let disp = ensure::up().await?;
    let key = std::env::var("OPENROUTER_API_KEY").map_err(|_| {
        anyhow::anyhow!(
            "OPENROUTER_API_KEY must be set (repo-root .env or the shell) to exercise the \
             BYOK path"
        )
    })?;
    let mut project = Project::prepare("openrouter", disp).await?;
    // Set the node's own key input, exactly as picking "Own key" in the editor
    // writes a real key into the api_key field.
    project.set_node_config("ask", "apiKey", &format!("{key:?}"))?;
    assert_metered(&mut project, "user-provided").await?;
    project.finish().await
}
