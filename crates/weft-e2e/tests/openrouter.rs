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
//! Two scenarios drive the SAME graph through the OpenRouterAccess node's
//! connection picker, exactly as a user picks a connection in the editor
//! (the pick lives on the ACCESS node; the inference node just consumes
//! the wired access value):
//!   - `ours`: a shared-door connection, so the call is made on the
//!     runtime's configured key (`OPENROUTER_API_KEY` on the broker),
//!     which the worker never holds directly.
//!   - `their-own`: a connection holding the user's own pasted key.
//! Both land a resolved cost, measured worker-side; the difference is only
//! whose credential spent, which the node never sees.
//!
//! Spends real money (fractions of a cent on the cheapest model). Needs an
//! OpenRouter key: `OPENROUTER_API_KEY` in the environment the daemon was
//! started from (the CLI packs it into the broker's secret for the runtime
//! path, and this test reads it for the BYOK path). Without it the runtime
//! scenario fails loudly with "the runtime has no key configured for
//! 'openrouter'", which is exactly what this test then reports.
#![cfg(feature = "e2e")]

use weft_e2e::access::{catalog_spec, connect_direct, set_account};
use weft_e2e::{ensure, project::Project, run};

/// Drive the openrouter fixture and assert a real blue-sky completion came
/// back with a resolved cost, spent on the expected credential (`origin`
/// is `"ours"` or `"their-own"`; the cost record says whose credential
/// spent, so a silent fall-through to the other one fails here).
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

/// The shared-door path: a one-click connection on the runtime's own
/// credential, exactly as picking "Use ours" in the editor.
#[tokio::test]
async fn openrouter_node_measures_a_call_on_the_runtime_key() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let conn = connect_direct(
        &disp,
        catalog_spec("ai/openrouter", "access")?,
        "shared",
        serde_json::json!({}),
    )
    .await?;
    let mut project = Project::prepare("openrouter", disp).await?;
    set_account(&project, "auth", "connection", conn.handle())?;
    assert_metered(&mut project, "ours").await?;
    project.finish().await?;
    conn.finish().await
}

/// The own-credential path: the user's own key is connected through the
/// "Your own" door, so the call rides it instead of the runtime's.
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
    // Connect the user's own key as its own connection, exactly as the
    // editor's "Your own" page does; the node config only ever holds the
    // handle. The inference node consumes the wired access; model
    // settings ride the wired config.
    let conn = connect_direct(
        &disp,
        catalog_spec("ai/openrouter", "access")?,
        "own",
        serde_json::json!({ "key": key }),
    )
    .await?;
    let mut project = Project::prepare("openrouter", disp).await?;
    set_account(&project, "auth", "connection", conn.handle())?;
    assert_metered(&mut project, "their-own").await?;
    project.finish().await?;
    conn.finish().await
}
