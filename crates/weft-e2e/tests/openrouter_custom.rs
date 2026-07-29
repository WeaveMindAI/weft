//! A PROJECT-DEFINED provider, metered end to end.
//!
//! The sibling `openrouter` test proves the built-in path: a provider whose
//! meter ships in weft. This proves the other path, the one this project can
//! own itself: a provider weft does NOT ship (`openrouter_custom`), whose meter
//! lives in the project (`fixtures/openrouter_custom/nodes/ask/mod.rs`, a bare
//! node with the meter registered at the bottom of its own `mod.rs`).
//!
//! What it proves that no unit test can: the project's own meter compiles into
//! the real worker binary, the worker discovers it, and it prices a real call.
//! The custom meter wraps weft's real OpenRouter meter (same real API, same
//! real pricing) under a different provider name, so the call really goes out,
//! really answers, and a real cost lands on the journal, attributed to
//! `openrouter_custom`, the project's provider, not to `openrouter`.
//!
//! Driven on the user's own key only: a project-defined service offers no
//! shared door (define your own provider, bring your own key), so the own
//! connection is the path that exercises it. Spends real money (fractions of a cent). Needs
//! `OPENROUTER_API_KEY` (repo-root `.env` or the shell), the real key the
//! custom provider's call rides.
#![cfg(feature = "e2e")]

use weft_e2e::access::{connect_direct, service_spec_of, set_account};
use weft_e2e::{ensure, project::Project, run};

/// The project defines its own `openrouter_custom` provider; the user's own key
/// is set on the node, so the call rides it and the project's meter prices it.
#[tokio::test]
async fn a_project_defined_provider_meters_a_real_call() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let key = std::env::var("OPENROUTER_API_KEY").map_err(|_| {
        anyhow::anyhow!(
            "OPENROUTER_API_KEY must be set (repo-root .env or the shell): the custom provider's \
             call rides the user's own key"
        )
    })?;
    let mut project = Project::prepare("openrouter_custom", disp.clone()).await?;
    // Connect the user's own key as a connection for the PROJECT-DEFINED
    // service, exactly as the editor's "Your own" page does; the spec is
    // read off the fixture's own access-node metadata.
    let spec = service_spec_of(&project.dir().join("nodes/ask/metadata.json"))?;
    let conn = connect_direct(&disp, spec, "own", serde_json::json!({ "key": key })).await?;
    set_account(&project, "ask", "connection", conn.handle())?;

    let mut settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // The provider answered (one word, and the sky is blue): a real completion
    // came back through the project-defined provider.
    let answer = settled
        .input_of("out")
        .and_then(|input| input.get("data").and_then(|v| v.as_str()).map(str::to_string))
        .unwrap_or_default();
    anyhow::ensure!(
        answer.to_lowercase().contains("blue"),
        "the completion did not come back as expected: {answer:?}"
    );

    // The PROJECT's own meter priced the call: the cost record is attributed to
    // `openrouter_custom` (the project's provider name), resolved to a real
    // amount, on the user's own key. If the worker had failed to discover the
    // project meter, there would be no `openrouter_custom` record at all.
    settled.assert_measured("openrouter_custom", "their-own").await?;

    project.finish().await?;
    conn.finish().await
}
