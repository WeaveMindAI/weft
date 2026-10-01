//! A real three-turn conversation kept in one file, with a stored image
//! attached mid-conversation.
//!
//! What it proves that the single-call openrouter test cannot:
//! - the file one call adds its turn to FEEDS the next call: the model
//!   recalls turn one's fact at turn three, so the full conversation
//!   really reached the provider each time;
//! - a stored image attached via the `media` input reaches the model
//!   (it names the square's color), i.e. externalize presigned the
//!   stored bytes into a URL the provider could fetch;
//! - the conversation file stays in stored form: the image slot holds
//!   the stored-file value (never a presigned URL, never base64), so a
//!   growing conversation holds references only;
//! - each turn wrote the one file in place: turn one makes it at version
//!   1, turns two and three edit it to 2 and 3, the same key throughout;
//! - every one of the three calls landed its own resolved cost record.
//!
//! Spends real money (three calls on the cheapest vision-capable
//! model). Needs `OPENROUTER_API_KEY` like the openrouter test.
#![cfg(feature = "e2e")]

use weft_e2e::access::{catalog_spec, connect_direct, set_account};
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn a_conversation_file_carries_turns_and_media() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let conn = connect_direct(
        &disp,
        catalog_spec("ai/llm", "openrouter")?,
        "shared",
        serde_json::json!({}),
    )
    .await?;
    let mut project = Project::prepare("openrouter_chat", disp.clone()).await?;
    set_account(&project, "prov", conn.handle()).await?;

    let mut settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // Diagnostic, not an assertion: which path the attached image took
    // to the provider. A relay link leaves a `public_file_link` row
    // until it expires; none means the bytes went inline (no tunnel /
    // public base on this install). Printed so a `--public-url` run
    // shows the link path actually engaged.
    let links = weft_e2e::platform::Platform::connect(&disp)
        .await?
        .public_file_link_count_for(&settled.execution_id, &project.id())
        .await?;
    if links > 0 {
        eprintln!("media path: PUBLIC RELAY LINK ({links} live link(s) minted)");
    } else {
        eprintln!("media path: INLINE BYTES (no public link served on this install)");
    }

    // Turn two saw the attached square: the stored image really reached
    // the model in a form it could read.
    let seen = settled
        .output_of("turn2")
        .and_then(|out| out.get("response").and_then(|v| v.as_str()).map(str::to_string))
        .ok_or_else(|| anyhow::anyhow!("turn2 never emitted a response"))?;
    // Exact word (punctuation-tolerant), not `contains`: "reddish" or
    // "not red" must fail, a stray period must not.
    anyhow::ensure!(
        seen.trim().trim_matches(|c: char| c.is_ascii_punctuation()).to_lowercase() == "red",
        "the model did not name the attached square's color: {seen:?}"
    );

    // Turn three remembered turn one: the history really carried the
    // whole conversation forward.
    let recalled = settled
        .input_of("out")
        .and_then(|input| input.get("data").and_then(|v| v.as_str()).map(str::to_string))
        .ok_or_else(|| anyhow::anyhow!("the response Debug never received a value"))?;
    anyhow::ensure!(
        recalled.to_lowercase().contains("biscuit"),
        "the model did not recall the first turn's fact: {recalled:?}"
    );

    // The conversation file holds the whole conversation in STORED
    // form: the seeded system message, then alternating turns, with the
    // image slot holding the stored-file value, never a presigned URL
    // and never inline base64. It is the one file every turn edited.
    let file = settled
        .input_of("hist")
        .and_then(|input| input.get("data").cloned())
        .ok_or_else(|| anyhow::anyhow!("the conversation Debug never received a value"))?;
    let file = weft_core::storage::StoredFile::from_value(&file)?;
    // Each turn wrote the same file, one version at a time.
    for (turn, debug) in [(1, "hist1"), (2, "hist2"), (3, "hist")] {
        let at = settled
            .input_of(debug)
            .and_then(|input| input.get("data").cloned())
            .ok_or_else(|| anyhow::anyhow!("the turn {turn} conversation Debug never received a value"))?;
        let at = weft_core::storage::StoredFile::from_value(&at)?;
        anyhow::ensure!(at.key == file.key, "turn {turn} wrote a different file: {at:?} vs {file:?}");
        anyhow::ensure!(at.version == turn, "turn {turn} left the file at the wrong version: {at:?}");
    }
    let history: serde_json::Value =
        serde_json::from_slice(&weft_e2e::storage::download(&disp, &file.key).await?)?;
    let messages = history
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("history is not a list: {history}"))?;
    let roles: Vec<&str> =
        messages.iter().filter_map(|m| m.get("role").and_then(|r| r.as_str())).collect();
    anyhow::ensure!(
        roles == ["system", "user", "assistant", "user", "assistant", "user", "assistant"],
        "unexpected conversation shape: {roles:?}"
    );
    let image_slot = &messages[3]["content"]
        .as_array()
        .and_then(|parts| {
            parts.iter().find_map(|p| p.get("image_url").and_then(|i| i.get("url")))
        })
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("turn two's message lost its image part: {history}"))?;
    anyhow::ensure!(
        image_slot.get("__weft_image__").and_then(|m| m.get("key")).is_some(),
        "the history's image slot is not a stored-file value: {image_slot}"
    );
    let serialized = history.to_string();
    anyhow::ensure!(
        !serialized.contains("data:image") && !serialized.contains("X-Amz"),
        "the emitted history leaked externalized material (base64 or a presigned URL)"
    );

    // Every turn was measured on the runtime's key.
    settled.assert_measured("openrouter", "platform", 3).await?;

    project.finish().await?;
    conn.finish().await
}
