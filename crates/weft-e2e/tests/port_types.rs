//! Layer-4: the declared type of a port is what judges a value. The
//! `port_types` fixture runs two Python snippets: one hands a dict that
//! happens to carry a stored-file marker to a `JsonDict` port (it flows
//! whole, a dict is a dict), the other hands a string to a `Number` port
//! (the send fails, the node fails naming the port, the run ends failed
//! and nothing behind it runs).
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn the_declared_type_judges_a_value_and_a_refusal_fails_the_node() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("port_types", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;

    // The marker-shaped dict reached the next node as the dict it is.
    settled.assert_completed("plain")?;
    settled.assert_completed("seen")?;
    settled.assert_input(
        "seen",
        "data",
        &json!({ "__weft_image__": { "key": "not/a/real/file", "mimeType": "image/png" } }),
    )?;
    anyhow::ensure!(
        settled.output_of("seen") == Some(json!({ "keys": "__weft_image__" })),
        "the marker key survived the wire: {:?}",
        settled.output_of("seen")
    );

    // The mistyped output failed its own node, by name, and the run with it.
    settled.failed_with("port 'n'")?;
    settled.failed_with("does not accept")?;
    let error = settled
        .events_of("wrong")
        .find(|e| e.kind() == "node_failed")
        .and_then(|e| e.str_field("error").map(str::to_string));
    let Some(error) = error else { anyhow::bail!("'wrong' did not fail") };
    anyhow::ensure!(error.contains("port 'n'") && error.contains("got String"), "{error}");
    settled.assert_skipped("after")?;

    project.finish().await
}
