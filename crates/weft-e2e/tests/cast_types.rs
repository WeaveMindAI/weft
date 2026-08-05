//! The Cast node's conversion table works end to end, including a
//! user-declared custom type (Profile) harvested from a project-local
//! node's metadata and baked into the worker binary.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn cast_conversions_and_custom_type() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("cast_types", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    // Each field proves one leg: "6.5" parsed to a Number, that Number
    // stringified back, "true" parsed to a Boolean, "[1, 2, 3]" parsed
    // into List[Number] (summed to 6.0), a JsonDict validated into the
    // named Profile type (readable both through the typed ProfileGreet
    // input and through a plain JsonDict input).
    settled.assert_input(
        "out",
        "data",
        &json!({
            "number": 6.5,
            "text": "6.5",
            "flag": true,
            "total": 6.0,
            "profile_name": "Ada",
            "greeting": "Hello Ada (36)",
        }),
    )?;

    project.finish().await
}
