//! A user-declared custom type (Profile) is harvested from a
//! project-local node's metadata, baked into the worker binary, and
//! validated + wired at runtime: a JsonDict Casts into the named type,
//! and the typed value feeds both a typed node input and a plain
//! JsonDict input. The Cast conversion table itself is node-tested in
//! basic/cast, never here.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn custom_type_bakes_and_validates() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("custom_type", disp).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    // The JsonDict validated into the named Profile type, readable both
    // through the typed ProfileGreet input and through a plain JsonDict
    // input.
    settled.assert_input(
        "out",
        "data",
        &json!({
            "profile_name": "Ada",
            "greeting": "Hello Ada (36)",
        }),
    )?;

    project.finish().await
}
