//! A project's own container opens a stored file through the link made for
//! it (`container_link`). On a local install a container's `127.0.0.1` is
//! the container itself, so only a link that reaches the machine the way
//! containers do gets the bytes there; the container reads them back and
//! the count it read reaches Debug.
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, infra, project::Project, run};

#[tokio::test]
async fn a_project_s_own_container_downloads_a_stored_file_by_its_link() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("container_link", disp).await?;
    let payload = b"weft-e2e: read from inside a project's own container".to_vec();
    let assets = project.dir().join("assets");
    std::fs::create_dir_all(&assets)?;
    std::fs::write(assets.join("data.bin"), &payload)?;
    project.set_node_config("fetch", "file", "@asset(\"assets/data.bin\", Blob)")?;
    infra::start_and_wait_running(&mut project, "fetch").await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!(payload.len()))?;

    infra::terminate_and_wait_gone(&project, "fetch").await?;
    project.finish().await
}
