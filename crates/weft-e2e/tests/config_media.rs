//! The media-config (asset) pipeline, end to end: a project file referenced by
//! `@asset("assets/…", <Type>)` is published to the asset plane by the
//! pre-build sync (content-hash keys, the dispatcher upload verbs, the real
//! bucket), resolved into the compiled config, and read back by a node at run
//! time as a normal media input. The URL form (`@asset("http…", <Type>)`)
//! resolves to a url-form value the WORKER fetches at run time; nothing is
//! ever uploaded for it.
//!
//! The multipart upload machinery itself (slicing, signed lengths, abort) is
//! proven in `storage_multipart.rs`; here the point is the SOURCE ->
//! sync/resolve -> INPUT bridge for media values. Source never holds storage
//! keys: both tests write exactly the one `@asset` line a dev types by hand
//! (and the editor's file field writes).
#![cfg(feature = "e2e")]

use serde_json::json;
use weft_e2e::{ensure, fakes::BytesFake, project::Project, run};

#[tokio::test]
async fn a_project_asset_ref_is_synced_and_read_at_runtime() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("config_media", disp.clone()).await?;

    // A file in the project's assets folder + ONE clean source line referencing
    // it: exactly what the editor's drop produces and what a dev types by hand.
    // The pre-build sync hashes + publishes the file to the project's asset
    // plane and the compile substitutes the resolved stored-file value.
    let payload = b"weft-e2e project asset payload".to_vec();
    let assets_dir = project.dir().join("assets");
    std::fs::create_dir_all(&assets_dir)?;
    std::fs::write(assets_dir.join("data.bin"), &payload)?;
    project.set_node_config("sized", "file", "@asset(\"assets/data.bin\", File)")?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // The node read the asset's bytes back from the bucket: the ref resolved
    // to a stored-file value whose key the worker fetched. (FileSize emits the
    // count as an integer, so the expected value is an integer, not a float.)
    settled.assert_input("out", "data", &json!(payload.len()))?;

    project.finish().await
}

#[tokio::test]
async fn a_url_file_ref_is_fetched_by_the_worker_at_runtime() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("config_media", disp.clone()).await?;

    // The URL form of the same source line (what the field's paste-URL writes):
    // the sync uploads NOTHING for it; the build resolves it to a url-form file
    // value, and the WORKER fetches the URL at run time inside its isolated
    // network. Stand up a fake serving known bytes at a cluster-reachable URL.
    let payload = b"weft-e2e url asset; fetched by the worker, never the server".to_vec();
    let fake = BytesFake::start(payload.clone()).await?;
    project.set_node_config("sized", "file", &format!("@asset(\"{}\", File)", fake.url()))?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // The node fetched the URL and counted the bytes: a url-handle file reads
    // through the same `get_bytes` path as a bucket-backed one.
    settled.assert_input("out", "data", &json!(payload.len()))?;

    project.finish().await
}
