//! A program fetches a file into storage; the rig downloads it back and asserts
//! its contents.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, fakes::BytesFake, project::Project, run, storage};

#[tokio::test]
async fn fetched_file_is_stored_and_downloadable() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("storage_file", disp.clone()).await?;
    let pid = project.id();

    // Stand up a fake serving known bytes, and point the fixture's fetch at it.
    let content = b"weft-e2e storage round-trip payload".to_vec();
    let fake = BytesFake::start(content.clone()).await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &fake.url())?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;

    // The kept execution-scoped file is downloadable and matches the bytes the
    // program fetched.
    let prefix = format!("exec/{}/", settled.color);
    let key = storage::assert_file_contents(&disp, &pid, &prefix, &content).await?;
    eprintln!("stored file verified at key {key}");

    project.finish().await
}
