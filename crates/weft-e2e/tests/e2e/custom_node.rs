//! A project-local custom node compiles into the worker and runs, and the
//! worker programs its rebuilds leave behind are reclaimed.
#![cfg(feature = "e2e")]

use weft_e2e::{ensure, project::Project, run};

/// The worker programs a project's builds leave behind stay bounded: after
/// three edits of its node, its current program and the one before remain,
/// and the oldest is reclaimed without anybody cleaning.
#[tokio::test]
async fn a_projects_older_worker_programs_are_reclaimed_after_each_build() -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let disp = ensure::up().await?;
    let platform = weft_e2e::platform::Platform::connect(&disp).await?;
    let mut project = Project::prepare("custom_node", disp).await?;
    let node = project.dir().join("nodes/multiply/mod.rs");
    let original = std::fs::read_to_string(&node)?;
    let mut builds = Vec::new();
    for edit in 1..=3 {
        std::fs::write(&node, format!("{original}\n// edit {edit}\n"))?;
        let out = project.weft(&["build"]).await?;
        let short = out
            .split("on worker ")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().next())
            .ok_or_else(|| anyhow::anyhow!("`weft build` named no worker: {out}"))?
            .to_string();
        builds.push(short);
    }
    anyhow::ensure!(builds[0] != builds[1] && builds[1] != builds[2], "each edit builds a new program: {builds:?}");
    let tags = || async { platform.worker_programs() };
    let has = |tags: &[String], short: &str| tags.iter().any(|t| t.starts_with(short));
    // The reclaim runs in the first gap between the install's builds, so
    // on a busy install (the whole suite at once) it can wait a while:
    // whatever the three builds left of the test's time, keeping enough
    // for the run and the cleanup after it.
    let wait = weft_e2e::time_left(started, std::time::Duration::from_secs(60));
    weft_e2e::poll_until("the oldest build's program to be reclaimed", wait, std::time::Duration::from_secs(1), || async {
        let now = tags().await?;
        Ok((!has(&now, &builds[0])).then_some(()))
    })
    .await?;
    let now = tags().await?;
    anyhow::ensure!(has(&now, &builds[1]) && has(&now, &builds[2]), "the current image and the one before stay: {builds:?} vs {now:?}");

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    project.finish().await
}
