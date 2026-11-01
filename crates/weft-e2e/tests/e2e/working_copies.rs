//! Layer-4: a folder is a working copy the install keeps in sync, and a
//! program edits a project's code through the same working copies.
//!
//! Three contracts, each one a real folder on disk and a real install:
//!
//!   - a program writes a file into ANOTHER project's code with
//!     `ctx.project(id).code()`, builds and runs it, and the run shows
//!     what it wrote; the person's folder of that project gets the file
//!     too, written by the runtime's sync;
//!   - a save in a registered folder reaches its working copy with no
//!     weft command run (read over the dispatcher's route, which syncs
//!     nothing, so it is the runtime's own sync that landed it), and
//!     `weft run` then runs that code with nothing left to send;
//!   - `weft branch <older version>` keeps the folder's unrecorded work
//!     as a version and prints the command that comes back, and that
//!     command brings the files back exactly.
#![cfg(feature = "e2e")]

use std::time::Duration;

use crate::common;

use common::{my_copy, run_id_of, tree_of, version_ids, working_copies};
use serde_json::{json, Value};
use uuid::Uuid;
use weft_core::working_copy::WorkingCopy;
use weft_e2e::{ensure, project::Project, run, SettledRun};

/// How long the runtime's sync gets to carry a change between a folder
/// and its working copy: it passes about a second after the folder goes
/// quiet, or at once when the install says the working copy moved.
const SYNC_DEADLINE: Duration = Duration::from_secs(30);

/// The working copy `id` of `project`, as the install holds it now.
async fn copy_of(project: &Project, id: Uuid) -> anyhow::Result<WorkingCopy> {
    working_copies(project.dispatcher(), &project.id())
        .await?
        .into_iter()
        .find(|copy| copy.id == id)
        .ok_or_else(|| anyhow::anyhow!("project {} has no working copy {id}", project.id()))
}

/// The id of the folder's own working copy, registering the folder if
/// nothing has yet (reading the tree syncs, and the first sync registers).
async fn folder_copy_id(project: &Project) -> anyhow::Result<Uuid> {
    let tree = tree_of(project).await?;
    Ok(tree["workingCopy"].as_str().unwrap_or_default().parse()?)
}

#[tokio::test]
async fn a_program_writes_another_projects_code_and_runs_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let target = Project::prepare("version_tree", disp.clone()).await?;
    // The person's folder of the target is its working copy: the
    // program's write lands there, and the runtime writes it to disk.
    target.weft(&["checkpoint", "--json"]).await?;
    let folder = folder_copy_id(&target).await?;

    let mut writer = Project::prepare("program_edits_code", disp.clone()).await?;
    writer.substitute_in_main("__E2E_TOKEN__", &target.id().to_string())?;
    let started = run::start(&mut writer).await?;
    // The writer's node waits for the target's build before it starts
    // the run, so it gets longer than a plain run.
    let settled = SettledRun::observe_within(&disp, started, Duration::from_secs(240)).await?;
    settled.completed()?;
    let target_run: Uuid = settled
        .output_of("write")
        .and_then(|out| out["runId"].as_str().map(str::to_string))
        .ok_or_else(|| anyhow::anyhow!("the writer handed on no run id: {:?}", settled.output_of("write")))?
        .parse()?;

    // The run is of the code the program wrote.
    let written = "written by another project's program";
    SettledRun::observe(&disp, target_run).await?.completed()?.assert_input("out", "data", &json!(written))?;

    // It ran on the person's folder copy, whose head is now that run.
    let copy = copy_of(&target, folder).await?;
    anyhow::ensure!(copy.head.head_run == Some(target_run), "the run moved the folder copy's head: {copy:?}");
    // And the runtime wrote the file into the folder, with no command run
    // there, beside the file it replaced, which the program copied by its
    // hash.
    weft_e2e::poll_until("the program's write to reach the target's folder", SYNC_DEADLINE, Duration::from_millis(250), || async {
        Ok((target.read_file("prompts/greeting.txt").unwrap_or_default() == written).then_some(()))
    })
    .await?;
    let before = target.read_file("prompts/before.txt")?;
    anyhow::ensure!(before == "hello", "the replaced file is kept, byte for byte: {before:?}");

    writer.finish().await?;
    target.finish().await
}

#[tokio::test]
async fn a_save_in_the_folder_reaches_its_working_copy_and_weft_run_runs_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp).await?;
    // Registers the project and the folder.
    project.weft(&["checkpoint", "--json"]).await?;
    let folder = folder_copy_id(&project).await?;
    let before = copy_of(&project, folder).await?;

    // A save, as an editor makes it. Nothing below runs a command in the
    // folder until `weft run`: the route the copy is read through syncs
    // nothing, so what it shows is the runtime's own doing.
    let saved = "saved in an editor";
    project.write_file("prompts/greeting.txt", saved)?;
    let landed = weft_e2e::poll_until("the runtime's sync to land the save", SYNC_DEADLINE, Duration::from_millis(250), || async {
        let copy = copy_of(&project, folder).await?;
        Ok((copy.working_version != before.working_version).then_some(copy))
    })
    .await?;
    anyhow::ensure!(landed.is_changed(), "the save is not a version until something stamps it: {landed:?}");
    anyhow::ensure!(landed.revision > before.revision, "the save moved the working copy: {before:?} then {landed:?}");

    // `weft run` runs exactly that code, and had nothing to send: a push
    // of its own would have moved the working copy's revision.
    let run_id = run_id_of(&project.weft(&["run", "--json", "--target", "out"]).await?)?;
    SettledRun::observe(project.dispatcher(), run_id).await?.completed()?.assert_input("out", "data", &json!(saved))?;
    let after = copy_of(&project, folder).await?;
    anyhow::ensure!(after.revision == landed.revision, "the run found the save already in the install: {landed:?} then {after:?}");
    anyhow::ensure!(
        after.head.head_version.as_deref() == Some(landed.working_version.as_str()) && after.head.head_run == Some(run_id),
        "the run stamped the saved files and is head's run: {after:?}"
    );

    project.finish().await
}

#[tokio::test]
async fn a_branch_keeps_the_work_as_a_version_and_its_come_back_line_restores_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp).await?;
    let base: Value = serde_json::from_str(project.weft(&["checkpoint", "base", "--json"]).await?.trim())?;
    let v1 = base["version"].as_str().unwrap_or_default().to_string();

    // Work nobody recorded: an edit and a new file.
    let edited = "work in progress";
    let added = "a file only the work has";
    project.write_file("prompts/greeting.txt", edited)?;
    project.write_file("prompts/notes.txt", added)?;

    // Branching away keeps it, and says how to come back.
    let said = project.weft(&["branch", "base"]).await?;
    anyhow::ensure!(said.contains(&format!("this folder is now at version {}", &v1[..8])), "{said}");
    let saved_short = said
        .split("your files as they were are version ")
        .nth(1)
        .and_then(|rest| rest.split(';').next())
        .map(str::trim)
        .ok_or_else(|| anyhow::anyhow!("the branch printed no way back: {said}"))?
        .to_string();
    anyhow::ensure!(said.contains(&format!("branch {saved_short}` comes back to them")), "the way back is a command: {said}");
    anyhow::ensure!(project.read_file("prompts/greeting.txt")? == "hello", "the base's file is back on disk");
    anyhow::ensure!(!project.has_file("prompts/notes.txt"), "a file the base never held is gone");

    let tree = tree_of(&project).await?;
    let saved = version_ids(&tree)
        .into_iter()
        .find(|id| id.starts_with(&saved_short))
        .ok_or_else(|| anyhow::anyhow!("the kept work {saved_short} is a version of the tree: {tree}"))?;
    let row = tree["versions"].as_array().unwrap().iter().find(|v| v["id"] == json!(saved)).cloned().unwrap_or_default();
    anyhow::ensure!(row["parentId"] == json!(v1), "the work parents on where it was made: {row}");
    anyhow::ensure!(
        row["diff"]["changed"] == json!(["prompts/greeting.txt"]) && row["diff"]["added"] == json!(["prompts/notes.txt"]),
        "the kept version names the work: {row}"
    );
    let copy = my_copy(&tree)?;
    anyhow::ensure!(copy["headVersion"] == json!(v1) && copy["workingVersion"] == json!(v1), "the folder is the base: {tree}");

    // The line it printed comes back, byte for byte, and leaving the
    // unchanged base records nothing new.
    let back: Value = serde_json::from_str(project.weft(&["branch", &saved_short, "--json"]).await?.trim())?;
    anyhow::ensure!(back["saved"] == json!(v1) && back["copy"]["headVersion"] == json!(saved), "{back}");
    anyhow::ensure!(project.read_file("prompts/greeting.txt")? == edited, "the edit is back exactly");
    anyhow::ensure!(project.read_file("prompts/notes.txt")? == added, "the new file is back exactly");
    let tree = tree_of(&project).await?;
    anyhow::ensure!(version_ids(&tree).len() == 2, "the base and the work, nothing else: {tree}");
    anyhow::ensure!(my_copy(&tree)?["workingVersion"] == json!(saved), "the folder is the work again: {tree}");

    project.finish().await
}
