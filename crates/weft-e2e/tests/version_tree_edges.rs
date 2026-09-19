//! Layer-4: the sensitive edges of the version tree, the ones where
//! getting it wrong costs a person their files, their money, or their
//! ability to run the project again.
//!
//! The happy paths live in `version_tree.rs` and `version_tree_grow.rs`.
//! What is here is the set that a unit test cannot honestly prove,
//! because the thing at risk is a real file on disk, a real row in the
//! tree, or two real sessions arriving at once:
//!
//!   - freezing over an example file that does not parse (it used to be
//!     read as "no file here" and overwritten);
//!   - a frozen example whose new run fails, with the old evidence
//!     preserved and the failure available for review;
//!   - build output inside a node, which used to be hashed into every
//!     version and deleted file by file on a branch back;
//!   - two sessions checkpointing at once, where head is read and then
//!     written and a lost update silently drops one session's version;
//!   - a project id in a spelling the database does not store, which
//!     used to authorize and then match nothing, so a cancel answered
//!     success having done nothing.
#![cfg(feature = "e2e")]

mod common;

use common::{color_of, graph_hold, spawn_weft, tree_of, HOLD, RELEASE};
use serde_json::Value;
use weft_e2e::fakes::PollFake;
use weft_e2e::status::{self, STATUS_DEADLINE};
use weft_e2e::{ensure, project::Project, run, SettledRun};

/// An `examples/<name>.json` that exists but does not parse is a file
/// somebody wrote and mistyped. Freezing over it would destroy the only
/// copy of the scope and kicks they meant to keep.
#[tokio::test]
async fn an_unparseable_example_file_is_never_overwritten() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    project.mark_registered();
    SettledRun::observe(project.dispatcher(), color_of(&stdout)?).await?.completed()?;

    // A spec with one character wrong: valid-looking, not valid JSON.
    let hand_written = "{ \"name\": \"chain\", \"target\": [\"out\"],, }";
    project.write_file("examples/chain.json", hand_written)?;

    let refused = project.weft_refused(&["freeze", "chain"]).await?;
    anyhow::ensure!(
        refused.contains("parse") && refused.contains("chain.json"),
        "the refusal has to name the file and say it does not parse: {refused}"
    );
    anyhow::ensure!(
        project.read_file("examples/chain.json")? == hand_written,
        "the file must be exactly as the person left it"
    );

    // And with the typo fixed, the same command works, so the refusal is
    // about the file rather than about freezing.
    project.write_file("examples/chain.json", "{ \"name\": \"chain\" }")?;
    project.weft(&["freeze", "chain"]).await?;
    let frozen: Value = serde_json::from_str(&project.read_file("examples/chain.json")?)?;
    anyhow::ensure!(frozen["expected"]["wires"].is_array(), "now it froze: {frozen}");

    project.finish().await
}

/// A failed run remains a failure; comparison does not invent a verdict
/// or replace accepted evidence.
#[tokio::test]
async fn a_frozen_example_with_a_failed_new_run_preserves_accepted_evidence() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    project.mark_registered();
    let color = color_of(&stdout)?;
    SettledRun::observe(project.dispatcher(), color).await?.completed()?;
    project.weft(&["freeze", "chain", &color.to_string()]).await?;
    let accepted = project.read_file("examples/chain.json")?;

    // Now make the run itself fail: `src` is text, and a Cast to Number
    // fails loudly on a value that does not fit.
    let main = project.read_file("src/main.weft")?;
    project.set_main(&main.replace("mid = Cast -> (value: String)", "mid = Cast -> (value: Number)"))?;

    let stdout = project.weft(&["run", "chain", "--json"]).await?;
    let failed_color = color_of(&stdout)?;
    SettledRun::observe(project.dispatcher(), failed_color).await?
        .failed_with("cannot cast")?;
    let tree = tree_of(&project).await?;
    let failed = tree["head"]["head_run"].as_str().expect("failed run recorded");
    anyhow::ensure!(failed == failed_color.to_string(), "the failed execution must be the recorded head run");
    let row = tree["runs"].as_array().unwrap().iter().find(|row| row["color"] == failed).unwrap();
    anyhow::ensure!(row["status"] == "failed", "execution failure is retained: {row}");
    let diff: Value = serde_json::from_str(project.weft(&["diff", failed, "example:chain", "--json"]).await?.trim())?;
    anyhow::ensure!(!diff["differing"].as_array().unwrap().is_empty(), "failure outputs remain reviewable: {diff}");
    anyhow::ensure!(project.read_file("examples/chain.json")? == accepted, "a failed run never overwrites accepted evidence");

    project.finish().await
}

/// A node that carries its own package has build output beside it.
/// Covering it put thousands of files through the hash and the publish,
/// made `weft branch` report a clean tree as dirty, and deleted them one
/// by one on a branch back. A directory that is genuinely
/// somebody's source, even one called `target`, still counts: what
/// tells the two apart is a `Cargo.toml` sitting beside it.
#[tokio::test]
async fn build_output_inside_a_node_is_not_part_of_a_version() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;

    // Build output, and two folders that merely look like it. What
    // makes `nodes/pkg/target` cargo's is the `Cargo.toml` beside it;
    // `prompts/target` has none, so it is somebody's writing.
    project.write_file("nodes/pkg/node_modules/left-pad/index.js", "module.exports = 1;\n")?;
    project.write_file("nodes/pkg/Cargo.toml", "[package]\nname = \"pkg\"\n")?;
    project.write_file("nodes/pkg/target/debug/build.log", "stale build output\n")?;
    project.write_file("prompts/target/tone.txt", "a prompt folder called target\n")?;

    // The first gesture in this project is the checkpoint: nothing has
    // run, nothing is built, and a save point still has to work.
    let first = project.weft(&["checkpoint", "--json"]).await?;
    project.mark_registered();
    let first: Value = serde_json::from_str(first.trim())?;
    let version = first["version"].as_str().unwrap_or_default().to_string();
    anyhow::ensure!(!version.is_empty(), "checkpoint answers a version: {first}");

    let tree = tree_of(&project).await?;
    let versions: &[Value] = tree["versions"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    let manifest = versions
        .iter()
        .find(|v| v["id"] == serde_json::json!(version))
        .map(|v| v["manifest"].clone())
        .unwrap_or(Value::Null);
    let paths: Vec<String> = match manifest.as_object() {
        Some(m) => m.keys().cloned().collect(),
        None => Vec::new(),
    };
    anyhow::ensure!(
        !paths.iter().any(|p| p.contains("node_modules")),
        "a node's installed packages are not part of a version: {paths:?}"
    );
    anyhow::ensure!(
        !paths.iter().any(|p| p.contains("nodes/pkg/target/")),
        "cargo's output beside a node is not part of a version: {paths:?}"
    );
    anyhow::ensure!(
        paths.iter().any(|p| p == "prompts/target/tone.txt"),
        "a source folder called `target` IS part of a version: {paths:?}"
    );
    anyhow::ensure!(
        paths.iter().any(|p| p == "nodes/pkg/Cargo.toml"),
        "and the file that marks the crate is source too: {paths:?}"
    );

    // A clean tree reads as clean: build output must not make `branch`
    // think there are unsaved changes.
    let branched = project.weft(&["branch", &version]).await?;
    anyhow::ensure!(!branched.contains("changes since head"), "{branched}");
    // And the branch back left the build output alone.
    anyhow::ensure!(
        project.has_file("nodes/pkg/node_modules/left-pad/index.js"),
        "a branch must not delete a node's installed packages"
    );
    anyhow::ensure!(project.has_file("prompts/target/tone.txt"), "nor its source");

    project.finish().await
}

/// Two sessions checkpointing at once, from two checkouts of one
/// project with DIFFERENT edits.
///
/// Head is read to decide a parent and written afterwards, so a lost
/// update would drop one session's version out of the lineage the next
/// seeded run walks, silently. Both may land (the tree write holds the
/// project's lock, so the second reads the head the first just moved),
/// and the property that matters either way is that the tree is a CHAIN:
/// nothing parents on a head that has moved on, and nothing dangles.
/// Running both from one directory would not test this: they would agree
/// on the same content and so on the same version.
#[tokio::test]
async fn two_sessions_checkpointing_at_once_leave_one_coherent_tree() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("version_tree", disp).await?;
    // The head the two sessions will then race to move. It is also what
    // registers the project: nothing has run here.
    project.weft(&["checkpoint", "--json"]).await?;
    project.mark_registered();

    // A second checkout of the SAME project, and a different edit in
    // each, so the two checkpoints are two versions rather than one.
    let other = project.second_checkout_with_catalog().await?;
    project.write_file("prompts/greeting.txt", "from the first session\n")?;
    std::fs::write(other.join("prompts/greeting.txt"), "from the second session\n")?;

    let (a, b) = tokio::join!(
        weft_e2e::client::cli(project.dir(), &["checkpoint", "first", "--json"]),
        weft_e2e::client::cli(&other, &["checkpoint", "second", "--json"]),
    );
    let (a, b) = (a?, b?);

    anyhow::ensure!(
        a.success || b.success,
        "at least one checkpoint must land:\nfirst: {} {}\nsecond: {} {}",
        a.stdout, a.stderr, b.stdout, b.stderr
    );
    for lost in [&a, &b].iter().filter(|o| !o.success) {
        anyhow::ensure!(
            lost.stderr.contains("head moved"),
            "a checkpoint that lost the race is told why, not handed a server error: {}",
            lost.stderr
        );
    }

    // Whatever happened, head names a version the tree holds, and every
    // version parents on one the tree holds too: no lineage dangles.
    let tree = tree_of(&project).await?;
    let head = tree["head"]["head_version"].as_str().unwrap_or_default().to_string();
    let versions: &[Value] = tree["versions"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    let ids: Vec<String> = versions.iter().filter_map(|v| v["id"].as_str().map(str::to_string)).collect();
    anyhow::ensure!(ids.contains(&head), "head {head} is a version the tree holds: {ids:?}");
    for v in versions {
        if let Some(parent) = v["parent_id"].as_str() {
            anyhow::ensure!(
                ids.contains(&parent.to_string()),
                "version {} parents on {parent}, which the tree does not hold: {ids:?}",
                v["id"]
            );
        }
    }
    // And the winner's version is actually recorded, not just answered.
    let landed = [&a, &b]
        .iter()
        .filter(|o| o.success)
        .filter_map(|o| serde_json::from_str::<Value>(o.stdout.trim()).ok())
        .filter_map(|v| v["version"].as_str().map(str::to_string))
        .collect::<Vec<_>>();
    for version in &landed {
        anyhow::ensure!(ids.contains(version), "a checkpoint that answered {version} recorded it: {ids:?}");
    }

    // The lineage, which is the whole point. Two versions that both
    // landed must be parent and child, in the order they were written,
    // and head must be the child: the lost update this test exists for
    // looks exactly like two siblings, both parented on the head from
    // before either landed, with one of them cut out of the line the next
    // seeded run walks.
    if landed.len() == 2 {
        let parent_of = |id: &str| -> Option<String> {
            versions
                .iter()
                .find(|v| v["id"].as_str() == Some(id))
                .and_then(|v| v["parent_id"].as_str().map(str::to_string))
        };
        let (first, second) = (&landed[0], &landed[1]);
        let chained = parent_of(first).as_deref() == Some(second.as_str())
            || parent_of(second).as_deref() == Some(first.as_str());
        anyhow::ensure!(
            chained,
            "both checkpoints landed, so one is the other's parent; they are siblings, which is \
             the lost update: {first} parents on {:?}, {second} on {:?}",
            parent_of(first),
            parent_of(second)
        );
        anyhow::ensure!(
            landed.contains(&head),
            "head is one of the two versions that landed: head {head}, landed {landed:?}"
        );
    }

    project.finish().await
}

/// A project id is a uuid, and a uuid has several legal spellings while
/// the database stores exactly one. A verb that authorized on the
/// parsed id and then queried on the caller's own text matched nothing
/// and answered success, so a cancel reported that it had cancelled and
/// nothing had been cancelled.
///
/// `cancel-running` only acts while a deactivate is draining (the drain
/// watcher fires from that state), so the test drives the project into
/// that window exactly as `lifecycle_transitions` does, and then cancels
/// with the id spelled the way a hand-written script or an id copied out
/// of a UI often is.
#[tokio::test]
async fn a_cancel_on_an_uppercase_project_id_really_cancels() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let gate = PollFake::start(HOLD).await?;
    project.set_main(&graph_hold(&gate.url()))?;

    let color = run::start(&mut project).await?;
    run::wait_for_status(&disp, color, "running").await?;

    // Deactivate with a Wait drain: it holds open while the run holds.
    let pid = project.id();
    let deact = spawn_weft(
        project.dir().to_path_buf(),
        vec![
            "deactivate".into(),
            "--mode".into(),
            "park".into(),
            "--running-policy".into(),
            "wait".into(),
            "--drain-timeout".into(),
            "300".into(),
        ],
    );
    status::wait_until(&disp, &pid, "the drain window", STATUS_DEADLINE, |s| {
        s.status() == "deactivating"
    })
    .await?;

    let shouted = pid.to_string().to_uppercase();
    let (code, body) = disp
        .post_raw(&format!("/projects/{shouted}/cancel-running"), &serde_json::json!({}))
        .await?;
    anyhow::ensure!(code.is_success(), "cancel-running answered HTTP {code}: {body}");

    // The claim it just made has to be true: it used to answer success
    // having matched no rows at all, and the drain would then hang until
    // its timeout.
    let settled = SettledRun::observe(&disp, color).await?;
    anyhow::ensure!(
        settled.status == "cancelled",
        "cancel-running answered success, so the run must actually be cancelled; got {}",
        settled.status
    );
    status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;
    let deact_out = deact.await??;
    anyhow::ensure!(deact_out.success, "the drain finishes once the cancel lands: {}", deact_out.stderr);

    gate.set_body(RELEASE).await;
    project.finish().await
}
