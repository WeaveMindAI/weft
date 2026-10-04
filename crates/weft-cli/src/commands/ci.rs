//! `weft ci add --cloud <cloud>`: write the GitHub Actions workflow that
//! deploys this project onto its weft install on that cloud (the program
//! with `weft activate`, the frontend in `front/` to the cloud's own
//! container service). `weft new --ci <cloud>` does the same at creation.
//!
//! The file is weft's until somebody edits it: its first line records a
//! hash of the rest, so running the command again (to switch cloud, or to
//! pick up a newer template) replaces an untouched file and refuses one
//! that was changed, rather than throwing the changes away.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use super::Ctx;

/// The clouds a workflow can deploy to. One template each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Cloud {
    Gcp,
}

impl Cloud {
    fn name(self) -> &'static str {
        match self {
            Cloud::Gcp => "gcp",
        }
    }

    fn template(self) -> &'static str {
        match self {
            Cloud::Gcp => include_str!("../../templates/ci/gcp.yml"),
        }
    }
}

/// Where the workflow lives in the project.
pub const WORKFLOW_PATH: &str = ".github/workflows/deploy.yml";

const HEADER_PREFIX: &str = "# Written by `weft ci add`; edit it and weft will leave it alone. sha256:";

pub async fn add(ctx: Ctx, cloud: Cloud) -> Result<()> {
    let project = ctx.project()?;
    let path = write_workflow(&project.root, &project.manifest.package.name, cloud)?;
    println!("wrote {} (deploys to {})", path.display(), cloud.name());
    println!("next: {}, to give it what it needs", super::target::export_command(project, ctx.on()));
    Ok(())
}

/// Write the workflow for `cloud` into the project at `root`, refusing to
/// replace one somebody edited.
pub fn write_workflow(root: &Path, project_name: &str, cloud: Cloud) -> Result<PathBuf> {
    let path = root.join(WORKFLOW_PATH);
    match std::fs::read_to_string(&path) {
        Ok(existing) => anyhow::ensure!(
            written_by_weft_untouched(&existing),
            "{} was changed since weft wrote it (or weft never wrote it), so it is left as it \
             is. Move your changes out and delete it, then run this again.",
            path.display()
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    }
    let parent = path.parent().expect("the workflow path has a parent");
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    std::fs::write(&path, render(cloud, project_name)).with_context(|| format!("write {}", path.display()))?;
    Ok(path)
}

/// The workflow file: a header line recording the hash of the body, then
/// the body, the template with the project's service name filled in.
fn render(cloud: Cloud, project_name: &str) -> String {
    let body = cloud.template().replace("{{SERVICE}}", &service_name(project_name));
    format!("{HEADER_PREFIX}{}\n{body}", body_hash(&body))
}

fn body_hash(body: &str) -> String {
    Sha256::digest(body.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

/// Whether `existing` is a file weft wrote and nobody changed since.
fn written_by_weft_untouched(existing: &str) -> bool {
    let Some((header, body)) = existing.split_once('\n') else { return false };
    header.strip_prefix(HEADER_PREFIX).is_some_and(|recorded| recorded == body_hash(body))
}

/// A name the cloud's container service accepts, from the project's: lower
/// case letters, digits and dashes, starting with a letter, short enough
/// for the `-front` suffix.
fn service_name(project_name: &str) -> String {
    let mut name: String = project_name
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    while name.contains("--") {
        name = name.replace("--", "-");
    }
    let mut name = name.trim_matches('-').to_string();
    if !name.starts_with(|c: char| c.is_ascii_lowercase()) {
        name.insert_str(0, "weft-");
    }
    name.truncate(40);
    name.trim_end_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rerun_replaces_an_untouched_file_and_refuses_an_edited_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_workflow(dir.path(), "my app", Cloud::Gcp).unwrap();
        let first = std::fs::read_to_string(&path).unwrap();
        assert!(first.contains("-frontends/my-app-front:"), "{first}");
        write_workflow(dir.path(), "my app", Cloud::Gcp).unwrap();

        std::fs::write(&path, first.replace("runs-on: ubuntu-latest", "runs-on: self-hosted")).unwrap();
        let e = write_workflow(dir.path(), "my app", Cloud::Gcp).unwrap_err().to_string();
        assert!(e.contains("changed since weft wrote it"), "{e}");
        assert!(std::fs::read_to_string(&path).unwrap().contains("self-hosted"), "left as it is");
    }

    #[test]
    fn a_workflow_weft_never_wrote_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".github/workflows")).unwrap();
        std::fs::write(dir.path().join(WORKFLOW_PATH), "name: mine\n").unwrap();
        assert!(write_workflow(dir.path(), "p", Cloud::Gcp).is_err());
    }

    #[test]
    fn every_placeholder_is_filled() {
        let rendered = render(Cloud::Gcp, "p");
        assert!(!rendered.contains("{{SERVICE}}"), "{rendered}");
        assert!(written_by_weft_untouched(&rendered));
    }

    #[test]
    fn service_names_are_what_cloud_run_accepts() {
        assert_eq!(service_name("My_Project"), "my-project");
        assert_eq!(service_name("--a..b--"), "a-b");
        assert_eq!(service_name("42"), "weft-42");
        assert_eq!(service_name(&"x".repeat(80)).len(), 40);
    }
}
