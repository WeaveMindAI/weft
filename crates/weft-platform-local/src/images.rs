//! Images on the local Docker daemon: built with `docker build`, kept in
//! the daemon's own image store (no registry; the containers the runner
//! and the infra host start use them straight from there).
//!
//! A build runs in this process. After a restart, a build it was running
//! is gone ([`BuildStatus::Gone`]), and the ledger lets the next caller
//! start it again.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use weft_platform_traits::{BuildHandle, BuildRequest, BuildStatus, ImageBuilder, ImageDeleted};

use crate::docker::{self, Docker};

/// How many of a failed build's last output lines its failure keeps.
const FAILURE_LOG_LINES: usize = 60;

pub struct DockerImageBuilder {
    docker: Arc<dyn Docker>,
    builds: Arc<parking_lot::Mutex<HashMap<String, Build>>>,
}

enum Build {
    Running(tokio::task::AbortHandle),
    Ended(BuildStatus),
}

impl DockerImageBuilder {
    pub fn new(docker: Arc<dyn Docker>) -> Self {
        Self { docker, builds: Arc::default() }
    }
}

/// `docker build` for `req`.
fn build_args(req: &BuildRequest) -> Vec<String> {
    let mut args = vec!["build".to_string(), "--tag".into(), req.image_ref.clone()];
    for (k, v) in &req.build_args {
        args.extend(["--build-arg".into(), format!("{k}={v}")]);
    }
    args.extend(["--file".into(), req.context_dir.join("Dockerfile").display().to_string()]);
    args.push(req.context_dir.display().to_string());
    args
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

#[async_trait]
impl ImageBuilder for DockerImageBuilder {
    /// Local images keep their tag as their whole name.
    fn image_ref(&self, tag: &str) -> String {
        tag.to_string()
    }

    async fn start(&self, req: BuildRequest) -> anyhow::Result<BuildHandle> {
        let name = req.name.clone();
        let args = build_args(&req);
        let docker = self.docker.clone();
        let builds = self.builds.clone();
        let task_name = name.clone();
        // Held until the build is registered, so a fast build never
        // records its end before its start.
        let mut table = self.builds.lock();
        let task = tokio::spawn(async move {
            let status = match docker.exec(&args).await {
                Ok(out) if out.success() => BuildStatus::Succeeded,
                Ok(out) => BuildStatus::Failed { reason: tail(&format!("{}\n{}", out.stdout, out.stderr), FAILURE_LOG_LINES) },
                Err(e) => BuildStatus::Failed { reason: format!("{e:#}") },
            };
            if let Some(entry) = builds.lock().get_mut(&task_name) {
                *entry = Build::Ended(status);
            }
        });
        table.insert(name.clone(), Build::Running(task.abort_handle()));
        Ok(BuildHandle { external_build_id: name })
    }

    async fn poll(&self, handle: &BuildHandle) -> anyhow::Result<BuildStatus> {
        Ok(match self.builds.lock().get(&handle.external_build_id) {
            Some(Build::Running(_)) => BuildStatus::Pending,
            Some(Build::Ended(status)) => status.clone(),
            None => BuildStatus::Gone,
        })
    }

    async fn image_exists(&self, image_ref: &str) -> anyhow::Result<bool> {
        Ok(self.docker.exec(&["image".into(), "inspect".into(), image_ref.into()]).await?.success())
    }

    async fn release(&self, handle: &BuildHandle) {
        if let Some(Build::Running(task)) = self.builds.lock().remove(&handle.external_build_id) {
            task.abort();
        }
    }

    async fn delete_image(&self, image_ref: &str) -> anyhow::Result<ImageDeleted> {
        let args = vec!["image".to_string(), "rm".into(), image_ref.into()];
        let out = self.docker.exec(&args).await?;
        if out.success() || out.stderr.contains("No such image") {
            Ok(ImageDeleted::Deleted)
        } else if out.stderr.contains("is using its referenced image") {
            Ok(ImageDeleted::InUse)
        } else {
            docker::DockerOutput::ok(out, &args).map(|_| ImageDeleted::Deleted)
        }
    }
}

/// The most BuildKit's cache may hold before its least recently used
/// records go, and how recent a record may be and still be spared (a build
/// of the last day is what the next build reuses).
// SYNC: the cap and the spare window <-> setup.sh (the "bound BuildKit cache" prune)
pub const BUILD_CACHE_CAP: &str = "20GB";
const BUILD_CACHE_SPARE: &str = "until=24h";

/// Keep BuildKit's cache under [`BUILD_CACHE_CAP`], least recently used
/// first. The same bound `setup.sh` applies, applied while the install
/// runs, since every project build adds to the cache between installs.
pub async fn bound_build_cache(docker: &dyn Docker) -> anyhow::Result<()> {
    docker::run(
        docker,
        vec![
            "builder".into(),
            "prune".into(),
            "--force".into(),
            "--max-used-space".into(),
            BUILD_CACHE_CAP.into(),
            "--filter".into(),
            BUILD_CACHE_SPARE.into(),
        ],
    )
    .await
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::docker::fake::FakeDocker;

    fn req(name: &str) -> BuildRequest {
        BuildRequest {
            name: name.into(),
            project_id: uuid::Uuid::from_u128(1),
            tenant: "local".into(),
            context_dir: "/ctx/a".into(),
            image_ref: "weft-worker:abc".into(),
            build_args: vec![("WEFT_COMPILE_LANE".into(), "2".into())],
        }
    }

    async fn settled(b: &DockerImageBuilder, h: &BuildHandle) -> BuildStatus {
        loop {
            match b.poll(h).await.unwrap() {
                BuildStatus::Pending => tokio::task::yield_now().await,
                other => return other,
            }
        }
    }

    #[tokio::test]
    async fn a_build_runs_docker_build_and_reports_how_it_ended() {
        let docker = Arc::new(FakeDocker::new());
        let b = DockerImageBuilder::new(docker.clone());
        let h = b.start(req("b1")).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded);
        assert_eq!(
            docker.calls_to("build")[0],
            vec!["build", "--tag", "weft-worker:abc", "--build-arg", "WEFT_COMPILE_LANE=2", "--file", "/ctx/a/Dockerfile", "/ctx/a"]
        );

        docker.fail(&["build"], "error: cargo failed");
        let h = b.start(req("b2")).await.unwrap();
        assert!(matches!(settled(&b, &h).await, BuildStatus::Failed { reason } if reason.contains("cargo failed")));

        b.release(&h).await;
        assert_eq!(b.poll(&h).await.unwrap(), BuildStatus::Gone, "a released build is gone");
        assert_eq!(b.poll(&BuildHandle { external_build_id: "never".into() }).await.unwrap(), BuildStatus::Gone);
    }

    #[tokio::test]
    async fn deleting_an_image_already_gone_is_fine() {
        let docker = Arc::new(FakeDocker::new());
        docker.fail(&["image", "rm"], "Error: No such image: x");
        let b = DockerImageBuilder::new(docker.clone());
        assert_eq!(b.delete_image("x").await.unwrap(), ImageDeleted::Deleted);
        docker.fail(&["image", "inspect"], "Error: No such image: x");
        assert!(!b.image_exists("x").await.unwrap());
    }

    #[tokio::test]
    async fn an_image_a_container_still_runs_from_is_kept() {
        let docker = Arc::new(FakeDocker::new());
        docker.fail(&["image", "rm"], "Error response from daemon: conflict: unable to delete x (must be forced) - container 9bcd46ba4da9 is using its referenced image 0e8952e178ee");
        let b = DockerImageBuilder::new(docker);
        assert_eq!(b.delete_image("x").await.unwrap(), ImageDeleted::InUse);
    }
}
