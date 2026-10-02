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
    /// The install whose names the images go under
    /// (`Install::local_image_ref`).
    install: weft_core::infra::Install,
    builds: Arc<parking_lot::Mutex<HashMap<String, Build>>>,
}

enum Build {
    Running(tokio::task::AbortHandle),
    Ended(BuildStatus),
}

impl DockerImageBuilder {
    pub fn new(docker: Arc<dyn Docker>, install: weft_core::infra::Install) -> Self {
        Self { docker, install, builds: Arc::default() }
    }
}

/// Give `image_ref` to an image another install already holds under the
/// same content-addressed tag (`Install::tag_of_local_image`), so a
/// content one install built is never built again by the next. `false`
/// when no install holds it, or the one that did removed its name before
/// the tag landed: the caller builds it then.
async fn adopt(docker: &dyn Docker, image_ref: &str) -> anyhow::Result<bool> {
    let Some(tag) = weft_core::infra::Install::tag_of_local_image(image_ref) else {
        return Ok(false);
    };
    let listing = docker::run(
        docker,
        vec![
            "image".into(),
            "ls".into(),
            "--format".into(),
            "{{.Repository}}:{{.Tag}}".into(),
            "--filter".into(),
            format!("reference={tag}"),
            "--filter".into(),
            format!("reference=localhost/*/{tag}"),
        ],
    )
    .await?;
    for held in listing.lines().map(str::trim) {
        if held == image_ref || weft_core::infra::Install::tag_of_local_image(held) != Some(tag) {
            continue;
        }
        if docker.exec(&["tag".into(), held.into(), image_ref.into()]).await?.success() {
            return Ok(true);
        }
    }
    Ok(false)
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
    /// The install's own name for the image (`Install::local_image_ref`).
    fn image_ref(&self, tag: &str) -> String {
        self.install.local_image_ref(tag)
    }

    /// Takes the image another install on this daemon already holds under
    /// the same tag when there is one (a name added, nothing built), and
    /// runs `docker build` otherwise.
    async fn start(&self, req: BuildRequest) -> anyhow::Result<BuildHandle> {
        let name = req.name.clone();
        let args = build_args(&req);
        let docker = self.docker.clone();
        let builds = self.builds.clone();
        let task_name = name.clone();
        // Held until the build is registered, so a fast build never
        // records its end before its start.
        let mut table = self.builds.lock();
        let image_ref = req.image_ref.clone();
        let task = tokio::spawn(async move {
            let status = match adopt(docker.as_ref(), &image_ref).await {
                Ok(true) => BuildStatus::Succeeded,
                Ok(false) => match docker.exec(&args).await {
                    Ok(out) if out.success() => BuildStatus::Succeeded,
                    Ok(out) => BuildStatus::Failed { reason: tail(&format!("{}\n{}", out.stdout, out.stderr), FAILURE_LOG_LINES) },
                    Err(e) => BuildStatus::Failed { reason: format!("{e:#}") },
                },
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

    /// Removes this install's name for the image (`docker image rm` of
    /// that name). Docker deletes the image itself only with its last
    /// name, so an image another install still holds under its own name
    /// stays for that install.
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

/// The most BuildKit's cache may hold. Over it, the least recently used
/// records go first until it fits, so what the last builds used (the
/// current builder base's layers, the compile caches) is the last to go.
// SYNC: the cap and the prune flags <-> setup.sh (the "bound BuildKit cache" prune)
pub const BUILD_CACHE_CAP: &str = "20GB";

/// `docker builder prune` holding BuildKit's cache to [`BUILD_CACHE_CAP`].
///
/// `--all`, because without it BuildKit only considers records it does not
/// count as shared or internal, and on a machine that has built many
/// versions of weft that left 2.4GB of 235GB reclaimable in place. No
/// `--filter until=`: that filter makes every record used inside its
/// window ineligible whatever the cap says, so a busy day of builds grew
/// the cache past the cap with nothing allowed to free it (measured: 221GB
/// freed without it, 20GB with it). A record a build is using right now is
/// never taken either way.
fn build_cache_bound_args() -> Vec<String> {
    ["builder", "prune", "--force", "--all", "--max-used-space", BUILD_CACHE_CAP].map(String::from).to_vec()
}

/// Keep BuildKit's cache under [`BUILD_CACHE_CAP`], least recently used
/// first. The same bound `setup.sh` applies, applied while the install
/// runs, since every project build adds to the cache between installs.
pub async fn bound_build_cache(docker: &dyn Docker) -> anyhow::Result<()> {
    docker::run(docker, build_cache_bound_args()).await.map(|_| ())
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
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::default_install());
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

    /// A content another install already holds is given this install's
    /// name instead of being built; with nobody holding it, it is built.
    #[tokio::test]
    async fn a_content_another_install_holds_is_named_not_built() {
        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["image", "ls"], "evil/weft-worker:abc\nweft-worker:abc\n");
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::named("cell7").unwrap());
        let mut r = req("b1");
        r.image_ref = b.image_ref("weft-worker:abc");
        assert_eq!(r.image_ref, "localhost/weft-cell7/weft-worker:abc");
        let h = b.start(r.clone()).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded);
        assert_eq!(docker.calls_to("tag"), vec![vec!["tag", "weft-worker:abc", "localhost/weft-cell7/weft-worker:abc"]]);
        assert!(docker.calls_to("build").is_empty(), "nothing built");

        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["image", "ls"], "evil/weft-worker:abc\n");
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::named("cell7").unwrap());
        let h = b.start(r).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded);
        assert!(docker.calls_to("tag").is_empty(), "an image no install holds is never taken");
        assert_eq!(docker.calls_to("build").len(), 1);
    }

    /// The bound caps every record, least recently used first: `--all`
    /// and no age filter, or most of the cache is never eligible.
    #[tokio::test]
    async fn the_build_cache_bound_caps_every_record() {
        let docker = FakeDocker::new();
        bound_build_cache(&docker).await.unwrap();
        assert_eq!(docker.calls_to("builder")[0], vec!["builder", "prune", "--force", "--all", "--max-used-space", "20GB"]);
    }

    #[tokio::test]
    async fn deleting_an_image_already_gone_is_fine() {
        let docker = Arc::new(FakeDocker::new());
        docker.fail(&["image", "rm"], "Error: No such image: x");
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::default_install());
        assert_eq!(b.delete_image("x").await.unwrap(), ImageDeleted::Deleted);
        docker.fail(&["image", "inspect"], "Error: No such image: x");
        assert!(!b.image_exists("x").await.unwrap());
    }

    #[tokio::test]
    async fn an_image_a_container_still_runs_from_is_kept() {
        let docker = Arc::new(FakeDocker::new());
        docker.fail(&["image", "rm"], "Error response from daemon: conflict: unable to delete x (must be forced) - container 9bcd46ba4da9 is using its referenced image 0e8952e178ee");
        let b = DockerImageBuilder::new(docker, weft_core::infra::Install::default_install());
        assert_eq!(b.delete_image("x").await.unwrap(), ImageDeleted::InUse);
    }
}
