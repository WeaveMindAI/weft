//! Images on Google Cloud: built by Cloud Build, kept in Artifact
//! Registry.
//!
//! A build uploads its staged context to the build bucket and asks Cloud
//! Build to run `docker build` on it and push the result. Cloud Build
//! keeps the build under its own id, so any copy of the runtime can poll
//! it by that id alone. Each build starts on a fresh machine, so the
//! worker's compile cache does not carry from one build to the next.

use std::path::Path;

use async_trait::async_trait;
use serde_json::{json, Value};
use weft_platform_traits::config::GcpPlatform;
use weft_platform_traits::{BuildHandle, BuildRequest, BuildStatus, ImageBuilder, ImageDeleted};

use crate::api::Google;

/// The longest Cloud Build lets a build run; a build is the user's own
/// compile, so weft sets the platform's ceiling rather than a deadline of
/// its own.
const BUILD_TIMEOUT: &str = "86400s";

pub struct CloudBuildImages {
    google: Google,
    gcp: GcpPlatform,
}

/// Where an Artifact Registry repository is, read from its address
/// (`us-central1-docker.pkg.dev/<project>/<repo>`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Repo {
    host: String,
    location: String,
    project: String,
    repo: String,
}

impl Repo {
    fn parse(address: &str) -> anyhow::Result<Self> {
        let mut parts = address.trim_end_matches('/').splitn(3, '/');
        let (Some(host), Some(project), Some(repo)) = (parts.next(), parts.next(), parts.next()) else {
            anyhow::bail!("artifactRegistry '{address}' is not <region>-docker.pkg.dev/<project>/<repository>");
        };
        let location = host
            .strip_suffix("-docker.pkg.dev")
            .ok_or_else(|| anyhow::anyhow!("artifactRegistry '{address}' is not an Artifact Registry address"))?;
        Ok(Self { host: host.into(), location: location.into(), project: project.into(), repo: repo.into() })
    }

    /// `(package, tag)` of a full reference in this repository.
    fn split<'a>(&self, image_ref: &'a str) -> anyhow::Result<(&'a str, &'a str)> {
        let prefix = format!("{}/{}/{}/", self.host, self.project, self.repo);
        let rest = image_ref
            .strip_prefix(&prefix)
            .ok_or_else(|| anyhow::anyhow!("{image_ref} is not in the install's repository {prefix}"))?;
        rest.rsplit_once(':').ok_or_else(|| anyhow::anyhow!("{image_ref} has no tag"))
    }
}

impl CloudBuildImages {
    pub fn new(google: Google, gcp: GcpPlatform) -> anyhow::Result<Self> {
        Repo::parse(&gcp.artifact_registry)?;
        Ok(Self { google, gcp })
    }

    fn repo(&self) -> Repo {
        Repo::parse(&self.gcp.artifact_registry).expect("checked when built")
    }

    fn builds_url(&self) -> String {
        format!("https://cloudbuild.googleapis.com/v1/projects/{}/locations/{}/builds", self.gcp.project, self.gcp.region)
    }

    fn context_object(name: &str) -> String {
        format!("contexts/{name}.tar.gz")
    }
}

/// The Cloud Build request for `req`, its context at `object` in `bucket`.
/// `account` is the build's service account as a resource path
/// (`projects/<p>/serviceAccounts/<email>`).
fn build_body(req: &BuildRequest, bucket: &str, object: &str, account: &str, machine: Option<&str>) -> Value {
    let mut args = vec!["build".to_string(), "--tag".into(), req.image_ref.clone()];
    for (k, v) in &req.build_args {
        args.extend(["--build-arg".into(), format!("{k}={v}")]);
    }
    args.push(".".into());
    json!({
        "source": { "storageSource": { "bucket": bucket, "object": object } },
        "steps": [{
            "name": "gcr.io/cloud-builders/docker",
            // The worker's Dockerfile uses BuildKit's cache mounts.
            "env": ["DOCKER_BUILDKIT=1"],
            "args": args,
        }],
        "images": [req.image_ref],
        "timeout": BUILD_TIMEOUT,
        "serviceAccount": account,
        "options": options(machine),
        "tags": ["weft", format!("weft-project-{}", req.project_id.simple())],
    })
}

/// A build's options: its log goes to Cloud Logging only (Cloud Build
/// refuses the default bucket for a build run as an account of its own),
/// on the install's chosen machine when it chose one.
fn options(machine: Option<&str>) -> Value {
    let mut options = json!({ "logging": "CLOUD_LOGGING_ONLY" });
    if let Some(machine) = machine {
        options["machineType"] = json!(machine);
    }
    options
}

/// How a Cloud Build build is doing, from its `status`.
fn status_of(build: &Value) -> BuildStatus {
    let status = build.get("status").and_then(Value::as_str).unwrap_or("STATUS_UNKNOWN");
    let detail = || {
        let why = build.get("statusDetail").and_then(Value::as_str).unwrap_or(status);
        match build.get("logUrl").and_then(Value::as_str) {
            Some(log) => format!("{why} (the build's log: {log})"),
            None => why.to_string(),
        }
    };
    match status {
        "SUCCESS" => BuildStatus::Succeeded,
        // Cancelled (by weft's own release, after the end was recorded, or
        // by somebody in the console) and expired (queued too long) are
        // ends Cloud Build knows and words: its words are the reason.
        "FAILURE" | "INTERNAL_ERROR" | "TIMEOUT" | "CANCELLED" | "EXPIRED" => BuildStatus::Failed { reason: detail() },
        _ => BuildStatus::Pending,
    }
}

fn tar_context(dir: &Path) -> anyhow::Result<Vec<u8>> {
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut archive = tar::Builder::new(gz);
    archive.follow_symlinks(false);
    archive.append_dir_all(".", dir)?;
    Ok(archive.into_inner()?.finish()?)
}

#[async_trait]
impl ImageBuilder for CloudBuildImages {
    fn image_ref(&self, tag: &str) -> String {
        format!("{}/{tag}", self.gcp.artifact_registry.trim_end_matches('/'))
    }

    async fn start(&self, req: BuildRequest) -> anyhow::Result<BuildHandle> {
        let dir = req.context_dir.clone();
        let bytes = tokio::task::spawn_blocking(move || tar_context(&dir)).await??;
        let object = Self::context_object(&req.name);
        self.google
            .put_bytes(
                &format!(
                    "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
                    self.gcp.build_bucket,
                    object.replace('/', "%2F")
                ),
                "application/gzip",
                bytes,
            )
            .await?;
        let op = self.google.post(&self.builds_url(), &build_body(&req, &self.gcp.build_bucket, &object, &format!("projects/{}/serviceAccounts/{}", self.gcp.project, self.gcp.builder_service_account), self.gcp.build_machine.as_deref())).await?;
        let id = op
            .pointer("/metadata/build/id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("Cloud Build started a build without an id: {op}"))?;
        let log_url = op.pointer("/metadata/build/logUrl").and_then(Value::as_str).map(str::to_string);
        Ok(BuildHandle { external_build_id: id.to_string(), log_url })
    }

    async fn poll(&self, handle: &BuildHandle) -> anyhow::Result<BuildStatus> {
        match self.google.get_opt(&format!("{}/{}", self.builds_url(), handle.external_build_id)).await? {
            Some(build) => Ok(status_of(&build)),
            None => Ok(BuildStatus::Gone),
        }
    }

    async fn image_exists(&self, image_ref: &str) -> anyhow::Result<bool> {
        let repo = self.repo();
        let (package, tag) = repo.split(image_ref)?;
        let url = format!(
            "https://artifactregistry.googleapis.com/v1/projects/{}/locations/{}/repositories/{}/packages/{}/tags/{tag}",
            repo.project,
            repo.location,
            repo.repo,
            package.replace('/', "%2F")
        );
        Ok(self.google.get_opt(&url).await?.is_some())
    }

    async fn release(&self, handle: &BuildHandle) {
        let url = format!("{}/{}", self.builds_url(), handle.external_build_id);
        match self.google.get_opt(&url).await {
            Ok(Some(build)) => {
                if status_of(&build) == BuildStatus::Pending {
                    if let Err(e) = self.google.post(&format!("{url}:cancel"), &json!({})).await {
                        tracing::warn!(target: "weft_platform_gcp::images", build = %handle.external_build_id, error = %format!("{e:#}"), "could not cancel a build");
                    }
                }
                if let Some(object) = build.pointer("/source/storageSource/object").and_then(Value::as_str) {
                    let del = format!(
                        "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
                        self.gcp.build_bucket,
                        object.replace('/', "%2F")
                    );
                    if let Err(e) = self.google.delete(&del).await {
                        tracing::warn!(target: "weft_platform_gcp::images", object, error = %format!("{e:#}"), "could not delete a build's context");
                    }
                }
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(target: "weft_platform_gcp::images", build = %handle.external_build_id, error = %format!("{e:#}"), "could not read a build to release it"),
        }
    }

    async fn delete_image(&self, image_ref: &str) -> anyhow::Result<ImageDeleted> {
        let repo = self.repo();
        let (package, tag) = repo.split(image_ref)?;
        let tag_url = format!(
            "https://artifactregistry.googleapis.com/v1/projects/{}/locations/{}/repositories/{}/packages/{}/tags/{tag}",
            repo.project,
            repo.location,
            repo.repo,
            package.replace('/', "%2F")
        );
        let Some(found) = self.google.get_opt(&tag_url).await? else { return Ok(ImageDeleted::Absent) };
        let version = found
            .get("version")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("the tag {image_ref} names no version: {found}"))?;
        if let Some(op) = self.google.delete(&format!("https://artifactregistry.googleapis.com/v1/{version}?force=true")).await? {
            self.google.wait("https://artifactregistry.googleapis.com/v1", op).await?;
        }
        // A registry image is never in use the way a local one is: a
        // running service keeps its own copy.
        Ok(ImageDeleted::Deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repository_address_is_read_and_a_ref_split_in_it() {
        let r = Repo::parse("us-central1-docker.pkg.dev/acme/weft").unwrap();
        assert_eq!(r.location, "us-central1");
        assert_eq!(r.split("us-central1-docker.pkg.dev/acme/weft/weft-worker:abc").unwrap(), ("weft-worker", "abc"));
        assert!(r.split("docker.io/library/postgres:18").is_err());
        assert!(Repo::parse("gcr.io/acme").is_err());
    }

    #[test]
    fn a_build_runs_docker_with_its_args_and_pushes_the_ref() {
        let req = BuildRequest {
            name: "b1".into(),
            project_id: uuid::Uuid::from_u128(1),
            tenant: "local".into(),
            context_dir: "/ctx".into(),
            image_ref: "us-central1-docker.pkg.dev/acme/weft/weft-worker:abc".into(),
            build_args: vec![("WEFT_COMPILE_LANE".into(), "0".into())],
        };
        let body = build_body(&req, "bucket", "contexts/b1.tar.gz", "projects/acme/serviceAccounts/b@acme.iam.gserviceaccount.com", None);
        assert_eq!(body["images"][0], req.image_ref);
        let args: Vec<&str> = body["steps"][0]["args"].as_array().unwrap().iter().map(|a| a.as_str().unwrap()).collect();
        assert_eq!(args, ["build", "--tag", req.image_ref.as_str(), "--build-arg", "WEFT_COMPILE_LANE=0", "."]);
        assert_eq!(body["serviceAccount"], "projects/acme/serviceAccounts/b@acme.iam.gserviceaccount.com");
        assert_eq!(body["options"]["logging"], "CLOUD_LOGGING_ONLY", "a build with its own account logs to Cloud Logging");
        assert!(body["options"].get("machineType").is_none(), "Cloud Build's own default machine, the free one");
        let bigger = build_body(&req, "bucket", "contexts/b1.tar.gz", "projects/acme/serviceAccounts/b@acme.iam.gserviceaccount.com", Some("E2_HIGHCPU_8"));
        assert_eq!(bigger["options"]["machineType"], "E2_HIGHCPU_8");
    }

    #[test]
    fn a_build_status_is_read_as_weft_reads_builds() {
        assert_eq!(status_of(&json!({ "status": "WORKING" })), BuildStatus::Pending);
        assert_eq!(status_of(&json!({ "status": "SUCCESS" })), BuildStatus::Succeeded);
        assert_eq!(status_of(&json!({ "status": "CANCELLED" })), BuildStatus::Failed { reason: "CANCELLED".into() }, "an end Cloud Build words itself");
        let BuildStatus::Failed { reason } = status_of(&json!({ "status": "FAILURE", "statusDetail": "step 0 failed", "logUrl": "https://log" })) else {
            panic!()
        };
        assert!(reason.contains("step 0 failed") && reason.contains("https://log"));
    }
}
