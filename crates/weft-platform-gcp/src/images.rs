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

use crate::api::{Google, Streamed, UPLOAD_PART};

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

/// The build-arg that tells a worker build its builder keeps no cache
/// between builds.
// SYNC: COMPILE_CACHE_ARG <-> crates/weft-compiler/src/worker_image.rs (COMPILE_CACHE_ARG)
const COMPILE_CACHE_ARG: &str = "WEFT_COMPILE_CACHE";

/// The Cloud Build request for `req`, its context at `object` in `bucket`.
/// `account` is the build's service account as a resource path
/// (`projects/<p>/serviceAccounts/<email>`).
fn build_body(req: &BuildRequest, bucket: &str, object: &str, account: &str, machine: Option<&str>) -> Value {
    let mut args = vec!["build".to_string(), "--tag".into(), req.image_ref.clone()];
    // Every Cloud Build runs on a fresh machine, so a build's cache mounts
    // start empty and are gone after it: a worker build compiles where its
    // builder base already compiled, instead of seeding a cache nothing
    // keeps (`weft_compiler::worker_image`'s build step).
    args.extend(["--build-arg".into(), format!("{COMPILE_CACHE_ARG}=none")]);
    args.push(".".into());
    let mut steps = vec![json!({
        "name": "gcr.io/cloud-builders/docker",
        // The worker's Dockerfile uses BuildKit's cache mounts.
        "env": ["DOCKER_BUILDKIT=1"],
        "args": args,
    })];
    if let weft_core::builds::BuildRecipe::Dockerfile { result_file: Some(path) } = &req.recipe {
        steps.push(result_step(&req.image_ref, path));
    }
    json!({
        "source": { "storageSource": { "bucket": bucket, "object": object } },
        "steps": steps,
        "images": [req.image_ref],
        "timeout": BUILD_TIMEOUT,
        "serviceAccount": account,
        "options": options(machine),
        "tags": ["weft", format!("weft-project-{}", req.project_id.simple())],
    })
}

/// The `id` of the step that reads a build's result file, which is how a
/// poll finds that step's output from the build alone.
const RESULT_STEP_ID: &str = "weft-result";

/// The step that reads `path` out of the image the build step just made,
/// without running it, into Cloud Build's step output (`$BUILDER_OUTPUT/
/// output`, handed back base64-encoded in `results.buildStepOutputs` at
/// the step's index; the command given to `docker create` is never run,
/// it lets an image that names none be created). The steps share one Docker daemon, so the image is
/// there before `images` pushes it. A file the image lacks fails the step,
/// and with it the build. Every `$` is doubled: Cloud Build substitutes a
/// single one itself and refuses a name it does not know.
const RESULT_SCRIPT: &str = r#"set -e
id=$$(docker create "$$1" weft-never-run)
status=0
docker cp "$$id:$$2" "$$BUILDER_OUTPUT/output" || status=$$?
docker rm --force "$$id" >/dev/null || echo "could not remove the container $$id" >&2
exit $$status"#;

fn result_step(image_ref: &str, path: &str) -> Value {
    json!({
        "id": RESULT_STEP_ID,
        "name": "gcr.io/cloud-builders/docker",
        "entrypoint": "bash",
        // `bash -c <script> <$0> <$1> <$2>`: the image and the path go in
        // as arguments, never spliced into the script.
        "args": ["-c", RESULT_SCRIPT, RESULT_STEP_ID, image_ref, path],
    })
}

/// A succeeded build's result: the output of its [`RESULT_STEP_ID`] step,
/// decoded, when it has one. That step's output missing, empty or not
/// base64 is a failed build: the step exists because the request asked for
/// the file.
fn result_of(build: &Value) -> Result<Option<Vec<u8>>, String> {
    let steps = build.get("steps").and_then(Value::as_array).map(Vec::as_slice).unwrap_or_default();
    let Some(index) = steps.iter().position(|step| step.get("id").and_then(Value::as_str) == Some(RESULT_STEP_ID)) else {
        return Ok(None);
    };
    let output = build.pointer("/results/buildStepOutputs").and_then(Value::as_array).and_then(|outputs| outputs.get(index)).and_then(Value::as_str);
    let encoded = match output {
        Some(encoded) if !encoded.is_empty() => encoded,
        _ => return Err(format!("the build succeeded but its result step (step {index}) handed back no output")),
    };
    use base64::Engine as _;
    match base64::engine::general_purpose::STANDARD.decode(encoded) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) => Err(format!("the build succeeded but its result step (step {index}) handed back an output that is not base64: {e}")),
    }
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
        "SUCCESS" => match result_of(build) {
            Ok(result) => BuildStatus::Succeeded { result },
            Err(why) => BuildStatus::Failed { reason: why },
        },
        // Cancelled (by weft's own release, after the end was recorded, or
        // by somebody in the console) and expired (queued too long) are
        // ends Cloud Build knows and words: its words are the reason.
        "FAILURE" | "INTERNAL_ERROR" | "TIMEOUT" | "CANCELLED" | "EXPIRED" => BuildStatus::Failed { reason: detail() },
        _ => BuildStatus::Pending,
    }
}

/// The build context at `dir`, tarred and gzipped into `parts` as it is
/// made, a part at a time ([`Parts`]), then its end: no copy of the whole
/// archive is ever held. A failure is handed on as the last thing sent.
fn tar_context(dir: &Path, parts: tokio::sync::mpsc::Sender<anyhow::Result<Streamed>>) {
    let mut out = Parts { held: Vec::with_capacity(UPLOAD_PART), parts: parts.clone() };
    let made = (|| {
        let mut archive = tar::Builder::new(flate2::write::GzEncoder::new(&mut out, flate2::Compression::fast()));
        archive.follow_symlinks(false);
        archive.append_dir_all(".", dir)?;
        archive.into_inner()?.finish()?;
        anyhow::Ok(())
    })();
    let last = made.and_then(|()| {
        let held = std::mem::take(&mut out.held);
        if !held.is_empty() {
            out.send(held)?;
        }
        Ok(Streamed::End)
    });
    // The uploader gone has failed already, saying why.
    let _ = parts.blocking_send(last);
}

/// What the archive is written into: it hands on every [`UPLOAD_PART`]
/// bytes as a part, holding only the part being filled.
struct Parts {
    held: Vec<u8>,
    parts: tokio::sync::mpsc::Sender<anyhow::Result<Streamed>>,
}

impl Parts {
    fn send(&self, part: Vec<u8>) -> std::io::Result<()> {
        self.parts
            .blocking_send(Ok(Streamed::Part(part)))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "the upload of the build context stopped"))
    }
}

impl std::io::Write for Parts {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.held.extend_from_slice(bytes);
        while self.held.len() >= UPLOAD_PART {
            let rest = self.held.split_off(UPLOAD_PART);
            let part = std::mem::replace(&mut self.held, rest);
            self.send(part)?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A build for a worker packaged as a host binary is refused: workers
/// here are images ([`ImageBuilder::worker_packaging`]), so one reaching
/// this builder is a dispatcher that planned for another platform.
fn refuse_host_recipe(req: &BuildRequest) -> anyhow::Result<()> {
    match req.recipe {
        weft_core::builds::BuildRecipe::Dockerfile { .. } => Ok(()),
        weft_core::builds::BuildRecipe::Host => anyhow::bail!(
            "build {} of {} asks for a host build, which Cloud Build cannot run: workers on Google Cloud are images, \
             so the build must be planned as a Dockerfile (the builder's worker packaging is `image`)",
            req.name,
            req.image_ref
        ),
    }
}

#[async_trait]
impl ImageBuilder for CloudBuildImages {
    fn image_ref(&self, tag: &str) -> String {
        format!("{}/{tag}", self.gcp.artifact_registry.trim_end_matches('/'))
    }

    /// Workers run as Cloud Run services, from images compiled FROM the
    /// builder base the install was set up with.
    fn worker_packaging(&self) -> weft_core::builds::WorkerPackaging {
        weft_core::builds::WorkerPackaging::Image { builder: self.gcp.builder_base_image.clone() }
    }

    /// Uploads the whole context before it answers: Cloud Build reads the
    /// bucket's copy, so `req.staging` goes when this returns. Builds only
    /// a Dockerfile: a recipe compiled on a machine has no machine here.
    async fn start(&self, req: BuildRequest) -> anyhow::Result<BuildHandle> {
        refuse_host_recipe(&req)?;
        let dir = req.context_dir.clone();
        let object = Self::context_object(&req.name);
        // One part being made while the one before is sent.
        let (parts, sent) = tokio::sync::mpsc::channel(1);
        let made = tokio::task::spawn_blocking(move || tar_context(&dir, parts));
        let uploaded = self
            .google
            .upload_streamed(
                &format!(
                    "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=resumable&name={}",
                    self.gcp.build_bucket,
                    object.replace('/', "%2F")
                ),
                "application/gzip",
                sent,
            )
            .await;
        // The upload's own failure comes first: a producer that died
        // (a panic) shows only as parts that stopped, so its join failure
        // is added to the upload's words rather than said instead of them.
        let made = made.await;
        match (uploaded, made) {
            (Ok(()), Ok(())) => {}
            (Err(e), Ok(())) => return Err(e.context(format!("upload the build context of {}", req.name))),
            (Err(e), Err(join)) => {
                return Err(e.context(format!("upload the build context of {} (what made it failed: {join})", req.name)));
            }
            (Ok(()), Err(join)) => anyhow::bail!("the build context of {} was uploaded, but what made it failed: {join}", req.name),
        }
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

    /// The context is handed on as parts the upload can send: every part
    /// but the last exactly [`UPLOAD_PART`] long, then the end, and the
    /// parts together are the whole archive.
    #[test]
    fn the_context_goes_out_in_parts_then_its_end() {
        let dir = std::env::temp_dir().join(format!("weft-context-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        // Bytes gzip cannot shrink, so the archive takes several parts.
        let mut seed: u64 = 7;
        let noise: Vec<u8> = (0..(UPLOAD_PART * 2 + 1000))
            .map(|_| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (seed >> 33) as u8
            })
            .collect();
        std::fs::write(dir.join("noise.bin"), &noise).unwrap();
        let (parts, mut sent) = tokio::sync::mpsc::channel(64);
        tar_context(&dir, parts);
        std::fs::remove_dir_all(&dir).unwrap();
        let mut archive = Vec::new();
        let mut sizes = Vec::new();
        while let Streamed::Part(part) = sent.try_recv().expect("an end is sent").unwrap() {
            sizes.push(part.len());
            archive.extend(part);
        }
        assert!(sizes.len() >= 3, "{sizes:?}");
        assert!(sizes[..sizes.len() - 1].iter().all(|size| *size == UPLOAD_PART), "{sizes:?}");
        let mut read = tar::Archive::new(flate2::read::GzDecoder::new(archive.as_slice()));
        let mut found = None;
        for entry in read.entries().unwrap() {
            let mut entry = entry.unwrap();
            if entry.path().unwrap().ends_with("noise.bin") {
                let mut bytes = Vec::new();
                std::io::Read::read_to_end(&mut entry, &mut bytes).unwrap();
                found = Some(bytes);
            }
        }
        assert!(found.as_deref() == Some(noise.as_slice()), "the archive holds the file as it was");
    }

    /// A context that cannot be read ends in its failure, never in an
    /// end, so no cut-off archive is ever completed.
    #[test]
    fn a_context_that_cannot_be_read_ends_in_its_failure() {
        let (parts, mut sent) = tokio::sync::mpsc::channel(64);
        tar_context(std::path::Path::new("/no/such/weft/context"), parts);
        let mut last = None;
        while let Ok(item) = sent.try_recv() {
            last = Some(item);
        }
        assert!(matches!(last, Some(Err(_))));
    }

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
            recipe: weft_core::builds::BuildRecipe::Dockerfile { result_file: None },
            staging: weft_platform_traits::Staging::new(()),
            image_ref: "us-central1-docker.pkg.dev/acme/weft/weft-worker:abc".into(),
        };
        let body = build_body(&req, "bucket", "contexts/b1.tar.gz", "projects/acme/serviceAccounts/b@acme.iam.gserviceaccount.com", None);
        assert_eq!(body["images"][0], req.image_ref);
        let args: Vec<&str> = body["steps"][0]["args"].as_array().unwrap().iter().map(|a| a.as_str().unwrap()).collect();
        assert_eq!(args, ["build", "--tag", req.image_ref.as_str(), "--build-arg", "WEFT_COMPILE_CACHE=none", "."]);
        assert_eq!(body["serviceAccount"], "projects/acme/serviceAccounts/b@acme.iam.gserviceaccount.com");
        assert_eq!(body["options"]["logging"], "CLOUD_LOGGING_ONLY", "a build with its own account logs to Cloud Logging");
        assert!(body["options"].get("machineType").is_none(), "Cloud Build's own default machine, the free one");
        assert_eq!(body["steps"].as_array().unwrap().len(), 1, "no result step when no result file is asked");
        let bigger = build_body(&req, "bucket", "contexts/b1.tar.gz", "projects/acme/serviceAccounts/b@acme.iam.gserviceaccount.com", Some("E2_HIGHCPU_8"));
        assert_eq!(bigger["options"]["machineType"], "E2_HIGHCPU_8");
    }

    #[test]
    fn a_host_recipe_is_refused_naming_why() {
        let mut req = BuildRequest {
            name: "b1".into(),
            project_id: uuid::Uuid::from_u128(1),
            tenant: "local".into(),
            context_dir: "/ctx".into(),
            recipe: weft_core::builds::BuildRecipe::Dockerfile { result_file: None },
            staging: weft_platform_traits::Staging::new(()),
            image_ref: "us-central1-docker.pkg.dev/acme/weft/weft-worker:abc".into(),
        };
        refuse_host_recipe(&req).unwrap();
        req.recipe = weft_core::builds::BuildRecipe::Host;
        let e = refuse_host_recipe(&req).unwrap_err().to_string();
        assert!(e.contains("host build") && e.contains("weft-worker:abc"), "{e}");
    }

    /// A build asked for a result file reads it in a step of its own after
    /// the build step, the image and the path passed as arguments.
    #[test]
    fn a_result_file_adds_a_step_reading_it_out_of_the_image() {
        let req = BuildRequest {
            name: "b1".into(),
            project_id: uuid::Uuid::from_u128(1),
            tenant: "local".into(),
            context_dir: "/ctx".into(),
            recipe: weft_core::builds::BuildRecipe::Dockerfile { result_file: Some(weft_core::builds::LEFT_OUT_RECORD_PATH.into()) },
            staging: weft_platform_traits::Staging::new(()),
            image_ref: "us-central1-docker.pkg.dev/acme/weft/weft-worker:abc".into(),
        };
        let body = build_body(&req, "bucket", "contexts/b1.tar.gz", "projects/acme/serviceAccounts/b@acme.iam.gserviceaccount.com", None);
        let steps = body["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0]["args"][0], "build");
        let step = &steps[1];
        assert_eq!(step["id"], RESULT_STEP_ID);
        assert_eq!(step["name"], "gcr.io/cloud-builders/docker");
        assert_eq!(step["entrypoint"], "bash");
        let args: Vec<&str> = step["args"].as_array().unwrap().iter().map(|a| a.as_str().unwrap()).collect();
        assert_eq!(args, ["-c", RESULT_SCRIPT, RESULT_STEP_ID, req.image_ref.as_str(), "/weft-left-out.json"]);
        assert!(RESULT_SCRIPT.starts_with("set -e\n"));
        assert!(RESULT_SCRIPT.contains(r#"docker cp "$$id:$$2" "$$BUILDER_OUTPUT/output""#));
        assert!(
            !RESULT_SCRIPT.replace("$$", "").contains('$'),
            "every `$` is doubled, or Cloud Build substitutes it"
        );
    }

    /// A succeeded build answers its result step's output, decoded; that
    /// output missing, empty or not base64 fails it.
    #[test]
    fn a_succeeded_build_answers_its_result_steps_output() {
        let steps = json!([{ "args": ["build"] }, { "id": RESULT_STEP_ID }]);
        let ok = json!({ "status": "SUCCESS", "steps": steps, "results": { "buildStepOutputs": ["", "eyJwYWNrYWdlcyI6W119"] } });
        assert_eq!(status_of(&ok), BuildStatus::Succeeded { result: Some(br#"{"packages":[]}"#.to_vec()) });
        for (results, why) in [
            (json!({}), "no output"),
            (json!({ "buildStepOutputs": [""] }), "no output"),
            (json!({ "buildStepOutputs": ["", ""] }), "no output"),
            (json!({ "buildStepOutputs": ["", "not base64!"] }), "not base64"),
        ] {
            let BuildStatus::Failed { reason } = status_of(&json!({ "status": "SUCCESS", "steps": steps, "results": results })) else {
                panic!("a build without its result succeeded: {results}")
            };
            assert!(reason.contains("result step (step 1)") && reason.contains(why), "{reason}");
        }
        let no_result_step = json!({ "status": "SUCCESS", "steps": [{ "args": ["build"] }], "results": { "buildStepOutputs": [""] } });
        assert_eq!(status_of(&no_result_step), BuildStatus::Succeeded { result: None });
        let failed = json!({ "status": "FAILURE", "statusDetail": "step 1 failed", "steps": steps });
        assert!(matches!(status_of(&failed), BuildStatus::Failed { reason } if reason.contains("step 1 failed")));
        assert_eq!(status_of(&json!({ "status": "WORKING", "steps": steps })), BuildStatus::Pending);
    }

    #[test]
    fn a_build_status_is_read_as_weft_reads_builds() {
        assert_eq!(status_of(&json!({ "status": "WORKING" })), BuildStatus::Pending);
        assert_eq!(status_of(&json!({ "status": "SUCCESS" })), BuildStatus::Succeeded { result: None });
        assert_eq!(status_of(&json!({ "status": "CANCELLED" })), BuildStatus::Failed { reason: "CANCELLED".into() }, "an end Cloud Build words itself");
        let BuildStatus::Failed { reason } = status_of(&json!({ "status": "FAILURE", "statusDetail": "step 0 failed", "logUrl": "https://log" })) else {
            panic!()
        };
        assert!(reason.contains("step 0 failed") && reason.contains("https://log"));
    }
}
