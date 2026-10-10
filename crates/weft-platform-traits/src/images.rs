//! Building images and keeping them: the platform's image store.
//!
//! Every image weft runs (a project's worker, an infra node's own image,
//! a node-test image) is built inside the install from a staged context
//! and named by its content, so an image already present is the same bytes
//! a build would make and its build is skipped. Where images are built and
//! kept is the platform's: the local Docker daemon, or a cloud's build
//! service pushing to its registry.
//!
//! Building is split in two so a build outlives whoever started it:
//! `start` launches it and answers the builder's own id for it (which the
//! builder may choose), and `poll` asks about it by that id alone.

use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// What deleting an image did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageDeleted {
    /// It was there and is gone.
    Deleted,
    /// It was not there to delete (a build that failed before its push,
    /// or an image somebody already removed).
    Absent,
    /// A container still runs from it (a unit a removed project left
    /// behind while its teardown finishes), so it stays for now.
    InUse,
}

#[async_trait]
pub trait ImageBuilder: Send + Sync {
    /// The full reference an image tagged `tag` (`weft-worker:<hash>`)
    /// has in this platform's store. A pure function of the tag.
    fn image_ref(&self, tag: &str) -> String;

    /// Launch the build of a staged context, producing `req.image_ref`.
    /// Returns as soon as it runs; the build goes on on its own. The
    /// context stays on disk exactly as long as somebody holds
    /// `req.staging`: the request that staged it answers without waiting
    /// for the build and lets go of its own, so a builder that reads the
    /// context after this returns keeps `req.staging` until it is done
    /// reading (the build ended, or was stopped), and one that read it
    /// all here lets go of it here.
    async fn start(&self, req: BuildRequest) -> anyhow::Result<BuildHandle>;

    /// How `handle`'s build is doing. Answers from the name alone, so a
    /// process that did not start it can poll it.
    async fn poll(&self, handle: &BuildHandle) -> anyhow::Result<BuildStatus>;

    /// Whether `image_ref` already exists in the store.
    async fn image_exists(&self, image_ref: &str) -> anyhow::Result<bool>;

    /// Free what `handle`'s build holds: stop it if it still runs and drop
    /// anything staged for it. Called on every way a build ends, AFTER the
    /// ledger records how. Idempotent.
    async fn release(&self, handle: &BuildHandle);

    /// Delete `image_ref` from the store. Deleting an image that is not
    /// there is not an error; one a container still runs from is kept
    /// ([`ImageDeleted::InUse`]), for a later prune to take.
    async fn delete_image(&self, image_ref: &str) -> anyhow::Result<ImageDeleted>;
}

/// A staged build context to build. `context_dir` holds a `Dockerfile`
/// and everything it copies; `image_ref` is the content-addressed target
/// (from [`ImageBuilder::image_ref`]); `name` is the build's name, minted
/// by the caller and recorded before the build starts.
#[derive(Debug, Clone)]
pub struct BuildRequest {
    pub name: String,
    pub project_id: uuid::Uuid,
    pub tenant: String,
    pub context_dir: PathBuf,
    /// What keeps `context_dir` on disk ([`Staging`]).
    pub staging: Staging,
    pub image_ref: String,
    /// `ARG`s the Dockerfile declares (the worker's compile lane).
    pub build_args: Vec<(String, String)>,
}

/// What keeps a staged build context on disk: the context exists while
/// any clone of this is held, and goes with the last one. Built from
/// whatever owns the directory (the staging request's temp dir), so the
/// files are never copied and keep the times they were staged with, which
/// a compile cache relies on.
///
/// Dropping one never blocks the async runtime: the last one lets go of
/// the directory on a blocking thread when a runtime is there to run it.
#[derive(Clone)]
pub struct Staging {
    _owner: std::sync::Arc<dyn std::any::Any + Send + Sync>,
}

impl Staging {
    /// The context exists for as long as `owner` lives.
    pub fn new(owner: impl std::any::Any + Send + Sync) -> Self {
        Self { _owner: std::sync::Arc::new(OffThread(Some(owner))) }
    }
}

impl std::fmt::Debug for Staging {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Staging")
    }
}

/// A staging owner, dropped (by the last [`Staging`] clone) on a blocking
/// thread when a runtime is there to run it.
struct OffThread<T: Send + 'static>(Option<T>);

impl<T: Send + 'static> Drop for OffThread<T> {
    fn drop(&mut self) {
        let Some(owner) = self.0.take() else { return };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn_blocking(move || drop(owner));
        }
    }
}

/// The builder's own id for a build, persisted on the `image_build` row
/// so a poll survives a restart. The builder may choose it (Cloud Build
/// names its builds itself), so it is the one [`ImageBuilder::start`]
/// answers, never the name the caller minted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildHandle {
    pub external_build_id: String,
    /// Where a person reads the build's log, when the builder keeps one
    /// at an address (Cloud Build's console page).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_url: Option<String>,
}

impl BuildHandle {
    /// A build known by its id alone.
    pub fn named(external_build_id: impl Into<String>) -> Self {
        Self { external_build_id: external_build_id.into(), log_url: None }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildStatus {
    /// Still running; poll again later.
    Pending,
    /// Produced `image_ref`.
    Succeeded,
    /// The build failed; `reason` is the builder's error.
    Failed { reason: String },
    /// Nothing runs under this name any more: it was released, or what
    /// ran it went away. Whether that is a failure is the ledger's to
    /// say: whoever released it recorded the end first.
    Gone,
}

/// A dumb in-memory builder for layer-3 tests: records every call and
/// returns scripted results.
#[cfg(any(test, feature = "test-helpers"))]
#[derive(Default)]
pub struct FakeImageBuilder {
    inner: std::sync::Mutex<FakeImageBuilderInner>,
}

#[cfg(any(test, feature = "test-helpers"))]
#[derive(Default)]
struct FakeImageBuilderInner {
    existing: std::collections::HashSet<String>,
    poll_results: std::collections::HashMap<String, BuildStatus>,
    starts: Vec<BuildRequest>,
    polls: Vec<String>,
    exists_checks: Vec<String>,
    releases: Vec<String>,
    deletes: Vec<String>,
    /// Name each build `<name>-id` instead of the caller's name, the way
    /// Cloud Build names its own.
    names_its_own: bool,
    /// Builds a poll of answers an error (the builder unreachable).
    unanswered: std::collections::HashSet<String>,
    /// The image each started build pushes, by its id: a poll answering
    /// `Succeeded` puts it in `existing`, as a real push does.
    pushes: std::collections::HashMap<String, String>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl FakeImageBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_image_exists(&self, image_ref: &str) {
        self.inner.lock().unwrap().existing.insert(image_ref.to_string());
    }

    /// Make every poll of `external_build_id` fail, as an unreachable
    /// builder does.
    pub fn fail_polls(&self, external_build_id: &str) {
        self.inner.lock().unwrap().unanswered.insert(external_build_id.to_string());
    }

    /// Give each build an id of the builder's own, as Cloud Build does.
    pub fn name_builds_itself(&self) {
        self.inner.lock().unwrap().names_its_own = true;
    }

    pub fn set_poll_result(&self, external_build_id: &str, status: BuildStatus) {
        self.inner.lock().unwrap().poll_results.insert(external_build_id.to_string(), status);
    }

    pub fn starts(&self) -> Vec<BuildRequest> {
        self.inner.lock().unwrap().starts.clone()
    }

    pub fn polls(&self) -> Vec<String> {
        self.inner.lock().unwrap().polls.clone()
    }

    pub fn exists_checks(&self) -> Vec<String> {
        self.inner.lock().unwrap().exists_checks.clone()
    }

    pub fn releases(&self) -> Vec<String> {
        self.inner.lock().unwrap().releases.clone()
    }

    pub fn deletes(&self) -> Vec<String> {
        self.inner.lock().unwrap().deletes.clone()
    }
}

#[cfg(any(test, feature = "test-helpers"))]
#[async_trait]
impl ImageBuilder for FakeImageBuilder {
    fn image_ref(&self, tag: &str) -> String {
        format!("fake.registry/{tag}")
    }

    async fn start(&self, req: BuildRequest) -> anyhow::Result<BuildHandle> {
        let mut inner = self.inner.lock().unwrap();
        let id = if inner.names_its_own { format!("{}-id", req.name) } else { req.name.clone() };
        inner.pushes.insert(id.clone(), req.image_ref.clone());
        inner.starts.push(req);
        Ok(BuildHandle::named(id))
    }

    async fn poll(&self, handle: &BuildHandle) -> anyhow::Result<BuildStatus> {
        let mut inner = self.inner.lock().unwrap();
        inner.polls.push(handle.external_build_id.clone());
        if inner.unanswered.contains(&handle.external_build_id) {
            anyhow::bail!("the builder did not answer");
        }
        let status = inner.poll_results.get(&handle.external_build_id).cloned().unwrap_or(BuildStatus::Pending);
        if status == BuildStatus::Succeeded {
            if let Some(image_ref) = inner.pushes.get(&handle.external_build_id).cloned() {
                inner.existing.insert(image_ref);
            }
        }
        Ok(status)
    }

    async fn image_exists(&self, image_ref: &str) -> anyhow::Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        inner.exists_checks.push(image_ref.to_string());
        Ok(inner.existing.contains(image_ref))
    }

    async fn release(&self, handle: &BuildHandle) {
        let mut inner = self.inner.lock().unwrap();
        inner.releases.push(handle.external_build_id.clone());
        inner.poll_results.insert(handle.external_build_id.clone(), BuildStatus::Gone);
    }

    async fn delete_image(&self, image_ref: &str) -> anyhow::Result<ImageDeleted> {
        let mut inner = self.inner.lock().unwrap();
        inner.deletes.push(image_ref.to_string());
        Ok(match inner.existing.remove(image_ref) {
            true => ImageDeleted::Deleted,
            false => ImageDeleted::Absent,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_handle_wire_round_trips() {
        let h = BuildHandle::named("build-op-42");
        let v = serde_json::to_value(&h).unwrap();
        assert_eq!(v["external_build_id"], "build-op-42");
        let back: BuildHandle = serde_json::from_value(v).unwrap();
        assert_eq!(back.external_build_id, "build-op-42");
    }

    #[tokio::test]
    async fn fake_builder_records_and_replays() {
        let b = FakeImageBuilder::new();
        b.set_image_exists("reg/weft-worker:present");
        assert!(b.image_exists("reg/weft-worker:present").await.unwrap());
        assert!(!b.image_exists("reg/weft-worker:absent").await.unwrap());
        assert_eq!(b.exists_checks().len(), 2);
        let handle = b
            .start(BuildRequest {
                name: "weft-build-1".into(),
                project_id: uuid::Uuid::from_u128(0x100),
                tenant: "t".into(),
                context_dir: PathBuf::from("/tmp/ctx"),
                staging: Staging::new(()),
                image_ref: "reg/weft-worker:x".into(),
                build_args: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(b.starts().len(), 1);
        assert_eq!(b.poll(&handle).await.unwrap(), BuildStatus::Pending);
        b.set_poll_result(&handle.external_build_id, BuildStatus::Succeeded);
        assert_eq!(b.poll(&handle).await.unwrap(), BuildStatus::Succeeded);
        b.delete_image("reg/weft-worker:present").await.unwrap();
        assert!(!b.image_exists("reg/weft-worker:present").await.unwrap());
    }
}
