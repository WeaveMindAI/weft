//! Building a project version inside the install: the one way a project's
//! images come to exist, on a laptop and on a cloud alike.
//!
//! The CLI uploads the version's files (only the blobs the store lacks,
//! straight to the bucket) and asks for a build of that version. Here the
//! version's files are fetched back and checked against their hashes, the
//! project is compiled with the catalog this install ships, every image it
//! needs is planned with content-addressed refs in the platform's image
//! store, and the stale ones are started on the platform's builder
//! (`weft_platform_traits::ImageBuilder`). The request answers as soon as
//! they run: a build takes minutes, and a request held open that long with
//! nothing on the wire is one any network path between may drop. The
//! version is written down as waiting on them (`waiting`), and the build
//! loop (`follow`) sees each build through and registers the version once
//! they all ended; the caller only follows it. Nothing the client computed
//! is believed: not a hash, not an image, not a stored file's metadata.
//!
//! What the compile cannot do on its own is read an `@asset` that lives on
//! the author's disk outside the project, or a URL the author fetched: the
//! CLI resolves those (uploading any file the asset plane lacks) and sends
//! the resolutions with the request. A stored-file resolution is re-read
//! from the store here and rebuilt from what the store says; a text
//! resolution is program data, the same as the source it came with.

pub mod blob_cache;
pub mod follow;
pub mod ledger;
pub mod prune;
pub mod source;
pub mod waiting;

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use weft_platform_traits::{BuildHandle, BuildRequest, ImageBuilder, Staging};

/// The control point the version builder (`crate::build`) calls around REAL build work, so the
/// dispatcher's `building` transition only engages when something actually
/// builds (a pure cache-hit verb never flips the marker, so concurrent runs on
/// an up-to-date project never serialize against each other).
///
/// The mechanism behind the gate is weft's (the project row's `transition`
/// marker, held by the request while it starts the builds and by the builds
/// themselves after, see `crate::transition`); the KNOWLEDGE of "a real
/// build is starting" is the builder's. This trait is the seam between them.
#[async_trait]
pub trait BuildGate: Send + Sync {
    /// Called once, just before the first actual image build is submitted.
    /// Errs when the project cannot enter the `building` transition right now
    /// (another verb is already building, or the lifecycle is mid-flip); the
    /// builder aborts with that error and the verb surfaces it. From here
    /// until [`Self::let_go`] the start is on record as alive.
    async fn begin(&self) -> anyhow::Result<()>;

    /// Called once the starts [`Self::begin`] opened are all answered and
    /// the version waiting on them is written down, whether or not anybody
    /// still waits on the request: the project rests from here on its
    /// waiting version. A no-op when `begin` was never called, or refused.
    async fn let_go(&self);
}

/// Why a build request failed when the person cancelled the project's
/// build while it was starting its images: an image claimed after the
/// cancel is refused (`ledger::claim`), and so is a version written down
/// after it (`waiting::ask`).
#[derive(Debug)]
pub struct BuildCancelled;

impl std::fmt::Display for BuildCancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the build was cancelled")
    }
}

impl std::error::Error for BuildCancelled {}

/// Whether `e` is a cancel's refusal ([`BuildCancelled`]), wherever in its
/// chain.
pub fn cancelled(e: &anyhow::Error) -> bool {
    e.downcast_ref::<BuildCancelled>().is_some()
}

/// Content-addressed image refs, named in the platform's image store.
pub struct ImageTags<'a>(pub &'a dyn ImageBuilder);

impl weft_compiler::build_plan::TagPolicy for ImageTags<'_> {
    fn worker_ref(&self, binary_hash: &str) -> String {
        self.0.image_ref(&weft_compiler::build::worker_image_tag(binary_hash))
    }
    fn infra_ref(&self, image_name: &str, content_hash: &str) -> String {
        self.0.image_ref(&weft_compiler::image_set::infra_image_tag(image_name, content_hash))
    }
}

/// The infra places of `after` whose images differ from `before` (what
/// the last build registered), sorted. A place new to this build replaces
/// nothing, so it is left out.
pub fn replaced_infra_images(
    before: &crate::project_store::InfraImageTags,
    after: &BTreeMap<String, BTreeMap<String, String>>,
) -> Vec<String> {
    after
        .iter()
        .filter(|(place, images)| {
            before.get(*place).is_some_and(|old| {
                old.len() != images.len() || images.iter().any(|(name, image)| old.get(name) != Some(image))
            })
        })
        .map(|(place, _)| place.clone())
        .collect()
}

/// What a build request comes to: the version ready to register, or the
/// version build now waiting on its images.
#[derive(Debug)]
pub enum VersionBuild {
    /// Every image the version needs is in the registry: register it
    /// (`waiting::register`) under `hold`, which claims them all.
    Ready { version: Box<Version>, hold: prune::ImageHold },
    /// These builds run for the version (started by this request, or
    /// already running for the same content), and the version waits on
    /// them: the install registers it once they ended.
    Waiting(weft_core::builds::BuildsUnderway),
}

/// A version as a build compiled it: what registration writes, and what a
/// version waiting on its builds keeps until then.
#[derive(Debug, Clone)]
pub struct Version {
    /// The project's name, as the request named it.
    pub name: String,
    /// The version's files, registered as the program's source.
    pub manifest: weft_core::project::hash::Manifest,
    /// The answer registration completes (its `replaced_infra_images`,
    /// from [`replaced_infra_images`]) and hands the client.
    pub program: weft_core::builds::BuiltProgram,
    /// Every image ref this version runs, built now or found already
    /// there. Registration records them as the project's running version
    /// (`ledger::note_running`); the client has no use for it.
    pub images: Vec<String>,
    /// Its request's place in the project's order of asks
    /// (`ProjectStoreOps::next_build_ask`): it registers only over an
    /// older one (`waiting::settle`).
    pub ask: i64,
}

/// The project's files and stored-file metadata, as the build reads them.
/// The production answer is the broker's storage plane
/// (`crate::storage::BrokerStorage`); a test hands a map.
#[async_trait::async_trait]
pub trait ProjectStorage: Send + Sync {
    /// The bytes of one stored file, by its tenant-anchored key.
    async fn read(&self, key: &str) -> Result<Vec<u8>>;
    /// What the store records about one stored file.
    async fn meta(&self, key: &str) -> Result<weft_core::storage::StoredFileMeta>;
}

/// Everything a build needs that belongs to the install rather than the
/// request: the base images, the platform's builder and image store, the
/// image ledger, and how many worker builds may compile side by side.
#[derive(Clone)]
pub struct VersionBuilder {
    /// The prebuilt builder a worker compiles in, and the image it runs
    /// on when its project names none.
    pub bases: weft_compiler::worker_image::BaseImages,
    pub images: Arc<dyn ImageBuilder>,
    pub pool: sqlx::PgPool,
    /// How many worker builds compile side by side, each in a compile
    /// cache of its own (`weft_compiler::worker_image::COMPILE_LANE_ARG`).
    pub compile_lanes: u32,
    /// The reclaims registered builds asked for (`prune::AfterBuildPrunes`).
    pub prunes: Arc<prune::AfterBuildPrunes>,
    /// The version files this replica fetched before (`blob_cache`).
    pub blobs: blob_cache::BlobCache,
}

impl VersionBuilder {
    /// Build `request` for `project_id`: fetch and check its files, compile,
    /// plan, and start a build of every stale image. `gate` is entered
    /// before the first real build; a version whose images all exist never
    /// touches it. `hold` claims every planned image against a prune until
    /// the version registers. `ask` is the request's place in the
    /// project's order of asks, taken as it arrived.
    #[allow(clippy::too_many_arguments)]
    pub async fn build(
        &self,
        storage: &dyn ProjectStorage,
        project_id: uuid::Uuid,
        tenant: &str,
        request: &weft_core::builds::VersionBuildRequest,
        ask: i64,
        gate: Arc<dyn BuildGate>,
        hold: prune::ImageHold,
    ) -> Result<VersionBuild> {
        let workdir = tempfile::Builder::new()
            .prefix("weft-version-")
            .tempdir()
            .context("create the build's working directory")?;
        let root = workdir.path().to_path_buf();
        // The image contexts are staged under it, and a build started from
        // one may read it after this request answered: every start keeps a
        // clone, and the directory goes with the last (`Staging`).
        let staging = Staging::new(workdir);
        source::materialize(storage, &self.blobs, tenant, &request.manifest, &root).await?;

        let assets = request.assets.clone();
        let node_set = request.node_set;
        let bases = self.bases.clone();
        let images = self.images.clone();
        // The compile and the staging are blocking filesystem work.
        let (mut compiled, catalog, project) = tokio::task::spawn_blocking(move || source::compile(&root))
            .await
            .context("the compile task panicked")??;
        resolve_assets(storage, tenant, &mut compiled, &assets).await?;
        let (plan, definition) = tokio::task::spawn_blocking(move || {
            let plan = weft_compiler::build_plan::plan_build_from(
                &project,
                &compiled,
                &catalog,
                &bases,
                &ImageTags(images.as_ref()),
                node_set,
            )
            .map_err(|e| anyhow!("plan the build: {e}"))?;
            Ok::<_, anyhow::Error>((plan, compiled))
        })
        .await
        .context("the planning task panicked")??;

        let images: Vec<String> =
            plan.images.iter().map(|image| image.image_ref.clone()).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        let infra_images = infra_places(&definition, &plan.images)?;
        let version = Version {
            name: request.name.clone(),
            manifest: request.manifest.clone(),
            program: weft_core::builds::BuiltProgram {
                definition,
                binary_hash: plan.binary_hash,
                definition_hash: plan.definition_hash,
                infra_hash: plan.infra_hash,
                implementations: plan.implementations,
                infra_images,
                replaced_infra_images: Vec::new(),
            },
            images,
            ask,
        };
        self.start_images(version, &plan.images, &staging, project_id, tenant, gate, hold).await
    }

    /// Start a build of every planned image the registry lacks, side by
    /// side, and write `version` down as waiting on the builds it needs:
    /// each one this request started, or the one already running for the
    /// same ref (another project with the same content). Ready when every
    /// image is there, including one a build of another request finished
    /// meanwhile. Every image is claimed in `hold` before the registry is
    /// asked about any, so a prune cannot take one this build found. The
    /// gate is entered once, before the first build. Each build started
    /// keeps a clone of `staging`, what keeps the contexts on disk.
    ///
    /// Nothing here waits for a build to end: the build loop sees each one
    /// through and registers the version (`follow`, `waiting`). A start
    /// that fails stops the builds this request may stop, so the project is
    /// left with nothing half started, and the request fails. All of that,
    /// from the gate entered to the gate let go, runs on a task the request
    /// waits on without owning: a caller that goes away drops the request,
    /// never the starts, the version waiting on them, or what they owe once
    /// they end.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_images(
        &self,
        version: Version,
        images: &[weft_compiler::build_plan::PlannedImage],
        staging: &Staging,
        project_id: uuid::Uuid,
        tenant: &str,
        gate: Arc<dyn BuildGate>,
        hold: prune::ImageHold,
    ) -> Result<VersionBuild> {
        let refs: Vec<String> = images.iter().map(|image| image.image_ref.clone()).collect();
        hold.claim(&refs).await?;
        let mut stale = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for image in images {
            if !seen.insert(image.image_ref.clone()) {
                continue;
            }
            if !self.images.image_exists(&image.image_ref).await? {
                stale.push(image.clone());
            }
        }
        if stale.is_empty() {
            return Ok(VersionBuild::Ready { version: Box::new(version), hold });
        }
        let (this, staging, tenant) = (self.clone(), staging.clone(), tenant.to_string());
        let (answer, answered) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let started = this.start_stale(stale, &staging, project_id, &tenant, gate.as_ref(), version, hold).await;
            gate.let_go().await;
            // Nobody waits any more (the caller went away): what went wrong
            // is said here, or nowhere.
            if let Err(Err(e)) = answer.send(started) {
                tracing::error!(
                    target: "weft_dispatcher::build",
                    %project_id,
                    error = %format!("{e:#}"),
                    "starting the project's images failed after its request went away"
                );
            }
        });
        answered.await.map_err(|_| anyhow!("the start of the project's images ended without an answer"))?
    }

    /// [`Self::start_images`]' starts and the version written down as
    /// waiting on them, on the task the request waits on.
    #[allow(clippy::too_many_arguments)]
    async fn start_stale(
        &self,
        stale: Vec<weft_compiler::build_plan::PlannedImage>,
        staging: &Staging,
        project_id: uuid::Uuid,
        tenant: &str,
        gate: &dyn BuildGate,
        version: Version,
        hold: prune::ImageHold,
    ) -> Result<VersionBuild> {
        gate.begin().await?;
        // Each image starts on a task of its own, so one start's panic is
        // that image's failure. Each carries the staging until its start is
        // recorded.
        let tasks: Vec<_> = stale
            .into_iter()
            .map(|image| {
                let (this, staging, tenant) = (self.clone(), staging.clone(), tenant.to_string());
                tokio::spawn(async move { this.start_one(&image, &staging, project_id, &tenant).await })
            })
            .collect();
        let starts = futures::future::join_all(tasks).await.into_iter().map(|joined| match joined {
            Ok(started) => started,
            Err(e) => Err(anyhow!("an image's start ended without an answer: {e}")),
        });
        let mut underway = Vec::new();
        // The builds this project may stop: the ones it started, and one
        // it joined that it had started before.
        let mut stoppable = Vec::new();
        let mut failures = Vec::new();
        for start in starts {
            match start {
                Ok(Some((build, ours))) => {
                    if ours {
                        stoppable.push(build.clone());
                    }
                    underway.push(build);
                }
                Ok(None) => {}
                Err(e) => failures.push(e),
            }
        }
        if failures.is_empty() {
            if underway.is_empty() {
                return Ok(VersionBuild::Ready { version: Box::new(version), hold });
            }
            // A claim that lapsed may have let a prune take an image the
            // version found, so it is never passed to a waiting version.
            let written = match hold.confirm().await {
                Ok(()) => waiting::ask(&self.pool, hold, project_id, tenant, &version, &underway, crate::lease::now_unix())
                    .await
                    .context("write the version down as waiting on its builds"),
                Err(e) => Err(e),
            };
            match written {
                Ok(build) => return Ok(VersionBuild::Waiting(weft_core::builds::BuildsUnderway { build, images: underway })),
                Err(e) => failures.push(e),
            }
        }
        // A failed stop never hides why a start failed: every error goes
        // into the answer.
        if let Err(e) = self.stop(&stoppable).await {
            failures.push(e.context("stop the builds this request started"));
        }
        if failures.iter().any(cancelled) {
            return Err(BuildCancelled.into());
        }
        let mut failures = failures.into_iter();
        let first = failures.next().expect("only a start that failed gets here");
        let others: Vec<String> = failures.map(|e| format!("{e:#}")).collect();
        if others.is_empty() {
            return Err(first);
        }
        bail!("{first:#}\n\n{} other image(s) failed too:\n{}", others.len(), others.join("\n\n"))
    }

    /// Start one image's build, or join the one already running for the
    /// same ref; `None` when a build of it finished meanwhile. With the
    /// build, whether a cancel of this project may stop it
    /// (`ledger::Claim::stoppable`).
    async fn start_one(
        &self,
        image: &weft_compiler::build_plan::PlannedImage,
        staging: &Staging,
        project_id: uuid::Uuid,
        tenant: &str,
    ) -> Result<Option<(weft_core::builds::ImageBuild, bool)>> {
        let now = crate::lease::now_unix();
        let lanes = (image.kind == weft_compiler::build_plan::ImageKind::Worker).then_some(self.compile_lanes);
        let claim = ledger::claim(&self.pool, self.images.as_ref(), &image.image_ref, project_id, tenant, lanes, now).await?;
        let stoppable = claim.stoppable();
        let name = match claim {
            ledger::Claim::Start { name, lane } => {
                self.start(image, staging, project_id, tenant, &name, lane).await?;
                name
            }
            ledger::Claim::Join { name, .. } => name,
            ledger::Claim::Built => return Ok(None),
        };
        Ok(Some((weft_core::builds::ImageBuild { image: image.image_ref.clone(), name }, stoppable)))
    }

    /// Stop `builds`: each one's end recorded as cancelled, then freed on
    /// the builder. A build not made on the builder yet is freed by its
    /// starter, which finds the row no longer its own (`ledger::started`);
    /// one that ended already is left as it ended.
    async fn stop(&self, builds: &[weft_core::builds::ImageBuild]) -> Result<()> {
        for build in builds {
            if let Some(Some(builder_id)) = ledger::cancel(&self.pool, &build.image, &build.name, crate::lease::now_unix()).await? {
                self.images.release(&BuildHandle::named(builder_id)).await;
            }
        }
        Ok(())
    }

    /// Cancel what `project` builds: every build it started is stopped
    /// ([`Self::stop`]), the ones other projects started go on for them,
    /// and its waiting version ends `cancelled` (`waiting::cancel`).
    pub async fn cancel_project(&self, project: uuid::Uuid) -> Result<()> {
        let own: Vec<weft_core::builds::ImageBuild> = ledger::running(&self.pool, Some(project))
            .await?
            .into_iter()
            .filter(|build| build.project_id == project)
            .map(|build| weft_core::builds::ImageBuild { image: build.image_ref, name: build.build_name })
            .collect();
        self.stop(&own).await?;
        waiting::cancel(&self.pool, project).await
    }

    /// Start the build `name` of `image` (in `lane`, for a worker), whose
    /// row the ledger already holds, and record the builder's id for it. A
    /// start that fails is recorded as the build's end; a build made whose
    /// id cannot be recorded is stopped.
    async fn start(
        &self,
        image: &weft_compiler::build_plan::PlannedImage,
        staging: &Staging,
        project_id: uuid::Uuid,
        tenant: &str,
        name: &str,
        lane: Option<u32>,
    ) -> Result<()> {
        let build_args = match lane {
            Some(lane) => vec![(weft_compiler::worker_image::COMPILE_LANE_ARG.to_string(), lane.to_string())],
            None => Vec::new(),
        };
        let request = BuildRequest {
            name: name.to_string(),
            project_id,
            tenant: tenant.to_string(),
            context_dir: image.context_dir.clone(),
            staging: staging.clone(),
            image_ref: image.image_ref.clone(),
            build_args,
        };
        let handle = match self.images.start(request).await {
            Ok(handle) => handle,
            Err(e) => {
                let e = e.context(format!("start the build of {}", image.image_ref));
                let recorded = ledger::finish(
                    &self.pool,
                    &image.image_ref,
                    name,
                    ledger::Outcome::Failed(format!("{e:#}")),
                    crate::lease::now_unix(),
                )
                .await
                .map(|_| ());
                return Err(with_secondary(e, recorded));
            }
        };
        match ledger::started(&self.pool, &image.image_ref, name, &handle).await {
            Ok(true) => Ok(()),
            // Cancelled, or given up on, while it was being made: nobody
            // will ask about it, so it is freed here. The row's end is the
            // answer the wait reads.
            Ok(false) => {
                self.images.release(&handle).await;
                Ok(())
            }
            Err(e) => {
                self.images.release(&handle).await;
                let recorded = ledger::finish(
                    &self.pool,
                    &image.image_ref,
                    name,
                    ledger::Outcome::Failed(format!("its id on the builder could not be recorded, so it was stopped: {e:#}")),
                    crate::lease::now_unix(),
                )
                .await
                .map(|_| ());
                Err(with_secondary(e, recorded))
            }
        }
    }
}

/// `primary` as the error to report, with a failure to record it beneath:
/// the reason the build failed stays the first thing anyone reads.
fn with_secondary(primary: anyhow::Error, recorded: Result<()>) -> anyhow::Error {
    match recorded {
        Ok(()) => primary,
        Err(finish) => anyhow!("{primary:#}\n(and recording that end failed too: {finish:#})"),
    }
}

/// Apply the author's `@asset` resolutions to the definition the build
/// compiled. A stored-file resolution names a key; its value is rebuilt
/// from what the store records for that key (never the metadata the client
/// sent), the key must be this tenant's, and the stored kind must be the
/// kind the ref declared. A text resolution is taken as sent.
async fn resolve_assets(
    storage: &dyn ProjectStorage,
    tenant: &str,
    definition: &mut weft_core::ProjectDefinition,
    sent: &BTreeMap<String, serde_json::Value>,
) -> Result<()> {
    let mut map = BTreeMap::new();
    let mut refused = Vec::new();
    let file_refs = weft_compiler::file_ref::collect_asset_refs(definition)
        .into_iter()
        .chain(weft_compiler::file_ref::collect_runtime_key_refs(definition));
    for r in file_refs {
        let key = r.resolution_key();
        let Some(value) = sent.get(&key) else { continue };
        let claimed = match weft_core::storage::StoredFile::from_value(value) {
            Ok(file) => file,
            Err(e) => {
                refused.push(format!("@asset({:?}, {}): {e}", r.path, r.ty));
                continue;
            }
        };
        if !claimed.key.starts_with(&format!("{tenant}/")) {
            refused.push(format!("@asset({:?}, {}): the stored file is not this install's", r.path, r.ty));
            continue;
        }
        let meta = storage.meta(&claimed.key).await.with_context(|| format!("read the stored file behind @asset({:?})", r.path))?;
        let file = weft_core::storage::StoredFile::from(&meta);
        if let Some(declared) = r.ty.concrete_file_kind() {
            if declared != weft_core::weft_type::FileKind::Blob && file.kind() != declared {
                refused.push(format!(
                    "@asset({:?}, {}): the stored file is {} ({}), not {}",
                    r.path,
                    r.ty,
                    file.kind().primitive(),
                    meta.mime_type,
                    declared.primitive()
                ));
                continue;
            }
        }
        map.insert(key, weft_core::storage::typed_file_value(&file, &r.ty));
    }
    for r in weft_compiler::file_ref::collect_text_refs(definition) {
        if let Some(value) = sent.get(&r.resolution_key()) {
            map.insert(r.resolution_key(), value.clone());
        }
    }
    if !refused.is_empty() {
        bail!("assets the build cannot use:\n  {}", refused.join("\n  "));
    }
    weft_compiler::file_ref::apply_asset_resolutions(definition, &map)
        .map_err(|errs| anyhow!("unresolved assets:\n  {}", errs.join("\n  ")))
}

/// Every infra place's image refs, from the plan: the images belong to
/// the node type, so each place of one node carries the same refs.
fn infra_places(
    definition: &weft_core::ProjectDefinition,
    images: &[weft_compiler::build_plan::PlannedImage],
) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
    let places = weft_core::project::selection::every_place(definition);
    let mut out: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for img in images.iter().filter(|i| i.kind == weft_compiler::build_plan::ImageKind::Infra) {
        let (Some(node_id), Some(image_name)) = (&img.node_id, &img.image_name) else {
            bail!("planned infra image {} is missing its node or name", img.image_ref);
        };
        let mut at_any_place = false;
        for place in places.iter().filter(|place| &place.id == node_id) {
            at_any_place = true;
            let spelled = weft_core::project::address_of(definition, &place.id, &place.path);
            out.entry(spelled).or_default().insert(image_name.clone(), img.image_ref.clone());
        }
        if !at_any_place {
            bail!(
                "planned infra image {} belongs to '{}', which is at no place in the program",
                img.image_ref,
                weft_core::project::plain_id(node_id)
            );
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
