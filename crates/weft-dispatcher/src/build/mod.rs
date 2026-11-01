//! Building a project version inside the install: the one way a project's
//! images come to exist, on a laptop and on a cloud alike.
//!
//! A build builds one version the install holds: a working copy's files
//! stamped into a version (`crate::actions::versions::stamp`), or a version
//! named as it is. Here the version's files are fetched back and checked
//! against their hashes (all but `assets/`, which the compile reads only
//! when a `@file` names one), the project is compiled with the catalog
//! this install ships, its `@asset`s are resolved by the install itself
//! (`assets`), every image it needs is planned with content-addressed refs
//! in the platform's image store, and the stale ones are started on the
//! platform's builder (`weft_platform_traits::ImageBuilder`). The request
//! answers as soon as they run: a build takes minutes, and a request held
//! open that long with nothing on the wire is one any network path between
//! may drop. The version is written down as waiting on them (`waiting`),
//! and the build loop (`follow`) sees each build through and registers the
//! version once they all ended; the caller only follows it. Nothing a
//! client computed is believed: not a hash, not an image, not a stored
//! file's metadata.

pub mod assets;
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
    /// The project's name, as the version's `weft.toml` says.
    pub name: String,
    /// The version's files, registered as the program's source.
    pub manifest: weft_core::project::hash::Manifest,
    /// What its files were compiled with, registered with them so the
    /// install can build the version again (`crate::maintenance`). `None`
    /// only for a version an older weft asked for and wrote down waiting,
    /// which kept none.
    pub inputs: Option<weft_core::builds::BuildInputs>,
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

/// What a build does between its compile and its plan, which differs by
/// who asked: resolve the program's `@asset`s, and refuse a run the build
/// is for that could not start. A build of a version asked for now
/// resolves them from the version (`assets::Resolving`); the build of a
/// registered version again on a newer weft takes the resolutions it was
/// first built with ([`Kept`]).
#[async_trait::async_trait]
pub trait Prepare: Send + Sync {
    /// What each of `definition`'s `@asset`s stands for.
    async fn assets(&self, storage: &dyn ProjectStorage, tenant: &str, definition: &weft_core::ProjectDefinition) -> Result<Assets>;
    /// Refuse (a [`RunRefused`]) the run this build is for, held to the
    /// compiled program, when it could not start.
    async fn check(&self, definition: &weft_core::ProjectDefinition) -> Result<()>;
}

/// What a build's `@asset`s stand for ([`Prepare::assets`]).
#[derive(Debug)]
pub struct Assets {
    /// Each `@asset`'s value, by resolution key: a stored file, or the
    /// marker naming a text's content (`weft_core::content`). What the
    /// program carries, and what the build keeps (`BuildInputs::assets`).
    pub resolutions: weft_core::project::AssetResolutions,
    /// The value of each text content those name, which the build's gate
    /// reads (`weft_compiler::unsettled`).
    pub contents: weft_core::content::ContentValues,
}

/// Why a build refused the run it was for: the refusal the run itself
/// would answer, so the caller reads it the same way (a 422 with every
/// line).
#[derive(Debug)]
pub struct RunRefused(pub weft_core::run_spec::Refusal);

impl std::fmt::Display for RunRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the run this build is for cannot start:\n{}", self.0)
    }
}

impl std::error::Error for RunRefused {}

/// The resolutions a registered version was first built with, taken as
/// they were (`BuildInputs::assets`). A stored-file resolution is still
/// rebuilt from what the store records for its key, which must be this
/// tenant's and of the kind the ref declares; a text resolution names
/// its content (`weft_core::content`), whose bytes are read back from the
/// tenant's assets (held to their hash), and one an older weft kept as the
/// text itself is stored and named now, so the build this registers keeps
/// the name.
pub struct Kept<'a>(pub &'a weft_core::project::AssetResolutions);

#[async_trait::async_trait]
impl Prepare for Kept<'_> {
    async fn assets(&self, storage: &dyn ProjectStorage, tenant: &str, definition: &weft_core::ProjectDefinition) -> Result<Assets> {
        kept_assets(storage, tenant, definition, self.0).await
    }

    async fn check(&self, _definition: &weft_core::ProjectDefinition) -> Result<()> {
        Ok(())
    }
}

/// The project's files and stored-file metadata, as the build reads them.
/// The production answer is the broker's storage plane
/// (`crate::storage::BrokerStorage`); a test hands a map.
#[async_trait::async_trait]
pub trait ProjectStorage: Send + Sync {
    /// The bytes of one stored file, by its tenant-anchored key.
    async fn read(&self, key: &str) -> Result<Vec<u8>>;
    /// One stored file streamed into a new file at `dest`, chunk by chunk,
    /// never whole in memory. What fails midway may leave part of it there;
    /// the caller removes it.
    async fn download(&self, key: &str, dest: &std::path::Path) -> Result<()>;
    /// What the store records about one stored file.
    async fn meta(&self, key: &str) -> Result<weft_core::storage::StoredFileMeta>;
    /// Store `texts` (bytes by the path they were read from, which names
    /// the media type) among `tenant`'s assets, each by its content: the
    /// text a program's `@asset`s name, so a worker that reads the program
    /// finds it (`assets::Resolving`, and [`Kept`] for the text an older
    /// weft inlined).
    async fn store_texts(&self, tenant: &str, texts: &BTreeMap<String, Vec<u8>>) -> Result<()>;
}

/// Everything a build needs that belongs to the install rather than the
/// request: the platform's builder and image store (which also says how
/// the platform packages a worker), and the image ledger.
#[derive(Clone)]
pub struct VersionBuilder {
    pub images: Arc<dyn ImageBuilder>,
    pub pool: weft_task_store::db::Pool,
    /// The reclaims registered builds asked for (`prune::AfterBuildPrunes`).
    pub prunes: Arc<prune::AfterBuildPrunes>,
    /// The version files this replica fetched before (`blob_cache`).
    pub blobs: blob_cache::BlobCache,
}

impl VersionBuilder {
    /// Build the version `source` for `project_id`: fetch and check its
    /// files, compile, resolve its `@asset`s and check the run it is for
    /// (`prepare`), plan, and start a build of every stale image. `gate` is
    /// entered before the first real build; a version whose images all
    /// exist never touches it. `hold` claims every planned image against a
    /// prune until the version registers. `ask` is the request's place in
    /// the project's order of asks, taken as it arrived.
    #[allow(clippy::too_many_arguments)]
    pub async fn build(
        &self,
        storage: Arc<dyn ProjectStorage>,
        project_id: uuid::Uuid,
        tenant: &str,
        source: &weft_core::builds::VersionSource,
        prepare: &dyn Prepare,
        ask: i64,
        gate: Arc<dyn BuildGate>,
        hold: prune::ImageHold,
    ) -> Result<VersionBuild> {
        let workdir = tempfile::Builder::new()
            .prefix("weft-version-")
            .tempdir()
            .context("create the build's working directory")?;
        let root = workdir.path().to_path_buf();
        source::materialize(storage.as_ref(), &self.blobs, tenant, &source.manifest, &root).await?;
        let files = Arc::new(source::VersionFiles::new(root.clone(), source.manifest.clone(), storage.clone(), tenant.to_string()));

        let node_set = source.inputs.node_set;
        let packaging = self.images.worker_packaging();
        let images = self.images.clone();
        // The compile and the staging are blocking filesystem work.
        let (mut compiled, mut contents, catalog, project) = tokio::task::spawn_blocking(move || source::compile(&root, files))
            .await
            .context("the compile task panicked")??;
        let Assets { resolutions: assets, contents: asset_contents } = prepare.assets(storage.as_ref(), tenant, &compiled).await?;
        weft_compiler::file_ref::apply_asset_resolutions(&mut compiled, &assets)
            .map_err(|errs| anyhow!("unresolved assets:\n  {}", errs.join("\n  ")))?;
        contents.extend(asset_contents);
        prepare.check(&compiled).await?;
        let name = project.manifest.package.name.clone();
        // Every file was checked against the version as it was laid out, so
        // the images are named after the version's own fingerprints.
        let fingerprints = weft_compiler::hash::Fingerprints::of_version(project.root.clone(), source.manifest.clone());
        let (plan, definition) = tokio::task::spawn_blocking(move || {
            let plan = weft_compiler::build_plan::plan_build_from(
                &project,
                &compiled,
                &contents,
                &catalog,
                // `source::compile` read the project from disk.
                weft_compiler::CompileFs::disk(&project.root),
                &packaging,
                &ImageTags(images.as_ref()),
                node_set,
                &fingerprints,
            )
            .map_err(|e| anyhow!("plan the build: {e}"))?;
            Ok::<_, anyhow::Error>((plan, compiled))
        })
        .await
        .context("the planning task panicked")??;

        let images: Vec<String> =
            plan.images.iter().map(|image| image.image_ref.clone()).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        let infra_images = infra_places(&definition, &plan.images)?;
        let worker_image = plan
            .images
            .iter()
            .find(|image| image.kind == weft_compiler::build_plan::ImageKind::Worker)
            .map(|image| image.image_ref.clone())
            .context("the build plan has no worker image")?;
        let version = Version {
            name,
            manifest: source.manifest.clone(),
            inputs: Some(weft_core::builds::BuildInputs { node_set, assets }),
            program: weft_core::builds::BuiltProgram {
                version: weft_core::project::hash::manifest_version_id(&source.manifest),
                definition,
                binary_hash: plan.binary_hash,
                definition_hash: plan.definition_hash,
                infra_hash: plan.infra_hash,
                implementations: plan.implementations,
                infra_images,
                replaced_infra_images: Vec::new(),
                infra_places: plan.infra_places,
                worker_image,
                left_out: Vec::new(),
                worker_log: None,
                node_needs: plan.node_needs,
            },
            images,
            ask,
        };
        // The image contexts are staged under the working directory, and a
        // build started from one may read it after this request answered:
        // every start keeps a clone, and the directory goes with the last
        // (`Staging`), and with it the claim on the shared crates the
        // worker staged.
        let staging = Staging::new((workdir, plan.hold.clone()));
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
            let recorded = image.kind == weft_compiler::build_plan::ImageKind::Worker;
            let there = self.images.image_exists(&image.image_ref).await?
                && (!recorded || self.built_whole(&image.image_ref).await?);
            if !there {
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

    /// Whether the worker image `image_ref`, which the store holds, may run
    /// as it is. A worker is only there once a build of it succeeded on the
    /// ledger, with the record of what it left out when its build reports
    /// one: one a failed read left in the store, or one nothing here built,
    /// is built again, never run with its gaps unknown (registration reads
    /// that record, `ledger::left_out`). The one exception is the standard
    /// worker, which the install made ready as it was set up (`weft
    /// build-images` on a cloud, `weft daemon start` on a machine), each of
    /// which refuses one that left a package out: found with no build on
    /// the ledger, it is recorded as built (`ledger::note_shared`), so the
    /// first stock project after an update runs on it instead of building
    /// it again. A build of it the ledger has on record (one that failed,
    /// or one still running) is left as it stands.
    async fn built_whole(&self, image_ref: &str) -> Result<bool> {
        if ledger::succeeded(&self.pool, image_ref).await? {
            return Ok(true);
        }
        if image_ref != prune::standard_worker_ref(self.images.as_ref())? {
            return Ok(false);
        }
        ledger::note_shared(&self.pool, image_ref, crate::lease::now_unix()).await?;
        ledger::succeeded(&self.pool, image_ref).await
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
                Ok(build) => {
                    return Ok(VersionBuild::Waiting(weft_core::builds::BuildsUnderway {
                        version: version.program.version.clone(),
                        build,
                        images: underway,
                    }))
                }
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
        let claim = ledger::claim(&self.pool, self.images.as_ref(), &image.image_ref, project_id, tenant, now).await?;
        let stoppable = claim.stoppable();
        let name = match claim {
            ledger::Claim::Start { name } => {
                self.start(image, staging, project_id, tenant, &name).await?;
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

    /// Start the build `name` of `image`, whose row the ledger already
    /// holds, and record the builder's id for it. A start that fails is
    /// recorded as the build's end; a build made whose id cannot be
    /// recorded is stopped.
    async fn start(
        &self,
        image: &weft_compiler::build_plan::PlannedImage,
        staging: &Staging,
        project_id: uuid::Uuid,
        tenant: &str,
        name: &str,
    ) -> Result<()> {
        let request = BuildRequest {
            name: name.to_string(),
            project_id,
            tenant: tenant.to_string(),
            context_dir: image.context_dir.clone(),
            staging: staging.clone(),
            image_ref: image.image_ref.clone(),
            recipe: image.recipe.clone(),
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

/// [`Kept`]'s resolutions, each held to the store as it is now.
async fn kept_assets(
    storage: &dyn ProjectStorage,
    tenant: &str,
    definition: &weft_core::ProjectDefinition,
    sent: &weft_core::project::AssetResolutions,
) -> Result<Assets> {
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
    // A text resolution is the content's name (`weft_core::content`),
    // whose bytes the first build stored: read back and held to their
    // hash, so a content the store lost is refused here, naming the file,
    // rather than in every run. One a weft from before names kept holds
    // the text itself: read as a String it is the file's bytes, stored
    // now and named; read as another type it no longer says what the file
    // held, and the version is built again from its files, by its person.
    let string = weft_core::weft_type::WeftType::Primitive(weft_core::weft_type::WeftPrimitive::String);
    let mut contents = weft_core::content::ContentValues::new();
    let mut kept_text: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for r in weft_compiler::file_ref::collect_text_refs(definition) {
        let Some(value) = sent.get(&r.resolution_key()) else { continue };
        if let Some(content) = weft_core::content::ContentRef::from_value(value) {
            if contents.contains_key(&content.id()) {
                map.insert(r.resolution_key(), value.clone());
                continue;
            }
            let read = storage.read(&format!("{tenant}/{}", content.asset_key())).await.map_err(|e| format!("{e:#}"));
            match read.and_then(|bytes| content.value_of(&bytes)) {
                Ok(read) => {
                    contents.insert(content.id(), std::sync::Arc::new(read));
                    map.insert(r.resolution_key(), value.clone());
                }
                Err(why) => refused.push(format!(
                    "@asset({:?}, {}): the text this build kept cannot be read back ({why}); run `weft resync` in the \
                     project's folder to build it again from its files",
                    r.path, r.ty
                )),
            }
            continue;
        }
        match value {
            serde_json::Value::String(text) if r.ty == string => {
                let (content, read) = weft_core::content::ContentRef::of_text(text, &r.ty, &r.path).map_err(anyhow::Error::msg)?;
                kept_text.insert(r.path.clone(), text.as_bytes().to_vec());
                contents.insert(content.id(), std::sync::Arc::new(read));
                map.insert(r.resolution_key(), content.to_value());
            }
            _ => refused.push(format!(
                "@asset({:?}, {}): this build kept the text read as {}, as weft did before it stored a file's text, and \
                 that no longer says what the file held; run `weft resync` in the project's folder to build it again from its files",
                r.path, r.ty, r.ty
            )),
        }
    }
    if !refused.is_empty() {
        bail!("assets the build cannot use:\n  {}", refused.join("\n  "));
    }
    if !kept_text.is_empty() {
        storage.store_texts(tenant, &kept_text).await.context("store the text a build from an older weft kept")?;
    }
    Ok(Assets { resolutions: map, contents })
}

/// Every infra place's image refs, from the plan: the images belong to
/// the node type, so each place of one node carries the same refs.
fn infra_places(
    definition: &weft_core::ProjectDefinition,
    images: &[weft_compiler::build_plan::PlannedImage],
) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
    let places = weft_core::project::places::every_place(definition);
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
