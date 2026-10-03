//! Building a project version inside the install: the one way a project's
//! images come to exist, on a laptop and on a cloud alike.
//!
//! The CLI uploads the version's files (only the blobs the store lacks,
//! straight to the bucket) and asks for a build of that version. Here the
//! version's files are fetched back and checked against their hashes, the
//! project is compiled with the catalog this install ships, every image it
//! needs is planned with content-addressed refs in the platform's image
//! store, the stale ones are built by the platform's builder
//! (`weft_platform_traits::ImageBuilder`), and the caller registers the
//! result. Nothing the client
//! computed is believed: not a hash, not an image, not a stored file's
//! metadata.
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

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use weft_platform_traits::{BuildHandle, BuildRequest, ImageBuilder};

/// The control point the version builder (`crate::build`) calls around REAL build work, so the
/// dispatcher's `building` transition only engages when something actually
/// builds (a pure cache-hit verb never flips the marker, so concurrent runs on
/// an up-to-date project never serialize against each other).
///
/// The mechanism behind the gate is weft's (the project row's `transition`
/// marker + heartbeat + the stuck-transition reaper); the KNOWLEDGE of "a real
/// build is starting" is the builder's. This trait is the seam between them.
#[async_trait]
pub trait BuildGate: Send + Sync {
    /// Called once, just before the first actual image build is submitted.
    /// Errs when the project cannot enter the `building` transition right now
    /// (another verb is already building, or the lifecycle is mid-flip); the
    /// builder aborts with that error and the verb surfaces it.
    async fn begin(&self) -> anyhow::Result<()>;

    /// Whether the user requested cancellation of this build. Polled while
    /// the builds run; on `true` every build this project started is
    /// stopped (`ImageBuilder::release`, after the ledger records the
    /// cancel), a build another project started is left running, and the
    /// verb errs with a cancellation message.
    async fn cancel_requested(&self) -> anyhow::Result<bool>;
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

/// What a build produced: the answer the caller registers and hands the
/// client (its `replaced_infra_images` filled at registration, from
/// [`replaced_infra_images`]), and the images the version runs.
#[derive(Debug, Clone)]
pub struct Build {
    pub program: weft_core::builds::BuiltProgram,
    /// Every image ref this version runs, built now or found already
    /// there. Registration records them as the project's running version
    /// (`ledger::note_running`); the client has no use for it.
    pub images: Vec<String>,
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
    /// How often an in-flight build is looked at.
    pub poll_every: std::time::Duration,
    /// The reclaims registered builds asked for (`prune::AfterBuildPrunes`).
    pub prunes: Arc<prune::AfterBuildPrunes>,
    /// The version files this replica fetched before (`blob_cache`).
    pub blobs: blob_cache::BlobCache,
}

impl VersionBuilder {
    /// Build `request` for `project_id`: fetch and check its files, compile,
    /// plan, build every stale image. `gate` is entered before the first
    /// real build and polled for cancellation while one runs; a version
    /// whose images all exist never touches it. `hold` claims every
    /// planned image against a prune until the caller registers them.
    pub async fn build(
        &self,
        storage: &dyn ProjectStorage,
        project_id: uuid::Uuid,
        tenant: &str,
        request: &weft_core::builds::VersionBuildRequest,
        gate: &dyn BuildGate,
        hold: &prune::ImageHold,
    ) -> Result<Build> {
        let workdir = tempfile::Builder::new()
            .prefix("weft-version-")
            .tempdir()
            .context("create the build's working directory")?;
        let root = workdir.path().to_path_buf();
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

        let built_images = self.ensure_images(&plan.images, project_id, tenant, gate, hold).await?;
        let images: Vec<String> =
            plan.images.iter().map(|image| image.image_ref.clone()).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        let infra_images = infra_places(&definition, &plan.images)?;
        drop(workdir);
        Ok(Build {
            program: weft_core::builds::BuiltProgram {
                built_images,
                definition,
                binary_hash: plan.binary_hash,
                definition_hash: plan.definition_hash,
                infra_hash: plan.infra_hash,
                implementations: plan.implementations,
                infra_images,
                replaced_infra_images: Vec::new(),
            },
            images,
        })
    }

    /// Make every planned image exist in the registry, building the ones
    /// that do not side by side, and answer the refs it built. Every image
    /// is claimed in `hold` before the registry is asked about any, so a
    /// prune cannot take one this build found. The gate is entered once,
    /// before the first build, whether this process starts it or joins one
    /// already running.
    ///
    /// Each image is waited on by a task of its own. A build never depends
    /// on this verb to be seen through: its row is moved forward by
    /// whoever looks (`follow::advance`), this verb while it waits and the
    /// build loop otherwise, so a sibling image failing, or the request
    /// that asked being dropped, leaves nothing stuck. The verb waits for
    /// every image and reports every failure, the first one first.
    pub async fn ensure_images(
        &self,
        images: &[weft_compiler::build_plan::PlannedImage],
        project_id: uuid::Uuid,
        tenant: &str,
        gate: &dyn BuildGate,
        hold: &prune::ImageHold,
    ) -> Result<Vec<String>> {
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
            return Ok(Vec::new());
        }
        gate.begin().await?;
        let (cancel, cancelled) = tokio::sync::watch::channel(false);
        let waiters: Vec<(String, tokio::task::JoinHandle<Result<()>>)> = stale
            .into_iter()
            .map(|image| {
                let builder = self.clone();
                let (tenant, cancelled) = (tenant.to_string(), cancelled.clone());
                let image_ref = image.image_ref.clone();
                (image_ref, tokio::spawn(async move { builder.build_one(&image, project_id, &tenant, cancelled).await }))
            })
            .collect();
        let (refs, handles): (Vec<String>, Vec<_>) = waiters.into_iter().unzip();
        let mut all = std::pin::pin!(futures::future::join_all(handles));
        let joined = loop {
            tokio::select! {
                joined = &mut all => break joined,
                _ = tokio::time::sleep(self.poll_every) => {
                    if !*cancel.borrow() && gate.cancel_requested().await? {
                        cancel.send_replace(true);
                    }
                }
            }
        };
        let built = refs.clone();
        let mut failures = Vec::new();
        for (image_ref, joined) in refs.into_iter().zip(joined) {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(e)) => failures.push(e),
                Err(e) => failures.push(anyhow!("the wait on the build of {image_ref} panicked: {e}")),
            }
        }
        let mut failures = failures.into_iter();
        let Some(first) = failures.next() else { return Ok(built) };
        let others: Vec<String> = failures.map(|e| format!("{e:#}")).collect();
        if others.is_empty() {
            return Err(first);
        }
        bail!("{first:#}\n\n{} other image(s) failed too:\n{}", others.len(), others.join("\n\n"))
    }

    /// See one image to a pushed image: start its build, or join the one
    /// already running for the same ref (another project with the same
    /// content), then wait for its row to end, moving it forward while
    /// waiting.
    ///
    /// A cancel of this project stops a build it may stop (`stoppable`):
    /// the end is recorded, then the build is freed. A build it may not
    /// stop goes on; the verb only stops waiting on it.
    async fn build_one(
        &self,
        image: &weft_compiler::build_plan::PlannedImage,
        project_id: uuid::Uuid,
        tenant: &str,
        cancelled: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let now = crate::lease::now_unix();
        let lanes = (image.kind == weft_compiler::build_plan::ImageKind::Worker).then_some(self.compile_lanes);
        let claim = ledger::claim(&self.pool, self.images.as_ref(), &image.image_ref, project_id, tenant, lanes, now).await?;
        let stoppable = claim.stoppable();
        // The build this verb waits on: a cancel stops that one, never a
        // later build of the same ref the row may run by then.
        let name = match claim {
            ledger::Claim::Start { name, lane } => {
                self.start(image, project_id, tenant, &name, lane).await?;
                name
            }
            ledger::Claim::Join { name, .. } => name,
            ledger::Claim::Built => return Ok(()),
        };
        loop {
            if *cancelled.borrow() {
                if !stoppable {
                    bail!("the build of {} was cancelled", image.image_ref);
                }
                // Recorded before anything is freed, like any end. A build
                // not made on the builder yet is freed by its starter, which
                // finds the row no longer its own.
                if let Some(Some(builder_id)) = ledger::cancel(&self.pool, &image.image_ref, &name, crate::lease::now_unix()).await? {
                    self.images.release(&BuildHandle::named(builder_id)).await;
                }
            }
            follow::advance(&self.pool, self.images.as_ref(), &image.image_ref).await?;
            if let ledger::Seen::Ended(outcome) = ledger::look(&self.pool, &image.image_ref).await? {
                return ended(&image.image_ref, outcome);
            }
            tokio::time::sleep(self.poll_every).await;
        }
    }

    /// Start the build `name` of `image` (in `lane`, for a worker), whose
    /// row the ledger already holds, and record the builder's id for it. A
    /// start that fails is recorded as the build's end; a build made whose
    /// id cannot be recorded is stopped.
    async fn start(
        &self,
        image: &weft_compiler::build_plan::PlannedImage,
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

/// What the verb waiting on `image_ref` gets from how its build ended.
fn ended(image_ref: &str, outcome: ledger::Outcome) -> Result<()> {
    match outcome {
        ledger::Outcome::Succeeded => Ok(()),
        ledger::Outcome::Failed(reason) => bail!("the build of {image_ref} failed:\n{reason}"),
        ledger::Outcome::Cancelled => bail!("the build of {image_ref} was cancelled"),
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
