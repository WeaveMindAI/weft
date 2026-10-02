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
pub mod ledger;
pub mod prune;
pub mod source;

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use weft_platform_traits::{BuildHandle, BuildRequest, BuildStatus, ImageBuilder};

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
    /// This dispatcher replica: the owner of the builds it drives.
    pub replica: String,
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
    /// Each image is driven by a task of its own that always sees its
    /// build to an end (or, for a build another project started, stops
    /// waiting on it): a sibling image failing, or the request that asked
    /// being dropped, never leaves a build this process drives without a
    /// driver. The verb waits for every driver and reports every failure,
    /// the first one first.
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
        let drivers: Vec<(String, tokio::task::JoinHandle<Result<()>>)> = stale
            .into_iter()
            .map(|image| {
                let driver = self.clone();
                let (tenant, cancelled) = (tenant.to_string(), cancelled.clone());
                let image_ref = image.image_ref.clone();
                (image_ref, tokio::spawn(async move { driver.drive(&image, project_id, &tenant, cancelled).await }))
            })
            .collect();
        let (refs, handles): (Vec<String>, Vec<_>) = drivers.into_iter().unzip();
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
                Err(e) => failures.push(anyhow!("the driver of the build of {image_ref} panicked: {e}")),
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

    /// Drive one image to a pushed image: start its build, join the one
    /// already running for the same ref (another project with the same
    /// content), or take over one whose driver died, then follow it to its
    /// end.
    ///
    /// A cancel of this project stops a build it may stop (`stoppable`).
    /// A build it may not stop but drives (it took it over, or started it
    /// for another project first) is never left without a driver: the
    /// verb stops waiting, and a detached task keeps driving it to its
    /// end. A build it neither may stop nor drives is left to its driver.
    async fn drive(
        &self,
        image: &weft_compiler::build_plan::PlannedImage,
        project_id: uuid::Uuid,
        tenant: &str,
        cancelled: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let now = crate::lease::now_unix();
        let claim = ledger::claim(&self.pool, &image.image_ref, project_id, tenant, &self.replica, self.compile_lanes, now)
            .await?;
        let stoppable = claim.stoppable();
        let mut follow = Follow { name: String::new(), driving: true, started: Vec::new() };
        match claim {
            ledger::Claim::Start { name, lane } => self.start(image, project_id, tenant, &name, lane, &mut follow).await?,
            ledger::Claim::Join { name, driving, .. } => {
                follow.name = name;
                follow.driving = driving;
            }
            ledger::Claim::Adopt { gone, name, lane, .. } => {
                self.took_over(image, &gone, &name).await;
                self.start(image, project_id, tenant, &name, lane, &mut follow).await?;
            }
            ledger::Claim::Built => return Ok(()),
        }
        match self.follow(image, project_id, tenant, &mut follow, stoppable, Some(&cancelled)).await {
            Ok(Followed::Ended(outcome)) => ended(&image.image_ref, outcome),
            Ok(Followed::StoppedWaiting) => {
                if follow.driving {
                    let (driver, image, tenant) = (self.clone(), image.clone(), tenant.to_string());
                    tokio::spawn(async move {
                        if let Err(e) = driver.follow(&image, project_id, &tenant, &mut follow, false, None).await {
                            tracing::warn!(
                                target: "weft_dispatcher::build",
                                image = %image.image_ref, error = %format!("{e:#}"),
                                "driving a build this project stopped waiting on failed"
                            );
                        }
                    });
                }
                bail!("the build of {} was cancelled", image.image_ref)
            }
            Err(e) => Err(e),
        }
    }

    /// Start the build `name` of `image` in `lane`, whose row the ledger
    /// already holds, and follow it from now on. A start that fails is
    /// recorded as the build's end.
    async fn start(
        &self,
        image: &weft_compiler::build_plan::PlannedImage,
        project_id: uuid::Uuid,
        tenant: &str,
        name: &str,
        lane: u32,
        follow: &mut Follow,
    ) -> Result<()> {
        let mut build_args = Vec::new();
        if image.kind == weft_compiler::build_plan::ImageKind::Worker {
            build_args.push((weft_compiler::worker_image::COMPILE_LANE_ARG.to_string(), lane.to_string()));
        }
        let request = BuildRequest {
            name: name.to_string(),
            project_id,
            tenant: tenant.to_string(),
            context_dir: image.context_dir.clone(),
            image_ref: image.image_ref.clone(),
            build_args,
        };
        match self.images.start(request).await {
            Ok(handle) => {
                follow.started.push(handle.clone());
                follow.name = handle.external_build_id;
                follow.driving = true;
                Ok(())
            }
            Err(e) => {
                let e = e.context(format!("start the build of {}", image.image_ref));
                let recorded = ledger::finish(
                    &self.pool,
                    &image.image_ref,
                    name,
                    ledger::Outcome::Failed(format!("{e:#}")),
                    crate::lease::now_unix(),
                )
                .await;
                Err(with_secondary(e, recorded))
            }
        }
    }

    /// Follow the row of `image` until its build ends, or, with a cancel
    /// channel, until this verb stops waiting on a build it may not stop.
    /// Whoever drives records the end, and every end frees what this task
    /// started (`ImageBuilder::release`).
    async fn follow(
        &self,
        image: &weft_compiler::build_plan::PlannedImage,
        project_id: uuid::Uuid,
        tenant: &str,
        follow: &mut Follow,
        stoppable: bool,
        cancelled: Option<&tokio::sync::watch::Receiver<bool>>,
    ) -> Result<Followed> {
        let result = self.follow_to_end(image, project_id, tenant, follow, stoppable, cancelled).await;
        let result = match result {
            Err(e) if follow.driving => {
                // A driver never walks away from a build it drives: an
                // error is recorded as its end, so nobody waits forever.
                let recorded = ledger::finish(
                    &self.pool,
                    &image.image_ref,
                    &follow.name,
                    ledger::Outcome::Failed(format!("the dispatcher driving it failed: {e:#}")),
                    crate::lease::now_unix(),
                )
                .await;
                Err(with_secondary(e, recorded))
            }
            other => other,
        };
        // Everything this task started is freed once the build ended. When
        // the verb only stopped waiting, the build it still drives goes on
        // (the detached task in `drive` frees it at its end); anything else
        // it started, taken over by another process since, is freed now.
        let keep = match &result {
            Ok(Followed::StoppedWaiting) if follow.driving => Some(follow.name.clone()),
            _ => None,
        };
        let (kept, freed): (Vec<_>, Vec<_>) =
            follow.started.drain(..).partition(|h| keep.as_deref() == Some(h.external_build_id.as_str()));
        follow.started = kept;
        for handle in freed {
            self.images.release(&handle).await;
        }
        result
    }

    /// This process took over `gone`, whose driver stopped renewing its hold,
    /// as `name`: the process `gone` ran in is nobody's any more, so it goes.
    async fn took_over(&self, image: &weft_compiler::build_plan::PlannedImage, gone: &str, name: &str) {
        tracing::info!(
            target: "weft_dispatcher::build",
            image = %image.image_ref, gone = %gone, build = %name,
            "took over a build whose driving dispatcher is gone; building it again"
        );
        self.images.release(&BuildHandle { external_build_id: gone.to_string() }).await;
    }

    async fn follow_to_end(
        &self,
        image: &weft_compiler::build_plan::PlannedImage,
        project_id: uuid::Uuid,
        tenant: &str,
        follow: &mut Follow,
        stoppable: bool,
        cancelled: Option<&tokio::sync::watch::Receiver<bool>>,
    ) -> Result<Followed> {
        loop {
            if cancelled.is_some_and(|c| *c.borrow()) {
                if !stoppable {
                    return Ok(Followed::StoppedWaiting);
                }
                // Recorded before anything is freed, like any end. The
                // build's driver, here or on another process, reads it next.
                ledger::finish(&self.pool, &image.image_ref, &follow.name, ledger::Outcome::Cancelled, crate::lease::now_unix())
                    .await?;
            }
            // The ledger first: a build ends once, whoever waits on it,
            // and whoever saw it end recorded that before freeing its process.
            let now = crate::lease::now_unix();
            match ledger::look(&self.pool, &image.image_ref, &follow.name).await? {
                ledger::Seen::Ended(outcome) => return Ok(Followed::Ended(outcome)),
                ledger::Seen::Running { name, driver, driver_until } => {
                    if name != follow.name {
                        // Taken over under another name (the ledger doc).
                        follow.name = name;
                    }
                    follow.driving = driver == self.replica;
                    if !follow.driving && driver_until < now {
                        if let Some((name, lane)) = ledger::take_over(
                            &self.pool,
                            &image.image_ref,
                            &follow.name,
                            &self.replica,
                            self.compile_lanes,
                            now,
                        )
                        .await?
                        {
                            let gone = follow.name.clone();
                            self.took_over(image, &gone, &name).await;
                            self.start(image, project_id, tenant, &name, lane, follow).await?;
                        }
                        continue;
                    }
                }
            }
            let handle = BuildHandle { external_build_id: follow.name.clone() };
            let outcome = match self.images.poll(&handle).await? {
                BuildStatus::Pending => {
                    if follow.driving {
                        ledger::renew(&self.pool, &image.image_ref, &follow.name, &self.replica, now).await?;
                    }
                    tokio::time::sleep(self.poll_every).await;
                    continue;
                }
                // Only the task that started the process may call it gone: any
                // other waiter (on another process, or on this one, joining a
                // build whose process is still being created) waits for that
                // task to record the end, or takes the build over once its
                // driver's hold lapses.
                BuildStatus::Gone if !follow.started.iter().any(|h| h.external_build_id == follow.name) => {
                    tokio::time::sleep(self.poll_every).await;
                    continue;
                }
                BuildStatus::Gone => ledger::Outcome::Failed(format!(
                    "the build {} is gone (deleted before it finished)",
                    follow.name
                )),
                BuildStatus::Succeeded => ledger::Outcome::Succeeded,
                BuildStatus::Failed { reason } => ledger::Outcome::Failed(reason),
            };
            // Whoever sees the end records it; the next look reads the
            // record back, so every waiter reports the same end.
            ledger::finish(&self.pool, &image.image_ref, &follow.name, outcome, crate::lease::now_unix()).await?;
        }
    }
}

/// The build a driver task follows: the row's current build, whether this
/// process drives it, and every build this task started (freed at the end).
struct Follow {
    name: String,
    driving: bool,
    started: Vec<BuildHandle>,
}

/// How following one build stopped.
enum Followed {
    Ended(ledger::Outcome),
    /// A build this project may not stop: this verb stopped waiting on it.
    StoppedWaiting,
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
