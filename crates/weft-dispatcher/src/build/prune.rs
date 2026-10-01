//! Deleting the images nothing uses any more from the platform's image
//! store (`weft clean --images`). Every build adds one image per content, and
//! nothing else ever removes one.
//!
//! Only images this install built (its `image_build` ledger) are touched,
//! and only those outside the referenced set: what a project's running
//! build names, what a live worker or a queued task was stamped
//! with, and every infra image a project's build or a running unit
//! names. An image a project needs again later (a rollback to an older
//! version) is simply built again. Whatever the platform keeps running
//! from a deleted image (a stopped container, a service revision) goes
//! with it (`Runner::forget_image`).
//!
//! A build finds (or builds) its images well before it registers them as
//! the project's running version, and until then nothing in the
//! referenced set names them. So each build claims every image it plans
//! before it looks for any ([`ImageHold`], rows in `image_claim`), and a
//! prune spares every claimed image. The check and the delete happen
//! under that image's lock (`ledger::begin_reclaim`), the same lock a
//! claim takes, so a claim lands either before the check (the image is
//! spared) or after the delete (the build finds it gone and builds it
//! again). The same check re-reads whether anything references that one
//! image now: a build registers, then lets go of its claim, possibly after
//! the prune read its keep-set, and the keep-set would not know.
//! Protection is per image: a prune never waits for builds to go quiet,
//! which on a busy install they never would.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{Context, Result};

use weft_compiler::build::WORKER_IMAGE_REPO;
use weft_core::images::PruneReport;

/// One build request's claim on the images it relies on, from its first
/// look at the registry until its registration commits. Renewed in the
/// background while the request lives, like a build driver's hold
/// (`ledger::DRIVER_LEASE_SECS`): a dispatcher that dies stops renewing
/// and its claim stops protecting anything, with no connection held open
/// and nothing to wait out.
pub struct ImageHold {
    pool: sqlx::PgPool,
    id: uuid::Uuid,
    /// The images the claim covers, to check none lapsed.
    held: std::sync::Mutex<BTreeSet<String>>,
    renewer: tokio::task::JoinHandle<()>,
}

impl ImageHold {
    pub fn new(pool: &sqlx::PgPool) -> Self {
        let id = uuid::Uuid::new_v4();
        let renew_pool = pool.clone();
        let renewer = tokio::spawn(async move {
            let every = std::time::Duration::from_secs((super::ledger::DRIVER_LEASE_SECS / 3) as u64);
            loop {
                tokio::time::sleep(every).await;
                if let Err(e) = super::ledger::renew_claim(&renew_pool, id, crate::lease::now_unix()).await {
                    tracing::warn!(target: "weft_dispatcher::build", error = %format!("{e:#}"), "renewing a build's claim on its images failed; trying again");
                }
            }
        });
        Self { pool: pool.clone(), id, held: Default::default(), renewer }
    }

    /// Claim `images` before the build looks for any of them.
    pub async fn claim(&self, images: &[String]) -> Result<()> {
        super::ledger::claim_images(&self.pool, self.id, images, crate::lease::now_unix()).await?;
        self.held.lock().expect("the claim's image set is never poisoned").extend(images.iter().cloned());
        Ok(())
    }

    /// Check the claim still covers every image it took, and extend it:
    /// called right before registering them. A claim that lapsed (this
    /// process could not renew it in time) may have let a prune take an
    /// image, so the build fails rather than register what may be gone.
    pub async fn confirm(&self) -> Result<()> {
        let live = super::ledger::renew_claim(&self.pool, self.id, crate::lease::now_unix()).await?;
        let held = self.held.lock().expect("the claim's image set is never poisoned").len() as u64;
        anyhow::ensure!(
            live == held,
            "this build's claim on its images lapsed ({live} of {held} still held), so an image cleanup may have \
             removed one; build again"
        );
        Ok(())
    }

    /// Let go once the images are registered (or the build ended).
    pub async fn release(self) -> Result<()> {
        self.renewer.abort();
        super::ledger::drop_claim(&self.pool, self.id).await
    }
}

impl Drop for ImageHold {
    /// A request abandoned midway: the claim stops being renewed, and
    /// lapses on its own.
    fn drop(&mut self) {
        self.renewer.abort();
    }
}

/// The images of `built` that nothing in `keep` references: a worker
/// image whose content hash is not a kept binary hash, an infra image
/// whose ref is not kept. Anything else this install built is left.
pub fn prunable(built: &[String], keep: &KeepSet) -> Vec<String> {
    built
        .iter()
        .filter(|image_ref| ImageScope::of(image_ref).is_some_and(|scope| !keep.references(scope)))
        .cloned()
        .collect()
}

/// Which images a referenced-images read covers: every one (the keep-set a
/// prune starts from), or the one image a prune is about to delete, re-read
/// under that image's lock (`ledger::begin_reclaim`). Both go through the
/// same query (`crate::api::project::referenced_images_query`), so "what is
/// referenced" has one definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageScope<'a> {
    All,
    /// One worker image, by its binary hash (its tag).
    Worker(&'a str),
    /// One infra image, by its full ref.
    Infra(&'a str),
}

impl<'a> ImageScope<'a> {
    /// The scope of one image this install may reclaim; `None` for any
    /// other image (a base image, a ref with no tag), which no prune takes.
    pub fn of(image_ref: &'a str) -> Option<Self> {
        let (repo, tag) = repository_and_tag(image_ref)?;
        if repo == WORKER_IMAGE_REPO {
            Some(Self::Worker(tag))
        } else if repo.starts_with("weft-infra-") {
            Some(Self::Infra(image_ref))
        } else {
            None
        }
    }

    /// The binary hash the worker arms are restricted to (`None`: all).
    pub fn worker_hash(self) -> Option<&'a str> {
        match self {
            Self::Worker(hash) => Some(hash),
            _ => None,
        }
    }

    pub fn covers_workers(self) -> bool {
        !matches!(self, Self::Infra(_))
    }

    pub fn covers_infra(self) -> bool {
        !matches!(self, Self::Worker(_))
    }

    /// Whether an infra ref read from the database falls in this scope.
    pub fn covers_infra_ref(self, image_ref: &str) -> bool {
        match self {
            Self::All => true,
            Self::Infra(only) => only == image_ref,
            Self::Worker(_) => false,
        }
    }
}

/// The last path segment of an image ref, split into its repository name
/// and tag (`us-docker.pkg.dev/p/r/weft-worker:ab` gives `weft-worker`,
/// `ab`; a local `weft-worker:ab` the same). `None` for a ref with no
/// tag, which no image this install built has.
fn repository_and_tag(image_ref: &str) -> Option<(&str, &str)> {
    let last = image_ref.rsplit('/').next()?;
    let (repo, tag) = last.split_once(':')?;
    (!repo.is_empty() && !tag.is_empty()).then_some((repo, tag))
}

/// The images nothing may reclaim: everything something may still run
/// (`crate::api::project::referenced_images_query`), and the standard
/// worker, which a project using only base catalog nodes builds to and
/// whose first run it makes instant.
#[derive(Debug, Default)]
pub struct KeepSet {
    pub worker_hashes: BTreeSet<String>,
    pub infra_refs: BTreeSet<String>,
}

impl KeepSet {
    /// Whether this set keeps the one image `scope` names (`All` asks
    /// nothing and answers false).
    pub fn references(&self, scope: ImageScope<'_>) -> bool {
        match scope {
            ImageScope::All => false,
            ImageScope::Worker(hash) => self.worker_hashes.contains(hash),
            ImageScope::Infra(image_ref) => self.infra_refs.contains(image_ref),
        }
    }
}

/// The keep-set restricted to `scope`, read on `conn`: the whole of it
/// before a prune picks candidates, and one image's part again under that
/// image's lock right before deleting it (`ledger::begin_reclaim`), since a
/// build may have registered the image in between.
pub async fn keep_set(conn: &mut sqlx::PgConnection, scope: ImageScope<'_>) -> Result<KeepSet> {
    let referenced = crate::api::project::referenced_images_query(conn, scope).await?;
    let mut worker_hashes: BTreeSet<String> = referenced.worker_hashes.into_iter().collect();
    if scope.covers_workers() {
        let standard = weft_compiler::build::standard_worker_hash()
            .map_err(|e| anyhow::anyhow!("the standard worker's hash: {e}"))?;
        if scope.worker_hash().is_none_or(|only| only == standard) {
            worker_hashes.insert(standard);
        }
    }
    Ok(KeepSet { worker_hashes, infra_refs: referenced.infra_refs.into_iter().collect() })
}

/// How many of a project's earlier builds stay beside the current one,
/// so going back one edit does not recompile.
pub const PREVIOUS_BUILDS_KEPT: usize = 1;

/// The images one of each project's newest `1 + PREVIOUS_BUILDS_KEPT`
/// builds names, of every project but `except`: what no automatic or
/// project-scoped prune may take, since that project could go back to it.
///
/// Counted per build, never per repository: an infra repository is named
/// after its image's directory, which two node types can share, so one
/// build may hold several images of one repository, and counting those
/// would push the previous build's images out.
fn recent_refs(uses: &[super::ledger::ImageUse], except: Option<uuid::Uuid>) -> BTreeSet<&str> {
    use std::collections::HashMap;
    let mut builds: HashMap<uuid::Uuid, BTreeSet<i64>> = HashMap::new();
    for u in uses {
        builds.entry(u.project_id).or_default().insert(u.running_since);
    }
    let newest: HashMap<uuid::Uuid, BTreeSet<i64>> = builds
        .into_iter()
        .map(|(p, stamps)| (p, stamps.into_iter().rev().take(1 + PREVIOUS_BUILDS_KEPT).collect()))
        .collect();
    uses.iter()
        .filter(|u| Some(u.project_id) != except && newest[&u.project_id].contains(&u.running_since))
        .map(|u| u.image_ref.as_str())
        .collect()
}

/// Of every project's image uses (`ledger::image_uses`), the images of
/// `project` a build may reclaim once the keep-set spares what still runs:
/// those not stamped by one of the project's newest `1 +
/// PREVIOUS_BUILDS_KEPT` builds, nor by one of any other project's newest
/// builds (the same content in use there).
pub fn older_builds(uses: &[super::ledger::ImageUse], project: uuid::Uuid) -> Vec<String> {
    let recent = recent_refs(uses, None);
    let older: BTreeSet<&str> = uses
        .iter()
        .filter(|u| u.project_id == project && !recent.contains(u.image_ref.as_str()))
        .map(|u| u.image_ref.as_str())
        .collect();
    older.into_iter().map(str::to_string).collect()
}

/// Of `project`'s images (`candidates`), those no other project counts
/// among its recent builds: what a project-scoped `weft clean --images`
/// may take, since another project could go back to the rest.
pub fn not_recent_elsewhere(uses: &[super::ledger::ImageUse], candidates: Vec<String>, project: uuid::Uuid) -> Vec<String> {
    let recent = recent_refs(uses, Some(project));
    candidates.into_iter().filter(|r| !recent.contains(r.as_str())).collect()
}

/// The prunes builds and removals asked for (`request`), run one at a
/// time per process.
///
/// Every prune works against the same install-wide keep-set, so a burst of
/// requests needs one prune after it, not one each: a request while one is
/// pending only adds to the next pass. This lives in RAM because it only
/// spares work; the per-image claims in Postgres are what keep a prune
/// safe, and a sibling instance's own pending prune changes nothing but
/// how soon the images go.
#[derive(Default)]
pub struct AfterBuildPrunes {
    pending: std::sync::Mutex<Pending>,
}

#[derive(Default)]
struct Pending {
    /// A task is draining the queue; a new request only joins it.
    draining: bool,
    /// A pass was asked for since the last one began.
    wanted: bool,
    /// Projects that registered a build: their older builds' images.
    projects: BTreeSet<uuid::Uuid>,
}

impl AfterBuildPrunes {
    /// Reclaim what nothing uses any more (`reclaim`), and `project`'s
    /// older images when a build of it just registered. A removed project
    /// names none: forgetting its uses already made its images unused.
    pub fn request(self: &Arc<Self>, state: &crate::state::DispatcherState, project: Option<uuid::Uuid>) {
        {
            let mut pending = self.pending.lock().expect("the prune queue's lock is never poisoned");
            pending.wanted = true;
            pending.projects.extend(project);
            if pending.draining {
                return;
            }
            pending.draining = true;
        }
        let (queue, state) = (self.clone(), state.clone());
        tokio::spawn(async move { queue.drain(&state).await });
    }

    async fn drain(self: Arc<Self>, state: &crate::state::DispatcherState) {
        // A panic ending this task must not leave the queue marked as
        // drained by a task that is gone: the next request starts another.
        struct Draining<'a>(&'a AfterBuildPrunes);
        impl Drop for Draining<'_> {
            fn drop(&mut self) {
                if !std::thread::panicking() {
                    return;
                }
                if let Ok(mut pending) = self.0.pending.lock() {
                    pending.draining = false;
                }
            }
        }
        let _draining = Draining(&self);
        loop {
            let projects = {
                let mut pending = self.pending.lock().expect("the prune queue's lock is never poisoned");
                if !pending.wanted {
                    pending.draining = false;
                    return;
                }
                pending.wanted = false;
                std::mem::take(&mut pending.projects)
            };
            match reclaim(state, &projects).await {
                Ok(report) if !report.removed.is_empty() || !report.failed.is_empty() => {
                    tracing::info!(target: "weft_dispatcher::build", projects = ?projects, removed = ?report.removed, failed = ?report.failed, "reclaimed older images");
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(target: "weft_dispatcher::build", projects = ?projects, error = %format!("{e:#}"), "could not reclaim older images; `weft clean --images` does it by hand");
                }
            }
        }
    }
}

/// Reclaim each of `projects`' images older than its newest `1 +
/// PREVIOUS_BUILDS_KEPT` builds, and every image no project uses at all
/// (`ledger::unused_images`: a removed project's, a build's that never
/// registered, a standard worker a newer weft replaced), sparing whatever
/// still runs or a build in progress claims. Every pass takes the unused
/// ones, so one a container still ran from last time goes on a later one.
async fn reclaim(state: &crate::state::DispatcherState, projects: &BTreeSet<uuid::Uuid>) -> Result<PruneReport> {
    let keep = keep_set(&mut *state.pg_pool.acquire().await.context("a connection to read the keep-set")?, ImageScope::All).await?;
    // The standard worker a newer weft replaced goes too: the current one
    // is recorded (and kept by the keep-set), earlier ones are not kept.
    let standard = state.builder.images.image_ref(&weft_compiler::build::worker_image_tag(
        &weft_compiler::build::standard_worker_hash().map_err(|e| anyhow::anyhow!("the standard worker's hash: {e}"))?,
    ));
    if state.builder.images.image_exists(&standard).await? {
        super::ledger::note_shared(&state.pg_pool, &standard, crate::lease::now_unix()).await?;
    }
    let uses = super::ledger::image_uses(&state.pg_pool).await?;
    let mut candidates: Vec<String> = projects.iter().flat_map(|project| older_builds(&uses, *project)).collect();
    candidates.extend(super::ledger::unused_images(&state.pg_pool).await?);
    prune(
        &state.pg_pool,
        state.builder.images.as_ref(),
        state.runner.as_ref(),
        &candidates,
        &keep,
    )
    .await
}

/// Delete every prunable image of `built` from the image store and the
/// ledger, and whatever the platform still keeps from it, sparing any a
/// build in progress claims (reported `in_use`). One failed delete does
/// not stop the others.
///
/// `keep` only narrows the candidates: it was read before the loop, and a
/// build may register one of them as running since. The deciding check
/// is `ledger::begin_reclaim`'s, under the image's lock.
pub async fn prune(
    pool: &sqlx::PgPool,
    images: &dyn weft_platform_traits::ImageBuilder,
    runner: &dyn weft_platform_traits::Runner,
    built: &[String],
    keep: &KeepSet,
) -> Result<PruneReport> {
    let mut report = PruneReport::default();
    super::ledger::drop_lapsed_claims(pool, crate::lease::now_unix()).await?;
    let candidates: BTreeSet<String> = prunable(built, keep).into_iter().collect();
    for image_ref in candidates {
        // Held until the image is deleted and forgotten: a build claiming
        // it waits here, then finds it gone (the module doc).
        let mut tx = match super::ledger::begin_reclaim(pool, &image_ref, crate::lease::now_unix()).await? {
            super::ledger::Reclaim::Free(tx) => tx,
            super::ledger::Reclaim::Claimed => {
                report.in_use.push(image_ref);
                continue;
            }
            // Registered since `keep` was read: it runs now, so it stays,
            // exactly as if the keep-set had named it.
            super::ledger::Reclaim::Referenced => continue,
        };
        let deleted = match runner.forget_image(&image_ref).await {
            Ok(()) => images.delete_image(&image_ref).await,
            Err(e) => Err(e),
        };
        match deleted {
            Ok(weft_platform_traits::ImageDeleted::Deleted) => {
                let forgotten = match super::ledger::forget(&mut tx, &image_ref).await {
                    Ok(()) => tx.commit().await.context("commit forgetting a deleted image"),
                    Err(e) => Err(e),
                };
                match forgotten {
                    Ok(()) => report.removed.push(image_ref),
                    // Gone from the store but still in the ledger: the
                    // next prune finds it again and forgets it then.
                    Err(e) => report.failed.push((image_ref, format!("deleted, but forgetting it failed: {e:#}"))),
                }
            }
            Ok(weft_platform_traits::ImageDeleted::InUse) => report.in_use.push(image_ref),
            Err(e) => report.failed.push((image_ref, format!("{e:#}"))),
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_unreferenced_worker_and_infra_images_are_prunable() {
        let built: Vec<String> = [
            "reg:5000/weft-worker:live",
            "reg:5000/weft-worker:old",
            "us-docker.pkg.dev/x/r/weft-infra-bridge:aa",
            "us-docker.pkg.dev/x/r/weft-infra-bridge:bb",
            "reg:5000/weft-builder-base:cc",
            "weft-worker:gone",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        let keep = KeepSet {
            worker_hashes: BTreeSet::from(["live".to_string()]),
            infra_refs: BTreeSet::from(["us-docker.pkg.dev/x/r/weft-infra-bridge:aa".to_string()]),
        };
        assert_eq!(
            prunable(&built, &keep),
            vec![
                "reg:5000/weft-worker:old".to_string(),
                "us-docker.pkg.dev/x/r/weft-infra-bridge:bb".to_string(),
                "weft-worker:gone".to_string(),
            ]
        );
        assert_eq!(repository_and_tag("weft-worker"), None, "no tag");
    }

    #[test]
    fn an_image_scope_names_the_one_image_a_prune_rechecks() {
        assert_eq!(ImageScope::of("reg:5000/weft-worker:ab"), Some(ImageScope::Worker("ab")));
        let infra = "us-docker.pkg.dev/x/r/weft-infra-bridge:aa";
        assert_eq!(ImageScope::of(infra), Some(ImageScope::Infra(infra)));
        assert_eq!(ImageScope::of("reg:5000/weft-builder-base:cc"), None);
        assert!(ImageScope::Infra(infra).covers_infra_ref(infra));
        assert!(!ImageScope::Infra(infra).covers_infra_ref("us-docker.pkg.dev/x/r/weft-infra-bridge:bb"));
        assert!(!ImageScope::Worker("ab").covers_infra() && !ImageScope::Infra(infra).covers_workers());
    }

    fn uses(rows: &[(uuid::Uuid, &str, i64)]) -> Vec<super::super::ledger::ImageUse> {
        rows.iter()
            .map(|(p, r, t)| super::super::ledger::ImageUse { project_id: *p, image_ref: r.to_string(), running_since: *t })
            .collect()
    }

    /// A project keeps every image of its current build and of the one
    /// before; older ones become candidates unless another project still
    /// has the same content among its own newest builds.
    #[test]
    fn a_project_keeps_its_current_and_previous_build() {
        let (a, b) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        let uses = uses(&[
            (a, "weft-worker:c", 30),
            (a, "weft-infra-db:z", 30),
            (a, "weft-worker:b", 20),
            (a, "weft-infra-db:y", 20),
            (a, "weft-worker:a", 10),
            (a, "weft-infra-db:x", 10),
            (a, "weft-worker:shared", 5),
            (b, "weft-worker:shared", 7),
        ]);
        assert_eq!(older_builds(&uses, a), ["weft-infra-db:x", "weft-worker:a"]);
        assert!(older_builds(&uses, b).is_empty());
    }

    /// A project-scoped clean takes the project's images no other project
    /// is on or could go back to, and nothing another project holds as its
    /// current or previous build, however old the project's own use is.
    #[test]
    fn a_scoped_clean_spares_what_another_project_built_to_lately() {
        let (a, b) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        let uses = uses(&[
            (a, "weft-worker:mine", 30),
            (a, "weft-worker:shared", 10),
            (b, "weft-worker:shared", 25),
            (b, "weft-worker:b-old", 5),
            (b, "weft-worker:b-newest", 40),
        ]);
        let mine = vec!["weft-worker:mine".to_string(), "weft-worker:shared".to_string(), "weft-worker:b-old".to_string()];
        assert_eq!(
            not_recent_elsewhere(&uses, mine, a),
            ["weft-worker:mine", "weft-worker:b-old"],
            "b's current and previous builds stay; its older one and a's own go"
        );
    }

    /// Two node types whose images live in same-named directories build
    /// into one repository; the current build holding two of them must
    /// not push the previous build's image out.
    #[test]
    fn two_images_of_one_repository_in_one_build_count_as_one_build() {
        let a = uuid::Uuid::from_u128(1);
        let uses = uses(&[
            (a, "weft-infra-server:new1", 20),
            (a, "weft-infra-server:new2", 20),
            (a, "weft-infra-server:old", 10),
            (a, "weft-infra-server:oldest", 5),
        ]);
        assert_eq!(older_builds(&uses, a), ["weft-infra-server:oldest"]);
    }
}
