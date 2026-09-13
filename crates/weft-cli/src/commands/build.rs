//! `weft build`: compile the current project into a worker container
//! image and register it, without starting anything. The image is
//! tagged `weft-worker:<binary-hash>`; the project id rides the
//! `weft.dev/project` label, so identical sources across projects share
//! one image. The build itself runs in a multi-stage `docker build` so
//! the host needs only docker + kind + kubectl.

use anyhow::Result;
use tokio::process::Command;

use super::Ctx;
use crate::commands::daemon::{cluster_config, ClusterBackend};
use crate::images;
use crate::progress::{ActionVerb, Progress};

pub async fn run(ctx: Ctx, node_set: weft_compiler::codegen::NodeSet) -> Result<()> {
    let inner = ctx.clone();
    ctx.with_progress(ActionVerb::Build, |progress| async move {
        // Build IS "make the dispatcher's picture of this project match
        // my disk": the image, and the compiled program registered under
        // its own hash, with nothing started. Same path every other verb
        // takes to get there, so a `weft run` straight after is a no-op
        // rather than a second opinion on what the project is.
        //
        // It is also how a person puts back code the dispatcher lost. A
        // run names the hash of the program it ran, and its inputs and
        // outputs are worked out by folding the journal against that
        // program, so registering unchanged files restores exactly what
        // the run needs and it reads again. It records nothing in the
        // VERSION TREE: that is `weft checkpoint`, and a version is a
        // point in a folder's history rather than something a build has
        // an opinion about.
        let handle = super::ensure::ensure_registered(&inner, &progress, node_set).await?;
        progress.complete(&format!(
            "{} ({}) on worker image {}",
            handle.name,
            handle.id,
            short_hash(&handle.plan.binary_hash)
        ));
        Ok(())
    })
    .await
}

/// The worker `PlannedImage` from a `BuildPlan` (there is always exactly one worker
/// image). A helper so callers don't re-scan `plan.images`.
pub fn worker_planned_image(
    plan: &weft_compiler::build_plan::BuildPlan,
) -> Result<&weft_compiler::build_plan::PlannedImage> {
    plan.images
        .iter()
        .find(|i| i.kind == weft_compiler::build_plan::ImageKind::Worker)
        .ok_or_else(|| anyhow::anyhow!("build plan has no worker image"))
}

/// `weft build-base`: ensure the shared worker builder-base image exists (build it
/// if stale) and print its content-addressed tag. Wraps the same
/// `ensure_worker_builder_base` a per-project `weft build` runs, exposed as a verb
/// so a cluster setup can build + load the base into its in-cluster registry. With
/// `quiet`, prints ONLY the tag on stdout (the rest goes to stderr) so a script can
/// capture it; otherwise prints a human line.
pub async fn run_build_base(quiet: bool) -> Result<()> {
    let tag = crate::images::ensure_worker_builder_base().await?;
    if quiet {
        // Tag on stdout (capturable), nothing else.
        println!("{tag}");
    } else {
        println!("builder-base ready: {tag}");
    }
    Ok(())
}

/// `weft build-images [--push | --push-suffix <s>]`: ensure the four system
/// images + the worker builder-base exist locally under their
/// content-addressed refs, then optionally push each to its registry. The
/// release workflow's verb: it runs on every push to the release branch so a
/// clean checkout's `weft daemon start` (and every worker build's `FROM`)
/// pulls instead of compiling. With a suffix, each ref is pushed as
/// `<ref><suffix>` (one architecture's half; the workflow stitches the bare
/// ref into a multi-arch manifest list afterwards).
/// Ensures run concurrently (independent input sets, per-image buildkit cache
/// mounts); pushes run after ALL ensures, so a failed build never publishes a
/// partial set's siblings out of order. Stdout carries ONLY the bare refs,
/// one per line, for the workflow to capture; progress rides stderr.
/// `--print` stops after resolving: the refs this tree's content hashes
/// to, touching no image (setup.sh keys its engine-change sweep on the
/// builder-base line moving).
pub async fn run_build_images(push: bool, push_suffix: Option<String>, print: bool) -> Result<()> {
    if print {
        // The SAME list the ensure path prints (`bare_refs` carries
        // the stdout contract), never a second construction of it.
        let shared = crate::images::SharedImages {
            system: crate::images::SystemImages::resolve()?,
            builder_base: Some(crate::images::builder_base_ref()?),
            worker: Some(crate::images::standard_worker_ref()?),
        };
        for image_ref in shared.bare_refs() {
            println!("{image_ref}");
        }
        return Ok(());
    }
    let shared = crate::images::ensure_all_shared_images(
        false,
        crate::images::BaseFailure::Fatal,
        push_suffix.as_deref(),
    )
    .await?;
    if push || push_suffix.is_some() {
        for image_ref in shared.bare_refs() {
            // The suffixed name is exactly what the ensure materialized
            // (one shared `suffixed_ref` on both sides).
            crate::images::docker_push(&crate::images::suffixed_ref(
                image_ref,
                push_suffix.as_deref(),
            ))
            .await?;
        }
    }
    for image_ref in shared.bare_refs() {
        println!("{image_ref}");
    }
    Ok(())
}

/// Top-level helper used by every verb that needs the worker image
/// in place. Owns the image-skip check + emits build/push events;
/// callers don't duplicate the existence check anymore. If the
/// hash-tagged image is missing, this compiles the project and
/// docker-builds it; otherwise it ensures kind has the image
/// loaded.
pub async fn ensure_worker_image_with_progress(
    progress: &Progress,
    project_id: &str,
    image_tag: &str,
    worker_context_dir: &std::path::Path,
    referenced: Option<&crate::images::ReferencedImages>,
) -> Result<()> {
    // The registry can only ever hold the full-library worker CI built;
    // a project's own hash is never there, so only that one ref is
    // worth a pull before deciding to compile.
    if !crate::images::image_present(image_tag).await? && image_tag == crate::images::standard_worker_ref()? {
        let cfg = cluster_config();
        let cluster = if cfg.backend == ClusterBackend::Kind && kind_available(&cfg.cluster_name).await {
            Some(cfg.cluster_name.as_str())
        } else { None };
        crate::images::import_published_worker(image_tag, cluster).await?;
    }
    if crate::images::image_present(image_tag).await? {
        progress.build_skip(image_tag, "hash_match");
        let cfg = cluster_config();
        match cfg.backend {
            // A "loading image" line only when there is a load: the
            // usual no-change build finds the tag already on the node.
            ClusterBackend::Kind
                if kind_available(&cfg.cluster_name).await
                    && !crate::images::kind_node_has_tag(&cfg.cluster_name, image_tag).await =>
            {
                progress.image_push_start(image_tag);
                crate::images::kind_load(&cfg.cluster_name, image_tag, false).await?;
                progress.image_push_done(image_tag);
            }
            ClusterBackend::Kind => {}
            ClusterBackend::K8s => return Err(bail_k8s_push_needed(image_tag)),
        }
        let dropped = gc_stale_images(
            &[image_tag.to_string()],
            &[format!("weft.dev/project={project_id}")],
            referenced,
        )
        .await;
        progress.images_reclaimed(&dropped);
        return Ok(());
    }

    // Build from the ALREADY-STAGED worker context (produced once by
    // `weft_compiler::build_plan::plan_build_from`, shared with the hashing above so
    // the project compiles once per verb, not twice). The context's Dockerfile
    // FROMs the shared builder base; ensure it exists first (a no-op
    // cache hit after the first clean-machine build).
    crate::images::ensure_worker_builder_base().await?;

    progress.build_start(image_tag);
    docker_build_and_kind_load(progress, image_tag, project_id, worker_context_dir, referenced)
        .await?;
    progress.build_done(image_tag);
    Ok(())
}


/// The CLI's image-ref naming for the shared build brain
/// (`weft_compiler::build_plan`): BARE content-addressed tags (`weft-worker:<hash>`,
/// `weft-infra-<name>:<hash>`) built with local docker and loaded onto the node, no
/// registry prefix. A registry-backed build uses the same tag SUFFIX with a
/// registry prefix (`RegistryConfig`), so both mint identical identities from
/// one source of truth.
pub struct CliTagPolicy;

impl weft_compiler::build_plan::TagPolicy for CliTagPolicy {
    fn worker_ref(&self, binary_hash: &str) -> String {
        weft_compiler::build::worker_image_tag(binary_hash)
    }
    fn infra_ref(&self, image_name: &str, content_hash: &str) -> String {
        weft_compiler::image_set::infra_image_tag(image_name, content_hash)
    }
}

/// 16-char prefix of the SHA-256 hash, for human-facing log lines ONLY (never the
/// image tag, which uses the full hash). Short enough to keep progress output
/// legible.
pub fn short_hash(hash: &str) -> String {
    hash.chars().take(16).collect()
}

/// One source of truth for the "K8s backend selected, image is
/// local-only, push it yourself" failure. Used by every CLI step
/// (worker, infra, and node-test images alike) that would otherwise
/// let a subsequent spawn ImagePullBackOff with no breadcrumb back to
/// the CLI.
pub(crate) fn bail_k8s_push_needed(tag: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "K8s backend selected but image '{tag}' is only in local docker. \
         Push it to your registry before activating, e.g.:\n\
         \n\
         \tdocker tag {tag} <your-registry>/{tag}\n\
         \tdocker push <your-registry>/{tag}\n\
         \n\
         Or run against the kind cluster (set WEFT_CLUSTER_BACKEND=kind)."
    )
}

/// Run the docker build (assumes the project has been compiled),
/// prune dangling images, and load into kind if available. Inner
/// step of `ensure_worker_image_with_progress`. Emits the
/// image_push events around the kind-load step.
async fn docker_build_and_kind_load(
    progress: &Progress,
    tag: &str,
    project_id: &str,
    context_dir: &std::path::Path,
    referenced: Option<&crate::images::ReferencedImages>,
) -> Result<()> {
    build_worker_image(tag, context_dir, project_id).await?;
    let cfg = cluster_config();
    match cfg.backend {
        ClusterBackend::Kind if kind_available(&cfg.cluster_name).await => {
            progress.image_push_start(tag);
            images::kind_load(&cfg.cluster_name, tag, false).await?;
            progress.image_push_done(tag);
        }
        ClusterBackend::Kind => {
            // Kind cluster declared but not running: image stays in
            // local docker. The next ensure_worker_image_with_progress
            // call (e.g. after `weft daemon start`) will load it.
        }
        ClusterBackend::K8s => return Err(bail_k8s_push_needed(tag)),
    }
    // GC only once the fresh tag is fully in place everywhere (same
    // rule as the infra and node-test paths): the build stamped the
    // `weft.dev/project` label, so the project's now-stale prior
    // worker tags and dangling leftovers can go. The worker tag is
    // content-addressed (`weft-worker:<hash>`) and shared across
    // projects, so the project id rides on the LABEL, not the tag.
    // Cargo build cache (the heavy part) lives in the baked base +
    // BuildKit and survives this.
    let dropped = gc_stale_images(
        &[tag.to_string()],
        &[format!("weft.dev/project={project_id}")],
        referenced,
    )
    .await;
    progress.images_reclaimed(&dropped);
    Ok(())
}

/// Build one worker image: the shared [`images::docker_build`] with
/// the `weft.dev/project` label [`gc_stale_images`] later selects on.
async fn build_worker_image(
    tag: &str,
    ctx_dir: &std::path::Path,
    project_id: &str,
) -> Result<()> {
    let label = format!("weft.dev/project={project_id}");
    images::docker_build(tag, &ctx_dir.join("Dockerfile"), ctx_dir, &[label], None).await
}

/// Drop STALE content-addressed images: every tag of `repo` carrying
/// ALL the given `weft.dev/*` labels except `fresh` and except any tag
/// whose hash is in `referenced`, on host docker and (matched through
/// the shared node-ref matcher) in the kind node's containerd, then
/// the matching dangling leftovers. Content-addressed tags mint one
/// image per source version and nothing ever untags the old ones, so
/// each successful ensure ends here or the pile grows without bound.
///
/// `fresh` is every ref of ONE repo this build just ensured, bare or
/// registry-qualified (the repo and the kept tags are derived from it
/// through the one ref split, so no caller hand-rolls either): a plan
/// can hold several images of one repo (two node types shipping an
/// image directory of the same name mint the same repo under two
/// content hashes), and a GC keyed on a single fresh tag would delete
/// the sibling it built a moment ago. Refs of mixed repos are a
/// programming error and skip the GC, the safe answer.
///
/// `referenced` is the dispatcher's answer for what must SURVIVE
/// beyond the fresh tags (an idle project's current image, a draining
/// pod's image, another project's infra unit running the same
/// content-addressed tag; see `images::referenced_images`). `None`
/// means the answer could not be learned, and the whole GC is skipped:
/// an unbounded pile is a disk problem, a deleted live image is a
/// broken cluster. Node-test images alone pass an empty set: test pods
/// are short-lived and never restart from an old tag, so beyond the
/// fresh tag nothing is referenced by construction, and one still in
/// use refuses its node-side remove.
///
/// Best effort throughout (a transient docker error never fails a
/// build), silent on stdout (docker's own chatter is captured), and
/// the shared BuildKit layer cache is untouched, so rebuilding a
/// dropped tag stays warm. Returns the host refs docker untagged, for
/// the caller to report.
pub async fn gc_stale_images(
    fresh: &[String],
    labels: &[String],
    referenced: Option<&crate::images::ReferencedImages>,
) -> Vec<String> {
    let Some(referenced) = referenced else { return Vec::new() };
    let Some((repo, keep)) = fresh_repo_and_tags(fresh) else { return Vec::new() };
    let filters: Vec<String> = labels.iter().map(|l| format!("label={l}")).collect();
    let mut list = images::docker();
    list.arg("images");
    for f in &filters {
        list.args(["--filter", f]);
    }
    list.args(["--format", "{{.Repository}}:{{.Tag}}"]);
    let Ok(out) = list.output().await else { return Vec::new() };
    if !out.status.success() {
        return Vec::new();
    }
    let stale = images::host_images_matching(&String::from_utf8_lossy(&out.stdout), |r, t| {
        r == repo && !keep.contains(t) && !referenced.is_referenced(r, t)
    });
    if stale.is_empty() {
        return Vec::new();
    }
    // No `-f`: a tag docker refuses to drop (an image a host container
    // still runs) is information, and force would untag it anyway and
    // strand the container's restart. Output is captured, never
    // inherited: docker's `Untagged:` / `Deleted:` chatter would land
    // in the middle of a `--json` event stream, and the caller reports
    // the outcome in its own voice. Docker keeps going past a refused
    // tag, so the untagged refs are read back from its output instead
    // of assumed from the request.
    let dropped: Vec<String> = match images::docker().args(["rmi"]).args(&stale).output().await {
        Ok(out) => String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| l.strip_prefix("Untagged: "))
            .map(|r| r.trim().to_string())
            .collect(),
        Err(_) => Vec::new(),
    };
    // The kind node's containerd holds its own copy of every loaded
    // tag (with no labels); condemn exactly the host-stale hash set
    // there, through the one matcher every node cleanup uses (bare,
    // docker-canonical, and registry-qualified spellings). A tag a
    // live pod still runs refuses the rmi and survives. A node image
    // whose tags mix stale and live (two builds produced identical
    // bytes) is intentionally spared here, forever: once the live tag
    // goes stale too it is no longer in the HOST's stale list, so
    // this sweep never condemns the group. `weft clean --images`
    // reclaims it, since it condemns against the dispatcher's
    // referenced set instead of the host-stale set.
    let stale_tags: std::collections::BTreeSet<&str> = stale
        .iter()
        .filter_map(|s| images::ref_repo_tag(s).ok().map(|(_, t)| t))
        .collect();
    let cfg = cluster_config();
    if cfg.backend == ClusterBackend::Kind && kind_available(&cfg.cluster_name).await {
        if let Ok(groups) = images::kind_node_image_tag_groups(&cfg.cluster_name).await {
            let node = format!("{}-control-plane", cfg.cluster_name);
            let node_stale =
                images::node_images_matching(&groups, |r, t| r == repo && stale_tags.contains(t));
            if !node_stale.is_empty() {
                let _ = images::docker()
                    .args(["exec", &node, "crictl", "rmi"])
                    .args(&node_stale)
                    .output()
                    .await;
            }
        }
    }
    let mut prune = images::docker();
    prune.args(["image", "prune", "--force", "--filter", "dangling=true"]);
    for f in &filters {
        prune.args(["--filter", f]);
    }
    let _ = prune.output().await;
    dropped
}

/// The one repo `fresh` names plus its bare tags, or `None` (with a
/// warning) when the refs do not all parse to the same repo: every
/// weft ref carries a content tag by construction, so a failure here
/// is a programming error and the GC skips rather than guesses.
fn fresh_repo_and_tags(fresh: &[String]) -> Option<(&str, std::collections::BTreeSet<&str>)> {
    let mut repo: Option<&str> = None;
    let mut tags = std::collections::BTreeSet::new();
    for r in fresh {
        match images::ref_repo_tag(r) {
            Ok((rp, t)) if repo.is_none() || repo == Some(rp) => {
                repo = Some(rp);
                tags.insert(t);
            }
            _ => {
                tracing::warn!(
                    target: "weft_cli::images",
                    "skipping the stale-image GC: fresh refs {fresh:?} do not name one repo"
                );
                return None;
            }
        }
    }
    repo.map(|rp| (rp, tags))
}

pub async fn kind_available(cluster_name: &str) -> bool {
    let which = Command::new("which").arg("kind").output().await;
    if !matches!(which, Ok(o) if o.status.success()) {
        return false;
    }
    let out = images::quiet_stdout("kind").args(["get", "clusters"]).output().await;
    matches!(out, Ok(o) if o.status.success()
        && String::from_utf8_lossy(&o.stdout).lines().any(|l| l == cluster_name))
}
