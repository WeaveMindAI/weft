//! The shared build-freshness BRAIN: compile a project on disk, compute the three
//! authoritative hashes, enumerate + stage every image it needs (worker + one per
//! infra image), and mint each image's ref.
//!
//! This is the ONE place that decides WHAT a project version needs, so every
//! caller agrees byte-for-byte. The one thing a caller supplies is a seam here:
//!   - `TagPolicy`: how an image ref is NAMED. A ref is either a bare tag or a
//!     registry-qualified ref; both derive from the SAME content hashes, so the
//!     identity is identical and only the string form differs.
//! Two things are NOT here because they interleave with each caller's own logic:
//! (1) probing whether a ref is already built (against a container daemon, or a
//! registry), and (2) the actual build execution of the stale set. Each caller
//! drives its own check-and-build over `plan.images`.
//!
//! Because the worker image ref is content-addressed on `binary_hash` (engine +
//! toolchain + builder-base Dockerfile + referenced node impls + node-type set +
//! weft.toml build config + deps) and each infra image ref on its own source dir,
//! recomputing the plan and re-checking presence is the COMPLETE staleness rule:
//! change the engine (or a node impl, or an infra image's source) and the affected
//! ref stops matching, so it rebuilds; change nothing and every ref is present, so
//! nothing rebuilds. There is no separate stamp to keep in sync.

use std::path::Path;

use crate::error::CompileResult;
use crate::image_set::{self, InfraImage};
use crate::project::Project;

/// Whether a planned image is the project's worker or one of its infra images.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageKind {
    Worker,
    Infra,
}

/// One image a project version needs, fully resolved: its kind, its content-
/// addressed ref (via the `TagPolicy`), the docker build context to build it from,
/// and (infra only) the `(node_id, image_name)` the ref resolves back to so the
/// caller can record `Image::Local { name } -> ref` for the supervisor.
#[derive(Debug, Clone)]
pub struct PlannedImage {
    pub kind: ImageKind,
    /// The content-addressed image ref (bare tag or registry-qualified), minted from
    /// the content hash via the `TagPolicy`. Always set; whether it is already built
    /// is answered separately by each environment's own presence probe.
    pub image_ref: String,
    /// The docker build context: the staged worker context dir for the worker, the
    /// node's own `images/<name>/` source dir for an infra image.
    pub context_dir: std::path::PathBuf,
    /// The declaring node id (infra images only).
    pub node_id: Option<String>,
    /// The image's local name (infra images only); the `Image::Local { name }` the
    /// infra spec references.
    pub image_name: Option<String>,
}

/// The full set of artifacts a compiled project version needs, plus the three
/// authoritative hashes and the compiled definition. Produced by [`plan_build`].
#[derive(Debug, Clone)]
pub struct BuildPlan {
    /// The compiled `ProjectDefinition`, serialized. Sent to the dispatcher at
    /// register so it stores the exact artifact the hashes describe.
    pub definition_json: String,
    /// Worker image identity: engine + node impls + build env. The worker ref is
    /// `TagPolicy::worker_ref(binary_hash)`.
    pub binary_hash: String,
    /// Runtime shape identity: topology + config. Drives resync (definition drift).
    pub definition_hash: String,
    /// Infra closure identity: infra-node sources + engine. Drives infra upgrade.
    pub infra_hash: String,
    /// Per-node-type content hashes of the node code compiled into the worker
    /// binary (`node_type -> package-root hash`). The build's provenance
    /// manifest: exactly which node source went in.
    pub node_source_hashes: std::collections::BTreeMap<String, String>,
    /// Per-node-type provider declarations (`node_type -> the paid service
    /// that node's `metadata.json` declares`), for exactly the node types in
    /// the binary (same referenced set as `node_source_hashes`, so the two
    /// can never disagree about what went in). Nodes with no declaration are
    /// absent. Recorded at register so the deployment forwards a node's
    /// deployment-key calls to the URL the node's OWN source declared.
    pub provider_decls: std::collections::BTreeMap<String, weft_core::node::ProviderDecl>,
    /// The worker image plus every infra image, each with its ref + build context.
    pub images: Vec<PlannedImage>,
}

/// How image refs are NAMED. The one naming difference between bare tags (loaded
/// onto the node) and registry-qualified refs; both feed on the SAME content
/// hashes so the identity is identical, only the string form differs.
pub trait TagPolicy {
    /// The worker image ref for a given binary hash.
    fn worker_ref(&self, binary_hash: &str) -> String;
    /// An infra image ref for a given local image name + its content hash.
    fn infra_ref(&self, image_name: &str, content_hash: &str) -> String;
}

/// Compile + enrich a project ALREADY ON DISK at `project_root`, compute the three
/// hashes, enumerate + stage every image, and mint each ref via `tags`. One path,
/// so a project builds identically whether the source came from the user's disk or
/// an unpacked content tree.
///
/// The project MUST already contain its own `nodes/` (including the seeded
/// `base_catalog/`); nothing is injected from outside the folder. Callers that start
/// from a bare upload seed the catalog into the folder FIRST (the create door), the
/// same self-contained shape `weft new` produces on disk.
///
/// Pure + blocking (filesystem + compile); run it on a blocking thread from async
/// callers. The returned worker `context_dir` lives under a temp dir the caller must
/// keep alive until it has built the worker image.
pub fn plan_build(
    project_root: &Path,
    builder_base_image: &str,
    tags: &dyn TagPolicy,
) -> CompileResult<BuildPlan> {
    let project = Project::load(project_root)?;
    let (definition, catalog) = crate::hash::load_enriched_project(&project)
        .map_err(|e| e.context("compile + enrich project"))?;
    plan_build_from(&project, &definition, &catalog, builder_base_image, tags)
}

/// Like [`plan_build`] but for a caller that ALREADY compiled + enriched the project
/// (e.g. the CLI, which compiles with structured diagnostics for the editor's error
/// modal). Avoids a second compile: it computes the hashes + stages the images from
/// the given `definition` + `catalog`. `plan_build` is the convenience that compiles
/// first; both share this body so the plan is identical whichever entry is used.
pub fn plan_build_from(
    project: &Project,
    definition: &weft_core::project::ProjectDefinition,
    catalog: &weft_catalog::FsCatalog,
    builder_base_image: &str,
    tags: &dyn TagPolicy,
) -> CompileResult<BuildPlan> {
    let weft_root = crate::build::resolve_weft_root()?;

    let binary_hash = crate::hash::compute_binary_hash(definition, project, &weft_root, catalog)
        .map_err(|e| e.context("compute binary hash"))?;
    let definition_hash = crate::hash::compute_definition_hash(definition)
        .map_err(|e| e.context("compute definition hash"))?;
    let infra_hash =
        crate::hash::compute_infra_hash(definition, &project.root, &weft_root, catalog)
            .map_err(|e| e.context("compute infra hash"))?;
    let node_source_hashes =
        crate::hash::compute_node_source_hashes(definition, &project.root, &weft_root, catalog)
            .map_err(|e| e.context("compute node source hashes"))?;
    let provider_decls = collect_provider_decls(definition, catalog)
        .map_err(|e| e.context("collect provider declarations"))?;
    let definition_json = serde_json::to_string(definition)
        .map_err(|e| anyhow::anyhow!("serialize compiled definition: {e}"))?;

    // Stage the worker docker build context (`FROM {{builder_base_image}}`).
    let staged = crate::build::build_project(&project.root, true, builder_base_image)?;

    let mut images = vec![PlannedImage {
        kind: ImageKind::Worker,
        image_ref: tags.worker_ref(&binary_hash),
        context_dir: staged.build_context,
        node_id: None,
        image_name: None,
    }];

    // Every infra image, via the shared enumerator (same set, same content hashes as
    // the CLI). Each infra image's context is its own source dir (holds a Dockerfile).
    for img in image_set::infra_images(definition, catalog)
        .map_err(|e| e.context("enumerate infra images"))?
    {
        let InfraImage { node_id, image_name, source_dir, content_hash } = img;
        images.push(PlannedImage {
            kind: ImageKind::Infra,
            image_ref: tags.infra_ref(&image_name, &content_hash),
            context_dir: source_dir,
            node_id: Some(node_id),
            image_name: Some(image_name),
        });
    }

    Ok(BuildPlan {
        definition_json,
        binary_hash,
        definition_hash,
        infra_hash,
        node_source_hashes,
        provider_decls,
        images,
    })
}

/// The provider declarations of exactly the node types the project references
/// (same set as `node_source_hashes`), from each node's resolved metadata
/// (`provider` key, own or package-inherited). Fails LOUDLY when two referenced node
/// types declare the same provider name with different base URLs: that is a
/// contradiction about where the deployment's key for that provider gets
/// sent, and no build may pick a side. Same name + same URL is fine (two
/// nodes, one service; package siblings share one file, so agreement is the
/// norm).
fn collect_provider_decls(
    definition: &weft_core::project::ProjectDefinition,
    catalog: &weft_catalog::FsCatalog,
) -> anyhow::Result<std::collections::BTreeMap<String, weft_core::node::ProviderDecl>> {
    let mut decls = std::collections::BTreeMap::new();
    for nt in crate::codegen::collect_node_types(definition) {
        if let Some(decl) = catalog.provider_of(&nt) {
            decls.insert(nt, decl.clone());
        }
    }
    check_provider_decl_conflicts(&decls)?;
    Ok(decls)
}

/// Pure conflict check over collected declarations: one provider name, one
/// base URL, project-wide. Split out so the rule is testable without a
/// filesystem catalog.
fn check_provider_decl_conflicts(
    decls: &std::collections::BTreeMap<String, weft_core::node::ProviderDecl>,
) -> anyhow::Result<()> {
    let mut by_name: std::collections::BTreeMap<&str, (&str, &str)> =
        std::collections::BTreeMap::new();
    for (node_type, decl) in decls {
        match by_name.get(decl.name.as_str()) {
            Some((first_node, first_url)) if *first_url != decl.base_url => {
                anyhow::bail!(
                    "conflicting provider declarations for '{}': node {} declares base_url {} \
                     but node {} declares base_url {}; a provider name must map to one URL \
                     project-wide",
                    decl.name, first_node, first_url, node_type, decl.base_url,
                );
            }
            Some(_) => {}
            None => {
                by_name.insert(&decl.name, (node_type, &decl.base_url));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::node::ProviderDecl;

    fn decl(name: &str, url: &str) -> ProviderDecl {
        ProviderDecl { name: name.into(), base_url: url.into() }
    }

    #[test]
    fn provider_decl_conflicts_fail_loudly_and_agreement_passes() {
        // No declarations: fine (the overwhelming majority of projects).
        check_provider_decl_conflicts(&Default::default()).unwrap();

        // Two nodes, one service, same URL: fine.
        let agree: std::collections::BTreeMap<String, ProviderDecl> = [
            ("A".to_string(), decl("openrouter", "https://openrouter.ai/api/v1")),
            ("B".to_string(), decl("openrouter", "https://openrouter.ai/api/v1")),
        ]
        .into();
        check_provider_decl_conflicts(&agree).unwrap();

        // Different services: fine.
        let distinct: std::collections::BTreeMap<String, ProviderDecl> = [
            ("A".to_string(), decl("openrouter", "https://openrouter.ai/api/v1")),
            ("B".to_string(), decl("elevenlabs", "https://api.elevenlabs.io/v1")),
        ]
        .into();
        check_provider_decl_conflicts(&distinct).unwrap();

        // Same name, different URL: a contradiction; the error names both sides.
        let conflict: std::collections::BTreeMap<String, ProviderDecl> = [
            ("A".to_string(), decl("openrouter", "https://openrouter.ai/api/v1")),
            ("B".to_string(), decl("openrouter", "https://evil.example")),
        ]
        .into();
        let err = check_provider_decl_conflicts(&conflict).unwrap_err().to_string();
        assert!(err.contains("openrouter"), "{err}");
        assert!(err.contains("A") && err.contains("B"), "{err}");
        assert!(err.contains("https://evil.example"), "{err}");
    }
}
