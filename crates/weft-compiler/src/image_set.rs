//! The set of infra images a project needs, enumerated once.
//!
//! A project's runnable artifacts are its WORKER image (one, content-addressed by
//! the binary hash) plus, for every `requires_infra` node, the IMAGES that node
//! declares (`metadata.images`, e.g. `images/bridge`). The worker image is built
//! identically everywhere off `binary_hash`; the infra images are enumerated the
//! same way by every caller. This module is the ONE place that walks the graph +
//! catalog to produce the infra-image list, so every caller builds the SAME set
//! with the SAME names + content hashes, then each applies its own tag form. Two
//! ref forms exist: a bare tag, and a registry-qualified tag pushed to a
//! registry.
//!
//! Pure + filesystem-reading (it hashes each image's source dir), so it is gated
//! behind the `build` feature alongside the rest of the hash path; the browser
//! parse build never enumerates images.

use std::path::PathBuf;

use anyhow::{anyhow, Result};
use weft_catalog::FsCatalog;
use weft_core::project::ProjectDefinition;

use crate::hash::compute_image_hash;

/// One infra image a project needs: which node declares it, its local NAME (the
/// `Image::Local { name }` the infra spec references, = the image dir's basename),
/// the source dir to build (holds the `Dockerfile`), and the content hash of that
/// source (the SAME `compute_image_hash` the CLI tag suffix uses). The caller
/// turns `(name, content_hash)` into a concrete tag its environment can pull.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InfraImage {
    /// The graph node id that declares (requires) this image.
    pub node_id: String,
    /// The image's local name = the image path's basename (`images/bridge` ->
    /// `bridge`). Matches `Image::Local { name }` on the node's infra spec, so the
    /// supervisor resolves the reference to the tag the caller stores under it.
    pub image_name: String,
    /// Directory holding the image's `Dockerfile` + build context.
    pub source_dir: PathBuf,
    /// Content hash of the image source (`compute_image_hash`); the tag suffix.
    pub content_hash: String,
}

/// Enumerate every infra image the project needs, walking the enriched
/// definition's `requires_infra` nodes and their declared `metadata.images`.
/// Fails loud (never silently skips) if a node's type is missing from the catalog
/// or declares an image with no `Dockerfile`, so a misconfigured node surfaces at
/// build time rather than as a dangling `Image::Local` at apply time.
///
/// Order follows the definition's node order then each node's image order, so the
/// list is deterministic across runs (callers that dedup by tag rely on this).
pub fn infra_images(definition: &ProjectDefinition, catalog: &FsCatalog) -> Result<Vec<InfraImage>> {
    let mut out = Vec::new();
    for node in definition.nodes.iter().filter(|n| n.requires_infra) {
        let entry = catalog.entry(&node.node_type).ok_or_else(|| {
            anyhow!(
                "node '{}' has type '{}' which is not in the catalog",
                node.id,
                node.node_type
            )
        })?;
        for image_path in &entry.metadata.images {
            let source_dir = entry.source_dir.join(image_path);
            let image_name = std::path::Path::new(image_path)
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| {
                    anyhow!(
                        "node type '{}' declared invalid image path '{image_path}'",
                        node.node_type
                    )
                })?
                .to_string();
            let dockerfile = source_dir.join("Dockerfile");
            if !dockerfile.is_file() {
                return Err(anyhow!(
                    "node type '{}' declares image '{image_name}' but no Dockerfile at {}",
                    node.node_type,
                    dockerfile.display()
                ));
            }
            let content_hash = compute_image_hash(&node.node_type, &source_dir)?;
            out.push(InfraImage {
                node_id: node.id.clone(),
                image_name,
                source_dir,
                content_hash,
            });
        }
    }
    Ok(out)
}

/// The registry repo segment for infra images, mirroring `WORKER_IMAGE_REPO`. An
/// infra image's bare repo is `weft-infra-<image_name>`; the full bare tag is
/// `weft-infra-<image_name>:<content_hash>`. ONE source of truth for the infra
/// tag shape, shared by the CLI (local tag) and the dispatcher (which prepends
/// the registry prefix), so the two cannot drift.
pub fn infra_image_repo(image_name: &str) -> String {
    format!("weft-infra-{image_name}")
}

/// The bare (registry-UNqualified) content-addressed infra image tag,
/// `weft-infra-<image_name>:<content_hash>`. The CLI builds + `kind load`s this;
/// the dispatcher prepends its registry prefix to the same suffix.
pub fn infra_image_tag(image_name: &str, content_hash: &str) -> String {
    format!("{}:{}", infra_image_repo(image_name), content_hash)
}
