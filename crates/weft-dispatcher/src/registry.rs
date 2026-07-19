//! Worker-image registry configuration: the "registry-at-a-URL" the build pushes
//! to and the worker pulls from. ONE source of truth for the registry URL, the
//! pull-secret, the builder-base image the worker Dockerfile FROMs, and the
//! CONTENT-addressed worker image ref (`<registry>/weft-worker:<binary_hash>`).
//!
//! Presence of a registry is the signal that worker images are pulled from a
//! registry they were pushed to. With no registry configured the runtime mints
//! bare tags (images built and loaded onto the node directly), so its
//! registry-ref minting is never reached.
//!
//! Worker images are content-addressed (no project id in the tag) so two users
//! who build the byte-identical thing dedup to ONE build + ONE image, exactly as
//! infra images already do (`weft-infra-<name>:<hash>`). The binary hash is a
//! strong SHA-256 over the actual source, so two different inputs never collide
//! onto one content tag, and only the trusted builder ever writes a content tag.

// The worker image repo segment + the bare content-addressed tag live in
// weft-compiler (`build::WORKER_IMAGE_REPO` / `build::worker_image_tag`), the one
// crate both the CLI (which builds the tag) and the dispatcher (which spawns it)
// share, so the two cannot drift. Re-exported here for the local (no-registry)
// spawn path and so `RegistryConfig` can prepend its prefix to the same suffix.
pub use weft_compiler::build::{worker_image_tag as bare_worker_image_ref, WORKER_IMAGE_REPO};

/// Worker-image registry config, used when worker images are pulled from a
/// registry they were pushed to. Read from env in `build_state`. Absent when
/// worker images are loaded onto the node directly.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// Registry host + repo prefix the worker image is tagged under and pulled
    /// from, e.g. `us-central1-docker.pkg.dev/my-project/weft-images`. No
    /// trailing slash (normalized on construction).
    pub url: String,
    /// The registry-qualified builder-base image the per-project worker
    /// Dockerfile FROMs (the builder built + pushed it), e.g.
    /// `<registry>/weft-builder-base:<hash>`. The dispatcher passes this to the
    /// compiler when staging the build context.
    pub builder_base_image: String,
    /// Name of the k8s `imagePullSecret` worker pods reference to pull from the
    /// registry. `None` when the cluster authenticates pulls implicitly (GKE via
    /// its node service account; a node-local emulation needs none). `Some` for a
    /// cluster handed an explicit registry credential (minikube via the gcp-auth
    /// addon writes one).
    pub pull_secret: Option<String>,
}

impl RegistryConfig {
    /// Read registry config from env. Returns `None` when no registry is
    /// configured (worker images are built + loaded onto the node directly).
    /// `Some` requires BOTH the URL and the
    /// builder-base image, since a build cannot produce a worker image without a
    /// base to compile against; a half-configured registry fails loud.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let url = std::env::var("WEFT_REGISTRY_URL")
            .ok()
            .filter(|s| !s.is_empty());
        let Some(url) = url else {
            return Ok(None);
        };
        let builder_base_image = std::env::var("WEFT_BUILDER_BASE_IMAGE")
            .ok()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "WEFT_REGISTRY_URL is set but WEFT_BUILDER_BASE_IMAGE is not; \
                     a build needs the registry-qualified builder-base image the \
                     worker image FROMs"
                )
            })?;
        let pull_secret = std::env::var("WEFT_REGISTRY_PULL_SECRET")
            .ok()
            .filter(|s| !s.is_empty());
        Ok(Some(Self {
            url: url.trim_end_matches('/').to_string(),
            builder_base_image,
            pull_secret,
        }))
    }

    /// Mint the CONTENT-addressed, registry-qualified worker image ref for a
    /// binary hash: `<registry>/weft-worker:<binary_hash>`. The single place push
    /// (the builder) and pull (the worker spawn) agree on the tag. The full
    /// binary hash is used (not a short prefix), so the tag is collision-free.
    pub fn worker_image_ref(&self, binary_hash: &str) -> String {
        format!("{}/{}:{}", self.url, WORKER_IMAGE_REPO, binary_hash)
    }

    /// Mint the CONTENT-addressed, registry-qualified INFRA image ref for an
    /// `(image_name, content_hash)`: `<registry>/weft-infra-<name>:<content_hash>`.
    /// The infra mirror of `worker_image_ref`, sharing the same registry prefix +
    /// the bare infra-tag suffix (`weft_compiler::image_set::infra_image_tag`) so
    /// push (the builder) and the supervisor's `Image::Local` resolution agree on
    /// the tag. The FULL content hash is used (not a short prefix), so identical
    /// infra image builds dedup across projects / tenants.
    pub fn infra_image_ref(&self, image_name: &str, content_hash: &str) -> String {
        format!(
            "{}/{}",
            self.url,
            weft_compiler::image_set::infra_image_tag(image_name, content_hash)
        )
    }
}

/// Registry-qualified image-ref NAMING for the shared build brain (what the
/// build pushes + the supervisor pulls). Passing the `RegistryConfig`
/// straight into `weft_compiler::build_plan::plan_build_from` produces the SAME image
/// set as the CLI, differing only in the tag string form.
impl weft_compiler::build_plan::TagPolicy for RegistryConfig {
    fn worker_ref(&self, binary_hash: &str) -> String {
        self.worker_image_ref(binary_hash)
    }
    fn infra_ref(&self, image_name: &str, content_hash: &str) -> String {
        self.infra_image_ref(image_name, content_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_ref_is_content_addressed_no_project_id() {
        let cfg = RegistryConfig {
            url: "us-central1-docker.pkg.dev/my-project/weft-images".into(),
            builder_base_image: "x/weft-builder-base:abc".into(),
            pull_secret: None,
        };
        let r = cfg.worker_image_ref("deadbeef00");
        assert_eq!(
            r,
            "us-central1-docker.pkg.dev/my-project/weft-images/weft-worker:deadbeef00"
        );
        // Two different projects with the SAME binary hash mint the SAME ref:
        // the tag is purely a function of the hash, so identical builds dedup.
        assert_eq!(r, cfg.worker_image_ref("deadbeef00"));
        // Different content -> different tag.
        assert_ne!(r, cfg.worker_image_ref("cafe1234"));
    }

    #[test]
    fn trailing_slash_normalized() {
        let cfg = RegistryConfig {
            url: "reg.example.com/repo/".trim_end_matches('/').to_string(),
            builder_base_image: "b".into(),
            pull_secret: None,
        };
        assert_eq!(cfg.worker_image_ref("h"), "reg.example.com/repo/weft-worker:h");
    }
}
