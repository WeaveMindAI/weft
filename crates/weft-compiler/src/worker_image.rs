//! Worker image codegen: the Dockerfile of a worker packaged as an image
//! (`weft_core::builds::WorkerPackaging::Image`, a cloud's). A local
//! install compiles its workers on the machine instead and never reads
//! any of this but the shared pieces the host build uses too (the cache
//! sweep, `[build.env]`).
//!
//! Given a project (for its `[build.worker]` config, a cloud-only
//! setting) and the set of referenced node types with what each needs,
//! emit a multi-stage Dockerfile that builds the worker container image.
//!
//! Pipeline:
//!
//! 1. **Parse the base image.** User's `[build.worker] base_image`
//!    or the built-in default ([`DEFAULT_BASE_IMAGE`]). Codegen
//!    derives a package manager and a `<distro>_<major>` key from
//!    the image string (`[build.worker] package_manager` names the
//!    manager when the family is not one weft knows, which is refused
//!    otherwise).
//!
//! 2. **Collect per-stage packages.** Each referenced node's
//!    `[[needs]]` (`weft_core::needs::Need`, read once by the build) has
//!    a stage (`build`: the builder stage, `run`: the runtime stage) and
//!    its packages by manager and distro. Codegen:
//!    - picks the entry matching the chosen distro_key if present,
//!    - else the `default` entry,
//!    - else the need contributes nothing to the image. Only an error
//!      if the need had entries for this manager but none matched AND
//!      no default. A need with no entry for the image's manager at all
//!      is left to the machine (`WorkerDockerfile::left_to_machine`), which
//!      the platform's needs check refuses, never the Dockerfile.
//!
//! 3. **Union across nodes, emit the Dockerfile.** Builder stage
//!    installs build-time packages, fetches rust via rustup (or
//!    starts from the prebuilt builder base), and builds the
//!    generated crate with `weft-isolate` (`crate::worker_isolate`):
//!    a `cargo build --release` that leaves out every node package
//!    that does not compile and records which it left out. Runtime
//!    stage installs runtime-only packages and copies the compiled
//!    binary and that record from the builder.
//!
//! The template is a simple `{{token}}` substitution. Built-in
//! template lives in `default_template()`. Users can override by
//! setting `[build.worker] dockerfile_template = "path"` in
//! weft.toml; the same tokens are substituted.

use std::collections::BTreeSet;
use std::path::Path;

use weft_catalog::FsCatalog;
use weft_core::needs::{NeedStage, NodeNeeds, PackageManager};

use crate::error::{BuildError, BuildResult};
use crate::project::WorkerBuildSection;

/// Output of `emit`: the Dockerfile body ready to be written
/// plus the resolved metadata the CLI uses for logs.
pub struct WorkerDockerfile {
    pub body: String,
    pub base: BaseImage,
    /// Union of BUILD-stage packages actually included.
    pub build_packages: Vec<String>,
    /// Union of RUNTIME-stage packages actually included.
    pub runtime_packages: Vec<String>,
    /// Union of `[build.env]` across referenced nodes, already
    /// substituted for `{{catalog_path}}`.
    pub build_env: std::collections::BTreeMap<String, String>,
    /// Builder-base image tag the rendered Dockerfile references.
    /// `Some(tag)` whenever the CHOSEN template (built-in prebuilt OR
    /// a user-supplied custom template) contains the
    /// `{{builder_base_image}}` token, so the image must exist before the
    /// build (on the cloud the install workflow builds it, `weft
    /// build-images`); `None` when the rendered Dockerfile never FROMs it
    /// (the from-scratch template, or a custom template that builds its
    /// own toolchain), whose build context then carries the weft
    /// workspace itself.
    pub builder_base: Option<String>,
    /// The stages whose install line the template renders
    /// (`{{install_build_system_packages}}`,
    /// `{{install_runtime_system_packages}}`): every stage for a built-in
    /// template, the ones a custom template names.
    pub installs_stages: BTreeSet<NeedStage>,
    /// Whether the image keeps the record of the node packages its build
    /// left out at `weft_core::builds::LEFT_OUT_RECORD_PATH`: true for
    /// both built-in templates, false for a custom template, which builds
    /// the crate its own way and leaves nothing out.
    pub writes_left_out_record: bool,
}

/// Parsed base-image metadata. `manager` drives the install
/// command; `distro_key` (e.g. `debian_12`) drives the lookup of each
/// need's packages (`Need::packages_for`).
#[derive(Debug, Clone)]
pub struct BaseImage {
    pub raw: String,
    pub manager: PackageManager,
    /// `<family>_<major>`. Empty when the image string doesn't
    /// resolve to a known distro; codegen falls back to `default`
    /// entries only in that case.
    pub distro_key: String,
}

/// The key a `deps.toml` names `manager` by (`[needs.install.<key>]`).
fn manager_key(manager: PackageManager) -> &'static str {
    match manager {
        PackageManager::Apt => "apt",
        PackageManager::Dnf => "dnf",
        PackageManager::Apk => "apk",
        PackageManager::Brew => "brew",
    }
}

/// The image a worker image runs on when its project names no
/// `[build.worker] base_image`, and the base the builder base's packages
/// are chosen for.
// SYNC: builder base distro <-> deploy/docker/worker-builder-base.Dockerfile (FROM ... AS toolchain)
pub const DEFAULT_BASE_IMAGE: &str = "debian:trixie-slim";

/// Weft workspace mount point INSIDE the builder container. The
/// docker build context copies the language-runtime workspace
/// (`crates/`, `Cargo.toml`, `Cargo.lock`) to this path, giving the
/// generated crate access to the weft-engine / weft-core crates via
/// `../weft/crates/*` path dependencies. No node code lives here.
pub const WEFT_MOUNT: &str = "/weft";

/// In-container path to the project's node source (each package at its
/// path in the project, under `nodes/` or `src/`). The build
/// context stages `project-nodes/` here; every node's `#[path]` shim
/// and every `{{catalog_path}}` substitution resolves under it (e.g.
/// `/weft/project-nodes/nodes/base_catalog/basic/exec_python/mod.rs`). This is the only
/// place node source comes from: the project owns all its nodes.
pub const NODES_MOUNT: &str = "/weft/project-nodes";

/// Image repo for the shared pre-built builder base. `weft build-images`
/// qualifies it with the registry and a short hash of the engine
/// workspace (`crate::hash::compute_builder_base_hash`), so an engine
/// bump produces a fresh ref; a cloud install's worker Dockerfiles `FROM`
/// the one it was set up with.
pub const BUILDER_BASE_REPO: &str = "weft-builder-base";

/// Build-context-relative directory the builder base's worker crate is
/// staged into: the stock project's full-library worker, emitted by
/// `codegen::emit` during `build::stage_builder_base_context` exactly as a
/// project build emits its own. The base Dockerfile COPYs it to `/work`
/// and compiles it into the shared `/weft/target`, precooking every rlib
/// an untouched project reuses.
pub const WARMUP_CRATE_DIR: &str = ".weft-warmup";

/// Repo-root-relative directory the builder-base docker build CONTEXT is staged
/// into (`build::stage_builder_base_context`): the worker-linked workspace slice
/// + toolchain pin + warm-up crate, and nothing else. A derived artifact
/// (gitignored), regenerated on every base build.
pub const BASE_CONTEXT_DIR: &str = ".weft-base-context";

/// Emit the Dockerfile for a project's worker image.
///
/// `project_root` is only used to resolve a relative
/// `dockerfile_template` path and each node's in-image path.
/// `binary_name` is the generated crate's binary name so the builder
/// stage knows which binary to copy over. `referenced` is the set of node
/// types the project compiles against (from codegen's own walk), and
/// `needs` what each of them needs (the build read their `deps.toml`
/// once, `codegen::node_needs`).
///
/// `builder_base` is the fully-qualified ref of the shared pre-built
/// builder-base image. When the user's runtime base is debian-family and
/// they haven't supplied a custom Dockerfile template, the builder stage
/// `FROM`s this image instead of re-installing rustup / build packages /
/// re-fetching the cargo registry per project. When no custom template
/// is supplied AND the runtime base is non-debian, we use the
/// from-scratch template instead (the builder must match the runtime
/// ABI: glibc base vs musl base). A user-supplied custom template is used
/// verbatim and bypasses both built-in templates.
///
/// `baked` is what the builder base already installed for the stock
/// worker (`crate::build::StockProject::builder_stage`): a builder stage
/// that FROMs the base installs only the packages beyond it, so an
/// untouched project's build runs no `apt-get update` at all.
#[allow(clippy::too_many_arguments)]
pub fn emit(
    build: &WorkerBuildSection,
    project_root: &Path,
    catalog: &FsCatalog,
    referenced: &BTreeSet<String>,
    needs: &NodeNeeds,
    binary_name: &str,
    builder_base: &str,
    baked: &BuilderStage,
) -> BuildResult<WorkerDockerfile> {
    let builder_base_ref = builder_base;
    let base = base_image_of(build)?;

    let mut build_packages = collect_stage_packages(needs, &base, NeedStage::Build)?;
    let runtime_packages = collect_stage_packages(needs, &base, NeedStage::Run)?;
    let NodesBuildEnv { vars: build_env, cache_tag, .. } = collect_build_env(catalog, referenced, &|node_type, source_dir| {
        node_catalog_path(node_type, source_dir, project_root)
    })?;

    // The pre-built base shortcuts the "install rustup + apt
    // build-essential" cycle. It only fits a runtime base of the very
    // distro release it was built on ([`DEFAULT_BASE_IMAGE`]'s): a
    // binary may need any glibc symbol of the release it was built on,
    // so the loader refuses it on an older glibc (bookworm, Ubuntu
    // 24.04), it cannot run on musl at all, and the build
    // packages are named for the runtime base's distro. It never
    // applies to a custom Dockerfile template (the Some arm below),
    // which may not respect our base layout. Every other base takes
    // the from-scratch template, which builds inside the runtime base
    // itself.
    let use_prebuilt_base = base.manager == PackageManager::Apt && base.distro_key == default_base_image().distro_key;
    if use_prebuilt_base && build.dockerfile_template.is_none() {
        build_packages.retain(|package| !baked.packages.contains(package));
    }

    let template = match &build.dockerfile_template {
        Some(rel) => {
            let path = project_root.join(rel);
            let template = std::fs::read_to_string(&path).map_err(|e| {
                BuildError::Build(format!(
                    "read custom Dockerfile template {}: {}",
                    path.display(),
                    e
                ))
            })?;
            refuse_retired_placeholder(&path, &template)?;
            template
        }
        None => {
            if use_prebuilt_base {
                prebuilt_base_template()
            } else {
                default_template()
            }
        }
    };

    // Whichever template was chosen (built-in or custom), if it references
    // `{{builder_base_image}}` the rendered Dockerfile FROMs that ref, and
    // the build context needs no weft workspace of its own.
    let builder_base_out = template
        .contains("{{builder_base_image}}")
        .then(|| builder_base_ref.to_string());

    // The compile cache is per builder: artifacts from a from-scratch
    // builder stage on `base.raw` must not meet those of the prebuilt
    // debian base (same rust target, different C toolchain and headers).
    // And per `[build.env]`: the sweep after each build keeps only what
    // that build used, so a project with its own `RUSTFLAGS` sharing the
    // cache with one without would compile the whole tree again each time
    // the two take turns.
    let builder = if builder_base_out.is_some() { "builder-base" } else { base.raw.as_str() };
    let weft_root = crate::build::resolve_weft_root()?;
    let worker_cache_key = crate::hash::compute_worker_cache_key(&weft_root, builder, cache_tag.as_deref())
        .map_err(|e| BuildError::Build(format!("worker compile cache key: {e}")))?;

    let body = template
        .replace("{{base_image}}", &base.raw)
        .replace("{{builder_base_image}}", builder_base_ref)
        .replace("{{worker_cache_key}}", &worker_cache_key)
        .replace("{{isolate_dir}}", &crate::codegen::isolate_dir_name())
        .replace("{{isolate_candidates}}", crate::codegen::ISOLATE_CANDIDATES_FILE)
        .replace("{{left_out_record}}", crate::codegen::LEFT_OUT_RECORD_FILE)
        .replace("{{left_out_record_path}}", weft_core::builds::LEFT_OUT_RECORD_PATH)
        .replace("{{cache_gc_script}}", CACHE_GC_SCRIPT_NAME)
        .replace("{{cargo_messages}}", CARGO_MESSAGES_PATH)
        .replace(
            "{{install_builder_base}}",
            &render_builder_base(base.manager),
        )
        .replace(
            "{{install_runtime_base}}",
            &render_runtime_base(base.manager),
        )
        .replace(
            "{{install_build_system_packages}}",
            &render_install_line(base.manager, &build_packages),
        )
        .replace(
            "{{install_runtime_system_packages}}",
            &render_install_line(base.manager, &runtime_packages),
        )
        .replace("{{build_env_lines}}", &render_build_env_lines(&build_env))
        .replace("{{binary_name}}", binary_name)
        .replace("{{weft_mount}}", WEFT_MOUNT)
        .replace("{{nodes_mount}}", NODES_MOUNT);

    Ok(WorkerDockerfile {
        body,
        base,
        build_packages,
        runtime_packages,
        build_env,
        builder_base: builder_base_out,
        installs_stages: [
            (NeedStage::Build, "{{install_build_system_packages}}"),
            (NeedStage::Run, "{{install_runtime_system_packages}}"),
        ]
        .into_iter()
        .filter(|(_, token)| template.contains(token))
        .map(|(stage, _)| stage)
        .collect(),
        writes_left_out_record: build.dockerfile_template.is_none(),
    })
}

/// The placeholder the 2026-10-30 (671e50f4) release had every custom
/// template carry before its `cargo build` (`ENV RUSTFLAGS="..."`). weft
/// no longer sets compiler flags for a worker build, so nothing fills it:
/// left in, it would reach cargo as literal text.
const RETIRED_RUSTFLAGS_PLACEHOLDER: &str = "{{worker_rustflags}}";

/// Refuses a custom template that still carries the retired placeholder,
/// naming the line to delete.
fn refuse_retired_placeholder(path: &Path, template: &str) -> BuildResult<()> {
    let Some((number, line)) =
        template.lines().enumerate().find(|(_, line)| line.contains(RETIRED_RUSTFLAGS_PLACEHOLDER))
    else {
        return Ok(());
    };
    Err(BuildError::Build(format!(
        "{}:{}: `{}` uses {RETIRED_RUSTFLAGS_PLACEHOLDER}, which weft no longer fills (worker builds take no \
         flags from weft any more): delete that line",
        path.display(),
        number + 1,
        line.trim()
    )))
}

impl WorkerDockerfile {
    /// Of `needs` (what each node type the image holds needs), the ones
    /// the image does not install, which the machine its worker runs on
    /// has to meet: a need with no packages for the base image's manager
    /// and distro, or of a stage whose install line the template leaves
    /// out.
    pub fn left_to_machine(&self, mut needs: weft_core::needs::NodeNeeds) -> weft_core::needs::NodeNeeds {
        let distro = (!self.base.distro_key.is_empty()).then_some(self.base.distro_key.as_str());
        for list in needs.values_mut() {
            list.retain(|need| !self.installs_stages.contains(&need.stage) || need.packages_for(self.base.manager, distro).is_none());
        }
        needs.retain(|_, list| !list.is_empty());
        needs
    }
}

/// What a builder stage needs beyond the toolchain to compile a node
/// set on the default (debian) base: the union of the packages of the
/// nodes' build-stage needs and their `[build.env]`, `{{catalog_path}}`
/// already expanded to the in-container node path.
pub struct BuilderStage {
    pub packages: Vec<String>,
    pub env: std::collections::BTreeMap<String, String>,
}

impl BuilderStage {
    /// The builder stage of `referenced`, whose needs are `needs`.
    pub fn for_nodes(
        catalog: &FsCatalog,
        referenced: &BTreeSet<String>,
        needs: &NodeNeeds,
        project_root: &Path,
    ) -> BuildResult<Self> {
        let base = default_base_image();
        Ok(Self {
            packages: collect_stage_packages(needs, &base, NeedStage::Build)?,
            env: collect_build_env(catalog, referenced, &|node_type, source_dir| {
                node_catalog_path(node_type, source_dir, project_root)
            })?
            .vars,
        })
    }

    /// The Dockerfile lines that install the packages and export the
    /// environment, for the default base's package manager.
    pub fn render(&self) -> (String, String) {
        (
            render_install_line(default_base_image().manager, &self.packages),
            render_build_env_lines(&self.env),
        )
    }
}

/// The union of packages to install for the image's manager and distro
/// from the needs of `stage` in `needs`. Per need: distro_key match, else
/// `default`, else an error if the need had entries for this manager but
/// neither resolved. A need with no entry for this manager contributes
/// nothing.
fn collect_stage_packages(needs: &NodeNeeds, base: &BaseImage, stage: NeedStage) -> BuildResult<Vec<String>> {
    let distro = (!base.distro_key.is_empty()).then_some(base.distro_key.as_str());
    let mut packages: BTreeSet<String> = BTreeSet::new();
    for (node_type, list) in needs {
        for need in list.iter().filter(|need| need.stage == stage) {
            let Some(table) = need.install.get(&base.manager) else {
                continue;
            };
            let Some(list) = need.packages_for(base.manager, distro) else {
                let keys: Vec<&String> = table.keys().collect();
                let manager = manager_key(base.manager);
                return Err(BuildError::Build(format!(
                    "node '{node_type}' needs '{}' with {manager} packages for {keys:?} \
                     but none matches the project's base image '{}' (distro key '{}') \
                     and no 'default' entry is set. Add `default = [...]` or \
                     `{} = [...]` under [needs.install.{manager}] in the node's deps.toml.",
                    need.name, base.raw, base.distro_key, base.distro_key,
                )));
            };
            packages.extend(list.iter().cloned());
        }
    }
    Ok(packages.into_iter().collect())
}

/// The `[build.env]` of a build's nodes, merged ([`collect_build_env`]).
pub(crate) struct NodesBuildEnv {
    /// What reaches cargo, `{{catalog_path}}` expanded.
    pub vars: std::collections::BTreeMap<String, String>,
    /// The node types that set each variable (one value, however many).
    pub set_by: std::collections::BTreeMap<String, BTreeSet<String>>,
    /// What tells this build's compile caches apart from the ones every
    /// build whose nodes set nothing shares: a short hash of each variable,
    /// the node type that sets it, and its value as that node declares it;
    /// `None` when the nodes declare none. Two builds share a tagged cache
    /// only when the same node types declare the same values.
    ///
    /// Every variable counts, not only the ones cargo reads itself. Cargo
    /// names the files of a unit compiled with other `RUSTFLAGS` or
    /// `CARGO_PROFILE_*` values by another hash, so they sit beside the
    /// others, but a cache swept down to what its last build used (a worker
    /// image's) loses them at the next build without them. And a crate
    /// whose build script watches a variable (`cargo:rerun-if-env-changed`,
    /// as the `cc` crate does for `CC` and `CFLAGS`, and `pkg-config` for
    /// `PKG_CONFIG_PATH`) compiles again in place, with every crate above
    /// it, each time the value changes. No list can name every variable a
    /// build script watches.
    ///
    /// The values are hashed as declared, `{{catalog_path}}` unexpanded: a
    /// host build expands it to the node's folder in its package crate,
    /// named by the node's content, so a hash of the expansion would start
    /// a new cache at every edit of the node and leave the old one behind.
    /// The setter is hashed so that one template two different nodes
    /// declare, which expands to two different folders, never shares a
    /// cache. Two builds of one node type at different contents (or, in an
    /// image, at different places in their projects) still share one, and
    /// see two expansions in it: the crate whose build script watches the
    /// variable compiles again in place, as it does after any edit of that
    /// node.
    pub cache_tag: Option<String>,
}

/// Collect and substitute `[build.env]` across referenced nodes.
/// `{{catalog_path}}` expands to `node_path(node_type, source_dir)`: the
/// node's directory where the build compiles it (its in-container path
/// under `NODES_MOUNT` for an image, its copy in the machine-wide crate
/// folder for a host build). Conflicts on the same variable abort the
/// build.
pub(crate) fn collect_build_env(
    catalog: &FsCatalog,
    referenced: &BTreeSet<String>,
    node_path: &dyn Fn(&str, &Path) -> BuildResult<String>,
) -> BuildResult<NodesBuildEnv> {
    let mut merged: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    // Each variable's first setter and its value as that node declares it.
    let mut declared: std::collections::BTreeMap<String, (String, String)> =
        std::collections::BTreeMap::new();
    let mut set_by: std::collections::BTreeMap<String, BTreeSet<String>> = std::collections::BTreeMap::new();

    for node_type in referenced {
        let Some(deps) = catalog
            .deps(node_type)
            .map_err(|e| BuildError::Build(format!("load deps for {node_type}: {e}")))?
        else {
            continue;
        };
        if deps.build.env.is_empty() {
            continue;
        }
        let Some(source_dir) = catalog.source_dir(node_type) else {
            return Err(BuildError::Build(format!(
                "node '{node_type}' declares [build.env] but has no source dir"
            )));
        };
        let catalog_path = node_path(node_type, source_dir)?;

        for (k, v) in deps.build.env.iter() {
            let resolved = v.replace("{{catalog_path}}", &catalog_path);
            if let Some(existing) = merged.get(k) {
                if existing != &resolved {
                    let other = declared.get(k).map(|(setter, _)| setter.clone()).unwrap_or_default();
                    return Err(BuildError::Build(format!(
                        "[build.env] conflict on '{k}': '{other}' sets '{existing}', \
                         '{node_type}' sets '{resolved}'"
                    )));
                }
            } else {
                merged.insert(k.clone(), resolved.clone());
                declared.insert(k.clone(), (node_type.clone(), v.clone()));
            }
            set_by.entry(k.clone()).or_default().insert(node_type.clone());
        }
    }
    Ok(NodesBuildEnv { cache_tag: build_env_cache_tag(&declared), vars: merged, set_by })
}

/// [`NodesBuildEnv::cache_tag`] of each variable's setter and its value as
/// that node declares it.
fn build_env_cache_tag(declared: &std::collections::BTreeMap<String, (String, String)>) -> Option<String> {
    use sha2::{Digest, Sha256};
    if declared.is_empty() {
        return None;
    }
    let mut hasher = Sha256::new();
    for (name, (setter, value)) in declared {
        for part in [name, setter, value] {
            hasher.update(part.as_bytes());
            hasher.update(b"\0");
        }
    }
    Some(hasher.finalize().iter().take(8).map(|b| format!("{b:02x}")).collect())
}

/// Compute the in-container path for a node's source dir. The docker
/// build stages every referenced node under `NODES_MOUNT` at its
/// project-relative path (`nodes/...`, or `src/...` for a node beside
/// the code); the node's in-container path is that same path.
fn node_catalog_path(
    node_type: &str,
    source_dir: &Path,
    project_root: &Path,
) -> BuildResult<String> {
    let rel = source_dir.strip_prefix(project_root).map_err(|_| {
        BuildError::Build(format!(
            "node '{node_type}' source dir {} is not under the project root {}",
            source_dir.display(),
            project_root.display()
        ))
    })?;
    Ok(format!("{NODES_MOUNT}/{}", rel.display()))
}

/// Parse a base-image string into a `BaseImage`. Recognizes the
/// Docker Hub tag conventions for the families we support.
pub fn parse_base_image(raw: &str) -> Option<BaseImage> {
    let (image, tag) = match raw.rsplit_once(':') {
        Some((i, t)) => (i.to_string(), t.to_string()),
        None => (raw.to_string(), String::new()),
    };
    let image_lc = image.to_ascii_lowercase();
    let tag_lc = tag.to_ascii_lowercase();

    if image_lc.ends_with("debian") || image_lc.contains("debian") {
        let version = debian_tag_to_version(&tag_lc);
        let distro_key = version
            .map(|v| format!("debian_{v}"))
            .unwrap_or_default();
        return Some(BaseImage {
            raw: raw.to_string(),
            manager: PackageManager::Apt,
            distro_key,
        });
    }
    if image_lc.ends_with("ubuntu") || image_lc.contains("ubuntu") {
        let version = ubuntu_tag_to_version(&tag_lc);
        let distro_key = version
            .map(|v| format!("ubuntu_{v}"))
            .unwrap_or_default();
        return Some(BaseImage {
            raw: raw.to_string(),
            manager: PackageManager::Apt,
            distro_key,
        });
    }

    if image_lc.ends_with("alpine") || image_lc.contains("alpine") {
        let version = alpine_tag_to_version(&tag_lc);
        let distro_key = version
            .map(|v| format!("alpine_{v}"))
            .unwrap_or_default();
        return Some(BaseImage {
            raw: raw.to_string(),
            manager: PackageManager::Apk,
            distro_key,
        });
    }

    for (needle, key_prefix) in [
        ("rockylinux", "rocky"),
        ("rocky", "rocky"),
        ("almalinux", "alma"),
        ("centos", "centos"),
        ("fedora", "fedora"),
        ("amazonlinux", "amazonlinux"),
        ("oraclelinux", "oracle"),
        ("rhel", "rhel"),
    ] {
        if image_lc.contains(needle) {
            let distro_key = rhel_family_version(&tag_lc)
                .map(|v| format!("{key_prefix}_{v}"))
                .unwrap_or_default();
            return Some(BaseImage {
                raw: raw.to_string(),
                manager: PackageManager::Dnf,
                distro_key,
            });
        }
    }

    if image_lc.contains("homebrew") || image_lc.contains("/brew") {
        return Some(BaseImage {
            raw: raw.to_string(),
            manager: PackageManager::Brew,
            distro_key: String::new(),
        });
    }

    if image_lc.starts_with("python") {
        // A tag naming no release (`3.15-slim`) follows whichever Debian
        // the image's maintainers moved it to, so no distro is guessed.
        let distro_key = if tag_lc.contains("trixie") {
            "debian_13".to_string()
        } else if tag_lc.contains("bookworm") {
            "debian_12".to_string()
        } else if tag_lc.contains("bullseye") {
            "debian_11".to_string()
        } else if tag_lc.contains("alpine") {
            "alpine_3".to_string()
        } else {
            String::new()
        };
        let manager = if tag_lc.contains("alpine") {
            PackageManager::Apk
        } else {
            PackageManager::Apt
        };
        return Some(BaseImage {
            raw: raw.to_string(),
            manager,
            distro_key,
        });
    }

    None
}

/// [`DEFAULT_BASE_IMAGE`], a family weft knows.
fn default_base_image() -> BaseImage {
    parse_base_image(DEFAULT_BASE_IMAGE).expect("the default base image is a family weft knows")
}

/// The base image a project's worker image is built on, and how needs
/// are installed on it: its family read off its name ([`parse_base_image`]),
/// with `[build.worker] package_manager` deciding the manager when it is
/// set. An image of no family weft knows, with no package manager named,
/// is refused: weft would have to guess how to install a need on it.
pub fn base_image_of(build: &WorkerBuildSection) -> BuildResult<BaseImage> {
    let raw = build.base_image.clone().unwrap_or_else(|| DEFAULT_BASE_IMAGE.to_string());
    match (parse_base_image(&raw), build.package_manager) {
        (Some(base), None) => Ok(base),
        (Some(base), Some(manager)) => Ok(BaseImage { manager, ..base }),
        (None, Some(manager)) => Ok(BaseImage { raw, manager, distro_key: String::new() }),
        (None, None) => Err(BuildError::Build(format!(
            "weft.toml's [build.worker] base_image {raw:?} is of no family weft knows how to install a node's needs on \
             (debian, ubuntu, alpine, the rhel family, python, homebrew); name its package manager with \
             [build.worker] package_manager = \"apt\" (or \"dnf\", \"apk\", \"brew\")"
        ))),
    }
}

fn debian_tag_to_version(tag: &str) -> Option<&'static str> {
    if tag.is_empty() || tag == "latest" {
        return Some("13");
    }
    for (codename, major) in [
        ("bookworm", "12"),
        ("bullseye", "11"),
        ("buster", "10"),
        ("trixie", "13"),
    ] {
        if tag.contains(codename) {
            return Some(major);
        }
    }
    for major in ["10", "11", "12", "13"] {
        if tag.starts_with(major) {
            return Some(match major {
                "10" => "10",
                "11" => "11",
                "12" => "12",
                "13" => "13",
                _ => unreachable!(),
            });
        }
    }
    None
}

fn ubuntu_tag_to_version(tag: &str) -> Option<String> {
    if tag.is_empty() || tag == "latest" {
        return Some("24_04".into());
    }
    for (codename, ver) in [
        ("noble", "24_04"),
        ("jammy", "22_04"),
        ("focal", "20_04"),
        ("mantic", "23_10"),
    ] {
        if tag.contains(codename) {
            return Some(ver.into());
        }
    }
    let mut parts = tag.split(|c: char| !(c.is_ascii_digit() || c == '.'));
    if let Some(numeric) = parts.next() {
        let mut chunks = numeric.split('.');
        let maj = chunks.next()?;
        let min = chunks.next()?;
        if !maj.is_empty() && !min.is_empty() {
            return Some(format!("{maj}_{min}"));
        }
    }
    None
}

fn alpine_tag_to_version(tag: &str) -> Option<String> {
    if tag.is_empty() || tag == "latest" {
        return Some("3".into());
    }
    if tag.starts_with('3') {
        let mut parts = tag.split('.');
        let maj = parts.next()?;
        if let Some(min) = parts.next() {
            return Some(format!("{maj}_{min}"));
        }
        return Some(maj.into());
    }
    None
}

fn rhel_family_version(tag: &str) -> Option<&'static str> {
    for major in ["7", "8", "9", "10"] {
        if tag == major || tag.starts_with(&format!("{major}.")) || tag.starts_with(&format!("{major}-")) {
            return Some(match major {
                "7" => "7",
                "8" => "8",
                "9" => "9",
                "10" => "10",
                _ => unreachable!(),
            });
        }
    }
    None
}

/// The `cargo build` RUN block shared by both built-in templates.
///
/// The speedup mechanism (see `worker-builder-base.Dockerfile`): the
/// builder base has already compiled the STOCK WORKER `--release` into
/// `/weft/target`: the engine workspace, every dependency the fixed set
/// and the stdlib packages pull in, and one package crate per stdlib
/// package, emitted at the same paths this build uses and pinned by
/// the SAME `Cargo.lock`. So this per-project build:
///
/// - on a builder that keeps nothing between builds (a cloud's, where
///   every build runs on a fresh machine, which says so with
///   [`COMPILE_CACHE_ARG`]`=none`) compiles in the baked `/weft/target`
///   itself: seeding a cache that is thrown away would copy the whole
///   baked target (gigabytes) on every build for nothing, and the sweep,
///   which only tends a cache kept for later builds, does not run;
/// - on a builder that keeps its BuildKit caches (`weft build-images`
///   run on one machine again and again) compiles into a persistent
///   cache per `{{worker_cache_key}}` (`hash::compute_worker_cache_key`:
///   the build environment, the builder and the nodes' `[build.env]`),
///   seeded ONCE from the baked
///   `/weft/target` (the marker file records a complete seed; a build
///   interrupted mid-copy seeds again). After that, cargo's own
///   fingerprints decide what compiles: a `pkg_<name>` crate whose
///   sources and generated shim are older than its cached rlib is reused
///   as is, whatever project asked for it. Reuse across projects rests on
///   each package crate living at a path named by its content
///   (`/work/pkg_<name>-<slot>`, `codegen::write_package_crates`: same
///   sources, same slot, shared rlib; an edited copy of a stock node gets
///   a slot of its own and can never be mistaken for the stock one) and
///   on every staged package file carrying the date its content gives it
///   (`build::package_source_mtime`), so a package compiled from another
///   checkout still counts. After every such build, [`CACHE_GC_SCRIPT`]
///   drops what that build did not use; the mount is `sharing=locked`, so
///   a second build waits for the first rather than race it;
/// - seeds the worker crate's `Cargo.lock` from `{{seed_lock}}`: on the
///   builder base, [`BASE_WORKER_LOCK`], the lock the stock worker
///   resolved to, so every crate the base compiled resolves to the SAME
///   version here and cargo only adds what the project's own nodes
///   bring (a build with nothing new never fetches the crates.io
///   index); without a base, the workspace lock at `/weft/Cargo.lock`.
///   A different resolution for a shared crate would re-fingerprint and
///   recompile the whole tree, defeating the reuse.
///
/// The cache mounts at `/cache/target`, never over `/weft/target`: a
/// mount there would shadow the baked layer the seed copies from. The
/// registry cache mount keeps the crates.io sources a build fetched
/// (immutable per version), so a builder downloads a crate once.
///
/// `cargo build` (NOT `--locked`): the worker manifest carries extra
/// path deps (the `pkg_<node>` crates) absent from the seeded
/// workspace lock, so cargo must be allowed to EXTEND the lock with
/// those. Seeding the lock pins the shared set; cargo appends the rest.
///
/// The build itself is `weft-isolate` (`crate::worker_isolate`), compiled
/// first from the package codegen put at `/work/{{isolate_dir}}` with the
/// same seeded lock (so its serde resolves to the version already
/// compiled, and nothing fetches the index): it runs `cargo build
/// --release --keep-going` in `/work`, and when package crates fail it
/// leaves them out (each package is an optional dependency behind its own
/// feature), writes their record to `/work/{{left_out_record}}`, and
/// builds the worker again with only the healthy features. The record is
/// what the runtime stage copies into the image. The helper is one fixed
/// package for every project, so after a cache's first build it is a
/// cache hit; a failure no package explains still fails the build.
///
/// Both builds of the helper and every build it runs write cargo's JSON
/// messages to [`CARGO_MESSAGES_PATH`]: the list of every unit the build
/// used, which is how the cache sweep after it ([`CACHE_GC_SCRIPT`])
/// knows what to mark used.
const CARGO_BUILD_RUN_FRAGMENT: &str = concat!(
    "ENV CARGO_TARGET_DIR=/cache/target\n",
    "ARG WEFT_COMPILE_CACHE=kept\n",
    "RUN --mount=type=cache,id=weft-worker-cargo-registry,target=/root/.cargo/registry,sharing=locked \\\n",
    "    --mount=type=cache,id=weft-worker-target-{{worker_cache_key}},target=/cache/target,sharing=locked \\\n",
    "    if [ \"$WEFT_COMPILE_CACHE\" = none ] && [ -d /weft/target ]; then export CARGO_TARGET_DIR=/weft/target; \\\n",
    "    elif ! [ -f /cache/target/.weft-seeded ] && [ -d /weft/target ]; then \\\n",
    "      cp -a /weft/target/. /cache/target/ && touch /cache/target/.weft-seeded; fi \\\n",
    "    && cp {{seed_lock}} /work/Cargo.lock \\\n",
    "    && cp {{seed_lock}} /work/{{isolate_dir}}/Cargo.lock \\\n",
    "    && cargo build --release --manifest-path /work/{{isolate_dir}}/Cargo.toml --message-format=json-render-diagnostics > {{cargo_messages}} \\\n",
    "    && \"$CARGO_TARGET_DIR\"/release/weft-isolate /work/{{isolate_candidates}} /work/{{left_out_record}} {{cargo_messages}} \\\n",
    "    && cp \"$CARGO_TARGET_DIR\"/release/{{binary_name}} /worker \\\n",
    "    && ( [ \"$CARGO_TARGET_DIR\" != /cache/target ] \\\n",
    "         || sh /work/{{cache_gc_script}} this-build /cache/target/release {{cargo_messages}} \\\n",
    "         || echo 'weft: the compile cache sweep failed; the build is unaffected' >&2 )\n",
);

/// The build-arg a builder that keeps nothing between builds sets to
/// `none` (see [`CARGO_BUILD_RUN_FRAGMENT`]).
// SYNC: COMPILE_CACHE_ARG <-> the `ARG WEFT_COMPILE_CACHE` line of CARGO_BUILD_RUN_FRAGMENT above, crates/weft-platform-gcp/src/images.rs (COMPILE_CACHE_ARG)
pub const COMPILE_CACHE_ARG: &str = "WEFT_COMPILE_CACHE";

/// Where the builder base keeps the lock its stock worker resolved to,
/// which a per-project build on that base starts from (see
/// [`CARGO_BUILD_RUN_FRAGMENT`]).
// SYNC: BASE_WORKER_LOCK <-> deploy/docker/worker-builder-base.Dockerfile (worker.Cargo.lock)
pub const BASE_WORKER_LOCK: &str = "/weft/worker.Cargo.lock";

/// What a build without the builder base seeds its lock from: the
/// workspace lock its own `COPY weft/` brings.
const WORKSPACE_LOCK: &str = "/weft/Cargo.lock";

/// The id prefix of the compile cache mounts, as `docker buildx du`
/// reports them (`with id "/weft-worker-target-<key>"`, followed by
/// `-<lane>` for one an older weft made): what a size report and a sweep
/// look for.
pub const WORKER_CACHE_MOUNT_ID_PREFIX: &str = "weft-worker-target-";

/// File name of [`CACHE_GC_SCRIPT`] inside the generated crate (`/work`
/// in the builder stage).
pub const CACHE_GC_SCRIPT_NAME: &str = "weft-cache-gc.sh";

/// Where a worker build writes cargo's JSON messages for the sweep to
/// read ([`CARGO_BUILD_RUN_FRAGMENT`]). Outside `/work`: a file written
/// into the worker crate's directory during its build would count as a
/// change to it for a build script that watches the whole package.
pub const CARGO_MESSAGES_PATH: &str = "/tmp/weft-cargo-messages.json";

/// The sweep of an image build's compile cache: it reads what the build
/// just used off cargo's JSON messages (every unit, the ones cargo reused
/// untouched included) and drops everything else, so a cache one build
/// owns (the runtime image's, the builder base's, a worker image's kept by
/// `weft build-images`) holds exactly one build's worth. Every
/// built-in-template worker image build that keeps its cache runs it
/// inside the mount, after the binary is copied out (a custom
/// `dockerfile_template` writes its own RUN and gets none of this). It can
/// never fail a build. A worker compiled on the machine is swept by
/// `weft_platform_local::host_build` instead, by what the programs on the
/// machine were built from.
// SYNC: the script's path <-> deploy/docker/runtime.Dockerfile (the sweep after `cargo build`), hash::CACHE_GC_SCRIPT_PATH
pub const CACHE_GC_SCRIPT: &str = include_str!("weft-cache-gc.sh");

/// The runtime stage shared by both built-in templates: install
/// runtime-only packages onto the user's base image, copy the
/// compiled binary from the builder, and the record of the packages its
/// build left out, where the install reads it from the image.
// SYNC: the record's COPY line <-> crates/weft-core/src/builds.rs LEFT_OUT_RECORD_PATH
const RUNTIME_STAGE_FRAGMENT: &str = concat!(
    "FROM {{base_image}}\n",
    "\n",
    "{{install_runtime_base}}",
    "{{install_runtime_system_packages}}",
    "\n",
    "COPY --from=builder /worker /usr/local/bin/worker\n",
    "COPY --from=builder /work/{{left_out_record}} {{left_out_record_path}}\n",
    "ENTRYPOINT [\"/usr/local/bin/worker\"]\n",
);

/// Built-in multi-stage template. Used when no custom template is
/// supplied AND the runtime base is non-debian (so the builder ABI must
/// match). Installs rustup + build tools inside the builder stage.
///
/// Stage 1 (`builder`): installs build-time packages + rust via
/// rustup, copies the generated crate + referenced catalog
/// subfolders, builds it with `weft-isolate`
/// ([`CARGO_BUILD_RUN_FRAGMENT`]), writes the binary to `/worker`.
///
/// Stage 2 (runtime): `RUNTIME_STAGE_FRAGMENT`.
fn default_template() -> String {
    [
        concat!(
            "FROM {{base_image}} AS builder\n",
            "\n",
            "# Always-present builder toolchain: every cargo build needs\n",
            "# a C compiler, linker, and curl/ca-certificates to fetch\n",
            "# rustup. Node-specific build packages get appended below.\n",
            "{{install_builder_base}}",
            "{{install_build_system_packages}}",
            "\n",
            "# Install rustup with NO default toolchain: the generated\n",
            "# crate carries a `rust-toolchain.toml` (copied from the weft\n",
            "# workspace root, the single source of truth), so the first\n",
            "# cargo invocation in /work auto-installs + selects the pinned\n",
            "# toolchain. Nothing here names a version.\n",
            "RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \\\n",
            "    | sh -s -- -y --default-toolchain none --profile minimal\n",
            "ENV PATH=\"/root/.cargo/bin:${PATH}\"\n",
            "\n",
            "WORKDIR /work\n",
            "COPY build/ /work/\n",
            "COPY weft/ {{weft_mount}}/\n",
            "COPY project-nodes/ {{nodes_mount}}/\n",
            "\n",
            "{{build_env_lines}}",
            "\n",
        ),
        &CARGO_BUILD_RUN_FRAGMENT.replace("{{seed_lock}}", WORKSPACE_LOCK),
        "\n",
        RUNTIME_STAGE_FRAGMENT,
    ]
    .concat()
}

/// Multi-stage template that FROMs the shared pre-built builder
/// base. The base image already has debian build packages, rustup
/// with the workspace's pinned toolchain installed, and the
/// whole workspace COMPILED `--release` into `/weft/target`. The
/// builder stage here adds only what's project-specific: per-node
/// system build packages, the generated worker crate, and the
/// project's node source (`nodes/` and `src/`). The base's `/weft/` directory
/// provides the engine workspace via path deps AND its precompiled
/// rlibs; no per-project COPY or recompile of the workspace. Target
/// dir + Cargo.lock mechanics are documented on
/// `CARGO_BUILD_RUN_FRAGMENT`.
fn prebuilt_base_template() -> String {
    [
        concat!(
            "FROM {{builder_base_image}} AS builder\n",
            "\n",
            "# Node-specific build packages. Base packages (build-essential,\n",
            "# ca-certificates, curl, pkg-config) are baked into the\n",
            "# builder base; only the per-node extras get installed here.\n",
            "{{install_build_system_packages}}",
            "\n",
            "WORKDIR /work\n",
            "COPY build/ /work/\n",
            "COPY project-nodes/ {{nodes_mount}}/\n",
            "\n",
            "{{build_env_lines}}",
            "\n",
        ),
        &CARGO_BUILD_RUN_FRAGMENT.replace("{{seed_lock}}", BASE_WORKER_LOCK),
        "\n",
        RUNTIME_STAGE_FRAGMENT,
    ]
    .concat()
}

/// Render `ENV K=V` lines for the builder stage. One per entry,
/// stable ordering from the sorted map. Returns empty when there
/// are no entries so the template collapses cleanly.
fn render_build_env_lines(env: &std::collections::BTreeMap<String, String>) -> String {
    if env.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    for (k, v) in env {
        out.push_str(&format!("ENV {k}={v}\n"));
    }
    out
}

/// Builder-stage baseline. Every cargo build needs a C compiler
/// + linker + ca-certificates + curl (for rustup). These are
/// independent of any node's declarations. Distro-specific
/// package names only.
fn render_builder_base(manager: PackageManager) -> String {
    match manager {
        PackageManager::Apt => concat!(
            "RUN apt-get update \\\n",
            "    && apt-get install -y --no-install-recommends \\\n",
            "       ca-certificates curl build-essential \\\n",
            "    && rm -rf /var/lib/apt/lists/*\n",
        )
        .to_string(),
        PackageManager::Apk => "RUN apk add --no-cache ca-certificates curl build-base\n"
        .to_string(),
        // `yum`, not `dnf`, inside an image: every RHEL-family image has
        // it (on 8 and later it is dnf under its old name), and an older
        // one has nothing else.
        PackageManager::Dnf => concat!(
            "RUN yum install -y ca-certificates curl gcc gcc-c++ make \\\n",
            "    && yum clean all\n",
        )
        .to_string(),
        // Homebrew base images already have the toolchain.
        PackageManager::Brew => String::new(),
    }
}

/// Runtime-stage baseline. ca-certificates is nearly universal
/// (anything talking HTTPS needs it). Keep the final image slim
/// otherwise.
fn render_runtime_base(manager: PackageManager) -> String {
    match manager {
        PackageManager::Apt => concat!(
            "RUN apt-get update \\\n",
            "    && apt-get install -y --no-install-recommends ca-certificates \\\n",
            "    && rm -rf /var/lib/apt/lists/*\n",
        )
        .to_string(),
        PackageManager::Apk => "RUN apk add --no-cache ca-certificates\n".to_string(),
        PackageManager::Dnf => "RUN yum install -y ca-certificates \\\n    && yum clean all\n"
        .to_string(),
        PackageManager::Brew => String::new(),
    }
}

/// Render the `RUN ... install ...` line for the chosen manager.
/// Empty string (no trailing newline) when the package list is
/// empty so the template collapses cleanly.
fn render_install_line(manager: PackageManager, packages: &[String]) -> String {
    if packages.is_empty() {
        return String::new();
    }
    let joined = packages.join(" ");
    match manager {
        PackageManager::Apt => format!(
            "RUN apt-get update \\\n    && apt-get install -y --no-install-recommends {joined} \\\n    && rm -rf /var/lib/apt/lists/*\n",
        ),
        PackageManager::Apk => format!("RUN apk add --no-cache {joined}\n"),
        PackageManager::Dnf => format!(
            "RUN yum install -y {joined} \\\n    && yum clean all\n",
        ),
        PackageManager::Brew => format!("RUN brew install {joined}\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nothing_baked() -> BuilderStage {
        BuilderStage { packages: Vec::new(), env: Default::default() }
    }

    /// The `[build.env]` tag tells builds apart by what their nodes
    /// declare and which node declares it: none when they declare nothing,
    /// another for another value, another for the same template another
    /// node declares (it expands to that node's folder), the same wherever
    /// the build puts the node; and a project's worker image compiles in
    /// the cache its tag names.
    #[test]
    fn the_build_env_names_the_compile_cache_by_what_the_nodes_declare() {
        let temp = tempfile::tempdir().unwrap();
        for (dir, ty, env) in [
            ("plain", "Plain", None),
            ("native", "Native", Some("RUSTFLAGS = \"-C target-cpu=native\"")),
            ("v3", "V3", Some("RUSTFLAGS = \"-C target-cpu=x86-64-v3\"")),
            ("vips", "Vips", Some("PKG_CONFIG_PATH = \"{{catalog_path}}/pkgconfig\"")),
            ("heif", "Heif", Some("PKG_CONFIG_PATH = \"{{catalog_path}}/pkgconfig\"")),
        ] {
            let at = temp.path().join("nodes").join(dir);
            std::fs::create_dir_all(&at).unwrap();
            std::fs::write(at.join("metadata.json"), format!(r#"{{"type": "{ty}", "label": "{ty}", "description": "d", "inputs": [], "outputs": []}}"#)).unwrap();
            std::fs::write(at.join("mod.rs"), "// impl\n").unwrap();
            if let Some(env) = env {
                std::fs::write(at.join("deps.toml"), format!("[build.env]\n{env}\n")).unwrap();
            }
        }
        let catalog = FsCatalog::discover(&temp.path().join("nodes")).unwrap();
        let referenced = |ty: &str| BTreeSet::from([ty.to_string()]);
        let tag = |ty: &str, at: &str| collect_build_env(&catalog, &referenced(ty), &|_, _| Ok(at.to_string())).unwrap().cache_tag;
        assert_eq!(tag("Plain", "/a"), None);
        let (native, v3, vips) = (tag("Native", "/a").unwrap(), tag("V3", "/a").unwrap(), tag("Vips", "/a").unwrap());
        assert!(native != v3 && native != vips && v3 != vips);
        assert_eq!(tag("Vips", "/b").unwrap(), vips, "where the node sits is no part of it");
        assert_ne!(tag("Heif", "/a").unwrap(), vips, "one template, declared by two nodes, names two folders");

        let mount = |ty: &str| {
            let build = WorkerBuildSection { base_image: None, dockerfile_template: None, package_manager: None };
            let out = emit(&build, temp.path(), &catalog, &referenced(ty), &NodeNeeds::new(), "worker_test", "weft-builder-base:cafebabe", &nothing_baked()).unwrap();
            let at = out.body.find(WORKER_CACHE_MOUNT_ID_PREFIX).unwrap() + WORKER_CACHE_MOUNT_ID_PREFIX.len();
            out.body[at..at + 16].to_string()
        };
        assert_eq!(mount("Plain"), crate::hash::compute_worker_cache_key(&crate::build::resolve_weft_root().unwrap(), "builder-base", None).unwrap());
        assert_ne!(mount("Native"), mount("Plain"));
        assert_ne!(mount("Native"), mount("V3"));
    }

    #[test]
    fn debian_bookworm_slim_parses_to_debian_12() {
        let b = parse_base_image("debian:bookworm-slim").unwrap();
        assert_eq!(b.manager, PackageManager::Apt);
        assert_eq!(b.distro_key, "debian_12");
    }

    #[test]
    fn debian_trixie_parses_to_debian_13() {
        let b = parse_base_image("debian:trixie-slim").unwrap();
        assert_eq!(b.distro_key, "debian_13");
    }

    #[test]
    fn debian_numeric_tag_parses_to_debian_12() {
        let b = parse_base_image("debian:12-slim").unwrap();
        assert_eq!(b.distro_key, "debian_12");
    }

    #[test]
    fn ubuntu_numeric_parses() {
        let b = parse_base_image("ubuntu:22.04").unwrap();
        assert_eq!(b.distro_key, "ubuntu_22_04");
    }

    #[test]
    fn alpine_parses() {
        let b = parse_base_image("alpine:3.19").unwrap();
        assert_eq!(b.manager, PackageManager::Apk);
        assert_eq!(b.distro_key, "alpine_3_19");
    }

    #[test]
    fn rocky_linux_parses() {
        let b = parse_base_image("rockylinux:9-minimal").unwrap();
        assert_eq!(b.manager, PackageManager::Dnf);
        assert_eq!(b.distro_key, "rocky_9");
    }

    #[test]
    fn python_image_uses_debian_under_the_hood() {
        let b = parse_base_image("python:3.13-slim-bookworm").unwrap();
        assert_eq!(b.manager, PackageManager::Apt);
        assert_eq!(b.distro_key, "debian_12");
        let unnamed = parse_base_image("python:3.15-slim").unwrap();
        assert_eq!((unnamed.manager, unnamed.distro_key.as_str()), (PackageManager::Apt, ""));
    }

    /// An image of no family weft knows is refused unless the project
    /// names its package manager, which then decides; a known one keeps
    /// its distro with the manager named.
    #[test]
    fn an_unknown_image_needs_its_package_manager_named() {
        let build = |base: &str, manager| WorkerBuildSection { base_image: Some(base.into()), dockerfile_template: None, package_manager: manager };
        assert!(parse_base_image("registry.example.com/my-org/my-base:abc123").is_none());
        let err = base_image_of(&build("registry.example.com/my-org/my-base:abc123", None)).unwrap_err().to_string();
        assert!(err.contains("package_manager"), "{err}");
        let named = base_image_of(&build("registry.example.com/my-org/my-base:abc123", Some(PackageManager::Apk))).unwrap();
        assert_eq!((named.manager, named.distro_key.as_str()), (PackageManager::Apk, ""));
        let known = base_image_of(&build("debian:bookworm-slim", Some(PackageManager::Apt))).unwrap();
        assert_eq!(known.distro_key, "debian_12");
    }

    #[test]
    fn empty_packages_produces_no_install_line() {
        assert_eq!(render_install_line(PackageManager::Apt, &[]), "");
    }

    #[test]
    fn apt_install_line_just_carries_declared_packages() {
        let line = render_install_line(PackageManager::Apt, &["libpython3.11".into()]);
        assert!(line.contains(" libpython3.11 "));
        assert!(!line.contains("ca-certificates"));
    }

    #[test]
    fn apt_builder_base_has_compiler_and_curl() {
        let base = render_builder_base(PackageManager::Apt);
        assert!(base.contains("build-essential"));
        assert!(base.contains("ca-certificates"));
        assert!(base.contains("curl"));
    }

    #[test]
    fn apt_runtime_base_has_only_ca_certs() {
        let base = render_runtime_base(PackageManager::Apt);
        assert!(base.contains("ca-certificates"));
        assert!(!base.contains("build-essential"));
    }

    #[test]
    fn build_env_lines_render_alphabetically() {
        let mut env = std::collections::BTreeMap::new();
        env.insert("B_VAR".into(), "two".into());
        env.insert("A_VAR".into(), "one".into());
        let out = render_build_env_lines(&env);
        assert_eq!(out, "ENV A_VAR=one\nENV B_VAR=two\n");
    }

    #[test]
    fn build_env_lines_empty_when_no_entries() {
        let env = std::collections::BTreeMap::new();
        assert_eq!(render_build_env_lines(&env), "");
    }

    /// The default + debian-family runtime resolves to the prebuilt
    /// template: builder stage FROMs `weft-builder-base:<tag>`, does
    /// NOT install rustup, does NOT COPY weft/, builds with the
    /// cargo cache mount.
    #[test]
    fn prebuilt_base_template_skips_rustup_and_weft_copy() {
        let body = prebuilt_base_template();
        assert!(
            body.contains("FROM {{builder_base_image}} AS builder"),
            "builder FROMs the prebuilt base: {body}"
        );
        assert!(
            !body.contains("curl --proto"),
            "prebuilt template must not re-install rustup: {body}"
        );
        assert!(
            !body.contains("COPY weft/"),
            "prebuilt template must not COPY weft/ (base ships it): {body}"
        );
        assert!(body.contains("COPY build/ /work/"));
        assert!(body.contains("COPY project-nodes/"));
    }

    /// Non-debian runtime (alpine) bypasses the prebuilt base
    /// because the worker binary's ABI has to match the runtime: a
    /// glibc-built binary doesn't run on a musl runtime. Fall back
    /// to the from-scratch template that installs rustup inside
    /// the user's chosen base.
    #[test]
    fn non_debian_runtime_falls_back_to_from_scratch_template() {
        use crate::project::WorkerBuildSection;
        let build = WorkerBuildSection {
            base_image: Some("alpine:3.19".into()),
            dockerfile_template: None,
            package_manager: None,
        };
        let project_root = std::path::Path::new("/tmp");
        let catalog = weft_catalog::FsCatalog::empty();
        let referenced = std::collections::BTreeSet::new();
        let out = emit(
            &build,
            project_root,
            &catalog,
            &referenced,
            &NodeNeeds::new(),
            "worker_test",
            "weft-builder-base:irrelevant",
            &nothing_baked(),
        )
        .expect("emit");
        assert!(
            out.builder_base.is_none(),
            "non-debian runtime opts out of the prebuilt base"
        );
        assert!(
            out.body.contains("sh.rustup.rs"),
            "fallback template installs rustup: {}",
            out.body
        );
        // Both templates compile into the shared per-host cache and seed
        // the worker lock from the workspace lock so the engine + dep
        // rlibs are reused instead of recompiled.
        assert!(
            out.body.contains("CARGO_TARGET_DIR=/cache/target"),
            "fallback template compiles into the shared cache: {}",
            out.body
        );
        assert!(
            out.body.contains("cp /weft/Cargo.lock /work/Cargo.lock"),
            "fallback template seeds the worker lock from the workspace lock: {}",
            out.body
        );
    }

    /// An apt runtime of another release than the builder base's (an
    /// older glibc, which a binary built on the base refuses to start
    /// on) builds inside that runtime base instead; the default base
    /// takes the prebuilt one.
    #[test]
    fn only_the_builder_bases_own_release_takes_the_prebuilt_base() {
        let uses_prebuilt = |base: &str| {
            let build = crate::project::WorkerBuildSection { base_image: Some(base.into()), dockerfile_template: None, package_manager: None };
            let out = emit(
                &build,
                std::path::Path::new("/tmp"),
                &weft_catalog::FsCatalog::empty(),
                &std::collections::BTreeSet::new(),
                &NodeNeeds::new(),
                "worker_test",
                "weft-builder-base:irrelevant",
                &nothing_baked(),
            )
            .expect("emit");
            out.builder_base.is_some()
        };
        assert!(uses_prebuilt(DEFAULT_BASE_IMAGE));
        for other in ["debian:bookworm-slim", "ubuntu:24.04", "python:3.15-slim"] {
            assert!(!uses_prebuilt(other), "{other} builds from scratch");
        }
    }

    /// A CUSTOM template that references `{{builder_base_image}}`
    /// must report `builder_base = Some(tag)`: the rendered
    /// Dockerfile FROMs that tag, so its context carries no weft
    /// workspace. This keys off actual token usage, not off which
    /// template branch was taken.
    #[test]
    fn custom_template_using_builder_base_token_reports_the_tag() {
        use crate::project::WorkerBuildSection;
        let dir = std::env::temp_dir().join(format!(
            "weft-worker-image-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).expect("mkdir");
        std::fs::write(
            dir.join("Dockerfile.tpl"),
            "FROM {{builder_base_image}} AS builder\nFROM {{base_image}}\n",
        )
        .expect("write template");
        let build = WorkerBuildSection {
            base_image: None,
            dockerfile_template: Some("Dockerfile.tpl".into()),
            package_manager: None,
        };
        let catalog = weft_catalog::FsCatalog::empty();
        let referenced = std::collections::BTreeSet::new();
        let out = emit(
            &build,
            &dir,
            &catalog,
            &referenced,
            &NodeNeeds::new(),
            "worker_test",
            "weft-builder-base:cafebabe",
            &nothing_baked(),
        )
        .expect("emit");
        assert_eq!(
            out.builder_base.as_deref(),
            Some("weft-builder-base:cafebabe"),
            "custom template using the token must report the tag the build FROMs"
        );
        assert!(out.body.contains("FROM weft-builder-base:cafebabe AS builder"));
        assert!(!out.writes_left_out_record, "a custom template builds its own way and keeps no record");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A custom template still carrying the compiler-flags placeholder
    /// weft no longer fills is refused, naming the line to delete, rather
    /// than handing cargo the placeholder as literal flags.
    #[test]
    fn custom_template_with_the_retired_rustflags_line_is_refused() {
        use crate::project::WorkerBuildSection;
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("Dockerfile.tpl"),
            "FROM {{base_image}}\nENV RUSTFLAGS=\"{{worker_rustflags}}\"\nRUN cargo build\n",
        )
        .expect("write template");
        let build = WorkerBuildSection {
            base_image: None,
            dockerfile_template: Some("Dockerfile.tpl".into()),
            package_manager: None,
        };
        let Err(err) = emit(
            &build,
            dir.path(),
            &weft_catalog::FsCatalog::empty(),
            &std::collections::BTreeSet::new(),
            &NodeNeeds::new(),
            "worker_test",
            "weft-builder-base:cafebabe",
            &nothing_baked(),
        ) else {
            panic!("a template with the retired placeholder renders");
        };
        let err = err.to_string();
        assert!(err.contains("Dockerfile.tpl:2"), "{err}");
        assert!(err.contains("ENV RUSTFLAGS=\"{{worker_rustflags}}\"") && err.contains("delete that line"), "{err}");
    }

    /// Default debian runtime + no custom template: prebuilt base
    /// kicks in, `builder_base` is reported back, and the rendered body
    /// FROMs the tag we passed in.
    #[test]
    fn default_debian_runtime_uses_prebuilt_base() {
        use crate::project::WorkerBuildSection;
        let build = WorkerBuildSection {
            base_image: None,
            dockerfile_template: None,
            package_manager: None,
        };
        let project_root = std::path::Path::new("/tmp");
        let catalog = weft_catalog::FsCatalog::empty();
        let referenced = std::collections::BTreeSet::new();
        let out = emit(
            &build,
            project_root,
            &catalog,
            &referenced,
            &NodeNeeds::new(),
            "worker_test",
            "weft-builder-base:abcdef0123456789",
            &nothing_baked(),
        )
        .expect("emit");
        assert_eq!(
            out.builder_base.as_deref(),
            Some("weft-builder-base:abcdef0123456789"),
            "default debian path returns the base tag"
        );
        assert!(
            out.body.contains("FROM weft-builder-base:abcdef0123456789 AS builder"),
            "rendered body FROMs the tag: {}",
            out.body
        );
        assert!(
            !out.body.contains("COPY weft/"),
            "prebuilt path does NOT COPY weft/: {}",
            out.body
        );
        // The prebuilt path reuses the base's precompiled engine: the
        // shared cache is seeded from the baked workspace dir and the
        // worker lock from the workspace lock, so only project crates
        // compile, and only the ones no earlier build left in the cache.
        assert!(
            out.body.contains("CARGO_TARGET_DIR=/cache/target"),
            "prebuilt path compiles into the shared cache: {}",
            out.body
        );
        assert!(
            out.body.contains("cp -a /weft/target/. /cache/target/"),
            "prebuilt path seeds the cache from the baked workspace dir: {}",
            out.body
        );
        assert!(
            out.body.contains("id=weft-worker-target-") && !out.body.contains("{{worker_cache_key}}"),
            "the cache is named by the substituted key: {}",
            out.body
        );
        assert!(
            !out.body.contains("WEFT_COMPILE_LANE") && out.body.contains("id=weft-worker-cargo-registry,"),
            "an image build compiles in no lane of the machine's: {}",
            out.body
        );
        assert!(
            out.body.contains("cp /weft/worker.Cargo.lock /work/Cargo.lock"),
            "prebuilt path seeds the worker lock from the stock worker's lock: {}",
            out.body
        );
        // The binary is copied out of wherever it compiled (the shared
        // cache, or the baked target on a builder that keeps nothing)
        // inside the same RUN (the mount is gone in the next instruction).
        assert!(
            out.body.contains("cp \"$CARGO_TARGET_DIR\"/release/worker_test /worker"),
            "prebuilt path copies the binary out of where it compiled: {}",
            out.body
        );
        // A builder that keeps nothing between builds compiles in the
        // baked target instead of seeding a cache it throws away.
        assert!(
            out.body.contains(&format!("ARG {COMPILE_CACHE_ARG}=kept"))
                && out.body.contains("= none ] && [ -d /weft/target ]; then export CARGO_TARGET_DIR=/weft/target"),
            "a builder that keeps no cache compiles in the baked target: {}",
            out.body
        );
        // The crate is built by `weft-isolate`, compiled first from its
        // own package on the same seeded lock, and the image keeps the
        // record of what it left out where the install reads it.
        let isolate = crate::codegen::isolate_dir_name();
        let steps = [
            "cp /weft/worker.Cargo.lock /work/Cargo.lock".to_string(),
            format!("cp /weft/worker.Cargo.lock /work/{isolate}/Cargo.lock"),
            format!("cargo build --release --manifest-path /work/{isolate}/Cargo.toml --message-format=json-render-diagnostics > {CARGO_MESSAGES_PATH}"),
            format!("\"$CARGO_TARGET_DIR\"/release/weft-isolate /work/isolate.json /work/left-out.json {CARGO_MESSAGES_PATH}"),
            "cp \"$CARGO_TARGET_DIR\"/release/worker_test /worker".to_string(),
        ];
        let at: Vec<usize> = steps.iter().map(|step| out.body.find(step.as_str()).unwrap_or_else(|| panic!("{step}: {}", out.body))).collect();
        assert!(at.windows(2).all(|w| w[0] < w[1]), "the steps run in order: {}", out.body);
        assert!(!out.body.contains("cargo build --release \\"), "no plain worker build is left: {}", out.body);
        assert!(
            out.body.contains(&format!("COPY --from=builder /work/left-out.json {}\n", weft_core::builds::LEFT_OUT_RECORD_PATH)),
            "the runtime stage keeps the record: {}",
            out.body
        );
        assert!(out.writes_left_out_record);
        assert!(!out.body.contains("{{"), "every token is substituted: {}", out.body);
        let sweep = format!("sh /work/{CACHE_GC_SCRIPT_NAME} this-build /cache/target/release {CARGO_MESSAGES_PATH}");
        assert!(out.body.contains(&sweep), "every build keeps only what it used in the cache: {}", out.body);
        assert!(
            out.body.find("/worker \\").unwrap() < out.body.find(&sweep).unwrap() && out.body.contains("|| echo 'weft: the compile cache sweep failed"),
            "the sweep runs after the binary is out and can never fail the build: {}",
            out.body
        );
    }

    /// What a node needs from the machine lands in the image: a run need
    /// in the runtime stage, a build need in the builder stage, each for
    /// the image's manager; a need with no entry for that manager adds
    /// nothing, and an unused node adds nothing at all.
    #[test]
    fn a_nodes_needs_install_into_the_stage_they_are_for() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = crate::codegen::tests::catalog_with_needs(dir.path());
        let referenced: BTreeSet<String> = ["Run".to_string(), "Plain".to_string()].into();
        let build = WorkerBuildSection { base_image: Some("debian:trixie-slim".into()), dockerfile_template: None, package_manager: None };
        let needs = crate::codegen::node_needs(&referenced, &catalog).unwrap();
        let out = emit(&build, dir.path(), &catalog, &referenced, &needs, "worker_test", "weft-builder-base:x", &nothing_baked()).expect("emit");
        assert_eq!(out.runtime_packages, ["python3"]);
        assert_eq!(out.build_packages, ["libvips-dev"]);
        let (builder, runtime) = out.body.split_once("\nFROM debian:trixie-slim\n").expect("two stages");
        assert!(builder.contains("apt-get install -y --no-install-recommends libvips-dev "), "{builder}");
        assert!(runtime.contains("apt-get install -y --no-install-recommends python3 "), "{runtime}");
        assert!(!out.body.contains("ffmpeg") && !out.body.contains(" say"), "{}", out.body);

        let rocky = WorkerBuildSection { base_image: Some("rockylinux:9".into()), dockerfile_template: None, package_manager: None };
        let out = emit(&rocky, dir.path(), &catalog, &referenced, &needs, "worker_test", "weft-builder-base:x", &nothing_baked()).expect("emit");
        assert_eq!(out.runtime_packages, ["python3-libs"]);
        assert!(out.build_packages.is_empty(), "the build need has no dnf entry: {:?}", out.build_packages);
        assert!(out.body.contains("yum install -y python3-libs"), "{}", out.body);
    }

    /// What an image leaves to the machine its worker runs on is each need
    /// it has no packages for on its base, and every need of a stage a
    /// custom template installs nothing in.
    #[test]
    fn an_image_leaves_to_the_machine_only_the_needs_it_does_not_install() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = crate::codegen::tests::catalog_with_needs(dir.path());
        let referenced: BTreeSet<String> = ["Run".to_string()].into();
        let needs = crate::codegen::node_needs(&referenced, &catalog).unwrap();
        let left = |build: &WorkerBuildSection| -> Vec<String> {
            let out = emit(build, dir.path(), &catalog, &referenced, &needs, "worker_test", "weft-builder-base:x", &nothing_baked()).expect("emit");
            out.left_to_machine(needs.clone()).values().flatten().map(|need| need.name.clone()).collect()
        };
        assert_eq!(left(&WorkerBuildSection { base_image: Some("debian:trixie-slim".into()), dockerfile_template: None, package_manager: None }), ["say"]);
        assert_eq!(left(&WorkerBuildSection { base_image: Some("rockylinux:9".into()), dockerfile_template: None, package_manager: None }), ["libvips", "say"]);
        std::fs::write(dir.path().join("Dockerfile.custom"), "FROM {{base_image}}\n{{install_runtime_system_packages}}\n").unwrap();
        let custom = WorkerBuildSection { base_image: Some("debian:trixie-slim".into()), dockerfile_template: Some("Dockerfile.custom".into()), package_manager: None };
        assert_eq!(left(&custom), ["libvips", "say"], "the template installs no build stage");
    }

    /// A need that names packages for the image's manager, none for its
    /// distro and no default, is an error naming the need and the fix.
    #[test]
    fn a_need_with_no_entry_for_the_distro_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let node = dir.path().join("nodes/old");
        std::fs::create_dir_all(&node).unwrap();
        std::fs::write(node.join("metadata.json"), r#"{"type": "Old", "label": "Old", "description": "d", "inputs": [], "outputs": []}"#).unwrap();
        std::fs::write(node.join("mod.rs"), "// impl\n").unwrap();
        std::fs::write(node.join("deps.toml"), "[[needs]]\nname = \"python3\"\ncheck = [\"python3\"]\n[needs.install.apt]\ndebian_11 = [\"python3.9\"]\n").unwrap();
        let catalog = FsCatalog::discover(&dir.path().join("nodes")).unwrap();
        let build = WorkerBuildSection { base_image: None, dockerfile_template: None, package_manager: None };
        let old: BTreeSet<String> = ["Old".to_string()].into();
        let needs = crate::codegen::node_needs(&old, &catalog).unwrap();
        let err = emit(&build, dir.path(), &catalog, &old, &needs, "worker_test", "weft-builder-base:x", &nothing_baked())
            .err()
            .expect("refused")
            .to_string();
        assert!(err.contains("'python3'") && err.contains("[needs.install.apt]") && err.contains("debian_13"), "{err}");
    }

    /// A fake cargo target dir for the sweep: `release/` and the JSON
    /// messages a build wrote, each unit named by a sixteen-hex hash.
    struct FakeCache {
        _root: tempfile::TempDir,
        release: std::path::PathBuf,
        messages: std::path::PathBuf,
        script: std::path::PathBuf,
    }

    impl FakeCache {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let release = root.path().join("release");
            for dir in ["deps", ".fingerprint", "build"] {
                std::fs::create_dir_all(release.join(dir)).unwrap();
            }
            let script = root.path().join(CACHE_GC_SCRIPT_NAME);
            std::fs::write(&script, CACHE_GC_SCRIPT).unwrap();
            let messages = root.path().join("messages.json");
            Self { release, messages, script, _root: root }
        }

        fn deps(&self) -> std::path::PathBuf {
            self.release.join("deps")
        }

        fn fingerprints(&self) -> std::path::PathBuf {
            self.release.join(".fingerprint")
        }

        /// A library unit of `package`: its outputs and dep-info under the
        /// crate's name (`_` where the package has `-`), its fingerprint
        /// under the package's.
        fn lib(&self, package: &str, hash: &str) {
            let krate = package.replace('-', "_");
            for file in [format!("lib{krate}-{hash}.rlib"), format!("lib{krate}-{hash}.rmeta"), format!("{krate}-{hash}.d")] {
                std::fs::write(self.deps().join(file), b"x").unwrap();
            }
            std::fs::create_dir_all(self.fingerprints().join(format!("{package}-{hash}"))).unwrap();
        }

        /// A binary unit, and the hard link of it cargo puts at the top
        /// of `release/` when it is what the build asked for.
        fn bin(&self, krate: &str, hash: &str, linked_as: Option<&str>) {
            let unit = self.deps().join(format!("{krate}-{hash}"));
            std::fs::write(&unit, b"bin").unwrap();
            std::fs::write(self.deps().join(format!("{krate}-{hash}.d")), b"x").unwrap();
            std::fs::create_dir_all(self.fingerprints().join(format!("{krate}-{hash}"))).unwrap();
            if let Some(name) = linked_as {
                std::fs::hard_link(&unit, self.release.join(name)).unwrap();
            }
        }

        /// The messages of a build that used `libs` (crate, hash), ran
        /// the build script units `scripts` (`build/<dir>`), and linked
        /// `top` at the top of `release/`, the way cargo writes them
        /// (fresh units included).
        fn used(&self, libs: &[(&str, &str)], scripts: &[&str], top: &str) {
            let release = self.release.display();
            let mut lines: Vec<String> = libs
                .iter()
                .map(|(krate, hash)| {
                    format!(
                        r#"{{"reason":"compiler-artifact","filenames":["{release}/deps/lib{krate}-{hash}.rlib","{release}/deps/lib{krate}-{hash}.rmeta"],"executable":null,"fresh":true}}"#
                    )
                })
                .collect();
            for dir in scripts {
                lines.push(format!(r#"{{"reason":"build-script-executed","out_dir":"{release}/build/{dir}/out"}}"#));
            }
            lines.push(format!(r#"{{"reason":"compiler-artifact","filenames":["{release}/{top}"],"executable":"{release}/{top}","fresh":false}}"#));
            lines.push(r#"{"reason":"build-finished","success":true}"#.to_string());
            std::fs::write(&self.messages, lines.join("\n") + "\n").unwrap();
        }

        fn sweep(&self, args: &[&str]) {
            let status = std::process::Command::new("sh")
                .arg(&self.script)
                .arg(args[0])
                .arg(&self.release)
                .arg(&self.messages)
                .args(&args[1..])
                .status()
                .unwrap();
            assert!(status.success());
        }

        fn age(&self, path: &std::path::Path, days: i64) {
            let when = filetime::FileTime::from_unix_time(filetime::FileTime::now().unix_seconds() - days * 24 * 3600, 0);
            filetime::set_file_mtime(path, when).unwrap();
        }
    }

    const H: [&str; 8] = [
        "00000000000000a1", "00000000000000a2", "00000000000000a3", "00000000000000a4",
        "00000000000000a5", "00000000000000a6", "00000000000000a7", "00000000000000a8",
    ];

    /// A cache one build owns keeps exactly what the build used: the
    /// units cargo reported (reused ones included), the binary it linked
    /// (known only through the hard link), the build scripts it ran, and
    /// as many incremental directories per crate as it used units of
    /// that crate; a superseded dependency version, an engine unit of an
    /// older lock and an older binary go, fingerprints included.
    #[test]
    fn a_this_build_sweep_keeps_exactly_what_the_build_used() {
        let cache = FakeCache::new();
        cache.lib("serde", H[0]); // used
        cache.lib("serde", H[1]); // an older version: goes
        cache.lib("weft-core", H[2]); // used
        cache.lib("weft-core", H[3]); // built against an older lock: goes
        cache.bin("weft_runtime", H[4], Some("weft-runtime")); // linked by this build
        cache.bin("weft_runtime", H[5], None); // an older link: goes
        let build = cache.release.join("build");
        for dir in [format!("ring-{}", H[6]), format!("ring-{}", H[7])] {
            std::fs::create_dir_all(build.join(&dir).join("out")).unwrap();
        }
        std::fs::write(cache.deps().join("not-a-unit.txt"), b"x").unwrap();
        let incremental = cache.release.join("incremental");
        for (dir, days) in [("weft_core-newest", 0), ("weft_core-older", 2), ("weft_runtime-x", 1), ("gone_crate-y", 0)] {
            std::fs::create_dir_all(incremental.join(dir)).unwrap();
            cache.age(&incremental.join(dir), days);
        }
        cache.used(&[("serde", H[0]), ("weft_core", H[2])], &[&format!("ring-{}", H[6])], "weft-runtime");

        cache.sweep(&["this-build"]);

        let deps = cache.deps();
        for kept in [format!("libserde-{}.rlib", H[0]), format!("weft_core-{}.d", H[2]), format!("weft_runtime-{}", H[4])] {
            assert!(deps.join(&kept).exists(), "{kept} was used and stays");
        }
        for gone in [format!("libserde-{}.rlib", H[1]), format!("serde-{}.d", H[1]), format!("libweft_core-{}.rmeta", H[3]), format!("weft_runtime-{}", H[5])] {
            assert!(!deps.join(&gone).exists(), "{gone} was not used and goes");
        }
        let fingerprints = cache.fingerprints();
        assert!(fingerprints.join(format!("weft-core-{}", H[2])).exists() && fingerprints.join(format!("weft_runtime-{}", H[4])).exists());
        assert!(!fingerprints.join(format!("weft-core-{}", H[3])).exists() && !fingerprints.join(format!("serde-{}", H[1])).exists());
        assert!(build.join(format!("ring-{}", H[6])).exists() && !build.join(format!("ring-{}", H[7])).exists());
        assert!(deps.join("not-a-unit.txt").exists(), "a file no hash names is not the sweep's business");
        assert!(cache.release.join("weft-runtime").exists(), "the linked binary stays");
        assert!(incremental.join("weft_core-newest").exists() && incremental.join("weft_runtime-x").exists());
        assert!(!incremental.join("weft_core-older").exists(), "one weft_core unit used, one incremental dir kept");
        assert!(!incremental.join("gone_crate-y").exists(), "a crate the build did not use keeps none");
    }

    /// When the binary the build asked for cannot be traced to its unit
    /// (no hard link), or there are no messages at all, a this-build sweep
    /// removes nothing: dropping on a partial picture would throw away what
    /// the build used.
    #[test]
    fn a_this_build_sweep_that_cannot_see_the_whole_build_removes_nothing() {
        let cache = FakeCache::new();
        cache.lib("serde", H[1]);
        cache.bin("weft_runtime", H[4], None);
        std::fs::write(cache.release.join("weft-runtime"), b"a copy").unwrap();
        cache.sweep(&["this-build"]);
        assert!(cache.deps().join(format!("libserde-{}.rlib", H[1])).exists(), "no messages: nothing goes");
        cache.used(&[("serde", H[0])], &[], "weft-runtime");
        cache.sweep(&["this-build"]);
        assert!(cache.deps().join(format!("libserde-{}.rlib", H[1])).exists(), "an untraced binary: nothing goes");
        assert!(cache.deps().join(format!("weft_runtime-{}", H[4])).exists());
    }
}
