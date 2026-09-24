//! Worker image codegen.
//!
//! Given a project (for its `[build.worker]` config) and the set
//! of referenced node types (for per-node `[system]` package
//! declarations), emit a multi-stage Dockerfile that builds the
//! worker container image.
//!
//! Pipeline:
//!
//! 1. **Parse the base image.** User's `[build.worker] base_image`
//!    or the built-in default (`debian:bookworm-slim`). Codegen
//!    derives a package manager and a `<distro>_<major>` key from
//!    the image string. Unknown families fall back to apt +
//!    default-only lookup.
//!
//! 2. **Walk referenced nodes, collect per-stage packages.** Each
//!    node's `deps.toml` has `[system.build]` (builder-stage
//!    packages) and `[system.runtime]` (runtime-stage packages),
//!    each keyed by manager and distro. Codegen:
//!    - picks the entry matching the chosen distro_key if present,
//!    - else the `default` entry,
//!    - else the node contributes nothing for that stage. Only an
//!      error if the node had entries for this manager but none
//!      matched AND no default.
//!
//! 3. **Union across nodes, emit the Dockerfile.** Builder stage
//!    installs build-time packages, fetches rust via rustup,
//!    runs `cargo build --release` against a build context that
//!    contains the generated crate + the referenced catalog
//!    subfolders. Runtime stage installs runtime-only packages
//!    and copies the compiled binary from the builder.
//!
//! The template is a simple `{{token}}` substitution. Built-in
//! template lives in `default_template()`. Users can override by
//! setting `[build.worker] dockerfile_template = "path"` in
//! weft.toml; the same tokens are substituted.

use std::collections::BTreeSet;
use std::path::Path;

use weft_catalog::{BuildStage, FsCatalog, SystemManagerKey};

use crate::error::{CompileError, CompileResult};
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
    /// `{{builder_base_image}}` token, so the CLI ensures the named
    /// image exists before invoking `docker build`; `None` when the
    /// rendered Dockerfile never FROMs it (the from-scratch template,
    /// or a custom template that builds its own toolchain).
    pub builder_base: Option<String>,
}

/// Parsed base-image metadata. `manager` drives the install
/// command; `distro_key` (e.g. `debian_12`) drives per-node
/// package lookup in each stage's `[system.*]` table.
#[derive(Debug, Clone)]
pub struct BaseImage {
    pub raw: String,
    pub manager: PackageManager,
    /// `<family>_<major>`. Empty when the image string doesn't
    /// resolve to a known distro; codegen falls back to `default`
    /// entries only in that case.
    pub distro_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageManager {
    Apt,
    Apk,
    Yum,
    Brew,
}

impl PackageManager {
    pub fn name(self) -> &'static str {
        match self {
            Self::Apt => "apt",
            Self::Apk => "apk",
            Self::Yum => "yum",
            Self::Brew => "brew",
        }
    }

    fn to_catalog_key(self) -> SystemManagerKey {
        match self {
            Self::Apt => SystemManagerKey::Apt,
            Self::Apk => SystemManagerKey::Apk,
            Self::Yum => SystemManagerKey::Yum,
            Self::Brew => SystemManagerKey::Brew,
        }
    }
}

pub const DEFAULT_BASE_IMAGE: &str = "debian:bookworm-slim";

/// Weft workspace mount point INSIDE the builder container. The
/// docker build context copies the language-runtime workspace
/// (`crates/`, `Cargo.toml`, `Cargo.lock`) to this path, giving the
/// generated crate access to the weft-engine / weft-core crates via
/// `../weft/crates/*` path dependencies. No node code lives here.
pub const WEFT_MOUNT: &str = "/weft";

/// In-container path to the project's `nodes/` directory. The build
/// context stages `project-nodes/` here; every node's `#[path]` shim
/// and every `{{catalog_path}}` substitution resolves under it (e.g.
/// `/weft/project-nodes/nodes/base_catalog/basic/exec_python/mod.rs`). This is the only
/// place node source comes from: the project owns all its nodes.
pub const NODES_MOUNT: &str = "/weft/project-nodes";

/// Image repo for the shared pre-built builder base. The CLI
/// qualifies it with the registry and a short hash of the engine
/// workspace (`crates/`, `Cargo.toml`, `Cargo.lock`,
/// `rust-toolchain.toml`), so an engine bump produces a fresh ref, and
/// stamps the result into each project's worker-Dockerfile `FROM`.
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
/// `dockerfile_template` path. `binary_name` is the generated
/// crate's binary name so the builder stage knows which binary to
/// copy over. `referenced` is the set of node types the project
/// compiles against (from codegen's own walk), and
/// `referenced_package_roots` lists the catalog subdirectories
/// the build context must include (the codegen's `#[path]`
/// includes point at these).
///
/// `builder_base_ref` is the fully-qualified ref of the shared
/// pre-built builder-base image. The CLI computes it from the
/// engine workspace hash and ensures the image exists. When the
/// user's runtime base is debian-family and they haven't supplied a
/// custom Dockerfile template, the builder stage `FROM`s this image
/// instead of re-installing rustup / build packages / re-fetching
/// the cargo registry per project. When no custom template is supplied
/// AND the runtime base is non-debian, we use the from-scratch template
/// instead (the builder must match the runtime ABI: glibc base vs musl
/// base). A user-supplied custom template is used verbatim and bypasses
/// both built-in templates.
///
/// `baked` is what the builder base already installed for the stock
/// worker ([`crate::build::stock_builder_stage`]): a builder stage that
/// FROMs the base installs only the packages beyond it, so an untouched
/// project's build runs no `apt-get update` at all.
pub fn emit(
    build: &WorkerBuildSection,
    project_root: &Path,
    catalog: &FsCatalog,
    referenced: &BTreeSet<String>,
    binary_name: &str,
    builder_base_ref: &str,
    baked: &BuilderStage,
) -> CompileResult<WorkerDockerfile> {
    let base_image_str = build
        .base_image
        .clone()
        .unwrap_or_else(|| DEFAULT_BASE_IMAGE.to_string());
    let base = parse_base_image(&base_image_str);

    let mut build_packages =
        collect_stage_packages(catalog, referenced, &base, BuildStage::Build)?;
    let runtime_packages =
        collect_stage_packages(catalog, referenced, &base, BuildStage::Runtime)?;
    let build_env = collect_build_env(catalog, referenced, project_root)?;

    // The pre-built base shortcuts the "install rustup + apt
    // build-essential" cycle. It only fits a debian-family runtime
    // base (glibc ABI match: a debian-built worker binary runs in a
    // debian runtime, not an alpine/musl one) and never applies to a
    // custom Dockerfile template (the Some arm below), which may not
    // respect our base layout. Otherwise fall back to the
    // from-scratch template that installs everything inside the
    // builder stage.
    let use_prebuilt_base = base.manager == PackageManager::Apt;
    if use_prebuilt_base && build.dockerfile_template.is_none() {
        build_packages.retain(|package| !baked.packages.contains(package));
    }

    let template = match &build.dockerfile_template {
        Some(rel) => {
            let path = project_root.join(rel);
            std::fs::read_to_string(&path).map_err(|e| {
                CompileError::Build(format!(
                    "read custom Dockerfile template {}: {}",
                    path.display(),
                    e
                ))
            })?
        }
        None => {
            if use_prebuilt_base {
                prebuilt_base_template()
            } else {
                default_template()
            }
        }
    };

    // The CLI's "ensure the builder base exists" step keys off actual
    // USAGE: whichever template was chosen (built-in or custom), if it
    // references `{{builder_base_image}}` the rendered Dockerfile will
    // FROM that ref and the image must exist before `docker build`.
    let builder_base_out = template
        .contains("{{builder_base_image}}")
        .then(|| builder_base_ref.to_string());

    // The compile cache is per builder: artifacts from a from-scratch
    // builder stage on `base.raw` must not meet those of the prebuilt
    // debian base (same rust target, different C toolchain and headers).
    let builder = if builder_base_out.is_some() { "builder-base" } else { base.raw.as_str() };
    let weft_root = crate::build::resolve_weft_root()?;
    let worker_cache_key = crate::hash::compute_worker_cache_key(&weft_root, builder)
        .map_err(|e| CompileError::Build(format!("worker compile cache key: {e}")))?;

    let body = template
        .replace("{{base_image}}", &base.raw)
        .replace("{{builder_base_image}}", builder_base_ref)
        .replace("{{worker_cache_key}}", &worker_cache_key)
        .replace("{{cache_gc_script}}", CACHE_GC_SCRIPT_NAME)
        .replace("{{cache_retention_days}}", &WORKER_CACHE_RETENTION_DAYS.to_string())
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
    })
}

/// What a builder stage needs beyond the toolchain to compile a node
/// set on the default (debian) base: the union of the nodes' build-stage
/// system packages and their `[build.env]`, `{{catalog_path}}` already
/// expanded to the in-container node path.
pub struct BuilderStage {
    pub packages: Vec<String>,
    pub env: std::collections::BTreeMap<String, String>,
}

impl BuilderStage {
    pub fn for_nodes(
        catalog: &FsCatalog,
        referenced: &BTreeSet<String>,
        project_root: &Path,
    ) -> CompileResult<Self> {
        let base = parse_base_image(DEFAULT_BASE_IMAGE);
        Ok(Self {
            packages: collect_stage_packages(catalog, referenced, &base, BuildStage::Build)?,
            env: collect_build_env(catalog, referenced, project_root)?,
        })
    }

    /// The Dockerfile lines that install the packages and export the
    /// environment, for the default base's package manager.
    pub fn render(&self) -> (String, String) {
        (
            render_install_line(parse_base_image(DEFAULT_BASE_IMAGE).manager, &self.packages),
            render_build_env_lines(&self.env),
        )
    }
}

/// Walk referenced nodes' `[system.<stage>.<manager>]` tables and
/// compute the union of packages to install. Per-node selection:
/// distro_key match → `default` fallback → error if the node had
/// entries on this manager but neither resolved.
fn collect_stage_packages(
    catalog: &FsCatalog,
    referenced: &BTreeSet<String>,
    base: &BaseImage,
    stage: BuildStage,
) -> CompileResult<Vec<String>> {
    let manager_key = base.manager.to_catalog_key();
    let mut packages: BTreeSet<String> = BTreeSet::new();

    for node_type in referenced {
        let Some(deps) = catalog
            .deps(node_type)
            .map_err(|e| CompileError::Build(format!("load deps for {node_type}: {e}")))?
        else {
            continue;
        };
        let stage_pkgs = match stage {
            BuildStage::Build => &deps.system.build,
            BuildStage::Runtime => &deps.system.runtime,
        };
        let table = stage_pkgs.for_manager(manager_key);
        if table.is_empty() {
            continue;
        }
        let resolved = if !base.distro_key.is_empty() {
            table.get(&base.distro_key).or_else(|| table.get("default"))
        } else {
            table.get("default")
        };
        match resolved {
            Some(list) => {
                for p in list {
                    packages.insert(p.clone());
                }
            }
            None => {
                let keys: Vec<&String> = table.keys().collect();
                let stage_name = match stage {
                    BuildStage::Build => "build",
                    BuildStage::Runtime => "runtime",
                };
                return Err(CompileError::Build(format!(
                    "node '{node_type}' declares [system.{stage_name}.{}] packages for {keys:?} \
                     but none matches the project's base image '{}' (distro key '{}') \
                     and no 'default' entry is set. Add `default = [...]` or \
                     `{} = [...]` to the node's deps.toml.",
                    base.manager.name(),
                    base.raw,
                    base.distro_key,
                    base.distro_key,
                )));
            }
        }
    }
    Ok(packages.into_iter().collect())
}

/// Collect and substitute `[build.env]` across referenced nodes.
/// `{{catalog_path}}` expands to the node's in-container path under
/// `NODES_MOUNT` (matches where the build context mounts the project's
/// nodes). Conflicts on the same variable abort the build.
fn collect_build_env(
    catalog: &FsCatalog,
    referenced: &BTreeSet<String>,
    project_root: &Path,
) -> CompileResult<std::collections::BTreeMap<String, String>> {
    let mut merged: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    let mut first_setter: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();

    for node_type in referenced {
        let Some(deps) = catalog
            .deps(node_type)
            .map_err(|e| CompileError::Build(format!("load deps for {node_type}: {e}")))?
        else {
            continue;
        };
        if deps.build.env.is_empty() {
            continue;
        }
        let Some(source_dir) = catalog.source_dir(node_type) else {
            return Err(CompileError::Build(format!(
                "node '{node_type}' declares [build.env] but has no source dir"
            )));
        };
        let catalog_path = node_catalog_path(node_type, source_dir, project_root)?;

        for (k, v) in deps.build.env.iter() {
            let resolved = v.replace("{{catalog_path}}", &catalog_path);
            if let Some(existing) = merged.get(k) {
                if existing != &resolved {
                    let other = first_setter.get(k).cloned().unwrap_or_default();
                    return Err(CompileError::Build(format!(
                        "[build.env] conflict on '{k}': '{other}' sets '{existing}', \
                         '{node_type}' sets '{resolved}'"
                    )));
                }
            } else {
                merged.insert(k.clone(), resolved.clone());
                first_setter.insert(k.clone(), node_type.clone());
            }
        }
    }
    Ok(merged)
}

/// Compute the in-container path for a node's source dir. The docker
/// build stages every referenced node under `NODES_MOUNT` at its
/// project-relative path (`nodes/...`, or `src/...` for a node beside
/// the code); the node's in-container path is that same path.
fn node_catalog_path(
    node_type: &str,
    source_dir: &Path,
    project_root: &Path,
) -> CompileResult<String> {
    let rel = source_dir.strip_prefix(project_root).map_err(|_| {
        CompileError::Build(format!(
            "node '{node_type}' source dir {} is not under the project root {}",
            source_dir.display(),
            project_root.display()
        ))
    })?;
    Ok(format!("{NODES_MOUNT}/{}", rel.display()))
}

/// Parse a base-image string into a `BaseImage`. Recognizes the
/// Docker Hub tag conventions for the families we support.
pub fn parse_base_image(raw: &str) -> BaseImage {
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
        return BaseImage {
            raw: raw.to_string(),
            manager: PackageManager::Apt,
            distro_key,
        };
    }
    if image_lc.ends_with("ubuntu") || image_lc.contains("ubuntu") {
        let version = ubuntu_tag_to_version(&tag_lc);
        let distro_key = version
            .map(|v| format!("ubuntu_{v}"))
            .unwrap_or_default();
        return BaseImage {
            raw: raw.to_string(),
            manager: PackageManager::Apt,
            distro_key,
        };
    }

    if image_lc.ends_with("alpine") || image_lc.contains("alpine") {
        let version = alpine_tag_to_version(&tag_lc);
        let distro_key = version
            .map(|v| format!("alpine_{v}"))
            .unwrap_or_default();
        return BaseImage {
            raw: raw.to_string(),
            manager: PackageManager::Apk,
            distro_key,
        };
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
            return BaseImage {
                raw: raw.to_string(),
                manager: PackageManager::Yum,
                distro_key,
            };
        }
    }

    if image_lc.contains("homebrew") || image_lc.contains("/brew") {
        return BaseImage {
            raw: raw.to_string(),
            manager: PackageManager::Brew,
            distro_key: String::new(),
        };
    }

    if image_lc.starts_with("python") {
        let distro_key = if tag_lc.contains("bookworm") || tag_lc.contains("slim") {
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
        return BaseImage {
            raw: raw.to_string(),
            manager,
            distro_key,
        };
    }

    tracing::warn!(
        raw = raw,
        "unknown base image family; defaulting to apt + default-only package selection. \
         Set `[build.worker] dockerfile_template` or pick a known base image for more control.",
    );
    BaseImage {
        raw: raw.to_string(),
        manager: PackageManager::Apt,
        distro_key: String::new(),
    }
}

fn debian_tag_to_version(tag: &str) -> Option<&'static str> {
    if tag.is_empty() || tag == "latest" {
        return Some("12");
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
/// - compiles into a persistent BuildKit cache per
///   `{{worker_cache_key}}` (`hash::compute_worker_cache_key`: the build
///   environment and the builder) and compile lane (below), shared by
///   every worker build on this host that holds that lane. Each cache
///   is seeded ONCE from the baked `/weft/target`
///   (the marker file records a complete seed; a build interrupted
///   mid-copy seeds again), so the precompiled engine is there from the
///   first build. After that, cargo's own fingerprints decide what
///   compiles: a `pkg_<name>` crate whose sources and generated shim are
///   older than its cached rlib is reused as is, whatever project asked
///   for it. The seed already holds every stock package compiled, so a
///   project with one custom or edited node compiles that node and links,
///   from its very first build on a host. Reuse
///   across projects rests on each package crate living at a path named
///   by its content (`/work/pkg_<name>-<slot>`,
///   `codegen::write_package_crates`: same sources, same slot, shared
///   rlib; an edited copy of a stock node gets a slot of its own and can
///   never be mistaken for the stock one) and on source mtimes the
///   staging preserves (`build::copy_dir_filtered`). Every distinct
///   content compiled keeps its slot's artifacts until no build has
///   linked them for [`WORKER_CACHE_RETENTION_DAYS`] ([`CACHE_GC_SCRIPT`]
///   runs after every build); `weft clean --images` reports the mount's
///   size and `weft clean --build-cache` throws it away whole;
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
/// (immutable per version), so a lane downloads a crate once.
///
/// Both caches are one per LANE (`{{worker_cache_key}}-<lane>` for the
/// compile cache), the lane a build-arg ([`COMPILE_LANE_ARG`]) the CLI
/// fills with a lane it holds for the whole build. Cargo locks a target
/// directory for a whole build, so one shared cache made every worker
/// build on a host wait for the one before it (ten builds at once meant
/// the last waited out nine); each lane is its own directory, seeded
/// once from the baked layer on its first use, so builds in different
/// lanes compile side by side and the host keeps at most one cache per
/// lane. The registry is per lane for the same reason: cargo's lock on
/// its package cache lives in `$CARGO_HOME`, outside the mount, so two
/// builds sharing one registry unpack the same crate over each other.
/// Both mounts are `sharing=locked`, which makes a build given the same
/// lane as a running one wait for it rather than race it.
///
/// `cargo build` (NOT `--locked`): the worker manifest carries extra
/// path deps (the `pkg_<node>` crates) absent from the seeded
/// workspace lock, so cargo must be allowed to EXTEND the lock with
/// those. Seeding the lock pins the shared set; cargo appends the rest.
const CARGO_BUILD_RUN_FRAGMENT: &str = concat!(
    "ENV CARGO_TARGET_DIR=/cache/target\n",
    "ARG WEFT_COMPILE_LANE\n",
    "RUN --mount=type=cache,id=weft-worker-cargo-registry-${WEFT_COMPILE_LANE},target=/root/.cargo/registry,sharing=locked \\\n",
    "    --mount=type=cache,id=weft-worker-target-{{worker_cache_key}}-${WEFT_COMPILE_LANE},target=/cache/target,sharing=locked \\\n",
    "    ( [ -f /cache/target/.weft-seeded ] || ! [ -d /weft/target ] \\\n",
    "      || ( cp -a /weft/target/. /cache/target/ && touch /cache/target/.weft-seeded ) ) \\\n",
    "    && cp {{seed_lock}} /work/Cargo.lock \\\n",
    "    && cargo build --release \\\n",
    "    && cp /cache/target/release/{{binary_name}} /worker \\\n",
    "    && ( sh /work/{{cache_gc_script}} /cache/target/release {{cache_retention_days}} /work {{binary_name}} \\\n",
    "         || echo 'weft: the compile cache sweep failed; the build is unaffected' >&2 )\n",
);

/// Where the builder base keeps the lock its stock worker resolved to,
/// which a per-project build on that base starts from (see
/// [`CARGO_BUILD_RUN_FRAGMENT`]).
// SYNC: BASE_WORKER_LOCK <-> deploy/docker/worker-builder-base.Dockerfile (worker.Cargo.lock)
pub const BASE_WORKER_LOCK: &str = "/weft/worker.Cargo.lock";

/// What a build without the builder base seeds its lock from: the
/// workspace lock its own `COPY weft/` brings.
const WORKSPACE_LOCK: &str = "/weft/Cargo.lock";

/// The build-arg naming the compile-cache lane a worker build uses (see
/// [`CARGO_BUILD_RUN_FRAGMENT`]).
// SYNC: COMPILE_LANE_ARG <-> the `ARG WEFT_COMPILE_LANE` line of CARGO_BUILD_RUN_FRAGMENT above
pub const COMPILE_LANE_ARG: &str = "WEFT_COMPILE_LANE";

/// The id prefix of the compile cache mounts, as `docker buildx du`
/// reports them (`with id "/weft-worker-target-<key>-<lane>"`): what a
/// size report looks for.
pub const WORKER_CACHE_MOUNT_ID_PREFIX: &str = "weft-worker-target-";

/// How long a compiled node package stays in the shared cache after the
/// last build that linked it. Every distinct content of a package keeps
/// its own compiled copy (`codegen::write_package_crates`), so without
/// this a host that edits nodes would keep every version it ever built.
pub const WORKER_CACHE_RETENTION_DAYS: u32 = 30;

/// File name of [`CACHE_GC_SCRIPT`] inside the generated crate (`/work`
/// in the builder stage).
pub const CACHE_GC_SCRIPT_NAME: &str = "weft-cache-gc.sh";

/// Runs at the end of every built-in-template worker build (a custom
/// `dockerfile_template` writes its own RUN and gets none of this),
/// inside the cache mount, after the binary is copied out: marks the
/// package crates this build linked as used, then removes every
/// per-project crate no build has used for the retention period. It
/// can never fail the build (the RUN reports and carries on).
///
/// "Used" is tracked on the crate's own files in `deps/`: cargo rewrites
/// them when it compiles the crate and leaves them alone when it reuses
/// the rlib, so touching them here on every build makes their mtime
/// "last linked" rather than "last compiled". Cargo writes a path
/// dependency's crate root relative to the workspace root into the
/// dep-info (`pkg_<name>-<slot>/src/lib.rs`), which is how a `.d` names
/// the slot directory this build staged. Expiry removes a crate's
/// outputs and fingerprint together (their stems are equal), so cargo
/// simply compiles it again if a later build wants it. Two families
/// are per project and expire: the `pkg_` package crates, and the top
/// worker crate (its build-script units included), whose metadata hash
/// folds in its package deps so every distinct project leaves its own
/// copy. The engine and the shared dependencies are never touched.
pub const CACHE_GC_SCRIPT: &str = r#"#!/bin/sh
# Emitted by weft codegen. Do not edit by hand.
# usage: weft-cache-gc.sh <target/release> <retention days> <crate root> <worker binary name>
set -eu
release="$1"; days="$2"; work="$3"; worker="$4"
deps="$release/deps"; fingerprints="$release/.fingerprint"; scripts="$release/build"
[ -d "$deps" ] || exit 0
# Mark: every file of a package crate this build linked is touched. One
# grep over every dep-info, against the list of staged slot directories.
slots="$(mktemp)"
for dir in "$work"/pkg_*-*/; do
  [ -d "$dir" ] && printf '%s/\n' "$(basename "$dir")" >> "$slots"
done
if [ -s "$slots" ]; then
  for depinfo in $(grep -l -F -f "$slots" "$deps"/pkg_*.d 2>/dev/null || true); do
    stem="$(basename "$depinfo" .d)"
    touch "$depinfo"
    for file in "$deps/lib$stem".* "$fingerprints/$stem"; do
      [ -e "$file" ] && touch "$file"
    done
  done
fi
rm -f "$slots"
# Expire: a per-project crate any of whose files is older than the
# retention period (a used crate had every file touched above; an orphan
# missing its dep-info still ages by its rlib or fingerprint).
keep=$((days - 1))
for file in "$deps"/pkg_*.d "$deps"/libpkg_*.rlib "$deps"/libpkg_*.rmeta "$deps"/"$worker"-*.d \
            "$fingerprints"/pkg_* "$fingerprints"/"$worker"-*; do
  [ -e "$file" ] || continue
  [ -n "$(find "$file" -maxdepth 0 -mtime +"$keep")" ] || continue
  stem="$(basename "$file")"
  case "$stem" in *.d|*.rlib|*.rmeta) stem="${stem%.*}";; esac
  stem="${stem#lib}"
  rm -f "$deps/$stem" "$deps/$stem".* "$deps/lib$stem".*
  rm -rf "$fingerprints/$stem"
done
# The worker crate's build-script units carry their own hashes; they age
# on their own and are cheap to run again.
for dir in "$scripts"/"$worker"-*/; do
  [ -d "$dir" ] || continue
  [ -n "$(find "$dir" -maxdepth 0 -mtime +"$keep")" ] && rm -rf "$dir"
done
exit 0
"#;

/// The runtime stage shared by both built-in templates: install
/// runtime-only packages onto the user's base image, copy the
/// compiled binary from the builder.
const RUNTIME_STAGE_FRAGMENT: &str = concat!(
    "FROM {{base_image}}\n",
    "\n",
    "{{install_runtime_base}}",
    "{{install_runtime_system_packages}}",
    "\n",
    "COPY --from=builder /worker /usr/local/bin/worker\n",
    "ENTRYPOINT [\"/usr/local/bin/worker\"]\n",
);

/// Built-in multi-stage template. Used when no custom template is
/// supplied AND the runtime base is non-debian (so the builder ABI must
/// match). Installs rustup + build tools inside the builder stage.
///
/// Stage 1 (`builder`): installs build-time packages + rust via
/// rustup, copies the generated crate + referenced catalog
/// subfolders, runs `cargo build --release`, writes the binary
/// to `/worker`.
///
/// Stage 2 (runtime): `RUNTIME_STAGE_FRAGMENT`.
fn default_template() -> String {
    [
        concat!(
            "# syntax=docker/dockerfile:1.6\n",
            "\n",
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
/// project's `nodes/` source tree. The base's `/weft/` directory
/// provides the engine workspace via path deps AND its precompiled
/// rlibs; no per-project COPY or recompile of the workspace. Target
/// dir + Cargo.lock mechanics are documented on
/// `CARGO_BUILD_RUN_FRAGMENT`.
fn prebuilt_base_template() -> String {
    [
        concat!(
            "# syntax=docker/dockerfile:1.6\n",
            "\n",
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
        PackageManager::Yum => concat!(
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
        PackageManager::Yum => "RUN yum install -y ca-certificates \\\n    && yum clean all\n"
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
        PackageManager::Yum => format!(
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

    #[test]
    fn debian_bookworm_slim_parses_to_debian_12() {
        let b = parse_base_image("debian:bookworm-slim");
        assert_eq!(b.manager, PackageManager::Apt);
        assert_eq!(b.distro_key, "debian_12");
    }

    #[test]
    fn debian_trixie_parses_to_debian_13() {
        let b = parse_base_image("debian:trixie-slim");
        assert_eq!(b.distro_key, "debian_13");
    }

    #[test]
    fn debian_numeric_tag_parses_to_debian_12() {
        let b = parse_base_image("debian:12-slim");
        assert_eq!(b.distro_key, "debian_12");
    }

    #[test]
    fn ubuntu_numeric_parses() {
        let b = parse_base_image("ubuntu:22.04");
        assert_eq!(b.distro_key, "ubuntu_22_04");
    }

    #[test]
    fn alpine_parses() {
        let b = parse_base_image("alpine:3.19");
        assert_eq!(b.manager, PackageManager::Apk);
        assert_eq!(b.distro_key, "alpine_3_19");
    }

    #[test]
    fn rocky_linux_parses() {
        let b = parse_base_image("rockylinux:9-minimal");
        assert_eq!(b.manager, PackageManager::Yum);
        assert_eq!(b.distro_key, "rocky_9");
    }

    #[test]
    fn python_image_uses_debian_under_the_hood() {
        let b = parse_base_image("python:3.13-slim-bookworm");
        assert_eq!(b.manager, PackageManager::Apt);
        assert_eq!(b.distro_key, "debian_12");
    }

    #[test]
    fn unknown_image_falls_back_to_apt_no_distro_key() {
        let b = parse_base_image("registry.example.com/my-org/my-base:abc123");
        assert_eq!(b.manager, PackageManager::Apt);
        assert_eq!(b.distro_key, "");
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
        };
        let project_root = std::path::Path::new("/tmp");
        let catalog = weft_catalog::FsCatalog::empty();
        let referenced = std::collections::BTreeSet::new();
        let out = emit(
            &build,
            project_root,
            &catalog,
            &referenced,
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

    /// A CUSTOM template that references `{{builder_base_image}}`
    /// must report `builder_base = Some(tag)`: the rendered
    /// Dockerfile FROMs that tag, so the CLI has to ensure the image
    /// exists. The ensure step keys off actual token usage, not off
    /// which template branch was taken.
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
        };
        let catalog = weft_catalog::FsCatalog::empty();
        let referenced = std::collections::BTreeSet::new();
        let out = emit(
            &build,
            &dir,
            &catalog,
            &referenced,
            "worker_test",
            "weft-builder-base:cafebabe",
            &nothing_baked(),
        )
        .expect("emit");
        assert_eq!(
            out.builder_base.as_deref(),
            Some("weft-builder-base:cafebabe"),
            "custom template using the token must report the tag for the CLI ensure step"
        );
        assert!(out.body.contains("FROM weft-builder-base:cafebabe AS builder"));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Default debian runtime + no custom template: prebuilt base
    /// kicks in, `builder_base` is reported back to the CLI so it
    /// can ensure the named image exists, and the rendered body
    /// FROMs the tag we passed in.
    #[test]
    fn default_debian_runtime_uses_prebuilt_base() {
        use crate::project::WorkerBuildSection;
        let build = WorkerBuildSection {
            base_image: None,
            dockerfile_template: None,
        };
        let project_root = std::path::Path::new("/tmp");
        let catalog = weft_catalog::FsCatalog::empty();
        let referenced = std::collections::BTreeSet::new();
        let out = emit(
            &build,
            project_root,
            &catalog,
            &referenced,
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
            out.body.contains("cp /weft/worker.Cargo.lock /work/Cargo.lock"),
            "prebuilt path seeds the worker lock from the stock worker's lock: {}",
            out.body
        );
        // The binary is copied out of the shared cache inside the same
        // RUN (the mount is gone in the next instruction).
        assert!(
            out.body.contains("cp /cache/target/release/worker_test /worker"),
            "prebuilt path copies the binary out of the shared cache: {}",
            out.body
        );
        let sweep = format!("sh /work/{CACHE_GC_SCRIPT_NAME} /cache/target/release {WORKER_CACHE_RETENTION_DAYS} /work worker_test");
        assert!(out.body.contains(&sweep), "every build expires unused per-project crates from the shared cache: {}", out.body);
        assert!(
            out.body.find("/worker \\").unwrap() < out.body.find(&sweep).unwrap() && out.body.contains("|| echo 'weft: the compile cache sweep failed"),
            "the sweep runs after the binary is out and can never fail the build: {}",
            out.body
        );
    }

    /// The cache sweep keeps what this build linked (even when cargo
    /// reused it untouched), expires per-project crates no build has
    /// linked within the retention period (an orphan without its dep-info
    /// included, and the top worker crate's per-project copies), and
    /// never touches the shared crates.
    #[test]
    fn the_cache_sweep_expires_only_per_project_crates_no_build_linked() {
        let root = tempfile::tempdir().unwrap();
        let release = root.path().join("release");
        let deps = release.join("deps");
        let fingerprints = release.join(".fingerprint");
        let work = root.path().join("work");
        std::fs::create_dir_all(work.join("pkg_basic-1111/src")).unwrap();
        std::fs::create_dir_all(&deps).unwrap();
        let old = filetime::FileTime::from_unix_time(
            filetime::FileTime::now().unix_seconds() - 40 * 24 * 3600, 0);
        let age = |name: &str| filetime::set_file_mtime(deps.join(name), old).unwrap();
        // Cargo writes a path dependency's crate root relative to the
        // workspace root (checked against a real `cargo build`), plus the
        // absolute `#[path]` includes.
        let pkg_crate = |stem: &str, slot_dir: &str| {
            std::fs::write(deps.join(format!("{stem}.d")),
                format!("deps/lib{stem}.rlib: {slot_dir}/src/lib.rs /weft/project-nodes/basic/text/mod.rs\n")).unwrap();
            std::fs::write(deps.join(format!("lib{stem}.rlib")), b"rlib").unwrap();
            std::fs::write(deps.join(format!("lib{stem}.rmeta")), b"rmeta").unwrap();
            std::fs::create_dir_all(fingerprints.join(stem)).unwrap();
            for file in [format!("{stem}.d"), format!("lib{stem}.rlib"), format!("lib{stem}.rmeta")] { age(&file); }
        };
        pkg_crate("pkg_basic-aaaa", "pkg_basic-1111"); // linked by this build: kept
        pkg_crate("pkg_basic-bbbb", "pkg_basic-2222"); // an earlier content, unused: expired
        std::fs::write(deps.join("libpkg_ai-cccc.rlib"), b"orphan").unwrap(); // no dep-info left: expired
        age("libpkg_ai-cccc.rlib");
        // A dep-info whose rlib is gone but which names a staged slot: marked,
        // and no junk file is created for the missing rlib.
        std::fs::write(deps.join("pkg_lonely-gggg.d"), b"deps/libpkg_lonely-gggg.rlib: pkg_basic-1111/src/lib.rs\n").unwrap();
        age("pkg_lonely-gggg.d");
        let scripts = release.join("build");
        for stem in ["worker_test-hhhh", "worker_test-iiii"] {
            std::fs::create_dir_all(scripts.join(stem).join("out")).unwrap();
        }
        filetime::set_file_mtime(scripts.join("worker_test-iiii"), old).unwrap();
        for stem in ["worker_test-dddd", "worker_test-eeee"] { // the top crate, one copy per project
            std::fs::write(deps.join(stem), b"bin").unwrap();
            std::fs::write(deps.join(format!("{stem}.d")), b"x").unwrap();
            std::fs::create_dir_all(fingerprints.join(stem)).unwrap();
        }
        age("worker_test-eeee"); age("worker_test-eeee.d");
        std::fs::write(deps.join("weft_engine-ffff.d"), "x").unwrap(); // shared: never touched
        std::fs::write(deps.join("libweft_engine-ffff.rlib"), "x").unwrap();
        age("weft_engine-ffff.d"); age("libweft_engine-ffff.rlib");

        let script = root.path().join(CACHE_GC_SCRIPT_NAME);
        std::fs::write(&script, CACHE_GC_SCRIPT).unwrap();
        let status = std::process::Command::new("sh")
            .arg(&script).arg(&release).arg(WORKER_CACHE_RETENTION_DAYS.to_string()).arg(&work).arg("worker_test")
            .status().unwrap();
        assert!(status.success());

        assert!(deps.join("libpkg_basic-aaaa.rlib").exists() && fingerprints.join("pkg_basic-aaaa").exists(), "the linked crate stays");
        for file in ["pkg_basic-aaaa.d", "libpkg_basic-aaaa.rlib", "libpkg_basic-aaaa.rmeta"] {
            let marked = std::fs::metadata(deps.join(file)).unwrap().modified().unwrap();
            assert!(marked.elapsed().unwrap().as_secs() < 3600, "{file} is marked used now");
        }
        for gone in ["libpkg_basic-bbbb.rlib", "pkg_basic-bbbb.d", "libpkg_basic-bbbb.rmeta", "libpkg_ai-cccc.rlib", "worker_test-eeee", "worker_test-eeee.d"] {
            assert!(!deps.join(gone).exists(), "{gone} expired");
        }
        assert!(!fingerprints.join("pkg_basic-bbbb").exists() && !fingerprints.join("worker_test-eeee").exists());
        assert!(deps.join("worker_test-dddd").exists() && fingerprints.join("worker_test-dddd").exists(), "this build's worker crate stays");
        assert!(deps.join("libweft_engine-ffff.rlib").exists(), "shared crates are not the sweep's business");
        let fingerprint_marked = std::fs::metadata(fingerprints.join("pkg_basic-aaaa")).unwrap().modified().unwrap();
        assert!(fingerprint_marked.elapsed().unwrap().as_secs() < 3600, "the linked crate's fingerprint is marked too");
        assert!(deps.join("pkg_lonely-gggg.d").exists(), "a marked dep-info stays");
        assert!(std::fs::read_dir(&deps).unwrap().all(|entry| !entry.unwrap().file_name().to_string_lossy().contains('*')),
            "an unmatched glob never becomes a file");
        assert!(scripts.join("worker_test-hhhh").exists(), "a fresh build-script unit stays");
        assert!(!scripts.join("worker_test-iiii").exists(), "an old build-script unit goes");
    }
}
