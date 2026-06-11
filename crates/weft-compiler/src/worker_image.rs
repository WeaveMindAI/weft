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
/// `/weft/project-nodes/basic/exec_python/mod.rs`). This is the only
/// place node source comes from: the project owns all its nodes.
pub const NODES_MOUNT: &str = "/weft/project-nodes";

/// Image repo for the shared pre-built builder base. Tagged by a
/// short hash of the engine workspace (`crates/`, `Cargo.toml`,
/// `Cargo.lock`, `rust-toolchain.toml`), so an engine bump produces
/// a fresh tag. The CLI builds + tags this image; per-project worker
/// Dockerfiles `FROM weft-builder-base:<hash>` in their builder
/// stage.
pub const BUILDER_BASE_REPO: &str = "weft-builder-base";

/// Compose the builder-base image tag from a workspace hash. The
/// hash is computed by the CLI (it knows the on-disk weft workspace
/// layout); the compiler stamps the tag into the generated
/// Dockerfile.
pub fn builder_base_tag(short_hash: &str) -> String {
    format!("{BUILDER_BASE_REPO}:{short_hash}")
}

/// Emit the Dockerfile for a project's worker image.
///
/// `project_root` is only used to resolve a relative
/// `dockerfile_template` path. `binary_name` is the crate's
/// `[package] name` so the builder stage knows which binary to
/// copy over. `referenced` is the set of node types the project
/// compiles against (from codegen's own walk), and
/// `referenced_package_roots` lists the catalog subdirectories
/// the build context must include (the codegen's `#[path]`
/// includes point at these).
///
/// `builder_base_tag` is the shared pre-built builder-base image
/// tag (`weft-builder-base:<hash>`). The CLI computes it from the
/// engine workspace hash and ensures the image exists. When the
/// user's runtime base is debian-family and they haven't supplied a
/// custom Dockerfile template, the builder stage `FROM`s this image
/// instead of re-installing rustup / build packages / re-fetching
/// the cargo registry per project. When no custom template is supplied
/// AND the runtime base is non-debian, we use the from-scratch template
/// instead (the builder must match the runtime ABI: glibc base vs musl
/// base). A user-supplied custom template is used verbatim and bypasses
/// both built-in templates.
pub fn emit(
    build: &WorkerBuildSection,
    project_root: &Path,
    catalog: &FsCatalog,
    referenced: &BTreeSet<String>,
    binary_name: &str,
    lock_key: &str,
    builder_base_tag: &str,
) -> CompileResult<WorkerDockerfile> {
    let base_image_str = build
        .base_image
        .clone()
        .unwrap_or_else(|| DEFAULT_BASE_IMAGE.to_string());
    let base = parse_base_image(&base_image_str);

    let build_packages =
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
    // FROM that tag and the image must exist before `docker build`.
    let builder_base_out = template
        .contains("{{builder_base_image}}")
        .then(|| builder_base_tag.to_string());

    let body = template
        .replace("{{base_image}}", &base.raw)
        .replace("{{builder_base_image}}", builder_base_tag)
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
        .replace("{{lock_key}}", lock_key)
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
/// build stages the project's `nodes/` under `NODES_MOUNT`; the node's
/// in-container path is its on-disk location relative to `nodes/`.
fn node_catalog_path(
    node_type: &str,
    source_dir: &Path,
    project_root: &Path,
) -> CompileResult<String> {
    let nodes_root = project_root.join("nodes");
    let rel = source_dir.strip_prefix(&nodes_root).map_err(|_| {
        CompileError::Build(format!(
            "node '{node_type}' source dir {} is not under project nodes root {}",
            source_dir.display(),
            nodes_root.display()
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
/// The two `--mount=type=cache` lines carry EXPLICIT `id=` values so
/// BuildKit reuses cache volumes across builds. Without an explicit
/// id, BuildKit auto-generates an id scoped to the build context, so
/// each build sees a fresh empty cache, cargo re-fetches the
/// registry index, and the entire project crate compiles from
/// scratch. The partitioning is deliberate per mount:
///
/// - The REGISTRY cache (`weft-worker-cargo-registry`) is shared by
///   every project: crates.io artifacts are immutable per
///   version+features, so cross-project sharing is safe and saves
///   the fetch.
/// - The TARGET cache is PER PROJECT
///   (`weft-worker-target-{{lock_key}}`). Sharing it would let cargo
///   link one project's compiled rlib into another's worker: two
///   projects carrying a same-named node package at the same
///   in-container path, with the build staging preserving host
///   mtimes, make cargo's mtime-based freshness check declare
///   project B's crate "fresh" against project A's fingerprint and
///   silently reuse A's code. Per-project incremental rebuilds (the
///   actual ~5s win for a single-node edit) are preserved; only
///   cross-project dep-compile sharing is given up, and correctness
///   beats that.
///
/// The Cargo.lock save/restore trick: the codegen emits per-build
/// the worker `Cargo.toml` + per-package `Cargo.toml` but never a
/// `Cargo.lock`. Without one, cargo re-resolves all 322+ deps
/// every build and writes a fresh `Cargo.lock`; that lock's mtime
/// flips every package crate's fingerprint dirty, so cargo
/// recompiles everything. We dodge that by stashing the lock
/// inside the persistent cache mount at
/// `/work/target/locks/<lock_key>.lock`. `lock_key` is the project
/// UUID, NOT the user-controlled `binary_name`: project UUIDs are
/// unique by construction, so both the lock file and the target
/// volume stay correctly partitioned.
const CARGO_BUILD_RUN_FRAGMENT: &str = concat!(
    "RUN --mount=type=cache,id=weft-worker-cargo-registry,target=/root/.cargo/registry,sharing=locked \\\n",
    "    --mount=type=cache,id=weft-worker-target-{{lock_key}},target=/work/target,sharing=locked \\\n",
    "    mkdir -p /work/target/locks \\\n",
    "    && if [ -f /work/target/locks/{{lock_key}}.lock ]; then cp /work/target/locks/{{lock_key}}.lock /work/Cargo.lock; fi \\\n",
    "    && cargo build --release \\\n",
    "    && cp /work/Cargo.lock /work/target/locks/{{lock_key}}.lock \\\n",
    "    && cp target/release/{{binary_name}} /worker\n",
);

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
        CARGO_BUILD_RUN_FRAGMENT,
        "\n",
        RUNTIME_STAGE_FRAGMENT,
    ]
    .concat()
}

/// Multi-stage template that FROMs the shared pre-built builder
/// base. The base image already has debian build packages, rustup
/// (with the workspace's pinned toolchain materialized), and the
/// cargo registry warmed against the workspace's `Cargo.lock`. The
/// builder stage here adds only what's project-specific: per-node
/// system build packages, the generated worker crate, and the
/// project's `nodes/` source tree. The base's `/weft/` directory
/// provides the engine workspace via path deps; no per-project
/// COPY of the workspace. Cache-mount and Cargo.lock mechanics are
/// documented on `CARGO_BUILD_RUN_FRAGMENT`.
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
        CARGO_BUILD_RUN_FRAGMENT,
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
        PackageManager::Apk => concat!(
            "RUN apk add --no-cache ca-certificates curl build-base\n",
        )
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
        PackageManager::Yum => concat!(
            "RUN yum install -y ca-certificates \\\n    && yum clean all\n",
        )
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
            "lock-key-test",
            "weft-builder-base:irrelevant",
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
        // The fallback (non-debian / custom-template) path must also
        // use the lock_key for Cargo.lock save/restore. Two alpine
        // projects whose `weft.toml` set the same `binary_name`
        // would otherwise clobber each other in the shared cache
        // mount, the exact bug the prebuilt-template fix addressed.
        assert!(
            out.body.contains("/work/target/locks/lock-key-test.lock"),
            "fallback template lock path uses lock_key, not binary_name: {}",
            out.body
        );
        assert!(
            !out.body.contains("/work/target/locks/worker_test.lock"),
            "fallback template must not embed the binary_name in the lock path: {}",
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
            "lock-key-test",
            "weft-builder-base:cafebabe",
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
            "00000000-0000-0000-0000-000000000001",
            "weft-builder-base:abcdef0123456789",
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
        // Lock file is keyed by project UUID, NOT the binary_name.
        assert!(
            out.body.contains("/work/target/locks/00000000-0000-0000-0000-000000000001.lock"),
            "lock path uses project_id (lock_key), not binary_name: {}",
            out.body
        );
        assert!(
            !out.body.contains("/work/target/locks/worker_test.lock"),
            "lock path must not embed the user-controlled binary_name: {}",
            out.body
        );
    }
}
