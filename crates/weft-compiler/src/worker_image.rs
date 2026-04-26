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
/// docker build context copies the entire weft repo to this
/// path, which gives the generated crate access to the
/// weft-engine / weft-core crates via `../weft/crates/*` path
/// dependencies, AND access to the catalog node sources via
/// `/weft/catalog/<pkg>/<node>/mod.rs` in the `#[path]` shims.
///
/// `{{catalog_path}}` substitutions in node `[build.env]` values
/// expand to the node's path under this mount (e.g.
/// `/weft/catalog/basic/exec_python`).
pub const WEFT_MOUNT: &str = "/weft";

/// In-container path to the catalog directory. Derived from
/// `WEFT_MOUNT` because catalog lives at `<weft>/catalog`.
pub const CATALOG_MOUNT: &str = "/weft/catalog";

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
pub fn emit(
    build: &WorkerBuildSection,
    project_root: &Path,
    catalog: &FsCatalog,
    referenced: &BTreeSet<String>,
    binary_name: &str,
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
    let build_env = collect_build_env(catalog, referenced)?;

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
        None => default_template().to_string(),
    };

    let body = template
        .replace("{{base_image}}", &base.raw)
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
        .replace("{{weft_mount}}", WEFT_MOUNT);

    Ok(WorkerDockerfile {
        body,
        base,
        build_packages,
        runtime_packages,
        build_env,
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
/// `{{catalog_path}}` expands to the node's in-container path
/// under `/catalog` (matches where the build context mounts it).
/// Conflicts on the same variable abort the build.
fn collect_build_env(
    catalog: &FsCatalog,
    referenced: &BTreeSet<String>,
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
        let catalog_path = node_catalog_path(catalog, node_type, source_dir)?;

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

/// Compute the in-container path for a node's source dir. The
/// docker build rebases the catalog under `CATALOG_MOUNT`
/// (`/weft/catalog`); the node's in-container path is its
/// on-disk location relative to the catalog root.
fn node_catalog_path(
    _catalog: &FsCatalog,
    node_type: &str,
    source_dir: &Path,
) -> CompileResult<String> {
    let catalog_root = weft_catalog::stdlib_root();
    let rel = source_dir.strip_prefix(&catalog_root).map_err(|_| {
        CompileError::Build(format!(
            "node '{node_type}' source dir {} is not under catalog root {}",
            source_dir.display(),
            catalog_root.display()
        ))
    })?;
    Ok(format!("{CATALOG_MOUNT}/{}", rel.display()))
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

/// Built-in multi-stage template.
///
/// Stage 1 (`builder`): installs build-time packages + rust via
/// rustup, copies the generated crate + referenced catalog
/// subfolders, runs `cargo build --release`, writes the binary
/// to `/worker`.
///
/// Stage 2 (runtime): installs runtime-only packages, copies the
/// binary from the builder.
///
/// `--mount=type=cache` on cargo's registry and the target dir
/// keeps repeat builds fast: first build is slow (fetch + compile
/// ~200 crates), subsequent builds reuse the cache. Requires
/// BuildKit (the Docker default since 23.0).
fn default_template() -> &'static str {
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
        "RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \\\n",
        "    | sh -s -- -y --default-toolchain stable --profile minimal\n",
        "ENV PATH=\"/root/.cargo/bin:${PATH}\"\n",
        "\n",
        "WORKDIR /work\n",
        "COPY build/ /work/\n",
        "COPY weft/ {{weft_mount}}/\n",
        "\n",
        "{{build_env_lines}}",
        "\n",
        "RUN --mount=type=cache,target=/root/.cargo/registry,sharing=locked \\\n",
        "    --mount=type=cache,target=/work/target,sharing=locked \\\n",
        "    cargo build --release \\\n",
        "    && cp target/release/{{binary_name}} /worker\n",
        "\n",
        "FROM {{base_image}}\n",
        "\n",
        "{{install_runtime_base}}",
        "{{install_runtime_system_packages}}",
        "\n",
        "COPY --from=builder /worker /usr/local/bin/worker\n",
        "ENTRYPOINT [\"/usr/local/bin/worker\"]\n",
    )
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
}
