//! Compile pipeline orchestration.
//!
//! Given a project root (containing `weft.toml` + `main.weft`),
//! parse + enrich + validate + codegen the generated cargo crate
//! to `.weft/target/build/`, then emit the multi-stage
//! Dockerfile + stage the docker build context. The actual
//! `cargo build` runs INSIDE that docker build, not on the host.
//! The host only needs docker.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::codegen;
use crate::error::{CompileError, CompileResult};
use crate::project::Project;
use crate::validate::ValidationMode;
use crate::worker_image;
use weft_catalog::FsCatalog;

/// Build-phase artifact. The host never holds a compiled binary
/// (cargo runs inside the builder container). What we return is
/// the staged docker build context + metadata the CLI uses to
/// call `docker build` and surface package info to the user.
pub struct BuildResult {
    /// Absolute path to the docker build context the CLI should
    /// feed to `docker build`. Contains `Dockerfile`, `build/`
    /// (the generated cargo crate), and, when the pre-built builder
    /// base is not in play, `weft/` (the worker-linked slice of the
    /// weft workspace, see `stage_worker_workspace`).
    pub build_context: PathBuf,
    /// Path to the generated Dockerfile inside the build context.
    /// Convenience for CLI callers that pass `-f` explicitly.
    pub dockerfile: PathBuf,
    /// Node types this project references, for CLI diagnostics.
    pub referenced_nodes: BTreeSet<String>,
    /// Summary of the generated Dockerfile for user-facing logs.
    pub dockerfile_summary: worker_image::WorkerDockerfile,
    /// Sanitized crate name; the CLI uses this to tag the worker
    /// image and to know what binary path the Dockerfile produces.
    pub binary_name: String,
}

/// Validate + codegen + stage from an ALREADY-COMPILED definition. Pipeline:
///
/// 1. Validate the (resolved) definition, abort on any error.
/// 2. Codegen the cargo crate at `.weft/target/build/`.
/// 3. Emit the multi-stage Dockerfile at
///    `.weft/target/Dockerfile.worker`.
/// 4. Stage the docker build context at
///    `.weft/target/worker-image/` with `build/` and, when not
///    using the pre-built builder base, `weft/` too; the Dockerfile
///    is laid out so `docker build .` works.
///
/// Takes the definition + catalog the caller already produced rather than
/// recompiling from source: the caller resolves `@asset` refs into the
/// definition (deferred file markers become concrete file values) BEFORE
/// this runs, and a fresh compile-from-source here would see the raw
/// markers instead (a file-typed `@asset` reads as a `String` at parse and
/// would fail validation against its `File` port). This is also the only
/// validation gate on the build path, so it runs on the resolved
/// definition, not the raw source. `Structural` (not `Runtime`): a project
/// may build without every secret filled; runtime-rule gaps surface at run.
///
/// `builder_base_tag` is the shared `weft-builder-base:<hash>` tag
/// the CLI computed + ensured. When it kicks in (debian-family
/// runtime, no custom template), the build context omits `weft/`
/// because the engine workspace already lives inside the base
/// image at `/weft/`.
///
/// `_release` is currently unused because cargo invocation moved
/// into the builder image, which always builds release. Kept on
/// the signature so future debug/release mode selection doesn't
/// require another plumbing pass.
pub fn build_project(
    project: &Project,
    definition: &weft_core::project::ProjectDefinition,
    catalog: &FsCatalog,
    _release: bool,
    builder_base_tag: &str,
) -> CompileResult<BuildResult> {
    let project_root = project.root.as_path();
    crate::bail_on_errors(crate::validate::validate_with_mode(
        definition,
        catalog,
        ValidationMode::Structural,
    ))?;
    // The crate/binary name is a project-identity property owned by the
    // manifest (`weft.toml` `[package] name`), not a parse-time property of the
    // graph. The parsed `ProjectDefinition` carries no name.
    let crate_name = project.manifest.package.name.clone();

    let crate_root = project_root.join(".weft").join("target").join("build");
    let referenced_nodes = codegen::collect_node_types(definition);
    codegen::emit(definition, project_root, &crate_root, catalog, &crate_name)?;
    let binary_name = sanitize_crate_name(&crate_name);

    let dockerfile_summary = worker_image::emit(
        &project.manifest.build.worker,
        project_root,
        &catalog,
        &referenced_nodes,
        &binary_name,
        builder_base_tag,
    )?;
    let dockerfile_path = project_root.join(".weft/target/Dockerfile.worker");
    if let Some(parent) = dockerfile_path.parent() {
        std::fs::create_dir_all(parent).map_err(CompileError::Io)?;
    }
    std::fs::write(&dockerfile_path, &dockerfile_summary.body).map_err(CompileError::Io)?;

    let weft_root = resolve_weft_root()?;
    // The pre-built builder base bakes `/weft/` (the engine workspace)
    // into its layers, so the per-project Dockerfile no longer COPYs
    // it. Skip the host-side stage of `weft/` in that case (a
    // unneeded COPY into the context would be wasted bytes + a slower
    // tarball for the docker build).
    let stage_weft = dockerfile_summary.builder_base.is_none();
    let build_context = stage_build_context(
        project_root,
        &crate_root,
        &weft_root,
        &dockerfile_path,
        &catalog,
        &referenced_nodes,
        stage_weft,
    )?;

    Ok(BuildResult {
        build_context,
        dockerfile: dockerfile_path,
        referenced_nodes,
        dockerfile_summary,
        binary_name,
    })
}

/// Stage the docker build context under
/// `.weft/target/worker-image/`. Layout:
///
/// ```text
/// worker-image/
///   Dockerfile           (copy of Dockerfile.worker)
///   build/               (the generated cargo crate)
///   weft/                (the weft workspace: crates, Cargo.toml, Cargo.lock)
///   project-nodes/       (the project's nodes/ dir: every node's source)
/// ```
///
/// `weft/` carries only the language runtime (the workspace crates the
/// generated binary depends on as path deps), NOT any node code. Node
/// source comes entirely from `project-nodes/`: the project owns all
/// its nodes. The copies are minimal so docker's tarball and layer
/// cache aren't churned by unrelated host artifacts (`.git`, `target/`,
/// `node_modules/`).
///
/// `project-nodes/` holds ONLY the package roots the project actually
/// references, each copied to its path relative to `nodes/` so the
/// `#[path]` shims resolve. Discovery already walked `nodes/` and
/// grouped nodes under package roots; staging copies those roots
/// rather than re-walking the tree, so there is a single directory
/// walker (discovery) and nothing for a second walker to disagree
/// with (e.g. on symlink handling). Unreferenced nodes never enter the
/// build context.
fn stage_build_context(
    project_root: &Path,
    crate_root: &Path,
    weft_root: &Path,
    dockerfile_path: &Path,
    catalog: &FsCatalog,
    referenced_nodes: &BTreeSet<String>,
    stage_weft: bool,
) -> CompileResult<PathBuf> {
    let ctx = project_root.join(".weft").join("target").join("worker-image");
    if ctx.exists() {
        std::fs::remove_dir_all(&ctx).map_err(CompileError::Io)?;
    }
    std::fs::create_dir_all(&ctx).map_err(CompileError::Io)?;

    std::fs::copy(dockerfile_path, ctx.join("Dockerfile")).map_err(CompileError::Io)?;

    // `build/` = generated cargo crate. Copy target excluded; it
    // doesn't exist on the host anymore (no host cargo build), but
    // belt-and-suspenders.
    copy_dir_filtered(crate_root, &ctx.join("build"), &["target"])?;

    // `weft/` = the language runtime workspace (crates + manifest).
    // Staged into the build context only when the project Dockerfile
    // needs to COPY it in (no pre-built builder base, e.g. custom
    // template or non-debian runtime). When the pre-built base is
    // used, `/weft/` lives in the base image layers and re-COPYing
    // it would just bloat the build context tarball.
    if stage_weft {
        stage_worker_workspace(weft_root, &ctx.join("weft"))?;
    }

    // `project-nodes/` = each referenced package root, placed at its
    // path relative to `nodes/` so the `#[path]` includes resolve.
    let nodes_root = project_root.join("nodes");
    let project_nodes = ctx.join("project-nodes");
    for root in catalog.package_roots_for(referenced_nodes) {
        let rel = root.strip_prefix(&nodes_root).map_err(|_| {
            CompileError::Build(format!(
                "package root {} is not under project nodes root {}",
                root.display(),
                nodes_root.display()
            ))
        })?;
        // The shared node-tree exclude (same set the source hash walks
        // over), so staging and hashing agree on a node's byte-content:
        // a file the build copies but the hash skips (or vice versa)
        // is how a stale worker image gets served.
        copy_dir_filtered(&root, &project_nodes.join(rel), weft_catalog::NODE_TREE_EXCLUDE)?;
    }

    Ok(ctx)
}

/// Top-level entries of a closure crate that enter the worker slice:
/// everything except the crate-ROOT `tests/` dir (integration tests cargo
/// never compiles for a path dependency, so staging or hashing them only
/// makes an unrelated test edit rebuild the base and every worker image)
/// and the shared node-tree excludes. Root-anchored on purpose: a dir named
/// `tests` deeper in the tree (weft-core's `src/tests`, reached via a
/// `#[path]` attribute) stays in, so the exclusion can never silently drop
/// a module the crate actually declares. THE one enumerator both the stager
/// (`stage_worker_workspace`) and the hasher (`hash::hash_worker_build_env`)
/// iterate, so staged and hashed bytes stay equal by construction. Sorted
/// for deterministic hashing.
pub fn worker_crate_entries(crate_dir: &Path) -> CompileResult<Vec<(String, PathBuf)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(crate_dir).map_err(CompileError::Io)? {
        let entry = entry.map_err(CompileError::Io)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "tests" || weft_catalog::is_node_tree_excluded(&name) {
            continue;
        }
        // Symlinks are skipped everywhere in the slice (stager and hasher
        // both), matching the deeper walks.
        if entry.file_type().map_err(CompileError::Io)?.is_symlink() {
            continue;
        }
        out.push((name, entry.path()));
    }
    out.sort();
    Ok(out)
}

/// Stage the worker-linked slice of the weft workspace at `dest`:
/// `crates/<closure>` (exactly `codegen::worker_workspace_crates`, the only
/// workspace crates a worker build links), a workspace manifest whose members
/// are rewritten to that closure, and the workspace `Cargo.lock`. Everything
/// else in the workspace (CLI, dispatcher, tests) is deliberately absent, so
/// it can neither bloat the docker context nor invalidate a worker build.
///
/// The staged set must match what `hash::hash_worker_build_env` hashes
/// (closure crates + manifests): a file staged-but-not-hashed (or vice versa)
/// is a stale-image hole. Crate copies preserve mtimes (see
/// `copy_dir_filtered`) so cargo's fingerprint inside the container sees
/// unchanged files as unchanged; the generated manifest + copied lock get the
/// SOURCE files' mtimes stamped for the same reason.
pub fn stage_worker_workspace(weft_root: &Path, dest: &Path) -> CompileResult<()> {
    std::fs::create_dir_all(dest).map_err(CompileError::Io)?;
    let closure = codegen::worker_workspace_crates(weft_root)?;
    for name in &closure {
        let crate_dest = dest.join("crates").join(name);
        std::fs::create_dir_all(&crate_dest).map_err(CompileError::Io)?;
        for (entry_name, entry_path) in
            worker_crate_entries(&weft_root.join("crates").join(name))?
        {
            let to = crate_dest.join(&entry_name);
            if entry_path.is_dir() {
                copy_dir_filtered(&entry_path, &to, weft_catalog::NODE_TREE_EXCLUDE)?;
            } else if entry_path.is_file() {
                std::fs::copy(&entry_path, &to).map_err(CompileError::Io)?;
                mirror_mtime(&entry_path, &to);
            }
        }
    }
    write_scoped_workspace_manifest(weft_root, dest, &closure)?;
    let lock_src = weft_root.join("Cargo.lock");
    let lock_dst = dest.join("Cargo.lock");
    std::fs::copy(&lock_src, &lock_dst).map_err(CompileError::Io)?;
    mirror_mtime(&lock_src, &lock_dst);
    Ok(())
}

/// Write `dest/Cargo.toml`: the real workspace manifest with `members`
/// rewritten to exactly `crates`. Everything else ([workspace.dependencies],
/// [workspace.package], profiles) is carried verbatim, so `workspace = true`
/// inheritance in the staged crates resolves identically to the full
/// workspace. Extra `[workspace.dependencies]` path entries pointing at
/// absent crates are harmless: cargo only opens a path when something in the
/// resolve graph references it, and nothing in the staged slice does.
fn write_scoped_workspace_manifest(
    weft_root: &Path,
    dest: &Path,
    crates: &[String],
) -> CompileResult<()> {
    let src = weft_root.join("Cargo.toml");
    let raw = std::fs::read_to_string(&src).map_err(CompileError::Io)?;
    let mut manifest: toml::Value = raw
        .parse()
        .map_err(|e| CompileError::Build(format!("parse {}: {e}", src.display())))?;
    let workspace = manifest
        .get_mut("workspace")
        .and_then(|w| w.as_table_mut())
        .ok_or_else(|| {
            CompileError::Build(format!("{} has no [workspace] table", src.display()))
        })?;
    workspace.insert(
        "members".into(),
        toml::Value::Array(
            crates
                .iter()
                .map(|c| toml::Value::String(format!("crates/{c}")))
                .collect(),
        ),
    );
    let body = toml::to_string_pretty(&manifest)
        .map_err(|e| CompileError::Build(format!("serialize scoped workspace manifest: {e}")))?;
    let dst = dest.join("Cargo.toml");
    std::fs::write(&dst, body).map_err(CompileError::Io)?;
    mirror_mtime(&src, &dst);
    Ok(())
}

/// Stamp `src`'s mtime onto `dst`. Cargo's fingerprint inside the docker
/// build container short-circuits a clean crate only when every source
/// file's mtime is older than its rlib; without mirroring, every staging
/// run gives cargo a wall-clock mtime and every crate looks dirty on every
/// build. Warn-only on failure: correctness is unaffected (only rebuild
/// time), but an invisible failure would look like the cache mysteriously
/// stopped working, so it must be loud.
fn mirror_mtime(src: &Path, dst: &Path) {
    let result = std::fs::metadata(src)
        .and_then(|meta| meta.modified())
        .and_then(|modified| {
            filetime::set_file_mtime(dst, filetime::FileTime::from_system_time(modified))
        });
    if let Err(e) = result {
        tracing::warn!(
            target: "weft_compiler::build",
            file = %dst.display(),
            error = %e,
            "could not mirror source mtime; the docker build will treat this \
             file as changed and rebuild its crate"
        );
    }
}

/// Stage the builder-base docker build context at
/// `<weft_root>/.weft-base-context/` and return its path. Layout matches what
/// the rendered Dockerfile COPYs:
///
/// ```text
/// .weft-base-context/
///   Dockerfile           (worker-builder-base.Dockerfile with the target
///                         cache key substituted; build with -f THIS file)
///   rust-toolchain.toml
///   Cargo.toml           (workspace manifest scoped to the worker closure)
///   Cargo.lock
///   crates/<closure>/    (only the worker-linked crates)
///   .weft-warmup/        (generated warm-up crate, see codegen::emit_warmup_crate)
/// ```
///
/// The Dockerfile is RENDERED, not copied: its `{{target_cache_key}}` token
/// becomes a hash of Cargo.lock + rust-toolchain.toml, so a dependency or
/// toolchain change starts a fresh compile cache instead of inheriting (and
/// baking into the image) every artifact ever built. The base hash keeps
/// covering the SOURCE template; the key is a pure function of two files the
/// hash already covers.
///
/// Regenerated wholesale on every call (it is a derived artifact): a stale
/// leftover crate from a previous closure would otherwise linger in the
/// context and the baked image. Callers only invoke this when the
/// content-addressed base tag is absent, so the staging cost is paid exactly
/// once per base rebuild.
pub fn stage_builder_base_context(weft_root: &Path) -> CompileResult<PathBuf> {
    let ctx = weft_root.join(worker_image::BASE_CONTEXT_DIR);
    if ctx.exists() {
        std::fs::remove_dir_all(&ctx).map_err(CompileError::Io)?;
    }
    stage_worker_workspace(weft_root, &ctx)?;
    let toolchain_src = weft_root.join("rust-toolchain.toml");
    let toolchain_dst = ctx.join("rust-toolchain.toml");
    std::fs::copy(&toolchain_src, &toolchain_dst).map_err(CompileError::Io)?;
    mirror_mtime(&toolchain_src, &toolchain_dst);
    codegen::emit_warmup_crate(&ctx.join(worker_image::WARMUP_CRATE_DIR), weft_root)?;

    let template_path = weft_root.join(crate::hash::BUILDER_BASE_DOCKERFILE);
    let template = std::fs::read_to_string(&template_path).map_err(CompileError::Io)?;
    let key = target_cache_key(weft_root)?;
    if !template.contains("{{target_cache_key}}") {
        return Err(CompileError::Build(format!(
            "{} lost its {{{{target_cache_key}}}} token; the base compile cache \
             would stop keying on the lock + toolchain",
            template_path.display()
        )));
    }
    std::fs::write(
        ctx.join("Dockerfile"),
        template.replace("{{target_cache_key}}", &key),
    )
    .map_err(CompileError::Io)?;
    Ok(ctx)
}

/// The builder-base compile-cache key: a short hash of `Cargo.lock` +
/// `rust-toolchain.toml`. Exactly the inputs whose change makes previous
/// cache contents dead weight (new dependency versions, new toolchain);
/// engine source edits keep the key, so they stay incremental.
fn target_cache_key(weft_root: &Path) -> CompileResult<String> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for rel in ["Cargo.lock", "rust-toolchain.toml"] {
        hasher.update(
            std::fs::read(weft_root.join(rel)).map_err(CompileError::Io)?,
        );
    }
    let digest = hasher.finalize();
    Ok(digest
        .iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect::<String>())
}

/// Recursive directory copy with a simple exclude list matched on
/// the immediate entry name. Skips symlinks to avoid infinite
/// recursion and unexpected escapes from the staged context.
///
/// Preserves mtime on every copied file. The staged build context
/// feeds straight into docker, which preserves the host mtime on
/// `COPY`. Cargo's fingerprint short-circuits a clean crate when
/// every source file's mtime is older than the crate's rlib in the
/// target dir; without mtime preservation a fresh stage gives every
/// file a wall-clock mtime, every per-package crate looks dirty,
/// and cargo recompiles all of them on every edit. Preserving the
/// host mtime keeps unchanged sources looking unchanged inside the
/// container, so cargo only rebuilds the package whose node source
/// genuinely changed (plus the worker relink).
pub(crate) fn copy_dir_filtered(src: &Path, dst: &Path, exclude: &[&str]) -> CompileResult<()> {
    std::fs::create_dir_all(dst).map_err(CompileError::Io)?;
    for entry in std::fs::read_dir(src).map_err(CompileError::Io)? {
        let entry = entry.map_err(CompileError::Io)?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if exclude.iter().any(|e| *e == name_str) {
            continue;
        }
        let ft = entry.file_type().map_err(CompileError::Io)?;
        let from = entry.path();
        let to = dst.join(&name);
        if ft.is_symlink() {
            continue;
        }
        if ft.is_dir() {
            copy_dir_filtered(&from, &to, exclude)?;
        } else {
            std::fs::copy(&from, &to).map_err(CompileError::Io)?;
            mirror_mtime(&from, &to);
        }
    }
    Ok(())
}

/// Locate the weft workspace root (honors `WEFT_REPO_ROOT`, else the repo layout).
/// Delegates to `weft_catalog::weft_repo_root` so this and the catalog's
/// `stdlib_root` resolve to the SAME path (they used to be two independent copies;
/// a drift would have the stdlib seed and the build context disagree). Public so
/// the CLI's hash + docker-build paths share one resolver.
pub fn resolve_weft_root() -> CompileResult<PathBuf> {
    weft_catalog::weft_repo_root()
        .ok_or_else(|| CompileError::Build("cannot resolve weft workspace root".into()))
}

/// The repository segment for content-addressed worker images. No project id:
/// a tag is purely a function of what was compiled, so identical builds across
/// projects/tenants resolve to ONE image (the same shape infra images use).
pub const WORKER_IMAGE_REPO: &str = "weft-worker";

/// The bare (registry-UNqualified) content-addressed worker image tag,
/// `weft-worker:<binary_hash>`. THE single source of truth for the worker tag,
/// shared by the CLI (which builds + loads it onto the node) and the dispatcher
/// (which spawns it, prepending a registry prefix when one is configured). The
/// FULL binary hash is used (not a short prefix) so the tag is collision-free and the CLI and
/// the dispatcher agree by construction. Lives in weft-compiler because both the
/// CLI and the dispatcher depend on this crate; a second copy would be a drift
/// hazard (the exact bug that had the CLI tag a 16-char prefix while the
/// dispatcher spawned the full hash).
pub fn worker_image_tag(binary_hash: &str) -> String {
    format!("{WORKER_IMAGE_REPO}:{binary_hash}")
}

/// Sanitize a project name to a valid cargo crate + binary name.
/// Used by both codegen and the CLI so the two agree on what
/// binary the Dockerfile produces.
pub fn sanitize_crate_name(raw: &str) -> String {
    let lowered = raw.to_ascii_lowercase();
    let mut out = String::with_capacity(lowered.len());
    for ch in lowered.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() || out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, 'p');
    }
    out
}

/// Build the catalog for a project: discover every node under its
/// `nodes/` directory. That is the single source of truth (the stdlib
/// is cloned in at `weft new`), so the project is self-contained and
/// nothing reaches into the weft installation at build time.
pub fn build_project_catalog(project_root: &Path) -> CompileResult<FsCatalog> {
    FsCatalog::discover(&project_root.join("nodes"))
        .map_err(|e| CompileError::Enrich(format!("catalog: {e}")))
}
