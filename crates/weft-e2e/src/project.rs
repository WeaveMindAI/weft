//! Fixture -> isolated, live project lifecycle.
//!
//! A fixture is a real weft project committed under `crates/weft-e2e/fixtures/
//! <name>/` (a `weft.toml`, a `main.weft`, and any custom nodes under
//! `nodes/`). It deliberately does NOT commit `nodes/base_catalog/`: that
//! built-in-node mirror is regenerated from current code by `weft catalog
//! update` during [`Project::prepare`], which is the whole point (built-in
//! nodes are tested against the code in this worktree, not a stale copy).
//!
//! [`Project::prepare`] copies the fixture to a temp dir, rewrites its id to a
//! fresh UUID (so concurrent / repeated runs never collide on the dispatcher),
//! refreshes the catalog, and is then ready to build/run/activate.
//!
//! Teardown is EXPLICIT and lives in the shared [`crate::teardown::Teardown`]
//! guard (so the local CLI suite and an API-driven HTTP suite cannot drift on the
//! policy): a passing test ends with `project.finish().await?`, which
//! deactivates + removes the project from the dispatcher, deletes the temp copy,
//! and marks the guard done, all awaited so a teardown failure surfaces loudly.
//! A test that panics / returns early never reaches `finish`, so the guard's
//! [`Drop`] is only a safety net: it leaves the remote project up (for
//! post-mortem) and just warns, pointing at the temp dir and id to inspect. We
//! do NOT do remote teardown in Drop (it cannot await, and a detached spawn
//! would race the process exit and orphan projects anyway).

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use uuid::Uuid;

use crate::client::{cli_ok, Dispatcher};
use crate::teardown::Teardown;

/// Root of the committed fixtures directory, resolved from this crate's
/// manifest dir so it is correct regardless of the test binary's cwd.
fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

/// A prepared, isolated copy of a fixture project living in a temp directory,
/// with a fresh id, ready to drive against the live system.
pub struct Project {
    /// Fresh project id (the rewritten `weft.toml` package.id).
    id: Uuid,
    /// Temp working directory holding the isolated copy.
    dir: PathBuf,
    /// Dispatcher client, shared from the suite's ensured-up system.
    disp: Dispatcher,
    /// The shared clean-on-pass / keep-and-warn-on-fail guard. Owns the id's
    /// registered/finished bookkeeping + the Drop warning, so this CLI fixture
    /// and an API-driven HTTP fixture share ONE teardown policy.
    teardown: Teardown,
}

impl Project {
    /// Prepare an isolated copy of `fixture` against the ensured-up system.
    /// Copies the fixture to a temp dir, mints a fresh id, and refreshes the
    /// built-in node catalog from current code. Does NOT build or activate yet
    /// (the test chooses run vs activate vs infra).
    pub async fn prepare(fixture: &str, disp: Dispatcher) -> Result<Self> {
        let src = fixtures_root().join(fixture);
        if !src.is_dir() {
            bail!(
                "fixture '{fixture}' not found at {} (add it under crates/weft-e2e/fixtures/)",
                src.display()
            );
        }
        let dir = unique_tempdir(fixture)?;
        copy_tree(&src, &dir)
            .with_context(|| format!("copy fixture {fixture} to {}", dir.display()))?;

        let id = Uuid::new_v4();
        rewrite_project_id(&dir, id)
            .with_context(|| format!("rewrite weft.toml id for {fixture}"))?;

        // Refresh built-in nodes from THIS worktree's catalog (the freshly
        // built CLI's stdlib_root points here), preserving any custom nodes the
        // fixture committed under nodes/. This is what makes the rig test
        // current node code rather than a stale mirror.
        cli_ok(&dir, &["catalog", "update"])
            .await
            .with_context(|| format!("catalog update for {fixture}"))?;

        // The recovery hint the guard's Drop prints if a test ends early: the
        // exact by-hand cleanup for a kept local project (remove it from the
        // dispatcher, then delete the temp copy).
        let recovery_hint = format!("`weft rm {id}` then `rm -rf {}`", dir.display());
        Ok(Self {
            id,
            dir,
            disp,
            teardown: Teardown::new(id, fixture, recovery_hint),
        })
    }

    /// The fresh project id.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// The temp working directory (where `weft` runs).
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The shared dispatcher client.
    pub fn dispatcher(&self) -> &Dispatcher {
        &self.disp
    }

    /// Run `weft <args>` in this project, requiring success, returning stdout.
    pub async fn weft(&self, args: &[&str]) -> Result<String> {
        cli_ok(&self.dir, args).await
    }

    /// Substitute `placeholder` with `value` everywhere in the project's
    /// `main.weft`. Used for reach-out fixtures whose trigger URL must point at
    /// a fake server the rig stood up at a port only known at runtime: the
    /// fixture commits a placeholder token (e.g. `__E2E_FAKE_URL__`) and the
    /// test rewrites it to the cluster-reachable fake URL before building. Call
    /// BEFORE [`Project::activate`] / a run so the compiled graph carries the
    /// real URL. Errors if the placeholder is absent (a fixture/test mismatch we
    /// want loud, never a silent no-op that ships a placeholder to the compiler).
    pub fn substitute_in_main(&self, placeholder: &str, value: &str) -> Result<()> {
        let path = self.dir.join("main.weft");
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("read {}", path.display()))?;
        if !raw.contains(placeholder) {
            bail!(
                "placeholder '{placeholder}' not found in {}; fixture and test disagree",
                path.display()
            );
        }
        let out = raw.replace(placeholder, value);
        std::fs::write(&path, out).with_context(|| format!("write {}", path.display()))?;
        Ok(())
    }

    /// Replace the project's `main.weft` wholesale. Transition tests evolve
    /// ONE project through several graph shapes (no-infra -> infra -> trigger
    /// -> ...) exactly as a user editing source would; the next verb picks the
    /// new shape up. The old content is discarded on purpose (a user's editor
    /// save), so no placeholder checking here; the caller writes complete,
    /// final source.
    pub fn set_main(&self, contents: &str) -> Result<()> {
        let path = self.dir.join("main.weft");
        std::fs::write(&path, contents).with_context(|| format!("write {}", path.display()))?;
        Ok(())
    }

    /// Set one config field on a node in the project's `main.weft`, exactly as
    /// the editor does it: parse the source to a lossless tree, apply a
    /// structural `SetConfig` op, reserialize. `value` is a `.weft` literal
    /// expression (`"a string"`, `42`, `true`), so a caller passing a string
    /// wraps it in quotes (e.g. via `format!("{v:?}")`). Call BEFORE a run so
    /// the compiled graph carries it. No string-surgery on source: the edit is
    /// the same operation a click in the editor performs.
    pub fn set_node_config(&self, node: &str, key: &str, value: &str) -> Result<()> {
        let path = self.dir.join("main.weft");
        let source = std::fs::read_to_string(&path)
            .with_context(|| format!("read {}", path.display()))?;
        let (edited, _inverse) = weft_compiler::edit::apply_edits(
            &source,
            None,
            // `main.weft`'s anonymous root takes the "Main" id; SetConfig
            // resolves the node against that, matching the lowering.
            "Main",
            &[weft_compiler::edit::EditOp::SetConfig {
                node: node.to_string(),
                key: key.to_string(),
                value: value.to_string(),
                form: None,
            }],
        )
        .map_err(|e| anyhow::anyhow!("set config {node}.{key} in {}: {e:?}", path.display()))?;
        std::fs::write(&path, edited).with_context(|| format!("write {}", path.display()))?;
        Ok(())
    }

    /// Copy a custom node from ANOTHER committed fixture into this project's
    /// `nodes/` directory (e.g. pull `infra_min`'s `mini_service` into a
    /// transition fixture before rewriting `main.weft` to use it). One
    /// committed source per node, shared across fixtures, instead of a copy
    /// per fixture that would drift.
    pub fn add_node_from_fixture(&self, fixture: &str, node: &str) -> Result<()> {
        let src = fixtures_root().join(fixture).join("nodes").join(node);
        if !src.is_dir() {
            bail!(
                "node '{node}' not found in fixture '{fixture}' at {}",
                src.display()
            );
        }
        let dst = self.dir.join("nodes").join(node);
        std::fs::create_dir_all(&dst)
            .with_context(|| format!("create node dir {}", dst.display()))?;
        copy_tree(&src, &dst)
            .with_context(|| format!("copy node {fixture}/{node} into {}", dst.display()))
    }

    /// Substitute the live-trigger mount-path placeholder `__E2E_PATH__` with a
    /// per-project-unique path, and return the CALLABLE path the test connects
    /// to. Mount paths are namespaced per tenant on the dispatcher, so the
    /// stored path (and the callable URL) is `/<tenant>/<path>`: the node config
    /// gets the bare `<path>`, but a caller reaches it at `/connect/<tenant>/<path>`
    /// (live) or `POST /<tenant>/<path>` (public fire). e2e runs as tenant
    /// `local`, so the callable path is `local/<path>`. The unique suffix is
    /// derived from the project's fresh id (stable within a run, distinct across
    /// runs). Call BEFORE activate.
    pub fn unique_live_path(&self) -> Result<String> {
        // First 12 hex of the id (sans hyphens): short, unique, path-safe.
        let suffix: String = self
            .id
            .simple()
            .to_string()
            .chars()
            .take(12)
            .collect();
        let path = format!("e2e-{suffix}");
        // The node config carries the BARE path; the dispatcher prefixes the
        // owning tenant when it stores + serves the mount path.
        self.substitute_in_main("__E2E_PATH__", &path)?;
        // The test connects at the tenant-namespaced path (e2e tenant = local).
        Ok(format!("local/{path}"))
    }

    /// Build the project's worker image (the real compile path). `weft build`
    /// compiles and builds the image but does not register with the dispatcher;
    /// registration happens through a mutating verb (a run or activate). Running
    /// build explicitly first means a later run/activate's build gate is a
    /// no-op. Optional: a plain run builds on its own.
    pub async fn build(&self) -> Result<()> {
        self.weft(&["build"]).await.map(|_| ())
    }

    /// Activate the project (build + register + enable triggers). Required for
    /// fixtures whose entry is a trigger (web, live, form, timer, feed). Marks
    /// the project registered so [`Project::finish`] removes it.
    pub async fn activate(&mut self) -> Result<()> {
        self.weft(&["activate"]).await?;
        self.teardown.mark_registered();
        Ok(())
    }

    /// Mark the project registered without going through activate. Used by the
    /// run path, where the first `weft run` builds + registers the project as a
    /// side effect, so teardown must still remove it.
    pub fn mark_registered(&mut self) {
        self.teardown.mark_registered();
    }

    /// End-of-test teardown for a PASSING test: remove the project from the
    /// dispatcher (deactivate + unregister, the real `weft rm` path) and delete
    /// the temp copy, all awaited so a teardown failure surfaces loudly rather
    /// than orphaning state. Then mark the shared guard done so its Drop stays
    /// silent. Call this as the last line of a passing test. (A removal failure
    /// returns early WITHOUT marking the guard done, so the guard keeps + warns,
    /// exactly as an early-exiting test does.)
    pub async fn finish(mut self) -> Result<()> {
        if self.teardown.registered() {
            // `weft rm <id>` deactivates then unregisters, exactly as a user
            // would clean up. Run by id so it is unambiguous.
            let id = self.id.to_string();
            cli_ok(&self.dir, &["rm", &id])
                .await
                .with_context(|| format!("teardown: weft rm {id}"))?;
        }
        std::fs::remove_dir_all(&self.dir)
            .with_context(|| format!("teardown: remove temp dir {}", self.dir.display()))?;
        self.teardown.complete();
        Ok(())
    }
}

/// Create a fresh temp directory for a fixture copy. Uses the system temp dir
/// plus a unique suffix so concurrent tests never share a path.
fn unique_tempdir(fixture: &str) -> Result<PathBuf> {
    let base = std::env::temp_dir().join(format!("weft-e2e-{fixture}-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&base)
        .with_context(|| format!("create temp dir {}", base.display()))?;
    Ok(base)
}

/// Recursively copy a directory tree. Skips `nodes/base_catalog` (regenerated
/// by catalog update) and any build/cache dirs so the copy is the SOURCE of the
/// fixture, never stale generated state.
fn copy_tree(src: &Path, dst: &Path) -> Result<()> {
    for entry in std::fs::read_dir(src).with_context(|| format!("read dir {}", src.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        // Never copy generated / cache trees: base_catalog is regenerated, and
        // .weft / target hold build output that must be fresh per isolated copy.
        if matches!(name_str.as_ref(), ".weft" | "target" | "node_modules" | ".git") {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        if entry.file_type()?.is_dir() {
            // Skip a committed base_catalog if one slipped in; catalog update
            // owns it.
            if from.ends_with("nodes/base_catalog") {
                continue;
            }
            std::fs::create_dir_all(&to)?;
            copy_tree(&from, &to)?;
        } else {
            if let Some(parent) = to.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::copy(&from, &to)
                .with_context(|| format!("copy {} -> {}", from.display(), to.display()))?;
        }
    }
    Ok(())
}

/// Rewrite the `package.id` in the copy's `weft.toml` to `new_id`, preserving
/// every other field. Parses + re-serializes via toml so we never string-munge
/// the manifest (which would be the kind of fragile patch the rules forbid).
fn rewrite_project_id(dir: &Path, new_id: Uuid) -> Result<()> {
    let path = dir.join("weft.toml");
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("read {}", path.display()))?;
    let mut doc: toml::Value = toml::from_str(&raw)
        .with_context(|| format!("parse {}", path.display()))?;
    let pkg = doc
        .get_mut("package")
        .and_then(|p| p.as_table_mut())
        .context("weft.toml missing [package] table")?;
    pkg.insert("id".to_string(), toml::Value::String(new_id.to_string()));
    let out = toml::to_string_pretty(&doc).context("re-serialize weft.toml")?;
    std::fs::write(&path, out).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}
