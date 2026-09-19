//! Fixture -> isolated, live project lifecycle.
//!
//! A fixture is a real weft project committed under `crates/weft-e2e/fixtures/
//! <name>/` (a `weft.toml`, a `src/main.weft`, and any custom nodes under
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
    /// Extra working directories this project handed out
    /// ([`Self::second_checkout`]), removed with `dir` at teardown.
    /// They sit beside `dir` rather than inside it, because a copy of a
    /// tree cannot live in that tree, so nothing else would ever remove
    /// them: every test that asked for one used to leave its whole
    /// source tree (and, with the catalog, the generated catalog too)
    /// behind in the system temp dir for good.
    extra_dirs: std::sync::Mutex<Vec<PathBuf>>,
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
            extra_dirs: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// The fresh project id.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// The program as it compiles right now, from the working copy: what
    /// an assertion spelled the way the source reads (`triage.up`)
    /// resolves against. Read fresh each time, so a test that edits the
    /// source sees the edit.
    pub fn definition(&self) -> Result<weft_core::ProjectDefinition> {
        let project = weft_compiler::project::Project::load(&self.dir)
            .map_err(|e| anyhow::anyhow!("load {}: {e}", self.dir.display()))?;
        let (definition, _) = weft_compiler::hash::load_enriched_project(&project)
            .map_err(|e| anyhow::anyhow!("compile {}: {e}", self.dir.display()))?;
        Ok(definition)
    }

    /// Observe a run to its end and read it through this program, so
    /// every node in an assertion is spelled the way the source reads.
    pub async fn settled(&self, color: Uuid) -> Result<crate::run::SettledRun> {
        Ok(crate::run::SettledRun::observe(&self.disp, color).await?.reading(self.definition()?))
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

    /// A SECOND checkout of this same project: a copy of the working
    /// directory, carrying the same `weft.toml` and so the same project
    /// id.
    ///
    /// This is what makes a two-session race expressible. One working
    /// directory cannot hold two different edits at once, so running two
    /// commands from one directory tests two sessions doing the SAME
    /// thing; the interesting race is two people who edited differently
    /// and both saved. Run commands in the returned directory with
    /// `weft_e2e::client::cli`.
    pub fn second_checkout(&self) -> Result<PathBuf> {
        // A fresh path per call. Deriving it from the project's directory
        // name alone gave two calls in one test the SAME directory, and
        // the second copy then merged into the first instead of being an
        // independent checkout.
        let other = self.dir.parent().unwrap_or(&self.dir).join(format!(
            "{}-second-{}",
            self.dir.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default(),
            Uuid::new_v4()
        ));
        std::fs::create_dir_all(&other)?;
        self.extra_dirs.lock().expect("extra dirs").push(other.clone());
        // The same copy the rig uses to stage a fixture, so the second
        // checkout is source only: no `.weft`, no build output.
        copy_tree(&self.dir, &other)
            .with_context(|| format!("copy {} to {}", self.dir.display(), other.display()))?;
        Ok(other)
    }

    /// A second checkout WITH the built-in node catalog, for a test whose
    /// commands compile.
    ///
    /// `second_checkout` copies source only, and the catalog is generated
    /// rather than committed, so a plain copy has no `Text`, `Cast` or
    /// `Debug`: anything that compiles in it fails with unknown node
    /// types. A test that only checkpoints never noticed, because a
    /// checkpoint of an already-registered project compiles nothing, and
    /// it also silently recorded a DIFFERENT installed-weft identity in
    /// its manifest (the catalog hash of an empty folder), which is a
    /// manifest difference no edit of the test's caused.
    pub async fn second_checkout_with_catalog(&self) -> Result<PathBuf> {
        let other = self.second_checkout()?;
        cli_ok(&other, &["catalog", "update"])
            .await
            .with_context(|| format!("catalog update for the second checkout at {}", other.display()))?;
        Ok(other)
    }

    /// Run `weft <args>` EXPECTING a refusal: errors if it succeeds,
    /// otherwise returns stdout and stderr together so the test asserts
    /// on the refusal's message.
    pub async fn weft_refused(&self, args: &[&str]) -> Result<String> {
        let out = crate::client::cli(&self.dir, args).await?;
        anyhow::ensure!(!out.success, "`{}` unexpectedly succeeded:\n{}", out.invocation, out.stdout);
        Ok(format!("{}\n{}", out.stdout, out.stderr))
    }

    /// Write a project file (a path relative to the project root), the
    /// edit a person makes between two runs.
    pub fn write_file(&self, rel: &str, contents: &str) -> Result<()> {
        let path = self.dir.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
        }
        std::fs::write(&path, contents).with_context(|| format!("write {}", path.display()))
    }

    /// Read a project file back, to see what `weft branch` put there.
    pub fn read_file(&self, rel: &str) -> Result<String> {
        let path = self.dir.join(rel);
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))
    }

    /// Whether a project file exists on disk.
    pub fn has_file(&self, rel: &str) -> bool {
        self.dir.join(rel).is_file()
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
        let path = self.dir.join("src").join("main.weft");
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

    /// Replace `placeholder` in every `.weft` file under `src/`, included
    /// files too. Errors if no file carries it (the fixture and the test
    /// disagree).
    pub fn substitute_in_sources(&self, placeholder: &str, value: &str) -> Result<()> {
        let mut touched = 0usize;
        for path in weft_sources(&self.dir.join("src"))? {
            let raw = std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
            if !raw.contains(placeholder) {
                continue;
            }
            std::fs::write(&path, raw.replace(placeholder, value)).with_context(|| format!("write {}", path.display()))?;
            touched += 1;
        }
        if touched == 0 {
            bail!("placeholder '{placeholder}' not found under {}; fixture and test disagree", self.dir.join("src").display());
        }
        Ok(())
    }

    /// Replace the project's `main.weft` wholesale. Transition tests evolve
    /// ONE project through several graph shapes (no-infra -> infra -> trigger
    /// -> ...) exactly as a user editing source would; the next verb picks the
    /// new shape up. The old content is discarded on purpose (a user's editor
    /// save), so no placeholder checking here; the caller writes complete,
    /// final source.
    pub fn set_main(&self, contents: &str) -> Result<()> {
        let path = self.dir.join("src").join("main.weft");
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
        let path = self.dir.join("src").join("main.weft");
        let source = std::fs::read_to_string(&path)
            .with_context(|| format!("read {}", path.display()))?;
        // The fixture's own catalog registry: an edit touching a declared
        // type name resolves exactly like it does in the editor.
        let registry = weft_compiler::build::build_project_catalog(&self.dir)
            .map_err(|e| anyhow::anyhow!("catalog for {}: {e}", self.dir.display()))?
            .type_registry();
        let (edited, _inverse) = weft_compiler::edit::apply_edits(
            &source,
            // `main.weft`'s anonymous root takes the "Main" id; SetConfig
            // resolves the node against that, matching the lowering.
            "Main",
            &[weft_compiler::edit::EditOp::SetConfig {
                node: node.to_string(),
                key: key.to_string(),
                value: value.to_string(),
                form: None,
            }],
            registry,
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
        self.mount_at(&self.bare_live_path())
    }

    /// The bare path [`Self::unique_live_path`] would claim, without
    /// claiming it. For a test that hands one project's path to a
    /// SECOND project, which is the only way to build a collision the
    /// compiler cannot see: two files, each fine on its own.
    pub fn bare_live_path(&self) -> String {
        // First 12 hex of the id (sans hyphens): short, unique, path-safe.
        let suffix: String = self
            .id
            .simple()
            .to_string()
            .chars()
            .take(12)
            .collect();
        format!("e2e-{suffix}")
    }

    /// Mount this project's live triggers at `path`, whoever chose it,
    /// and answer the callable path.
    pub fn mount_at(&self, path: &str) -> Result<String> {
        // The node config carries the BARE path; the dispatcher prefixes the
        // owning tenant when it stores + serves the mount path.
        self.substitute_in_sources("__E2E_PATH__", path)?;
        // The test connects at the tenant-namespaced path (e2e tenant = local).
        Ok(format!("local/{path}"))
    }

    /// Build the project's worker image and register it (the real
    /// compile path), starting nothing. A later run or activate then
    /// finds everything current and does no build of its own. Optional:
    /// a plain run builds on its own.
    pub async fn build(&mut self) -> Result<()> {
        self.weft(&["build"]).await?;
        self.teardown.mark_registered();
        Ok(())
    }

    /// Activate the project (build + register + enable triggers). Required for
    /// fixtures whose entry is a trigger (web, live, form, timer, feed). Marks
    /// the project registered so [`Project::finish`] removes it.
    pub async fn activate(&mut self) -> Result<()> {
        self.weft(&["activate"]).await?;
        self.teardown.mark_registered();
        Ok(())
    }

    /// Run `weft activate` EXPECTING a refusal: errors if activation
    /// succeeds, otherwise returns the CLI's combined output so the
    /// test asserts on the refusal's message. The project is still
    /// marked for teardown (a refused activation may have registered
    /// state before the failing trigger; `weft rm` cleans either way).
    pub async fn activate_refused(&mut self) -> Result<String> {
        let out = crate::client::cli(&self.dir, &["activate"]).await?;
        self.teardown.mark_registered();
        anyhow::ensure!(
            !out.success,
            "`weft activate` unexpectedly succeeded; this scenario expects a refusal"
        );
        Ok(format!("{}\n{}", out.stdout, out.stderr))
    }

    /// Mark the project registered without going through activate. Used by the
    /// run path, where the first `weft run` builds + registers the project as a
    /// side effect, so teardown must still remove it.
    pub fn mark_registered(&mut self) {
        self.teardown.mark_registered();
    }

    /// Remove the project the way a user does, as the thing under
    /// test rather than as teardown: `weft rm <id> --yes`. Teardown
    /// then only has the temp directory left to clear.
    pub async fn remove(&mut self) -> Result<String> {
        let id = self.id.to_string();
        let out = cli_ok(&self.dir, &["rm", &id, "--yes"])
            .await
            .with_context(|| format!("weft rm {id}"))?;
        self.teardown.mark_removed();
        Ok(out)
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
            // would clean up. Run by id so it is unambiguous; `--yes`
            // answers the confirmation a script cannot type.
            let id = self.id.to_string();
            cli_ok(&self.dir, &["rm", &id, "--yes"])
                .await
                .with_context(|| format!("teardown: weft rm {id}"))?;
        }
        let extras: Vec<PathBuf> = std::mem::take(&mut *self.extra_dirs.lock().expect("extra dirs"));
        for extra in extras {
            std::fs::remove_dir_all(&extra)
                .with_context(|| format!("teardown: remove temp dir {}", extra.display()))?;
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

/// Every `.weft` file under `dir`, recursively, in a stable order.
fn weft_sources(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("read dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            out.extend(weft_sources(&path)?);
        } else if path.extension().is_some_and(|ext| ext == "weft") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
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
