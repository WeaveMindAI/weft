//! Project loader. Reads `weft.toml`, resolves paths, walks the
//! project directory.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{CompileError, CompileResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectManifest {
    pub package: PackageSection,
    #[serde(default)]
    pub dispatcher: DispatcherSection,
    #[serde(default)]
    pub build: BuildSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageSection {
    pub name: String,
    /// Stable project identifier. Minted on first `weft run` /
    /// `weft build`; must survive across invocations so the
    /// dispatcher sees the same project when the user re-runs.
    pub id: Uuid,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DispatcherSection {
    /// URL of the dispatcher this project talks to. Defaults to
    /// `http://localhost:9999` if unset.
    pub url: Option<String>,
}

/// Optional `[build]` block in weft.toml. Controls how the
/// project's worker container image gets generated. Left
/// empty, codegen uses the built-in Debian-slim template and
/// the package manager inferred from it (apt).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BuildSection {
    #[serde(default)]
    pub worker: WorkerBuildSection,
}

/// `[build.worker]` customization. Both fields are optional:
///
/// - `base_image`: Docker base image. Codegen picks a package
///   manager from this string (`debian`/`ubuntu` → apt,
///   `alpine` → apk, `rhel`/`centos`/`fedora`/`rocky`/`amazonlinux`
///   → yum, `homebrew/brew` → brew). Unknown base images fall back
///   to apt with a warning.
/// - `dockerfile_template`: path (relative to the project root)
///   to a user-provided Dockerfile template. When set, overrides the
///   built-in template entirely. Must use the same substitution
///   tokens the built-in one does; see `worker_image::default_template`
///   for the canonical token set (kept there so this doc can't drift
///   out of sync with what `emit` actually substitutes).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerBuildSection {
    #[serde(default)]
    pub base_image: Option<String>,
    #[serde(default)]
    pub dockerfile_template: Option<String>,
}

pub struct Project {
    pub root: PathBuf,
    pub manifest: ProjectManifest,
}

impl Project {
    /// Load a project from `<root>/weft.toml`. Errors if the manifest
    /// is missing or malformed; use `init` to create a new project.
    pub fn load(root: &Path) -> CompileResult<Self> {
        let manifest_path = root.join("weft.toml");
        let raw = std::fs::read_to_string(&manifest_path)
            .map_err(|e| CompileError::Project(format!("{}: {}", manifest_path.display(), e)))?;
        let manifest: ProjectManifest = toml::from_str(&raw)
            .map_err(|e| CompileError::Project(format!("weft.toml parse: {e}")))?;
        Ok(Self { root: root.to_path_buf(), manifest })
    }

    /// Create a new project with a fresh id and the minimal files.
    /// Used by `weft new`. Returns an error if `weft.toml` already
    /// exists in the target directory.
    pub fn init(root: &Path, name: &str) -> CompileResult<Self> {
        if root.join("weft.toml").exists() {
            return Err(CompileError::Project(format!(
                "{} already contains weft.toml",
                root.display()
            )));
        }
        std::fs::create_dir_all(root).map_err(CompileError::Io)?;

        let manifest = ProjectManifest {
            package: PackageSection {
                name: name.to_string(),
                id: Uuid::new_v4(),
                version: Some("0.1.0".into()),
                description: None,
            },
            dispatcher: DispatcherSection::default(),
            build: BuildSection::default(),
        };
        let raw = toml::to_string_pretty(&manifest)
            .map_err(|e| CompileError::Project(format!("serialize manifest: {e}")))?;
        std::fs::write(root.join("weft.toml"), raw).map_err(CompileError::Io)?;

        // Minimal main.weft with a single pure graph the user can
        // edit immediately.
        let main_weft = format!(
            "# Project: {name}\n\n\
             greeting = Text {{ value: \"hello world\" }}\n\
             out = Debug\n\n\
             out.value = greeting.value\n"
        );
        std::fs::write(root.join("main.weft"), main_weft).map_err(CompileError::Io)?;

        std::fs::create_dir_all(root.join("nodes")).map_err(CompileError::Io)?;
        std::fs::create_dir_all(root.join(".weft")).map_err(CompileError::Io)?;

        // Seed the standard library into `nodes/base_catalog/`. From
        // here the project owns all its nodes and the build never
        // reaches back into the weft installation. The user's own
        // nodes live elsewhere under `nodes/`; `base_catalog/` is the
        // managed mirror that `weft catalog update` re-syncs.
        seed_base_catalog(root)?;

        Self::load(root)
    }

    pub fn id(&self) -> Uuid {
        self.manifest.package.id
    }

    pub fn dispatcher_url(&self) -> String {
        self.manifest
            .dispatcher
            .url
            .clone()
            .unwrap_or_else(|| "http://localhost:9999".into())
    }

    pub fn main_weft(&self) -> PathBuf {
        self.root.join("main.weft")
    }

    pub fn nodes_dir(&self) -> PathBuf {
        self.root.join("nodes")
    }

    /// The managed stdlib mirror under `nodes/`. Seeded at `weft new`
    /// and re-synced by `weft catalog update`. The user's own nodes
    /// live elsewhere under `nodes/`, never here.
    pub fn base_catalog_dir(&self) -> PathBuf {
        base_catalog_dir(&self.root)
    }

    pub fn state_dir(&self) -> PathBuf {
        self.root.join(".weft")
    }

    /// Read and return the weft source.
    pub fn read_main_weft(&self) -> CompileResult<String> {
        std::fs::read_to_string(self.main_weft()).map_err(CompileError::Io)
    }

    /// Search upward from `start` for a directory containing `weft.toml` and
    /// load it. `Ok(Some)` = found and loaded; `Ok(None)` = no project (no
    /// `weft.toml` in the tree, OR `start` doesn't resolve to a real directory,
    /// e.g. an unsaved editor buffer); `Err` = a `weft.toml` was found but FAILED
    /// to load (a malformed manifest). Only the LAST case is loud, so a caller
    /// that tolerates "no project" (the editor's lenient parse) surfaces a BROKEN
    /// manifest while still degrading gracefully on an unresolvable path.
    pub fn find(start: &Path) -> CompileResult<Option<Self>> {
        // An unresolvable start path (a phantom/unsaved buffer location) is "no
        // project", not an error: there's simply nowhere to search from.
        let Ok(start) = start.canonicalize() else { return Ok(None) };
        let mut cursor: &Path = &start;
        loop {
            if cursor.join("weft.toml").exists() {
                return Self::load(cursor).map(Some);
            }
            match cursor.parent() {
                Some(p) => cursor = p,
                None => return Ok(None),
            }
        }
    }

    /// Like [`find`], but a missing project is an error (the common case for
    /// commands that require a project root). Lets the user invoke `weft run`
    /// from a subfolder without naming the project root every time.
    pub fn discover(start: &Path) -> CompileResult<Self> {
        Self::find(start)?.ok_or_else(|| {
            CompileError::Project(format!(
                "no weft.toml found at {} or any parent",
                start.display()
            ))
        })
    }
}

/// `nodes/base_catalog/` under a project root.
pub fn base_catalog_dir(project_root: &Path) -> PathBuf {
    project_root.join("nodes").join("base_catalog")
}

/// (Re)seed `nodes/base_catalog/` from the weft installation's bundled
/// catalog. Wipes the existing `base_catalog/` and copies the current
/// catalog in: picks up edited node source, added nodes, and removed
/// nodes in one shot. The user's own nodes (anywhere else under
/// `nodes/`) are untouched. Used by `weft new` and `weft catalog
/// update`. (A future registry replaces `stdlib_root()` as the source;
/// the destination shape stays the same.)
pub fn seed_base_catalog(project_root: &Path) -> CompileResult<()> {
    let dest = base_catalog_dir(project_root);
    if dest.exists() {
        std::fs::remove_dir_all(&dest).map_err(CompileError::Io)?;
    }
    // Same node-tree exclude the build's staging copy uses, so a seed
    // source that ever carries a build/cache dir (a `target/`, a
    // `node_modules/`) doesn't get cloned into the user's `nodes/`.
    crate::build::copy_dir_filtered(
        &weft_catalog::stdlib_root(),
        &dest,
        weft_catalog::NODE_TREE_EXCLUDE,
    )
}

#[cfg(test)]
mod find_tests {
    use super::*;

    /// `Project::find` is three-way: a valid manifest loads, a missing manifest is
    /// `Ok(None)` (lenient), and a MALFORMED manifest is a loud `Err` (never
    /// silently degraded to no-project, which would render every node unknown).
    #[test]
    fn find_distinguishes_missing_from_malformed() {
        // No weft.toml anywhere -> Ok(None).
        let empty = tempfile::tempdir().unwrap();
        assert!(matches!(Project::find(empty.path()), Ok(None)), "missing manifest is no-project");

        // A valid weft.toml -> Ok(Some).
        let good = tempfile::tempdir().unwrap();
        std::fs::write(
            good.path().join("weft.toml"),
            "[package]\nname = \"p\"\nid = \"00000000-0000-0000-0000-000000000000\"\n",
        ).unwrap();
        assert!(matches!(Project::find(good.path()), Ok(Some(_))), "valid manifest loads");

        // A malformed weft.toml -> Err (loud), NOT Ok(None).
        let bad = tempfile::tempdir().unwrap();
        std::fs::write(bad.path().join("weft.toml"), "this is = = not valid toml [[[\n").unwrap();
        assert!(matches!(Project::find(bad.path()), Err(_)), "malformed manifest fails loud, not silent no-project");

        // An unresolvable start path -> Ok(None) (an unsaved buffer is no-project,
        // not an error).
        let phantom = empty.path().join("does/not/exist");
        assert!(matches!(Project::find(&phantom), Ok(None)), "unresolvable path is no-project");
    }
}
