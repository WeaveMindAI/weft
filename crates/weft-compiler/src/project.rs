//! Project loader. Reads `weft.toml`, resolves paths, walks the
//! project directory.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{CompileError, CompileResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectManifest {
    pub package: PackageSection,
    /// The weft installs this project deploys to, by name
    /// (`[targets.prod] url = "https://..."`). Shared by the team, so
    /// committed; the credential for each lives in the person's own
    /// `~/.config/weft/credentials.toml`, never here. `local` is always
    /// a target (the machine's own install) and may be overridden here.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub targets: BTreeMap<String, TargetSection>,
    #[serde(default)]
    pub build: BuildSection,
    /// The one-target spelling `[targets]` replaced. Read only to refuse
    /// it by name at load: ignored, a `url` in it would silently send
    /// every command to the local install instead.
    #[serde(default, skip_serializing)]
    dispatcher: Option<toml::Table>,
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

/// One `[targets.<name>]` entry: where that install's dispatcher answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetSection {
    pub url: String,
}

/// The target every command acts on when `--on` is not given: the
/// machine's own install. There is deliberately no setting that makes a
/// remote target the default, so a command that forgets to name one
/// lands on the laptop, never on a shared install.
// SYNC: LOCAL_TARGET <-> packages/weft-graph/src/protocol.ts LOCAL_INSTALL
pub const LOCAL_TARGET: &str = "local";

/// The one spelling of an install's base address: an http or https URL,
/// its scheme and host lowercased (the parser does that), with no query,
/// no fragment and no trailing slash, so `HTTPS://Weft.Example.com/` and
/// `https://weft.example.com` are the same install everywhere one is
/// compared or stored (a target's url, a stored key's entry). Anything
/// else is refused with the reason.
pub fn normalize_install_url(raw: &str) -> Result<String, String> {
    let parsed = url::Url::parse(raw).map_err(|e| format!("'{raw}' is not a URL ({e})"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(format!("'{raw}' must be an http or https address"));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(format!("'{raw}' must be the install's base address, with no query or fragment"));
    }
    Ok(parsed.as_str().trim_end_matches('/').to_string())
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

#[derive(Debug, Clone)]
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
        if manifest.dispatcher.is_some() {
            return Err(CompileError::Project(format!(
                "{} has a [dispatcher] section, which weft no longer reads: the installs a \
                 project talks to are named targets now. Delete the section; if it set a \
                 url, write it as `[targets.local]\nurl = \"...\"` for this machine's \
                 install, or under a name of its own (`[targets.prod]`) and pass \
                 `--on prod` to act there.",
                manifest_path.display()
            )));
        }
        // Every target's address is checked here, so a bad one is refused
        // naming its target the moment the project loads, not on the first
        // command that happens to act there.
        for (name, target) in &manifest.targets {
            normalize_install_url(&target.url).map_err(|e| {
                CompileError::Project(format!("{}: target '{name}': {e}", manifest_path.display()))
            })?;
        }
        let project = Self { root: root.to_path_buf(), manifest };
        // The program lives in `src/` (see `main_weft`). A project written
        // before that held it at the root; it is refused here, at load,
        // with the move spelled out, rather than failing later as a
        // missing file. Two valid places for the entry would be the
        // free-for-all the `src/` layout exists to end.
        if !project.main_weft().exists() && root.join(ENTRY_FILE).exists() {
            return Err(CompileError::Project(format!(
                "{} holds its program at the root; weft reads it from {}. \
                 Move it: `mkdir -p src && mv main.weft src/`, move any \
                 file it includes with it (an `@include` path is relative \
                 to the file; an `@file` / `@asset` path is relative to \
                 the project root and needs no change), and move \
                 `layouts/main.layout` to `layouts/src/main.layout`.",
                root.display(),
                Path::new(SRC_DIR).join(ENTRY_FILE).display(),
            )));
        }
        Ok(project)
    }

    /// Create a new project with a fresh id and the minimal files.
    /// Used by `weft new`. Returns an error if `weft.toml` already
    /// exists in the target directory. Build/CLI path (it seeds the stdlib
    /// catalog), so gated behind `build`; the WASM parse build never scaffolds.
    #[cfg(feature = "build")]
    pub fn init(root: &Path, name: &str) -> CompileResult<Self> {
        if root.join("weft.toml").exists() {
            return Err(CompileError::Project(format!(
                "{} already contains weft.toml",
                root.display()
            )));
        }
        std::fs::create_dir_all(root).map_err(CompileError::Io)?;

        // The canonical starter files (weft.toml + src/main.weft). Defined ONCE in
        // `scaffold_files` so every caller of `weft new` produces byte-identical
        // projects.
        for (rel, bytes) in scaffold_files(name, Uuid::new_v4())? {
            let path = root.join(&rel);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(CompileError::Io)?;
            }
            std::fs::write(path, bytes).map_err(CompileError::Io)?;
        }

        std::fs::create_dir_all(root.join(NODES_DIR)).map_err(CompileError::Io)?;
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

    /// The dispatcher URL of the target called `name`: its
    /// `[targets.<name>]` entry, or for `local` the machine's own install
    /// (the port it saved, see `weft_core::ports::local_public_url`) when
    /// the project does not override it. An unknown name is an
    /// error naming every target the project has.
    pub fn target_url(&self, name: &str) -> CompileResult<String> {
        if let Some(target) = self.manifest.targets.get(name) {
            return normalize_install_url(&target.url).map_err(|e| {
                CompileError::Project(format!("target '{name}' in {}: {e}", self.root.join("weft.toml").display()))
            });
        }
        if name == LOCAL_TARGET {
            return weft_core::ports::local_public_url().map_err(CompileError::Project);
        }
        let mut known: Vec<&str> = self.manifest.targets.keys().map(String::as_str).collect();
        if !known.contains(&LOCAL_TARGET) {
            known.push(LOCAL_TARGET);
        }
        known.sort_unstable();
        Err(CompileError::Project(format!(
            "no target '{name}' in {}; its targets are: {}. Add one with \
             `weft target add {name} <url>`",
            self.root.join("weft.toml").display(),
            known.join(", ")
        )))
    }

    /// The program's entry file, `src/main.weft`. Source lives under
    /// `src/` like any other language's: `main.weft` is the entry, a
    /// sibling `.weft` is a module (one group per file, pulled in by
    /// `@include`), and a folder under `src/` is a package of them,
    /// grouped by what the code is about. Everything else at the root
    /// is not source: `nodes/` (dependencies), `assets/` (content
    /// pulled in by `@file` / `@asset`), `examples/` (frozen runs),
    /// `layouts/` (generated), `front/` (a frontend weft ignores).
    pub fn main_weft(&self) -> PathBuf {
        self.src_dir().join(ENTRY_FILE)
    }

    /// The source folder, `src/`: what a marker's path in the program is
    /// relative to.
    pub fn src_dir(&self) -> PathBuf {
        self.root.join(SRC_DIR)
    }

    pub fn nodes_dir(&self) -> PathBuf {
        self.root.join(NODES_DIR)
    }

    /// The managed stdlib mirror under `nodes/`. Seeded at `weft new`
    /// and re-synced by `weft catalog update`. The user's own nodes
    /// live elsewhere under `nodes/`, never here.
    #[cfg(feature = "build")]
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

    /// The one wording for "no project here", so [`Self::find`]'s
    /// callers that treat "not found" as an error cannot fork the
    /// message.
    pub fn no_project_here(start: &Path) -> CompileError {
        CompileError::Project(format!(
            "no weft.toml found at {} or any parent",
            start.display()
        ))
    }
}

/// `nodes/base_catalog/` under a project root: the standard library, a
/// folder of nodes like any other. Loading, validating, building and
/// hashing treat it as any node folder, and a project may leave it out.
/// The only thing particular to it is `weft catalog update` (which
/// `weft new` runs to seed it), replacing it from the installation.
#[cfg(feature = "build")]
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
#[cfg(feature = "build")]
pub fn seed_base_catalog(project_root: &Path) -> CompileResult<()> {
    let dest = base_catalog_dir(project_root);
    if dest.exists() {
        std::fs::remove_dir_all(&dest).map_err(CompileError::Io)?;
    }
    // Same node-tree exclude the build's staging copy uses, so a seed
    // source that ever carries a build/cache dir (a `target/`, a
    // `node_modules/`) doesn't get cloned into the user's `nodes/`.
    crate::build::copy_dir_filtered(
        &weft_catalog::stdlib_root().map_err(CompileError::Build)?,
        &dest,
        &weft_catalog::is_node_tree_excluded,
    )
}

/// The source folder and the entry file inside it (`src/main.weft`).
/// The editor host restates the pair to tell the entry file apart
/// from a file opened on its own.
// SYNC: SRC_DIR / ENTRY_FILE <-> extension-vscode/src/graphView.ts viewPlace
pub const SRC_DIR: &str = "src";
pub const ENTRY_FILE: &str = "main.weft";
/// The shared node tree: the standard library under `base_catalog/`
/// and any node several parts of the program use.
pub const NODES_DIR: &str = "nodes";

/// Where a project's nodes are looked for: `nodes/`, and beside the
/// code under `src/`. A node used by one module sits next to that
/// module's file (`src/billing/charge.weft` and `src/billing/stripe/`),
/// and the catalog finds it by the same two marks it uses everywhere,
/// a `metadata.json` (a node) or a `package.toml` (a package). Both
/// trees form ONE catalog, so a type name is unique across them.
// SYNC: node_roots <-> extension-vscode/src/diagnostics.ts (NODE_GLOBS)
pub fn node_roots(project_root: &Path) -> [PathBuf; 2] {
    [project_root.join(NODES_DIR), project_root.join(SRC_DIR)]
}

/// The canonical starter files of a brand-new project (`weft.toml` + `src/main.weft`),
/// as `(relative-path, bytes)`. This is the ONE definition of "what a new project
/// contains" (minus the seeded catalog, which `seed_base_catalog` adds), so
/// every caller of `weft new` produces byte-identical projects. `id` is the
/// project id stamped into the manifest (each caller mints its own).
pub fn scaffold_files(name: &str, id: Uuid) -> CompileResult<Vec<(String, Vec<u8>)>> {
    let manifest = ProjectManifest {
        package: PackageSection {
            name: name.to_string(),
            id,
            version: Some("0.1.0".into()),
            description: None,
        },
        targets: BTreeMap::new(),
        build: BuildSection::default(),
        dispatcher: None,
    };
    let toml = toml::to_string_pretty(&manifest)
        .map_err(|e| CompileError::Project(format!("serialize manifest: {e}")))?;
    // Minimal main.weft: a single pure graph the user can edit immediately. No
    // `# Project:` header: the project name is authoritative in `weft.toml` (and
    // the folder), so a name comment in the source would just be a stale
    // duplicate. The source carries only the graph.
    //
    // The connection is written inline, in the body of the node that consumes
    // it, because that is the spelling the book teaches and every example uses.
    // The first file a new user opens shows them the form they should write.
    let main_weft = "greeting = Text { value: \"Hello, World!\" }\n\
         out = Debug { data: greeting.value }\n";
    Ok(vec![
        ("weft.toml".to_string(), toml.into_bytes()),
        (format!("{SRC_DIR}/{ENTRY_FILE}"), main_weft.as_bytes().to_vec()),
    ])
}

#[cfg(test)]
mod find_tests {
    use super::*;

    fn project_with(extra: &str) -> (tempfile::TempDir, CompileResult<Project>) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/main.weft"), "").unwrap();
        std::fs::write(
            dir.path().join("weft.toml"),
            format!("[package]\nname = \"p\"\nid = \"00000000-0000-0000-0000-000000000000\"\n{extra}"),
        )
        .unwrap();
        let loaded = Project::load(dir.path());
        (dir, loaded)
    }

    #[test]
    fn local_is_always_a_target_and_a_project_may_override_it() {
        let (_d, bare) = project_with("");
        assert_eq!(bare.unwrap().target_url(LOCAL_TARGET).unwrap(), weft_core::ports::local_public_url().unwrap());
        let (_d, overridden) = project_with("[targets.local]\nurl = \"http://127.0.0.1:19999/\"\n");
        assert_eq!(
            overridden.unwrap().target_url(LOCAL_TARGET).unwrap(),
            "http://127.0.0.1:19999",
            "a trailing slash is dropped so paths join cleanly"
        );
    }

    #[test]
    fn a_target_url_has_one_spelling() {
        assert_eq!(normalize_install_url("HTTPS://Weft.Example.COM/").unwrap(), "https://weft.example.com");
        assert_eq!(normalize_install_url("http://h:8080/base/").unwrap(), "http://h:8080/base");
        assert!(normalize_install_url("ftp://x").is_err());
        assert!(normalize_install_url("https://x/?a=1").is_err());
        assert!(normalize_install_url("https://x/#f").is_err());
        assert!(normalize_install_url("weft.example.com").is_err());
    }

    #[test]
    fn a_bad_target_url_is_refused_at_load_naming_the_target() {
        let (_d, p) = project_with("[targets.prod]\nurl = \"weft.example.com\"\n");
        let error = p.unwrap_err().to_string();
        assert!(error.contains("target 'prod'"), "{error}");
    }

    #[test]
    fn an_unknown_target_names_every_known_one() {
        let (_d, p) = project_with("[targets.prod]\nurl = \"https://weft.example.com\"\n");
        let p = p.unwrap();
        assert_eq!(p.target_url("prod").unwrap(), "https://weft.example.com");
        let error = p.target_url("staging").unwrap_err().to_string();
        assert!(error.contains("no target 'staging'"), "{error}");
        assert!(error.contains("local, prod"), "{error}");
    }

    #[test]
    fn the_old_dispatcher_section_is_refused_with_the_move_spelled_out() {
        // Ignored, its url would quietly send every command to the local
        // install; an empty one is refused too, so there is one spelling.
        for section in ["[dispatcher]\n", "[dispatcher]\nurl = \"https://x\"\n"] {
            let (_d, p) = project_with(section);
            let error = p.unwrap_err().to_string();
            assert!(error.contains("[targets.local]"), "{error}");
        }
    }

    #[test]
    fn a_target_with_a_misspelled_key_is_refused() {
        let (_d, p) = project_with("[targets.prod]\nulr = \"https://x\"\n");
        assert!(p.is_err(), "a typo must not leave the target without a url");
    }

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
        assert!(Project::find(bad.path()).is_err(), "malformed manifest fails loud, not silent no-project");

        // An unresolvable start path -> Ok(None) (an unsaved buffer is no-project,
        // not an error).
        let phantom = empty.path().join("does/not/exist");
        assert!(matches!(Project::find(&phantom), Ok(None)), "unresolvable path is no-project");
    }

    /// The scaffold `main.weft` carries NO `# Project:` header: the name lives in
    /// `weft.toml`, so a name comment in the source would be a stale duplicate.
    /// The scaffold source is therefore name-independent; per-project identity
    /// (and per-project storage uniqueness) rests on `weft.toml`, which bakes the
    /// minted project id. Regression guard against re-introducing the old header.
    /// A project from before `src/` held its program at the root. It is
    /// refused at load with the move spelled out, so the first command run
    /// against it says what to do instead of failing on a missing file.
    #[test]
    fn a_program_at_the_root_is_refused_with_the_move_spelled_out() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("weft.toml"), "[package]\nname='old'\nid='00000000-0000-0000-0000-000000000001'\nversion='0.1.0'\n").unwrap();
        std::fs::write(dir.path().join("main.weft"), "out = Debug\n").unwrap();
        let err = Project::load(dir.path()).unwrap_err().to_string();
        assert!(err.contains("mv main.weft src/"), "{err}");
        assert!(err.contains("layouts/src/main.layout"), "{err}");
        // Once moved, the same folder loads.
        std::fs::create_dir(dir.path().join("src")).unwrap();
        std::fs::rename(dir.path().join("main.weft"), dir.path().join("src/main.weft")).unwrap();
        assert!(Project::load(dir.path()).is_ok());
    }

    #[test]
    fn scaffold_main_weft_has_no_project_header() {
        let main_of = |name: &str, id: Uuid| {
            let files = scaffold_files(name, id).unwrap();
            String::from_utf8(files.iter().find(|(p, _)| p == "src/main.weft").unwrap().1.clone()).unwrap()
        };
        let a = main_of("alpha", Uuid::new_v4());
        assert!(!a.contains("# Project:"), "scaffold main.weft must not carry a project header: {a:?}");
        // Name-independent source: two differently-named projects have identical
        // scaffold main.weft (their uniqueness comes from weft.toml, not the graph).
        let same_id = Uuid::new_v4();
        assert_eq!(
            main_of("alpha", same_id),
            main_of("beta", same_id),
            "scaffold main.weft does not depend on the project name",
        );
    }
}
