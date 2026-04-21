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

    pub fn main_loom(&self) -> PathBuf {
        self.root.join("main.loom")
    }

    pub fn nodes_dir(&self) -> PathBuf {
        self.root.join("nodes")
    }

    pub fn vendor_dir(&self) -> PathBuf {
        self.root.join("nodes").join("vendor")
    }

    pub fn state_dir(&self) -> PathBuf {
        self.root.join(".weft")
    }

    /// Read and return the weft source.
    pub fn read_main_weft(&self) -> CompileResult<String> {
        std::fs::read_to_string(self.main_weft()).map_err(CompileError::Io)
    }

    /// Search upward from `start` for a directory containing
    /// `weft.toml`. Lets the user invoke `weft run` from a subfolder
    /// without naming the project root every time.
    pub fn discover(start: &Path) -> CompileResult<Self> {
        let start = start.canonicalize().map_err(CompileError::Io)?;
        let mut cursor: &Path = &start;
        loop {
            if cursor.join("weft.toml").exists() {
                return Self::load(cursor);
            }
            match cursor.parent() {
                Some(p) => cursor = p,
                None => {
                    return Err(CompileError::Project(format!(
                        "no weft.toml found at {} or any parent",
                        start.display()
                    )));
                }
            }
        }
    }
}
