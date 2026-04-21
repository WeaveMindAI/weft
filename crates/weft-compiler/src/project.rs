//! Project loader. Reads `weft.toml`, resolves paths, walks the
//! project directory to find node sources.
//!
//! Phase A2: implement full project discovery including stdlib lookup,
//! `nodes/` traversal, `nodes/vendor/` resolution, `lib.rs` discovery.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

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
    pub fn load(root: &Path) -> CompileResult<Self> {
        let manifest_path = root.join("weft.toml");
        let raw = std::fs::read_to_string(&manifest_path)
            .map_err(|e| CompileError::Project(format!("{}: {}", manifest_path.display(), e)))?;
        let manifest: ProjectManifest = toml::from_str(&raw)
            .map_err(|e| CompileError::Project(format!("weft.toml parse: {e}")))?;
        Ok(Self { root: root.to_path_buf(), manifest })
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
}
