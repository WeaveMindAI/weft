//! User-level CLI settings, persisted at `~/.config/weft/cli.toml`.
//! Distinct from the per-project `weft.toml`: these are preferences of
//! the PERSON at the keyboard, valid across every project.

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UserConfig {
    /// `weft test-node --tier live` asks a yes/no confirmation before
    /// running live tests (they use a real credential and can spend
    /// money). Answering "don't ask again" persists this.
    #[serde(default)]
    pub skip_live_test_confirmation: bool,
}

fn config_path() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").context("HOME is not set; cannot locate ~/.config")?;
    Ok(PathBuf::from(home).join(".config").join("weft").join("cli.toml"))
}

/// Load the user config. A missing file is the default config; a
/// present-but-malformed file is a loud error naming the path (never
/// silently reset: the user wrote it).
pub fn load() -> Result<UserConfig> {
    let path = config_path()?;
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(UserConfig::default()),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    toml::from_str(&raw).with_context(|| format!("parse {}", path.display()))
}

pub fn save(config: &UserConfig) -> Result<()> {
    let path = config_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let body = toml::to_string_pretty(config).context("serialize user config")?;
    std::fs::write(&path, body).with_context(|| format!("write {}", path.display()))
}
