//! Operator keys, per install, kept at `~/.config/weft/credentials.toml`.
//!
//! A key belongs to the PERSON, never to the project: `weft.toml` names
//! the targets and is committed, and this file says what this person may
//! do on each. Entries are keyed by the dispatcher URL rather than the
//! target's name, because two projects may call the same install by
//! different names, and one name (`prod`) means different installs in
//! different projects.
//!
//! CI has no such file: it passes the key as `WEFT_OPERATOR_KEY`, which
//! wins over the file for a command that names a remote target with
//! `--on <name>`, and only then: a command on the local install or on a
//! raw `--dispatcher` address never carries it, so a key exported in a
//! shell is never handed to an install nobody named.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// The environment variable CI passes an operator key in.
pub const OPERATOR_KEY_ENV: &str = "WEFT_OPERATOR_KEY";

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    /// Dispatcher URL (in [`url_key`]'s spelling) -> what this person holds there.
    #[serde(default)]
    pub installs: BTreeMap<String, InstallCredential>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallCredential {
    pub operator_key: String,
}

/// The spelling a URL is keyed under
/// ([`weft_compiler::project::normalize_install_url`]): `https://X/` and
/// `https://x` are the same install.
pub fn url_key(url: &str) -> Result<String> {
    weft_compiler::project::normalize_install_url(url).map_err(|e| anyhow::anyhow!("{e}"))
}

impl Credentials {
    pub fn key_for(&self, url: &str) -> Result<Option<&str>> {
        Ok(self.installs.get(&url_key(url)?).map(|c| c.operator_key.as_str()))
    }

    pub fn set(&mut self, url: &str, operator_key: String) -> Result<()> {
        self.installs.insert(url_key(url)?, InstallCredential { operator_key });
        Ok(())
    }

    /// Whether anything was removed.
    pub fn remove(&mut self, url: &str) -> Result<bool> {
        Ok(self.installs.remove(&url_key(url)?).is_some())
    }
}

/// The key a request to `url` carries (see [`resolve_key`]); `on` is the
/// target name the command was given with `--on`, if any.
pub fn operator_key_for(url: &str, on: Option<&str>) -> Result<Option<String>> {
    resolve_key(std::env::var(OPERATOR_KEY_ENV).ok(), &load()?, url, on)
}

/// `WEFT_OPERATOR_KEY` when it is set, non-empty, and the command named a
/// remote target (`on` is a name other than the local one); else this
/// person's stored key for that install; else none (the local install
/// needs none). Given its inputs, so the rule is testable without
/// touching the environment or the disk.
pub fn resolve_key(
    env: Option<String>,
    stored: &Credentials,
    url: &str,
    on: Option<&str>,
) -> Result<Option<String>> {
    let names_remote = on.is_some_and(|name| name != weft_compiler::project::LOCAL_TARGET);
    if let Some(key) = env.filter(|_| names_remote) {
        let key = key.trim().to_string();
        if !key.is_empty() {
            return Ok(Some(key));
        }
    }
    Ok(stored.key_for(url)?.map(str::to_string))
}

fn path() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").context("HOME is not set; cannot locate ~/.config")?;
    Ok(PathBuf::from(home).join(".config").join("weft").join("credentials.toml"))
}

/// A missing file holds no keys; a malformed one is an error naming it
/// (never silently treated as empty: every command on a remote install
/// would then fail as unauthorized, pointing away from the real cause).
pub fn load() -> Result<Credentials> {
    let path = path()?;
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Credentials::default()),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    toml::from_str(&raw).with_context(|| format!("parse {}", path.display()))
}

/// Written readable by its owner only, and replaced whole through a
/// sibling file so a crash mid-write never leaves half a key file.
pub fn save(credentials: &Credentials) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = path()?;
    let parent = path.parent().expect("a file under ~/.config/weft");
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let body = toml::to_string_pretty(credentials).context("serialize credentials")?;
    let staged = parent.join(".credentials.toml.new");
    let _ = std::fs::remove_file(&staged);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&staged)
        .with_context(|| format!("create {}", staged.display()))?;
    file.write_all(body.as_bytes()).with_context(|| format!("write {}", staged.display()))?;
    file.sync_all().with_context(|| format!("write {}", staged.display()))?;
    std::fs::rename(&staged, &path).with_context(|| format!("replace {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_found_whatever_the_trailing_slash() {
        let mut c = Credentials::default();
        c.set("https://Weft.example.com/", "k1".into()).unwrap();
        assert_eq!(c.key_for("https://weft.example.com").unwrap(), Some("k1"));
        assert_eq!(c.key_for("https://other.example.com").unwrap(), None);
        assert!(c.remove("https://weft.example.com").unwrap());
        assert!(!c.remove("https://weft.example.com").unwrap());
    }

    #[test]
    fn the_environment_wins_for_a_named_remote_target_and_blank_is_unset() {
        let mut c = Credentials::default();
        c.set("https://x", "stored".into()).unwrap();
        let prod = Some("prod");
        assert_eq!(resolve_key(Some("ci".into()), &c, "https://x", prod).unwrap().as_deref(), Some("ci"));
        assert_eq!(resolve_key(Some("  ".into()), &c, "https://x", prod).unwrap().as_deref(), Some("stored"));
        assert_eq!(resolve_key(None, &c, "https://y", prod).unwrap(), None);
    }

    #[test]
    fn the_environment_never_reaches_an_install_nobody_named() {
        let c = Credentials::default();
        // A raw `--dispatcher` address, and the local target, named or not.
        for on in [None, Some("local")] {
            assert_eq!(resolve_key(Some("ci".into()), &c, "http://127.0.0.1:14111", on).unwrap(), None);
        }
    }

    #[test]
    fn the_file_round_trips() {
        let mut c = Credentials::default();
        c.set("https://x", "k".into()).unwrap();
        let back: Credentials = toml::from_str(&toml::to_string_pretty(&c).unwrap()).unwrap();
        assert_eq!(back, c);
    }
}
