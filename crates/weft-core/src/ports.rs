//! Every fixed port weft picks on a machine, in one place.
//!
//! They sit in 14111-14118, a block chosen from data: unassigned at IANA,
//! never seen open in nmap's frequency tables, and absent from the
//! Prometheus port list and the default lists of the usual dev tools, so a
//! fresh machine almost never has one of them taken. The default install
//! starts on them; setting `WEFT_PUBLIC_PORT`, `WEFT_OUTSIDE_PORT`,
//! `WEFT_INTERNAL_PORT` or `WEFT_POSTGRES_PORT` on a `weft daemon start`
//! moves that port, and every install keeps the ports it runs on in its
//! `ports.json` ([`InstallPorts`]), which is where the CLI looks for it.
//! `WEFT_SEAWEED_PORT` moves the object store, which all installs share.
//!
//! SYNC: these numbers <-> setup.sh (weft_local_url, the summary),
//! extension-vscode/src/localInstall.ts
//! (DEFAULT_LOCAL_URL),
//! extension-browser/src/entrypoints/popup/App.svelte (the bare-token
//! default), and the prose that names them: docs/src/**,
//! tangle/*/ (skills and personas), README files, CHANGELOG.md.

/// The API and dashboard of the default install (`WEFT_PUBLIC_PORT`).
pub const PUBLIC: u16 = 14111;
/// The doors outside callers use, the port a tunnel forwards to
/// (`WEFT_OUTSIDE_PORT`).
pub const OUTSIDE: u16 = 14112;
/// The port weft's own roles, workers and containers call it on
/// (`WEFT_INTERNAL_PORT`; on GCP the machine's private port).
pub const INTERNAL: u16 = 14113;
/// The default install's Postgres, on the host (`WEFT_POSTGRES_PORT`);
/// inside its container Postgres keeps 5432.
pub const POSTGRES: u16 = 14114;
/// The object store, on the host (`WEFT_SEAWEED_PORT`); inside its
/// container it keeps 8333.
pub const OBJECT_STORE: u16 = 14115;
/// The agent beside every infra unit. On a GCP unit machine a user's
/// container publishes its ports as is, so the agent must stay off any
/// port a user's image is likely to use.
pub const UNIT_AGENT: u16 = 14116;

/// Where the default install answers on this machine until it is started
/// on another port. A literal because a `&str` const cannot be formatted;
/// the test below keeps it on [`PUBLIC`].
pub const LOCAL_PUBLIC_URL: &str = "http://127.0.0.1:14111";

/// The host ports one install listens on, saved in its `ports.json` by
/// `weft daemon start` and read back by every command that talks to it.
/// Saved for every install, the default one included, so a port moved
/// once stays moved: the address is the file's, never whatever the
/// environment of the latest command happens to say.
// SYNC: the file's shape <-> setup.sh (weft_local_url reads `public`),
// extension-vscode/src/localInstall.ts (localInstallUrl reads `public`)
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InstallPorts {
    pub public: u16,
    pub internal: u16,
    pub outside: u16,
    pub postgres: u16,
}

impl InstallPorts {
    /// The block's ports: what the default install starts on.
    pub const DEFAULT: Self = Self { public: PUBLIC, internal: INTERNAL, outside: OUTSIDE, postgres: POSTGRES };

    /// Where an install whose files live in `dir` keeps its ports.
    pub fn path(dir: &std::path::Path) -> std::path::PathBuf {
        dir.join("ports.json")
    }

    /// The ports saved in `dir`, `None` when the install was never
    /// started. A file that exists and cannot be read is an error, never
    /// a silent return to the defaults.
    pub fn load(dir: &std::path::Path) -> Result<Option<Self>, String> {
        let path = Self::path(dir);
        match std::fs::read_to_string(&path) {
            Ok(raw) => serde_json::from_str(&raw).map(Some).map_err(|e| format!("{} is not valid: {e}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("cannot read {}: {e}", path.display())),
        }
    }

    /// Written whole to a file beside it, then renamed over it: the
    /// editor's watcher, the CLI and setup.sh read this file while an
    /// install starts, and a rename on one filesystem means they see the
    /// old ports or the new ones, never a half-written file.
    pub fn save(&self, dir: &std::path::Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let path = Self::path(dir);
        let partial = dir.join(format!("ports.json.{}.tmp", std::process::id()));
        let written = std::fs::write(&partial, serde_json::to_vec(self)?).and_then(|()| std::fs::rename(&partial, &path));
        if written.is_err() {
            // The save failed either way; this only keeps a stray file out
            // of the install's folder.
            let _ = std::fs::remove_file(&partial);
        }
        written
    }

    /// The address the API and dashboard answer on.
    pub fn public_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.public)
    }
}

/// Where the install this process acts on ([`Install::from_env`])
/// answers on this machine: the public port it saved. A default install
/// never started answers nowhere yet, and its first start takes
/// [`PUBLIC`] unless told otherwise, so that is its address until then;
/// a named install never started has no port at all, and saying so beats
/// pointing at the default install's.
///
/// [`Install::from_env`]: crate::infra::Install::from_env
pub fn local_public_url() -> Result<String, String> {
    let install = crate::infra::Install::from_env()?;
    local_public_url_in(&install, &install.dir())
}

fn local_public_url_in(install: &crate::infra::Install, dir: &std::path::Path) -> Result<String, String> {
    match (InstallPorts::load(dir)?, install.name()) {
        (Some(saved), _) => Ok(saved.public_url()),
        (None, None) => Ok(LOCAL_PUBLIC_URL.to_string()),
        (None, Some(name)) => Err(format!(
            "install '{name}' has no ports yet ({} does not exist); start it with `weft daemon start`",
            InstallPorts::path(dir).display()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_public_url_is_on_the_public_port() {
        assert_eq!(LOCAL_PUBLIC_URL, format!("http://127.0.0.1:{PUBLIC}"));
    }

    #[test]
    fn the_local_url_is_the_saved_public_port_once_there_is_one() {
        use crate::infra::Install;
        let dir = std::env::temp_dir().join(format!("weft-ports-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let default = Install::default_install();
        assert_eq!(local_public_url_in(&default, &dir).unwrap(), LOCAL_PUBLIC_URL);
        let named = Install::named("cell1").unwrap();
        assert!(local_public_url_in(&named, &dir).unwrap_err().contains("weft daemon start"));
        let moved = InstallPorts { public: 15000, ..InstallPorts::DEFAULT };
        moved.save(&dir).unwrap();
        assert_eq!(InstallPorts::load(&dir).unwrap(), Some(moved));
        // The partial file it wrote first was renamed over ports.json.
        let files: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(files, vec![std::ffi::OsString::from("ports.json")]);
        assert_eq!(local_public_url_in(&default, &dir).unwrap(), "http://127.0.0.1:15000");
        assert_eq!(local_public_url_in(&named, &dir).unwrap(), "http://127.0.0.1:15000");
        std::fs::write(InstallPorts::path(&dir), "{").unwrap();
        assert!(InstallPorts::load(&dir).unwrap_err().contains("is not valid"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn every_port_is_in_the_block_and_distinct() {
        let all = [PUBLIC, OUTSIDE, INTERNAL, POSTGRES, OBJECT_STORE, UNIT_AGENT];
        assert!(all.iter().all(|p| (14111..=14118).contains(p)));
        let mut sorted = all.to_vec();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), all.len());
    }
}
