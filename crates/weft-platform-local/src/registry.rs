//! The worker processes an install's runtime started, kept in a file
//! (`<install dir>/workers/running.json`) so the runtime's next life finds
//! the ones an earlier life left running: a restart of the runtime leaves
//! its workers up to finish their runs, and the next life tells them to
//! stop and forgets them once they are gone. Beside it, each worker
//! writes its log to `logs/<name>.log` and keeps its scratch folder at
//! `scratch/<name>/`, both removed once it is forgotten.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::processes::{Processes, Signal};

/// One worker process the runtime started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Entry {
    /// The runner's name for it (`runner::worker_name`, then the life).
    pub name: String,
    pub pid: u32,
    /// The life of the runtime that started it.
    pub life: String,
    /// Its worker key, which its health check asks for.
    pub key: String,
    pub project: uuid::Uuid,
    /// The program it runs, as `WorkerTarget::image` names it
    /// (`weft-worker:<binary hash>`, `weft-node-tests:<test hash>`).
    pub image: String,
    /// When the process started (`Processes::started`): a pid another
    /// program took since has another, so it is never signalled.
    pub started: String,
}

/// The registry file of an install.
pub struct Registry {
    path: PathBuf,
}

/// The registry's file name in `<install dir>/workers/`.
const FILE: &str = "running.json";

impl Registry {
    /// The registry in the folder `dir` (`<install dir>/workers`).
    pub fn in_dir(dir: &Path) -> Self {
        Self { path: dir.join(FILE) }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn dir(&self) -> &Path {
        self.path.parent().expect("the registry is in a folder")
    }

    /// Where the worker named `name` writes what it prints.
    pub fn log_of(&self, name: &str) -> PathBuf {
        self.dir().join("logs").join(format!("{name}.log"))
    }

    /// The worker named `name`'s scratch folder.
    pub fn scratch_of(&self, name: &str) -> PathBuf {
        self.dir().join("scratch").join(name)
    }

    /// What `entry`'s worker left on the machine besides its process (its
    /// log, its scratch folder), removed: the worker is gone, and no later
    /// worker takes its name (a name carries the life that started it).
    pub fn remove_leftovers(&self, entry: &Entry) {
        remove_quietly(&self.log_of(&entry.name), false);
        remove_quietly(&self.scratch_of(&entry.name), true);
    }

    /// Kill `entry`'s worker outright (`SIGKILL`) when it still runs, and
    /// remove what it left ([`Self::remove_leftovers`]). Answers whether
    /// it was still running. Taking it out of the list is the caller's:
    /// the runner keeps its own copy of the list, and stopping the
    /// install empties it.
    pub async fn kill_and_clear(&self, processes: &dyn Processes, entry: &Entry) -> anyhow::Result<bool> {
        let ran = still_runs(processes, entry).await?;
        if ran {
            processes.signal(entry.pid, Signal::Kill)?;
        }
        self.remove_leftovers(entry);
        Ok(ran)
    }

    /// Every entry; none when there is no file yet. A file that does not
    /// read is an error naming what to do: the workers it lists are
    /// unknown to weft from then on.
    pub fn load(&self) -> anyhow::Result<Vec<Entry>> {
        let bytes = match std::fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => anyhow::bail!("read {}: {e}", self.path.display()),
        };
        serde_json::from_slice(&bytes).map_err(|e| {
            anyhow::anyhow!(
                "{} is not the list of worker processes weft keeps ({e}); stop the workers it names by hand \
                 (`kill <pid>`), then delete the file",
                self.path.display()
            )
        })
    }

    /// Make `entries` the registry: written beside the file, then renamed
    /// over it, so a reader sees the old list or the new one. Readable by
    /// this user alone: it holds the workers' keys.
    pub fn save(&self, entries: &[Entry]) -> anyhow::Result<()> {
        write_private(&self.path, &serde_json::to_vec_pretty(entries).expect("entries serialize"))
    }
}

/// Make `bytes` the file at `path` (its folder made when missing): written
/// beside it, then renamed over it, so a reader sees the old content or the
/// new. Readable by this user alone: what weft keeps this way holds keys
/// and environments.
pub fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write as _;
    let dir = path.parent().expect("a file weft keeps is in a folder");
    std::fs::create_dir_all(dir).map_err(|e| anyhow::anyhow!("create {}: {e}", dir.display()))?;
    let name = path.file_name().expect("a file weft keeps has a name").to_string_lossy();
    let partial = dir.join(format!("{name}.{}.tmp", std::process::id()));
    let written = (|| -> std::io::Result<()> {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
        let mut file = opts.open(&partial)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&partial, path)
    })();
    if let Err(e) = written {
        // The save failed either way; this only keeps a stray file out of
        // the folder.
        let _ = std::fs::remove_file(&partial);
        anyhow::bail!("write {}: {e}", path.display());
    }
    Ok(())
}

/// Whether `entry`'s process still runs: its pid is held by the process
/// that started then, not by one that took the pid since.
pub async fn still_runs(processes: &dyn Processes, entry: &Entry) -> anyhow::Result<bool> {
    crate::processes::runs(processes, entry.pid, &entry.started).await
}

/// Stop every worker process the install's registry (in `dir`) names,
/// at once (`SIGKILL`), and remove what each left: what a person stopping
/// or removing the install asks for, since its workers serve only its
/// runtime. A pid another program holds since is left alone. Answers how
/// many were stopped.
pub async fn stop_all(processes: &dyn Processes, dir: &Path) -> anyhow::Result<usize> {
    let registry = Registry::in_dir(dir);
    let mut stopped = 0;
    for entry in registry.load()? {
        if registry.kill_and_clear(processes, &entry).await? {
            stopped += 1;
        }
    }
    registry.save(&[])?;
    Ok(stopped)
}

/// Remove a file (or a folder, `dir`) weft made for a worker that is gone.
/// One already gone is fine; one that will not go is said, since nothing
/// else will remove it.
fn remove_quietly(path: &Path, dir: bool) {
    let removed = if dir { std::fs::remove_dir_all(path) } else { std::fs::remove_file(path) };
    if let Err(e) = removed {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(target: "weft_platform_local::registry", path = %path.display(), error = %e, "could not remove what a stopped worker left; delete it by hand");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str) -> Entry {
        Entry {
            name: name.into(),
            pid: 42,
            life: "ab12".into(),
            key: "k".into(),
            project: uuid::Uuid::from_u128(7),
            image: "weft-worker:abc".into(),
            started: "123".into(),
        }
    }

    #[test]
    fn the_registry_round_trips_and_starts_empty() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::in_dir(&dir.path().join("workers"));
        assert_eq!(registry.load().unwrap(), Vec::new());
        registry.save(&[entry("a"), entry("b")]).unwrap();
        assert_eq!(registry.load().unwrap(), vec![entry("a"), entry("b")]);
        let on_disk: serde_json::Value = serde_json::from_slice(&std::fs::read(registry.path()).unwrap()).unwrap();
        assert_eq!(on_disk[0]["project"], "00000000-0000-0000-0000-000000000007");
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(std::fs::metadata(registry.path()).unwrap().permissions().mode() & 0o777, 0o600, "it holds the workers' keys");
        let left: Vec<String> = std::fs::read_dir(dir.path().join("workers")).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
        assert_eq!(left, ["running.json"], "no partial file left behind");
    }

    /// A stop kills every worker the registry names that is still the
    /// process it started, never a pid another program took since, removes
    /// every worker's log and scratch folder, and leaves the registry
    /// empty.
    #[tokio::test]
    async fn stopping_the_install_kills_its_workers_and_no_reused_pid() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry::in_dir(dir.path());
        let processes = crate::processes::fake::FakeProcesses::new();
        processes.running(42, "123");
        processes.running(43, "999");
        let reused = Entry { pid: 43, ..entry("b") };
        registry.save(&[entry("a"), reused]).unwrap();
        for name in ["a", "b"] {
            std::fs::create_dir_all(registry.log_of(name).parent().unwrap()).unwrap();
            std::fs::write(registry.log_of(name), "printed").unwrap();
            std::fs::create_dir_all(registry.scratch_of(name)).unwrap();
        }
        assert_eq!(stop_all(&processes, dir.path()).await.unwrap(), 1);
        assert_eq!(processes.signals(), [(42, Signal::Kill)]);
        assert!(processes.is_alive(43), "another program's process is left alone");
        assert!(registry.load().unwrap().is_empty());
        for name in ["a", "b"] {
            assert!(!registry.log_of(name).exists() && !registry.scratch_of(name).exists(), "{name}'s leftovers go");
        }
    }

    #[test]
    fn a_file_that_does_not_read_is_refused_naming_what_to_do() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("running.json"), "[{").unwrap();
        let e = format!("{:#}", Registry::in_dir(dir.path()).load().unwrap_err());
        assert!(e.contains("running.json") && e.contains("kill"), "{e}");
    }
}
