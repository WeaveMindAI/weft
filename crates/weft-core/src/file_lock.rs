//! An exclusive lock on a file, the one way weft processes on a machine
//! take turns: whoever holds the lock on a path goes, everyone else waits
//! or moves on. The lock is the operating system's (`flock` on Unix), so
//! it lets go when the holder drops the file or the process ends, a crash
//! included: a lock file left behind never blocks anyone.

use std::fs::File;
use std::io;
use std::path::Path;

/// Hold the lock on `path` (made, with its folder, when missing), waiting
/// for it while another process holds it. `on_wait` runs once before the
/// wait, so a caller can say what it is waiting for instead of sitting
/// silent. Blocking: an async caller runs it on its blocking pool.
pub fn hold(path: &Path, on_wait: impl FnOnce()) -> io::Result<File> {
    take(open(path)?, on_wait)
}

/// The lock on `path` if nobody holds it right now, `None` when someone
/// does. Never waits.
pub fn try_hold(path: &Path) -> io::Result<Option<File>> {
    let file = open(path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(e)) => Err(e),
    }
}

/// Hold `path` together with every other shared holder, waiting while an
/// exclusive one ([`hold`], [`try_hold`]) has it: many readers, one
/// writer.
pub fn hold_shared(path: &Path) -> io::Result<File> {
    let file = open(path)?;
    file.lock_shared()?;
    Ok(file)
}

/// Hold the lock on `path` like [`hold`], but only if the file is there:
/// `None` when it is not, and nothing is made. For a reader that must
/// never bring back a folder another process just removed.
pub fn hold_existing(path: &Path, on_wait: impl FnOnce()) -> io::Result<Option<File>> {
    let file = match std::fs::OpenOptions::new().write(true).open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    take(file, on_wait).map(Some)
}

/// Lock `file`, saying `on_wait` once first when someone holds it.
fn take(file: File, on_wait: impl FnOnce()) -> io::Result<File> {
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => {
            on_wait();
            file.lock()?;
            Ok(file)
        }
        Err(std::fs::TryLockError::Error(e)) => Err(e),
    }
}

fn open(path: &Path) -> io::Result<File> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_holder_at_a_time_and_a_dropped_lock_is_free_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub/x.lock");
        let held = try_hold(&path).unwrap().expect("nobody holds it");
        assert!(try_hold(&path).unwrap().is_none(), "a second holder is refused while the first holds it");
        drop(held);
        let mut waited = false;
        let again = hold(&path, || waited = true).unwrap();
        assert!(!waited, "a free lock is taken without waiting");
        drop(again);
        assert!(try_hold(&path).unwrap().is_some());
    }

    #[test]
    fn holding_an_existing_lock_never_makes_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gone/x.lock");
        assert!(hold_existing(&path, || {}).unwrap().is_none());
        assert!(!dir.path().join("gone").exists(), "nothing is made");
        drop(hold(&path, || {}).unwrap());
        assert!(hold_existing(&path, || {}).unwrap().is_some());
    }
}
