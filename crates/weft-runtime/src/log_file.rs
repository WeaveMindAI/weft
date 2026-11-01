//! The runtime's own log file, kept to a bounded size: when it passes
//! [`MAX_BYTES`] it moves aside to `<name>.1` (replacing the one there)
//! and a fresh file starts, so a runtime up for months holds at most two
//! files' worth of log.
//!
//! The service that starts the runtime points its stdout and stderr at the
//! same file (`weft daemon start`'s command line), for what never passes
//! through the logger: a panic's last words, a library's print. When it
//! does, each roll moves them onto the fresh file too: left on the file
//! moved aside, they would land in `<name>.1`, and after the next roll in
//! a file nobody can open any more.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// How big the log grows before it rolls over.
pub const MAX_BYTES: u64 = 50 * 1024 * 1024;

pub struct LogFile {
    path: PathBuf,
    max_bytes: u64,
    /// The process's standard streams that are this file, which each roll
    /// moves on to the fresh one.
    carried: Vec<i32>,
    state: Mutex<State>,
}

struct State {
    file: File,
    size: u64,
    /// Whether the last roll over failed. The failure is said once on
    /// stderr (a write error inside the logger has nowhere else to go),
    /// and the log goes on in the file it has until a roll works.
    roll_failing: bool,
}

impl LogFile {
    pub fn open(path: &Path, max_bytes: u64) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let size = file.metadata()?.len();
        let mut carried = Vec::new();
        for (fd, stream) in [(libc::STDOUT_FILENO, std::io::stdout().as_fd()), (libc::STDERR_FILENO, std::io::stderr().as_fd())] {
            if same_file(&file, &File::from(stream.try_clone_to_owned()?))? {
                carried.push(fd);
            }
        }
        Ok(Self { path: path.to_path_buf(), max_bytes, carried, state: Mutex::new(State { file, size, roll_failing: false }) })
    }

    fn write_all(&self, buf: &[u8]) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.size > 0 && state.size + buf.len() as u64 > self.max_bytes {
            match self.roll() {
                Ok(mut fresh) => {
                    for &fd in &self.carried {
                        // SAFETY: both descriptors are open in this process;
                        // `dup2` only repoints `fd` at the fresh file.
                        if unsafe { libc::dup2(fresh.as_raw_fd(), fd) } < 0 {
                            // Said in the log itself: the stream is on the
                            // file just moved aside, which nobody reads.
                            let e = std::io::Error::last_os_error();
                            let _ = writeln!(
                                fresh,
                                "weft-runtime: could not move descriptor {fd} onto the fresh log ({e}); what it prints from now on lands in {}.1",
                                self.path.display()
                            );
                        }
                    }
                    state.file = fresh;
                    state.size = 0;
                    state.roll_failing = false;
                }
                Err(e) => {
                    if !state.roll_failing {
                        eprintln!(
                            "weft-runtime: could not roll the log {} over ({e}); it keeps growing until a roll works",
                            self.path.display()
                        );
                    }
                    state.roll_failing = true;
                }
            }
        }
        state.file.write_all(buf)?;
        state.size += buf.len() as u64;
        Ok(())
    }

    /// Move the log aside and open a fresh one.
    fn roll(&self) -> std::io::Result<File> {
        let mut rolled = self.path.clone().into_os_string();
        rolled.push(".1");
        std::fs::rename(&self.path, &rolled)?;
        OpenOptions::new().create(true).append(true).open(&self.path)
    }
}

/// Whether `a` and `b` are open on the same file.
fn same_file(a: &File, b: &File) -> std::io::Result<bool> {
    let (a, b) = (a.metadata()?, b.metadata()?);
    Ok(a.dev() == b.dev() && a.ino() == b.ino())
}

/// One `tracing` write, handed to the shared file.
pub struct LogWriter<'a>(&'a LogFile);

impl Write for LogWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write_all(buf)?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogFile {
    type Writer = LogWriter<'a>;
    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file is told apart from another one, and from itself opened twice
    /// (how the log tells whether stderr is it).
    #[test]
    fn the_same_file_opened_twice_is_one_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.log");
        let one = OpenOptions::new().create(true).append(true).open(&path).unwrap();
        let again = OpenOptions::new().append(true).open(&path).unwrap();
        let other = File::create(dir.path().join("other")).unwrap();
        assert!(same_file(&one, &again).unwrap());
        assert!(!same_file(&one, &other).unwrap());
    }

    #[test]
    fn a_full_log_moves_aside_and_starts_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.log");
        let log = LogFile::open(&path, 10).unwrap();
        log.write_all(b"12345678").unwrap();
        log.write_all(b"abcdef").unwrap();
        assert_eq!(std::fs::read(dir.path().join("runtime.log.1")).unwrap(), b"12345678");
        assert_eq!(std::fs::read(&path).unwrap(), b"abcdef");
        log.write_all(b"ghijklmno").unwrap();
        assert_eq!(std::fs::read(dir.path().join("runtime.log.1")).unwrap(), b"abcdef", "only one older file is kept");
    }

    /// A roll that cannot happen (here the older file's name is taken by
    /// a directory with something in it) never costs a line: the log goes
    /// on in the file it has, and rolls again once it can.
    #[test]
    fn a_failed_roll_keeps_the_log_going() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.log");
        let blocker = dir.path().join("runtime.log.1");
        std::fs::create_dir(&blocker).unwrap();
        std::fs::write(blocker.join("x"), b"x").unwrap();
        let log = LogFile::open(&path, 10).unwrap();
        log.write_all(b"12345678").unwrap();
        log.write_all(b"abcdef").unwrap();
        log.write_all(b"ghi").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"12345678abcdefghi");
        assert!(log.state.lock().unwrap().roll_failing);
        std::fs::remove_dir_all(&blocker).unwrap();
        log.write_all(b"jkl").unwrap();
        assert_eq!(std::fs::read(&blocker).unwrap(), b"12345678abcdefghi");
        assert_eq!(std::fs::read(&path).unwrap(), b"jkl");
        assert!(!log.state.lock().unwrap().roll_failing);
    }
}
