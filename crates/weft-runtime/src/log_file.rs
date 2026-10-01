//! The runtime's own log file, kept to a bounded size: when it passes
//! [`MAX_BYTES`] it moves aside to `<name>.1` (replacing the one there)
//! and a fresh file starts, so a runtime up for months holds at most two
//! files' worth of log.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// How big the log grows before it rolls over.
pub const MAX_BYTES: u64 = 50 * 1024 * 1024;

pub struct LogFile {
    path: PathBuf,
    max_bytes: u64,
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
        Ok(Self { path: path.to_path_buf(), max_bytes, state: Mutex::new(State { file, size, roll_failing: false }) })
    }

    fn write_all(&self, buf: &[u8]) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.size > 0 && state.size + buf.len() as u64 > self.max_bytes {
            match self.roll() {
                Ok(fresh) => {
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
