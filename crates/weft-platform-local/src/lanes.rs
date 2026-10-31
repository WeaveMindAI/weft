//! The machine's compile lanes: how many host builds compile side by side.
//!
//! Cargo locks a target folder for a whole build, so one compile cache
//! for the machine would make every build wait for the one before it. Each
//! lane keeps a compile cache of its own (`MachineLayout::lane_cache`, one
//! per set of `[build.env]` values and the node types that set them), held
//! by one build at a time through a lock file (`MachineLayout::lane_lock`):
//! every weft process on the machine (each install's runtime, every CLI)
//! picks from the same lock files, so no two builds ever compile in one
//! lane, and a sweep can tell a lane in use from one free to clean.
//! The seed every lane starts from has a lock of its own
//! (`MachineLayout::seed_lock`), held while it is compiled and while a
//! lane copies it.

use std::path::Path;

use crate::layout::MachineLayout;

/// How many lanes a machine has when nobody says otherwise. Each keeps a
/// compile cache of its own (about 2GB), so this is also how many of
/// those are kept.
pub const DEFAULT_COMPILE_LANES: u32 = 4;

/// Overrides [`DEFAULT_COMPILE_LANES`]: more for a machine that builds
/// many projects at once (and has the disk), 1 to keep a single cache.
/// Read by every weft process from its own environment; `weft daemon
/// start` hands it on to the install's runtime.
pub const COMPILE_LANES_ENV: &str = "WEFT_COMPILE_LANES";

/// How long a build waiting for a lane waits between looks.
const LANE_WAIT: std::time::Duration = std::time::Duration::from_millis(250);

/// How many lanes this machine has: [`COMPILE_LANES_ENV`] from this
/// process's environment, or [`DEFAULT_COMPILE_LANES`].
pub fn lane_count() -> anyhow::Result<u32> {
    parse_lane_count(std::env::var(COMPILE_LANES_ENV).ok().as_deref())
}

/// [`lane_count`] from the variable's value.
pub fn parse_lane_count(raw: Option<&str>) -> anyhow::Result<u32> {
    match raw {
        None => Ok(DEFAULT_COMPILE_LANES),
        Some(raw) => match raw.trim().parse::<u32>() {
            Ok(n) if n >= 1 => Ok(n),
            _ => anyhow::bail!("{COMPILE_LANES_ENV}={raw:?}: expected a whole number of at least 1"),
        },
    }
}

/// One of the machine's compile lanes, held (an exclusive lock on its
/// file) until dropped.
#[derive(Debug)]
pub struct Lane {
    number: u32,
    _lock: std::fs::File,
}

impl Lane {
    pub fn number(&self) -> u32 {
        self.number
    }

    /// Lane `number` if no build holds it right now, `None` when one does.
    pub fn try_take(machine: &MachineLayout, number: u32) -> anyhow::Result<Option<Self>> {
        let path = machine.lane_lock(number);
        let lock = weft_core::file_lock::try_hold(&path).map_err(|e| anyhow::anyhow!("lock compile lane {number} ({}): {e}", path.display()))?;
        Ok(lock.map(|_lock| Self { number, _lock }))
    }

    /// The first of `lanes` lanes no build holds, `None` when every one is
    /// held.
    pub fn first_free(machine: &MachineLayout, lanes: u32) -> anyhow::Result<Option<Self>> {
        for number in 0..lanes {
            if let Some(lane) = Self::try_take(machine, number)? {
                return Ok(Some(lane));
            }
        }
        Ok(None)
    }

    /// The first free lane of `lanes`, waiting for one to free up when
    /// every one is held, after saying `on_wait` once.
    pub async fn take(machine: &MachineLayout, lanes: u32, on_wait: impl FnOnce()) -> anyhow::Result<Self> {
        let mut on_wait = Some(on_wait);
        loop {
            if let Some(lane) = Self::first_free(machine, lanes)? {
                return Ok(lane);
            }
            if let Some(say) = on_wait.take() {
                say();
            }
            tokio::time::sleep(LANE_WAIT).await;
        }
    }
}

/// Every compile cache on the machine, held: each lane (the ones this
/// machine counts and any other a cache is there for), then the seed,
/// waiting for the builds in them to end, after saying `on_wait` once.
/// What every build compiles from can be dropped while these are held.
///
/// The lanes come before the seed because a build takes them in that
/// order (its lane, then the seed to copy it), so the two never wait on
/// each other: taking the seed first would hold it while a build holding
/// its lane waits for the seed.
pub async fn hold_every_cache(machine: &MachineLayout, on_wait: impl FnOnce() + Send + 'static) -> anyhow::Result<Vec<std::fs::File>> {
    let mut lanes: std::collections::BTreeSet<u32> = (0..lane_count()?).collect();
    match std::fs::read_dir(machine.compile_caches()) {
        Ok(entries) => lanes.extend(entries.filter_map(Result::ok).filter_map(|e| Some(MachineLayout::lane_cache_of(e.file_name().to_str()?)?.0))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => anyhow::bail!("read {}: {e}", machine.compile_caches().display()),
    }
    let said = std::sync::Arc::new(std::sync::Mutex::new(Some(on_wait)));
    let mut held = Vec::new();
    for number in lanes {
        held.push(hold(&machine.lane_lock(number), say_once(&said)).await?);
    }
    held.push(hold(&machine.seed_lock(), say_once(&said)).await?);
    Ok(held)
}

/// `said`'s words, said by whichever wait comes first and never again.
fn say_once<F: FnOnce() + Send + 'static>(said: &std::sync::Arc<std::sync::Mutex<Option<F>>>) -> impl FnOnce() + Send + 'static {
    let said = said.clone();
    move || {
        let say = said.lock().ok().and_then(|mut s| s.take());
        if let Some(say) = say {
            say();
        }
    }
}

/// Hold the lock at `path`, waiting (on the blocking pool, so the caller's
/// other futures keep going) while another weft process holds it, after
/// saying `on_wait` once.
pub async fn hold(path: &Path, on_wait: impl FnOnce() + Send + 'static) -> anyhow::Result<std::fs::File> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || weft_core::file_lock::hold(&path, on_wait).map_err(|e| anyhow::anyhow!("lock {}: {e}", path.display())))
        .await
        .map_err(|e| anyhow::anyhow!("a lock wait stopped: {e}"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lane_count_is_the_variable_or_four() {
        assert_eq!(parse_lane_count(None).unwrap(), DEFAULT_COMPILE_LANES);
        assert_eq!(parse_lane_count(Some(" 8 ")).unwrap(), 8);
        for bad in ["0", "-1", "many", ""] {
            let e = parse_lane_count(Some(bad)).unwrap_err().to_string();
            assert!(e.contains(COMPILE_LANES_ENV), "{bad}: {e}");
        }
    }

    /// Holding every cache holds the seed, each counted lane and any
    /// other lane a cache is there for, tagged or not.
    #[tokio::test]
    async fn holding_every_cache_holds_each_lane_and_the_seed() {
        let dir = tempfile::tempdir().unwrap();
        let machine = MachineLayout::new(dir.path());
        std::fs::create_dir_all(machine.lane_cache(9, None)).unwrap();
        std::fs::create_dir_all(machine.lane_cache(7, Some("0123456789abcdef"))).unwrap();
        let held = hold_every_cache(&machine, || {}).await.unwrap();
        for number in [0, 7, 9] {
            assert!(Lane::try_take(&machine, number).unwrap().is_none(), "lane {number} is held");
        }
        assert!(weft_core::file_lock::try_hold(&machine.seed_lock()).unwrap().is_none(), "the seed is held");
        drop(held);
        assert!(Lane::try_take(&machine, 9).unwrap().is_some());
    }

    /// A cleanup waiting for a build's lane leaves the seed free, so the
    /// build (holding its lane, then wanting the seed to copy it) goes on
    /// and the cleanup gets everything once it ends.
    #[tokio::test]
    async fn holding_every_cache_never_takes_the_seed_a_lane_build_waits_for() {
        let dir = tempfile::tempdir().unwrap();
        let machine = MachineLayout::new(dir.path());
        let lane = Lane::try_take(&machine, 0).unwrap().unwrap();
        let waits = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cleanup = tokio::spawn({
            let (machine, waits) = (machine.clone(), waits.clone());
            async move { hold_every_cache(&machine, move || waits.store(true, std::sync::atomic::Ordering::SeqCst)).await.map(|held| held.len()) }
        });
        while !waits.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        let seed = weft_core::file_lock::try_hold(&machine.seed_lock()).unwrap();
        assert!(seed.is_some(), "the build holding its lane takes the seed");
        drop(seed);
        drop(lane);
        assert!(cleanup.await.unwrap().unwrap() > 1, "the cleanup holds every lane and the seed once the build let go");
    }

    /// A build takes the first lane nobody holds; a lane comes back free
    /// once its holder lets go; with every lane held there is none.
    #[test]
    fn a_build_takes_the_first_free_lane() {
        let dir = tempfile::tempdir().unwrap();
        let machine = MachineLayout::new(dir.path());
        let first = Lane::first_free(&machine, 2).unwrap().unwrap();
        assert_eq!(first.number(), 0);
        let second = Lane::first_free(&machine, 2).unwrap().unwrap();
        assert_eq!(second.number(), 1);
        assert!(Lane::first_free(&machine, 2).unwrap().is_none(), "both held");
        assert!(Lane::try_take(&machine, 1).unwrap().is_none());
        drop(first);
        assert_eq!(Lane::first_free(&machine, 2).unwrap().unwrap().number(), 0, "free again once let go");
    }

    /// A build waits politely for a lane, saying so once, and takes the
    /// one let go of.
    #[tokio::test]
    async fn a_build_waits_for_a_lane_and_says_so_once() {
        let dir = tempfile::tempdir().unwrap();
        let machine = MachineLayout::new(dir.path());
        let held = Lane::first_free(&machine, 1).unwrap().unwrap();
        let said = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let waiting = tokio::spawn({
            let (machine, said) = (machine.clone(), said.clone());
            async move { Lane::take(&machine, 1, move || { said.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }).await.unwrap().number() }
        });
        while said.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        drop(held);
        assert_eq!(waiting.await.unwrap(), 0);
        assert_eq!(said.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
