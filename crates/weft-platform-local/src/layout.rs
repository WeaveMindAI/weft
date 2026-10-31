//! Where weft keeps its files on this machine: the folders every install
//! on it shares ([`MachineLayout`], under weft's data folder,
//! `weft_core::infra::data_dir`) and the ones each install keeps for
//! itself ([`InstallLayout`], under its own folder). Every place that
//! reads or writes one of them names it here.

use std::path::{Path, PathBuf};

/// The folders every install on the machine shares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineLayout {
    root: PathBuf,
}

impl MachineLayout {
    /// The layout under `root`, weft's data folder (a test's own folder).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The layout under this machine's data folder.
    pub fn here() -> Self {
        Self::new(weft_core::infra::data_dir())
    }

    /// Weft's data folder: the default install's folder too, and the
    /// parent of `installs/`, where every named install keeps its own.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The worker programs, one folder per binary hash
    /// (`images::Program::Worker`), with the node-test servers beside them.
    pub fn workers(&self) -> PathBuf {
        self.root.join("workers")
    }

    /// The node-test servers, one folder per test hash
    /// (`images::Program::NodeTestServer`).
    pub fn node_test_servers(&self) -> PathBuf {
        node_test_servers_in(&self.workers())
    }

    /// Every compile cache host builds compile in: one per lane, and the
    /// seed, each with the mark saying it is warm beside it.
    pub fn compile_caches(&self) -> PathBuf {
        self.root.join("worker-target")
    }

    /// The compile cache of lane `number` (`lanes::Lane`) for builds whose
    /// nodes' `[build.env]` has the tag `cache_tag`
    /// (`weft_core::builds::HostRecipe::cache_tag`): `<number>` for the
    /// builds whose nodes set nothing, `<number>-<tag>` for each other set
    /// of values and the node types that set them, all held through the
    /// one lane's lock.
    pub fn lane_cache(&self, number: u32, cache_tag: Option<&str>) -> PathBuf {
        self.compile_caches().join(lane_cache_name(number, cache_tag))
    }

    /// The compile cache the standard worker is compiled in, which every
    /// lane's cache starts from (`host_build::compile`).
    pub fn seed_cache(&self) -> PathBuf {
        self.compile_caches().join("seed")
    }

    /// There once the seed is whole (`host_build::mark_seed`). Beside the
    /// seed's cache, never in it: a copy of the cache never carries the
    /// mark, so a copy cut short leaves nothing claiming it whole.
    pub fn seed_mark(&self) -> PathBuf {
        self.compile_caches().join("seed.weft-seeded")
    }

    /// There once lane `number`'s cache is warm: the seed copied whole into
    /// it, or a build finished in it. Beside the cache, for the same reason
    /// as [`Self::seed_mark`].
    pub fn lane_mark(&self, number: u32, cache_tag: Option<&str>) -> PathBuf {
        self.compile_caches().join(format!("{}.weft-seeded", lane_cache_name(number, cache_tag)))
    }

    /// The lane, and the `[build.env]` tag, of a compile cache or its mark
    /// under [`Self::compile_caches`], by its name (what
    /// [`Self::lane_cache`] and [`Self::lane_mark`] name it): `None` for
    /// the seed's and anything else.
    pub fn lane_cache_of(name: &str) -> Option<(u32, Option<&str>)> {
        let cache = name.strip_suffix(".weft-seeded").unwrap_or(name);
        let (number, tag) = match cache.split_once('-') {
            Some((number, tag)) => (number, Some(tag)),
            None => (cache, None),
        };
        Some((number.parse().ok()?, tag))
    }

    /// The lock of lane `number`: held by the build compiling in it.
    pub fn lane_lock(&self, number: u32) -> PathBuf {
        self.locks().join(format!("{number}.lock"))
    }

    /// The lock of the seed: held while it is compiled, and while a lane
    /// copies it.
    pub fn seed_lock(&self) -> PathBuf {
        self.locks().join("seed.lock")
    }

    fn locks(&self) -> PathBuf {
        self.root.join("compile-lanes")
    }

    /// What every host build shares: the worker-linked slice of each weft
    /// workspace (`weft-<key>/`) and every package crate
    /// (`pkg_<name>-<slot>/`), each named by its content and written once
    /// (`weft_compiler::build::stage_host_build`).
    pub fn shared_crates(&self) -> PathBuf {
        self.root.join("worker-crates")
    }

    /// What a program compiled on this machine is packaged as: a process
    /// the machine runs, its build sharing the machine's crates.
    pub fn host_packaging(&self) -> weft_core::builds::WorkerPackaging {
        weft_core::builds::WorkerPackaging::Host { shared_crates: self.shared_crates() }
    }
}

/// The name of lane `number`'s cache for the tag `cache_tag`
/// ([`MachineLayout::lane_cache`]).
fn lane_cache_name(number: u32, cache_tag: Option<&str>) -> String {
    match cache_tag {
        None => number.to_string(),
        Some(tag) => format!("{number}-{tag}"),
    }
}

/// The node-test servers' folder in the machine's workers folder
/// `workers` ([`MachineLayout::workers`]).
pub fn node_test_servers_in(workers: &Path) -> PathBuf {
    workers.join("node-tests")
}

/// The folders one install keeps for itself, under its own folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallLayout {
    dir: PathBuf,
}

impl InstallLayout {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// Its runtime's worker processes: their registry, logs and scratch
    /// folders (`runner`, `registry`).
    pub fn workers(&self) -> PathBuf {
        self.dir.join("workers")
    }

    /// The logs of its builds, one per artifact.
    pub fn build_logs(&self) -> PathBuf {
        self.dir.join("build-logs")
    }

    /// The log of the build of `image_ref`: one file per artifact, which
    /// lives as long as the install holds it, so a refusal naming it a
    /// month later still points at a file.
    pub fn build_log(&self, image_ref: &str) -> PathBuf {
        self.build_logs().join(format!("{}.log", image_ref.replace(['/', ':'], "_")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every shared folder is under the data folder, each lane's cache and
    /// lock its own, and the seed beside the lanes.
    #[test]
    fn the_machines_folders_sit_under_its_data_folder() {
        let m = MachineLayout::new("/d");
        assert_eq!(m.workers(), Path::new("/d/workers"));
        assert_eq!(m.node_test_servers(), Path::new("/d/workers/node-tests"));
        assert_eq!(m.lane_cache(2, None), Path::new("/d/worker-target/2"));
        assert_eq!(m.lane_cache(2, Some("0123456789abcdef")), Path::new("/d/worker-target/2-0123456789abcdef"));
        assert_eq!(m.seed_cache(), Path::new("/d/worker-target/seed"));
        assert_eq!(m.compile_caches(), Path::new("/d/worker-target"));
        assert_eq!(m.seed_mark(), Path::new("/d/worker-target/seed.weft-seeded"), "beside the seed's cache, never in it");
        assert_eq!(m.lane_mark(2, None), Path::new("/d/worker-target/2.weft-seeded"));
        assert_eq!(m.lane_mark(2, Some("0123456789abcdef")), Path::new("/d/worker-target/2-0123456789abcdef.weft-seeded"));
        let tag = Some("0123456789abcdef");
        for (name, lane) in [
            ("2", Some((2, None))),
            ("2-0123456789abcdef", Some((2, tag))),
            ("2.weft-seeded", Some((2, None))),
            ("2-0123456789abcdef.weft-seeded", Some((2, tag))),
            ("seed", None),
            ("seed.weft-seeded", None),
        ] {
            assert_eq!(MachineLayout::lane_cache_of(name), lane, "{name}");
        }
        assert_eq!(m.host_packaging(), weft_core::builds::WorkerPackaging::Host { shared_crates: m.shared_crates() });
        assert_eq!(m.lane_lock(2), Path::new("/d/compile-lanes/2.lock"));
        assert_eq!(m.seed_lock(), Path::new("/d/compile-lanes/seed.lock"));
        assert_eq!(m.shared_crates(), Path::new("/d/worker-crates"));
        assert_eq!(node_test_servers_in(&m.workers()), m.node_test_servers());
    }

    /// An install's build log is one flat file per artifact ref.
    #[test]
    fn an_installs_folders_sit_under_its_own() {
        let i = InstallLayout::new("/i");
        assert_eq!(i.workers(), Path::new("/i/workers"));
        assert_eq!(i.build_log("localhost/weft-cell7/weft-infra-pg:abc"), Path::new("/i/build-logs/localhost_weft-cell7_weft-infra-pg_abc.log"));
    }
}
