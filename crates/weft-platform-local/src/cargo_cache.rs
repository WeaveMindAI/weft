//! Keeps a cargo build folder to what is still used.
//!
//! cargo never deletes anything from its build folder: every time a
//! dependency moves or weft is updated, it compiles a new copy of each
//! piece under a new name and leaves the old one beside it. Each piece (a
//! "unit") is named by a 16-hex hash that every one of its files carries:
//! `deps/lib<crate>-<hash>.rlib`, `deps/<crate>-<hash>.d`,
//! `examples/<example>-<hash>`, `.fingerprint/<package>-<hash>/`,
//! `build/<package>-<hash>/`. Removing all of a unit's files together leaves cargo nothing stale: a build that
//! wants the unit again compiles it again.
//!
//! Two ways to know what is used, one per kind of folder:
//! - [`keep_used`]: a folder whose builds say what they used. cargo's JSON
//!   messages list every unit a build used, the ones it reused untouched
//!   included ([`used_units`]); a compile lane keeps the units some program
//!   still on the machine was built from (`host_build`).
//! - [`drop_superseded`]: a folder anybody runs cargo in (a checkout's
//!   `target/`, where tests, clippy and setup all build). There, each
//!   unit's fingerprint says how it was built (its features, profile,
//!   compiler, and the same for every unit it was built against). Of the
//!   top units (the ones no other unit was built against: test and
//!   program binaries) built the same way, only the newest is what
//!   today's sources and lock build; what those were built against is
//!   used, and every other unit goes.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

/// What a sweep removed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Swept {
    pub units: usize,
    pub bytes: u64,
}

impl std::ops::AddAssign for Swept {
    fn add_assign(&mut self, other: Self) {
        self.units += other.units;
        self.bytes += other.bytes;
    }
}

/// The unit hash a file name in `deps/`, `.fingerprint/` or `build/`
/// carries: the 16 hex after its last `-`, before any extension.
fn unit_of(name: &str) -> Option<&str> {
    let stem = name.split('.').next()?;
    let (_, hash) = stem.rsplit_once('-')?;
    (hash.len() == 16 && hash.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())).then_some(hash)
}

/// Every unit cargo's JSON messages (`--message-format=json...`, one
/// object per line) say the build used, as hashes. A binary is reported
/// where cargo linked it, at the top of `profile_dir`; that file is a hard
/// link of its unit's file in `deps/`, which is how it is traced.
pub fn used_units(messages: &[u8], profile_dir: &Path) -> anyhow::Result<BTreeSet<String>> {
    let mut used = BTreeSet::new();
    let mut top = Vec::new();
    for line in messages.split(|b| *b == b'\n') {
        let Ok(message) = serde_json::from_slice::<serde_json::Value>(line) else { continue };
        let paths = message["filenames"].as_array().into_iter().flatten().chain(std::iter::once(&message["out_dir"]));
        for path in paths.filter_map(|p| p.as_str()).map(Path::new) {
            let Ok(rest) = path.strip_prefix(profile_dir) else { continue };
            let mut parts = rest.components().map(|c| c.as_os_str().to_string_lossy());
            match (parts.next(), parts.next()) {
                (Some(dir), Some(name)) if dir == "deps" || dir == "build" => {
                    if let Some(hash) = unit_of(&name) {
                        used.insert(hash.to_string());
                    }
                }
                (Some(_), None) => top.push(path.to_path_buf()),
                _ => {}
            }
        }
    }
    if top.is_empty() {
        return Ok(used);
    }
    let deps = profile_dir.join("deps");
    let ids: Vec<_> = top.iter().filter_map(|path| file_id(path)).collect();
    for entry in std::fs::read_dir(&deps).map_err(|e| anyhow::anyhow!("read {}: {e}", deps.display()))? {
        let entry = entry.map_err(|e| anyhow::anyhow!("read {}: {e}", deps.display()))?;
        if file_id(&entry.path()).is_some_and(|id| ids.contains(&id)) {
            if let Some(hash) = unit_of(&entry.file_name().to_string_lossy()) {
                used.insert(hash.to_string());
            }
        }
    }
    Ok(used)
}

/// The folders right under `under` that the units cargo's messages report
/// were compiled from (each unit's `manifest_path`).
pub fn source_folders(messages: &[u8], under: &Path) -> BTreeSet<String> {
    messages
        .split(|b| *b == b'\n')
        .filter_map(|line| serde_json::from_slice::<serde_json::Value>(line).ok())
        .filter_map(|message| {
            let path = Path::new(message["manifest_path"].as_str()?).strip_prefix(under).ok()?.components().next()?;
            Some(path.as_os_str().to_string_lossy().into_owned())
        })
        .collect()
}

/// The device and inode of a file: equal for two hard links of one file.
fn file_id(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| (m.dev(), m.ino()))
}

/// Remove from `profile_dir` (a `target/<profile>`) every unit whose hash
/// is not in `used`.
pub fn keep_used(profile_dir: &Path, used: &BTreeSet<String>) -> anyhow::Result<Swept> {
    let gone: BTreeSet<String> = units_in(profile_dir)?.into_iter().filter(|hash| !used.contains(hash)).collect();
    remove_units(profile_dir, &gone)
}

/// Remove from every profile folder of `target` (`debug`, `release`, ...)
/// the units no up-to-date build uses.
pub fn drop_superseded(target: &Path) -> anyhow::Result<Swept> {
    let mut swept = Swept::default();
    for entry in std::fs::read_dir(target).map_err(|e| anyhow::anyhow!("read {}: {e}", target.display()))? {
        let profile_dir = entry.map_err(|e| anyhow::anyhow!("read {}: {e}", target.display()))?.path();
        if profile_dir.join(".fingerprint").is_dir() {
            swept += drop_superseded_in(&profile_dir)?;
        }
    }
    Ok(swept)
}

/// One unit's fingerprint folder, read.
struct Unit {
    /// The package, from the folder's name (`<package>-<hash>`).
    package: String,
    /// When cargo last compiled it.
    compiled: std::time::SystemTime,
    /// One entry per fingerprint file (`lib-<crate>`, `test-lib-<crate>`,
    /// `run-build-script-...`).
    builds: Vec<Build>,
}

/// One fingerprint file of a unit: its kind, how it was built, and what
/// against (each dep's name and the fingerprint it had).
type Build = (String, String, Vec<(String, String)>);

fn drop_superseded_in(profile_dir: &Path) -> anyhow::Result<Swept> {
    let fingerprints = profile_dir.join(".fingerprint");
    let mut units: BTreeMap<String, Unit> = BTreeMap::new();
    // The fingerprint a unit's file records (as hex) -> the unit.
    let mut by_fingerprint: HashMap<String, String> = HashMap::new();
    for entry in std::fs::read_dir(&fingerprints).map_err(|e| anyhow::anyhow!("read {}: {e}", fingerprints.display()))? {
        let entry = entry.map_err(|e| anyhow::anyhow!("read {}: {e}", fingerprints.display()))?;
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(hash) = unit_of(&name).map(str::to_string) else { continue };
        let package = name[..name.len() - hash.len() - 1].to_string();
        let dir = entry.path();
        let mut builds = Vec::new();
        // When cargo last wrote any of its fingerprint files: a build
        // script's run has no `invoked.timestamp`.
        let mut compiled = std::time::UNIX_EPOCH;
        for file in std::fs::read_dir(&dir).map_err(|e| anyhow::anyhow!("read {}: {e}", dir.display()))? {
            let file = file.map_err(|e| anyhow::anyhow!("read {}: {e}", dir.display()))?.path();
            if file.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let Ok(record) = std::fs::read(&file).map_err(anyhow::Error::from).and_then(|b| Ok(serde_json::from_slice::<serde_json::Value>(&b)?)) else {
                continue;
            };
            if let Ok(fingerprint) = std::fs::read_to_string(file.with_extension("")) {
                by_fingerprint.insert(fingerprint.trim().to_string(), hash.clone());
            }
            if let Ok(written) = std::fs::metadata(&file).and_then(|m| m.modified()) {
                compiled = compiled.max(written);
            }
            let kind = file.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
            // Everything that says how it was built, except what changes
            // with the sources or the lock: `path` (a registry crate's
            // folder names its version), `local` and the deps' own hashes.
            let how = ["rustc", "features", "target", "profile", "rustflags", "config", "compile_kind"]
                .iter()
                .map(|key| record[*key].to_string())
                .collect::<Vec<_>>()
                .join(" ");
            let against = record["deps"]
                .as_array()
                .into_iter()
                .flatten()
                // cargo writes a fingerprint as the hex of its little-endian bytes.
                .filter_map(|dep| Some((dep[1].as_str()?.to_string(), hex::encode(dep[3].as_u64()?.to_le_bytes()))))
                .collect();
            builds.push((kind, how, against));
        }
        // A folder cargo has not written a fingerprint into yet is left
        // alone.
        if builds.is_empty() {
            continue;
        }
        builds.sort();
        units.insert(hash, Unit { package, compiled, builds });
    }

    // How a unit was built, its deps' included, as one key: two units with
    // the same key are generations of one piece. A dep is found by the
    // fingerprint the unit recorded for it; one recompiled since (the
    // unit itself is then out of date, and rebuilt in place by its next
    // build) counts by its name alone.
    fn key_of(hash: &str, units: &BTreeMap<String, Unit>, by_fingerprint: &HashMap<String, String>, keys: &mut HashMap<String, String>) -> String {
        if let Some(key) = keys.get(hash) {
            return key.clone();
        }
        let unit = &units[hash];
        let mut key = unit.package.clone();
        for (kind, how, against) in &unit.builds {
            key.push_str(&format!("|{kind} {how}"));
            let mut deps: Vec<String> = against
                .iter()
                .map(|(name, fingerprint)| match by_fingerprint.get(fingerprint) {
                    Some(dep) if dep != hash => format!("{name}={}", key_of(dep, units, by_fingerprint, keys)),
                    _ => format!("{name}=?"),
                })
                .collect();
            deps.sort();
            key.push_str(&format!("[{}]", deps.join(",")));
        }
        let key = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(key.as_bytes()));
        keys.insert(hash.to_string(), key.clone());
        key
    }

    // The tops: units no other unit was built against (test and program
    // binaries, the libraries a build asked for). Of the units built the
    // same way only the newest is today's; every unit a top of today's was
    // built against is used, and nothing else is.
    let mut keys = HashMap::new();
    let referenced: HashSet<&String> = units
        .values()
        .flat_map(|unit| unit.builds.iter().flat_map(|(_, _, against)| against.iter()))
        .filter_map(|(_, fingerprint)| by_fingerprint.get(fingerprint))
        .collect();
    let newest_by_key = |hashes: &mut dyn Iterator<Item = &String>, keys: &mut HashMap<String, String>| {
        let mut newest: HashMap<String, &String> = HashMap::new();
        for hash in hashes {
            let key = key_of(hash, &units, &by_fingerprint, keys);
            if newest.get(&key).is_none_or(|other| units[*other].compiled < units[hash].compiled) {
                newest.insert(key, hash);
            }
        }
        newest.into_values().cloned().collect::<Vec<String>>()
    };
    let mut used: HashSet<String> = HashSet::new();
    // A top counts only when no unit built the same way is newer: an old
    // generation of a library nothing is built against any more is a top
    // too, and its newer generation is not.
    let newest = newest_by_key(&mut units.keys(), &mut keys);
    let mut todo: Vec<String> = newest.into_iter().filter(|hash| !referenced.contains(hash)).collect();
    while let Some(hash) = todo.pop() {
        if !used.insert(hash.clone()) {
            continue;
        }
        for (_, _, against) in &units[&hash].builds {
            for (name, fingerprint) in against {
                match by_fingerprint.get(fingerprint) {
                    Some(dep) => todo.push(dep.clone()),
                    // Built against a dep since recompiled in its place:
                    // the unit is out of date, and its next build uses the
                    // newest build of that dep, whichever way it is built.
                    None => todo.extend(newest_by_key(
                        &mut units.iter().filter(|(_, unit)| unit.package.replace('-', "_") == *name).map(|(hash, _)| hash),
                        &mut keys,
                    )),
                }
            }
        }
    }
    let gone: BTreeSet<String> = units.keys().filter(|hash| !used.contains(*hash)).cloned().collect();
    for hash in &gone {
        units.remove(hash);
    }
    let mut swept = remove_units(profile_dir, &gone)?;
    swept += trim_incremental(profile_dir, &units)?;
    Ok(swept)
}

/// rustc keeps a crate's incremental state in `incremental/<crate>-<id>`,
/// a name cargo's hash is not in, so it is bounded by count: a crate keeps
/// as many folders as it has units left, the most recently written first.
fn trim_incremental(profile_dir: &Path, units: &BTreeMap<String, Unit>) -> anyhow::Result<Swept> {
    let incremental = profile_dir.join("incremental");
    let Ok(entries) = std::fs::read_dir(&incremental) else { return Ok(Swept::default()) };
    let mut wanted: HashMap<String, usize> = HashMap::new();
    for unit in units.values() {
        *wanted.entry(unit.package.replace('-', "_")).or_default() += 1;
    }
    let mut by_crate: HashMap<String, Vec<(std::time::SystemTime, PathBuf)>> = HashMap::new();
    for entry in entries {
        let path = entry.map_err(|e| anyhow::anyhow!("read {}: {e}", incremental.display()))?.path();
        let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
        let Some((krate, _)) = name.rsplit_once('-') else { continue };
        let written = std::fs::metadata(&path).and_then(|m| m.modified()).unwrap_or(std::time::UNIX_EPOCH);
        by_crate.entry(krate.to_string()).or_default().push((written, path));
    }
    let mut swept = Swept::default();
    for (krate, mut dirs) in by_crate {
        dirs.sort_by_key(|dir| std::cmp::Reverse(dir.0));
        for (_, dir) in dirs.into_iter().skip(wanted.get(&krate).copied().unwrap_or(0)) {
            swept.bytes += size_of(&dir);
            std::fs::remove_dir_all(&dir).map_err(|e| anyhow::anyhow!("remove {}: {e}", dir.display()))?;
        }
    }
    Ok(swept)
}

/// Every unit hash with a file in `profile_dir`.
fn units_in(profile_dir: &Path) -> anyhow::Result<BTreeSet<String>> {
    let mut units = BTreeSet::new();
    for dir in ["deps", "examples", ".fingerprint", "build"] {
        let Ok(entries) = std::fs::read_dir(profile_dir.join(dir)) else { continue };
        for entry in entries {
            let entry = entry.map_err(|e| anyhow::anyhow!("read {}: {e}", profile_dir.join(dir).display()))?;
            if let Some(hash) = unit_of(&entry.file_name().to_string_lossy()) {
                units.insert(hash.to_string());
            }
        }
    }
    Ok(units)
}

/// Remove every file of the units `gone` from `profile_dir`.
fn remove_units(profile_dir: &Path, gone: &BTreeSet<String>) -> anyhow::Result<Swept> {
    let mut swept = Swept { units: gone.len(), bytes: 0 };
    if gone.is_empty() {
        return Ok(swept);
    }
    let gone: HashSet<&str> = gone.iter().map(String::as_str).collect();
    for dir in ["deps", "examples", ".fingerprint", "build"] {
        let dir = profile_dir.join(dir);
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries {
            let entry = entry.map_err(|e| anyhow::anyhow!("read {}: {e}", dir.display()))?;
            if !unit_of(&entry.file_name().to_string_lossy()).is_some_and(|hash| gone.contains(hash)) {
                continue;
            }
            let path = entry.path();
            swept.bytes += size_of(&path);
            let removed = if path.is_dir() { std::fs::remove_dir_all(&path) } else { std::fs::remove_file(&path) };
            removed.map_err(|e| anyhow::anyhow!("remove {}: {e}", path.display()))?;
        }
    }
    Ok(swept)
}

/// The bytes a file or folder takes, its contents included.
fn size_of(path: &Path) -> u64 {
    let Ok(meta) = std::fs::symlink_metadata(path) else { return 0 };
    if !meta.is_dir() {
        return meta.len();
    }
    std::fs::read_dir(path).into_iter().flatten().flatten().map(|entry| size_of(&entry.path())).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "00000000000000a1";
    const B: &str = "00000000000000b2";
    const C: &str = "00000000000000c3";
    const D: &str = "00000000000000d4";

    fn touch(path: &Path, bytes: &[u8]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    /// A unit `package-hash` built with `features` against `deps` (each a
    /// dep's name and the fingerprint it recorded), whose own fingerprint
    /// is `fingerprint`, compiled `age` seconds after the epoch.
    fn unit(profile: &Path, package: &str, hash: &str, features: &str, fingerprint: u64, deps: &[(&str, u64)], age: u64) {
        let dir = profile.join(".fingerprint").join(format!("{package}-{hash}"));
        let krate = package.replace('-', "_");
        let record = serde_json::json!({
            "rustc": 1, "features": features, "target": 2, "profile": 3, "path": 4, "rustflags": [], "config": 5, "compile_kind": 0,
            "deps": deps.iter().map(|(name, fp)| serde_json::json!([9, name, false, fp])).collect::<Vec<_>>(),
            "local": [],
        });
        touch(&dir.join(format!("lib-{krate}.json")), record.to_string().as_bytes());
        touch(&dir.join(format!("lib-{krate}")), hex::encode(fingerprint.to_le_bytes()).as_bytes());
        touch(&dir.join("invoked.timestamp"), b"");
        let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000 + age);
        std::fs::File::options().write(true).open(dir.join(format!("lib-{krate}.json"))).unwrap().set_modified(at).unwrap();
        touch(&profile.join("deps").join(format!("lib{krate}-{hash}.rlib")), b"rlib");
        touch(&profile.join("deps").join(format!("{krate}-{hash}.d")), b"d");
    }

    fn has(profile: &Path, package: &str, hash: &str) -> bool {
        profile.join(".fingerprint").join(format!("{package}-{hash}")).exists()
            && profile.join("deps").join(format!("lib{}-{hash}.rlib", package.replace('-', "_"))).exists()
    }

    /// Of two units built the same way, the older goes with all its files;
    /// one built with other features is another piece and stays, and so
    /// does each dependent, told apart by how its dep was built.
    #[test]
    fn only_the_newest_build_of_each_piece_stays() {
        let target = tempfile::tempdir().unwrap();
        let debug = target.path().join("debug");
        unit(&debug, "tokio", A, "[\"rt\"]", 0xa, &[], 1);
        unit(&debug, "tokio", B, "[\"rt\"]", 0xb, &[], 2);
        unit(&debug, "tokio", C, "[\"rt\", \"net\"]", 0xc, &[], 1);
        unit(&debug, "weft-core", D, "[]", 0xd, &[("tokio", 0xb)], 3);
        unit(&debug, "weft-core", "00000000000000e5", "[]", 0xe, &[("tokio", 0xc)], 3);
        let swept = drop_superseded(target.path()).unwrap();
        assert_eq!(swept.units, 1);
        assert!(!has(&debug, "tokio", A) && !debug.join("deps").join(format!("tokio-{A}.d")).exists(), "the older generation goes whole");
        assert!(has(&debug, "tokio", B) && has(&debug, "tokio", C));
        assert!(has(&debug, "weft-core", D) && has(&debug, "weft-core", "00000000000000e5"), "built against different tokios");
    }

    /// A unit built against a version of its dep since replaced is an
    /// older generation of the unit built against the new one.
    #[test]
    fn a_unit_built_against_a_replaced_dep_is_an_older_generation() {
        let target = tempfile::tempdir().unwrap();
        let release = target.path().join("release");
        unit(&release, "serde", A, "[]", 0xa, &[], 1);
        unit(&release, "serde", B, "[]", 0xb, &[], 2);
        unit(&release, "weft-core", C, "[]", 0xc, &[("serde", 0xa)], 1);
        unit(&release, "weft-core", D, "[]", 0xd, &[("serde", 0xb)], 2);
        drop_superseded(target.path()).unwrap();
        assert!(!has(&release, "serde", A) && !has(&release, "weft-core", C));
        assert!(has(&release, "serde", B) && has(&release, "weft-core", D));
    }

    /// Two versions of one crate a build uses side by side are both kept,
    /// however alike they were built.
    #[test]
    fn two_versions_of_a_crate_in_use_both_stay() {
        let target = tempfile::tempdir().unwrap();
        let debug = target.path().join("debug");
        unit(&debug, "hyper", A, "[]", 0xa, &[], 1);
        unit(&debug, "hyper", B, "[]", 0xb, &[], 2);
        unit(&debug, "weft-core", C, "[]", 0xc, &[("hyper", 0xa), ("hyper", 0xb)], 3);
        drop_superseded(target.path()).unwrap();
        assert!(has(&debug, "hyper", A) && has(&debug, "hyper", B) && has(&debug, "weft-core", C));
    }

    /// The units a build used are read off cargo's messages, a binary
    /// traced through its hard link to its unit, and everything else goes;
    /// the shared folders its units came from are read off them too.
    #[test]
    fn a_cache_keeps_the_units_its_builds_used() {
        let dir = tempfile::tempdir().unwrap();
        let release = dir.path().join("release");
        for hash in [A, B, C, D] {
            unit(&release, "pkg", hash, "[]", 1, &[], 1);
        }
        let exe = release.join("deps").join(format!("worker-{C}"));
        touch(&exe, b"\x7fELF");
        std::fs::hard_link(&exe, release.join("worker")).unwrap();
        let messages = [
            serde_json::json!({
                "reason": "compiler-artifact", "fresh": true, "filenames": [release.join(format!("deps/libpkg-{A}.rlib"))],
                "manifest_path": dir.path().join("shared/pkg_x-1/Cargo.toml"),
            }),
            serde_json::json!({"reason": "build-script-executed", "out_dir": release.join(format!("build/pkg-{B}/out"))}),
            serde_json::json!({"reason": "compiler-artifact", "filenames": [release.join("worker")]}),
        ]
        .iter()
        .map(|m| format!("{m}\n"))
        .collect::<String>();
        let used = used_units(messages.as_bytes(), &release).unwrap();
        assert_eq!(used, [A, B, C].iter().map(|h| h.to_string()).collect());
        assert_eq!(source_folders(messages.as_bytes(), &dir.path().join("shared")), ["pkg_x-1".to_string()].into());
        keep_used(&release, &used).unwrap();
        assert!(has(&release, "pkg", A) && has(&release, "pkg", B) && exe.exists());
        assert!(!has(&release, "pkg", D) && !release.join("deps").join(format!("pkg-{D}.d")).exists());
    }
}
