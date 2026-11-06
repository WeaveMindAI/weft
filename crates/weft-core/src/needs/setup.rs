//! Meeting a need weft installs itself ([`Recipe`]): use the machine's own
//! when it has one, else the one in weft's folder for it, else install it
//! there.
//!
//! The folder is shared by every program and every install on the
//! machine, so two setups may want the same need at once (two nodes, two
//! projects, two installs): an exclusive lock beside the folder makes one
//! of them install it while the others wait, then find it there.
//!
//! A folder counts as installed only once it holds [`INSTALLED_MARK`],
//! written (through a rename) after the script succeeded and the need's
//! check passed with its programs on the `PATH`. A script stopped halfway
//! (a crash, a stop, a machine shut down) leaves a folder without it,
//! which the next setup empties and installs again. The script installs
//! straight into the folder it keeps, never into a temporary one renamed
//! into place: what such scripts install (a Python environment, a package
//! manager's launchers) writes its own absolute path into its files, and a
//! renamed copy would point at a folder that is gone.

use std::fs::File;
use std::path::{Path, PathBuf};

use super::{Need, Os, Recipe, System};
use crate::file_lock;

/// The file in a need's folder saying its install finished.
// SYNC: INSTALLED_MARK <-> catalog/agents/agent.rs (computer_script, which installs the same way inside a computer)
pub const INSTALLED_MARK: &str = ".weft-installed";

/// How many of the install log's last lines a failure carries.
const LOG_TAIL: usize = 20;

/// Where a recipe's need lives under the needs folder `root` (`needs/` in
/// weft's data folder, `weft_platform_local::MachineLayout::needs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Place {
    /// `<root>/<group>/<name>`: what the script installs into
    /// (`WEFT_NEED_DIR`).
    pub dir: PathBuf,
    /// `<root>/<group>`: shared by the group (`WEFT_NEEDS_ROOT`).
    pub group: PathBuf,
    /// The folders of `dir` holding its programs.
    pub bins: Vec<PathBuf>,
    /// `<group>/<name>.lock`: held by whoever installs it.
    pub lock: PathBuf,
    /// `<group>/<name>.install.log`: what the latest install printed.
    /// Beside the folder, which an install empties first.
    pub log: PathBuf,
}

impl Recipe {
    /// Where it lives under the needs folder `root`.
    pub fn place(&self, root: &Path) -> Result<Place, String> {
        if let Some(why) = self.refusal() {
            return Err(format!("its recipe {why}"));
        }
        let (group, name) = self.folder.split_once('/').expect("a checked folder is <group>/<name>");
        let group = root.join(group);
        let dir = group.join(name);
        Ok(Place {
            bins: self.bin.iter().map(|b| dir.join(b)).collect(),
            lock: group.join(format!("{name}.lock")),
            log: group.join(format!("{name}.install.log")),
            dir,
            group,
        })
    }
}

/// `bins` put first on `path`.
pub fn path_with(bins: &[PathBuf], path: &str) -> String {
    bins.iter().map(|b| b.display().to_string()).chain((!path.is_empty()).then(|| path.to_string())).collect::<Vec<_>>().join(":")
}

/// What a setup is doing, as it goes, for whoever waits on it to show.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetupStep {
    /// Another setup holds the lock: it is installing the need.
    Waiting,
    /// Installing it, writing to `log`.
    Installing { log: PathBuf },
}

impl SetupStep {
    /// The step said of the need called `name`.
    pub fn words(&self, name: &str) -> String {
        match self {
            Self::Waiting => format!("waiting for another setup to finish installing {name}"),
            Self::Installing { log } => format!("installing {name} (its log: {})", log.display()),
        }
    }
}

impl Need {
    /// The folders that put this need on `path` when it is met without an
    /// install: none when it is met on `path` as it is, its recipe's
    /// program folders when its folder under `root` holds a finished
    /// install it is met with; `None` when neither. Blocks while its check
    /// runs.
    pub fn met_at(&self, root: &Path, path: &str) -> Option<Vec<PathBuf>> {
        if self.is_met_on(path) {
            return Some(Vec::new());
        }
        let place = self.recipe.as_ref()?.place(root).ok()?;
        (place.dir.join(INSTALLED_MARK).is_file() && self.is_met_on(&path_with(&place.bins, path))).then_some(place.bins)
    }

    /// Meet this need on a machine whose `PATH` is `path`: the folders to
    /// put first on it ([`Self::met_at`]), after installing it with its
    /// recipe into its folder under `root` (the needs folder) when it is
    /// met nowhere yet. `on_step` hears what it is doing while it takes
    /// time. `Err` says what is missing, or what the install printed when
    /// it failed, and how to get it by hand.
    ///
    /// No deadline: a person waits on it and an install takes as long as
    /// its downloads do. Blocks throughout.
    // SYNC: Need::meet <-> catalog/agents/agent.rs (install_lines: the same steps in bash, inside a computer)
    pub fn meet(&self, root: &Path, path: &str, system: &Result<System, String>, on_step: &mut dyn FnMut(SetupStep)) -> Result<Vec<PathBuf>, String> {
        if let Some(bins) = self.met_at(root, path) {
            return Ok(bins);
        }
        let name = &self.name;
        let Some(recipe) = &self.recipe else {
            return Err(format!("{name} is not installed (`{}` does not run); {}", self.check.join(" "), self.by_hand()));
        };
        let place = recipe.place(root).map_err(|why| format!("{name} cannot be installed: {why}"))?;
        let os = Os::here().ok_or_else(|| format!("weft installs {name} on Linux and macOS only; {}", self.by_hand()))?;
        let script = recipe.script.get(&os).ok_or_else(|| format!("{name} has no recipe weft installs it with on {}; {}", os.name(), self.by_hand()))?;
        if let Ok(system) = system {
            let lacking = self.packages_for(system.manager, system.distro.as_deref()).map(|p| system.lacking(p)).unwrap_or_default();
            if !lacking.is_empty() {
                return Err(format!(
                    "{name} needs system packages weft cannot install without sudo; install them with:  {}",
                    system.install_command(&lacking).join(" ")
                ));
            }
        }
        let lock = file_lock::hold(&place.lock, || on_step(SetupStep::Waiting)).map_err(|e| format!("lock {}: {e}", place.lock.display()))?;
        // Another setup may have installed it while this one waited.
        if let Some(bins) = self.met_at(root, path) {
            return Ok(bins);
        }
        on_step(SetupStep::Installing { log: place.log.clone() });
        self.install(&place, &recipe.prelude, script, path, &lock)?;
        let mark = place.dir.join(INSTALLED_MARK);
        let partial = place.dir.join(format!("{INSTALLED_MARK}.tmp"));
        std::fs::write(&partial, b"").and_then(|()| std::fs::rename(&partial, &mark)).map_err(|e| format!("write {}: {e}", mark.display()))?;
        Ok(place.bins)
    }

    /// Run `script` (after `prelude`) into `place`'s folder, emptied
    /// first, and check the need with its programs on `path`. `lock` is the
    /// script's standard input: an empty file it reads as nothing (an
    /// install runs unattended), whose lock it, and everything it starts,
    /// holds through that descriptor until it ends. So a setup that dies
    /// while its script still runs leaves the lock held until the script
    /// is done too, and nothing installs over it meanwhile.
    fn install(&self, place: &Place, prelude: &str, script: &str, path: &str, lock: &File) -> Result<(), String> {
        let name = &self.name;
        let io = |what: &Path, e: std::io::Error| format!("installing {name}: {}: {e}", what.display());
        match std::fs::remove_dir_all(&place.dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io(&place.dir, e)),
        }
        std::fs::create_dir_all(&place.dir).map_err(|e| io(&place.dir, e))?;
        // The prelude is a file of its own, never pasted above the script:
        // a helper that runs bash again sources the file it was defined in
        // (`BASH_SOURCE`), which must not be the script.
        let stem = place.dir.file_name().expect("a need's folder has a name").to_string_lossy().to_string();
        let prelude_file = place.group.join(format!("{stem}.prelude.sh"));
        let script_file = place.group.join(format!("{stem}.install.sh"));
        std::fs::write(&prelude_file, prelude).map_err(|e| io(&prelude_file, e))?;
        let quoted = format!("'{}'", prelude_file.display().to_string().replace('\'', "'\\''"));
        std::fs::write(&script_file, format!("set -euo pipefail\nsource {quoted}\n{script}\n")).map_err(|e| io(&script_file, e))?;
        let log = File::create(&place.log).map_err(|e| io(&place.log, e))?;
        let status = std::process::Command::new("bash")
            .arg(&script_file)
            .current_dir(&place.dir)
            .env("PATH", path)
            .env("WEFT_NEED_DIR", &place.dir)
            .env("WEFT_NEEDS_ROOT", &place.group)
            .stdin(lock.try_clone().map_err(|e| io(&place.lock, e))?)
            .stdout(log.try_clone().map_err(|e| io(&place.log, e))?)
            .stderr(log)
            .status()
            .map_err(|e| format!("installing {name}: run bash: {e}"))?;
        if !status.success() {
            return Err(format!(
                "installing {name} into {} failed ({status}); the end of its log, {}:\n{}\nfix what it says, or install it yourself: {}",
                place.dir.display(),
                place.log.display(),
                tail(&place.log),
                self.by_hand()
            ));
        }
        if !self.is_met_on(&path_with(&place.bins, path)) {
            return Err(format!(
                "{name} was installed into {} and `{}` still does not run with {} on the PATH; the end of the install's log, {}:\n{}",
                place.dir.display(),
                self.check.join(" "),
                place.bins.iter().map(|b| b.display().to_string()).collect::<Vec<_>>().join(":"),
                place.log.display(),
                tail(&place.log)
            ));
        }
        Ok(())
    }
}

/// The last lines of the file at `path`.
fn tail(path: &Path) -> String {
    match std::fs::read(path) {
        Ok(bytes) => {
            let text = String::from_utf8_lossy(&bytes);
            let lines: Vec<&str> = text.lines().collect();
            lines[lines.len().saturating_sub(LOG_TAIL)..].join("\n")
        }
        Err(e) => format!("(it could not be read: {e})"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{BufRead, Write};

    use super::*;
    use crate::needs::NeedStage;

    /// Every test's own folder: weft's data folder under it, and the file
    /// its recipes count their runs in.
    struct Rig {
        dir: tempfile::TempDir,
    }

    impl Rig {
        fn new() -> Self {
            Self { dir: tempfile::tempdir().unwrap() }
        }

        fn root(&self) -> PathBuf {
            self.dir.path().join("root")
        }

        /// A need for the program `fakeprog`, which its recipe `script`
        /// installs into `<root>/tools/fake/bin`; the prelude's `say_done`
        /// counts a run.
        fn need(&self, script: &str) -> Need {
            let os = Os::here().expect("the tests run on Linux or macOS");
            Need {
                name: "Fake".into(),
                stage: NeedStage::Run,
                check: vec!["fakeprog".into()],
                install: BTreeMap::new(),
                how: Some("get it from fake.example".into()),
                recipe: Some(Recipe {
                    folder: "tools/fake".into(),
                    script: [(os, script.to_string())].into(),
                    bin: vec!["bin".into()],
                    prelude: format!("say_done() {{ echo \"$1\" >> '{}'; }}", self.dir.path().join("count").display()),
                }),
            }
        }

        /// A recipe that counts its run and installs a `fakeprog` that
        /// exits 0, after `before`.
        fn counting(&self, before: &str) -> Need {
            self.need(&format!(
                "{before}\nsay_done ran\nmkdir -p \"$WEFT_NEED_DIR/bin\"\nprintf '#!/bin/sh\\nexit 0\\n' > \"$WEFT_NEED_DIR/bin/fakeprog\"\nchmod +x \"$WEFT_NEED_DIR/bin/fakeprog\""
            ))
        }

        /// How many times a recipe ran.
        fn runs(&self) -> usize {
            std::fs::read_to_string(self.dir.path().join("count")).map(|s| s.lines().count()).unwrap_or(0)
        }

        fn meet(&self, need: &Need) -> Result<Vec<PathBuf>, String> {
            need.meet(&self.root(), &system_path(), &Err("not asked".into()), &mut |_| {})
        }
    }

    /// The tests' PATH: the system's, which has bash and coreutils and no
    /// `fakeprog`.
    fn system_path() -> String {
        "/usr/bin:/bin".into()
    }

    #[test]
    fn a_need_on_the_path_is_met_as_it_is_and_nothing_is_installed() {
        let rig = Rig::new();
        let need = Need { check: vec!["true".into()], ..rig.counting("") };
        assert_eq!(rig.meet(&need), Ok(Vec::new()));
        assert_eq!(rig.runs(), 0);
        assert!(!rig.root().exists(), "no folder is made for it");
    }

    #[test]
    fn a_need_missing_is_installed_once_then_found_in_its_folder() {
        let rig = Rig::new();
        let need = rig.counting("");
        let bins = rig.meet(&need).unwrap();
        assert_eq!(bins, [rig.root().join("tools/fake/bin")]);
        assert_eq!(rig.runs(), 1);
        assert!(rig.root().join("tools/fake").join(INSTALLED_MARK).is_file(), "marked installed");
        assert_eq!(need.met_at(&rig.root(), &system_path()), Some(bins.clone()), "found in its folder");
        assert!(need.is_met_on(&path_with(&bins, &system_path())));
        assert_eq!(rig.meet(&need), Ok(bins), "found again");
        assert_eq!(rig.runs(), 1, "never installed twice");
        let log = std::fs::read_to_string(rig.root().join("tools/fake.install.log")).unwrap();
        assert!(log.is_empty(), "what the script printed: {log}");
    }

    /// A folder without the mark is what an install stopped halfway left:
    /// emptied and installed again, never taken as installed even when its
    /// program runs.
    #[test]
    fn a_half_done_install_is_never_taken_as_done() {
        let rig = Rig::new();
        let dir = rig.root().join("tools/fake");
        std::fs::create_dir_all(dir.join("bin")).unwrap();
        std::fs::write(dir.join("leftover"), "half").unwrap();
        std::fs::write(dir.join("bin/fakeprog"), "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(dir.join("bin/fakeprog"), std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
        let need = rig.counting("");
        assert_eq!(need.met_at(&rig.root(), &system_path()), None, "no mark, not installed");
        rig.meet(&need).unwrap();
        assert_eq!(rig.runs(), 1, "installed again");
        assert!(!dir.join("leftover").exists(), "into an emptied folder");
    }

    #[test]
    fn a_failed_install_fails_with_its_log_and_leaves_nothing_marked() {
        let rig = Rig::new();
        let need = rig.need("say_done ran\necho 'no network here'\nexit 3");
        let why = rig.meet(&need).unwrap_err();
        assert!(why.starts_with(&format!("installing Fake into {} failed (exit status: 3)", rig.root().join("tools/fake").display())), "{why}");
        assert!(why.contains("\nno network here\n"), "the log's end: {why}");
        assert!(why.ends_with("or install it yourself: get it from fake.example"), "{why}");
        assert!(!rig.root().join("tools/fake").join(INSTALLED_MARK).exists());
        assert!(rig.meet(&need).is_err());
        assert_eq!(rig.runs(), 2, "a failed install is tried again by the next setup");
        let silent = rig.need("true");
        let why = rig.meet(&silent).unwrap_err();
        assert!(why.starts_with("Fake was installed into") && why.contains("`fakeprog` still does not run"), "{why}");
        assert!(!rig.root().join("tools/fake").join(INSTALLED_MARK).exists());
    }

    #[test]
    fn a_need_weft_cannot_install_says_how_to_get_it() {
        let rig = Rig::new();
        let need = Need { recipe: None, ..rig.need("") };
        let why = need.meet(Path::new("/nowhere"), &system_path(), &Err("not asked".into()), &mut |_| {}).unwrap_err();
        assert_eq!(why, "Fake is not installed (`fakeprog` does not run); get it from fake.example");
        let mut elsewhere = rig.need("");
        elsewhere.recipe.as_mut().unwrap().script.clear();
        let why = elsewhere.meet(Path::new("/nowhere"), &system_path(), &Err("not asked".into()), &mut |_| {}).unwrap_err();
        assert!(why.starts_with("Fake has no recipe weft installs it with on "), "{why}");
    }

    #[test]
    fn a_recipe_installs_under_its_group_and_nowhere_else() {
        let recipe = |folder: &str, bin: &str| Recipe { folder: folder.into(), script: BTreeMap::new(), bin: vec![bin.into()], prelude: String::new() };
        let place = recipe("agents/claude", "bin").place(Path::new("/d")).unwrap();
        assert_eq!(place.dir, Path::new("/d/agents/claude"));
        assert_eq!(place.group, Path::new("/d/agents"));
        assert_eq!(place.lock, Path::new("/d/agents/claude.lock"));
        assert_eq!(place.log, Path::new("/d/agents/claude.install.log"));
        assert_eq!(place.bins, [Path::new("/d/agents/claude/bin")]);
        assert!(recipe("agents/x", "../node-22/bin").refusal().is_none(), "a runtime the group shares");
        for (folder, bin) in [("claude", "bin"), ("a/b/c", "bin"), ("../x", "bin"), ("a/..", "bin"), ("a/ b", "bin"), ("a/b", "/usr/bin"), ("a/b", "../../x")] {
            assert!(recipe(folder, bin).refusal().is_some(), "{folder} {bin}");
        }
        assert_eq!(path_with(&[PathBuf::from("/a"), PathBuf::from("/b")], "/usr/bin"), "/a:/b:/usr/bin");
        assert_eq!(path_with(&[PathBuf::from("/a")], ""), "/a");
    }

    /// Two setups of the same need at once: one installs while the other
    /// waits on the lock, then finds it installed. The recipe blocks on a
    /// pipe until the second setup is seen waiting, so the two really
    /// overlap.
    #[test]
    fn two_setups_at_once_run_the_recipe_once() {
        let rig = Rig::new();
        let go = rig.dir.path().join("go");
        assert!(std::process::Command::new("mkfifo").arg(&go).status().unwrap().success());
        let need = rig.counting(&format!("read -r _ < '{}'", go.display()));
        let (steps, heard) = std::sync::mpsc::channel();
        let both = std::thread::scope(|scope| {
            let setup = |who: &'static str| {
                let steps = steps.clone();
                let (need, root) = (&need, rig.root());
                move || need.meet(&root, &system_path(), &Err("not asked".into()), &mut |step| steps.send((who, step)).unwrap())
            };
            let first = scope.spawn(setup("first"));
            assert!(matches!(heard.recv().unwrap(), ("first", SetupStep::Installing { .. })));
            let second = scope.spawn(setup("second"));
            assert_eq!(heard.recv().unwrap(), ("second", SetupStep::Waiting));
            std::fs::OpenOptions::new().write(true).open(&go).unwrap().write_all(b"go\n").unwrap();
            [first.join().unwrap(), second.join().unwrap()]
        });
        let bins = vec![rig.root().join("tools/fake/bin")];
        assert_eq!(both, [Ok(bins.clone()), Ok(bins)]);
        assert_eq!(rig.runs(), 1, "the recipe ran once");
        assert!(heard.try_recv().is_err(), "the second never installed");
    }

    /// The lock is the operating system's, so a setup waits on one another
    /// process holds (another install's runtime, `weft deps-install`), and
    /// goes on once it lets go.
    #[test]
    fn a_setup_waits_on_another_process_holding_the_lock() {
        let rig = Rig::new();
        let lock = rig.root().join("tools/fake.lock");
        std::fs::create_dir_all(lock.parent().unwrap()).unwrap();
        let mut holder = std::process::Command::new("python3")
            .args(["-c", "import fcntl, sys\nf = open(sys.argv[1], 'a')\nfcntl.flock(f, fcntl.LOCK_EX)\nprint('held', flush=True)\nsys.stdin.read()"])
            .arg(&lock)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut said = String::new();
        std::io::BufReader::new(holder.stdout.take().unwrap()).read_line(&mut said).unwrap();
        assert_eq!(said, "held\n");
        let need = rig.counting("");
        let (steps, heard) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let setup = scope.spawn(|| need.meet(&rig.root(), &system_path(), &Err("not asked".into()), &mut |step| steps.send(step).unwrap()));
            assert_eq!(heard.recv().unwrap(), SetupStep::Waiting);
            assert_eq!(rig.runs(), 0, "nothing installs while the other process holds it");
            drop(holder.stdin.take());
            assert!(setup.join().unwrap().is_ok());
        });
        holder.wait().unwrap();
        assert_eq!(rig.runs(), 1);
    }

    #[test]
    fn each_step_is_said_with_the_needs_name() {
        assert_eq!(SetupStep::Waiting.words("Codex"), "waiting for another setup to finish installing Codex");
        assert_eq!(SetupStep::Installing { log: "/d/agents/codex.install.log".into() }.words("Codex"), "installing Codex (its log: /d/agents/codex.install.log)");
    }
}
