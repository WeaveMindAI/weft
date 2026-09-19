//! `weft new <name>`: scaffold a new project directory with
//! weft.toml, src/main.weft, nodes/, .weft/, and an initialized git
//! repo. With `--assistant <name>`, also install the Tangle
//! assistant persona for that AI coding assistant, copied out of
//! the weft checkout (see `AssistantSpec`). The choice is
//! remembered, so later `weft new` calls install the same
//! assistants without repeating the flag; `--assistant none`
//! clears it, and `--assistant agents` is the fallback for an
//! assistant weft has no template for.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Context;

use super::Ctx;

/// One supported AI coding assistant: which template directory under
/// `tangle/` in the weft checkout holds its persona, the shorthand the
/// CLI accepts for it, and which entries land in the project under which
/// name. Adding an assistant is one more row here (plus the template
/// directory itself); nothing else in `weft new` is assistant-specific.
#[derive(Debug)]
pub struct AssistantSpec {
    /// Directory under `tangle/` in the weft checkout.
    pub dir: &'static str,
    /// Shorthand accepted as the `--assistant` value (`cc` for
    /// `claude-code`, `kc` for `kilo-code`).
    pub shorthand: &'static str,
    /// `(name in the project, path inside the template dir)` pairs, each
    /// copied into the project.
    pub links: &'static [(&'static str, &'static str)],
}

/// Every assistant weft ships a Tangle template for. Each row's `links` are
/// the paths that assistant actually reads, which is why no two rows look
/// alike: the persona is a rules file in one and a plain `AGENTS.md` in the
/// next, and the specialists are TOML here and markdown there. The template
/// directories are independent copies on purpose (see `tangle/README.md`):
/// a wording that works better on one assistant belongs only in that
/// assistant's copy.
pub const ASSISTANTS: &[AssistantSpec] = &[
    AssistantSpec {
        dir: "claude-code",
        shorthand: "cc",
        links: &[("CLAUDE.md", "CLAUDE.md"), (".claude", ".claude")],
    },
    AssistantSpec {
        dir: "kilo-code",
        shorthand: "kc",
        links: &[("kilo.json", "kilo.json"), (".kilo", ".kilo")],
    },
    AssistantSpec {
        dir: "cursor",
        shorthand: "cu",
        links: &[(".cursor", ".cursor")],
    },
    AssistantSpec {
        dir: "codex",
        shorthand: "cx",
        links: &[
            ("AGENTS.md", "AGENTS.md"),
            (".agents", ".agents"),
            (".codex", ".codex"),
        ],
    },
    AssistantSpec {
        dir: "github-copilot",
        shorthand: "gh",
        links: &[(".github", ".github")],
    },
    AssistantSpec {
        dir: "gemini-cli",
        shorthand: "gc",
        links: &[("GEMINI.md", "GEMINI.md"), (".gemini", ".gemini")],
    },
    AssistantSpec {
        dir: "cline",
        shorthand: "cl",
        links: &[(".clinerules", ".clinerules"), (".cline", ".cline")],
    },
    AssistantSpec {
        dir: "opencode",
        shorthand: "oc",
        links: &[
            ("AGENTS.md", "AGENTS.md"),
            ("opencode.json", "opencode.json"),
            (".opencode", ".opencode"),
        ],
    },
    AssistantSpec {
        dir: "devin-desktop",
        shorthand: "dd",
        links: &[(".devin", ".devin"), (".windsurf", ".windsurf")],
    },
    AssistantSpec {
        dir: "junie",
        shorthand: "ju",
        links: &[(".junie", ".junie")],
    },
];

/// The template for an assistant weft has no row for. It is a plain
/// `AGENTS.md` plus the skills beside it, the two shapes almost every
/// assistant reads, so an unknown one still gets Tangle.
///
/// It installs ONLY when nothing else did, and that restraint is the whole
/// design. Cursor, Cline and Gemini each merge a root `AGENTS.md` with their
/// own persona file rather than treating it as an alternative, so shipping
/// both would put Tangle in the context twice, at double the tokens, with
/// two copies free to disagree.
pub const FALLBACK: AssistantSpec = AssistantSpec {
    dir: "fallback",
    shorthand: "agents",
    links: &[("AGENTS.md", "AGENTS.md"), (".agents", ".agents")],
};

/// Where the remembered default assistants live: one canonical assistant
/// dir name per line, empty (or absent) meaning "no default". Sits beside
/// the recorded `repo-root` in `~/.local/share/weft/`, same pattern.
fn assistants_record_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| {
        PathBuf::from(h)
            .join(".local/share/weft")
            .join("default-assistants")
    })
}

/// Resolve `--assistant` values the user passed right now. Strict: an
/// unknown name fails the whole command, because a typo'd
/// `--assistant cluade-code` that silently produced a Tangle-less project
/// is the bad outcome.
pub fn resolve_assistants(values: &[String]) -> anyhow::Result<Vec<&'static AssistantSpec>> {
    let mut resolved: Vec<&'static AssistantSpec> = Vec::new();
    for value in values {
        let found = std::iter::once(&FALLBACK)
            .chain(ASSISTANTS.iter())
            .find(|a| a.dir == value || a.shorthand == value);
        match found {
            Some(spec) => {
                if !resolved.iter().any(|r| r.dir == spec.dir) {
                    resolved.push(spec);
                }
            }
            None => {
                let known = ASSISTANTS
                    .iter()
                    .map(|a| format!("{} (shorthand {})", a.dir, a.shorthand))
                    .collect::<Vec<_>>()
                    .join(", ");
                anyhow::bail!(
                    "unknown assistant '{}'; known assistants: {}, \
                     or '{}' for an assistant not on that list",
                    value,
                    known,
                    FALLBACK.shorthand
                );
            }
        }
    }
    // The fallback is a fallback: it ships the persona as a root `AGENTS.md`,
    // and the assistants that have their own row read that file IN ADDITION
    // to their own persona rather than instead of it. Installing both would
    // load Tangle twice.
    if resolved.len() > 1 && resolved.iter().any(|a| a.dir == FALLBACK.dir) {
        let named: Vec<&str> = resolved
            .iter()
            .filter(|a| a.dir != FALLBACK.dir)
            .map(|a| a.dir)
            .collect();
        anyhow::bail!(
            "'{}' is for an assistant weft has no template for, so it cannot be \
             installed alongside {}: those read a root AGENTS.md on top of their \
             own persona, and Tangle would load twice. Pick one.",
            FALLBACK.shorthand,
            named.join(" and ")
        );
    }
    Ok(resolved)
}

/// Resolve the remembered defaults. Lenient: a name this weft does not
/// know (a record from another machine or a future binary) is warned
/// about and skipped, not fatal, and the rest still installs.
fn recorded_assistants() -> Vec<&'static AssistantSpec> {
    let Some(path) = assistants_record_path() else {
        return Vec::new();
    };
    let Ok(record) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    let mut resolved: Vec<&'static AssistantSpec> = Vec::new();
    for value in record.split_whitespace() {
        match std::iter::once(&FALLBACK)
            .chain(ASSISTANTS.iter())
            .find(|a| a.dir == value || a.shorthand == value)
        {
            Some(spec) => {
                if !resolved.iter().any(|r| r.dir == spec.dir) {
                    resolved.push(spec);
                }
            }
            None => {
                eprintln!(
                    "warning: remembered assistant '{}' is unknown to this weft; \
                     pass --assistant <name> to choose ({} has the record)",
                    value,
                    path.display()
                );
            }
        }
    }
    resolved
}

/// Record the assistants to install by default in future `weft new` runs.
/// One canonical dir name per line; an empty list writes an empty record
/// (the "no default" state), it never deletes the file, so "cleared" and
/// "never chosen" stay distinguishable from "unreadable".
fn write_recorded_assistants(installed: &[&AssistantSpec]) -> anyhow::Result<()> {
    let Some(path) = assistants_record_path() else {
        // No HOME to remember in; this weft install has bigger problems,
        // and skipping the memory is not worth failing the scaffold over.
        eprintln!("warning: HOME is unset; tangle default not remembered");
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context("create weft data dir")?;
    }
    let body = installed
        .iter()
        .map(|spec| spec.dir)
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&path, if body.is_empty() { String::new() } else { body + "\n" })
        .with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

pub async fn run(_ctx: Ctx, name: String, assistants: Vec<String>) -> anyhow::Result<()> {
    if name.is_empty() {
        anyhow::bail!("project name cannot be empty");
    }
    let root = PathBuf::from(&name);
    if root.exists() {
        anyhow::bail!("{} already exists", root.display());
    }

    // `--assistant none` is the explicit opt-out: install nothing and
    // clear the memory. Any other explicit choice installs and becomes
    // the new memory. No flag at all falls back to the memory.
    let explicit = !assistants.is_empty();
    let opt_out = explicit && assistants.iter().all(|a| a == "none");
    let installed = if opt_out {
        Vec::new()
    } else if explicit {
        resolve_assistants(&assistants)?
    } else {
        recorded_assistants()
    };

    let project = weft_compiler::project::Project::init(&root, &name)
        .map_err(|e| anyhow::anyhow!("init: {e}"))?;

    // Tangle is copied in, so its files are ordinary project files and
    // belong in the repo: clone the project on another machine and the
    // persona is there, with no weft checkout to point at.
    // `.env` auto-loads next to a project and is where a key the
    // project MINTS lands (`weft connect --set-env`), so it holds
    // secrets by the time anybody would commit it.
    let gitignore = String::from("target/\n.weft/\nnode_modules/\n.env\n");

    // Initialize git. Best-effort: skip quietly if git is missing.
    let git_init = Command::new("git").arg("init").current_dir(&root).status();
    match git_init {
        Ok(status) if status.success() => {
            std::fs::write(root.join(".gitignore"), gitignore).context("write .gitignore")?;
        }
        Ok(_) | Err(_) => {
            // git missing or `git init` failed. Not fatal: user can
            // opt into git later.
        }
    }

    install_tangle(&root, &installed)?;

    if explicit {
        write_recorded_assistants(&installed)?;
    }

    println!(
        "created project {} (id {}) at {}",
        project.manifest.package.name,
        project.id(),
        root.display()
    );
    let shown: Vec<&str> = installed.iter().map(|s| s.dir).collect();
    if !installed.is_empty() {
        if explicit {
            println!(
                "tangle ({}) installed: open the project in that assistant and it is there",
                shown.join(", ")
            );
        } else {
            println!(
                "tangle ({}) installed (remembered from your last choice; \
                 --assistant <name> changes it, --assistant none stops it)",
                shown.join(", ")
            );
        }
    } else if opt_out {
        println!("tangle default cleared; future projects start without it");
    } else {
        let known = ASSISTANTS
            .iter()
            .map(|a| format!("{} ({})", a.dir, a.shorthand))
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "tip: --assistant <name> installs tangle, the AI builder persona, into the project; \
             the choice is remembered"
        );
        println!("     {known}");
        println!(
            "     using something else? --assistant {} installs the persona as a plain AGENTS.md",
            FALLBACK.shorthand
        );
    }
    println!("next: cd {name} && weft daemon start && weft run");
    Ok(())
}

/// The one file every Tangle template puts inside the assistant's own
/// directory. A project HAS Tangle for an assistant when this file is
/// there under one of that assistant's names, which is how
/// `weft tangle update` knows what to refresh: nothing but an install
/// writes it, so a `.github/` an assistant made for itself is not
/// mistaken for a persona.
const WITNESS: &str = "skills/weft-running/SKILL.md";

/// Names an assistant writes into its own directory that are not ours
/// to ship: dependency trees, lockfiles and scratch state that the
/// checkout picks up from being used as a project itself. The templates
/// track none of these.
const TEMPLATE_EXCLUDE: &[&str] = &[
    ".git",
    ".gitignore",
    "target",
    "node_modules",
    "package.json",
    "package-lock.json",
    "pnpm-lock.yaml",
    "bun.lock",
    "yarn.lock",
    "agent-manager.json",
    "worktrees",
];

/// True when `root` already holds Tangle for `spec`.
pub fn tangle_installed(root: &Path, spec: &AssistantSpec) -> bool {
    spec.links
        .iter()
        .any(|(name, _)| root.join(name).join(WITNESS).is_file())
}

/// Copy every selected Tangle persona out of the weft checkout into
/// `root`, replacing what the template owns and leaving alone anything
/// the assistant itself put there. Every template is checked before a
/// byte is written, so a bad later assistant cannot leave an earlier one
/// half installed.
///
/// Copies, not symlinks: an assistant reads a project's instructions as
/// untrusted input, and Kilo refuses outright to read an agent file that
/// resolves outside the project, so a link into the checkout loads
/// nothing and says nothing. A copy is also what makes Tangle survive a
/// clone, which is why the generated `.gitignore` no longer hides it.
/// The cost is that a project's Tangle is the version that installed it;
/// `weft tangle update` is how it catches up, the same way
/// `weft catalog update` re-seeds the stdlib.
pub fn install_tangle(root: &Path, assistants: &[&AssistantSpec]) -> anyhow::Result<()> {
    let repo = weft_catalog::weft_repo_root()
        .map_err(|e| anyhow::anyhow!("locating the weft checkout for tangle: {e}"))?;
    let mut pairs = Vec::new();

    for spec in assistants {
        let template = repo.join("tangle").join(spec.dir);
        if !template.is_dir() {
            anyhow::bail!(
                "the tangle template for {} is missing in this weft checkout (expected {}); \
                 git pull the checkout, or create the project without --assistant {}",
                spec.dir,
                template.display(),
                spec.dir
            );
        }

        for (link_name, rel) in spec.links {
            let source = template.join(rel);
            // metadata follows links: a dangling source in the template
            // must not become a half-written entry in the project.
            if let Err(error) = std::fs::metadata(&source) {
                if error.kind() == std::io::ErrorKind::NotFound {
                    anyhow::bail!(
                        "the tangle template for {} is incomplete in this weft checkout ({} is missing); \
                         git pull the checkout, or create the project without --assistant {}",
                        spec.dir,
                        source.display(),
                        spec.dir
                    );
                }
                return Err(error).with_context(|| format!("inspect {}", source.display()));
            }
            pairs.push((root.join(link_name), source));
        }
    }

    for (dest, source) in pairs {
        merge_template(&source, &dest)?;
    }
    Ok(())
}

/// Put the template at `source` into `dest`, entry by entry. A file
/// replaces whatever is at `dest`; a directory keeps everything the
/// template does not name (an assistant's own `worktrees/`, its
/// installed `node_modules/`) and replaces everything it does.
///
/// An earlier weft installed these as symlinks into the checkout, so a
/// `dest` that IS a link is removed rather than written through: that
/// would edit the checkout itself and leave the project still pointing
/// at it.
fn merge_template(source: &Path, dest: &Path) -> anyhow::Result<()> {
    let linked = std::fs::symlink_metadata(dest)
        .map(|meta| meta.file_type().is_symlink())
        .unwrap_or(false);
    if linked {
        std::fs::remove_file(dest)
            .with_context(|| format!("remove the old tangle link at {}", dest.display()))?;
    }
    if !source.is_dir() {
        return replace_with_template(source, dest);
    }
    std::fs::create_dir_all(dest).with_context(|| format!("create {}", dest.display()))?;
    for entry in std::fs::read_dir(source)
        .with_context(|| format!("read the tangle template at {}", source.display()))?
    {
        let entry = entry.with_context(|| format!("read the tangle template at {}", source.display()))?;
        let name = entry.file_name();
        if TEMPLATE_EXCLUDE.contains(&&*name.to_string_lossy()) {
            continue;
        }
        replace_with_template(&entry.path(), &dest.join(&name))?;
    }
    Ok(())
}

/// Throw away whatever is at `dest` and write the template's copy of it,
/// so a file the template dropped since the last install does not linger
/// in the project claiming to be part of Tangle.
fn replace_with_template(source: &Path, dest: &Path) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(dest) {
        Ok(meta) if meta.is_dir() => std::fs::remove_dir_all(dest)
            .with_context(|| format!("replace {}", dest.display()))?,
        Ok(_) => {
            std::fs::remove_file(dest).with_context(|| format!("replace {}", dest.display()))?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).with_context(|| format!("inspect {}", dest.display())),
    }
    if source.is_dir() {
        weft_compiler::build::copy_dir_filtered(source, dest, TEMPLATE_EXCLUDE)
            .map_err(|e| anyhow::anyhow!("copy {} into {}: {e}", source.display(), dest.display()))
    } else {
        std::fs::copy(source, dest)
            .with_context(|| format!("copy {} into {}", source.display(), dest.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{merge_template, resolve_assistants, tangle_installed, ASSISTANTS, FALLBACK, WITNESS};

    #[test]
    fn resolves_kilo_code_and_its_shorthand() {
        let assistants = resolve_assistants(&["kilo-code".into(), "kc".into()]).unwrap();
        assert_eq!(assistants.len(), 1);
        assert_eq!(assistants[0].dir, "kilo-code");
        assert_eq!(assistants[0].links, &[("kilo.json", "kilo.json"), (".kilo", ".kilo")]);
    }

    #[test]
    fn every_assistant_has_a_unique_name_shorthand_and_link_set() {
        // A duplicate shorthand would silently resolve to whichever row came
        // first, and two rows claiming the same link name would collide in
        // the project when a user installs both.
        let mut names = std::collections::BTreeSet::new();
        for spec in std::iter::once(&FALLBACK).chain(ASSISTANTS.iter()) {
            assert!(names.insert(spec.dir), "duplicate dir {}", spec.dir);
            assert!(names.insert(spec.shorthand), "duplicate shorthand {}", spec.shorthand);
            assert!(!spec.links.is_empty(), "{} installs nothing", spec.dir);
        }
    }

    #[test]
    fn the_fallback_refuses_to_install_beside_a_real_assistant() {
        // Cursor, Cline and Gemini read a root AGENTS.md ON TOP OF their own
        // persona file, so shipping both would load Tangle twice.
        let error = resolve_assistants(&["cursor".into(), "agents".into()]).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("cannot be installed alongside"), "{message}");
        assert!(message.contains("cursor"), "{message}");
    }

    #[test]
    fn the_fallback_installs_on_its_own() {
        let assistants = resolve_assistants(&["agents".into()]).unwrap();
        assert_eq!(assistants.len(), 1);
        assert_eq!(assistants[0].dir, "fallback");
    }

    #[test]
    fn an_unknown_assistant_names_the_fallback_in_its_error() {
        // The typo case has to point somewhere useful: a user on an
        // assistant weft has no row for still has a way in.
        let error = resolve_assistants(&["cluade-code".into()]).unwrap_err();
        assert!(error.to_string().contains("agents"), "{error}");
    }

    #[test]
    fn every_row_points_at_a_template_that_exists() {
        // A row naming a directory nobody wrote fails only at `weft new`,
        // in a user's terminal. Check the shipped tree instead: the crate
        // sits two levels under the checkout root.
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("checkout root")
            .to_path_buf();
        for spec in std::iter::once(&FALLBACK).chain(ASSISTANTS.iter()) {
            let template = repo.join("tangle").join(spec.dir);
            assert!(template.is_dir(), "missing template {}", template.display());
            for (_, rel) in spec.links {
                let target = template.join(rel);
                assert!(
                    target.exists(),
                    "{} promises {} but it is not in the template",
                    spec.dir,
                    target.display()
                );
            }
        }
    }

    #[test]
    fn every_template_carries_the_witness_under_one_of_its_own_names() {
        // `weft tangle update` finds what a project has by looking for
        // WITNESS under each of an assistant's names. A template that
        // stopped shipping it would go silently un-refreshable.
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("checkout root")
            .to_path_buf();
        for spec in std::iter::once(&FALLBACK).chain(ASSISTANTS.iter()) {
            let template = repo.join("tangle").join(spec.dir);
            let found = spec
                .links
                .iter()
                .any(|(_, rel)| template.join(rel).join(WITNESS).is_file());
            assert!(found, "{} ships no {WITNESS} under any of its names", spec.dir);
        }
    }

    #[test]
    fn a_refresh_replaces_what_tangle_owns_and_keeps_what_the_assistant_made() {
        let temp = tempfile::tempdir().unwrap();
        let template = temp.path().join("template");
        std::fs::create_dir_all(template.join("agent")).unwrap();
        std::fs::write(template.join("agent/tangle.md"), "new").unwrap();
        std::fs::write(template.join("node_modules"), "never copied").unwrap();

        // A project whose last install left a stale agent behind, and
        // whose assistant keeps its own scratch state in the same tree.
        let dest = temp.path().join("project/.kilo");
        std::fs::create_dir_all(dest.join("agent")).unwrap();
        std::fs::create_dir_all(dest.join("worktrees")).unwrap();
        std::fs::write(dest.join("agent/tangle.md"), "old").unwrap();
        std::fs::write(dest.join("agent/dropped.md"), "gone after this").unwrap();
        std::fs::write(dest.join("worktrees/mine"), "the assistant's").unwrap();

        merge_template(&template, &dest).unwrap();

        assert_eq!(std::fs::read_to_string(dest.join("agent/tangle.md")).unwrap(), "new");
        assert!(!dest.join("agent/dropped.md").exists(), "a dropped template file goes");
        assert!(!dest.join("node_modules").exists(), "an assistant's own tree never travels");
        assert_eq!(
            std::fs::read_to_string(dest.join("worktrees/mine")).unwrap(),
            "the assistant's",
            "what the assistant put there survives a refresh"
        );
    }

    #[test]
    fn a_refresh_turns_an_older_install_s_symlink_into_real_files() {
        // Before copies, these were symlinks into the weft checkout.
        // Writing through one would edit the checkout and leave the
        // project still pointing at it.
        let temp = tempfile::tempdir().unwrap();
        let template = temp.path().join("template");
        std::fs::create_dir_all(template.join("agent")).unwrap();
        std::fs::write(template.join("agent/tangle.md"), "new").unwrap();

        let dest = temp.path().join(".kilo");
        std::os::unix::fs::symlink(&template, &dest).unwrap();
        merge_template(&template, &dest).unwrap();

        assert!(!std::fs::symlink_metadata(&dest).unwrap().file_type().is_symlink());
        assert_eq!(std::fs::read_to_string(dest.join("agent/tangle.md")).unwrap(), "new");
    }

    #[test]
    fn a_project_is_only_seen_as_installed_when_the_witness_is_there() {
        // An assistant that made its own `.kilo/` (Kilo does) must not
        // read as a project that has Tangle.
        let temp = tempfile::tempdir().unwrap();
        let kilo = &ASSISTANTS[1];
        std::fs::create_dir_all(temp.path().join(".kilo/worktrees")).unwrap();
        assert!(!tangle_installed(temp.path(), kilo));

        std::fs::create_dir_all(temp.path().join(".kilo").join(WITNESS).parent().unwrap()).unwrap();
        std::fs::write(temp.path().join(".kilo").join(WITNESS), "skill").unwrap();
        assert!(tangle_installed(temp.path(), kilo));
    }
}
