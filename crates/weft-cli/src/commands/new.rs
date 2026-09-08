//! `weft new <name>`: scaffold a new project directory with
//! weft.toml, main.weft, nodes/, .weft/, and an initialized git
//! repo. With `--assistant <name>`, also install the Tangle
//! assistant persona for that AI coding assistant, via symlinks
//! into the weft checkout (see `AssistantSpec`). The choice is
//! remembered, so later `weft new` calls install the same
//! assistants without repeating the flag; `--assistant none`
//! clears it, and `--assistant agents` is the fallback for an
//! assistant weft has no template for.

use std::collections::BTreeSet;
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
struct AssistantSpec {
    /// Directory under `tangle/` in the weft checkout.
    dir: &'static str,
    /// Shorthand accepted as the `--assistant` value (`cc` for
    /// `claude-code`, `kc` for `kilo-code`).
    shorthand: &'static str,
    /// `(name in the project, path inside the template dir)` pairs, each
    /// installed as a symlink.
    links: &'static [(&'static str, &'static str)],
}

/// Every assistant weft ships a Tangle template for. Each row's `links` are
/// the paths that assistant actually reads, which is why no two rows look
/// alike: the persona is a rules file in one and a plain `AGENTS.md` in the
/// next, and the specialists are TOML here and markdown there. The template
/// directories are independent copies on purpose (see `tangle/README.md`):
/// a wording that works better on one assistant belongs only in that
/// assistant's copy.
const ASSISTANTS: &[AssistantSpec] = &[
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
const FALLBACK: AssistantSpec = AssistantSpec {
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
fn resolve_assistants(values: &[String]) -> anyhow::Result<Vec<&'static AssistantSpec>> {
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

    // The Tangle symlinks point into this machine's weft checkout
    // (absolute paths), so they are machine-local: a teammate cloning the
    // project would get dangling links. Keep them out of the repo, one
    // entry per link name across every installed assistant.
    let mut gitignore = String::from("target/\n.weft/\nnode_modules/\n");
    if !installed.is_empty() {
        gitignore.push_str("# tangle: symlinked from the local weft checkout\n");
        let mut link_names: BTreeSet<&str> = BTreeSet::new();
        for spec in &installed {
            for (link, _) in spec.links {
                link_names.insert(link);
            }
        }
        for link in link_names {
            gitignore.push_str(link);
            gitignore.push('\n');
        }
    }

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
                "tangle ({}) installed via symlinks: open the project in that assistant and it is there",
                shown.join(", ")
            );
        } else {
            println!(
                "tangle ({}) installed via symlinks (remembered from your last choice; \
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

/// Symlink every selected Tangle persona from the weft checkout into a fresh
/// project. All template sources and destinations are checked before any link
/// is written, so a bad later assistant cannot leave an earlier one half
/// installed.
///
/// Symlinks, deliberately: the point of the flag is that a `git pull` of the
/// checkout refreshes Tangle in every project that asked for it, with no
/// per-project copy to drift stale. The targets are absolute, because the
/// project can live anywhere relative to the checkout and
/// `weft_repo_root()` already handles a moved checkout (recorded install
/// root, env override, cwd walk-up). The tradeoff is stated in the
/// project's `.gitignore`: the links are machine-local and stay out of the
/// repo.
///
/// Unlike `seed_base_catalog` (which COPIES the stdlib so a project is
/// self-contained), this is an intentional reach back into the
/// installation, the one place a running project depends on the checkout
/// staying put.
fn install_tangle(root: &Path, assistants: &[&AssistantSpec]) -> anyhow::Result<()> {
    let repo = weft_catalog::weft_repo_root()
        .map_err(|e| anyhow::anyhow!("locating the weft checkout for tangle: {e}"))?;
    let mut links = Vec::new();

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
            let target = template.join(rel);
            // metadata follows links: a dangling source in the template must
            // not become a dangling link in the new project.
            if let Err(error) = std::fs::metadata(&target) {
                if error.kind() == std::io::ErrorKind::NotFound {
                    anyhow::bail!(
                        "the tangle template for {} is incomplete in this weft checkout ({} is missing); \
                         git pull the checkout, or create the project without --assistant {}",
                        spec.dir,
                        target.display(),
                        spec.dir
                    );
                }
                return Err(error).with_context(|| format!("inspect {}", target.display()));
            }

            let link = root.join(link_name);
            ensure_link_available(&link, spec)?;
            links.push((link, target));
        }
    }

    for (link, target) in links {
        std::os::unix::fs::symlink(&target, &link).with_context(|| {
            format!("symlink {} -> {}", link.display(), target.display())
        })?;
    }
    Ok(())
}

fn ensure_link_available(link: &Path, spec: &AssistantSpec) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(link) {
        Ok(_) => anyhow::bail!(
            "{} already exists; tangle ({}) not installed over it",
            link.display(),
            spec.dir
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("inspect {}", link.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::{ensure_link_available, resolve_assistants, ASSISTANTS, FALLBACK};

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
    fn refuses_to_replace_a_dangling_tangle_link() {
        let temp = tempfile::tempdir().unwrap();
        let link = temp.path().join("kilo.json");
        std::os::unix::fs::symlink(temp.path().join("missing"), &link).unwrap();

        let error = ensure_link_available(&link, &ASSISTANTS[1]).unwrap_err();
        assert!(error.to_string().contains("not installed over it"));
    }
}
