//! `weft new <name>`: scaffold a new project directory with
//! weft.toml, main.weft, nodes/, .weft/, and an initialized git
//! repo. With `--assistant <name>`, also install the Tangle
//! assistant persona for that AI coding assistant, via symlinks
//! into the weft checkout (see `AssistantSpec`). The choice is
//! remembered, so later `weft new` calls install the same
//! assistants without repeating the flag; `--assistant none`
//! clears it.

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
struct AssistantSpec {
    /// Directory under `tangle/` in the weft checkout.
    dir: &'static str,
    /// Shorthand accepted as the `--assistant` value (`cc` for
    /// `claude-code`).
    shorthand: &'static str,
    /// `(name in the project, path inside the template dir)` pairs, each
    /// installed as a symlink.
    links: &'static [(&'static str, &'static str)],
}

const ASSISTANTS: &[AssistantSpec] = &[AssistantSpec {
    dir: "claude-code",
    shorthand: "cc",
    links: &[("CLAUDE.md", "CLAUDE.md"), (".claude", ".claude")],
}];

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
        let found = ASSISTANTS
            .iter()
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
                anyhow::bail!("unknown assistant '{}'; known assistants: {}", value, known);
            }
        }
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
        match ASSISTANTS
            .iter()
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

    for spec in &installed {
        install_tangle(&root, spec)?;
    }

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
            .map(|a| format!("{} (shorthand {})", a.dir, a.shorthand))
            .collect::<Vec<_>>()
            .join(", ");
        println!("tip: --assistant <name> installs tangle, the AI builder persona, into the project ({known}); the choice is remembered");
    }
    println!("next: cd {name} && weft daemon start && weft run");
    Ok(())
}

/// Symlink one assistant's Tangle persona from the weft checkout into a
/// fresh project, one link per entry in the spec.
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
fn install_tangle(root: &Path, spec: &AssistantSpec) -> anyhow::Result<()> {
    let repo = weft_catalog::weft_repo_root()
        .map_err(|e| anyhow::anyhow!("locating the weft checkout for tangle: {e}"))?;
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
        if !target.exists() {
            anyhow::bail!(
                "the tangle template for {} is incomplete in this weft checkout ({} is missing); \
                 git pull the checkout, or create the project without --assistant {}",
                spec.dir,
                target.display(),
                spec.dir
            );
        }
        let link = root.join(link_name);
        // symlink_metadata, not exists(): a dangling link must also count
        // as taken, or we would layer a second link over it.
        if std::fs::symlink_metadata(&link).is_ok() {
            anyhow::bail!(
                "{} already exists; tangle ({}) not installed over it",
                link.display(),
                spec.dir
            );
        }
        std::os::unix::fs::symlink(&target, &link).with_context(|| {
            format!("symlink {} -> {}", link.display(), target.display())
        })?;
    }
    Ok(())
}
