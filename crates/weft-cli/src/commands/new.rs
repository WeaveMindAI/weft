//! `weft new <name>`: scaffold a new project directory with
//! weft.toml, main.weft, nodes/, .weft/, and an initialized git
//! repo.

use std::path::PathBuf;
use std::process::Command;

use anyhow::Context;

use super::Ctx;

pub async fn run(_ctx: Ctx, name: String) -> anyhow::Result<()> {
    if name.is_empty() {
        anyhow::bail!("project name cannot be empty");
    }
    let root = PathBuf::from(&name);
    if root.exists() {
        anyhow::bail!("{} already exists", root.display());
    }

    let project = weft_compiler::project::Project::init(&root, &name)
        .map_err(|e| anyhow::anyhow!("init: {e}"))?;

    // Initialize git. Best-effort: skip quietly if git is missing.
    let git_init = Command::new("git").arg("init").current_dir(&root).status();
    match git_init {
        Ok(status) if status.success() => {
            let gitignore = "target/\n.weft/\nnode_modules/\n";
            std::fs::write(root.join(".gitignore"), gitignore).context("write .gitignore")?;
        }
        Ok(_) | Err(_) => {
            // git missing or `git init` failed. Not fatal: user can
            // opt into git later.
        }
    }

    println!(
        "created project {} (id {}) at {}",
        project.manifest.package.name,
        project.id(),
        root.display()
    );
    println!("next: cd {name} && weft start && weft run");
    Ok(())
}
