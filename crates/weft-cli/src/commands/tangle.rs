//! `weft tangle update`: re-copy the project's Tangle personas, skills
//! and commands from the installed weft, the way `weft catalog update`
//! re-copies the base node catalog.
//!
//! Tangle lives in the project as ordinary files, so a project holds the
//! version that installed it. This is how it catches up: every file the
//! template owns is replaced, everything the assistant put there itself
//! is left alone, and the diff is the user's to read.

use anyhow::Result;

use super::new::{install_tangle, resolve_assistants, tangle_installed, AssistantSpec, ASSISTANTS, FALLBACK};
use super::Ctx;

pub async fn update(ctx: Ctx, assistants: Vec<String>) -> Result<()> {
    let project = ctx.project()?;
    let root = project.root.clone();

    // With no flag, refresh what the project already has. With one, that
    // is also how Tangle is added to a project created without it.
    let chosen: Vec<&'static AssistantSpec> = if assistants.is_empty() {
        std::iter::once(&FALLBACK)
            .chain(ASSISTANTS.iter())
            .filter(|spec| tangle_installed(&root, spec))
            .collect()
    } else {
        resolve_assistants(&assistants)?
    };

    if chosen.is_empty() {
        let known = ASSISTANTS
            .iter()
            .map(|a| format!("{} ({})", a.dir, a.shorthand))
            .collect::<Vec<_>>()
            .join(", ");
        anyhow::bail!(
            "{} has no tangle to update; --assistant <name> installs it here\n     {}\n     \
             using something else? --assistant {} installs the persona as a plain AGENTS.md",
            root.display(),
            known,
            FALLBACK.shorthand
        );
    }

    install_tangle(&root, &chosen)?;
    let named: Vec<&str> = chosen.iter().map(|spec| spec.dir).collect();
    println!(
        "re-synced tangle ({}) at {} from the installed weft",
        named.join(", "),
        root.display()
    );
    Ok(())
}
