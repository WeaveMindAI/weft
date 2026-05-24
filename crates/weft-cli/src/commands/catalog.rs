//! `weft catalog update`: re-sync the project's base node catalog
//! (`nodes/base_catalog/`) from the installed weft's bundled catalog.

use anyhow::Result;

use super::Ctx;

pub async fn update(ctx: Ctx) -> Result<()> {
    let project = ctx.project()?;
    weft_compiler::project::seed_base_catalog(&project.root)
        .map_err(|e| anyhow::anyhow!("update base catalog: {e}"))?;
    let dest = weft_compiler::project::base_catalog_dir(&project.root);
    println!("re-synced base catalog at {} from the installed weft", dest.display());
    Ok(())
}
