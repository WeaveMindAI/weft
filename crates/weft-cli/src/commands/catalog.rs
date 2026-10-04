//! `weft catalog update`: re-sync the project's base node catalog
//! (`nodes/base_catalog/`) from the installed weft's bundled catalog. And
//! the install's preload of that same catalog into its tenant's assets.

use anyhow::{Context, Result};

use super::Ctx;

pub async fn update(ctx: Ctx) -> Result<()> {
    let project = ctx.project()?;
    weft_compiler::project::seed_base_catalog(&project.root)
        .map_err(|e| anyhow::anyhow!("update base catalog: {e}"))?;
    let dest = weft_compiler::project::base_catalog_dir(&project.root);
    println!("re-synced base catalog at {} from the installed weft", dest.display());
    Ok(())
}

/// `weft catalog preload`: [`preload_standard_library`] on the install
/// this command names, with its stored key or one piped in.
pub async fn preload(ctx: Ctx, key_stdin: bool) -> Result<()> {
    let (url, stored) = ctx.install_access()?;
    let key = if key_stdin {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).context("read the key from stdin")?;
        let key = line.trim().to_string();
        anyhow::ensure!(!key.is_empty(), "no key on stdin; nothing was preloaded");
        Some(key)
    } else {
        stored.map(str::to_string)
    };
    let client = crate::client::DispatcherClient::new(url.to_string(), key);
    preload_standard_library(&client).await.with_context(|| format!("store the standard library in {url}"))?;
    println!("{url} holds this weft's standard library");
    Ok(())
}

/// Store the installed standard library in the tenant's assets, so the
/// first version of the first project finds every `nodes/base_catalog/`
/// file already stored and uploads none of it. The files are exactly the
/// ones `weft new` seeds, published the way a version snapshot publishes
/// (hashed, verified, and only what the tenant lacks is sent), so a re-run
/// stores nothing new. Until a project references them they sit on the
/// store's ordinary countdown, like any upload nothing references yet.
pub async fn preload_standard_library(client: &crate::client::DispatcherClient) -> Result<()> {
    let seeded = tempfile::tempdir().context("make a folder to lay the standard library out in")?;
    weft_compiler::project::seed_base_catalog(seeded.path())
        .map_err(|e| anyhow::anyhow!("lay out the installed standard library: {e}"))?;
    let paths = super::versions::covered_paths(seeded.path())?;
    let source = super::assets::DiskSource::new(seeded.path().to_path_buf());
    let store = super::assets::DispatcherStore::new(client);
    weft_assets::publish_files(&paths, &source, &store).await?;
    Ok(())
}
