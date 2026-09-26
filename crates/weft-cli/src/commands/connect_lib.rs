//! `weft connect-lib`: copy weft's connect library into the project's
//! frontend, so a site's pages can show the same connection pickers the
//! weft editor shows (and a member's own settings page).
//!
//! The library is plain TypeScript plus Svelte components with relative
//! imports and no dependency of its own beyond Svelte, so it is copied as
//! source, like `weft tangle update` copies Tangle. Not every project has
//! a frontend, which is why `weft new` does not do this.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::Ctx;

/// Where the library lands unless `--into` says otherwise: the default
/// SvelteKit frontend's `$lib`.
const DEFAULT_INTO: &str = "front/src/lib/weft-connect";

pub async fn install(ctx: Ctx, into: Option<PathBuf>) -> Result<()> {
    let project = ctx.project()?;
    let into = into.unwrap_or_else(|| PathBuf::from(DEFAULT_INTO));
    // The folder is replaced whole, so it has to be one inside the
    // project: a path climbing out of it could name anything.
    anyhow::ensure!(
        into.components().all(|c| matches!(c, std::path::Component::Normal(_))) && into.components().count() > 0,
        "--into takes a folder inside the project, written relative to its root (`front/src/lib/weft-connect`); got `{}`",
        into.display()
    );
    let dest = project.root.join(into);
    let repo = weft_catalog::weft_repo_root()
        .map_err(|e| anyhow::anyhow!("locating the weft checkout for the connect library: {e}"))?;
    let source = repo.join("packages").join("weft-connect");
    let files = library_files(&source)?;
    // The folder is the library's alone: replacing it whole is what keeps
    // a file the library dropped from lingering in the site.
    if dest.exists() {
        std::fs::remove_dir_all(&dest).with_context(|| format!("clear {}", dest.display()))?;
    }
    for rel in &files {
        // `src/index.ts` lands as `index.ts`, so the folder imports as
        // `$lib/weft-connect` the way the package imports as `@weft/connect`.
        let to = dest.join(rel.strip_prefix("src").unwrap_or(rel));
        if let Some(parent) = to.parent() {
            std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        std::fs::copy(source.join(rel), &to).with_context(|| format!("copy {} to {}", rel.display(), to.display()))?;
    }
    println!(
        "copied the connect library ({} files) into {}\n  import {{ MemberDoor }} from '$lib/weft-connect';\n  import {{ MemberSettings }} from '$lib/weft-connect/svelte';\n  import {{ weftPassThrough }} from '$lib/weft-connect/server';\nmount the pass-through at src/routes/weft/[...path]/+server.ts (its README shows the route) and set WEFT_DISPATCHER_URL in the server env: pages call their own site at /weft/..., never the dispatcher\nrun `weft connect-lib` again after updating weft; edits made inside that folder are replaced",
        files.len(),
        dest.display()
    );
    Ok(())
}

/// The files the library ships, relative to the package root: its README
/// and every source file under `src/` but the tests.
fn library_files(package: &Path) -> Result<Vec<PathBuf>> {
    let src = package.join("src");
    anyhow::ensure!(
        src.is_dir(),
        "the connect library is missing in this weft checkout (expected {}); git pull the checkout",
        src.display()
    );
    let mut out = vec![PathBuf::from("README.md")];
    let mut pending = vec![src];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else if !path.to_string_lossy().ends_with(".test.ts") {
                out.push(path.strip_prefix(package).expect("walked under the package").to_path_buf());
            }
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_library_ships_its_sources_and_readme_but_not_its_tests() {
        let package = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packages/weft-connect");
        let files = library_files(&package).unwrap();
        let names: Vec<String> = files.iter().map(|p| p.to_string_lossy().replace('\\', "/")).collect();
        assert!(names.contains(&"README.md".to_string()));
        assert!(names.contains(&"src/index.ts".to_string()));
        assert!(names.contains(&"src/svelte/MemberSettings.svelte".to_string()));
        assert!(names.iter().all(|n| !n.ends_with(".test.ts")), "{names:?}");
    }
}
