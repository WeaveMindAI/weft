//! Every fixture's catalog parses, checked without a cluster.
//!
//! A fixture's `metadata.json` is only read once the rig has built and
//! deployed, so a typo in it (a widget kind that does not exist, a field
//! spelled the way the author assumed) surfaces as a failing end-to-end
//! test minutes in, with the real subject of the test never reached.
//! That is expensive and it reads like a bug in the feature rather than
//! a bug in the fixture.
//!
//! This runs in an ordinary `cargo test`: no cluster, no docker, no
//! feature flag. It is deliberately NOT behind `e2e`, because its whole
//! value is catching the mistake before the slow suite runs.

use std::path::{Path, PathBuf};

/// Every directory under `fixtures/` that declares a project.
fn fixtures() -> anyhow::Result<Vec<PathBuf>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&root)? {
        let path = entry?.path();
        if path.join("weft.toml").is_file() {
            out.push(path);
        }
    }
    out.sort();
    anyhow::ensure!(!out.is_empty(), "no fixtures under {}", root.display());
    Ok(out)
}

/// Every fixture's node metadata parses into the catalog the compiler
/// builds from it.
#[test]
fn every_fixture_catalog_parses() -> anyhow::Result<()> {
    let mut broken: Vec<String> = Vec::new();
    for fixture in fixtures()? {
        if let Err(e) = weft_compiler::build::build_project_catalog(&fixture) {
            broken.push(format!(
                "{}: {e}",
                fixture.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default()
            ));
        }
    }
    anyhow::ensure!(broken.is_empty(), "fixture catalogs that do not parse:\n{}", broken.join("\n"));
    Ok(())
}

/// No two fixtures claim the same project id.
///
/// The suites run one after another, so a shared id is not a collision
/// today; it is still one project row two fixtures write to, which makes
/// a leftover from the previous suite look like this one's state.
#[test]
fn every_fixture_has_its_own_project_id() -> anyhow::Result<()> {
    let mut seen: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for fixture in fixtures()? {
        let toml = std::fs::read_to_string(fixture.join("weft.toml"))?;
        let name = fixture.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
        if let Some(id) = toml
            .lines()
            .find_map(|l| l.trim().strip_prefix("id = "))
            .map(|v| v.trim().trim_matches('"').to_string())
        {
            seen.entry(id).or_default().push(name);
        }
    }
    let shared: Vec<String> = seen
        .iter()
        .filter(|(_, who)| who.len() > 1)
        .map(|(id, who)| format!("{id}: {}", who.join(", ")))
        .collect();
    anyhow::ensure!(shared.is_empty(), "fixtures sharing a project id:\n{}", shared.join("\n"));
    Ok(())
}
