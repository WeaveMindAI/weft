//! Every fixture's catalog parses AND its graph compiles, checked
//! without a cluster.
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

/// Every fixture's GRAPH compiles: it parses against its own catalog
/// plus the stdlib, enriches, and validate finds nothing STRUCTURALLY
/// wrong with it.
///
/// The catalog check above catches a bad `metadata.json`; this catches
/// a bad `.weft`, which is the other half and the one an author writing
/// a fixture gets wrong more often. A mistake here used to surface
/// minutes into a cluster run as a failure of whatever feature the
/// fixture was written to prove, which reads like a bug in the feature
/// rather than a typo in its fixture.
///
/// STRUCTURAL, not runtime: a fixture is deliberately incomplete until
/// the rig fills it in (`substitute_in_main` writes the live path, a
/// chat id, a bucket), so "this required input has no driver" is the
/// fixture working as intended, not a mistake. The structural tier is
/// exactly the one that does not depend on the rig having run, which is
/// what makes this test honest without a cluster.
/// The fixtures that are INCOMPLETE on disk on purpose: their test
/// writes the missing value in before running (a chat id read from the
/// environment, a file the rig uploads first), so the graph only stands
/// up once the rig has been through it.
///
/// Named one by one rather than matched by the diagnostic, so adding a
/// fixture that genuinely does not compile fails here instead of
/// quietly joining a category.
const FILLED_IN_BY_THE_RIG: [&str; 7] = [
    "access_s3",
    "access_telegram",
    "audio_transcribe",
    "config_media",
    "email_send",
    "fetch_once",
    // `MiniService` is copied in from `infra_min` by the test
    // (`add_node_from_fixture`), so the committed source names a node
    // this parse cannot see.
    "include_twice_infra",
];

#[test]
fn every_fixture_graph_compiles() -> anyhow::Result<()> {
    let stdlib = weft_catalog::stdlib_root().map_err(|e| anyhow::anyhow!("stdlib root: {e}"))?;
    // One fixture's errors, or `None` when it has no program or its
    // catalog does not load. Every fixture compiles on its own thread:
    // they share nothing, and one after another this is among the
    // slowest tests in the workspace.
    let errors_of = |fixture: &std::path::Path| -> anyhow::Result<Option<Vec<String>>> {
        let main = fixture.join("src").join("main.weft");
        if !main.is_file() {
            return Ok(None);
        }
        let source = std::fs::read_to_string(&main)?;
        // The project's own node roots (`nodes/` AND `src/`, since a node
        // may sit beside the file that includes it) plus the stdlib. The
        // rig copies the stdlib into each project at deploy time; here
        // every root is read where it already is, so this stays a plain
        // cargo test with no copying and no cluster.
        let mut roots = weft_compiler::project::node_roots(fixture).to_vec();
        roots.push(stdlib.clone());
        let catalog = match weft_catalog::FsCatalog::discover_roots_with_policy(
            &roots.iter().map(|r| r.as_path()).collect::<Vec<_>>(),
            weft_catalog::DiscoverPolicy::Strict,
        ) {
            Ok(c) => c,
            // The catalog's own failure is the other test's finding; not
            // repeating it here keeps one mistake to one report.
            Err(_) => return Ok(None),
        };
        // The two anchors the CLI gives a program, and they differ:
        // `@file("assets/...")` resolves from the PROJECT ROOT, while
        // `@include("lib/x.weft")` resolves from the including file's
        // OWN directory, which for the program is `src/`.
        let src = fixture.join("src");
        let (_, diagnostics) = weft_compiler::compile_strict(
            &source,
            uuid::Uuid::new_v4(),
            weft_compiler::CompileFs::disk(fixture).anchored_at(Some(&src)),
            &catalog,
            weft_compiler::validate::ValidationMode::Structural,
            None,
        );
        Ok(Some(
            diagnostics
                .into_iter()
                .filter(|d| d.severity == weft_compiler::Severity::Error)
                .map(|d| format!("{}: {}", d.code.clone().unwrap_or_default(), d.message))
                .collect(),
        ))
    };
    let fixtures = fixtures()?;
    let results: Vec<(String, anyhow::Result<Option<Vec<String>>>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = fixtures
            .iter()
            .map(|fixture| {
                let name = fixture
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
                (name, scope.spawn(|| errors_of(fixture)))
            })
            .collect();
        handles
            .into_iter()
            .map(|(name, handle)| (name, handle.join().expect("a fixture's compile panicked")))
            .collect()
    });
    let mut never_needed: Vec<&str> = Vec::new();
    let mut broken: Vec<String> = Vec::new();
    for (name, result) in results {
        let Some(errors) = result? else { continue };
        let excused = FILLED_IN_BY_THE_RIG.contains(&name.as_str());
        if !errors.is_empty() && !excused {
            broken.push(format!("{name}:\n    {}", errors.join("\n    ")));
        }
        if errors.is_empty() && excused {
            never_needed.push(FILLED_IN_BY_THE_RIG
                .iter()
                .find(|f| **f == name)
                .expect("just matched"));
        }
    }
    anyhow::ensure!(
        broken.is_empty(),
        "fixture graphs that do not compile:\n{}",
        broken.join("\n")
    );
    // An excuse nobody needs any more is a lie about the fixture, and
    // it hides the next real break behind it.
    anyhow::ensure!(
        never_needed.is_empty(),
        "these fixtures compile on their own now; drop them from \
         FILLED_IN_BY_THE_RIG: {never_needed:?}"
    );
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
