//! `weft describe-nodes`: print the project's full node catalog as
//! JSON (every node under `nodes/`), for the editor's node palette.
//! Runs locally because the catalog lives in the project's `nodes/`.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::Serialize;

use weft_catalog::{stdlib_root, DiscoverPolicy, FsCatalog};

use super::node_catalog::NodeCatalogEntry;
use super::Ctx;

#[derive(Serialize)]
struct NodesResponse {
    catalog: BTreeMap<String, NodeCatalogEntry>,
    /// Soft errors from scanning `nodes/` (malformed metadata.json,
    /// duplicate types). Surfaced, not silent: a node mid-rename has a
    /// transient parse error the editor should see but not crash on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
}

pub async fn run(ctx: Ctx, stdlib: bool) -> Result<()> {
    // `--stdlib`: describe the bundled stdlib catalog directly (no project on
    // disk). Otherwise describe the project's own `nodes/`. Lenient discovery
    // either way: the editor's palette must survive a node mid-edit. Same
    // traversal as the build, only the error reaction differs (warn vs abort),
    // so the palette and the build never disagree about what a node is.
    let nodes_dir = if stdlib {
        stdlib_root()
    } else {
        ctx.project()?.root.join("nodes")
    };
    let cat = FsCatalog::discover_with_policy(&nodes_dir, DiscoverPolicy::Lenient)
        .map_err(|e| anyhow::anyhow!("describe: {e}"))?;

    let mut catalog = BTreeMap::new();
    for entry in cat.iter() {
        if entry.metadata.features.hidden {
            continue;
        }
        catalog.insert(
            entry.node_type.clone(),
            NodeCatalogEntry {
                metadata: entry.metadata.clone(),
                form_field_specs: entry.form_field_specs.clone(),
            },
        );
    }
    let resp = NodesResponse {
        catalog,
        warnings: cat.warnings().to_vec(),
    };
    println!("{}", serde_json::to_string(&resp).context("serialize describe response")?);
    Ok(())
}
