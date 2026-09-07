//! `weft describe-nodes`: print the project's node catalog as JSON
//! (every node under `nodes/`), for the editor's node palette, and
//! (`--compact`) as the token-cheap wiring view an AI reads instead of
//! whole `metadata.json` files. Runs locally because the catalog lives
//! in the project's `nodes/`.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::Serialize;

use weft_catalog::{stdlib_root, DiscoverPolicy, FsCatalog};

use super::Ctx;

#[derive(Serialize)]
struct NodesResponse<T> {
    // The metadata itself is the wire entry: everything the palette needs
    // (form-field specs included) is a metadata key. Under `--compact`
    // the values are the filtered wiring view instead, same envelope.
    catalog: BTreeMap<String, T>,
    /// Soft errors from scanning `nodes/` (malformed metadata.json,
    /// duplicate types). Surfaced, not silent: a node mid-rename has a
    /// transient parse error the editor should see but not crash on.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
}

/// One row of `--list`: what a reader scanning for a node needs to
/// pick one, and nothing else.
#[derive(Serialize)]
struct ListRow<'a> {
    #[serde(rename = "type")]
    node_type: &'a str,
    tags: &'a [String],
    description: &'a str,
}

pub async fn run(
    ctx: Ctx,
    stdlib: bool,
    node: Option<String>,
    compact: bool,
    list: bool,
) -> Result<()> {
    // `--stdlib`: describe the bundled stdlib catalog directly (no project on
    // disk). Otherwise describe the project's own `nodes/`. Lenient discovery
    // either way: the editor's palette must survive a node mid-edit. Same
    // traversal as the build, only the error reaction differs (warn vs abort),
    // so the palette and the build never disagree about what a node is.
    let nodes_dir = if stdlib {
        stdlib_root().map_err(|e| anyhow::anyhow!(e))?
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
        // Ship RESOLVED metadata: every input's accepts + widget filled
        // with its effective value, so the editor never re-derives either.
        catalog.insert(entry.node_type.clone(), entry.metadata.resolved());
    }

    // The listing: one line per type, sorted (the BTreeMap's order), so a
    // reader (a person, an agent) scans a catalog of a hundred nodes in
    // a hundred short lines and reaches for `--node` on the one they
    // want. Tags come before the description because they are what
    // one greps for.
    if list {
        let rows: Vec<ListRow<'_>> = catalog
            .iter()
            .map(|(node_type, metadata)| ListRow {
                node_type,
                tags: &metadata.tags,
                description: &metadata.description,
            })
            .collect();
        // The scan warnings go out either way: a node mid-rename that
        // dropped out of the listing has to say so, JSON or not.
        for warning in cat.warnings() {
            eprintln!("warning: {warning}");
        }
        if ctx.json() {
            println!("{}", serde_json::to_string(&rows).context("serialize node listing")?);
            return Ok(());
        }
        let width = rows.iter().map(|r| r.node_type.len()).max().unwrap_or(0);
        for row in &rows {
            let tags = if row.tags.is_empty() {
                String::new()
            } else {
                format!("[{}] ", row.tags.join(","))
            };
            println!("{:<width$}  {tags}{}", row.node_type, row.description);
        }
        return Ok(());
    }

    // One node: the full resolved metadata (pretty, a person reads it), or
    // its compact wiring view. A wrong type is a loud error naming the fix,
    // never an empty answer. The fix names the listing command in the mode
    // the user is in: hinting the bare command under `--stdlib` would send
    // them to the project's catalog (or to an error, outside a project).
    if let Some(wanted) = &node {
        let Some(metadata) = catalog.get(wanted) else {
            let listing = if stdlib {
                "weft describe-nodes --stdlib"
            } else {
                "weft describe-nodes"
            };
            anyhow::bail!(
                "unknown node type `{wanted}`; run `{listing}` to list the catalog's types"
            );
        };
        let printed = if compact {
            serde_json::to_string(&metadata.compact_json())
        } else {
            serde_json::to_string_pretty(metadata)
        };
        println!("{}", printed.context("serialize describe response")?);
        return Ok(());
    }

    let warnings = cat.warnings().to_vec();
    let printed = if compact {
        let catalog: BTreeMap<String, serde_json::Value> = catalog
            .iter()
            .map(|(node_type, metadata)| (node_type.clone(), metadata.compact_json()))
            .collect();
        serde_json::to_string(&NodesResponse { catalog, warnings })
    } else {
        serde_json::to_string(&NodesResponse { catalog, warnings })
    };
    println!("{}", printed.context("serialize describe response")?);
    Ok(())
}
