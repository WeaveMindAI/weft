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
/// pick one, and nothing else. The description is its first sentence:
/// a hundred nodes at a sentence each is a screen, at a paragraph each
/// it is a file nobody reads (a whole-catalog listing weighed 52 KB).
#[derive(Serialize)]
struct ListRow<'a> {
    #[serde(rename = "type")]
    node_type: &'a str,
    tags: &'a [String],
    description: &'a str,
}

/// The first sentence of a description: up to and including the first
/// full stop that ends one. A stop ends a sentence when it is followed
/// by whitespace or is the last character, sits outside any open
/// parenthesis (a `(e.g. ...)` aside is inside the sentence), and does
/// not end an abbreviation (`e.g.`, `i.e.`, `etc.`, `vs.`). A dot inside
/// `weft.dev` or `ctx.inputs` is followed by a letter, so it never
/// qualifies. A description with no such stop is one sentence.
fn first_sentence(description: &str) -> &str {
    const ABBREVIATIONS: [&str; 4] = ["e.g", "i.e", "etc", "vs"];
    let bytes = description.as_bytes();
    let mut depth: usize = 0;
    for (i, b) in bytes.iter().enumerate() {
        match b {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            b'.' if depth == 0 && bytes.get(i + 1).is_none_or(|next| next.is_ascii_whitespace()) => {
                let word_start = description[..i]
                    .rfind(|c: char| c.is_whitespace() || c == '(')
                    .map_or(0, |at| at + 1);
                let word = &description[word_start..i];
                if ABBREVIATIONS.iter().any(|abbr| word.eq_ignore_ascii_case(abbr)) {
                    continue;
                }
                return description[..=i].trim_end();
            }
            _ => {}
        }
    }
    description.trim_end()
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
    let roots: Vec<std::path::PathBuf> = if stdlib {
        vec![stdlib_root().map_err(|e| anyhow::anyhow!(e))?]
    } else {
        weft_compiler::project::node_roots(&ctx.project()?.root).to_vec()
    };
    let cat = FsCatalog::discover_roots_with_policy(
        &roots.iter().map(|r| r.as_path()).collect::<Vec<_>>(),
        DiscoverPolicy::Lenient,
    )
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
                description: first_sentence(&metadata.description),
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

#[cfg(test)]
mod tests {
    use super::first_sentence;

    /// The listing shows one sentence per node, and a dot inside a
    /// name (`weft.dev`, `ctx.inputs`) is not the end of one.
    #[test]
    fn the_listing_keeps_the_first_sentence_whole() {
        assert_eq!(first_sentence("Fire on a schedule. The `cron` expression has six fields."), "Fire on a schedule.");
        assert_eq!(first_sentence("Read ctx.inputs by name. Then emit."), "Read ctx.inputs by name.");
        assert_eq!(first_sentence("Search weft.dev"), "Search weft.dev");
        assert_eq!(first_sentence("One sentence.\nA second."), "One sentence.");
        assert_eq!(first_sentence(""), "");
        // An aside in parentheses stays inside its sentence, and an
        // abbreviation's stop is not the sentence's.
        assert_eq!(
            first_sentence("Read records (e.g. `{Status} = 'open'`). Fields come back typed."),
            "Read records (e.g. `{Status} = 'open'`)."
        );
        assert_eq!(first_sentence("Pull a file, e.g. a PDF. Then read it."), "Pull a file, e.g. a PDF.");
        assert_eq!(first_sentence("Fires per message (a search query, i.e. Gmail's own). Twice."), "Fires per message (a search query, i.e. Gmail's own).");
    }
}
