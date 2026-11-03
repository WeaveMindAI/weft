//! `weft describe-nodes`: print the project's node catalog as JSON
//! (every node under `nodes/` and beside the code under `src/`), for the
//! editor's node palette, and (`--compact`) as the token-cheap wiring
//! view an AI reads instead of whole `metadata.json` files. Runs locally
//! because the catalog lives in the project's folders.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::Serialize;

use weft_catalog::{stdlib_root, FsCatalog};

use super::Ctx;

#[derive(Serialize)]
struct NodesResponse<T> {
    // The metadata itself is the wire entry: everything the palette needs
    // (form-field specs included) is a metadata key. Under `--compact`
    // the values are the filtered wiring view instead, same envelope.
    catalog: BTreeMap<String, T>,
    /// Soft errors from scanning the node roots (malformed metadata.json,
    /// duplicate types), and names a loaded node spells off the weft way.
    /// Surfaced, not silent: a node mid-rename has a transient parse
    /// error the editor should see but not crash on.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<weft_catalog::CatalogWarning>,
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

/// `--list --json`: the node rows, and apart from them the `.weft` files
/// shipped under `nodes/` that a program can `@include`, so a file is
/// never read as a node type.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Listing<'a> {
    nodes: Vec<ListRow<'a>>,
    include_files: Vec<IncludeRow>,
}

/// One shipped `.weft` file: the path a program writes in `@include`
/// (`__catalog/base_catalog/computer/agent.weft`), and its first-line
/// comment when it has one.
#[derive(Serialize)]
struct IncludeRow {
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

/// Every `.weft` file under `root` as an [`IncludeRow`], its path spelled
/// the way a program reaches it: `__catalog/` then `under`, the folder
/// `root` is inside `nodes/` (none for the project's `nodes/` itself,
/// `base_catalog` for the bundled stdlib, which a project holds there).
fn include_rows(root: &std::path::Path, under: Option<&str>) -> Result<Vec<IncludeRow>> {
    let files = weft_catalog::include_files(root).map_err(|e| anyhow::anyhow!("list the .weft files under {}: {e}", root.display()))?;
    let prefix = match under {
        Some(under) => format!("{}/{under}/", weft_compiler::project::CATALOG_PREFIX),
        None => format!("{}/", weft_compiler::project::CATALOG_PREFIX),
    };
    Ok(files
        .into_iter()
        .map(|file| IncludeRow { path: format!("{prefix}{}", file.path), description: file.description })
        .collect())
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
    // disk). Otherwise describe the project's own `nodes/`. The same
    // discovery as the build: a node that fails to load is left out and
    // listed in the warnings, so the palette survives a node mid-edit and
    // never disagrees with the build about what a node is.
    let roots: Vec<std::path::PathBuf> = if stdlib {
        vec![stdlib_root().map_err(|e| anyhow::anyhow!(e))?]
    } else {
        weft_compiler::project::node_roots(&ctx.project()?.root).to_vec()
    };
    let cat = FsCatalog::discover_roots(&roots.iter().map(|r| r.as_path()).collect::<Vec<_>>())
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
        // The scan warnings go out either way, JSON or not: a node
        // mid-rename that dropped out of the listing has to say so, and so
        // does a name a listed node spells off the weft way.
        for warning in cat.warnings() {
            eprintln!("warning: {warning}");
        }
        // Only `nodes/` is reachable through `__catalog/`, so a `.weft`
        // beside the code under `src/` is not listed.
        let include_files = if stdlib {
            include_rows(&roots[0], Some(weft_compiler::project::BASE_CATALOG_DIR))?
        } else {
            include_rows(&ctx.project()?.nodes_dir(), None)?
        };
        if ctx.json() {
            let listing = Listing { nodes: rows, include_files };
            println!("{}", serde_json::to_string(&listing).context("serialize node listing")?);
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
        // The files after the types, under their own heading: each line
        // starts with the path a program writes, so a grep for a word
        // still lands on a line that says what to do with it.
        if !include_files.is_empty() {
            println!("\nFiles a program can @include(\"<path>\"):");
            let width = include_files.iter().map(|f| f.path.len()).max().unwrap_or(0);
            for file in &include_files {
                match &file.description {
                    Some(description) => println!("{:<width$}  {description}", file.path),
                    None => println!("{}", file.path),
                }
            }
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
            if let Some(answer) = language_construct(wanted) {
                anyhow::bail!("{answer}");
            }
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
            serde_json::to_string_pretty(&editor_entry(metadata)?)
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
        let catalog = catalog
            .iter()
            .map(|(node_type, metadata)| Ok((node_type.clone(), editor_entry(metadata)?)))
            .collect::<Result<BTreeMap<String, serde_json::Value>>>()?;
        serde_json::to_string(&NodesResponse { catalog, warnings })
    };
    println!("{}", printed.context("serialize describe response")?);
    Ok(())
}

/// One node's full entry, as the editor reads it: its resolved metadata,
/// and on a node checking its callers the recipe of the connection its
/// `auth` reads (`callerRecipes`), keyed by each word the value is read
/// by: a name `auth` takes, or the `kind` of a check written as a record.
/// No catalog node carries these; its Connect panel follows `auth`.
// SYNC: callerRecipes <-> packages/weft-graph/src/protocol.ts CatalogEntry.callerRecipes
fn editor_entry(metadata: &weft_core::node::NodeMetadata) -> Result<serde_json::Value> {
    let mut entry = serde_json::to_value(metadata).context("serialize a node's metadata")?;
    if metadata.features.checks_callers {
        use weft_core::access::callers::{CallerScheme, NAMED_CHECKS};
        let named = NAMED_CHECKS.iter().map(|named| {
            (named.name, CallerScheme::of(&named.verify()).expect("a named check checks callers"))
        });
        let kinds = CallerScheme::ALL.into_iter().map(|scheme| (scheme.name(), scheme));
        let recipes: serde_json::Map<String, serde_json::Value> = named
            .chain(kinds)
            .map(|(word, scheme)| Ok((word.to_string(), serde_json::to_value(scheme.recipe())?)))
            .collect::<Result<_, serde_json::Error>>()
            .context("serialize the caller checks' recipes")?;
        entry["callerRecipes"] = serde_json::Value::Object(recipes);
    }
    Ok(entry)
}

/// What `--node <name>` answers for a name the language owns rather
/// than the catalog: what it is, and where it is explained. Every
/// type name weft owns (`own_types`) has an answer (a test holds that), plus
/// `Include`, which is written `@include(...)` but is what someone
/// looking for "the include node" asks for.
fn language_construct(name: &str) -> Option<String> {
    let (what, page) = match name {
        "Group" => ("a group, written `name = Group(...) { ... }`", "docs/src/language/groups.md"),
        "Loop" => ("a loop, written `name = Loop(...) { ... }`", "docs/src/language/loops.md"),
        "Include" => ("an include, written `name = @include(\"file.weft\")`", "docs/src/language/files-and-reuse.md"),
        weft_core::project::boundary_types::LOOP_IN | weft_core::project::boundary_types::LOOP_OUT => (
            "a node the compiler makes out of a loop; you never write it",
            "docs/src/language/how-a-program-runs.md",
        ),
        weft_core::project::INCLUDED_GROUP_TYPE => (
            "the block an include shows when its file is not opened up, or stands in for an \
             included file that does not compile; you never write it",
            "docs/src/language/files-and-reuse.md",
        ),
        _ => return None,
    };
    Some(format!(
        "`{name}` is not a catalog node, it is part of the weft language: {what}. \
         The weft-language skill explains it, and so does {page}"
    ))
}

#[cfg(test)]
mod tests {
    use super::{editor_entry, first_sentence, language_construct};

    /// A Route's `auth` choices are in its wiring view, and its editor
    /// entry carries each scheme's recipe for the Connect panel.
    #[test]
    fn a_route_shows_its_auth_choices_and_carries_their_recipes() {
        let cat = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
        let route = weft_core::node::MetadataCatalog::lookup(&cat, "Route").expect("Route is in the catalog").resolved();
        let compact = route.compact_json();
        let auth = compact["inputs"].as_array().unwrap().iter().find(|i| i["name"] == "auth").expect("an auth input");
        assert_eq!(
            auth["widget"]["options"],
            serde_json::json!(["none", "api_keys", "hmac_sha256", "hmac_sha1", "hmac_sha512", "ed25519", "ecdsa_p256", "oidc"])
        );
        let entry = editor_entry(&route).unwrap();
        assert_eq!(entry["callerRecipes"]["api_keys"]["service"], "api_key_auth");
        assert_eq!(entry["callerRecipes"]["oidc"]["service"], "oidc_auth");
        assert_eq!(entry["callerRecipes"]["hmac_sha512"]["service"], "hmac_auth");
        assert_eq!(entry["callerRecipes"]["ecdsa_p256"]["service"], "signature_auth");
        // A check written as a record finds its recipe by its `kind`.
        assert_eq!(entry["callerRecipes"]["hmac"]["service"], "hmac_auth");
        let reply = weft_core::node::MetadataCatalog::lookup(&cat, "Reply").unwrap().resolved();
        assert!(editor_entry(&reply).unwrap().get("callerRecipes").is_none());
    }

    /// Every type name the language reserves answers with what it is,
    /// never "unknown node type".
    #[test]
    fn a_language_construct_is_named_as_one() {
        for name in weft_core::project::own_types::ALL.iter().chain(&["Include"]) {
            let answer = language_construct(name).unwrap_or_else(|| panic!("{name} has no answer"));
            assert!(answer.contains("part of the weft language"), "{answer}");
            assert!(answer.contains("docs/src/language/"), "{answer}");
        }
        assert!(language_construct("Loop").unwrap().contains("loops.md"));
        assert_eq!(language_construct("HttpRequest"), None);
    }

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
