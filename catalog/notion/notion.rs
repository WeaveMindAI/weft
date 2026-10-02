//! Shared Notion plumbing: the API base, the database -> data-source
//! resolution every database node repeats, and the plain-text ->
//! paragraph-blocks conversion the writing nodes share.
//!
//! Notion's 2025-09-03 API model: a DATABASE is a container; its rows
//! live in one or more DATA SOURCES. Queries and row creation address
//! a data source id, never the database id.

use serde_json::{json, Value};

use weft::access::client::get_json;
use weft::{NodeErrExt, WeftResult};

pub const API: &str = "https://api.notion.com/v1";

/// The data source a database node acts on: the picked one when the
/// user picked one, else the database's ONLY data source (several is
/// a loud error naming them; acting on an arbitrary one would write
/// rows into whichever source happened to sort first).
pub async fn data_source_id(
    http: &weft::reqwest_middleware::ClientWithMiddleware,
    database: &str,
    picked: Option<&str>,
) -> WeftResult<String> {
    if let Some(id) = picked.filter(|s| !s.trim().is_empty()) {
        return Ok(id.to_string());
    }
    let db = get_json(
        http,
        &format!("{API}/databases/{database}"),
        "notion: read the database",
    )
    .await?;
    let sources: Vec<&Value> =
        db["data_sources"].as_array().into_iter().flatten().collect();
    match sources.as_slice() {
        [] => weft::node_bail!("notion database {database} has no data source"),
        [one] => Ok(one["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .node_err("notion: the database's only data source carries no id")?
            .to_string()),
        several => {
            let names: Vec<&str> =
                several.iter().filter_map(|s| s["name"].as_str()).collect();
            Err(weft::WeftError::Input(format!(
                "notion database {database} has several data sources ({}); pick one on \
                 the node's Source field",
                names.join(", ")
            )))
        }
    }
}

/// Plain text as Notion paragraph blocks: one paragraph per line,
/// blank lines dropped.
pub fn paragraph_blocks(content: &str) -> Vec<Value> {
    content
        .lines()
        .map(str::trim_end)
        .filter(|l| !l.is_empty())
        .map(|line| {
            json!({
                "object": "block",
                "type": "paragraph",
                "paragraph": { "rich_text": [{ "type": "text", "text": { "content": line } }] },
            })
        })
        .collect()
}

/// The children a writing node sends: the raw `blocks` list when one
/// was wired (full Notion block objects, power path), else `content`
/// as paragraphs. Empty when neither holds anything: a new page may
/// have no body, and a node for which that is a mistake says so itself.
pub fn children_of(content: Option<&str>, blocks: Option<&Value>) -> WeftResult<Vec<Value>> {
    if let Some(blocks) = blocks {
        let Some(list) = blocks.as_array() else {
            return Err(weft::WeftError::Input(
                "blocks must be a list of Notion block objects".to_string(),
            ));
        };
        if !list.is_empty() {
            return Ok(list.clone());
        }
    }
    Ok(content.map(paragraph_blocks).unwrap_or_default())
}
