//! Shared Notion plumbing: the API base, the database -> data-source
//! resolution every database node repeats, and the plain-text ->
//! paragraph-blocks conversion the writing nodes share.
//!
//! Notion's 2025-09-03 API model: a DATABASE is a container; its rows
//! live in one or more DATA SOURCES. Queries and row creation address
//! a data source id, never the database id.

use serde_json::{json, Value};

use weft::access::client::{get_json, required_str, url};
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
    if let Some(id) = picked {
        return Ok(id.to_string());
    }
    let db = get_json(http, url(API, &["databases", database])?.as_str(), "notion: read the database").await?;
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
            weft::input_bail!(
                "notion database {database} has several data sources ({}); pick one on \
                 the node's Source field",
                names.join(", ")
            )
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

/// The children a writing node sends: the `blocks` list when it holds
/// any (full Notion block objects, power path), else `content` as
/// paragraphs. Empty when neither holds anything: a new page may have
/// no body.
pub fn children_of(content: Option<&str>, blocks: Vec<Value>) -> Vec<Value> {
    if !blocks.is_empty() {
        return blocks;
    }
    content.map(paragraph_blocks).unwrap_or_default()
}

/// A page (or a row, which is a page) a Notion call answered with: its
/// id and its link, the two outputs every page-writing node emits.
pub fn page_output(page: &Value, what: &str) -> WeftResult<weft::node::NodeOutput> {
    Ok(weft::node::NodeOutput::new()
        .set("pageId", required_str(page, what, "id")?)
        .set("url", page["url"].clone()))
}
