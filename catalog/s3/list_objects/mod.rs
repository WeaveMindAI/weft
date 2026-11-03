//! S3ListObjects: list one page of a bucket's keys under a prefix
//! (list-type=2). A bucket can hold far more keys than the 100 KB a
//! wire carries, so a call lists at most `maxKeys` of them after
//! `startAfter`, and only as many as fit a wire, and says where the
//! next page starts (`lastKey`, S3's own `start-after`) and whether
//! there is one. The store answers XML; the tiny tag-scan below pulls
//! exactly the fields the node emits, so the package carries no XML
//! dependency for a few tag names.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::{body_error, checked_send};
use weft::node::NodeOutput;
use weft::storage::wire_page;
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct S3ListObjectsNode;

#[async_trait]
impl Node for S3ListObjectsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let bucket: String = ctx.inputs.get("bucket")?;
        let prefix: Option<String> = ctx.inputs.opt("prefix")?;
        let start_after: Option<String> = ctx.inputs.opt("startAfter")?;
        let max_keys: u64 = ctx.inputs.get("maxKeys")?;

        let s3 = ctx.client(&access).await?;
        let url = super::s3::bucket_url(&bucket)?
            .query("list-type", 2)
            .query("max-keys", max_keys)
            .query_opt("prefix", prefix.as_deref())
            .query_opt("start-after", start_after.as_deref());
        let body = checked_send(s3.get(url.as_str()), "list the bucket's objects")
            .await?
            .text()
            .await
            .map_err(|e| body_error("s3: read list response", e))?;
        let listed = listing_objects(&body)?;
        let truncated = first_tag(&body, "IsTruncated").as_deref() == Some("true");

        // `keys` holds less of each object, so what fits `objects` fits
        // `keys` too. A key is at most 1024 bytes, which JSON escapes can
        // swell to about 6 KB, so no entry outweighs a wire on its own.
        let page = wire_page(&listed, 0, usize::MAX).map_err(|i| {
            weft::node_error(format!("s3: listing entry {i} weighs more than a wire carries on its own"))
        })?;
        let has_more = truncated || page.end < listed.len();
        let objects = &listed[page];
        // With nothing listed, the next page still starts where this
        // one was asked to.
        let last_key =
            objects.last().and_then(|o| o["key"].as_str()).or(start_after.as_deref()).unwrap_or_default().to_string();
        let count = objects.len() as f64;
        let keys: Vec<Value> = objects.iter().map(|o| o["key"].clone()).collect();
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("objects", json!(objects))
                .set("keys", json!(keys))
                .set("count", count)
                .set("lastKey", last_key)
                .set("hasMore", has_more),
        )
        .await
    }
}

/// The objects of one listing answer, in key order.
fn listing_objects(body: &str) -> WeftResult<Vec<Value>> {
    // S3 always sends Key, Size, and LastModified on a Contents
    // element; an absence means the response is not the listing shape
    // we think it is, and a silently-zero size would make a downstream
    // "skip empty objects" filter drop real files.
    tag_bodies(body, "Contents")
        .iter()
        .map(|content| {
            let key = first_tag(content, "Key").node_err("s3: a listing entry carries no Key")?;
            let size: f64 = first_tag(content, "Size")
                .and_then(|s| s.parse().ok())
                .node_err("s3: a listing entry carries no numeric Size")?;
            let modified =
                first_tag(content, "LastModified").node_err("s3: a listing entry carries no LastModified")?;
            Ok(json!({ "key": key, "sizeBytes": size, "lastModified": modified }))
        })
        .collect()
}

/// Every `<tag>...</tag>` body in `xml`, in order. A plain scan is
/// exact here BECAUSE S3 entity-escapes `<` inside every value (a key
/// containing "<Contents>" arrives as "&lt;Contents&gt;"), carries no
/// attributes on these tags, and never nests a tag inside itself.
fn tag_bodies(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else { break };
        out.push(after[..end].to_string());
        rest = &after[end + close.len()..];
    }
    out
}

/// The first `<tag>` body, XML-entity-decoded (keys may contain `&`).
fn first_tag(xml: &str, tag: &str) -> Option<String> {
    tag_bodies(xml, tag).into_iter().next().map(|s| decode_entities(&s))
}

/// Decode XML character references in one left-to-right scan, so a
/// decoded `&` never starts a second reference (`&amp;lt;` is `&lt;`):
/// the five named entities plus decimal (`&#13;`) and hex (`&#x0D;`)
/// numeric references. Anything else is kept verbatim.
fn decode_entities(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        let tail = &rest[amp..];
        let decoded = tail.find(';').and_then(|semi| {
            let name = &tail[1..semi];
            let ch = match name {
                "amp" => Some('&'),
                "lt" => Some('<'),
                "gt" => Some('>'),
                "quot" => Some('"'),
                "apos" => Some('\''),
                _ => {
                    let code = if let Some(hex) =
                        name.strip_prefix("#x").or_else(|| name.strip_prefix("#X"))
                    {
                        u32::from_str_radix(hex, 16).ok()
                    } else if let Some(dec) = name.strip_prefix('#') {
                        dec.parse::<u32>().ok()
                    } else {
                        None
                    };
                    code.and_then(char::from_u32)
                }
            };
            ch.map(|c| (c, semi))
        });
        match decoded {
            Some((c, semi)) => {
                out.push(c);
                rest = &tail[semi + 1..];
            }
            None => {
                out.push('&');
                rest = &tail[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(feature = "node-tests")]
mod tests;
