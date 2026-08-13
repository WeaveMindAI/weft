//! S3ListObjects: list a bucket's keys under a prefix, paging
//! (list-type=2) until exhausted. The store answers XML; the tiny
//! tag-scan below pulls exactly the fields the node emits, so the
//! package carries no XML dependency for three tag names.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct S3ListObjectsNode;

#[async_trait]
impl Node for S3ListObjectsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let bucket: String = ctx.inputs.get("bucket")?;
        let prefix: String = ctx.inputs.get_or("prefix", String::new())?;

        let s3 = ctx.client(&access).await?;
        let mut objects: Vec<Value> = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut query: Vec<(&str, &str)> =
                vec![("list-type", "2"), ("max-keys", "1000")];
            if !prefix.is_empty() {
                query.push(("prefix", prefix.as_str()));
            }
            if let Some(t) = &token {
                query.push(("continuation-token", t.as_str()));
            }
            let url = super::s3::bucket_url(&bucket, &query);
            let resp = s3.get(&url).send().await.node_err("s3: list objects")?;
            let resp = super::s3::ok_or_bail(resp, "the list").await?;
            let body = resp.text().await.node_err("s3: read list response")?;
            // S3 always sends Key, Size, and LastModified on a
            // Contents element; an absence means the response is not
            // the listing shape we think it is, and a silently-zero
            // size would make a downstream "skip empty objects" filter
            // drop real files.
            for content in tag_bodies(&body, "Contents") {
                let key = first_tag(&content, "Key")
                    .node_err("s3: a listing entry carries no Key")?;
                let size: f64 = first_tag(&content, "Size")
                    .and_then(|s| s.parse().ok())
                    .node_err("s3: a listing entry carries no numeric Size")?;
                let modified = first_tag(&content, "LastModified")
                    .node_err("s3: a listing entry carries no LastModified")?;
                objects.push(json!({ "key": key, "sizeBytes": size, "lastModified": modified }));
            }
            token = first_tag(&body, "NextContinuationToken").filter(|t| !t.is_empty());
            if token.is_none() {
                break;
            }
        }

        let count = objects.len() as f64;
        let keys: Vec<Value> = objects.iter().map(|o| o["key"].clone()).collect();
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("objects", json!(objects))
                .set("keys", json!(keys))
                .set("count", count),
        )
        .await
    }
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
    tag_bodies(xml, tag).into_iter().next().map(|s| {
        s.replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
    })
}

#[cfg(feature = "node-tests")]
mod tests;
