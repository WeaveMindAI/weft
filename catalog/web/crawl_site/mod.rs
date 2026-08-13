//! CrawlSite: crawl a site through Firecrawl and emit every page as
//! clean markdown. Submits the crawl job, then waits ON THE RESULT
//! (polling the job until it reaches a terminal state; a crawl the
//! user asked for takes as long as it takes, so there is no deadline
//! here; cancelling the execution cancels the wait).

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::context::LogLevel;
use weft::node::NodeOutput;
use weft::access::client::get_json;
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[cfg(feature = "node-tests")]
mod tests;

#[derive(NodeManifest)]
pub struct CrawlSiteNode;

#[async_trait]
impl Node for CrawlSiteNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let url: String = ctx.inputs.get("url")?;
        let limit: f64 = ctx.inputs.get("limit")?;

        let http = ctx.client(&access).await?;
        let body = json!({
            "url": url,
            "limit": (limit as u64).clamp(1, 1000),
            "scrapeOptions": { "formats": ["markdown"] },
        });
        let started =
            super::firecrawl::call(&http, "crawl", &body, "firecrawl: start the crawl").await?;
        let job = started["id"].as_str().node_err("firecrawl: crawl carries no id")?.to_string();

        // Wait on the job. Pages accumulate across status reads (the
        // completed answer pages through `next` links).
        let mut pages: Vec<Value> = Vec::new();
        let mut next = format!("{}/crawl/{job}", super::firecrawl::API);
        let mut polls: u64 = 0;
        loop {
            let answer: Value = get_json(&http, &next, "firecrawl: read crawl status").await?;
            let state = answer["status"].as_str().unwrap_or_default();
            match state {
                "completed" => {
                    collect_pages(&answer, &mut pages);
                    match answer["next"].as_str() {
                        Some(n) => next = n.to_string(),
                        None => break,
                    }
                }
                "failed" | "cancelled" => {
                    weft::node_bail!(
                        "the crawl ended {state}: {}",
                        answer["error"].as_str().unwrap_or("no detail")
                    );
                }
                // "scraping" is the one status Firecrawl documents for a
                // crawl still in flight. A breadcrumb every ~10 polls
                // keeps a long crawl legible in the logs.
                "scraping" => {
                    polls += 1;
                    if polls % 10 == 0 {
                        ctx.log(
                            LogLevel::Info,
                            format!(
                                "crawl {job} still running: {}/{} pages scraped",
                                answer["completed"].as_u64().unwrap_or(0),
                                answer["total"].as_u64().unwrap_or(0)
                            ),
                        )
                        .await?;
                    }
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                // Anything else (a missing status included) is an answer
                // this node does not understand; spinning on it forever
                // would hide the problem behind an execution that never
                // ends.
                other => weft::node_bail!(
                    "firecrawl answered an unexpected status '{other}' for crawl {job}"
                ),
            }
        }

        let count = pages.len() as f64;
        ctx.pulse_downstream(
            NodeOutput::new().set("pages", json!(pages)).set("count", count),
        )
        .await
    }
}

fn collect_pages(answer: &Value, pages: &mut Vec<Value>) {
    for d in answer["data"].as_array().into_iter().flatten() {
        pages.push(json!({
            "url": d.pointer("/metadata/sourceURL").cloned(),
            "title": d.pointer("/metadata/title").cloned(),
            "markdown": d["markdown"],
        }));
    }
}
