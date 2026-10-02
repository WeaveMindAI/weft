//! CrawlSite: crawl a site through Firecrawl and emit every page as
//! clean markdown. Submits the crawl job, then parks on the job without
//! holding a worker until it reaches a terminal state (a crawl the user
//! asked for takes as long as it takes, so there is no deadline here;
//! cancelling the execution cancels the wait).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::get_json;
use weft::node::NodeOutput;
use weft::signal::{PollEndpoint, Predicate};
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
            "limit": limit as u64,
            "scrapeOptions": { "formats": ["markdown"] },
        });
        // Starting the crawl is the paid call, and the body replays from
        // the top when the wait resumes: journaled, the replay reads this
        // job id back instead of starting a second crawl.
        let started = ctx
            .run("firecrawl_start_crawl", || async {
                super::firecrawl::call(&http, "crawl", &body, "firecrawl: start the crawl").await
            })
            .await?;
        let job = started["id"].as_str().node_err("firecrawl: crawl carries no id")?.to_string();

        // "scraping" is the one status Firecrawl documents for a crawl
        // still in flight; any other ends the wait, so a status this node
        // does not know is refused below instead of being waited on
        // forever.
        // The finished answer embeds the first page of crawled data, which
        // can weigh megabytes: the wait carries only the status, and the
        // pages are read once it has ended.
        let status_url = format!("{}/crawl/{job}", super::firecrawl::API);
        let answer = ctx
            .await_signal(PollEndpoint {
                url: status_url.clone(),
                interval_secs: 5,
                access: Some(weft::primitive::AccessRef::from(&access)),
                filters: vec![Predicate::neq("status", "scraping")],
                carry: vec!["status".into(), "error".into()],
                ..Default::default()
            })
            .await?;
        match answer["status"].as_str().unwrap_or_default() {
            "completed" => {}
            state @ ("failed" | "cancelled") => weft::node_bail!(
                "the crawl ended {state}: {}",
                answer["error"].as_str().unwrap_or("no detail")
            ),
            other => weft::node_bail!(
                "firecrawl answered an unexpected status '{other}' for crawl {job}"
            ),
        }

        // The completed answer pages through `next` links; pages
        // accumulate across them.
        let mut pages: Vec<Value> = Vec::new();
        let mut answer = get_json(&http, &status_url, "firecrawl: read the finished crawl").await?;
        loop {
            collect_pages(&answer, &mut pages)?;
            let Some(next) = answer["next"].as_str().map(str::to_string) else { break };
            answer = get_json(&http, &next, "firecrawl: read the crawl's next page").await?;
        }

        let count = pages.len() as f64;
        ctx.pulse_downstream(
            NodeOutput::new().set("pages", json!(pages)).set("count", count),
        )
        .await
    }
}

/// Each scraped page of one status answer. The markdown IS the node's
/// product, so a page without it is refused, as FetchPage refuses it.
fn collect_pages(answer: &Value, pages: &mut Vec<Value>) -> WeftResult<()> {
    for d in answer["data"].as_array().into_iter().flatten() {
        let markdown = d["markdown"].as_str().node_err(
            "firecrawl: the crawl completed but one of its pages carries no markdown",
        )?;
        pages.push(json!({
            "url": d.pointer("/metadata/sourceURL").cloned(),
            "title": d.pointer("/metadata/title").cloned(),
            "markdown": markdown,
        }));
    }
    Ok(())
}
