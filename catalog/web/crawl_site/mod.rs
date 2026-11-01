//! CrawlSite: crawl a site through Firecrawl and emit every page as
//! clean markdown. Submits the crawl job, then parks on the job without
//! holding a worker until it reaches a terminal state (a crawl the user
//! asked for takes as long as it takes, so there is no deadline here;
//! cancelling the run cancels the wait and the crawl).
//!
//! A crawl's pages weigh far more than the 100 KB a wire carries, so
//! they come out as one stored file, a JSON line per page, with the
//! page count beside it, streamed into storage an answer at a time. The
//! node reads the finished crawl the way Firecrawl documents it: the
//! answer and each `next` link it carries, until an answer has no
//! `next` or brings no new pages.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::{json, Value};

use weft::access::client::get_json;
use weft::node::NodeOutput;
use weft::signal::{CancelCall, PollEndpoint, Predicate};
use weft::reqwest_middleware::ClientWithMiddleware;
use weft::storage::{ByteStream, StorageScope};
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftError, WeftResult};

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

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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
        // pages are read once it has ended. Cancelling the run cancels the
        // crawl too, with the DELETE Firecrawl documents for it
        // (https://docs.firecrawl.dev/api-reference/endpoint/crawl-delete).
        let status_url = format!("{}/crawl/{job}", super::firecrawl::API);
        let answer = ctx
            .await_signal_with_cancel(
                PollEndpoint {
                    url: status_url.clone(),
                    interval_secs: 5,
                    access: Some(weft::primitive::AccessRef::from(&access)),
                    filters: vec![Predicate::neq("status", "scraping")],
                    carry: vec!["status".into(), "error".into()],
                    ..Default::default()
                },
                CancelCall::delete(status_url.clone()),
            )
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

        // The completed answer pages through `next` links. Each answer's
        // pages stream into the stored file as it is read, one JSON line
        // each, so the worker holds one answer at a time, never the
        // crawl. A read that fails mid-way fails the stream, the storage
        // abandons the upload (no half file is kept), and the node fails
        // with that read's own error.
        let count = Arc::new(AtomicUsize::new(0));
        let failure: Arc<Mutex<Option<WeftError>>> = Arc::new(Mutex::new(None));
        let lines = answer_lines(http.clone(), status_url, count.clone(), failure.clone());
        let stored = ctx
            .storage(StorageScope::Run)
            .put_stream(lines, "application/jsonl", &format!("crawl-{job}.jsonl"), None)
            .await;
        let pages = match stored {
            Ok(pages) => pages,
            Err(e) => return Err(failure.lock().expect("failure poisoned").take().unwrap_or(e)),
        };
        let count = count.load(Ordering::Relaxed) as f64;
        ctx.pulse_downstream(NodeOutput::new().set("pages", pages).set("count", count)).await
    }
}

/// The finished crawl as a stream of JSON lines, one chunk per Firecrawl
/// answer: the answer at `first`, then each `next` link, until an answer
/// has no `next` or brings no new pages (Firecrawl's documented rule).
/// Every page that passes is added to `count`. A failed read ends the
/// stream with an error and leaves the node's own error in `failure`,
/// since the storage only sees the stream's.
fn answer_lines(
    http: ClientWithMiddleware,
    first: String,
    count: Arc<AtomicUsize>,
    failure: Arc<Mutex<Option<WeftError>>>,
) -> ByteStream {
    let reads = futures::stream::try_unfold(Some((first, true)), move |at| {
        let (http, count, failure) = (http.clone(), count.clone(), failure.clone());
        async move {
            let Some((url, is_first)) = at else { return Ok(None) };
            let what =
                if is_first { "firecrawl: read the finished crawl" } else { "firecrawl: read the crawl's next page" };
            let read = async {
                let answer = get_json(&http, &url, what).await?;
                let mut lines = Vec::new();
                let added = write_pages(&answer, &mut lines)?;
                let next = answer["next"].as_str().filter(|_| added > 0).map(str::to_string);
                WeftResult::Ok((lines, added, next))
            };
            match read.await {
                Ok((lines, added, next)) => {
                    count.fetch_add(added, Ordering::Relaxed);
                    Ok(Some((Bytes::from(lines), next.map(|n| (n, false)))))
                }
                Err(e) => {
                    let said = e.to_string();
                    *failure.lock().expect("failure poisoned") = Some(e);
                    Err(std::io::Error::other(said))
                }
            }
        }
    });
    Box::pin(reads)
}

/// Each scraped page of one status answer, appended to `lines` as one
/// JSON line; answers how many. The markdown IS the node's product, so
/// a page without it is refused, as FetchPage refuses it.
fn write_pages(answer: &Value, lines: &mut Vec<u8>) -> WeftResult<usize> {
    let mut added = 0;
    for d in answer["data"].as_array().into_iter().flatten() {
        let markdown = d["markdown"].as_str().node_err(
            "firecrawl: the crawl completed but one of its pages carries no markdown",
        )?;
        let page = json!({
            "url": d.pointer("/metadata/sourceURL").cloned(),
            "title": d.pointer("/metadata/title").cloned(),
            "markdown": markdown,
        });
        serde_json::to_writer(&mut *lines, &page).node_err("firecrawl: write a crawled page")?;
        lines.push(b'\n');
        added += 1;
    }
    Ok(added)
}
