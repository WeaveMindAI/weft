//! RssFeed: fires once per new entry in an RSS or Atom feed.
//! Registers a feed-format delta poll (the listener parses the XML
//! and dedups on entry ids; activation primes silently, history never
//! replays, the position survives restarts). No account: feeds are
//! public URLs.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::signal::{DeltaMode, PollDelta, PollEndpoint, PollFormat};
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct RssFeedNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for RssFeedNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let url: String = ctx.inputs.get("feedUrl")?;
        let interval: f64 = ctx.inputs.get("intervalSecs")?;

        ctx.register_signal(PollEndpoint {
            url,
            format: PollFormat::Feed,
            interval_secs: interval as u64,
            delta: Some(PollDelta {
                items: "items".into(),
                cursor_field: Some("id".into()),
                mode: DeltaMode::Set,
                cursor_param: None,
            }),
            ..Default::default()
        })
        .await
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let wake = ctx.wake.record()?;
        let entry = wake["item"].clone();
        let title = entry["title"].as_str().unwrap_or_default().to_string();
        let link = entry["link"].as_str().unwrap_or_default().to_string();
        entry["id"].as_str().node_err("the poll wake carries no entry id")?;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("title", title)
                .set("link", link)
                .set("summary", entry["summary"].clone())
                .set("published", entry["published"].clone())
                .set("entry", entry),
        )
        .await
    }
}
