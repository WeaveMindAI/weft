//! RssFeed: fires once per new entry in an RSS or Atom feed.
//! Registers a feed-format delta poll (the listener parses the XML
//! and dedups on entry ids; activation primes silently, history never
//! replays, the position survives restarts). No account: feeds are
//! public URLs.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::signal::{DeltaMode, PollDelta, PollEndpoint, PollFormat};
use weft::{RunContext, Node, NodeManifest, WeftResult};

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

    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()> {
        let url: String = ctx.inputs.get("feedUrl")?;
        let interval: u64 = ctx.inputs.get("intervalSecs")?;

        ctx.register_signal(PollEndpoint {
            url,
            format: PollFormat::Feed,
            interval_secs: interval,
            delta: Some(PollDelta {
                items: "items".into(),
                cursor_field: Some("id".into()),
                mode: DeltaMode::Set,
                cursor_param: None,
                // A syndication feed has no next-page cursor to follow.
                paging: None,
            }),
            ..Default::default()
        })
        .await
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let entry = ctx.wake.get::<weft::serde_json::Value>("item")?;
        // A feed that gives no title or no link says so with a null.
        // Turning either into an empty string sent an empty URL down a
        // wire that then fetched nothing, with nothing to look at.
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("title", entry["title"].clone())
                .set("link", entry["link"].clone())
                .set("summary", entry["summary"].clone())
                .set("published", entry["published"].clone())
                .set("entry", entry),
        )
        .await
    }
}
