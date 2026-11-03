//! GoogleCalendarListEvents: the events in a time window, expanded
//! (recurring events appear as their instances) and ordered by start.

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};

use weft::access::client::url;
use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct GoogleCalendarListEventsNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleCalendarListEventsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let calendar: String = ctx.inputs.get("calendar")?;
        let time_min: DateTime<Utc> = ctx.inputs.get("timeMin")?;
        let time_max: DateTime<Utc> = ctx.inputs.get("timeMax")?;
        let query: Option<String> = ctx.inputs.opt("query")?;
        let max_results: u64 = ctx.inputs.get("maxResults")?;
        let page_token: Option<String> = ctx.inputs.opt("pageToken")?;

        let base = url(super::api::CALENDAR_API, &["calendars", &calendar, "events"])?
            .query("singleEvents", true)
            .query("orderBy", "startTime")
            .query("maxResults", max_results)
            .query("timeMin", time_min.to_rfc3339_opts(SecondsFormat::AutoSi, true))
            .query("timeMax", time_max.to_rfc3339_opts(SecondsFormat::AutoSi, true))
            .query_opt("q", query);
        let http = ctx.client(&account).await?;

        // One Google page per call: a window can hold more events than
        // any wire, and a workflow that needs all of them pages with
        // `nextPageToken` until `hasMore` is false.
        let page = super::api::list_page(
            &http,
            base,
            "items",
            page_token.as_deref(),
            event,
            "google calendar: list events",
            "maxResults",
        )
        .await?;
        let count = page.items.len() as f64;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("events", Value::Array(page.items))
                .set("count", count)
                .set("hasMore", page.next_page_token.is_some())
                .set("nextPageToken", page.next_page_token.unwrap_or_default()),
        )
        .await
    }
}

/// One event as the node emits it, its start and end flattened to the
/// time (or the date, for an all-day event).
fn event(e: &Value) -> Value {
    json!({
        "id": e["id"],
        "summary": e["summary"],
        "start": e["start"]["dateTime"].as_str().or(e["start"]["date"].as_str()),
        "end": e["end"]["dateTime"].as_str().or(e["end"]["date"].as_str()),
        "location": e["location"],
        "description": e["description"],
        "link": e["htmlLink"],
    })
}
