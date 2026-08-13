//! GoogleCalendarListEvents: the events in a time window, expanded
//! (recurring events appear as their instances) and ordered by start.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

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

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let calendar: String = ctx.inputs.get("calendar")?;
        let time_min: String = ctx.inputs.get("timeMin")?;
        let time_max: String = ctx.inputs.get("timeMax")?;
        let query: Option<String> = ctx.inputs.opt("query")?;

        let mut base = format!(
            "https://www.googleapis.com/calendar/v3/calendars/{}/events?singleEvents=true&orderBy=startTime&maxResults=250&timeMin={}&timeMax={}",
            urlencoding::encode(&calendar),
            urlencoding::encode(&time_min),
            urlencoding::encode(&time_max),
        );
        if let Some(q) = query.filter(|q| !q.trim().is_empty()) {
            base.push_str(&format!("&q={}", urlencoding::encode(&q)));
        }
        let http = ctx.client(&account).await?;

        // Page until the window is exhausted: a `count` that silently
        // stopped at one page would be a wrong number the workflow
        // trusts.
        let events: Vec<Value> =
            super::api::paged(&http, &base, "items", "google calendar: list events", |_| false)
                .await?
                .iter()
                .map(|e| {
                    json!({
                        "id": e["id"],
                        "summary": e["summary"],
                        "start": e["start"]["dateTime"].as_str().or(e["start"]["date"].as_str()),
                        "end": e["end"]["dateTime"].as_str().or(e["end"]["date"].as_str()),
                        "location": e["location"],
                        "description": e["description"],
                        "link": e["htmlLink"],
                    })
                })
                .collect();
        let count = events.len() as f64;
        ctx.pulse_downstream(
            NodeOutput::new().set("events", json!(events)).set("count", count),
        )
        .await
    }
}
