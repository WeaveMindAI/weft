//! GoogleCalendarCreateEvent: put an event on a calendar. Timed
//! events take RFC 3339 datetimes; all-day events take plain dates
//! (YYYY-MM-DD); the shape is picked per value, so mixing (a timed
//! start, a dated end) fails loudly at Google with its own message.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::access::client::required_str;
use weft::node::NodeOutput;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

/// A start/end value to the API's shape: a bare date is all-day.
fn when(value: &str) -> Value {
    if value.len() == 10 && value.as_bytes().get(4) == Some(&b'-') {
        json!({ "date": value })
    } else {
        json!({ "dateTime": value })
    }
}

#[derive(NodeManifest)]
pub struct GoogleCalendarCreateEventNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleCalendarCreateEventNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let calendar: String = ctx.inputs.get("calendar")?;
        let summary: String = ctx.inputs.get("summary")?;
        let start: String = ctx.inputs.get("start")?;
        let end: String = ctx.inputs.get("end")?;
        let description: Option<String> = ctx.inputs.opt("description")?;
        let location: Option<String> = ctx.inputs.opt("location")?;
        let attendees: Vec<String> = ctx.inputs.list("attendees")?;

        let mut body = json!({
            "summary": summary,
            "start": when(&start),
            "end": when(&end),
        });
        if let Some(d) = description {
            body["description"] = json!(d);
        }
        if let Some(l) = location {
            body["location"] = json!(l);
        }
        if !attendees.is_empty() {
            body["attendees"] = Value::Array(
                attendees.into_iter().map(|e| json!({ "email": e })).collect(),
            );
        }

        let http = ctx.client(&account).await?;
        let answer = weft::access::client::json_call(
            http.post(format!(
                "https://www.googleapis.com/calendar/v3/calendars/{}/events",
                urlencoding::encode(&calendar)
            ))
            .json(&body),
            "create the event",
        )
        .await?;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("eventId", required_str(&answer, "create the event", "id")?.to_string())
                .set("link", required_str(&answer, "create the event", "htmlLink")?.to_string()),
        )
        .await
    }
}
