//! GoogleCalendarListEvents self-tests: paging until the window is
//! exhausted, and the flattened event shape.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::GoogleCalendarListEventsNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("pages_and_flattens_the_events", pages_and_flattens),
        NodeTest::live("one_real_window_listing", "google", live_list),
    ]
}

async fn pages_and_flattens(rig: FakeRig) -> WeftResult<()> {
    let base = "/calendar/v3/calendars/primary/events?singleEvents=true&orderBy=startTime\
                &maxResults=250&timeMin=2026-08-01T00%3A00%3A00Z&timeMax=2026-08-31T00%3A00%3A00Z";
    rig.respond(
        "GET",
        base,
        json!({ "items": [{
            "id": "e1", "summary": "First",
            "start": { "dateTime": "2026-08-10T09:00:00Z" },
            "end": { "dateTime": "2026-08-10T10:00:00Z" },
            "htmlLink": "l1",
        }], "nextPageToken": "t2" }),
    );
    rig.respond(
        "GET",
        &format!("{base}&pageToken=t2"),
        json!({ "items": [{
            "id": "e2", "summary": "Second",
            "start": { "date": "2026-08-12" },
            "end": { "date": "2026-08-13" },
            "htmlLink": "l2",
        }] }),
    );
    let outcome = rig
        .run(
            &GoogleCalendarListEventsNode,
            json!({
                "account": rig.access("google"),
                "timeMin": "2026-08-01T00:00:00Z",
                "timeMax": "2026-08-31T00:00:00Z",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(2.0), "both pages counted");
    let events = outcome.outputs["events"].as_array().expect("events list").clone();
    assert_eq!(events[0]["start"], json!("2026-08-10T09:00:00Z"), "timed start flattened");
    assert_eq!(events[1]["start"], json!("2026-08-12"), "all-day start flattened");
    Ok(())
}

async fn live_list(rig: LiveRig) -> WeftResult<()> {
    // Read-only: whatever the window holds, count and events agree.
    let outcome = rig
        .run(
            &GoogleCalendarListEventsNode,
            json!({
                "account": rig.access("google"),
                "timeMin": "2000-01-01T00:00:00Z",
                "timeMax": "2000-01-08T00:00:00Z",
            }),
        )
        .await
        .ok()?;
    let events = outcome.output("events")?.as_array().expect("events list").len();
    let count = outcome.output("count")?.as_f64().expect("count");
    assert_eq!(events as f64, count, "count matches the listed events");
    Ok(())
}
