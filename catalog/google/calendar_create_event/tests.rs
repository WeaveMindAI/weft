//! GoogleCalendarCreateEvent self-tests: the timed/all-day shape pick
//! and the emitted identifiers.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::GoogleCalendarCreateEventNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("a_timed_event_sends_datetimes", timed_event),
        NodeTest::fake("a_bare_date_becomes_all_day", all_day_event),
        NodeTest::live("one_real_event_then_deleted", "google", live_event),
    ]
}

/// Create one short event on the primary calendar via the node, then
/// delete it through the test's own connection so repeated runs never
/// pile events onto the calendar.
async fn live_event(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &GoogleCalendarCreateEventNode,
            json!({
                "account": rig.access("google"),
                "summary": "weft-node-tests",
                "start": "2030-01-01T09:00:00Z",
                "end": "2030-01-01T09:15:00Z",
            }),
        )
        .await
        .ok()?;
    // The id IS the delete key, so an empty one makes the delete fail
    // loudly on its own; a guard between create and delete would only
    // risk leaking the event it names.
    let id = outcome.output("eventId")?.as_str().expect("event id").to_string();
    let conn = rig.connect().await?;
    crate::testing::delete(
        &conn,
        &format!("https://www.googleapis.com/calendar/v3/calendars/primary/events/{id}"),
        "delete the test event",
    )
    .await
}

async fn timed_event(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/calendar/v3/calendars/primary/events",
        json!({ "id": "ev1", "htmlLink": "https://calendar.google.com/event?eid=ev1" }),
    );
    let outcome = rig
        .run(
            &GoogleCalendarCreateEventNode,
            json!({
                "account": rig.access("google"),
                "summary": "Standup",
                "start": "2026-08-10T09:00:00+02:00",
                "end": "2026-08-10T09:15:00+02:00",
                "attendees": ["a@example.com"],
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["eventId"], json!("ev1"));
    let body = rig.requests()[0].body.clone().expect("event body");
    assert_eq!(body["start"], json!({ "dateTime": "2026-08-10T09:00:00+02:00" }));
    assert_eq!(body["attendees"], json!([{ "email": "a@example.com" }]));
    Ok(())
}

async fn all_day_event(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/calendar/v3/calendars/primary/events",
        json!({ "id": "ev2", "htmlLink": "l" }),
    );
    rig.run(
        &GoogleCalendarCreateEventNode,
        json!({
            "account": rig.access("google"),
            "summary": "Holiday",
            "start": "2026-08-10",
            "end": "2026-08-11",
        }),
    )
    .await
    .ok()?;
    let body = rig.requests()[0].body.clone().expect("event body");
    assert_eq!(body["start"], json!({ "date": "2026-08-10" }), "a bare date is all-day");
    Ok(())
}
