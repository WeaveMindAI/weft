//! Wakes on Google Cloud: one Cloud Tasks task per wake.
//!
//! The task is named after the wake (`Wake::name`), so setting the same
//! wake twice makes one task; Cloud Tasks answers the second with "already
//! exists". At its time it posts the wake to the role, signed as the
//! install's core service account. A role on the machine is reached at the
//! machine's front door (`<public url>/_internal/<role>`), since the
//! machine's internal port is not on the internet; a serverless role at
//! its own service.
//!
//! Cloud Tasks refuses a task more than 30 days ahead, so a wake further
//! out is delivered in hops (`hop`): the task goes off early, at the wake's
//! time minus a whole number of `HORIZON`s, still carrying the wake's own
//! time. The receiver sees a call before its time and sets the same wake
//! again, which lands one hop closer under a new task name.
//!
//! Cloud Tasks remembers a used name for a while after delivering it, so
//! a wake set again for a moment already delivered (its signal was parked
//! when it went off, and has just come back) answers "already exists"
//! and would never go off. A wake whose moment has passed is therefore
//! set under a name of its own when its plain name is taken.

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::json;
use weft_platform_traits::config::GcpPlatform;
use weft_platform_traits::{Alarm, RoleAddresses, Wake};

use crate::api::{is_status, Google};

pub struct CloudTasksAlarm {
    google: Google,
    gcp: GcpPlatform,
    /// Where Cloud Tasks reaches each role.
    targets: RoleAddresses,
}

impl CloudTasksAlarm {
    pub fn new(google: Google, gcp: GcpPlatform, targets: RoleAddresses) -> Self {
        Self { google, gcp, targets }
    }

    fn queue(&self) -> String {
        format!("projects/{}/locations/{}/queues/{}", self.gcp.project, self.gcp.region, self.gcp.tasks_queue)
    }
}

/// The furthest ahead one task is set: under Cloud Tasks' 30-day limit,
/// with a day to spare for the clock of whoever sets it.
const HORIZON_MS: i64 = 29 * 24 * 3600 * 1000;

/// How far past the clock the hops are counted from. A hop goes off at
/// the wake's time minus a whole number of `HORIZON_MS`, and the receiver
/// sets the wake again right then; with its clock a hair behind the one
/// the hop was counted on, counting from its bare clock would land on the
/// same hop, the same task name, which Cloud Tasks drops as already set,
/// and the wake would be lost. Counting from an hour ahead puts any
/// receiver within an hour of the hop past it, and costs the first setter
/// at most an hour more than `HORIZON_MS`, still inside the 30 days.
const HOP_SLACK_MS: i64 = 3600 * 1000;

/// One task for a wake: when it goes off and its name.
#[derive(Debug, PartialEq, Eq)]
struct Planned {
    deliver_ms: i64,
    name: String,
}

/// The task for `wake`, set at `now_ms`: the wake itself when it is
/// within `HORIZON_MS`, otherwise the hop that still leaves `left`
/// horizons to go. The hop times are counted back from the wake's own
/// time, so everyone setting the same wake inside one hop picks the same
/// task, and the receiver setting it again at a hop gets the next one.
fn plan(wake: &Wake, now_ms: i64) -> Planned {
    let ahead = wake.at_unix_ms - now_ms - HOP_SLACK_MS;
    if ahead <= HORIZON_MS {
        return Planned { deliver_ms: wake.at_unix_ms, name: wake.name() };
    }
    let left = (ahead + HORIZON_MS - 1) / HORIZON_MS - 1;
    // A hop has a name of its own: Cloud Tasks keeps a used name for
    // days, so the final task must not share it.
    Planned { deliver_ms: wake.at_unix_ms - left * HORIZON_MS, name: format!("{}-h{left}", wake.name()) }
}

/// The task for a wake whose moment has passed and whose plain name is
/// taken: delivered now, named after the moment it is set at.
fn late(wake: &Wake, now_ms: i64) -> Planned {
    Planned { deliver_ms: now_ms, name: format!("{}-late-{now_ms}", wake.name()) }
}

/// The Cloud Tasks task for `wake` in `queue`, delivered to `base` as
/// `planned` says.
fn task_body(queue: &str, wake: &Wake, planned: &Planned, base: &str, account: &str) -> anyhow::Result<serde_json::Value> {
    let at = chrono::DateTime::from_timestamp_millis(planned.deliver_ms)
        .ok_or_else(|| anyhow::anyhow!("a wake at {} ms is not a time", wake.at_unix_ms))?;
    let body = serde_json::to_vec(&wake.call())?;
    let name = &planned.name;
    Ok(json!({
        "task": {
            "name": format!("{queue}/tasks/{name}"),
            "scheduleTime": at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            "httpRequest": {
                "url": format!("{base}{}", wake.path),
                "httpMethod": "POST",
                "headers": { "Content-Type": "application/json" },
                "body": base64::engine::general_purpose::STANDARD.encode(body),
                "oidcToken": { "serviceAccountEmail": account, "audience": base },
            },
        }
    }))
}

#[async_trait]
impl Alarm for CloudTasksAlarm {
    async fn set(&self, wake: Wake) -> anyhow::Result<()> {
        let queue = self.queue();
        let base = self.targets.of(wake.role)?;
        let account = &self.gcp.core_service_account;
        let url = format!("https://cloudtasks.googleapis.com/v2/{queue}/tasks");
        let now_ms = chrono::Utc::now().timestamp_millis();
        let planned = plan(&wake, now_ms);
        match self.google.post(&url, &task_body(&queue, &wake, &planned, base, account)?).await {
            Ok(_) => Ok(()),
            // Taken, for a moment still ahead: that task is this wake.
            Err(e) if is_status(&e, 409) && planned.deliver_ms > now_ms => Ok(()),
            // Taken, for a moment already past: the task under that name
            // may have gone off already, so it cannot stand for this one.
            Err(e) if is_status(&e, 409) => {
                let late = late(&wake, now_ms);
                match self.google.post(&url, &task_body(&queue, &wake, &late, base, account)?).await {
                    Ok(_) => Ok(()),
                    // Set in this same millisecond by another setter.
                    Err(e) if is_status(&e, 409) => Ok(()),
                    Err(e) => Err(e.context(format!("set the late wake {}", wake.key))),
                }
            }
            Err(e) => Err(e.context(format!("set the wake {}", wake.key))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_platform_traits::CoreRole;

    fn wake_at(at_unix_ms: i64) -> Wake {
        Wake {
            key: "signal:t".into(),
            at_unix_ms,
            role: CoreRole::Listener,
            path: "/wake".into(),
            body: json!({ "token": "t" }),
        }
    }

    #[test]
    fn a_task_is_named_after_its_wake_and_signed_for_its_role() {
        let wake = wake_at(1_700_000_000_123);
        let planned = plan(&wake, 1_700_000_000_000);
        let body = task_body("projects/a/locations/r/queues/q", &wake, &planned, "https://1.2.3.4/_internal/listener", "core@a").unwrap();
        let t = &body["task"];
        assert_eq!(t["name"], format!("projects/a/locations/r/queues/q/tasks/{}", wake.name()));
        assert_eq!(t["scheduleTime"], "2023-11-14T22:13:20.123Z");
        assert_eq!(t["httpRequest"]["url"], "https://1.2.3.4/_internal/listener/wake");
        assert_eq!(t["httpRequest"]["oidcToken"]["audience"], "https://1.2.3.4/_internal/listener");
        let sent = base64::engine::general_purpose::STANDARD.decode(t["httpRequest"]["body"].as_str().unwrap()).unwrap();
        let call: serde_json::Value = serde_json::from_slice(&sent).unwrap();
        assert_eq!(call["at_unix_ms"], 1_700_000_000_123i64);
        assert_eq!(call["body"]["token"], "t");
    }

    #[test]
    fn a_wake_past_the_horizon_hops_toward_its_time() {
        let now = 1_700_000_000_000;
        let near = wake_at(now + 1000);
        assert_eq!(plan(&near, now), Planned { deliver_ms: now + 1000, name: near.name() });
        let at = now + 70 * 24 * 3600 * 1000;
        let wake = wake_at(at);
        let first = plan(&wake, now);
        assert_eq!(first.name, format!("{}-h2", wake.name()));
        assert!(first.deliver_ms - now <= HORIZON_MS + HOP_SLACK_MS && first.deliver_ms > now);
        // Set again when the first hop goes off (a little late): the next hop.
        let second = plan(&wake, first.deliver_ms + 500);
        assert_eq!(second, Planned { deliver_ms: at - HORIZON_MS, name: format!("{}-h1", wake.name()) });
        assert_eq!(plan(&wake, second.deliver_ms + 500), Planned { deliver_ms: at, name: wake.name() });
        // Two setters inside one hop pick the same task.
        assert_eq!(plan(&wake, now + 3_600_000), plan(&wake, now));
    }

    /// The receiver of a hop whose clock runs behind the one the hop was
    /// counted on still moves to the next hop: re-deriving the same hop
    /// would rebuild the same task name, which Cloud Tasks drops as
    /// already set, and the wake would be lost.
    #[test]
    fn a_receiver_whose_clock_is_behind_still_moves_to_the_next_hop() {
        let now = 1_700_000_000_000;
        let wake = wake_at(now + 70 * 24 * 3600 * 1000);
        let first = plan(&wake, now);
        for behind_ms in [1, 1000, 60_000, HOP_SLACK_MS - 1] {
            let again = plan(&wake, first.deliver_ms - behind_ms);
            assert_ne!(again.name, first.name, "{behind_ms} ms behind");
            assert!(again.deliver_ms > first.deliver_ms);
        }
    }

    /// A wake set again after its moment passed never shares the plain
    /// name, which a delivered task may still hold.
    #[test]
    fn a_late_wake_has_a_name_of_its_own() {
        let now = 1_700_000_000_000;
        let wake = wake_at(now - 5000);
        assert_eq!(plan(&wake, now).name, wake.name());
        let late = late(&wake, now);
        assert_eq!(late.deliver_ms, now);
        assert_ne!(late.name, wake.name());
        assert!(late.name.len() <= 100);
    }
}
