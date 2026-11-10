//! Wakes on Google Cloud: one Cloud Tasks task per wake.
//!
//! The task is named after the wake (`Wake::name`), in the queue its kind
//! goes through (`WakeQueue`): what keeps a user's triggers going in the
//! install's wakes queue, retried with no time limit (its receiver gives
//! up after `GcpPlatform::wake_attempts` deliveries it answered, read off
//! the task's execution count), weft's own looks in its ticks queue,
//! retried briefly. At
//! its time the task posts the wake to the
//! role, signed as the install's core service account. A role on the
//! machine is reached at the machine's front door (`<public url>/_internal/<role>`),
//! since the machine's internal port is not on the internet; a serverless
//! role at its own service.
//!
//! Cloud Tasks refuses a task more than 30 days ahead, so a wake further
//! out is delivered in hops (`hop`): the task goes off early, at the wake's
//! time minus a whole number of `HORIZON`s, still carrying the wake's own
//! time. The receiver sees a call before its time and sets the wake again
//! (a booking of its own), which lands one hop closer.
//!
//! Cloud Tasks keeps a used name for a while after the task was delivered
//! or deleted, and refuses a new task under it. Every setter names each of
//! its bookings apart (`Wake::generation`), so a name is only ever set
//! again by the same booking: "already exists" means this wake is set (an
//! earlier try whose answer was lost, or the same booking armed twice),
//! and is read as set.
//!
//! Cancelling deletes the wake's task by name, and the names its hops can
//! have around now. A task already delivered, or never made, answers 404,
//! which is what a cancel wants.

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::json;
use weft_platform_traits::config::GcpPlatform;
use weft_platform_traits::{Alarm, RoleAddresses, Wake, WakeQueue};

use crate::api::{is_status, Google};

/// Where Cloud Tasks' API is.
const CLOUD_TASKS: &str = "https://cloudtasks.googleapis.com/v2";

/// How long Cloud Tasks waits for the receiver of one of weft's own wakes
/// to answer before it counts the try as failed and tries again: the
/// longest it allows. A role's pass can run a long while (the supervisor's
/// pass answers only once every command it runs has ended, and a slow
/// drain takes minutes), and a try cut short is delivered again into a
/// second pass beside the first. A user's wakes keep Cloud Tasks' own
/// default.
const INTERNAL_DISPATCH_DEADLINE: &str = "1800s";

pub struct CloudTasksAlarm {
    google: Google,
    gcp: GcpPlatform,
    /// Where Cloud Tasks reaches each role.
    targets: RoleAddresses,
    /// Cloud Tasks' API root ([`CLOUD_TASKS`], or a test's own server).
    api: String,
}

impl CloudTasksAlarm {
    pub fn new(google: Google, gcp: GcpPlatform, targets: RoleAddresses) -> Self {
        Self { google, gcp, targets, api: CLOUD_TASKS.to_string() }
    }

    fn queue(&self, queue: WakeQueue) -> String {
        let name = match queue {
            WakeQueue::Triggers => &self.gcp.tasks_queue,
            WakeQueue::Internal => &self.gcp.ticks_queue,
        };
        format!("projects/{}/locations/{}/queues/{name}", self.gcp.project, self.gcp.region)
    }
}

/// The furthest ahead one task is set: under Cloud Tasks' 30-day limit,
/// with a day to spare for the clock of whoever sets it.
const HORIZON_MS: i64 = 29 * 24 * 3600 * 1000;

/// How far past the clock the hops are counted from. A hop goes off at
/// the wake's time minus a whole number of `HORIZON_MS`, and the receiver
/// sets the wake again right then; with its clock a hair behind the one
/// the hop was counted on, counting from its bare clock would land on the
/// same hop, the same task name. Counting from an hour ahead puts any
/// receiver within an hour of the hop past it, and costs the first setter
/// at most an hour more than `HORIZON_MS`, still inside the 30 days.
const HOP_SLACK_MS: i64 = 3600 * 1000;

/// One task for a wake: when it goes off and its name.
#[derive(Debug, PartialEq, Eq)]
struct Planned {
    deliver_ms: i64,
    name: String,
}

/// How many horizons a wake at `at_ms` set at `now_ms` still has to hop
/// through, 0 when its own task can be set.
fn hops_left(at_ms: i64, now_ms: i64) -> i64 {
    let ahead = at_ms - now_ms - HOP_SLACK_MS;
    if ahead <= HORIZON_MS {
        return 0;
    }
    (ahead + HORIZON_MS - 1) / HORIZON_MS - 1
}

/// The name of the hop that still leaves `left` horizons to go.
fn hop_name(wake: &Wake, left: i64) -> String {
    format!("{}-h{left}", wake.name())
}

/// The task for `wake`, set at `now_ms`: the wake itself when it is
/// within `HORIZON_MS`, otherwise the hop that still leaves `left`
/// horizons to go. The hop times are counted back from the wake's own
/// time, so everyone setting the same wake inside one hop picks the same
/// task, and the receiver setting it again at a hop gets the next one.
fn plan(wake: &Wake, now_ms: i64) -> Planned {
    match hops_left(wake.at_unix_ms, now_ms) {
        0 => Planned { deliver_ms: wake.at_unix_ms, name: wake.name() },
        // A hop has a name of its own, so the final task never shares it.
        left => Planned { deliver_ms: wake.at_unix_ms - left * HORIZON_MS, name: hop_name(wake, left) },
    }
}

/// Every name a task for `wake` can have while it waits, seen at `now_ms`:
/// its own, and the hops around now (the one a setter at `now_ms` makes,
/// and the one before it, set while the wake was further out).
fn task_names(wake: &Wake, now_ms: i64) -> Vec<String> {
    let left = hops_left(wake.at_unix_ms, now_ms);
    let mut names = vec![wake.name()];
    for l in [left, left + 1].into_iter().filter(|l| *l > 0) {
        names.push(hop_name(wake, l));
    }
    names
}

/// The Cloud Tasks task for `wake` in `queue`, delivered to `base` as
/// `planned` says.
fn task_body(queue: &str, wake: &Wake, planned: &Planned, base: &str, account: &str) -> anyhow::Result<serde_json::Value> {
    let at = chrono::DateTime::from_timestamp_millis(planned.deliver_ms)
        .ok_or_else(|| anyhow::anyhow!("a wake at {} ms is not a time", wake.at_unix_ms))?;
    let body = serde_json::to_vec(&wake.call())?;
    let name = &planned.name;
    let mut task = json!({
        "name": format!("{queue}/tasks/{name}"),
        "scheduleTime": at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        "httpRequest": {
            "url": format!("{base}{}", wake.path),
            "httpMethod": "POST",
            "headers": { "Content-Type": "application/json" },
            "body": base64::engine::general_purpose::STANDARD.encode(body),
            "oidcToken": { "serviceAccountEmail": account, "audience": base },
        },
    });
    if wake.queue == WakeQueue::Internal {
        task["dispatchDeadline"] = json!(INTERNAL_DISPATCH_DEADLINE);
    }
    Ok(json!({ "task": task }))
}

/// The largest wake body a task carries: well under Cloud Tasks' 1 MB
/// task limit once sent as base64 (a third larger), with room for the
/// rest of the task.
const LARGEST_BODY: usize = 64 * 1024;

#[async_trait]
impl Alarm for CloudTasksAlarm {
    fn largest_body(&self) -> usize {
        LARGEST_BODY
    }

    async fn set(&self, wake: Wake) -> anyhow::Result<()> {
        let queue = self.queue(wake.queue);
        let base = self.targets.of(wake.role)?;
        let account = &self.gcp.core_service_account;
        let url = format!("{}/{queue}/tasks", self.api);
        let now_ms = chrono::Utc::now().timestamp_millis();
        let planned = plan(&wake, now_ms);
        match self.google.post(&url, &task_body(&queue, &wake, &planned, base, account)?).await {
            Ok(_) => Ok(()),
            // This booking's own task, made by an earlier try or arming.
            Err(e) if is_status(&e, 409) => Ok(()),
            Err(e) => Err(e.context(format!("set the wake {}", wake.key))),
        }
    }

    async fn cancel(&self, wake: &Wake) -> anyhow::Result<()> {
        let queue = self.queue(wake.queue);
        let now_ms = chrono::Utc::now().timestamp_millis();
        for name in task_names(wake, now_ms) {
            // 404: delivered already, or never made under this name.
            self.google
                .delete(&format!("{}/{queue}/tasks/{name}", self.api))
                .await
                .map_err(|e| e.context(format!("cancel the wake {}", wake.key)))?;
        }
        Ok(())
    }

    fn answered_header(&self) -> &'static str {
        ANSWERED_HEADER
    }

    fn tried_header(&self) -> &'static str {
        TRIED_HEADER
    }

    fn attempts(&self) -> u32 {
        self.gcp.wake_attempts
    }
}

/// The header Cloud Tasks puts on each delivery of a task: how many times
/// the receiver answered it before, leaving out 5xx answers, which may
/// come from in front of the receiver
/// (`weft_platform_traits::alarm::answered_by_receiver`). Its retry count
/// ([`TRIED_HEADER`]) also counts the tries that never reached the
/// receiver, so an outage would spend them.
const ANSWERED_HEADER: &str = "x-cloudtasks-taskexecutioncount";

/// The header Cloud Tasks puts on each delivery of a task: how many times
/// it was tried before, answered or not (0 on the first). A try the
/// receiver never answered (it died mid-wake, or ran past the dispatch
/// deadline) counts here and not in [`ANSWERED_HEADER`].
const TRIED_HEADER: &str = "x-cloudtasks-taskretrycount";

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use weft_platform_traits::CoreRole;

    fn wake_at(at_unix_ms: i64) -> Wake {
        Wake {
            key: "signal:t".into(),
            at_unix_ms,
            generation: 4,
            queue: WakeQueue::Triggers,
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
        assert!(t.get("dispatchDeadline").is_none(), "a user's wake keeps Cloud Tasks' own deadline");
        let sent = base64::engine::general_purpose::STANDARD.decode(t["httpRequest"]["body"].as_str().unwrap()).unwrap();
        let call: serde_json::Value = serde_json::from_slice(&sent).unwrap();
        assert_eq!(call["at_unix_ms"], 1_700_000_000_123i64);
        assert_eq!(call["body"]["token"], "t");
        let internal = Wake { queue: WakeQueue::Internal, ..wake.clone() };
        let body = task_body("q", &internal, &plan(&internal, 0), "https://b", "core@a").unwrap();
        assert_eq!(body["task"]["dispatchDeadline"], INTERNAL_DISPATCH_DEADLINE);
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
    /// would rebuild the same task name.
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

    /// A cancel reaches every name the wake's task can have right now:
    /// its own and the hops around now.
    #[test]
    fn a_cancel_names_the_wake_and_its_hops_around_now() {
        let now = 1_700_000_000_000;
        let near = wake_at(now + 1000);
        assert_eq!(task_names(&near, now), vec![near.name(), hop_name(&near, 1)]);
        let far = wake_at(now + 70 * 24 * 3600 * 1000);
        assert_eq!(task_names(&far, now), vec![far.name(), hop_name(&far, 2), hop_name(&far, 3)]);
        assert!(task_names(&far, now).contains(&plan(&far, now).name));
    }

    /// One request a fake Cloud Tasks saw: its method and path.
    type Seen = Arc<parking_lot::Mutex<Vec<(String, String)>>>;

    /// A fake Cloud Tasks on a local port: records each request and answers
    /// the statuses in `answers` in order (then 200).
    async fn fake_cloud_tasks(answers: Vec<u16>) -> (String, Seen) {
        let seen: Seen = Arc::default();
        let recorded = seen.clone();
        let answers = parking_lot::Mutex::new(answers.into_iter());
        let api = crate::api::fake::serve(move |method, path, _| {
            recorded.lock().push((method.to_string(), path.to_string()));
            (answers.lock().next().unwrap_or(200), json!({}))
        })
        .await;
        (api, seen)
    }

    fn alarm_at(api: String) -> CloudTasksAlarm {
        let tokens = Arc::new(crate::metadata::MetadataTokens::with_access_token("test-token"));
        let gcp = GcpPlatform {
            name: "weft".into(),
            project: "p".into(),
            region: "r".into(),
            zone: "r-a".into(),
            network: "n".into(),
            subnet: "s".into(),
            artifact_registry: "a".into(),
            build_bucket: "b".into(),
            builder_service_account: "builder@p".into(),
            builder_base_image: "base".into(),
            tasks_queue: "wakes".into(),
            wake_attempts: 20,
            ticks_queue: "ticks".into(),
            news_topic: "news".into(),
            holders_topic: "holders".into(),
            core_service_account: "core@p".into(),
            holder_pool: "h".into(),
            dispatcher_service: "d".into(),
            runtime_image: "i".into(),
            deployer_service_account: "deployer@p".into(),
            frontend_service_account: "frontend@p".into(),
            workload_identity_provider: "w".into(),
            infra_network_tag: "t".into(),
            build_machine: None,
            alerts: true,
        };
        let targets = RoleAddresses {
            dispatcher: Some("https://d".into()),
            broker: "https://b".into(),
            listener: "https://l".into(),
            supervisor: "https://s".into(),
        };
        CloudTasksAlarm { google: Google::new(tokens), gcp, targets, api }
    }

    /// A delivery's try is Cloud Tasks' count of the receiver's earlier
    /// answers plus one, out of the install's `wake_attempts`; its retry
    /// count, which also counts tries that never reached the receiver,
    /// says only whether the task was delivered before.
    #[test]
    fn a_delivery_is_counted_by_cloud_tasks_execution_count_and_redelivered_by_its_retry_count() {
        let alarm = alarm_at("http://unused".into());
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::HeaderName::from_static("x-cloudtasks-taskretrycount"), "0".parse().unwrap());
        headers.insert(reqwest::header::HeaderName::from_static("x-cloudtasks-taskexecutioncount"), "0".parse().unwrap());
        let first = alarm.delivery(&headers).unwrap();
        assert_eq!((first.attempt, first.attempts, first.is_last(), first.redelivered), (1, 20, false, false));
        headers.insert(reqwest::header::HeaderName::from_static("x-cloudtasks-taskretrycount"), "40".parse().unwrap());
        let after_outage = alarm.delivery(&headers).unwrap();
        assert_eq!((after_outage.attempt, after_outage.redelivered), (1, true), "an outage's tries spend none, and still came before");
        headers.insert(reqwest::header::HeaderName::from_static("x-cloudtasks-taskexecutioncount"), "19".parse().unwrap());
        assert!(alarm.delivery(&headers).unwrap().is_last());
    }

    #[tokio::test]
    async fn a_cancel_deletes_the_wake_and_its_hop_names_and_reads_404_as_done() {
        let (api, seen) = fake_cloud_tasks(vec![404, 404]).await;
        let wake = Wake { queue: WakeQueue::Internal, ..wake_at(chrono::Utc::now().timestamp_millis() + 60_000) };
        alarm_at(api).cancel(&wake).await.expect("404 is a cancel done");
        let seen = seen.lock().clone();
        let queue = "projects/p/locations/r/queues/ticks";
        assert_eq!(
            seen,
            vec![
                ("DELETE".to_string(), format!("/{queue}/tasks/{}", wake.name())),
                ("DELETE".to_string(), format!("/{queue}/tasks/{}", hop_name(&wake, 1))),
            ]
        );
    }

    /// A taken name is this booking's own task: the wake is set, with one
    /// call and nothing read back. Any other refusal fails, loudly.
    #[tokio::test]
    async fn a_taken_name_is_this_booking_already_set() {
        let wake = wake_at(chrono::Utc::now().timestamp_millis() + 60_000);
        let (api, seen) = fake_cloud_tasks(vec![409]).await;
        alarm_at(api).set(wake.clone()).await.expect("already set");
        let seen = seen.lock().clone();
        assert_eq!(seen, vec![("POST".to_string(), "/projects/p/locations/r/queues/wakes/tasks".to_string())]);

        let (api, _) = fake_cloud_tasks(vec![403]).await;
        assert!(alarm_at(api).set(wake).await.is_err(), "a refusal is a failure");
    }
}
