//! Which revision of a role's Cloud Run service its requests go to, as
//! Cloud Run says in the service's traffic status.

use anyhow::Context as _;
use async_trait::async_trait;
use serde_json::Value;
use weft_platform_traits::{Routing, RoutingRefused, ServiceRevisions};

use crate::api::Google;

const RUN: &str = "https://run.googleapis.com/v2";

/// The traffic allocation that follows the service's latest ready
/// revision, rather than one named revision.
const LATEST: &str = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST";

pub struct CloudRunRevisions {
    google: Google,
    /// `projects/<p>/locations/<r>/services/<n>`.
    service: String,
}

impl CloudRunRevisions {
    pub fn new(google: Google, service: String) -> Self {
        Self { google, service }
    }
}

#[async_trait]
impl ServiceRevisions for CloudRunRevisions {
    async fn routing(&self) -> anyhow::Result<Routing> {
        // Refused (no permission to read it) or not found (a wrong name):
        // asking again answers the same.
        let service = self.google.get(&format!("{RUN}/{}", self.service)).await.map_err(|e| match crate::api::is_status(&e, 403) || crate::api::is_status(&e, 404) {
            true => anyhow::Error::new(RoutingRefused(format!("read the Cloud Run service {}: {e:#}", self.service))),
            false => e.context(format!("read the Cloud Run service {}", self.service)),
        })?;
        routing_of(&service).with_context(|| format!("read where the Cloud Run service {} sends its requests", self.service))
    }
}

/// A revision's own name, as `K_REVISION` holds it: Cloud Run names one
/// by its full resource name in some fields and by its own in others.
fn own_name(name: &str) -> &str {
    name.rsplit('/').next().unwrap_or(name)
}

/// Where a service sends its requests, from Cloud Run's description of
/// it: the revisions its traffic status gives a share to (the share that
/// follows the latest names that revision, or the latest ready one), and
/// the latest revision made, while the service follows its latest and
/// that one receives nothing yet. Pure.
fn routing_of(service: &Value) -> anyhow::Result<Routing> {
    let named = |field: &str| service.get(field).and_then(Value::as_str).filter(|n| !n.is_empty()).map(own_name);
    let latest_ready = named("latestReadyRevision");
    let shares = |field: &str| service.get(field).and_then(Value::as_array).cloned().unwrap_or_default();
    let has_share = |target: &Value| target.get("percent").and_then(Value::as_u64).unwrap_or(0) > 0;
    let follows_latest = |target: &Value| target.get("type").and_then(Value::as_str) == Some(LATEST);

    let mut serving: Vec<String> = Vec::new();
    for target in shares("trafficStatuses").iter().filter(|t| has_share(t)) {
        let revision = match target.get("revision").and_then(Value::as_str).filter(|n| !n.is_empty()) {
            Some(revision) => own_name(revision),
            None if follows_latest(target) => latest_ready.context("the service's traffic follows its latest ready revision, and it names none")?,
            None => anyhow::bail!("the service gives a share of its traffic to a revision it does not name: {target}"),
        };
        if !serving.iter().any(|s| s == revision) {
            serving.push(revision.to_string());
        }
    }
    anyhow::ensure!(!serving.is_empty(), "the service's traffic status gives no revision any share");
    let incoming = match shares("traffic").iter().any(|t| has_share(t) && follows_latest(t)) {
        true => named("latestCreatedRevision").filter(|latest| !serving.iter().any(|s| s == latest)).map(str::to_string),
        false => None,
    };
    Ok(Routing { serving, incoming })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const P: &str = "projects/p/locations/r/services/weft-role-dispatcher/revisions/";

    fn routing(serving: &[&str], incoming: Option<&str>) -> Routing {
        Routing { serving: serving.iter().map(|s| s.to_string()).collect(), incoming: incoming.map(str::to_string) }
    }

    /// A service that follows its latest revision: once that one is ready,
    /// it is the one serving; while it starts, the older one still serves
    /// and the new one is incoming.
    #[test]
    fn a_service_following_its_latest_serves_it_once_ready() {
        let settled = json!({
            "traffic": [{ "type": LATEST, "percent": 100 }],
            "trafficStatuses": [{ "type": LATEST, "percent": 100 }],
            "latestReadyRevision": format!("{P}d-00005"),
            "latestCreatedRevision": format!("{P}d-00005"),
        });
        assert_eq!(routing_of(&settled).unwrap(), routing(&["d-00005"], None));

        let rolling = json!({
            "traffic": [{ "type": LATEST, "percent": 100 }],
            "trafficStatuses": [{ "type": LATEST, "percent": 100, "revision": "d-00004" }],
            "latestReadyRevision": format!("{P}d-00004"),
            "latestCreatedRevision": format!("{P}d-00005"),
        });
        assert_eq!(routing_of(&rolling).unwrap(), routing(&["d-00004"], Some("d-00005")));
    }

    /// Traffic pinned to a named revision (a rollback to an older one)
    /// serves that one, and nothing is incoming, however new the latest.
    #[test]
    fn a_pinned_revision_serves_and_nothing_is_incoming() {
        let pinned = json!({
            "traffic": [
                { "type": "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION", "revision": "d-00004", "percent": 100 },
                { "type": LATEST },
            ],
            "trafficStatuses": [
                { "type": "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION", "revision": "d-00004", "percent": 100 },
                { "type": LATEST, "revision": "d-00005" },
            ],
            "latestReadyRevision": format!("{P}d-00005"),
            "latestCreatedRevision": format!("{P}d-00005"),
        });
        assert_eq!(routing_of(&pinned).unwrap(), routing(&["d-00004"], None));
    }

    /// Traffic split between two revisions serves both.
    #[test]
    fn a_split_serves_every_revision_with_a_share() {
        let split = json!({
            "traffic": [
                { "type": "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION", "revision": "d-00004", "percent": 50 },
                { "type": "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION", "revision": "d-00005", "percent": 50 },
            ],
            "trafficStatuses": [
                { "type": "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION", "revision": "d-00004", "percent": 50 },
                { "type": "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION", "revision": "d-00005", "percent": 50 },
            ],
            "latestReadyRevision": format!("{P}d-00005"),
        });
        assert_eq!(routing_of(&split).unwrap(), routing(&["d-00004", "d-00005"], None));
    }

    /// A description that names no serving revision is an error, never a
    /// routing that replaces everyone.
    #[test]
    fn a_service_naming_no_serving_revision_is_an_error() {
        assert!(routing_of(&json!({})).is_err());
        assert!(routing_of(&json!({ "trafficStatuses": [{ "type": LATEST, "percent": 100 }] })).is_err());
    }
}
