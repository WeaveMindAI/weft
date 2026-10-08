//! What the door asks the broker: its project's triggers, a run's facts,
//! the broker's word on a caller or an instance token, the other copies'
//! counts, and a fire put in its trigger's queue.
//!
//! The triggers and the run facts are kept in memory and dropped the
//! moment the broker's line says they changed, so a call reaches the
//! broker only for what is checked per call (a gated route's caller, an
//! instance token). The triggers are kept compiled: by token for an event,
//! and as the route table a call is matched against, its patterns parsed
//! once per copy. Each copy carries a version, read off what it holds,
//! which a run's plan is keyed by (`crate::plan`): a change makes the next
//! call build its plan again, and reading the same thing again (a refusal
//! confirmed against the broker) keeps every plan.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use weft_broker_client::line::{ACCESS_CHANNEL, INFRA_STATUS_CHANNEL, TRIGGERS_CHANNEL};
use weft_broker_client::protocol::{
    ArmedEntry, CallerVerified, CallerVerifyRequest, DoorEntry, DoorParked, DoorRunFacts, DoorTick, DoorTickRequest, DoorTrigger,
};
use weft_core::instance::InstanceId;
use weft_task_store::held_copy::{Changed, HeldCopy};

/// See the module doc. The broker's own answers, behind a trait so the
/// door's rules are tested against a fake.
#[async_trait]
pub trait DoorBroker: Send + Sync {
    /// The project's triggers as this worker holds them.
    async fn triggers(&self) -> anyhow::Result<Arc<Triggers>>;
    /// The triggers as the broker answers now: what a refusal of an event
    /// is confirmed against, since a trigger armed or taken over a moment
    /// ago may not have been heard.
    async fn fresh_triggers(&self) -> anyhow::Result<Arc<Triggers>>;
    /// A run's facts for `instance` as this worker holds them.
    async fn run_facts(&self, instance: Option<&InstanceId>) -> anyhow::Result<Arc<RunFacts>>;
    /// [`Self::run_facts`] as the broker answers now.
    async fn fresh_run_facts(&self, instance: Option<&InstanceId>) -> anyhow::Result<Arc<RunFacts>>;
    /// What a caller of a gated route proved, `None` when refused.
    async fn verify_caller(&self, request: &CallerVerifyRequest) -> anyhow::Result<Option<CallerVerified>>;
    /// The instance an instance token names in this project, `None` when it
    /// names nobody here.
    async fn instance_token(&self, token: &str) -> anyhow::Result<Option<InstanceId>>;
    async fn tick(&self, request: &DoorTickRequest) -> anyhow::Result<DoorTick>;
    /// Put a fire in its trigger's queue.
    async fn park_fire(&self, token: &str, fire: &weft_task_store::parked_fires::Waiting, held_by: Option<&str>) -> anyhow::Result<DoorParked>;
}

/// The version of a copy holding `answer`: read off what it holds, so two
/// reads of the same thing share one, and any change makes another.
fn version_of(answer: &impl serde::Serialize) -> u64 {
    let value = serde_json::to_value(answer).expect("a broker answer serializes");
    let digest = weft_core::project::hash::sha256_hex(weft_core::project::hash::canonical_json(&value).as_bytes());
    u64::from_str_radix(&digest[..16], 16).expect("a sha256 digest is hex")
}

/// One trigger of the project, as the door holds it.
#[derive(Debug)]
pub struct HeldTrigger {
    pub token: String,
    /// Its place, spelled the way the program reads it.
    pub node_id: String,
    pub entry: HeldEntry,
    /// The holder serving it, for a signal a holder holds.
    pub held_by: Option<String>,
    /// How its callers are served (`weft_core::signal::live_connection`),
    /// read once from its armed spec: `None` for one not armed, `Err` for
    /// one whose kind serves no caller or whose settings do not parse.
    pub caller: Option<Result<(weft_core::signal::Protocol, weft_core::signal::live_connection::LiveConnectionConfig), String>>,
}

impl HeldTrigger {
    pub fn new(token: String, node_id: String, entry: HeldEntry, held_by: Option<String>) -> Self {
        let caller = match &entry {
            HeldEntry::Armed(armed) => Some(weft_core::signal::live_connection(&armed.spec)),
            HeldEntry::Unservable { .. } => None,
        };
        Self { token, node_id, entry, held_by, caller }
    }
}

/// A trigger's entry as armed, or why work for it cannot be served.
#[derive(Debug)]
pub enum HeldEntry {
    Armed(Arc<ArmedEntry>),
    Unservable { status: u16, why: String },
}

/// The project's triggers, compiled (see the module doc).
#[derive(Debug, Default)]
pub struct Triggers {
    /// This copy's own version.
    pub version: u64,
    by_token: HashMap<String, Arc<HeldTrigger>>,
    /// The routes among them, their patterns parsed.
    routes: Vec<(weft_core::route::RouteKey, Arc<HeldTrigger>)>,
}

impl Triggers {
    /// Compile the broker's answer. A stored route pattern that no longer
    /// parses leaves its route unreachable, said in the log.
    pub fn of(answer: Vec<DoorTrigger>) -> Self {
        let version = version_of(&answer);
        let mut by_token = HashMap::with_capacity(answer.len());
        let mut routes = Vec::new();
        for trigger in answer {
            let entry = match trigger.entry {
                DoorEntry::Armed(entry) => HeldEntry::Armed(Arc::from(entry)),
                DoorEntry::Unservable { status, why } => HeldEntry::Unservable { status, why },
            };
            let held = Arc::new(HeldTrigger::new(trigger.token, trigger.node_id, entry, trigger.held_by));
            if let Some(mount) = trigger.route {
                match weft_core::route::RoutePattern::parse(&mount.pattern) {
                    Ok(pattern) => routes.push((weft_core::route::RouteKey { pattern, methods: mount.methods }, held.clone())),
                    Err(e) => tracing::error!(target: "weft_engine::door", token = %held.token, pattern = %mount.pattern, error = %e, "a stored route pattern no longer parses; the route is unreachable"),
                }
            }
            by_token.insert(held.token.clone(), held);
        }
        Self { version, by_token, routes }
    }

    /// The trigger whose signal token is `token`.
    pub fn get(&self, token: &str) -> Option<&Arc<HeldTrigger>> {
        self.by_token.get(token)
    }

    /// Every trigger's token: what the door's counts are stated for.
    pub fn tokens(&self) -> impl Iterator<Item = &String> {
        self.by_token.keys()
    }

    /// The route serving `method` on `path`: the most specific pattern wins
    /// (`weft_core::route::find_route`).
    pub fn route(&self, method: &str, path: &str) -> weft_core::route::RouteMatch<&Arc<HeldTrigger>> {
        weft_core::route::find_route(self.routes.iter().map(|(key, trigger)| (key, trigger)), method, path)
    }
}

/// A run's facts for one instance (or for nobody), as the door holds them,
/// with the version of this copy (see the module doc).
#[derive(Debug, Default)]
pub struct RunFacts {
    pub version: u64,
    pub facts: DoorRunFacts,
}

impl RunFacts {
    pub fn of(facts: DoorRunFacts) -> Self {
        Self { version: version_of(&facts), facts }
    }
}

/// The broker's answers, kept (see the module doc).
pub struct HeldDoorBroker {
    client: Arc<weft_broker_client::BrokerDoorClient>,
    project_id: uuid::Uuid,
    triggers: Arc<HeldCopy<(), Triggers>>,
    /// A run's facts by instance, dropped when its infra or its
    /// connections change.
    run_facts: Arc<HeldCopy<Option<InstanceId>, RunFacts>>,
}

/// How many instances' run facts a worker keeps.
const KEPT: usize = 1024;

impl HeldDoorBroker {
    pub fn new(
        client: Arc<weft_broker_client::BrokerDoorClient>,
        line: Arc<dyn crate::context::WorkerLine>,
        project_id: uuid::Uuid,
    ) -> Arc<Self> {
        Arc::new(Self {
            triggers: HeldCopy::following(line.subscribe(), TRIGGERS_CHANNEL, 1, everything, |_| true),
            run_facts: HeldCopy::following_any(line.subscribe(), &[INFRA_STATUS_CHANNEL, ACCESS_CHANNEL], KEPT, everything, |_| true),
            client,
            project_id,
        })
    }

    async fn read_triggers(&self) -> anyhow::Result<Triggers> {
        Ok(Triggers::of(self.client.triggers().await?.triggers))
    }

    async fn read_run_facts(&self, instance: Option<&InstanceId>) -> anyhow::Result<RunFacts> {
        Ok(RunFacts::of(self.client.run_facts(instance).await?))
    }
}

/// Every notification a copy hears concerns this worker's own project or
/// tenant (the broker pushes no other), so any one drops the whole copy.
fn everything<K, V>(_payload: &str) -> Changed<K, V> {
    Changed::Everything
}

#[async_trait]
impl DoorBroker for HeldDoorBroker {
    async fn triggers(&self) -> anyhow::Result<Arc<Triggers>> {
        self.triggers.get_or_load((), || self.read_triggers()).await
    }

    async fn fresh_triggers(&self) -> anyhow::Result<Arc<Triggers>> {
        self.triggers.load_fresh((), || self.read_triggers()).await
    }

    async fn run_facts(&self, instance: Option<&InstanceId>) -> anyhow::Result<Arc<RunFacts>> {
        self.run_facts.get_or_load(instance.cloned(), || self.read_run_facts(instance)).await
    }

    async fn fresh_run_facts(&self, instance: Option<&InstanceId>) -> anyhow::Result<Arc<RunFacts>> {
        self.run_facts.load_fresh(instance.cloned(), || self.read_run_facts(instance)).await
    }

    async fn verify_caller(&self, request: &CallerVerifyRequest) -> anyhow::Result<Option<CallerVerified>> {
        self.client.verify_caller(request).await
    }

    async fn instance_token(&self, token: &str) -> anyhow::Result<Option<InstanceId>> {
        Ok(self
            .client
            .instance_token(token)
            .await?
            .filter(|named| named.project_id == self.project_id)
            .map(|named| named.instance))
    }

    async fn tick(&self, request: &DoorTickRequest) -> anyhow::Result<DoorTick> {
        self.client.tick(request).await
    }

    async fn park_fire(&self, token: &str, fire: &weft_task_store::parked_fires::Waiting, held_by: Option<&str>) -> anyhow::Result<DoorParked> {
        self.client.park_fire(token, fire, held_by).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trigger(token: &str, pattern: Option<&str>) -> DoorTrigger {
        DoorTrigger {
            token: token.into(),
            node_id: token.into(),
            route: pattern.map(|pattern| weft_broker_client::protocol::DoorMount { pattern: pattern.into(), methods: Vec::new() }),
            entry: DoorEntry::Unservable { status: 428, why: "not armed".into() },
            held_by: None,
        }
    }

    /// Every trigger is found by its token, the routes among them by their
    /// path, and a copy's version follows what it holds.
    #[test]
    fn a_copy_finds_triggers_by_token_and_routes_by_path() {
        let first = Triggers::of(vec![trigger("tick", None), trigger("users", Some("users/{id}"))]);
        assert!(first.get("tick").is_some() && first.get("users").is_some());
        match first.route("GET", "users/7") {
            weft_core::route::RouteMatch::Found { route, params } => {
                assert_eq!(route.token, "users");
                assert_eq!(params.get("id").map(String::as_str), Some("7"));
            }
            other => panic!("the route: {other:?}"),
        }
        assert!(matches!(first.route("GET", "tick"), weft_core::route::RouteMatch::NotFound), "a timer is no route");
        assert_ne!(first.version, Triggers::of(Vec::new()).version);
        let again = Triggers::of(vec![trigger("tick", None), trigger("users", Some("users/{id}"))]);
        assert_eq!(first.version, again.version, "the same triggers read again keep their plans");
    }
}
