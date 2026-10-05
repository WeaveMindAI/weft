//! What a worker keeps between runs: the answers that depend on its
//! project's infra and connections and not on the run asking, kept in
//! memory and dropped the moment the broker says they changed.
//!
//! A run that reads a database's address and opens its connection would
//! otherwise ask the broker every time: an infra node's own run reads its
//! endpoints, reads the connection it published, opens it, publishes it
//! again, and the node downstream opens it once more, on every call of a
//! route. None of those answers changes between calls. The broker pushes
//! every change to them down the worker's line (`weft_broker_client::line`:
//! an infra copy that changes status or address, a connection, a pick or
//! an instance value that changes), so the worker keeps each answer until
//! then (`weft_task_store::held_copy`), and keeps nothing while the line is
//! down.
//!
//! The broker stays the judge: what is kept is exactly what it answered,
//! keyed by everything its answer depended on, and a question it would
//! refuse on the run's own facts (a handle naming another instance's copy)
//! goes to it every time. Credentials the runtime supplies per firing are
//! never kept, and stored values that expire are kept only until they are
//! due a refresh (`ResolveConnectionResponse::keep_until_unix`).

use std::sync::Arc;

use async_trait::async_trait;

use weft_broker_client::line::{BrokerLink, ACCESS_CHANNEL, INFRA_STATUS_CHANNEL};
use weft_broker_client::protocol::{
    PublishAccessRequest, PublishAccessResponse, PublishedAccessRequest, PublishedAccessResponse, ResolveConnectionRequest,
    ResolveConnectionResponse,
};
use weft_core::infra::{EndpointAddress, InfraHandle};
use weft_core::instance::InstanceId;
use weft_task_store::held_copy::{Changed, HeldCopy};
use weft_task_store::InfraReader;

use crate::context::AccessBroker;

/// How many answers of each kind a worker keeps; past that the least
/// recently used go and are asked for again.
const KEPT: usize = 1024;

/// The infra reader that keeps addresses (see the module doc).
pub struct HeldInfra {
    inner: Arc<dyn InfraReader>,
    addresses: Arc<HeldCopy<InfraHandle, Option<EndpointAddress>>>,
}

impl HeldInfra {
    pub fn new(inner: Arc<dyn InfraReader>, link: &BrokerLink) -> Arc<Self> {
        let addresses = HeldCopy::following(link.subscribe(), INFRA_STATUS_CHANNEL, KEPT, everything, |_| true);
        Arc::new(Self { inner, addresses })
    }
}

#[async_trait]
impl InfraReader for HeldInfra {
    async fn endpoint_address(
        &self,
        execution_id: weft_core::ExecutionId,
        run_instance: Option<&InstanceId>,
        infra: &InfraHandle,
    ) -> anyhow::Result<Option<EndpointAddress>> {
        // A handle naming another instance's copy is the broker's to
        // refuse, every time.
        if infra.instance().is_some() && infra.instance() != run_instance {
            return self.inner.endpoint_address(execution_id, run_instance, infra).await;
        }
        let address = self
            .addresses
            .get_or_load(infra.clone(), || self.inner.endpoint_address(execution_id, run_instance, infra))
            .await?;
        Ok((*address).clone())
    }
}

/// Every notification a copy hears concerns this worker's own project (the
/// broker pushes no other), so any one drops the whole copy.
fn everything<K, V>(_payload: &str) -> Changed<K, V> {
    Changed::Everything
}

/// Which connection an open is for, and who is opening it: what the
/// broker's answer depends on besides the run.
#[derive(Clone, PartialEq, Eq, Hash)]
struct OpenKey {
    connection_id: String,
    service: String,
    required_permissions: Vec<String>,
    required_values: Vec<String>,
    /// An instance's connection serves that instance's runs alone.
    run_instance: Option<InstanceId>,
}

/// Which published connection a read-back is for: the node's place, its
/// service, and whose copy of the node (`None` for the shared one). Whether
/// the node exists once per instance is part of it: the broker refuses a
/// per-instance node with no instance, which must never be answered with
/// the shared copy's connection.
#[derive(Clone, PartialEq, Eq, Hash)]
struct PublishedKey {
    place: String,
    service: String,
    per_instance: bool,
    copy: Option<InstanceId>,
}

/// One publish, whole: the same publish answered once is answered the
/// same again until the connection changes.
#[derive(Clone, PartialEq, Eq, Hash)]
struct PublishKey {
    published: PublishedKey,
    values: std::collections::BTreeMap<String, String>,
    /// The recipe as JSON text: the recipe type itself does not hash.
    spec: String,
    label: Option<String>,
}

/// The connection desk that keeps answers (see the module doc).
pub struct HeldAccess {
    inner: Arc<dyn AccessBroker>,
    opened: Arc<HeldCopy<OpenKey, ResolveConnectionResponse>>,
    published: Arc<HeldCopy<PublishedKey, PublishedAccessResponse>>,
    publishes: Arc<HeldCopy<PublishKey, PublishAccessResponse>>,
}

impl HeldAccess {
    pub fn new(inner: Arc<dyn AccessBroker>, link: &BrokerLink) -> Arc<Self> {
        Arc::new(Self {
            inner,
            opened: HeldCopy::following(link.subscribe(), ACCESS_CHANNEL, KEPT, everything, |answer: &ResolveConnectionResponse| {
                answer.keep_until_unix.is_some()
            }),
            published: HeldCopy::following(link.subscribe(), ACCESS_CHANNEL, KEPT, everything, |_| true),
            publishes: HeldCopy::following(link.subscribe(), ACCESS_CHANNEL, KEPT, everything, |_| true),
        })
    }
}

/// Whose copy of a node a publish or a read-back concerns: the run's
/// instance for a node that exists once per instance, the shared copy
/// otherwise (the broker decides the same way, from the run's row).
fn copy_of(per_instance: bool, run_instance: Option<&InstanceId>) -> Option<InstanceId> {
    per_instance.then(|| run_instance.cloned()).flatten()
}

#[async_trait]
impl AccessBroker for HeldAccess {
    async fn resolve_connection(
        &self,
        req: &ResolveConnectionRequest,
        run_instance: Option<&InstanceId>,
    ) -> anyhow::Result<ResolveConnectionResponse> {
        let key = OpenKey {
            connection_id: req.connection_id.clone(),
            service: req.service.clone(),
            required_permissions: req.required_permissions.clone(),
            required_values: req.required_values.clone(),
            run_instance: run_instance.cloned(),
        };
        let now = crate::now_unix() as i64;
        if let Some(held) = self.opened.held(&key).filter(|held| held.keep_until_unix.is_some_and(|until| until > now)) {
            return Ok((*held).clone());
        }
        let answer = self.opened.load_fresh(key, || self.inner.resolve_connection(req, run_instance)).await?;
        Ok((*answer).clone())
    }

    async fn release_connection(
        &self,
        req: &weft_broker_client::protocol::ReleaseConnectionRequest,
    ) -> anyhow::Result<weft_broker_client::protocol::ReleaseConnectionResponse> {
        self.inner.release_connection(req).await
    }

    async fn publish_access(
        &self,
        req: &PublishAccessRequest,
        run_instance: Option<&InstanceId>,
    ) -> anyhow::Result<PublishAccessResponse> {
        let key = PublishKey {
            published: PublishedKey {
                place: req.node_id.clone(),
                service: req.service.clone(),
                per_instance: req.per_instance,
                copy: copy_of(req.per_instance, run_instance),
            },
            values: req.values.clone(),
            spec: serde_json::to_string(&req.spec)?,
            label: req.label.clone(),
        };
        let answer = self.publishes.get_or_load(key, || self.inner.publish_access(req, run_instance)).await?;
        Ok((*answer).clone())
    }

    async fn published_access(
        &self,
        req: &PublishedAccessRequest,
        run_instance: Option<&InstanceId>,
    ) -> anyhow::Result<PublishedAccessResponse> {
        let key = PublishedKey {
            place: req.node_id.clone(),
            service: req.service.clone(),
            per_instance: req.per_instance,
            copy: copy_of(req.per_instance, run_instance),
        };
        let answer = self.published.get_or_load(key, || self.inner.published_access(req, run_instance)).await?;
        Ok((*answer).clone())
    }

    async fn mint_instance_token(
        &self,
        req: &weft_broker_client::protocol::ProgramMintInstanceTokenRequest,
    ) -> anyhow::Result<weft_core::program::MintedInstanceToken> {
        self.inner.mint_instance_token(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::broadcast;
    use weft_task_store::pg_signal::{Heard, Subscription};

    /// Counts the reads that reached it.
    struct Counting {
        reads: AtomicUsize,
    }

    #[async_trait]
    impl InfraReader for Counting {
        async fn endpoint_address(
            &self,
            _execution_id: weft_core::ExecutionId,
            _run_instance: Option<&InstanceId>,
            _infra: &InfraHandle,
        ) -> anyhow::Result<Option<EndpointAddress>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }

    fn held_over(inner: Arc<Counting>) -> (HeldInfra, broadcast::Sender<Heard>) {
        let (tx, rx) = broadcast::channel(16);
        let subscription = Subscription::with_listening(rx, Arc::new(AtomicBool::new(true)));
        let addresses = HeldCopy::following(subscription, INFRA_STATUS_CHANNEL, KEPT, everything, |_| true);
        (HeldInfra { inner, addresses }, tx)
    }

    /// An address is asked for once and kept; a change heard drops it; a
    /// handle naming another instance's copy is asked for every time, so
    /// the broker refuses it.
    #[tokio::test]
    async fn an_address_is_kept_until_its_project_says_it_changed() {
        let counting = Arc::new(Counting { reads: AtomicUsize::new(0) });
        let (held, changes) = held_over(counting.clone());
        let run = weft_core::ExecutionId::new_v4();
        let shared = InfraHandle::new("db", "sql", None);
        held.endpoint_address(run, None, &shared).await.unwrap();
        held.endpoint_address(run, None, &shared).await.unwrap();
        assert_eq!(counting.reads.load(Ordering::SeqCst), 1, "kept");
        changes.send(Heard::Signal { channel: INFRA_STATUS_CHANNEL, payload: "p".into() }).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while held.addresses.held(&shared).is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the change drops the copy");
        held.endpoint_address(run, None, &shared).await.unwrap();
        assert_eq!(counting.reads.load(Ordering::SeqCst), 2, "asked again after the change");

        let ann = InstanceId::new("ann").unwrap();
        let bob = InstanceId::new("bob").unwrap();
        let anns = InfraHandle::new("db", "sql", Some(ann.clone()));
        held.endpoint_address(run, Some(&bob), &anns).await.unwrap();
        held.endpoint_address(run, Some(&bob), &anns).await.unwrap();
        assert_eq!(counting.reads.load(Ordering::SeqCst), 4, "another instance's copy goes to the broker every time");
        held.endpoint_address(run, Some(&ann), &anns).await.unwrap();
        held.endpoint_address(run, Some(&ann), &anns).await.unwrap();
        assert_eq!(counting.reads.load(Ordering::SeqCst), 5, "the run's own instance's copy is kept");
    }

    /// Counts what reached it, and answers a resolve kept or not as told.
    struct CountingAccess {
        resolves: AtomicUsize,
        publishes: AtomicUsize,
        keep_until: Option<i64>,
    }

    #[async_trait]
    impl AccessBroker for CountingAccess {
        async fn resolve_connection(&self, _req: &ResolveConnectionRequest, _run: Option<&InstanceId>) -> anyhow::Result<ResolveConnectionResponse> {
            self.resolves.fetch_add(1, Ordering::SeqCst);
            Ok(ResolveConnectionResponse {
                values: Default::default(),
                auth: Vec::new(),
                identity: None,
                relay_url: None,
                owner: weft_core::CredentialOwner::Author,
                keep_until_unix: self.keep_until,
            })
        }
        async fn release_connection(
            &self,
            _req: &weft_broker_client::protocol::ReleaseConnectionRequest,
        ) -> anyhow::Result<weft_broker_client::protocol::ReleaseConnectionResponse> {
            unreachable!()
        }
        async fn publish_access(&self, _req: &PublishAccessRequest, _run: Option<&InstanceId>) -> anyhow::Result<PublishAccessResponse> {
            self.publishes.fetch_add(1, Ordering::SeqCst);
            Ok(PublishAccessResponse {
                connection: weft_core::access::wire::PublishedConnection { connection_id: "c".into(), identity: None },
            })
        }
        async fn published_access(&self, _req: &PublishedAccessRequest, _run: Option<&InstanceId>) -> anyhow::Result<PublishedAccessResponse> {
            unreachable!()
        }
        async fn mint_instance_token(
            &self,
            _req: &weft_broker_client::protocol::ProgramMintInstanceTokenRequest,
        ) -> anyhow::Result<weft_core::program::MintedInstanceToken> {
            unreachable!()
        }
    }

    fn held_access(inner: Arc<CountingAccess>) -> HeldAccess {
        let copy = || {
            let (_tx, rx) = broadcast::channel(16);
            std::mem::forget(_tx);
            Subscription::with_listening(rx, Arc::new(AtomicBool::new(true)))
        };
        HeldAccess {
            inner,
            opened: HeldCopy::following(copy(), ACCESS_CHANNEL, KEPT, everything, |answer: &ResolveConnectionResponse| answer.keep_until_unix.is_some()),
            published: HeldCopy::following(copy(), ACCESS_CHANNEL, KEPT, everything, |_| true),
            publishes: HeldCopy::following(copy(), ACCESS_CHANNEL, KEPT, everything, |_| true),
        }
    }

    fn resolve() -> ResolveConnectionRequest {
        ResolveConnectionRequest {
            execution_id: "e".into(),
            node_id: "n".into(),
            frames: Default::default(),
            node_type: "T".into(),
            connection_id: "c".into(),
            service: "s".into(),
            required_permissions: Vec::new(),
            required_values: Vec::new(),
            expected_duration_secs: 60,
        }
    }

    /// Stored values are kept until the connection changes; a credential
    /// lent per firing never is; one past its keep is asked for again.
    #[tokio::test]
    async fn an_opened_connection_is_kept_only_as_long_as_its_answer_allows() {
        let kept = Arc::new(CountingAccess { resolves: AtomicUsize::new(0), publishes: AtomicUsize::new(0), keep_until: Some(i64::MAX) });
        let access = held_access(kept.clone());
        access.resolve_connection(&resolve(), None).await.unwrap();
        access.resolve_connection(&resolve(), None).await.unwrap();
        assert_eq!(kept.resolves.load(Ordering::SeqCst), 1);
        let bob = InstanceId::new("bob").unwrap();
        access.resolve_connection(&resolve(), Some(&bob)).await.unwrap();
        assert_eq!(kept.resolves.load(Ordering::SeqCst), 2, "another instance's run asks for itself");

        let lent = Arc::new(CountingAccess { resolves: AtomicUsize::new(0), publishes: AtomicUsize::new(0), keep_until: None });
        let access = held_access(lent.clone());
        access.resolve_connection(&resolve(), None).await.unwrap();
        access.resolve_connection(&resolve(), None).await.unwrap();
        assert_eq!(lent.resolves.load(Ordering::SeqCst), 2, "a lent credential is never kept");

        let expired = Arc::new(CountingAccess { resolves: AtomicUsize::new(0), publishes: AtomicUsize::new(0), keep_until: Some(1) });
        let access = held_access(expired.clone());
        access.resolve_connection(&resolve(), None).await.unwrap();
        access.resolve_connection(&resolve(), None).await.unwrap();
        assert_eq!(expired.resolves.load(Ordering::SeqCst), 2, "past its keep, asked again");
    }

    /// The same publish is answered once; different values publish again.
    #[tokio::test]
    async fn a_publish_that_changes_nothing_is_answered_from_memory() {
        let inner = Arc::new(CountingAccess { resolves: AtomicUsize::new(0), publishes: AtomicUsize::new(0), keep_until: None });
        let access = held_access(inner.clone());
        let spec: weft_core::AccessSpec = serde_json::from_value(serde_json::json!({
            "service": "postgres", "acquisition": { "kind": "static", "fields": [] }, "auth": []
        }))
        .unwrap();
        let publish = |password: &str| PublishAccessRequest {
            execution_id: "e".into(),
            node_id: "db".into(),
            service: "postgres".into(),
            spec: spec.clone(),
            values: [("password".to_string(), password.to_string())].into_iter().collect(),
            label: Some("db".into()),
            per_instance: false,
        };
        access.publish_access(&publish("a"), None).await.unwrap();
        access.publish_access(&publish("a"), None).await.unwrap();
        assert_eq!(inner.publishes.load(Ordering::SeqCst), 1);
        access.publish_access(&publish("b"), None).await.unwrap();
        assert_eq!(inner.publishes.load(Ordering::SeqCst), 2);
    }
}
