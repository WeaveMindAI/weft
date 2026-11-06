//! Sending the calls a node owes when it ends (`ctx.after_node`,
//! `weft_core::signal::cancel_call`), from the worker that ends it.
//!
//! The calls are on the run's record (`RunEvent::EndCallOwed`), and the
//! run's book of them (`weft_journal::OwedCalls`, on its [`RunRecord`]) is
//! built from that record when a worker picks the run up and kept as its
//! nodes register more. A firing's calls go when it ends
//! (`RunnerHandle::settle_end_calls`), before its ending is written: a
//! crash in between sends them again from the next worker, where a `404`
//! counts as done. A drive that ends with calls still owed (a firing the
//! engine ended without its body, or a run cancelled with bodies in
//! flight) sends what is left ([`settle_run`]). A run that pauses keeps
//! them on its record for the next drive.
//!
//! Each call goes through its own connection, opened for the node that
//! owed it, and never races the run's cancel: a cancel is exactly when an
//! `on_cancel` call has to go out. A refusal shows on the node's log,
//! quoting what the provider said; nothing retries it.

use std::sync::Arc;

use weft_core::frames::LoopFrames;
use weft_core::project::ProjectDefinition;
use weft_core::signal::CancelMethod;
use weft_core::RunId;
use weft_journal::{JournalClient, OwedCall, RunEvent};

use crate::context::{EngineClients, RunRecord};

/// How long one call may take before it counts as not having gone
/// through: one short call of weft's own, which must not hold the end of
/// a node for good.
const CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Where the calls of one run go: what opens their connections and where
/// their refusals are written.
pub(crate) struct Sender<'a> {
    pub(crate) clients: &'a EngineClients,
    pub(crate) journal: &'a Arc<dyn JournalClient>,
    pub(crate) replica: &'a str,
    pub(crate) run_id: RunId,
    pub(crate) instance: Option<&'a weft_core::instance::InstanceId>,
}

impl Sender<'_> {
    /// Send each of `owed` that goes out for how its firing ended
    /// (`cancelled`: the run's cancel ended it), once, in order. A call
    /// that does not go through is written on its node's log.
    /// Each firing's `EndCallsSettled` goes on the record once its calls
    /// went (those its end did not send included: they are owed no more).
    pub(crate) async fn send(&self, owed: Vec<OwedCall>, cancelled: bool, node_type: impl Fn(&str) -> String) {
        let settled = weft_journal::owed::settled_places(&owed);
        for owed in owed.into_iter().filter(|owed| owed.call.when.sends(cancelled)) {
            let what = format!("{} {}", owed.call.method.as_http(), owed.call.url);
            if let Err(why) = self.send_one(&owed, &node_type(&owed.node_id)).await {
                tracing::error!(
                    target: "weft_engine::end_calls",
                    run_id = %self.run_id, node = %owed.node_id, frames = ?owed.frames, call = %what, error = %why,
                    "a call a node owed when it ended did not go through"
                );
                self.log_refusal(&owed.node_id, &owed.frames, format!("the call {what} this node owed when it ended did not go through: {why}"))
                    .await;
            }
        }
        let at_unix = crate::now_unix();
        for (node_id, frames) in settled {
            let event = RunEvent::EndCallsSettled { run_id: self.run_id, node_id, frames, at_unix };
            crate::context::record_from_replica(self.journal.as_ref(), event, self.replica).await;
        }
    }

    async fn send_one(&self, owed: &OwedCall, node_type: &str) -> Result<(), String> {
        owed.call.validate()?;
        let client = match &owed.access {
            None => weft_core::access::client::plain_client(),
            Some(access) => {
                let req = weft_broker_client::protocol::ResolveConnectionRequest {
                    run_id: self.run_id,
                    node_id: owed.node_id.clone(),
                    frames: owed.frames.clone(),
                    node_type: node_type.to_string(),
                    connection_id: access.id.clone(),
                    service: access.service.clone(),
                    required_permissions: Vec::new(),
                    required_values: access.required_values.clone(),
                    expected_duration_secs: CALL_TIMEOUT.as_secs(),
                    stale: None,
                };
                let resp = self
                    .clients
                    .access_broker
                    .resolve_connection(&req, self.instance)
                    .await
                    .map_err(|e| format!("open the '{}' connection: {e:#}", access.service))?;
                let steps = weft_core::access::client::resolve_steps(&resp.auth, &resp.values)
                    .map_err(|e| format!("the '{}' connection's auth steps do not resolve: {e}", access.service))?;
                let client = weft_core::access::client::authed_client(steps);
                // A credential the runtime lent goes back once the call is
                // made, as a node's does when its body ends.
                if resp.owner.is_platform() {
                    let sent = self.request(&client, owed).await;
                    let release = weft_broker_client::protocol::ReleaseConnectionRequest { run_id: self.run_id, values: resp.values };
                    if let Err(e) = self.clients.access_broker.release_connection(&release).await {
                        tracing::error!(target: "weft_engine::end_calls", run_id = %self.run_id, "releasing a connection failed (its window remains the backstop): {e:#}");
                    }
                    return sent;
                }
                client
            }
        };
        self.request(&client, owed).await
    }

    async fn request(&self, client: &weft_core::reqwest_middleware::ClientWithMiddleware, owed: &OwedCall) -> Result<(), String> {
        let url = owed.call.url.as_str();
        let request = match owed.call.method {
            CancelMethod::Post => client.post(url),
            CancelMethod::Put => client.put(url),
            CancelMethod::Patch => client.patch(url),
            CancelMethod::Delete => client.delete(url),
        };
        let request = match &owed.call.body {
            Some(body) => request.json(body),
            None => request,
        };
        let resp = request.timeout(CALL_TIMEOUT).send().await.map_err(|e| format!("it did not reach the service: {e}"))?;
        let status = resp.status();
        // A 404: what the call ends or removes is gone already.
        if status.is_success() || status == weft_core::reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_else(|e| format!("<body read failed: {e}>"));
        Err(format!("the service refused it ({status}): {}", weft_core::access::client::error_detail(&body)))
    }

    async fn log_refusal(&self, node_id: &str, frames: &LoopFrames, message: String) {
        let at_unix_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64);
        let line = RunEvent::LogLine {
            run_id: self.run_id,
            node_id: node_id.to_string(),
            frames: frames.clone(),
            level: "error".into(),
            message,
            at_unix_ms: Some(at_unix_ms),
            seq: None,
            at_unix: at_unix_ms / 1000,
        };
        crate::context::record_from_replica(self.journal.as_ref(), line, self.replica).await;
    }
}

/// Send what the run's firings still owe as its drive ends for good
/// (`cancelled`: the run was cancelled): the firings it ended without their
/// bodies, and those its cancel stopped mid-body.
pub(crate) async fn settle_run(sender: &Sender<'_>, record: &RunRecord, project: &ProjectDefinition, cancelled: bool) {
    let owed = record.owed.lock().expect("owed calls poisoned").take_all();
    if owed.is_empty() {
        return;
    }
    sender.send(owed, cancelled, |node_id| node_type_of(project, node_id)).await;
}

/// The catalog type of `node_id` in `project`, which a connection is
/// opened for.
pub(crate) fn node_type_of(project: &ProjectDefinition, node_id: &str) -> String {
    project.nodes.iter().find(|node| node.id == node_id).map(|node| node.node_type.clone()).unwrap_or_default()
}
