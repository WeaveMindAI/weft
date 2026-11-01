//! The dispatcher's client for the listener role.
//!
//! The listener is one logical service: a module of the machine's process,
//! or a service of its own that scales to zero. It holds signals and runs
//! the kind-specific logic of every fire. The durable `signal` table is the
//! truth about which signals exist; the listener rebuilds what it needs
//! from it (every held signal at boot, a single one on first use), so the
//! dispatcher never tracks WHERE a signal is held: it registers a signal
//! when one is born, tells the listener to forget one when it goes, and
//! asks it to process a fire, all at the one address.
//!
//! Every call carries the dispatcher's platform identity: the listener's
//! endpoints answer weft's own roles only.

use anyhow::Result;
use serde_json::Value;
use weft_task_store::db::{Client as _, Pool};

use crate::role_client::RoleClient;

/// How long a call that only reads what the listener holds may take: a
/// signal looked up (in the listener's memory, or one row through the
/// broker) and a kind's pure code run on it (`/live`, `/process`,
/// `/match_push`, `/wake_by_hand`, `/fresh_places`). Well under a second
/// on a warm listener, a few seconds on one starting from zero, and the
/// broker's own wait for a database connection when the database is busy.
/// The calls that reach outside services through a kind (`/prepare`,
/// `/check`, `/start`, `/rehydrate`, `/unregister`, `/cancel_call`: a provider's
/// webhook set or taken down, a connection opened, the last call of a
/// cancelled wait) pass no limit: how long a provider takes is not weft's
/// to bound, and a call cut short there may already have changed things
/// outside.
const LISTENER_LOOKUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Clone)]
pub struct ListenerClient {
    role: RoleClient,
}

impl ListenerClient {
    /// `role` addresses the listener role (`CoreRole::Listener`).
    pub fn new(role: RoleClient) -> Self {
        Self { role }
    }

    /// POST `body` to `route`, within `limit` when there is one
    /// ([`LISTENER_LOOKUP_TIMEOUT`]).
    async fn post(&self, route: &str, limit: Option<std::time::Duration>, body: &impl serde::Serialize) -> Result<reqwest::Response> {
        let mut request = self.role.request(reqwest::Method::POST, route).await?.json(body);
        if let Some(limit) = limit {
            request = request.timeout(limit);
        }
        let resp = request.send().await.map_err(|e| anyhow::anyhow!("listener {route}: {e}"))?;
        Ok(resp)
    }

    /// Fail with [`ListenerRefused`] unless 2xx.
    async fn ok(resp: reqwest::Response, route: &str) -> Result<reqwest::Response> {
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status();
        let body = resp.text().await.unwrap_or_else(|e| format!("<body read failed: {e}>"));
        Err(ListenerRefused { route: route.to_string(), status, body }.into())
    }

    /// What the row of a new signal holds, as its kind computes it
    /// (routing, starting kind state, consumer payload). Starts nothing:
    /// [`Self::start`] brings the signal up once the row is committed.
    pub async fn prepare(
        &self,
        req: &weft_core::signal::listener_protocol::PrepareRequest,
    ) -> Result<weft_core::signal::listener_protocol::PrepareResponse> {
        Ok(Self::ok(self.post("/prepare", None, req).await?, "/prepare").await?.json().await?)
    }

    /// Which of `signals` could not be registered and come up on this
    /// install, each with why, registering and starting nothing: asked
    /// before a resync takes down what they would replace. Reaches outside
    /// services through a kind (the broker's read of a connection), so no
    /// limit.
    pub async fn check(
        &self,
        signals: Vec<weft_core::signal::listener_protocol::CheckSignal>,
    ) -> Result<Vec<weft_core::signal::listener_protocol::CheckRefused>> {
        let req = weft_core::signal::listener_protocol::CheckRequest { signals };
        let answer: weft_core::signal::listener_protocol::CheckResponse = Self::ok(self.post("/check", None, &req).await?, "/check").await?.json().await?;
        Ok(answer.refused)
    }

    /// What the signal's kind is showing (and, for a run's wait, the raw
    /// last answer it saw), or `None` when the listener holds no such
    /// signal. `address` is where an outside caller reaches this signal,
    /// which the listener cannot work out on its own (see
    /// `LiveRequest::address`).
    pub async fn live(&self, token: &str, address: Option<&str>) -> Result<Option<weft_core::signal::listener_protocol::LiveResponse>> {
        let resp = self
            .post(
                "/live",
                Some(LISTENER_LOOKUP_TIMEOUT),
                &weft_core::signal::listener_protocol::LiveRequest {
                    token: token.to_string(),
                    address: address.map(str::to_string),
                },
            )
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::ok(resp, "/live").await?.json().await?))
    }

    pub async fn process(&self, token: &str, payload: &Value) -> Result<weft_core::signal::listener_protocol::ProcessOutcome> {
        let resp = self.post("/process", Some(LISTENER_LOOKUP_TIMEOUT), &serde_json::json!({ "token": token, "payload": payload })).await?;
        Ok(Self::ok(resp, "/process").await?.json().await?)
    }

    /// Which of `tokens` a verified provider push feeds, and with what
    /// payload. The dispatcher knows a push arrived, that the broker called
    /// it genuine, and which connections it names; which SIGNALS that comes
    /// to depends on a kind's own settings, and the kinds live in the
    /// listener.
    pub async fn match_push(
        &self,
        push: &weft_core::signal::listener_protocol::PushEvent,
        tokens: &[String],
    ) -> Result<Vec<weft_core::signal::listener_protocol::MatchedPush>> {
        let resp = self
            .post(
                "/match_push",
                Some(LISTENER_LOOKUP_TIMEOUT),
                &weft_core::signal::listener_protocol::MatchPushRequest { push: push.clone(), tokens: tokens.to_vec() },
            )
            .await?;
        Ok(Self::ok(resp, "/match_push").await?.json::<weft_core::signal::listener_protocol::MatchPushResponse>().await?.matched)
    }

    /// What one signal wakes with when a person wakes it by hand, or
    /// `None` when its kind cannot be woken that way. The wake payload is a
    /// kind's own shape, so the listener answers it.
    pub async fn wake_by_hand(&self, token: &str) -> Result<Option<Value>> {
        let resp = self
            .post("/wake_by_hand", Some(LISTENER_LOOKUP_TIMEOUT), &weft_core::signal::listener_protocol::WakeByHandRequest { token: token.to_string() })
            .await?;
        Ok(Self::ok(resp, "/wake_by_hand")
            .await?
            .json::<weft_core::signal::listener_protocol::WakeByHandResponse>()
            .await?
            .payload)
    }

    /// Bring up the signal whose row was just committed under `token`
    /// (its first wake, its held connection), as a new registration or a
    /// row put back (see `StartMode`). `replaced` is the row the commit
    /// wrote over, whose wakes the listener cancels; `None` for a token's
    /// first row. A kind refusing to come up answers its reason.
    pub async fn start(
        &self,
        token: &str,
        mode: weft_core::signal::listener_protocol::StartMode,
        replaced: Option<&crate::store::SignalRegistration>,
    ) -> Result<()> {
        let replaced = match replaced {
            Some(row) => Some(weft_core::signal::listener_protocol::ReplacedSignal {
                spec: row.spec()?,
                kind_state: row.kind_state.clone(),
                generation: row.generation,
            }),
            None => None,
        };
        let req = weft_core::signal::listener_protocol::StartRequest { token: token.to_string(), mode, replaced };
        Self::ok(self.post("/start", None, &req).await?, "/start").await?;
        Ok(())
    }

    /// Have the listener send the last call of a wait whose run was
    /// cancelled (`weft_core::signal::cancel_call`). Answers once the
    /// outside service answered it; a refusal or a call that could not
    /// reach the service is an error naming why.
    pub async fn cancel_call(&self, call: &weft_core::signal::listener_protocol::CancelCallRequest) -> Result<()> {
        Self::ok(self.post("/cancel_call", None, call).await?, "/cancel_call").await?;
        Ok(())
    }

    /// The fresh kind state of each of the entry signals `signals` whose
    /// kind keeps a place in what it reads, read at the version each was
    /// read at: what a door that refuses writes on them
    /// (`crate::activation_store::SignalsGoing::Places`).
    pub async fn fresh_places(&self, signals: &[crate::store::SignalRegistration]) -> Result<Vec<crate::store::FreshPlace>> {
        use weft_core::signal::listener_protocol::{FreshPlaceAsk, FreshPlacesRequest, FreshPlacesResponse};
        if signals.is_empty() {
            return Ok(Vec::new());
        }
        let asks = signals.iter().map(|sig| Ok(FreshPlaceAsk { token: sig.token.clone(), spec: sig.spec()? })).collect::<Result<Vec<_>>>()?;
        let answer: FreshPlacesResponse =
            Self::ok(self.post("/fresh_places", Some(LISTENER_LOOKUP_TIMEOUT), &FreshPlacesRequest { signals: asks }).await?, "/fresh_places").await?.json().await?;
        answer
            .places
            .into_iter()
            .map(|place| {
                let read = signals.iter().find(|sig| sig.token == place.token).ok_or_else(|| {
                    anyhow::anyhow!("the listener answered a fresh place for signal {}, which was not asked", place.token)
                })?;
                Ok(crate::store::FreshPlace { token: place.token, kind_state_seq: read.kind_state_seq, kind_state: place.kind_state })
            })
            .collect()
    }

    /// Tell the listener to reconcile what it holds of `project` with
    /// the durable signal table, leaving the signals in `skip` down.
    /// Idempotent. Fails naming each of the project's rows that could not
    /// come up.
    pub async fn rehydrate(&self, project: uuid::Uuid, skip: &[String]) -> Result<()> {
        let req = weft_core::signal::listener_protocol::RehydrateRequest { project, skip: skip.to_vec() };
        Self::ok(self.post("/rehydrate", None, &req).await?, "/rehydrate").await?;
        Ok(())
    }

    /// Tell the listener `sig`'s row is gone, with what it needs to tear
    /// down what the signal arranged outside.
    pub async fn unregister(&self, sig: &crate::store::SignalRegistration) -> Result<()> {
        Self::ok(self.post("/unregister", None, &Self::unregister_request(sig, false)?).await?, "/unregister").await?;
        Ok(())
    }

    /// [`Self::unregister`] for a row replaced by one under the same token
    /// that is brought up next: answered once the teardown is over, so it
    /// never reaches what the new row arranges. A row the listener can
    /// never tear down (it cannot read it, and says so with a 422) is
    /// logged and passed: refusing would keep the token from ever being
    /// registered again, and nothing would tear it down later either.
    /// Answers whether the row was stopped (`false` for one passed). On an
    /// error the listener may have stopped it before the answer was lost,
    /// unless it answered that it does not know the row's kind (503),
    /// which it says before touching anything.
    pub async fn unregister_replaced(&self, sig: &crate::store::SignalRegistration) -> Result<bool> {
        let resp = self.post("/unregister", None, &Self::unregister_request(sig, true)?).await?;
        if resp.status() == reqwest::StatusCode::UNPROCESSABLE_ENTITY {
            let why = resp.text().await.unwrap_or_else(|e| format!("<body read failed: {e}>"));
            tracing::error!(
                target: "weft_dispatcher::listener",
                token = %sig.token, %why,
                "the row being replaced cannot be torn down; what it arranged outside stays until it lapses"
            );
            return Ok(false);
        }
        Self::ok(resp, "/unregister").await?;
        Ok(true)
    }

    fn unregister_request(
        sig: &crate::store::SignalRegistration,
        reused: bool,
    ) -> Result<weft_core::signal::listener_protocol::UnregisterRequest> {
        Ok(weft_core::signal::listener_protocol::UnregisterRequest {
            token: sig.token.clone(),
            tenant_id: sig.tenant_id.clone(),
            spec: sig.spec()?,
            kind_state: sig.kind_state.clone(),
            generation: sig.generation,
            reused,
        })
    }

    /// Tell the listener to forget each of `signals`, best effort. Does
    /// NOT touch the durable rows: what the caller does with them is its
    /// choice (a delete, cancel or consume deletes them; a door that
    /// refuses with what is held kept keeps them, so the next activation
    /// brings them back). A failure is logged: the
    /// durable row is the truth, and a listener that kept a stale entry
    /// finds the row gone the next time it is asked about the token.
    pub async fn unregister_many(&self, signals: &[crate::store::SignalRegistration]) {
        for sig in signals {
            if let Err(e) = self.unregister(sig).await {
                tracing::warn!(
                    target: "weft_dispatcher::listener",
                    token = %sig.token,
                    error = %e,
                    "unregister failed; the listener may keep a stale entry until it next reads the row"
                );
            }
        }
    }
}

/// A listener's answer that was not a success.
#[derive(Debug)]
pub struct ListenerRefused {
    pub route: String,
    pub status: reqwest::StatusCode,
    pub body: String,
}

impl std::fmt::Display for ListenerRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "listener {} returned {}: {}", self.route, self.status, self.body)
    }
}

impl std::error::Error for ListenerRefused {}

/// Whether the listener is listening for `project`: it holds every signal
/// the durable table has, so that is whether the project has an armed
/// trigger.
pub async fn project_is_listening(pool: &Pool, project: uuid::Uuid) -> Result<bool> {
    Ok(pool.scalar("SELECT EXISTS (SELECT 1 FROM signal WHERE project_id = $1 AND is_resume = FALSE)", &[&project]).await?)
}

/// How long one call to the listener while letting go may take: a
/// teardown can call out to a provider, so this is generous; one that
/// runs past it counts as kept.
const LET_GO_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// How many signals are let go of at once.
const LET_GO_AT_ONCE: usize = 16;

/// Have the listener let go of `signals`, whose activations took no work
/// when the caller looked (a door that refuses, or whose window ended), each
/// answered once its teardown is over ([`ListenerClient::unregister_replaced`],
/// which also passes a row no listener can ever read). Then any of them
/// listening by now (a reactivation landed meanwhile, and its own bring-up
/// may have been undone by the teardown) is torn down and brought up again
/// whole (what fails there is logged with its recovery, never stopping the
/// others). Answers the signals the listener did not let go of, each by
/// token with why, for the caller to write down ([`note_let_go`]); fails
/// only when the look after could not be made.
pub async fn let_go_of_stopped(
    pool: &Pool,
    listener: &ListenerClient,
    signals: &[crate::store::SignalRegistration],
) -> Result<Vec<(String, String)>> {
    use futures::StreamExt;
    if signals.is_empty() {
        return Ok(Vec::new());
    }
    let kept: Vec<(String, String)> = futures::stream::iter(signals.iter().cloned())
        .map(|sig| {
            let listener = listener.clone();
            async move {
                match tokio::time::timeout(LET_GO_WAIT, listener.unregister_replaced(&sig)).await {
                    Ok(Ok(_)) => None,
                    Ok(Err(e)) => {
                        tracing::warn!(target: "weft_dispatcher::listener", token = %sig.token, error = %format!("{e:#}"), "the listener did not let go of a signal");
                        Some((sig.token.clone(), format!("{e:#}")))
                    }
                    Err(_) => {
                        tracing::warn!(target: "weft_dispatcher::listener", token = %sig.token, "the listener did not answer in time when told to let go of a signal");
                        Some((sig.token.clone(), format!("the listener did not answer within {}s", LET_GO_WAIT.as_secs())))
                    }
                }
            }
        })
        .buffer_unordered(LET_GO_AT_ONCE)
        .filter_map(|kept| async move { kept })
        .collect()
        .await;
    let tokens: Vec<&str> = signals.iter().map(|s| s.token.as_str()).collect();
    let listening: Vec<(String, uuid::Uuid)> = pool
        .all(
            &format!(
                "SELECT s.token, s.project_id FROM signal s {} WHERE s.token = ANY($1) AND {}",
                weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
                weft_broker_client::protocol::ACTIVATION_LISTENS,
            ),
            &[&tokens],
        )
        .await?;
    // Every one of them is brought up, whatever happens to another: one
    // left torn down would read as listening with nothing behind it, and
    // nothing would look at it again.
    let mut projects = std::collections::BTreeSet::new();
    let mut failed = Vec::new();
    for (token, project) in &listening {
        if let Some(sig) = signals.iter().find(|s| &s.token == token) {
            match tokio::time::timeout(LET_GO_WAIT, listener.unregister_replaced(sig)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => failed.push(format!("{token}: {e:#}")),
                Err(_) => failed.push(format!("{token}: no answer within {}s", LET_GO_WAIT.as_secs())),
            }
        }
        projects.insert(*project);
    }
    for project in projects {
        match tokio::time::timeout(LET_GO_WAIT, listener.rehydrate(project, &[])).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => failed.push(format!("project {project}: {e:#}")),
            Err(_) => failed.push(format!("project {project}: no answer within {}s", LET_GO_WAIT.as_secs())),
        }
    }
    if !failed.is_empty() {
        tracing::error!(
            target: "weft_dispatcher::listener",
            failed = %failed.join("; "),
            "signals switched back on while being let go of could not all be brought up again; `weft activate` on their project brings them up"
        );
    }
    Ok(kept)
}

/// Write down, on each of `signals`, whether the listener let go of it
/// ([`let_go_of_stopped`] answered the ones it `kept`, with why), in
/// `signal.let_go_failed`: one it kept, and whose trigger still takes no
/// work, is marked, to be asked again
/// (`crate::store::postgres::signals_kept_after_let_go` reads them back);
/// the rest are cleared.
pub async fn note_let_go(pool: &Pool, signals: &[crate::store::SignalRegistration], kept: &[(String, String)]) -> Result<()> {
    let tokens: Vec<&str> = signals.iter().map(|s| s.token.as_str()).collect();
    let why: Vec<Option<&str>> =
        signals.iter().map(|s| kept.iter().find(|(token, _)| token == &s.token).map(|(_, why)| why.as_str())).collect();
    pool.execute(
        &format!(
            "UPDATE signal SET let_go_failed = t.why \
             FROM (SELECT s.token, CASE WHEN {listens} THEN NULL ELSE k.why END AS why \
                   FROM unnest($1::text[], $2::text[]) AS k(token, why) JOIN signal s ON s.token = k.token {join}) t \
             WHERE signal.token = t.token AND signal.let_go_failed IS DISTINCT FROM t.why",
            listens = weft_broker_client::protocol::ACTIVATION_LISTENS,
            join = weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
        ),
        &[&tokens, &why],
    )
    .await?;
    Ok(())
}
