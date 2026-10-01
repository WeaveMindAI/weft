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

use crate::role_client::RoleClient;

#[derive(Clone)]
pub struct ListenerClient {
    role: RoleClient,
}

impl ListenerClient {
    /// `role` addresses the listener role (`CoreRole::Listener`).
    pub fn new(role: RoleClient) -> Self {
        Self { role }
    }

    async fn post(&self, route: &str, body: &impl serde::Serialize) -> Result<reqwest::Response> {
        let resp = self
            .role
            .request(reqwest::Method::POST, route)
            .await?
            .json(body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("listener {route}: {e}"))?;
        Ok(resp)
    }

    /// Bail with `<route> returned <status>: <body>` unless 2xx.
    async fn ok(resp: reqwest::Response, route: &str) -> Result<reqwest::Response> {
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status();
        let body = resp.text().await.unwrap_or_else(|e| format!("<body read failed: {e}>"));
        anyhow::bail!("listener {route} returned {status}: {body}")
    }

    /// What the row of a new signal holds, as its kind computes it
    /// (routing, starting kind state, consumer payload). Starts nothing:
    /// [`Self::start`] brings the signal up once the row is committed.
    pub async fn prepare(
        &self,
        req: &weft_core::signal::listener_protocol::PrepareRequest,
    ) -> Result<weft_core::signal::listener_protocol::PrepareResponse> {
        Ok(Self::ok(self.post("/prepare", req).await?, "/prepare").await?.json().await?)
    }

    /// What the trigger's kind is showing, or `None` when the listener
    /// holds no such signal. `address` is where an outside caller
    /// reaches this signal, which the listener cannot work out on its own
    /// (see `LiveRequest::address`).
    pub async fn live(&self, token: &str, address: Option<&str>) -> Result<Option<weft_core::live::LiveFeed>> {
        let resp = self
            .post(
                "/live",
                &weft_core::signal::listener_protocol::LiveRequest {
                    token: token.to_string(),
                    address: address.map(str::to_string),
                },
            )
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let body: weft_core::signal::listener_protocol::LiveResponse = Self::ok(resp, "/live").await?.json().await?;
        Ok(Some(body.live))
    }

    pub async fn process(&self, token: &str, payload: &Value) -> Result<weft_core::signal::listener_protocol::ProcessOutcome> {
        let resp = self.post("/process", &serde_json::json!({ "token": token, "payload": payload })).await?;
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
            .post("/wake_by_hand", &weft_core::signal::listener_protocol::WakeByHandRequest { token: token.to_string() })
            .await?;
        Ok(Self::ok(resp, "/wake_by_hand")
            .await?
            .json::<weft_core::signal::listener_protocol::WakeByHandResponse>()
            .await?
            .payload)
    }

    /// Bring up the signal whose row was just committed under `token`
    /// (its first wake, its held connection), as a new registration or a
    /// row put back (see `StartMode`). A kind refusing to come up answers
    /// its reason.
    pub async fn start(&self, token: &str, mode: weft_core::signal::listener_protocol::StartMode) -> Result<()> {
        let req = weft_core::signal::listener_protocol::StartRequest { token: token.to_string(), mode };
        Self::ok(self.post("/start", &req).await?, "/start").await?;
        Ok(())
    }

    /// Tell the listener to reconcile what it holds of `project` with
    /// the durable signal table, leaving the signals in `skip` down.
    /// Idempotent. Fails naming each of the project's rows that could not
    /// come up.
    pub async fn rehydrate(&self, project: uuid::Uuid, skip: &[String]) -> Result<()> {
        let req = weft_core::signal::listener_protocol::RehydrateRequest { project, skip: skip.to_vec() };
        Self::ok(self.post("/rehydrate", &req).await?, "/rehydrate").await?;
        Ok(())
    }

    pub async fn unregister(&self, token: &str) -> Result<()> {
        Self::ok(self.post("/unregister", &serde_json::json!({ "token": token })).await?, "/unregister").await?;
        Ok(())
    }

    /// Tell the listener to forget each of `signals`, best effort. Does
    /// NOT touch the durable rows: what the caller does with them is its
    /// choice (a delete, cancel or consume deletes them; a hibernate keeps
    /// them so reactivate brings them back). A failure is logged: the
    /// durable row is the truth, and a listener that kept a stale entry
    /// finds the row gone the next time it is asked about the token.
    pub async fn unregister_many(&self, signals: &[crate::journal::SignalRegistration]) {
        for sig in signals {
            if let Err(e) = self.unregister(&sig.token).await {
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

/// Whether the listener is listening for `project`: it holds every signal
/// the durable table has, so that is whether the project has an armed
/// trigger.
pub async fn project_is_listening(pool: &sqlx::PgPool, project: uuid::Uuid) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM signal WHERE project_id = $1 AND is_resume = FALSE)")
        .bind(project)
        .fetch_one(pool)
        .await?)
}
