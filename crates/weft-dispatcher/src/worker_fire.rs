//! Handing a trigger's event to a worker of the program its trigger is
//! armed on (`POST /_weft/fire`, `weft_core::door_fire::DoorFire`): an event
//! the install took at one of its own addresses (a webhook, a provider's
//! push), a listener's event it could not hand over itself, or a parked
//! fire replayed. The worker's door decides what becomes of it.
//!
//! The worker answers on the first line of its answer and keeps the rest
//! of it open until a run it started ends; this side holds the call (and
//! the platform's hold on the worker) for that long in the background, so
//! the run is counted as work by a platform that gives a worker CPU only
//! during a request, or stops one it sees idle.


use weft_core::door_fire::{DoorFire, Fired};

use crate::state::DispatcherState;

/// Hand `fire` to a worker of `project` running `binary_hash`.
pub(crate) async fn fire(state: &DispatcherState, tenant: &str, project: uuid::Uuid, binary_hash: &str, fire: &DoorFire) -> anyhow::Result<Fired> {
    let target = crate::delivery::worker_target(state, tenant, project, binary_hash).await?;
    let endpoint = state.runner.endpoint(&target, weft_platform_traits::Patience::Brief).await?;
    let (base_url, auth) = (endpoint.base_url.clone(), endpoint.auth_value());
    let send = async {
        let answer = state
            .http
            .post(format!("{}/_weft/fire", base_url.trim_end_matches('/')))
            .header(weft_platform_traits::WORKER_AUTH_HEADER, auth)
            .json(fire)
            .send()
            .await
            .map_err(|e| {
                state.runner.call_ended(&target, weft_platform_traits::WorkerCall::no_answer(&e));
                anyhow::anyhow!("call the worker at {base_url}: {e}")
            })?;
        let status = answer.status();
        let call = weft_platform_traits::WorkerCall::answered(status, answer.headers());
        state.runner.call_ended(&target, call);
        if call.platform_refused() {
            anyhow::bail!("the platform refused weft's call to the worker at {base_url} ({status}): weft's account may not invoke it");
        }
        Ok(answer)
    };
    // The endpoint is held while the run the fire started goes: a platform
    // that stops idle workers never stops this one under it.
    weft_core::door_fire::hand_over(send, &fire.token, endpoint).await
}
