//! Remaking an infra node's baked outputs (`weft_core::infra::bake`) by
//! hand.
//!
//! The run that applies a copy's infra (an infra setup) runs the node's
//! body, and the step saves what went out on its baked outputs before it
//! completes (`weft_engine`'s execution driver, through the broker's
//! `/v1/infra/bake`), so a setup that ended has saved its bake. That
//! happens whenever the copy's infra setup runs (`weft infra start`,
//! `upgrade`, a program's own start), and by hand ([`rebake`],
//! `weft infra rebake`). In between, the infra itself tells weft what
//! changed, through the agent beside its unit
//! (`weft_access_store::write_pushed_values`).

use crate::state::DispatcherState;

/// Remake the bake of copy `node` (spelled) of `project_id` for
/// `instance` (`None`: the shared copy): run its infra setup again, which
/// applies it (a copy already as asked is left as it is) and runs its
/// body, which saves the new values. Answers once the setup ended. A
/// setup of the same copies already under way (for this node or another)
/// is waited out first, whatever it ends in: it may not cover `node`, so
/// this one runs after it rather than counting on it.
pub(crate) async fn rebake(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    node: &str,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<(), (axum::http::StatusCode, String)> {
    let nodes = [node.to_string()];
    loop {
        let next = crate::lease::with_project_transition_lock(&state.lock_pool, project_id, || async {
            let live = crate::api::project::infra_setup_execution_ids(state, project_id, Some(instance)).await?;
            if !live.is_empty() {
                return Ok(Err(live));
            }
            Ok(Ok(crate::api::project::start_infra_setup(state, project_id, instance, &nodes).await))
        })
        .await
        .map_err(|e| crate::lease::lock_answer("project transition lock", e))?;
        match next {
            Ok(started) => {
                let Some(run) = started? else { return Ok(()) };
                return crate::api::project::await_infra_setup(state, run).await.map_err(<(axum::http::StatusCode, String)>::from);
            }
            // Its own outcome is its own: this rebake only waits for it to
            // end, then looks again.
            Err(live) => {
                for execution_id in live {
                    let other = crate::api::project::InfraSetupRun::follow(state, project_id, execution_id).await;
                    let _ = crate::api::project::await_infra_setup(state, other).await;
                }
            }
        }
    }
}
