//! A project's front: the program its callers reach at the project's own
//! address, straight, with nothing of weft's in between
//! (`weft_platform_traits::Runner::front`).
//!
//! The front is the binary the project's triggers that are on fire: one
//! binary answers a project's routes, which every verb that turns
//! triggers on keeps true (`crate::api::project::on_another_binary`), so a
//! build registered meanwhile never takes the routes from the triggers
//! still on the one before; with none on, the registered build's. It is put
//! in place when the project is
//! activated or resynced, when its worker settings change, and, on a machine,
//! again by the reaper whenever it went down; it is let go once nothing of
//! the project takes work any more. What callers and the listener reach it
//! at is kept on the project's row (`project.api_address`).
//!
//! Every change of one project's front happens under that project's lock
//! ([`locked`]), so an activation, a let-go and the reaper's look never
//! interleave, whichever dispatcher process makes them.
//!
//! A call that reaches the front for a route still armed on another
//! program (a new version taking over) is refused like one for a route that
//! takes no calls, and lands here once the route moved (`weft_engine::door`).

use weft_core::projects::ProjectAddress;
use weft_task_store::db::Client as _;

use crate::state::DispatcherState;

/// What a look at a project's front came to.
pub(crate) struct Served {
    /// Where its callers reach it now, or why they cannot (kept on its
    /// row); `None` when nothing of the project takes work, or it runs no
    /// program yet.
    pub address: Option<ProjectAddress>,
    /// The front could not be put in place: why, as kept.
    pub failed: Option<anyhow::Error>,
    /// The kept address changed.
    pub changed: bool,
}

/// Put the project's front in place and keep its address on its row, while
/// anything of the project takes work, and let it go when nothing does; a
/// failure is kept as the address, under the same lock, so the row always
/// says what the last look found.
/// On an install that gives projects a port, `port` is the one the person
/// asked for ([`weft_core::activation::ActivateRequest::port`]).
pub(crate) async fn serve(state: &DispatcherState, project: uuid::Uuid, port: Option<u16>) -> anyhow::Result<Served> {
    locked(state, project, || async {
        match put_in_place(state, project, port).await {
            // Nothing of it takes work (or it runs nothing yet): no front.
            Ok(None) => {
                state.runner.let_front_go(project).await?;
                let changed = keep(state, project, None).await?;
                Ok(Served { address: None, failed: None, changed })
            }
            Ok(Some(address)) => {
                let changed = keep(state, project, Some(&address)).await?;
                Ok(Served { address: Some(address), failed: None, changed })
            }
            Err(e) => {
                let address = ProjectAddress::Unavailable { why: format!("{e:#}") };
                let changed = keep(state, project, Some(&address)).await?;
                Ok(Served { address: Some(address), failed: Some(e), changed })
            }
        }
    })
    .await
}

/// [`serve`]'s work, under its lock.
async fn put_in_place(state: &DispatcherState, project: uuid::Uuid, port: Option<u16>) -> anyhow::Result<Option<ProjectAddress>> {
    if !takes_work(state, project).await? {
        return Ok(None);
    }
    let Some(binary) = routes_binary(state, project).await? else {
        return Ok(None);
    };
    let tenant = state.projects.tenant_for(project).await?.ok_or_else(|| anyhow::anyhow!("project {project} is not registered"))?;
    let target = crate::delivery::worker_target(state, &tenant, project, &binary).await?;
    let Some(ports) = &state.project_ports else {
        anyhow::ensure!(port.is_none(), "this install gives projects no port: a project's own address is the platform's");
        return Ok(Some(state.runner.front(&target, None).await?));
    };
    let mut at = ports.port_of(&state.pg_pool, project, port).await?;
    // A port another program took moves the project to a free one, except
    // the one this activation named, which it checked free just before
    // (`ProjectPorts::check_asked`): taken in between, it fails rather than
    // move. Each move probes its port free, so only a program grabbing it
    // in between makes another.
    const MOVES: usize = 3;
    for _ in 0..MOVES {
        match state.runner.front(&target, Some(at)).await {
            Ok(address) => return Ok(Some(address)),
            Err(e) => match weft_platform_traits::PortTaken::of(&e) {
                Some(taken) if port.is_some() => {
                    anyhow::bail!("port {taken}, which `weft activate --port` asked for, was taken by another program as the project activated")
                }
                Some(taken) => {
                    at = ports.move_from(&state.pg_pool, project, taken).await?;
                    tracing::info!(target: "weft_dispatcher::front", %project, taken, port = at, "another program holds the project's port; it moves to a free one");
                }
                None => return Err(e),
            },
        }
    }
    anyhow::bail!("every free port tried for project {project} was taken by another program before its front could open it")
}

/// The binary that answers `project`'s routes: the one its triggers that
/// are on fire, else the registered build's; `None` when it runs no
/// program yet.
async fn routes_binary(state: &DispatcherState, project: uuid::Uuid) -> anyhow::Result<Option<String>> {
    let on: std::collections::BTreeSet<String> = state
        .activations
        .list(project)
        .await?
        .into_iter()
        .filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active)
        .filter_map(|a| a.program.map(|program| program.binary_hash))
        .collect();
    match on.len() {
        0 => state.projects.running_binary_hash(project).await,
        1 => Ok(on.into_iter().next()),
        _ => anyhow::bail!(
            "project {project}'s triggers that are on fire {} binaries ({}), and one binary answers a project's routes; \
             `weft resync --all` brings every one onto the registered build",
            on.len(),
            on.into_iter().collect::<Vec<_>>().join(", ")
        ),
    }
}

/// [`serve`], logged instead of raised: for an activation, whose own work
/// is done whether or not the front could be put in place, so the person
/// reads why on the activation and on `weft status`.
pub(crate) async fn serve_or_log(state: &DispatcherState, project: uuid::Uuid, port: Option<u16>) -> Option<ProjectAddress> {
    match serve(state, project, port).await {
        Ok(served) => {
            if let Some(e) = &served.failed {
                tracing::error!(target: "weft_dispatcher::front", %project, error = %format!("{e:#}"), "could not put the project's front in place; its routes answer under the install's shared address meanwhile");
            }
            served.address
        }
        Err(e) => {
            tracing::error!(target: "weft_dispatcher::front", %project, error = %format!("{e:#}"), "could not look at the project's front");
            Some(ProjectAddress::Unavailable { why: format!("{e:#}") })
        }
    }
}

/// Let the project's front go once nothing of it takes work: every
/// activation it has refuses arriving work (`weft_core::arrival`).
pub(crate) async fn let_go_if_idle(state: &DispatcherState, project: uuid::Uuid) -> anyhow::Result<()> {
    locked(state, project, || async {
        if takes_work(state, project).await? {
            return Ok(());
        }
        state.runner.let_front_go(project).await?;
        keep(state, project, None).await.map(|_| ())
    })
    .await
}

/// The project's address, as its front last answered it.
pub(crate) async fn address(state: &DispatcherState, project: uuid::Uuid) -> anyhow::Result<Option<ProjectAddress>> {
    let kept: Option<String> =
        state.pg_pool.scalar_opt("SELECT api_address FROM project WHERE id = $1", &[&project]).await?.flatten();
    kept.map(|kept| serde_json::from_str(&kept).map_err(|e| anyhow::anyhow!("project {project}'s address: {e}"))).transpose()
}

/// Which failures [`serve_all`] logs.
pub enum Say {
    /// Every one: the install starting again says what it found.
    Every,
    /// Only one whose kept address changed: the reaper's look every half
    /// minute says nothing while nothing moves.
    Changes,
}

/// Put every project that takes work back at its front, and let go of one
/// that takes none: the install started again (a machine's fronts went with
/// its last process), or, on the reaper's look, a front went down. One
/// project failing never keeps the others from their look.
pub async fn serve_all(state: &DispatcherState, say: Say) -> anyhow::Result<()> {
    let projects: Vec<uuid::Uuid> = state.pg_pool.scalars("SELECT id FROM project WHERE running_binary_hash IS NOT NULL", &[]).await?;
    for project in projects {
        match serve(state, project, None).await {
            Ok(Served { failed: Some(e), changed, .. }) if changed || matches!(say, Say::Every) => {
                tracing::error!(target: "weft_dispatcher::front", %project, error = %format!("{e:#}"), "could not put the project's front in place; its routes answer under the install's shared address meanwhile")
            }
            Ok(_) => {}
            Err(e) => tracing::error!(target: "weft_dispatcher::front", %project, error = %format!("{e:#}"), "could not look at the project's front"),
        }
    }
    Ok(())
}

/// Whether anything of the project takes work: an activation that does not
/// refuse arriving work (`weft_core::arrival`).
async fn takes_work(state: &DispatcherState, project: uuid::Uuid) -> anyhow::Result<bool> {
    let now = crate::lease::now_unix();
    Ok(state.activations.list(project).await?.iter().any(|a| a.lifecycle.standing().arrival(now) != weft_core::arrival::Arrival::Refused))
}

/// Run `change` holding the project's front, across every dispatcher
/// process: a lease (`weft_task_store::locks::with_lease_waiting`), since a
/// front may take minutes to start or deploy, and a lock held that long
/// would hold one of the few lock connections the whole time.
async fn locked<T, F, Fut>(state: &DispatcherState, project: uuid::Uuid, change: F) -> anyhow::Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    weft_task_store::locks::with_lease_waiting(
        &state.pg_pool,
        &state.signals,
        &format!("front/{project}"),
        &format!("project {project}'s front"),
        change,
    )
    .await
}

/// Keep `address` as the project's; whether it changed.
async fn keep(state: &DispatcherState, project: uuid::Uuid, address: Option<&ProjectAddress>) -> anyhow::Result<bool> {
    let kept = address.map(weft_task_store::db::JsonText);
    let changed = state
        .pg_pool
        .execute("UPDATE project SET api_address = $2 WHERE id = $1 AND api_address IS DISTINCT FROM $2", &[&project, &kept])
        .await?;
    Ok(changed > 0)
}
