//! `register_signal` task: a dispatcher Pod runs the entire register
//! flow inside `ListenerPool::with_listener` (so the SHARED OP-lock
//! fences the reaper out for the spawn → POST /register → POST
//! /render → INSERT signal row sequence), then returns the minted
//! token to the worker that requested it.
//!
//! Producers: the worker calls `task_client::enqueue` (in weft-engine)
//! when it hits `ctx.register_signal` or `ctx.await_signal`. The
//! worker blocks on the task's terminal state and reads the resulting
//! token from `task.result`.
//!
//! Idempotency: dedup keyed on `(color, node_id, frames, is_resume,
//! call_index)` so a Pod-crash retry converges on the same task. The
//! task's body is itself idempotent: entry rows reuse a stable token
//! per `(project_id, node_id)`, resume rows mint per-suspension.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use weft_core::frames::LoopFrames;
use weft_core::primitive::SignalSpec;
use weft_core::signal as core_signal;
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterSignalPayload {
    pub color: String,
    pub node_id: String,
    pub frames: LoopFrames,
    pub spec: SignalSpec,
    pub is_resume: bool,
    /// 0-based ordinal of the `await_signal` call within this
    /// (color, node_id, frames). Set by the worker; the dispatcher
    /// stamps it on the SuspensionRegistered event so replay can
    /// rebuild the per-(node, frames) sequence in order. Must not
    /// vary across replays of the same body, so the dedup key
    /// includes it. Required: a missing field would silently default
    /// to 0 and collide every await on the same frame stack.
    pub call_index: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterSignalResult {
    pub token: String,
}

pub struct RegisterSignalExecutor;

/// Normalize the public mount path for a `SignalSurface`. Empty
/// string ⇒ "/", non-empty ⇒ "/<path>" with one leading slash.
/// Returns `None` for surfaces that don't expose a mount path
/// (`TaskCallback`, `Internal`).
fn mount_path_for(surface: &weft_core::primitive::SignalSurface) -> Option<String> {
    match surface {
        weft_core::primitive::SignalSurface::PublicEntry { path } => Some(if path.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", path.trim_start_matches('/'))
        }),
        weft_core::primitive::SignalSurface::TaskCallback
        | weft_core::primitive::SignalSurface::Internal => None,
    }
}

#[async_trait]
impl TaskExecutor<DispatcherState> for RegisterSignalExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: RegisterSignalPayload = serde_json::from_value(task.payload.clone())?;
        let color: weft_core::Color = payload
            .color
            .parse()
            .map_err(|e| anyhow::anyhow!("bad color: {e}"))?;

        // Per-kind validation: each `Signal` impl owns its rules
        // (cron parses, path well-formed, url is http(s), etc).
        // Surfacing this here, before the listener round-trip, keeps
        // the failure attached to the worker that asked, with a clean
        // message instead of a half-set-up signal row.
        if let Err(e) = core_signal::validate_spec(&payload.spec) {
            anyhow::bail!("invalid signal spec: {e}");
        }

        // Resolve tenant + project_id, then run the entire register
        // flow inside `with_listener` so the listener can't be reaped
        // between spawn and `/register` POST. Token reuse for entry
        // rows keeps the registration stable across reactivates;
        // resume rows always mint fresh.
        let project_id = match state.journal.execution_project(color).await? {
            crate::journal::ColorLookup::Found(p) => p,
            crate::journal::ColorLookup::NotFound => {
                anyhow::bail!("no project for color {color}")
            }
            crate::journal::ColorLookup::Corrupt => anyhow::bail!(
                "journal row for color {color} is corrupt; see dispatcher logs"
            ),
        };
        let tenant = state.tenant_router.tenant_for_project(&project_id);
        let namespace = state.namespace_mapper.namespace_for(&tenant);

        let token = if payload.is_resume {
            // Derive resume token from the suspension identity so a
            // retry of this task converges on the same token. The
            // identity (color, node_id, frames, call_index) is what
            // the engine's fold uses to match resumes.
            let mut hasher = Sha256::new();
            hasher.update(color.to_string().as_bytes());
            hasher.update(b":");
            hasher.update(payload.node_id.as_bytes());
            hasher.update(b":");
            for frame in &payload.frames {
                hasher.update(frame.index.to_le_bytes());
            }
            hasher.update(b":");
            hasher.update(payload.call_index.to_le_bytes());
            let bytes = hasher.finalize();
            let mut buf = [0u8; 16];
            buf.copy_from_slice(&bytes[..16]);
            uuid::Uuid::from_bytes(buf).to_string()
        } else {
            let existing: Option<(String,)> = sqlx::query_as(
                "SELECT token FROM signal \
                 WHERE project_id = $1 AND node_id = $2 AND is_resume = FALSE",
            )
            .bind(&project_id)
            .bind(&payload.node_id)
            .fetch_optional(&state.pg_pool)
            .await?;
            match existing {
                Some((t,)) => t,
                None => uuid::Uuid::new_v4().to_string(),
            }
        };

        let resume_color = if payload.is_resume {
            Some(color.to_string())
        } else {
            None
        };

        let token_call = token.clone();
        let project_id_call = project_id.clone();
        let node_id_call = payload.node_id.clone();
        let spec_call = payload.spec.clone();
        let resume_color_owned = resume_color.clone();
        let pool_call = state.pg_pool.clone();

        let (routing, kind_state, rendered) = state
            .listeners
            .with_listener(
                &tenant,
                &namespace,
                state.listener_backend.as_ref(),
                &state.pg_pool,
                state.pod_id.as_str(),
                |handle| async move {
                    let (routing, kind_state) = crate::listener::register_signal(
                        &handle,
                        &token_call,
                        &spec_call,
                        &node_id_call,
                        payload.is_resume,
                        resume_color_owned.as_deref(),
                    )
                    .await?;

                    // Everything after the listener register has to
                    // roll back the listener-side registration if it
                    // fails: otherwise the listener holds a registry
                    // entry (and any minted secret) with no
                    // corresponding signal row. On task retry,
                    // `register_signal` would re-run and mint a
                    // fresh secret, flipping the user's API key.
                    let post_register: Result<_> = async {
                        // Mount-path collision check: another
                        // (project, node) already owns this path.
                        // Refuse with a clear error rather than
                        // letting the unique index surface a generic
                        // SQL error. Same (project, node) reclaiming
                        // the path on reactivate is fine because we
                        // reused the existing token above.
                        if let Some(mp) = mount_path_for(&routing.surface) {
                            if let Some((existing_project, existing_node)) =
                                sqlx::query_as::<_, (String, String)>(
                                    "SELECT project_id, node_id FROM signal \
                                     WHERE mount_path = $1 \
                                       AND NOT (project_id = $2 AND node_id = $3) \
                                     LIMIT 1",
                                )
                                .bind(&mp)
                                .bind(&project_id_call)
                                .bind(&node_id_call)
                                .fetch_optional(&pool_call)
                                .await?
                            {
                                anyhow::bail!(
                                    "mount path '{mp}' already registered \
                                     (project='{existing_project}', node='{existing_node}'); \
                                     change `path` config or unregister the existing node"
                                );
                            }
                        }

                        let rendered =
                            crate::listener::render_signal(&handle, &token_call).await?;
                        Ok(rendered)
                    }
                    .await;
                    match post_register {
                        Ok(rendered) => Ok((routing, kind_state, rendered)),
                        Err(e) => {
                            // Best-effort rollback: if this fails too
                            // the listener entry leaks, but the user
                            // gets the original error which is the
                            // more useful diagnostic.
                            if let Err(unreg_err) =
                                crate::listener::unregister_signal(&handle, &token_call).await
                            {
                                tracing::warn!(
                                    target: "weft_dispatcher::register_signal",
                                    token = %token_call,
                                    rollback_error = %unreg_err,
                                    original_error = %e,
                                    "register_signal rollback failed; listener entry may leak"
                                );
                            }
                            Err(e)
                        }
                    }
                },
            )
            .await?;

        let surface_kind_str = routing.surface.kind_tag().to_string();
        let mount_path = mount_path_for(&routing.surface);
        let auth_kind_str = routing.auth.kind_tag().to_string();
        let auth_config_value = if routing.auth_config.is_null() {
            None
        } else {
            Some(routing.auth_config.clone())
        };
        let consumer_payload = if rendered.is_null() {
            None
        } else {
            Some(rendered)
        };

        let spec_json = serde_json::to_string(&payload.spec)?;

        // Look up the registering node's _tags from the project
        // definition so the signal row carries them. Tags drive the
        // api_token enumeration filter; charset already validated
        // at parse time.
        let project_uuid: uuid::Uuid = project_id
            .parse()
            .map_err(|e| anyhow::anyhow!("project_id parse: {e}"))?;
        // Unfold the lookup. Both "project missing" and "node not in
        // project" used to collapse to empty tags, which downstream
        // signal-row enumeration filters by tag : so the trigger
        // would silently never fire. Fail explicitly on either case.
        let project_def = state
            .projects
            .project(project_uuid)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("register_signal: project_id={project_uuid} not registered")
            })?;
        let node = project_def
            .nodes
            .iter()
            .find(|n| n.id == payload.node_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "register_signal: node_id='{}' not in project_id={project_uuid}",
                    payload.node_id
                )
            })?;
        let tags = node.tags();

        let insert_result = state
            .journal
            .signal_insert(&crate::journal::SignalRegistration {
                token: token.clone(),
                tenant_id: tenant.to_string(),
                project_id,
                color: if payload.is_resume { Some(color) } else { None },
                node_id: payload.node_id.clone(),
                is_resume: payload.is_resume,
                spec_json,
                consumer_kind: payload.spec.consumer_kind.clone(),
                tags,
                consumer_payload,
                surface_kind: surface_kind_str,
                mount_path,
                auth_kind: auth_kind_str,
                auth_config: auth_config_value,
                kind_state,
            })
            .await;
        if let Err(e) = insert_result {
            // signal_insert failed AFTER the listener registered.
            // Roll the listener side back so the in-RAM registry +
            // any minted secret don't outlive their non-existent DB
            // row. Best-effort: the listener may have been reaped
            // between with_listener returning and this rollback
            // attempt, in which case the registry is already gone
            // (and the next rehydrate skips this token because no
            // DB row exists).
            let token_for_rollback = token.clone();
            let rollback = state
                .listeners
                .with_listener_if_alive(&tenant, &state.pg_pool, |handle| async move {
                    crate::listener::unregister_signal(&handle, &token_for_rollback).await
                })
                .await;
            if let Err(unreg_err) = rollback {
                tracing::warn!(
                    target: "weft_dispatcher::register_signal",
                    %token,
                    rollback_error = %unreg_err,
                    original_error = %e,
                    "signal_insert rollback failed; listener entry may leak"
                );
            }
            return Err(e);
        }

        if payload.is_resume {
            // Suspension state lives on the signal row; we also
            // journal SuspensionRegistered so the engine's fold can
            // rebuild the awaited-sequence replay structure on
            // worker restart. Sequenced AFTER signal_insert so the
            // signal row exists by the time anything reads the
            // journal entry. Dedup key collapses retries on the
            // same (color, node_id, frames, call_index); a failure
            // here triggers the task framework to retry, and
            // signal_insert's UPSERT is idempotent so the second
            // pass converges cleanly.
            let now = crate::lease::now_unix() as u64;
            let frames_key = payload
                .frames
                .iter()
                .map(|f| format!("{}", f.index))
                .collect::<Vec<_>>()
                .join("/");
            state
                .journal
                .record_event_dedup(
                    &weft_journal::ExecEvent::SuspensionRegistered {
                        color,
                        node_id: payload.node_id.clone(),
                        frames: payload.frames.clone(),
                        token: token.clone(),
                        spec: payload.spec.clone(),
                        call_index: payload.call_index,
                        at_unix: now,
                    },
                    &format!(
                        "register_signal:{color}:{node_id}:{frames_key}:{call_index}",
                        color = color,
                        node_id = payload.node_id,
                        call_index = payload.call_index
                    ),
                )
                .await?;
        }

        Ok(serde_json::to_value(RegisterSignalResult { token })?)
    }
}
