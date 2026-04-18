use axum::{
    routing::{get, post, put, delete},
    Router,
    http::{header, Method, HeaderValue},
};
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use std::sync::Arc;

mod routes;
mod state;
mod extension_tokens;
mod extension_api;
mod trigger_store;
mod usage_store;
mod webhooks;
mod crypto;
mod log_utils;
mod publish;

use state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Starting Weft API server");

    // Fail-closed startup: refuse to boot in cloud mode without a non-empty
    // INTERNAL_API_KEY. The publish_execute handler and the node-routing
    // surface authenticate the service-to-service hop via this key; an
    // empty value silently disables the check and lets any internet caller
    // hit internal endpoints with spoofed owner headers. Local OSS runs
    // without the var stay permissive for developer convenience.
    {
        let deployment_mode = std::env::var("DEPLOYMENT_MODE").unwrap_or_else(|_| "cloud".to_string());
        if deployment_mode != "local" {
            match std::env::var("INTERNAL_API_KEY") {
                Ok(v) if !v.is_empty() => {
                    tracing::info!("INTERNAL_API_KEY configured ({} chars)", v.len());
                }
                _ => {
                    anyhow::bail!(
                        "INTERNAL_API_KEY is required in cloud mode but was missing or empty. \
                         Refusing to start. Configure it before launching weft-api."
                    );
                }
            }
        }
    }

    let state = Arc::new(AppState::new().await);

    // Load and restart triggers from database
    load_triggers_from_database(state.clone()).await;

    // Start trigger event listener
    start_trigger_event_listener(state.clone());
    
    // Start trigger heartbeat and recovery background task
    start_trigger_maintenance_task(state.clone());

    // Backfill daily usage aggregation for any days missed during downtime
    backfill_usage_on_startup(state.clone()).await;

    // Start daily usage aggregation cron
    start_usage_aggregation_task(state.clone());

    let app = Router::new()
        .route("/health", get(routes::health))
        // Trigger routes
        .route("/api/v1/triggers", get(routes::list_triggers))
        .route("/api/v1/triggers", post(routes::register_trigger))
        .route("/api/v1/triggers/project/{project_id}", delete(routes::unregister_project_triggers))
        .route("/api/v1/triggers/{trigger_id}/setup-completed", post(routes::trigger_setup_completed))
        // Webhook endpoint - generic handler for all webhook triggers
        .route("/api/v1/webhooks/{trigger_id}", post(webhooks::handle_webhook))
        // Extension token management API
        .route("/api/extension/tokens", post(extension_tokens::create_token))
        .route("/api/extension/tokens/user/{user_id}", get(extension_tokens::list_tokens))
        .route("/api/extension/tokens/{token_id}", delete(extension_tokens::delete_token))
        // Extension API - authenticated via token in path
        .route("/ext/{token}/health", get(extension_api::health_check))
        .route("/api/extension/tokens/validate/{token}", get(extension_api::validate_token_handler))
        .route("/ext/{token}/tasks", get(extension_api::list_tasks))
        .route("/ext/{token}/tasks/{execution_id}/complete", post(extension_api::complete_task))
        .route("/ext/{token}/tasks/{execution_id}/cancel", post(extension_api::cancel_task))
        .route("/ext/{token}/actions/{action_id}/dismiss", post(extension_api::dismiss_action))
        .route("/ext/{token}/triggers/{trigger_task_id}/submit", post(extension_api::submit_trigger))
        // Infrastructure management
        .route("/api/infra/{project_id}/start", post(routes::start_infra))
        .route("/api/infra/{project_id}/force-retry", post(routes::force_retry_infra))
        .route("/api/infra/{project_id}/stop", post(routes::stop_infra))
        .route("/api/infra/{project_id}/terminate", post(routes::terminate_infra))
        .route("/api/infra/{project_id}/status", get(routes::get_infra_status))
        .route("/api/infra/{project_id}/nodes/{node_id}/live", get(routes::get_infra_live_data))
        // Usage tracking API
        .route("/api/v1/usage/{user_id}", get(routes::get_usage))
        .route("/api/v1/usage/events", post(routes::record_usage_event))
        .route("/api/v1/usage/start-execution", post(routes::start_execution))
        .route("/api/v1/usage/execution-cost", get(routes::get_execution_cost))
        // Credits API (admin)
        .route("/api/v1/admin/credits", post(routes::add_credits))
        .route("/api/v1/credits", get(routes::get_credits))
        // Unified file storage
        .route("/api/v1/files", post(routes::create_file).get(routes::list_files))
        .route("/api/v1/files/{file_id}", get(routes::get_file).delete(routes::delete_file))
        .route("/api/v1/files/{file_id}/upload", put(routes::upload_file_bytes).layer(axum::extract::DefaultBodyLimit::max(2_147_483_648)))

        // Publish routes (deploy-as-a-page base CRUD, open source).
        // Public endpoints are keyed on (username, slug); owner-scoped
        // mutation endpoints stay slug-only because the owner's user_id
        // already uniquely identifies their slug namespace.
        .route("/api/v1/publish", get(publish::list_publications).post(publish::publish_project))
        .route("/api/v1/publish/execute", post(publish::publish_execute))
        .route("/api/v1/publish/{slug}", axum::routing::patch(publish::update_publication).delete(publish::delete_publication))
        .route("/api/v1/publish/by-user/{username}/{slug}", get(publish::get_by_user_slug))
        .route("/api/v1/publish/by-user/{username}/{slug}/latest-trigger-run", get(publish::latest_trigger_run))
        // Public visitor run. OSS-local only: cloud production routes
        // this URL through cloud-api which does rate-limiting and credit
        // gating before forwarding to `/api/v1/publish/execute`. In OSS
        // standalone the dashboard's `/p/<u>/<s>` page hits weft-api
        // directly, so we expose a thin wrapper that delegates to the
        // same execute logic without the cloud-only gates.
        .route("/api/v1/publish/by-user/{username}/{slug}/run", post(publish::public_run_handler))
        .layer(build_cors_layer())
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = format!("0.0.0.0:{}", port);
    
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("Listening on {}", addr);

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

fn start_trigger_event_listener(state: Arc<AppState>) {
    tokio::spawn(async move {
        // Take the event receiver from the trigger service
        let mut event_rx = {
            let mut service = state.trigger_service.lock().await;
            service.take_event_receiver()
        };

        if let Some(ref mut rx) = event_rx {
            tracing::info!("Trigger event listener started");
            while let Some(event) = rx.recv().await {
                tracing::info!(
                    "Trigger event fired: trigger={}, project={}",
                    event.triggerId, event.projectId
                );

                // Look up trigger record from database. The `triggers` table
                // is the single source of truth for both builder projects
                // and published deployments (deployments ARE projects, with
                // their own trigger rows keyed on their own project_id).
                let trigger_record = match trigger_store::get_trigger(&state.db_pool, &event.triggerId).await {
                    Ok(Some(t)) => t,
                    Ok(None) => {
                        tracing::error!("Trigger {} not found in database", event.triggerId);
                        continue;
                    }
                    Err(e) => {
                        tracing::error!("Failed to look up trigger {}: {}", event.triggerId, e);
                        continue;
                    }
                };
                let node_type = trigger_record.nodeType.clone();

                // Get weft code: stored as a JSON string in project_definition, or fall back to weft_code column
                let weft_code = match trigger_record.projectDefinition {
                    Some(ref wf) => {
                        // project_definition is stored as a JSON string value containing weft code
                        match wf.as_str() {
                            Some(code) if !code.is_empty() => code.to_string(),
                            _ => {
                                tracing::warn!("Trigger {} has non-string project_definition, fetching weft_code from projects table", event.triggerId);
                                String::new()
                            }
                        }
                    }
                    None => String::new(),
                };

                let weft_code = if weft_code.is_empty() {
                    // Fall back to weft_code column from projects table.
                    // Works for both builder and deployment projects because
                    // a deployment is just another row in `projects`.
                    tracing::warn!(
                        "Trigger {} has no stored weft code, fetching from projects table",
                        event.triggerId
                    );
                    match sqlx::query_scalar::<_, Option<String>>(
                        "SELECT weft_code FROM projects WHERE id = $1::uuid",
                    )
                    .bind(&event.projectId)
                    .fetch_optional(&state.db_pool)
                    .await
                    {
                        Ok(Some(Some(code))) if !code.is_empty() => {
                            // Backfill trigger record
                            let _ = sqlx::query("UPDATE triggers SET project_definition = $1 WHERE id = $2")
                                .bind(serde_json::Value::String(code.clone()))
                                .bind(&event.triggerId)
                                .execute(&state.db_pool)
                                .await;
                            code
                        }
                        Ok(_) => {
                            tracing::error!(
                                "Project {} not found or has no weft_code for trigger {}",
                                event.projectId, event.triggerId
                            );
                            continue;
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to fetch project {} for trigger {}: {}",
                                event.projectId, event.triggerId, e
                            );
                            continue;
                        }
                    }
                } else {
                    weft_code
                };

                // Compile weft code to ProjectDefinition
                let project_uuid = match uuid::Uuid::parse_str(&event.projectId) {
                    Ok(p) => p,
                    Err(_) => {
                        tracing::error!("Invalid project UUID for trigger {}: {}", event.triggerId, event.projectId);
                        continue;
                    }
                };
                let mut project = match weft_core::weft_compiler::compile(&weft_code, project_uuid) {
                    Ok(w) => w,
                    Err(e) => {
                        tracing::error!("Failed to compile weftCode for trigger {}: {:?}", event.triggerId, e);
                        continue;
                    }
                };
                if let Err(errors) = weft_nodes::enrich::enrich_project(&mut project, state.node_registry) {
                    tracing::error!("Project validation failed for trigger {}: {}", event.triggerId, errors.join("; "));
                    continue;
                }

                // Start project execution via Axum executor
                let execution_id = uuid::Uuid::new_v4().to_string();
                let executor_url = format!(
                    "{}/ProjectExecutor/{}/start/send",
                    state.executor_url, execution_id
                );

                // Use userId from the trigger record.
                // In cloud mode, a missing userId means the trigger is misconfigured: skip.
                let is_local = std::env::var("DEPLOYMENT_MODE").unwrap_or_default() != "cloud";
                let user_id = match trigger_record.userId.as_deref() {
                    Some(uid) => uid,
                    None if is_local => "local",
                    None => {
                        tracing::warn!("Skipping trigger {} execution: no userId on trigger record", event.triggerId);
                        continue;
                    }
                };

                // Credit gate and per-execution fee are enforced by the
                // orchestrator in handle_start. If the user has no credits,
                // the POST below returns 402 and we skip the trigger run.

                // Build callback URL for execution status updates (Restate calls this back)
                let dashboard_url = std::env::var("DASHBOARD_URL")
                    .unwrap_or_else(|_| "http://localhost:5174".to_string());
                let status_callback_url = format!("{}/api/executions/{}", dashboard_url, execution_id);

                let start_request = serde_json::json!({
                    "project": project,
                    "input": {
                        "triggerNodeId": event.triggerNodeId,
                        "triggerPayload": event.payload,
                    },
                    "statusCallbackUrl": status_callback_url,
                    "userId": user_id,
                    "weftCode": weft_code,
                    "triggerId": event.triggerId,
                    "nodeType": node_type,
                });

                match state.http_client.post(&executor_url)
                    .json(&start_request)
                    .send()
                    .await
                {
                    Ok(response) if response.status().is_success() => {
                        tracing::info!(
                            "Project execution {} started from trigger {}",
                            execution_id, event.triggerId
                        );
                    }
                    Ok(response) => {
                        let status = response.status();
                        let body = response.text().await.unwrap_or_default();
                        // No `executions` row exists yet at this point: the
                        // orchestrator creates it inside the same tx as the
                        // billing event, only on success. So there's nothing
                        // to update to "failed"; just log.
                        tracing::error!(
                            "Failed to start project from trigger {} (execution {}): {} - {}",
                            event.triggerId, execution_id, status, body
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to call orchestrator for trigger {} (execution {}): {}",
                            event.triggerId, execution_id, e
                        );
                    }
                }
            }
            tracing::info!("Trigger event listener stopped");
        }
    });
}

/// Load triggers from database and restart them on startup
async fn load_triggers_from_database(state: Arc<AppState>) {
    let pool = &state.db_pool;
    
    // First, recover any stale triggers from crashed instances (2 minute threshold)
    if let Err(e) = trigger_store::recover_stale_triggers(pool, 120).await {
        tracing::warn!("Failed to recover stale triggers: {}", e);
    }
    
    // Claim orphaned triggers from other instances that shut down
    // This handles the case where we restart quickly (within heartbeat threshold)
    match trigger_store::claim_orphaned_triggers(pool, &state.instance_id, 100).await {
        Ok(triggers) if !triggers.is_empty() => {
            tracing::info!("Claimed {} orphaned triggers from previous instances", triggers.len());
            restart_triggers(&state, triggers).await;
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!("Failed to claim orphaned triggers: {}", e);
        }
    }
    
    // Re-dispatch setup_pending triggers (their setup sub-execution may have been lost)
    match trigger_store::list_setup_pending_triggers(pool).await {
        Ok(triggers) if !triggers.is_empty() => {
            tracing::info!("Found {} setup_pending triggers to re-dispatch", triggers.len());
            redispatch_trigger_setups(&state, triggers).await;
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!("Failed to list setup_pending triggers: {}", e);
        }
    }

    // Claim pending triggers for this instance
    match trigger_store::claim_pending_triggers(pool, &state.instance_id, 100).await {
        Ok(triggers) if triggers.is_empty() => {
            tracing::info!("No pending triggers to restart");
        }
        Ok(triggers) => {
            tracing::info!("Restarting {} pending triggers from database", triggers.len());
            restart_triggers(&state, triggers).await;
        }
        Err(e) => {
            tracing::error!("Failed to claim pending triggers: {}", e);
        }
    }
}

/// Re-dispatch trigger setup sub-executions for triggers stuck in setup_pending state.
/// Called on startup to recover from crashes during trigger setup.
async fn redispatch_trigger_setups(state: &Arc<AppState>, triggers: Vec<trigger_store::TriggerRecord>) {
    use weft_core::project::ProjectDefinition;
    use weft_core::executor_core::ProjectExecutionRequest;

    let pool = &state.db_pool;

    for trigger in triggers {
        // project_definition is stored as Value::String(weft_code) by register_trigger.
        // Extract the weft code string and compile it into a ProjectDefinition.
        let weft_code = match &trigger.projectDefinition {
            Some(wf) => match wf.as_str() {
                Some(code) if !code.is_empty() => code.to_string(),
                _ => {
                    tracing::warn!("setup_pending trigger {} has non-string or empty project definition, marking failed", trigger.id);
                    let _ = trigger_store::update_trigger_status(pool, &trigger.id, "failed", None).await;
                    continue;
                }
            },
            None => {
                tracing::warn!("setup_pending trigger {} has no project definition, marking failed", trigger.id);
                let _ = trigger_store::update_trigger_status(pool, &trigger.id, "failed", None).await;
                continue;
            }
        };

        let project_uuid = match uuid::Uuid::parse_str(&trigger.projectId) {
            Ok(p) => p,
            Err(_) => {
                tracing::error!("Invalid project UUID for trigger {}: {}", trigger.id, trigger.projectId);
                let _ = trigger_store::update_trigger_status(pool, &trigger.id, "failed", None).await;
                continue;
            }
        };
        let mut wf: ProjectDefinition = match weft_core::weft_compiler::compile(&weft_code, project_uuid) {
            Ok(w) => w,
            Err(e) => {
                tracing::error!("Failed to compile weft code for trigger {}: {:?}", trigger.id, e);
                let _ = trigger_store::update_trigger_status(pool, &trigger.id, "failed", None).await;
                continue;
            }
        };
        if let Err(errors) = weft_nodes::enrich::enrich_project(&mut wf, state.node_registry) {
            tracing::error!("Project validation failed for trigger {}: {}", trigger.id, errors.join("; "));
            let _ = trigger_store::update_trigger_status(pool, &trigger.id, "failed", None).await;
            continue;
        }

        let sub_wf = match wf.extract_trigger_setup_subgraph(&trigger.triggerNodeId) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to extract trigger setup subgraph for {}: {}", trigger.id, e);
                let _ = trigger_store::update_trigger_status(pool, &trigger.id, "failed", None).await;
                continue;
            }
        };

        // Increment run counter and build execution ID matching register_trigger's format
        let run_counter = match trigger_store::increment_setup_run_counter(pool, &trigger.id).await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("Failed to increment setup run counter for trigger {}: {}", trigger.id, e);
                let _ = trigger_store::update_trigger_status(pool, &trigger.id, "failed", None).await;
                continue;
            }
        };
        let execution_id = format!("trigger-setup-{}-{}", trigger.id, run_counter);

        // Register the execution ID so the callback won't be rejected as stale
        let _ = trigger_store::set_setup_execution_id(pool, &trigger.id, &execution_id).await;

        let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
        let api_base = std::env::var("API_BASE_URL")
            .or_else(|_| std::env::var("WEBHOOK_BASE_URL"))
            .unwrap_or_else(|_| format!("http://localhost:{}", port));
        let callback_url = format!("{}/api/v1/triggers/{}/setup-completed", api_base, trigger.id);

        let start_req = ProjectExecutionRequest {
            project: sub_wf,
            input: serde_json::json!({
                "projectId": trigger.projectId,
                "triggerNodeId": trigger.triggerNodeId,
            }),
            userId: trigger.userId.clone(),
            statusCallbackUrl: Some(callback_url),
            isInfraSetup: false,
            isTriggerSetup: true,
            weftCode: None,
            testMode: false,
            triggerId: None,
            nodeType: None,
            mocks: None,
        };

        let url = format!("{}/ProjectExecutor/{}/start", state.executor_url, execution_id);
        match state.http_client.post(&url)
            .json(&start_req)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            Ok(_) => {
                tracing::info!("Re-dispatched trigger setup for {} (execution: {})", trigger.id, execution_id);
            }
            Err(e) => {
                tracing::error!("Failed to re-dispatch trigger setup for {}: {}", trigger.id, e);
                let _ = trigger_store::update_trigger_status(pool, &trigger.id, "failed", None).await;
            }
        }
    }
}

/// Helper to restart a list of triggers with retry and exponential backoff.
/// Retries each trigger up to ~10 minutes to give dependent infra time to come up.
async fn restart_triggers(state: &Arc<AppState>, triggers: Vec<trigger_store::TriggerRecord>) {
    let pool = &state.db_pool;
    let backoff_delays = [5, 10, 15, 30, 60, 120, 180];
    
    for trigger in triggers {
        let mut succeeded = false;
        let max_attempts = backoff_delays.len() + 1;
        
        for attempt in 0..max_attempts {
            let start_config = weft_nodes::TriggerStartConfig {
                id: trigger.id.clone(),
                projectId: trigger.projectId.clone(),
                triggerNodeId: trigger.triggerNodeId.clone(),
                config: trigger.config.clone(),
                credentials: trigger.credentials.clone(),
            };

            let service = state.trigger_service.lock().await;
            match service.register_trigger(start_config, &trigger.triggerCategory).await {
                Ok(_) => {
                    tracing::info!("Restarted trigger {} ({})", trigger.id, trigger.triggerCategory);
                    succeeded = true;
                    break;
                }
                Err(e) => {
                    if attempt < backoff_delays.len() {
                        let delay = backoff_delays[attempt];
                        tracing::warn!(
                            "Failed to restart trigger {} (attempt {}/{}): {}. Retrying in {}s...",
                            trigger.id, attempt + 1, max_attempts, e, delay
                        );
                        drop(service);
                        tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                    } else {
                        tracing::error!(
                            "Failed to restart trigger {} after {} attempts: {}",
                            trigger.id, max_attempts, e
                        );
                    }
                }
            }
        }
        
        if !succeeded {
            let _ = trigger_store::update_trigger_status(pool, &trigger.id, "failed", None).await;
        }
    }
}

/// Background task for trigger maintenance:
/// - Heartbeat: Update last_heartbeat for owned triggers
/// - Recovery: Claim triggers from crashed instances
fn start_trigger_maintenance_task(state: Arc<AppState>) {
    tokio::spawn(async move {
        let pool = &state.db_pool;
        
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        
        loop {
            interval.tick().await;
            
            // Update heartbeat for our triggers
            match trigger_store::update_heartbeat(pool, &state.instance_id).await {
                Ok(count) if count > 0 => {
                    tracing::debug!("Updated heartbeat for {} triggers", count);
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("Failed to update trigger heartbeat: {}", e);
                }
            }
            
            // Recover stale triggers (2 minute threshold)
            if let Err(e) = trigger_store::recover_stale_triggers(pool, 120).await {
                tracing::warn!("Failed to recover stale triggers: {}", e);
            }

            // Sweep orphan triggers: any `triggers` row whose `project_id`
            // does not reference an existing `projects` row. This happens
            // when a project is deleted through a path that doesn't
            // cascade to triggers (e.g. the historical `deleteProject`
            // bug that left weather_cron orphans firing every 10s).
            // triggers.project_id is TEXT with no FK to projects(id),
            // so we can't rely on ON DELETE CASCADE here.
            //
            // Three-phase to keep the TriggerService mutex held for the
            // shortest possible window:
            //   1. Scan for orphans (no lock).
            //   2. Acquire the service mutex, unregister each orphan
            //      handle, drop the mutex.
            //   3. Batch-delete the orphan rows from the DB in one
            //      `DELETE ... WHERE id = ANY($1)` query (no lock).
            // Without this, a sweep finding N orphans would hold the
            // mutex across N sequential DB round-trips, blocking every
            // other trigger operation for the duration.
            let orphan_ids: Vec<String> = match sqlx::query_as::<_, (String,)>(
                r#"
                SELECT t.id
                FROM triggers t
                LEFT JOIN projects p ON p.id::text = t.project_id
                WHERE p.id IS NULL
                "#,
            )
            .fetch_all(pool)
            .await
            {
                Ok(rows) => rows.into_iter().map(|(id,)| id).collect(),
                Err(e) => {
                    tracing::warn!("Failed to scan for orphan triggers: {}", e);
                    Vec::new()
                }
            };
            if !orphan_ids.is_empty() {
                tracing::warn!("Found {} orphan triggers with missing project rows, cleaning up", orphan_ids.len());

                // Phase 2: tear down dispatcher handles under the lock.
                // We hold the mutex for the unregister loop only, not
                // for the DB delete below.
                {
                    let service = state.trigger_service.lock().await;
                    for trigger_id in orphan_ids.iter() {
                        if let Err(e) = service.unregister_trigger(trigger_id).await {
                            tracing::warn!("Failed to unregister orphan trigger {} from dispatcher: {}", trigger_id, e);
                        }
                    }
                }

                // Phase 3: one batch DELETE for every orphan row.
                // Using ANY($1::text[]) avoids a round-trip per row.
                if let Err(e) = sqlx::query("DELETE FROM triggers WHERE id = ANY($1)")
                    .bind(&orphan_ids)
                    .execute(pool)
                    .await
                {
                    tracing::error!("Failed to batch-delete {} orphan triggers: {}", orphan_ids.len(), e);
                } else {
                    tracing::info!("Removed {} orphan triggers", orphan_ids.len());
                }
            }

            // Try to claim any newly pending triggers
            match trigger_store::claim_pending_triggers(pool, &state.instance_id, 10).await {
                Ok(triggers) if !triggers.is_empty() => {
                    tracing::info!("Claimed {} new triggers", triggers.len());
                    
                    let service = state.trigger_service.lock().await;
                    
                    for trigger in triggers {
                        let start_config = weft_nodes::TriggerStartConfig {
                            id: trigger.id.clone(),
                            projectId: trigger.projectId.clone(),
                            triggerNodeId: trigger.triggerNodeId.clone(),
                            config: trigger.config.clone(),
                            credentials: trigger.credentials.clone(),
                        };
                        
                        // register_trigger checks requiresRunningInstance feature flag internally
                        if let Err(e) = service.register_trigger(start_config, &trigger.triggerCategory).await {
                            tracing::error!("Failed to start claimed trigger {}: {}", trigger.id, e);
                            let _ = trigger_store::update_trigger_status(pool, &trigger.id, "failed", None).await;
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("Failed to claim pending triggers: {}", e);
                }
            }
        }
    });
}

/// Backfill daily usage aggregation for any days missed during server downtime.
async fn backfill_usage_on_startup(state: Arc<AppState>) {
    match usage_store::backfill_daily(&state.db_pool).await {
        Ok(0) => tracing::info!("Usage aggregation: no backfill needed"),
        Ok(rows) => tracing::info!("Usage aggregation: backfilled {} rows", rows),
        Err(e) => tracing::warn!("Failed to backfill usage aggregation: {}", e),
    }
}

/// Background task that aggregates usage_events into usage_daily once per hour.
fn start_usage_aggregation_task(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut agg_interval = tokio::time::interval(std::time::Duration::from_secs(3600));

        loop {
            agg_interval.tick().await;

            let today = chrono::Utc::now().date_naive();

            // Aggregate usage events into daily summaries
            match usage_store::aggregate_daily(&state.db_pool, today).await {
                Ok(rows) if rows > 0 => {
                    tracing::info!("Usage aggregation: {} rows for {}", rows, today);
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("Usage aggregation failed for {}: {}", today, e);
                }
            }
        }
    });
}

/// Build CORS layer based on environment configuration.
/// 
/// Environment variables:
/// - `DEPLOYMENT_MODE`: "local" (default) or "cloud"
/// - `ALLOWED_ORIGINS`: Comma-separated list of allowed origins (cloud mode only)
/// 
/// Local mode: Allows any localhost origin (standalone development)
/// Cloud mode: Only allows origins specified in ALLOWED_ORIGINS
fn build_cors_layer() -> CorsLayer {
    let deployment_mode = std::env::var("DEPLOYMENT_MODE").unwrap_or_else(|_| "local".to_string());
    let is_local = deployment_mode.to_lowercase() == "local";
    
    if is_local {
        tracing::info!("CORS mode: local (allowing localhost origins)");
        // Local mode: allow any localhost origin
        CorsLayer::new()
            .allow_origin(tower_http::cors::AllowOrigin::predicate(|origin, _| {
                origin.as_bytes().starts_with(b"http://localhost:")
                    || origin.as_bytes().starts_with(b"http://127.0.0.1:")
                    || origin == "http://localhost"
                    || origin == "http://127.0.0.1"
            }))
            .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS])
            .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION, header::ACCEPT, header::COOKIE, header::HeaderName::from_static("x-user-id")])
            .allow_credentials(true)
    } else {
        // Cloud mode: only allow explicitly configured origins
        let allowed_origins_str = std::env::var("ALLOWED_ORIGINS")
            .unwrap_or_else(|_| "https://app.weavemind.ai,https://weavemind.ai".to_string());
        
        let allowed_origins: Vec<HeaderValue> = allowed_origins_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        
        tracing::info!("CORS mode: cloud (allowed origins: {:?})", allowed_origins);
        
        CorsLayer::new()
            .allow_origin(allowed_origins)
            .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS])
            .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION, header::ACCEPT, header::COOKIE, header::HeaderName::from_static("x-user-id")])
            .allow_credentials(true)
    }
}
