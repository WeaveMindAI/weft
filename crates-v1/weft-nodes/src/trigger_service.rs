//! Unified TriggerService - manages trigger instances using the NodeTypeRegistry.
//!
//! This service is fully generic - it uses the NodeTypeRegistry to find nodes by type
//! and delegates trigger runtime to each node's `keep_alive` method.
//!
//! Trigger categories:
//! - Webhook: Handled by HTTP endpoints, no instance needed
//! - Polling: Generic polling mechanism  
//! - Schedule: Cron-based scheduling
//! - Socket: Persistent connections (WebSocket, etc.)
//! - Local: Only runs in standalone mode
//! - Manual: Triggered manually by user

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use crate::node::{
    TriggerContext, TriggerEvent, TriggerEventSender, TriggerError,
    TriggerStartConfig, TriggerHandle,
};
use crate::form_registrar::FormRegistrar;
use crate::registry::NodeTypeRegistry;

/// Information about a trigger for status display
#[derive(Debug, Clone, serde::Serialize)]
#[allow(non_snake_case)]
pub struct TriggerInfo {
    pub triggerId: String,
    pub triggerCategory: String,
    pub projectId: String,
    pub status: String,
    pub projectHash: Option<String>,
}

/// Unified trigger service that works with Node-based triggers.
/// 
/// Uses the NodeTypeRegistry to find nodes and delegates trigger runtime
/// to each node's `keep_alive` method - no node-specific code here.
/// Maps trigger_id → form submission sender, for routing form submissions
/// from the API to the waiting trigger.
pub type FormSubmissionSenders = Arc<RwLock<HashMap<String, mpsc::UnboundedSender<crate::form_registrar::FormSubmission>>>>;

pub struct TriggerService {
    handles: Arc<RwLock<HashMap<String, TriggerHandle>>>,
    registry: &'static NodeTypeRegistry,
    event_tx: mpsc::UnboundedSender<TriggerEvent>,
    event_rx: Option<mpsc::UnboundedReceiver<TriggerEvent>>,
    http_client: reqwest::Client,
    restate_url: String,
    /// Senders for form submissions, keyed by trigger_id.
    /// The API layer uses this to route submissions to the right trigger.
    pub form_submission_senders: FormSubmissionSenders,
}

impl TriggerService {
    pub fn with_registry(registry: &'static NodeTypeRegistry) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let restate_url = std::env::var("RESTATE_URL")
            .unwrap_or_else(|_| "http://localhost:9070".to_string());
        Self {
            handles: Arc::new(RwLock::new(HashMap::new())),
            registry,
            event_tx,
            event_rx: Some(event_rx),
            http_client: reqwest::Client::new(),
            restate_url,
            form_submission_senders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<TriggerEvent>> {
        self.event_rx.take()
    }

    pub fn event_sender(&self) -> TriggerEventSender {
        self.event_tx.clone()
    }

    /// Register and start a trigger using the NodeTypeRegistry.
    /// 
    /// The config must contain a `nodeType` field specifying which node to use.
    /// The service looks up the node in the registry and calls its `keep_alive` method.
    pub async fn register_trigger(&self, config: TriggerStartConfig, trigger_category: &str) -> Result<(), TriggerError> {
        tracing::info!("register_trigger called: category={}, config={:?}", trigger_category, config.config);
        
        // Get the node type from config - required for all triggers
        let node_type = config.config
            .get("nodeType")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                tracing::error!("Missing 'nodeType' in trigger config: {:?}", config.config);
                TriggerError::Config(
                    "Missing 'nodeType' in trigger config. The dashboard must include the node type.".to_string()
                )
            })?;
        
        tracing::info!("Looking up node type: {}", node_type);
        tracing::info!("Registry has {} nodes: {:?}", self.registry.len(), self.registry.all_types());
        
        // Look up the node in the registry
        let node = self.registry.get(&node_type)
            .ok_or_else(|| {
                tracing::error!("Unknown node type: {}. Available: {:?}", node_type, self.registry.all_types());
                TriggerError::Config(format!("Unknown node type: {}", node_type))
            })?;
        
        // Verify this node is actually a trigger
        let metadata = node.metadata();
        if !metadata.features.isTrigger {
            return Err(TriggerError::Config(format!("Node type {} is not a trigger", node_type)));
        }
        
        // Check if this trigger requires a running instance
        if !metadata.features.requiresRunningInstance {
            tracing::info!("Trigger {} (node: {}) doesn't require a running instance", config.id, node_type);
            return Ok(());
        }
        
        // Create a FormRegistrar for this trigger
        let registrar = FormRegistrar::new(
            self.http_client.clone(),
            self.restate_url.clone(),
            config.config.get("userId").and_then(|v| v.as_str()).map(|s| s.to_string()),
            config.id.clone(),
        );

        // Store the submission sender so the API can route submissions to this trigger
        self.form_submission_senders.write().await.insert(
            config.id.clone(),
            registrar.submission_sender(),
        );

        // Delegate to the node's keep_alive method
        let handle = node.keep_alive(config, TriggerContext::new(self.event_tx.clone()).with_form_registrar(registrar)).await?;

        // Store the handle
        let trigger_id = handle.triggerId.clone();
        self.handles.write().await.insert(trigger_id.clone(), handle);

        tracing::info!("Trigger {} started via node type {}", trigger_id, node_type);
        Ok(())
    }

    pub async fn unregister_trigger(&self, trigger_id: &str) -> Result<(), TriggerError> {
        if let Some(mut handle) = self.handles.write().await.remove(trigger_id) {
            handle.stop();
        }
        // Clean up form submission sender
        self.form_submission_senders.write().await.remove(trigger_id);
        Ok(())
    }

    pub async fn stop_trigger(&self, trigger_id: &str) -> Result<(), TriggerError> {
        let mut handles = self.handles.write().await;
        if let Some(handle) = handles.get_mut(trigger_id) {
            handle.stop();
        }
        Ok(())
    }

    pub async fn stop_all(&self) -> Result<(), TriggerError> {
        let mut handles = self.handles.write().await;
        for (id, handle) in handles.iter_mut() {
            handle.stop();
            tracing::info!("Stopped trigger {}", id);
        }
        Ok(())
    }

    pub async fn list_triggers(&self) -> Vec<String> {
        self.handles.read().await.keys().cloned().collect()
    }

    pub async fn get_trigger_info(&self, trigger_id: &str) -> Option<TriggerInfo> {
        let handles = self.handles.read().await;
        handles.get(trigger_id).map(|handle| TriggerInfo {
            triggerId: handle.triggerId.clone(),
            triggerCategory: handle.triggerCategory.to_string(),
            projectId: handle.projectId.clone(),
            status: format!("{:?}", handle.status),
            projectHash: None,
        })
    }

    pub async fn list_trigger_infos(&self) -> Vec<TriggerInfo> {
        let handles = self.handles.read().await;
        handles.values().map(|handle| {
            TriggerInfo {
                triggerId: handle.triggerId.clone(),
                triggerCategory: handle.triggerCategory.to_string(),
                projectId: handle.projectId.clone(),
                status: format!("{:?}", handle.status),
                projectHash: None,
            }
        }).collect()
    }
}

