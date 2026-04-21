//! Form input primitive.
//!
//! Provides `request_form_input()` on `ExecutionContext`, allowing any node
//! to pause mid-execution, present a form to a consumer (human, AI, or other),
//! and continue when the form is submitted. Can be called multiple times in a loop.
//!
//! Consumers filter tasks by metadata to decide which forms they handle.
//! For example, the browser extension filters for `{ "source": "human" }`.
//!
//! Under the hood this works by:
//! 1. Registering a oneshot channel keyed by a unique callback_id
//! 2. POSTing a WaitingForInput callback to the executor (which registers the task)
//! 3. Awaiting the oneshot receiver
//! 4. The executor forwards form input to `/input_response/{callback_id}` on this service
//! 5. The handler resolves the channel, the node continues

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, oneshot};
use weft_core::{FormSchema, NodeCallbackRequest, NodeExecutionStatus, WaitingMetadata};

/// Shared channel map for pending form input requests.
/// Lives on the NodeRunner and is shared with ExecutionContext via Arc.
pub struct FormInputChannels {
    pending: Mutex<HashMap<String, oneshot::Sender<serde_json::Value>>>,
    /// Monotonic counter for generating unique callback IDs when a node
    /// calls request_form_input multiple times in a loop.
    seq: AtomicU64,
}

impl std::fmt::Debug for FormInputChannels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FormInputChannels").finish_non_exhaustive()
    }
}

impl FormInputChannels {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            seq: AtomicU64::new(0),
        }
    }

    /// Generate a unique callback ID for a form input request.
    pub fn next_callback_id(&self, execution_id: &str, node_id: &str, pulse_id: &str) -> String {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        format!("{}-{}-{}-{}", execution_id, node_id, pulse_id, seq)
    }

    /// Register a new channel and return the receiver.
    pub async fn register(&self, callback_id: &str) -> oneshot::Receiver<serde_json::Value> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(callback_id.to_string(), tx);
        rx
    }

    /// Resolve a pending channel with the form response. Returns false if not found.
    pub async fn resolve(&self, callback_id: &str, value: serde_json::Value) -> bool {
        let mut map = self.pending.lock().await;
        if let Some(tx) = map.remove(callback_id) {
            let _ = tx.send(value);
            true
        } else {
            false
        }
    }

    /// Remove a pending channel without resolving (e.g., on cancellation).
    pub async fn cancel(&self, callback_id: &str) {
        self.pending.lock().await.remove(callback_id);
    }
}

/// Request for form input. Built by the node, sent to the executor.
pub struct FormInputRequest {
    pub title: Option<String>,
    pub description: Option<String>,
    pub form_schema: FormSchema,
    /// Free-form metadata for consumer filtering (e.g., { "source": "human" }).
    pub metadata: serde_json::Value,
}

impl FormInputRequest {
    pub fn new(form_schema: FormSchema) -> Self {
        Self {
            title: None,
            description: None,
            form_schema,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Send a WaitingForInput callback to the executor and await the form response.
///
/// This is the core implementation called by `ExecutionContext::request_form_input()`.
pub(crate) async fn request_form_input_impl(
    channels: &Arc<FormInputChannels>,
    client: &reqwest::Client,
    callback_url: &str,
    execution_id: &str,
    node_id: &str,
    pulse_id: &str,
    request: FormInputRequest,
) -> Result<serde_json::Value, String> {
    let callback_id = channels.next_callback_id(execution_id, node_id, pulse_id);

    // 1. Register the channel before sending the callback (avoid race condition)
    let rx = channels.register(&callback_id).await;

    // 2. POST WaitingForInput to the executor
    let callback = NodeCallbackRequest {
        executionId: execution_id.to_string(),
        nodeId: node_id.to_string(),
        status: NodeExecutionStatus::WaitingForInput,
        output: None,
        error: None,
        waitingMetadata: Some(WaitingMetadata {
            callbackId: callback_id.clone(),
            title: request.title,
            description: request.description,
            formSchema: Some(request.form_schema),
            metadata: request.metadata,
        }),
        pulseId: pulse_id.to_string(),
        costUsd: 0.0,
    };

    let resp = client.post(callback_url).json(&callback).send().await;
    match resp {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => {
            channels.cancel(&callback_id).await;
            return Err(format!("Executor rejected WaitingForInput callback: {}", r.status()));
        }
        Err(e) => {
            channels.cancel(&callback_id).await;
            return Err(format!("Failed to send WaitingForInput callback: {}", e));
        }
    }

    // 3. Await the form response
    match rx.await {
        Ok(value) => Ok(value),
        Err(_) => Err("Form input channel was dropped (node service shutting down?)".to_string()),
    }
}

