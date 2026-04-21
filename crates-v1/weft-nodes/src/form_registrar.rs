//! Form registrar for triggers.
//!
//! Provides the ability for trigger nodes to register persistent forms in the
//! TaskRegistry and wait for submissions. This is a general system feature
//! that any trigger node can use.
//!
//! The registrar handles:
//! - Registering a form (with metadata for consumer filtering)
//! - Waiting for form submissions (via a channel)
//! - Re-registering after submission (for persistent triggers)
//! - Cleanup when the trigger shuts down

use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use weft_core::executor_core::{PendingTask, TaskType, FormSchema};

/// A submission received from a form consumer.
#[derive(Debug, Clone)]
pub struct FormSubmission {
    /// The form response data (field key → value).
    pub data: serde_json::Value,
}

/// Registrar that allows triggers to register forms and receive submissions.
///
/// Created by the TriggerService and passed to `keep_alive()`.
/// The trigger calls `register_form()` to make a form available, then
/// `wait_for_submission()` to block until someone submits it.
#[derive(Clone)]
pub struct FormRegistrar {
    /// HTTP client for TaskRegistry calls
    http_client: reqwest::Client,
    /// Restate URL for TaskRegistry operations
    restate_url: String,
    /// Receiver for form submissions (shared across clones)
    submission_rx: Arc<Mutex<mpsc::UnboundedReceiver<FormSubmission>>>,
    /// Sender for form submissions (used by the submission handler)
    submission_tx: mpsc::UnboundedSender<FormSubmission>,
    /// User ID for task registration
    user_id: Option<String>,
    /// Trigger ID for unique task identification
    trigger_id: String,
}

impl FormRegistrar {
    pub fn new(
        http_client: reqwest::Client,
        restate_url: String,
        user_id: Option<String>,
        trigger_id: String,
    ) -> Self {
        let (submission_tx, submission_rx) = mpsc::unbounded_channel();
        Self {
            http_client,
            restate_url,
            submission_rx: Arc::new(Mutex::new(submission_rx)),
            submission_tx,
            user_id,
            trigger_id,
        }
    }

    /// Get the submission sender. Used by the external handler that receives
    /// form submissions and forwards them to the waiting trigger.
    pub fn submission_sender(&self) -> mpsc::UnboundedSender<FormSubmission> {
        self.submission_tx.clone()
    }

    /// Register a form in the TaskRegistry as a persistent trigger form.
    pub async fn register_form(
        &self,
        title: String,
        description: Option<String>,
        form_schema: FormSchema,
        metadata: serde_json::Value,
    ) -> Result<String, String> {
        let task_id = format!("trigger-{}", self.trigger_id);

        let task = PendingTask {
            executionId: task_id.clone(),
            nodeId: self.trigger_id.clone(),
            title,
            description,
            data: serde_json::Value::Null,
            createdAt: chrono::Utc::now().to_rfc3339(),
            userId: self.user_id.clone(),
            taskType: TaskType::Trigger,
            actionUrl: None,
            formSchema: Some(form_schema),
            metadata,
        };

        let url = format!("{}/TaskRegistry/global/register_task", self.restate_url);
        let resp = self.http_client.post(&url).json(&task).send().await
            .map_err(|e| format!("Failed to register trigger form: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("TaskRegistry rejected trigger form: {}", resp.status()));
        }

        tracing::info!("[FormRegistrar] Registered trigger form '{}' for trigger {}", task_id, self.trigger_id);
        Ok(task_id)
    }

    /// Wait for a form submission. Blocks until someone submits the form.
    pub async fn wait_for_submission(&self) -> Option<FormSubmission> {
        self.submission_rx.lock().await.recv().await
    }

    /// Remove the trigger form from the TaskRegistry (cleanup on shutdown).
    pub async fn unregister_form(&self) {
        let task_id = format!("trigger-{}", self.trigger_id);
        let url = format!("{}/TaskRegistry/global/remove_task", self.restate_url);
        if let Err(e) = self.http_client.post(&url).json(&task_id).send().await {
            tracing::warn!("[FormRegistrar] Failed to unregister trigger form: {}", e);
        }
    }
}
