//! Core Node trait and related types for the unified node system.
//!
//! Each node type implements the `Node` trait and registers itself using `inventory::submit!`.
//! The `NodeRegistry` collects all registered nodes at startup.
//!
//! Nodes can optionally be triggers by setting `isTrigger: true` in their features
//! and implementing the trigger lifecycle methods (`start_trigger`, `stop_trigger`).

use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use weft_core::NodeExecuteRequest;
use crate::NodeResult;
use crate::form_input::{FormInputChannels, FormInputRequest};

// Re-export NodeFeatures and TriggerCategory from weft-core
pub use weft_core::{NodeFeatures, TriggerCategory};
// Re-export port types from weft-core (single source of truth)
pub use weft_core::{WeftType, WeftPrimitive};
pub use weft_core::project::{LaneMode, PortDefinition};

/// Config field types (mirrors frontend FieldType)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Text,
    Textarea,
    Code,
    Select,
    Multiselect,
    Number,
    Checkbox,
    Password,
    Blob,
    ApiKey,
    FormBuilder,
}

/// Definition of a config field (mirrors frontend FieldDefinition)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    pub key: &'static str,
    pub field_type: FieldType,
    #[serde(default)]
    pub default_value: Option<serde_json::Value>,
    #[serde(default)]
    pub min: Option<f64>,
    #[serde(default)]
    pub max: Option<f64>,
    #[serde(default)]
    pub options: Vec<&'static str>,
    #[serde(default)]
    pub provider: Option<&'static str>,
    #[serde(default)]
    pub accept: Option<&'static str>,
}

impl FieldDef {
    pub fn new(key: &'static str, field_type: FieldType) -> Self {
        Self { key, field_type, default_value: None, min: None, max: None, options: vec![], provider: None, accept: None }
    }

    pub fn with_default(mut self, value: serde_json::Value) -> Self {
        self.default_value = Some(value);
        self
    }

    pub fn with_range(mut self, min: f64, max: f64) -> Self {
        self.min = Some(min);
        self.max = Some(max);
        self
    }

    pub fn with_options(mut self, options: Vec<&'static str>) -> Self {
        self.options = options;
        self
    }

    pub fn with_provider(mut self, provider: &'static str) -> Self {
        self.provider = Some(provider);
        self
    }

    pub fn api_key(key: &'static str, provider: &'static str) -> Self {
        Self::new(key, FieldType::ApiKey).with_provider(provider)
    }

    pub fn number(key: &'static str) -> Self {
        Self::new(key, FieldType::Number)
    }

    pub fn text(key: &'static str) -> Self {
        Self::new(key, FieldType::Text)
    }

    pub fn textarea(key: &'static str) -> Self {
        Self::new(key, FieldType::Textarea)
    }

    pub fn checkbox(key: &'static str) -> Self {
        Self::new(key, FieldType::Checkbox)
    }

    pub fn code(key: &'static str) -> Self {
        Self::new(key, FieldType::Code)
    }

    pub fn select(key: &'static str, options: Vec<&'static str>) -> Self {
        Self::new(key, FieldType::Select).with_options(options)
    }

    pub fn password(key: &'static str) -> Self {
        Self::new(key, FieldType::Password)
    }

    pub fn blob(key: &'static str, accept: &'static str) -> Self {
        Self { accept: Some(accept), ..Self::new(key, FieldType::Blob) }
    }
}

/// Return type for Node::resolve_types()
#[derive(Debug, Clone, Default)]
pub struct ResolvedTypes {
    pub inputs: Vec<(String, WeftType)>,
    pub outputs: Vec<(String, WeftType)>,
}

/// Definition of a port (input or output)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortDef {
    pub name: &'static str,
    pub portType: WeftType,
    pub required: bool,
    #[serde(default)]
    pub laneMode: LaneMode,
    /// Whether this port can be filled by a same-named config field on the
    /// node (in addition to being wired by an edge). Defaults to true unless
    /// the type is a Media leaf or otherwise non-configurable. Catalog
    /// authors override per port via PortDef::wired_only(...).
    #[serde(default)]
    pub configurable: bool,
}

/// Convenience functions for creating PortDefs.
/// Type is specified as a string literal matching Weft syntax:
///   "String", "Number", "Boolean", "Image", "Video", "Audio", "Document",
///   "List[String]", "Dict[String, Number]", "List[List[String]]",
///   "String | Number", "Media", "Dict", "List"
impl PortDef {
    pub fn new(name: &'static str, type_str: &str, required: bool) -> Self {
        let portType = WeftType::parse(type_str)
            .unwrap_or_else(|| panic!("invalid port type: {}", type_str));
        let configurable = portType.is_default_configurable();
        Self {
            name,
            portType,
            required,
            laneMode: LaneMode::Single,
            configurable,
        }
    }

    /// Like `new`, but explicitly opts the port out of config-fillability.
    /// Use this when the port must be wired (e.g. it expects an object that
    /// only makes sense as a wire-time value, like a streaming handle).
    pub fn wired_only(name: &'static str, type_str: &str, required: bool) -> Self {
        let mut p = Self::new(name, type_str, required);
        p.configurable = false;
        p
    }

    pub fn typed(name: &'static str, port_type: WeftType, required: bool) -> Self {
        let configurable = port_type.is_default_configurable();
        Self { name, portType: port_type, required, laneMode: LaneMode::Single, configurable }
    }

    pub fn gather(name: &'static str, type_str: &str, required: bool) -> Self {
        let portType = WeftType::parse(type_str)
            .unwrap_or_else(|| panic!("invalid port type: {}", type_str));
        let configurable = portType.is_default_configurable();
        Self {
            name,
            portType,
            required,
            laneMode: LaneMode::Gather,
            configurable,
        }
    }

    pub fn expand(name: &'static str, type_str: &str, required: bool) -> Self {
        let portType = WeftType::parse(type_str)
            .unwrap_or_else(|| panic!("invalid port type: {}", type_str));
        let configurable = portType.is_default_configurable();
        Self {
            name,
            portType,
            required,
            laneMode: LaneMode::Expand,
            configurable,
        }
    }

}

/// Lane mode overrides for specific ports, returned by Node::lane_modes().
/// Maps port name to LaneMode. Ports not listed default to Single.
pub type LaneModeMap = Vec<(&'static str, LaneMode)>;

/// Specification for a form field type in hasFormSchema nodes.
/// Defines which input and output ports each field type contributes.
/// Port name templates use `{key}` as placeholder for the field's key.
#[derive(Debug, Clone)]
pub struct FormFieldSpec {
    pub field_type: &'static str,
    /// Default render config for this field type. Used when the field JSON
    /// doesn't include an explicit `render` object.
    pub render: serde_json::Value,
    pub adds_inputs: Vec<FormFieldPort>,
    pub adds_outputs: Vec<FormFieldPort>,
}

/// A port template contributed by a form field spec.
#[derive(Debug, Clone)]
pub struct FormFieldPort {
    pub name_template: &'static str,
    pub port_type: WeftType,
}

impl FormFieldPort {
    pub fn new(name_template: &'static str, type_str: &str) -> Self {
        Self {
            name_template,
            port_type: WeftType::parse(type_str)
                .unwrap_or_else(|| panic!("invalid port type: {}", type_str)),
        }
    }

    /// Port template accepting any type, independent from sibling ports.
    /// See `T_Auto` marker handling in enrich.rs.
    pub fn any(name_template: &'static str) -> Self {
        Self { name_template, port_type: WeftType::type_var("T_Auto") }
    }

    /// Resolve the port name by replacing `{key}` with the actual field key.
    pub fn resolve_name(&self, key: &str) -> String {
        self.name_template.replace("{key}", key)
    }
}

/// Status of a running trigger
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum TriggerStatus {
    #[default]
    Stopped,
    Starting,
    Running,
    Error,
}

/// Event emitted by a trigger when it fires
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerEvent {
    pub triggerId: String,
    pub projectId: String,
    pub triggerNodeId: String,
    pub payload: serde_json::Value,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Channel for sending trigger events
pub type TriggerEventSender = mpsc::UnboundedSender<TriggerEvent>;

/// Signal that resolves when the trigger should shut down.
pub type ShutdownSignal = tokio::sync::oneshot::Receiver<()>;

/// Emits trigger events. The node only provides the payload; the framework
/// wraps it in a full `TriggerEvent` with IDs and timestamp.
#[derive(Clone)]
pub struct Emitter {
    trigger_id: String,
    project_id: String,
    trigger_node_id: String,
    sender: TriggerEventSender,
}

impl Emitter {
    pub fn emit(&self, payload: serde_json::Value) -> Result<(), TriggerError> {
        let event = TriggerEvent {
            triggerId: self.trigger_id.clone(),
            projectId: self.project_id.clone(),
            triggerNodeId: self.trigger_node_id.clone(),
            payload,
            timestamp: chrono::Utc::now(),
        };
        self.sender.send(event)
            .map_err(|_| TriggerError::Connection("Event channel closed".to_string()))
    }
}

/// Context passed to trigger `keep_alive`. Contains the event sender and
/// optional system features (form registrar, etc.). Triggers only access
/// the features they need.
pub struct TriggerContext {
    pub event_sender: TriggerEventSender,
    pub form_registrar: Option<crate::form_registrar::FormRegistrar>,
}

impl TriggerContext {
    pub fn new(event_sender: TriggerEventSender) -> Self {
        Self { event_sender, form_registrar: None }
    }

    pub fn with_form_registrar(mut self, registrar: crate::form_registrar::FormRegistrar) -> Self {
        self.form_registrar = Some(registrar);
        self
    }

    /// Spawn a trigger event loop. Handles all infrastructure plumbing:
    /// shutdown channel, TriggerHandle construction, and event wrapping.
    ///
    /// The closure receives an `Emitter` (call `.emit(payload)` to fire events)
    /// and a `ShutdownSignal` (resolves when the trigger should stop).
    /// Return `Ok(())` on clean shutdown, `Err` on fatal error.
    pub fn spawn<F, Fut>(self,
        config: &TriggerStartConfig,
        category: TriggerCategory,
        f: F,
    ) -> Result<TriggerHandle, TriggerError>
    where
        F: FnOnce(Emitter, ShutdownSignal) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), TriggerError>> + Send + 'static,
    {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let emitter = Emitter {
            trigger_id: config.id.clone(),
            project_id: config.projectId.clone(),
            trigger_node_id: config.triggerNodeId.clone(),
            sender: self.event_sender,
        };

        let trigger_id = config.id.clone();
        tokio::spawn(async move {
            if let Err(e) = f(emitter, shutdown_rx).await {
                tracing::error!("Trigger {} event loop failed: {}", trigger_id, e);
            }
        });

        Ok(TriggerHandle::new(
            config.id.clone(),
            config.projectId.clone(),
            category,
            shutdown_tx,
        ))
    }
}

/// Configuration for starting a trigger
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerStartConfig {
    pub id: String,
    pub projectId: String,
    pub triggerNodeId: String,
    pub config: serde_json::Value,
    pub credentials: Option<serde_json::Value>,
}

impl TriggerStartConfig {
    /// Get a required string from the config. Returns `TriggerError::Config` if missing or empty.
    pub fn require_str(&self, key: &str) -> Result<String, TriggerError> {
        let val = self.config.get(key)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| TriggerError::Config(format!("Missing '{}' in config", key)))?;
        Ok(val.to_string())
    }

    /// Get an optional string from the config. Returns `None` if missing or empty.
    pub fn get_str(&self, key: &str) -> Option<String> {
        self.config.get(key)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    }

    /// Get an optional u64 from the config (handles both number and string values).
    pub fn get_u64(&self, key: &str) -> Option<u64> {
        self.config.get(key)
            .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
    }

    /// Deserialize the entire config into a typed struct.
    pub fn parse_config<T: serde::de::DeserializeOwned>(&self) -> Result<T, TriggerError> {
        serde_json::from_value(self.config.clone())
            .map_err(|e| TriggerError::Config(format!("Invalid config: {}", e)))
    }
}

/// Handle returned when a trigger is started - used to stop it
pub struct TriggerHandle {
    pub triggerId: String,
    pub projectId: String,
    pub triggerCategory: TriggerCategory,
    pub status: TriggerStatus,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl TriggerHandle {
    pub fn new(
        trigger_id: String,
        project_id: String,
        trigger_category: TriggerCategory,
        shutdown_tx: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        Self {
            triggerId: trigger_id,
            projectId: project_id,
            triggerCategory: trigger_category,
            status: TriggerStatus::Running,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.status = TriggerStatus::Stopped;
    }
}

/// Error type for trigger operations
#[derive(Debug, thiserror::Error)]
pub enum TriggerError {
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("Not a trigger node")]
    NotATrigger,
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Metadata about a node type for UI and discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetadata {
    pub label: &'static str,
    pub inputs: Vec<PortDef>,
    pub outputs: Vec<PortDef>,
    pub features: NodeFeatures,
    #[serde(default)]
    pub fields: Vec<FieldDef>,
}

/// Execution context passed to nodes
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    pub executionId: String,
    pub nodeId: String,
    pub nodeType: String,
    pub config: serde_json::Value,
    pub input: serde_json::Value,
    pub userId: Option<String>,
    pub projectId: Option<String>,
    pub isInfraSetup: bool,
    pub isTriggerSetup: bool,
    pub pulseId: String,
    pub callbackUrl: String,
    /// Shared HTTP client for making outbound requests (callbacks, etc.).
    /// Set by the runner before execute(). Reused across calls to avoid
    /// allocating connection pools per request.
    pub http_client: reqwest::Client,
    /// Channel map for form input requests. Set by the runner before execute().
    pub form_input_channels: Option<Arc<FormInputChannels>>,
    /// Accumulated cost in microdollars (USD * 1_000_000). Incremented automatically
    /// by report_usage_cost and tracked_ai_context. Read by the runner after execute().
    /// Node authors never touch this directly.
    pub cost_accumulator: Arc<std::sync::atomic::AtomicU64>,
}

impl From<NodeExecuteRequest> for ExecutionContext {
    fn from(req: NodeExecuteRequest) -> Self {
        Self {
            executionId: req.executionId,
            nodeId: req.nodeId,
            nodeType: req.nodeType,
            config: req.config,
            input: req.input,
            userId: req.userId,
            projectId: req.projectId,
            isInfraSetup: req.isInfraSetup,
            isTriggerSetup: req.isTriggerSetup,
            pulseId: req.pulseId,
            callbackUrl: req.callbackUrl,
            http_client: reqwest::Client::new(), // Overridden by runner before execute()
            form_input_channels: None, // Set by the runner before execute()
            cost_accumulator: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

/// Result of resolving an api_key field.
pub struct ResolvedApiKey {
    pub key: String,
    pub is_byok: bool,
}

impl ExecutionContext {
    /// Coerce config values to match declared field types.
    /// Called automatically by the runner before execute().
    pub fn coerce_config(&mut self, fields: &[FieldDef]) {
        if fields.is_empty() {
            return;
        }
        let config_obj = match self.config.as_object_mut() {
            Some(obj) => obj,
            None => return,
        };
        for field in fields {
            let Some(value) = config_obj.get(field.key).cloned() else { continue };
            let coerced = match field.field_type {
                FieldType::Number => coerce_to_number(&value),
                FieldType::Checkbox => coerce_to_bool(&value),
                FieldType::Text | FieldType::Textarea | FieldType::Code | FieldType::Password => coerce_to_string(&value),
                // Select/Multiselect/ApiKey/Blob/FormBuilder: leave as-is
                _ => None,
            };
            if let Some(new_val) = coerced {
                config_obj.insert(field.key.to_string(), new_val);
            }
        }
    }

    /// Read a config value as u64 with coercion and fallback default.
    pub fn config_u64(&self, key: &str, default: u64) -> u64 {
        self.config.get(key)
            .and_then(|v| v.as_u64()
                .or_else(|| v.as_f64().map(|f| f as u64))
                .or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(default)
    }

    /// Read a config value as f64 with coercion and fallback default.
    pub fn config_f64(&self, key: &str, default: f64) -> f64 {
        self.config.get(key)
            .and_then(|v| v.as_f64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(default)
    }

    /// Read a config value as bool with coercion and fallback default.
    pub fn config_bool(&self, key: &str, default: bool) -> bool {
        self.config.get(key)
            .and_then(|v| v.as_bool()
                .or_else(|| v.as_str().map(|s| s == "true" || s == "1")))
            .unwrap_or(default)
    }

    /// Read a config value as string with coercion and fallback default.
    pub fn config_str(&self, key: &str, default: &str) -> String {
        self.config.get(key)
            .and_then(|v| {
                if let Some(s) = v.as_str() { Some(s.to_string()) }
                else if v.is_number() || v.is_boolean() { Some(v.to_string()) }
                else { None }
            })
            .unwrap_or_else(|| default.to_string())
    }

    /// Parse an input value as `Vec<String>`, accepting either a JSON array of strings
    /// or a single comma-separated string. Returns empty vec if the key is missing.
    pub fn input_string_list(&self, key: &str) -> Vec<String> {
        match self.input.get(key) {
            Some(v) if v.is_array() => v.as_array().unwrap()
                .iter().filter_map(|s| s.as_str().map(|s| s.to_string())).collect(),
            Some(v) if v.is_string() => v.as_str().unwrap()
                .split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
            _ => vec![],
        }
    }

    /// Get the lane count for this execution (how many parallel lanes exist at this depth).
    /// Injected by the executor for nodes with Gather inputs. Defaults to 1.
    pub fn lane_count(&self) -> u64 {
        self.input.get("__laneCount__")
            .and_then(|v| v.as_u64())
            .unwrap_or(1)
    }

    /// Check if this execution has been cancelled by polling the executor.
    ///
    /// Derives the executor status URL from the callbackUrl
    /// (format: `{base}/ProjectExecutor/{id}/execution_callback` → `{base}/ProjectExecutor/{id}/get_status`).
    ///
    /// Returns true if the executor reports "cancelled", false otherwise
    /// (including on network errors : we don't want transient failures to
    /// falsely signal cancellation).
    pub async fn is_cancelled(&self) -> bool {
        if self.callbackUrl.is_empty() {
            return false;
        }
        let status_url = self.callbackUrl.replace("/execution_callback", "/get_status");
        match reqwest::Client::new()
            .get(&status_url)
            .timeout(std::time::Duration::from_secs(3))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(body) = resp.text().await {
                    // The executor returns a JSON string like "cancelled", "running", etc.
                    body.contains("cancelled")
                } else {
                    false
                }
            }
            // 404 means the executor cleaned up the execution (after cancel or completion).
            // Treat as cancelled so this node task exits cleanly.
            Ok(resp) if resp.status() == reqwest::StatusCode::NOT_FOUND => true,
            // Network errors: don't false-positive, the executor might just be busy.
            _ => false,
        }
    }

    /// Request form input mid-execution.
    ///
    /// Sends a form to a consumer (identified by metadata), blocks until
    /// the form is submitted, returns the response.
    /// Can be called multiple times in a loop for multi-step interactions.
    pub async fn request_form_input(&self, request: FormInputRequest) -> Result<serde_json::Value, String> {
        let channels = self.form_input_channels.as_ref()
            .ok_or_else(|| "Form input channels not available (node not running in NodeRunner?)".to_string())?;

        crate::form_input::request_form_input_impl(
            channels,
            &self.http_client,
            &self.callbackUrl,
            &self.executionId,
            &self.nodeId,
            &self.pulseId,
            request,
        ).await
    }

    /// Build a notify action payload. The executor intercepts this from the output
    /// and registers a pending task for the browser extension.
    /// Returns a JSON value to merge into your output.
    pub fn notify_action(&self, url: &str) -> serde_json::Value {
        let action_id = format!("{}-{}-action", self.executionId, self.nodeId);
        serde_json::json!({
            "__notify_action__": {
                "actionId": action_id,
                "actionUrl": url,
            }
        })
    }

    fn internal_api_key() -> Option<String> {
        std::env::var("INTERNAL_API_KEY").ok().filter(|v| !v.is_empty())
    }

    /// Resolve an api_key field value into an actual key.
    ///
    /// - Empty or "__PLATFORM__": use the platform key from the env var for this provider.
    /// - Anything else: BYOK (user's own key).
    ///
    /// Returns None if the platform key is needed but the env var is not set.
    // TODO: add "openai" -> OPENAI_API_KEY, "anthropic" -> ANTHROPIC_API_KEY
    pub fn resolve_api_key(&self, config_value: Option<&str>, provider: &str) -> Option<ResolvedApiKey> {
        let is_byok = matches!(config_value, Some(v) if !v.is_empty() && v != "__PLATFORM__" && v != "__BYOK__");

        if is_byok {
            Some(ResolvedApiKey {
                key: config_value.unwrap().to_string(),
                is_byok: true,
            })
        } else {
            let env_var = match provider {
                "openrouter" => "OPENROUTER_API_KEY",
                "elevenlabs" => "ELEVENLABS_API_KEY",
                "tavily" => "TAVILY_API_KEY",
                "apollo" => "APOLLO_API_KEY",
                "minimax" => "MINIMAX_API_KEY",
                _ => {
                    tracing::error!("Unknown api_key provider: {}", provider);
                    return None;
                }
            };
            match std::env::var(env_var) {
                Ok(key) if !key.is_empty() => Some(ResolvedApiKey { key, is_byok: false }),
                _ => {
                    tracing::error!("Platform API key not configured (env var {} missing)", env_var);
                    None
                }
            }
        }
    }

    /// Download media from a source URL and store it as a temporary file via the API.
    ///
    /// Returns a media-compatible JSON object: `{ file_id, url, mimeType, filename }`.
    /// The `file_id` ensures `resolve_blob_urls` refreshes the presigned URL on each
    /// node execution (cloud mode). In local mode, the URL is a direct local path.
    ///
    /// If `mime_type` contains "/unknown" or is empty, the actual Content-Type from
    /// the download response is used instead.
    ///
    /// Uses the unified file API (`POST /api/v1/files` → `PUT upload_url`).
    /// Local stores to disk, cloud uploads to R2 via presigned URL.
    pub async fn store_temp_media(
        &self,
        source_url: &str,
        mime_type: &str,
        filename: &str,
    ) -> Result<serde_json::Value, String> {
        // Download from source
        let resp = self.http_client
            .get(source_url)
            .timeout(std::time::Duration::from_secs(120))
            .send()
            .await
            .map_err(|e| format!("Failed to download media from {}: {}", source_url, e))?;

        if !resp.status().is_success() {
            return Err(format!("Media download returned HTTP {}", resp.status()));
        }

        // Auto-detect mime type from response if caller didn't provide a specific one
        let effective_mime = if mime_type.is_empty() || mime_type.contains("/unknown") {
            resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string()
        } else {
            mime_type.to_string()
        };

        let bytes = resp.bytes().await
            .map_err(|e| format!("Failed to read media bytes: {}", e))?;

        if bytes.is_empty() {
            return Err("Downloaded media is empty".to_string());
        }

        let api_url = std::env::var("API_URL")
            .unwrap_or_else(|_| "http://localhost:3000".to_string());
        let user_id = self.userId.as_deref().unwrap_or("local");

        // Step 1: Create file record (POST /api/v1/files)
        let create_body = serde_json::json!({
            "filename": filename,
            "mimeType": effective_mime,
            "sizeBytes": bytes.len(),
            "ephemeral": true,
            "executionId": self.executionId,
        });

        let mut request = self.http_client
            .post(format!("{}/api/v1/files", api_url))
            .header("x-user-id", user_id)
            .json(&create_body);

        if let Some(internal_key) = Self::internal_api_key() {
            request = request.header("x-internal-api-key", internal_key);
        }

        let resp = request.send().await
            .map_err(|e| format!("Failed to create file record: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("File create failed (HTTP {}): {}", status, body));
        }

        let result: serde_json::Value = resp.json().await
            .map_err(|e| format!("Failed to parse file create response: {}", e))?;

        // Step 2: Upload bytes to upload_url (PUT)
        let upload_url = result.get("upload_url")
            .and_then(|v| v.as_str())
            .ok_or("Missing upload_url in file create response")?;

        let put_resp = self.http_client
            .put(upload_url)
            .header("content-type", &effective_mime)
            .body(bytes.to_vec())
            .send()
            .await
            .map_err(|e| format!("Failed to upload file bytes: {}", e))?;

        if !put_resp.status().is_success() {
            let status = put_resp.status();
            let body = put_resp.text().await.unwrap_or_default();
            return Err(format!("File upload failed (HTTP {}): {}", status, body));
        }

        // Return the file record (has file_id, url, filename, mimeType)
        Ok(result)
    }

    /// Report a service usage cost to the usage tracking API.
    ///
    /// This is the standard way for nodes to report costs. All HTTP plumbing
    /// (internal API key, endpoint URL, JSON construction) is handled here.
    /// Node authors only need to provide the service-specific details.
    ///
    /// - `model`: identifier for the service (e.g. "tavily-search", "scribe_v2", "apollo")
    /// - `subtype`: analytics category (e.g. "web_search", "speech_to_text", "people_enrich")
    /// - `cost_usd`: raw cost in USD (margin is applied downstream by the billing system)
    /// - `is_byok`: whether the user provided their own API key
    /// - `metadata`: optional extra fields for analytics (e.g. creditsUsed, durationSecs)
    pub async fn report_usage_cost(
        &self,
        model: &str,
        subtype: &str,
        cost_usd: f64,
        is_byok: bool,
        metadata: Option<serde_json::Value>,
    ) {
        let api_url = std::env::var("API_URL")
            .unwrap_or_else(|_| "http://localhost:3000".to_string());
        let user_id = self.userId.as_deref().unwrap_or("local");
        let client = reqwest::Client::new();
        let mut request = client.post(format!("{}/api/v1/usage/events", api_url));
        if let Some(internal_key) = Self::internal_api_key() {
            request = request.header("x-internal-api-key", internal_key);
        }
        let body = serde_json::json!({
            "userId": user_id,
            "eventType": "service",
            "subtype": subtype,
            "projectId": self.projectId,
            "executionId": self.executionId,
            "nodeId": self.nodeId,
            "model": model,
            "costUsd": cost_usd,
            "isByok": is_byok,
            "metadata": metadata.unwrap_or(serde_json::json!({})),
        });

        if let Err(e) = request.json(&body).send().await {
            tracing::warn!("Failed to report usage cost for {}/{}: {}", model, subtype, e);
        }

        // Accumulate cost for NodeExecution tracking (invisible to node authors)
        let microdollars = (cost_usd * 1_000_000.0) as u64;
        self.cost_accumulator.fetch_add(microdollars, std::sync::atomic::Ordering::Relaxed);
    }

    /// One-call helper to get a tracked CompletionContext for an AI node.
    ///
    /// Handles API key resolution (BYOK vs platform), env-based API URL,
    /// and cost tracking setup. Node developers should use this instead of
    /// manually calling `resolve_api_key` + `completion_context`.
    ///
    /// `provider` is the API key provider (e.g. "openrouter").
    /// `model` is the model identifier (e.g. "anthropic/claude-3.5-sonnet").
    /// `config` is the config source to read `apiKey` from. Pass `None` to
    /// use the node's own config (`ctx.config`). Nodes that accept config
    /// from an upstream node (e.g. LLM reading from LlmConfig) should pass
    /// that upstream config here.
    ///
    /// Returns `Err(message)` if no API key is available.
    pub async fn tracked_ai_context(
        &self,
        provider: &str,
        model: &str,
        config: Option<&serde_json::Value>,
    ) -> Result<minillmlib::CompletionContext, String> {
        let config_source = config.unwrap_or(&self.config);
        let api_key_value = config_source.get("apiKey").and_then(|v| v.as_str());
        let resolved = self.resolve_api_key(api_key_value, provider)
            .ok_or_else(|| "No API key available. Configure your own key or ensure platform credits are set up.".to_string())?;

        let generator = match provider {
            "openrouter" => minillmlib::GeneratorInfo::openrouter(model).with_api_key(&resolved.key),
            "minimax" => {
                let base_url = std::env::var("MINIMAX_BASE_URL")
                    .unwrap_or_else(|_| "https://api.minimax.io/v1".to_string());
                minillmlib::GeneratorInfo::new("MiniMax", base_url, model).with_api_key(&resolved.key)
            },
            _ => return Err(format!("Unknown provider: {}", provider)),
        };

        let api_url = std::env::var("API_URL")
            .unwrap_or_else(|_| "http://localhost:3000".to_string());

        Ok(self.completion_context(generator, &api_url, resolved.is_byok))
    }

    /// Get a ready-to-use InfraClient for an infrastructure sidecar.
    ///
    /// `endpoint_url` is the sidecar's action endpoint URL, received through
    /// edges from the infrastructure node's `endpointUrl` output port.
    pub fn infra_client(&self, endpoint_url: &str) -> weft_core::InfraClient {
        weft_core::InfraClient::new(endpoint_url)
    }

    /// Build a CompletionContext for tracked AI calls.
    ///
    /// Prefer `tracked_ai_context()` for most use cases. This lower-level method
    /// is available when you need custom generator or API URL configuration.
    pub fn completion_context(
        &self,
        generator: minillmlib::GeneratorInfo,
        api_url: &str,
        is_byok: bool,
    ) -> minillmlib::CompletionContext {
        let meta = serde_json::json!({
            "userId": self.userId.clone().unwrap_or_else(|| "local".to_string()),
            "projectId": self.projectId,
            "executionId": self.executionId,
            "nodeId": self.nodeId,
            "isByok": is_byok,
        });

        let api_url = api_url.to_string();
        let cost_acc = self.cost_accumulator.clone();
        let callback: minillmlib::AsyncCostCallback = std::sync::Arc::new(
            move |cost_info: minillmlib::CostInfo, meta: serde_json::Value| {
                let api_url = api_url.clone();
                let cost_acc = cost_acc.clone();
                Box::pin(async move {
                    // Accumulate cost for NodeExecution tracking
                    let microdollars = (cost_info.cost * 1_000_000.0) as u64;
                    cost_acc.fetch_add(microdollars, std::sync::atomic::Ordering::Relaxed);

                    let client = reqwest::Client::new();
                    let mut request = client.post(format!("{}/api/v1/usage/events", api_url));
                    if let Some(internal_key) = Self::internal_api_key() {
                        request = request.header("x-internal-api-key", internal_key);
                    }
                    let body = serde_json::json!({
                        "userId": meta["userId"],
                        "eventType": "service",
                        "subtype": "llm",
                        "projectId": meta["projectId"],
                        "executionId": meta["executionId"],
                        "nodeId": meta["nodeId"],
                        "model": cost_info.model,
                        "promptTokens": cost_info.prompt_tokens,
                        "completionTokens": cost_info.completion_tokens,
                        "costUsd": cost_info.cost,
                        "isByok": meta["isByok"],
                        "metadata": {
                            "responseId": cost_info.response_id,
                        },
                    });

                    if let Err(e) = request.json(&body).send().await {
                        tracing::warn!("Failed to report AI cost to API: {}", e);
                    }
                })
            },
        );

        minillmlib::CompletionContext::new(
            generator,
            meta,
            callback,
            "https://app.weavemind.ai",
            "WeaveMind",
        )
    }
}

/// The core Node trait that all node types implement.
///
/// Nodes are stateless - all state is passed via ExecutionContext.
/// Each node type should be a unit struct that implements this trait.
///
/// Trigger nodes should set `isTrigger: true` in their features and
/// implement `keep_alive`.
///
/// For form-based interactions, nodes call `ctx.request_form_input()`
/// during execute() instead of returning WaitingForInput directly.
#[async_trait]
pub trait Node: Send + Sync + 'static {
    /// Unique type identifier (e.g., "LlmInference", "Http", "Text", "DiscordReceive")
    /// Must match the NodeType enum variant name in PascalCase
    fn node_type(&self) -> &'static str;

    /// Metadata for UI and discovery
    fn metadata(&self) -> NodeMetadata;

    /// Lane mode overrides for specific ports.
    /// Ports not listed default to Single.
    /// Example: ForEach returns vec![("value", LaneMode::Expand)] for its output.
    fn lane_modes(&self) -> LaneModeMap { vec![] }

    /// Form field specs for hasFormSchema nodes.
    /// Each spec maps a field type to the input/output ports it contributes.
    /// Used by enrichment to derive ports from config.fields.
    fn form_field_specs(&self) -> Vec<FormFieldSpec> { vec![] }

    /// Dynamically resolve port types based on current port definitions.
    /// Called at compile time when the node has TypeVar or dynamic ports.
    /// Returns overrides for input and output port types.
    /// Only implement this for nodes with dynamic type behavior (Pack, Unpack, etc.).
    fn resolve_types(
        &self,
        _inputs: &[PortDefinition],
        _outputs: &[PortDefinition],
    ) -> ResolvedTypes {
        ResolvedTypes::default()
    }

    /// Execute the node logic (for regular nodes)
    /// For trigger nodes, this is called when the project starts and the trigger
    /// node needs to pass through its payload to downstream nodes.
    async fn execute(&self, ctx: ExecutionContext) -> NodeResult;

    /// Keep the trigger alive (only for nodes with isTrigger: true).
    /// Called after execute() during trigger setup has completed.
    /// Starts the long-lived runtime (event loop, polling, WebSocket, etc.)
    /// and returns a TriggerHandle for lifecycle management.
    /// Default implementation returns NotATrigger error.
    async fn keep_alive(
        &self,
        _config: TriggerStartConfig,
        _ctx: TriggerContext,
    ) -> Result<TriggerHandle, TriggerError> {
        Err(TriggerError::NotATrigger)
    }
}

// ---- Config coercion helpers ----

/// Try to coerce a JSON value to a number. Returns None if already a number.
fn coerce_to_number(value: &serde_json::Value) -> Option<serde_json::Value> {
    if value.is_number() { return None; } // already correct type
    if let Some(s) = value.as_str() {
        if s.is_empty() { return None; }
        if let Ok(n) = s.parse::<i64>() {
            return Some(serde_json::json!(n));
        }
        if let Ok(n) = s.parse::<f64>() {
            return Some(serde_json::json!(n));
        }
    }
    if let Some(b) = value.as_bool() {
        return Some(serde_json::json!(if b { 1 } else { 0 }));
    }
    None
}

/// Try to coerce a JSON value to a bool. Returns None if already a bool.
fn coerce_to_bool(value: &serde_json::Value) -> Option<serde_json::Value> {
    if value.is_boolean() { return None; }
    if let Some(s) = value.as_str() {
        return Some(serde_json::Value::Bool(s == "true" || s == "1"));
    }
    if let Some(n) = value.as_f64() {
        return Some(serde_json::Value::Bool(n != 0.0));
    }
    None
}

/// Try to coerce a JSON value to a string. Returns None if already a string.
fn coerce_to_string(value: &serde_json::Value) -> Option<serde_json::Value> {
    if value.is_string() { return None; }
    if value.is_number() || value.is_boolean() {
        return Some(serde_json::Value::String(value.to_string()));
    }
    None
}


/// Wrapper for inventory registration
/// This allows us to collect all Node implementations at runtime
pub struct NodeEntry {
    pub node: &'static dyn Node,
}

impl NodeEntry {
    pub const fn new(node: &'static dyn Node) -> Self {
        Self { node }
    }
}

inventory::collect!(NodeEntry);

/// Macro to simplify node registration
///
/// Usage:
/// ```ignore
/// pub struct LlmNode;
///
/// #[async_trait]
/// impl Node for LlmNode {
///     // ... implementation
/// }
///
/// register_node!(LlmNode);
/// ```
#[macro_export]
macro_rules! register_node {
    ($nodeType:ident) => {
        static __NODE_INSTANCE: $nodeType = $nodeType;

        inventory::submit! {
            $crate::node::NodeEntry::new(&__NODE_INSTANCE)
        }
    };
}

#[cfg(test)]
#[path = "tests/minimax_tests.rs"]
mod minimax_tests;
