//! Unified NodeRunner service that handles all node types.
//!
//! This replaces the category-based services (ComputeNodeService, DataNodeService, etc.)
//! with a single service that uses the NodeTypeRegistry to dispatch to the appropriate node.

use std::sync::Arc;
use async_trait::async_trait;
use axum::{routing::post, Router, Json, http::StatusCode, extract::DefaultBodyLimit};
use tower_http::cors::CorsLayer;
use weft_core::NodeExecuteRequest;
use crate::node::ExecutionContext;
use crate::registry::NodeTypeRegistry;
use crate::{NodeService, NodeServiceConfig, NodeResult};
use crate::form_input::FormInputChannels;

/// Unified node runner that handles all registered node types.
///
/// Instead of having separate binaries for compute, data, io, feedback nodes,
/// this single runner can execute any registered node type.
#[derive(Clone)]
pub struct NodeRunner {
    config: NodeServiceConfig,
    client: reqwest::Client,
    registry: &'static NodeTypeRegistry,
    pub channels: Arc<FormInputChannels>,
}

impl NodeRunner {
    /// Create a new NodeRunner with all registered nodes.
    pub fn new() -> Self {
        // Create a static registry that lives for the project duration
        let registry: &'static NodeTypeRegistry = Box::leak(Box::new(NodeTypeRegistry::new()));

        // Collect all node types from the registry
        let node_types: Vec<String> = registry
            .all_types()
            .iter()
            .map(|s| s.to_string())
            .collect();

        tracing::info!("NodeRunner initialized with node types: {:?}", node_types);

        Self {
            config: NodeServiceConfig::from_env(
                "NODE_ID",
                "NODE_PORT",
                "node-runner",
                "9080",
                node_types,
            ),
            client: reqwest::Client::new(),
            registry,
            channels: Arc::new(FormInputChannels::new()),
        }
    }

    /// Get the node type registry.
    pub fn registry(&self) -> &NodeTypeRegistry {
        self.registry
    }

    /// Handler for `/input_response/{callback_id}`.
    /// Called by the executor when a form is submitted.
    /// Resolves the waiting oneshot channel so the node can continue.
    async fn input_response_handler(
        axum::extract::State(runner): axum::extract::State<Self>,
        axum::extract::Path(callback_id): axum::extract::Path<String>,
        Json(input): Json<serde_json::Value>,
    ) -> StatusCode {
        tracing::info!("[runner] input_response for callback_id={}", callback_id);
        if runner.channels.resolve(&callback_id, input).await {
            StatusCode::OK
        } else {
            tracing::warn!("[runner] No waiting channel for callback_id={}", callback_id);
            StatusCode::NOT_FOUND
        }
    }
}

impl Default for NodeRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodeService for NodeRunner {
    fn config(&self) -> &NodeServiceConfig {
        &self.config
    }

    fn client(&self) -> &reqwest::Client {
        &self.client
    }

    async fn execute(&self, req: NodeExecuteRequest) -> NodeResult {
        let node_type = &req.nodeType;

        match self.registry.get(node_type) {
            Some(node) => {
                let mut ctx = ExecutionContext::from(req);
                let metadata = node.metadata();
                ctx.coerce_config(&metadata.fields);
                ctx.http_client = self.client.clone();
                ctx.form_input_channels = Some(self.channels.clone());
                let cost_acc = ctx.cost_accumulator.clone();

                let mut result = node.execute(ctx).await;

                // Read accumulated cost from the context (set by report_usage_cost / LLM callback)
                let microdollars = cost_acc.load(std::sync::atomic::Ordering::Relaxed);
                result.costUsd = microdollars as f64 / 1_000_000.0;

                result
            }
            None => {
                tracing::error!("Unknown node type: {}", node_type);
                NodeResult::failed(&format!("Unknown node type: {}", node_type))
            }
        }
    }

    fn build_router(self) -> Router {
        Router::new()
            .route("/health", axum::routing::get(Self::health_handler))
            .route("/execute", post(Self::execute_handler))
            .route("/input_response/{callback_id}", post(Self::input_response_handler))
            .with_state(self)
            .layer(CorsLayer::permissive())
            // Increase body size limit to 15MB (default is 2MB) to handle audio files
            .layer(DefaultBodyLimit::max(15 * 1024 * 1024))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runner_creation() {
        // This test verifies the runner can be created
        // Actual functionality depends on registered nodes
        let runner = NodeRunner::new();
        assert!(!runner.config().nodeTypes.is_empty() || runner.registry().is_empty());
    }
}
