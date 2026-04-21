use serde::{Deserialize, Serialize};
use restate_sdk::prelude::*;
use crate::executor_core::NodeExecutionStatus;

// =============================================================================
// NODE INSTANCE - Represents a running node service
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInstance {
    pub id: String,
    pub endpoint: String,
    pub nodeTypes: Vec<String>,
    pub status: NodeInstanceStatus,
    pub registeredAt: String,
    pub lastHeartbeat: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeInstanceStatus {
    Online,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInstanceList {
    pub instances: Vec<NodeInstance>,
}

// =============================================================================
// NODE INSTANCE REGISTRY - Tracks available node instances (Restate virtual object)
// =============================================================================

#[restate_sdk::object]
pub trait NodeInstanceRegistry {
    async fn register(instance: Json<NodeInstance>) -> Result<(), HandlerError>;
    async fn unregister(instance_id: String) -> Result<(), HandlerError>;
    async fn heartbeat(instance_id: String) -> Result<(), HandlerError>;
    
    #[shared]
    async fn list_instances() -> Result<Json<NodeInstanceList>, HandlerError>;
    
    #[shared]
    async fn find_instance_for_node_type(nodeType: String) -> Result<Option<Json<NodeInstance>>, HandlerError>;
}

pub struct NodeInstanceRegistryImpl;

impl NodeInstanceRegistry for NodeInstanceRegistryImpl {
    async fn register(
        &self,
        ctx: ObjectContext<'_>,
        Json(instance): Json<NodeInstance>,
    ) -> Result<(), HandlerError> {
        let instances_json: String = ctx.get("instances").await?.unwrap_or_else(|| "[]".to_string());
        let mut instances: Vec<NodeInstance> = serde_json::from_str(&instances_json)
            .map_err(|e| TerminalError::new(format!("Failed to deserialize instances: {}", e)))?;
        
        // Remove existing instance with same ID if exists
        instances.retain(|i| i.id != instance.id);
        instances.push(instance.clone());
        
        let new_json = serde_json::to_string(&instances)
            .map_err(|e| TerminalError::new(format!("Serialization error: {}", e)))?;
        ctx.set("instances", new_json);
        
        tracing::info!("Node instance registered: {} at {}", instance.id, instance.endpoint);
        Ok(())
    }

    async fn unregister(
        &self,
        ctx: ObjectContext<'_>,
        instance_id: String,
    ) -> Result<(), HandlerError> {
        let instances_json: String = ctx.get("instances").await?.unwrap_or_else(|| "[]".to_string());
        let mut instances: Vec<NodeInstance> = serde_json::from_str(&instances_json)
            .map_err(|e| TerminalError::new(format!("Failed to deserialize instances: {}", e)))?;
        instances.retain(|i| i.id != instance_id);
        let new_json = serde_json::to_string(&instances)
            .map_err(|e| TerminalError::new(format!("Serialization error: {}", e)))?;
        ctx.set("instances", new_json);
        
        tracing::info!("Node instance unregistered: {}", instance_id);
        Ok(())
    }

    async fn heartbeat(
        &self,
        ctx: ObjectContext<'_>,
        instance_id: String,
    ) -> Result<(), HandlerError> {
        let instances_json: String = ctx.get("instances").await?.unwrap_or_else(|| "[]".to_string());
        let mut instances: Vec<NodeInstance> = serde_json::from_str(&instances_json)
            .map_err(|e| TerminalError::new(format!("Failed to deserialize instances: {}", e)))?;
        
        if let Some(instance) = instances.iter_mut().find(|i| i.id == instance_id) {
            instance.lastHeartbeat = chrono::Utc::now().to_rfc3339();
            instance.status = NodeInstanceStatus::Online;
        }
        
        let new_json = serde_json::to_string(&instances)
            .map_err(|e| TerminalError::new(format!("Serialization error: {}", e)))?;
        ctx.set("instances", new_json);
        Ok(())
    }

    async fn list_instances(
        &self,
        ctx: SharedObjectContext<'_>,
    ) -> Result<Json<NodeInstanceList>, HandlerError> {
        let instances_json: String = ctx.get("instances").await?.unwrap_or_else(|| "[]".to_string());
        let instances: Vec<NodeInstance> = serde_json::from_str(&instances_json)
            .map_err(|e| TerminalError::new(format!("Failed to deserialize instances: {}", e)))?;
        Ok(Json(NodeInstanceList { instances }))
    }

    async fn find_instance_for_node_type(
        &self,
        ctx: SharedObjectContext<'_>,
        node_type: String,
    ) -> Result<Option<Json<NodeInstance>>, HandlerError> {
        let instances_json: String = ctx.get("instances").await?.unwrap_or_else(|| "[]".to_string());
        let instances: Vec<NodeInstance> = serde_json::from_str(&instances_json)
            .map_err(|e| TerminalError::new(format!("Failed to deserialize instances: {}", e)))?;
        
        // Filter to online instances that support this node type
        let matching: Vec<_> = instances.into_iter()
            .filter(|i| i.status == NodeInstanceStatus::Online && i.nodeTypes.contains(&node_type))
            .collect();
        
        // Prefer hybrid-cloud-api if available (for cloud mode hybrid routing)
        let instance = matching.iter()
            .find(|i| i.id.contains("hybrid"))
            .cloned()
            .or_else(|| matching.into_iter().next());
        
        Ok(instance.map(Json))
    }
}

// =============================================================================
// NODE EXECUTION REQUEST/RESPONSE - Used for HTTP communication
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct NodeExecuteRequest {
    pub executionId: String,
    pub nodeId: String,
    pub nodeType: String,
    pub config: serde_json::Value,
    pub input: serde_json::Value,
    pub callbackUrl: String,
    #[serde(default)]
    pub userId: Option<String>,
    #[serde(default)]
    pub projectId: Option<String>,
    /// Output port definitions for this node instance (used for output extraction)
    #[serde(default)]
    pub outputs: Vec<crate::project::PortDefinition>,
    /// Node features/capabilities (used for output extraction, etc.)
    #[serde(default)]
    pub features: crate::node::NodeFeatures,
    /// Whether this execution is part of infrastructure setup (true) or normal project execution (false)
    #[serde(default)]
    pub isInfraSetup: bool,
    /// Whether this execution is part of trigger setup (true) or normal project execution (false)
    #[serde(default)]
    pub isTriggerSetup: bool,
    #[serde(default)]
    pub pulseId: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExecuteResponse {
    pub accepted: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCallbackRequest {
    #[serde(default)]
    pub executionId: String,
    pub nodeId: String,
    #[serde(default = "default_callback_status")]
    pub status: NodeExecutionStatus,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    #[serde(default)]
    pub waitingMetadata: Option<WaitingMetadata>,
    #[serde(default)]
    pub pulseId: String,
    /// Accumulated cost in USD for this node execution.
    #[serde(default)]
    pub costUsd: f64,
}

fn default_callback_status() -> NodeExecutionStatus {
    NodeExecutionStatus::Completed
}

impl NodeCallbackRequest {
    pub fn failed(execution_id: &str, node_id: &str, pulse_id: &str, error: &str) -> Self {
        Self {
            executionId: execution_id.to_string(),
            nodeId: node_id.to_string(),
            status: NodeExecutionStatus::Failed,
            output: None,
            error: Some(error.to_string()),
            waitingMetadata: None,
            pulseId: pulse_id.to_string(),
            costUsd: 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitingMetadata {
    pub callbackId: String,
    pub title: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub formSchema: Option<crate::executor_core::FormSchema>,
    /// Free-form metadata for consumer filtering (e.g., { "source": "human" }).
    #[serde(default)]
    pub metadata: serde_json::Value,
}

