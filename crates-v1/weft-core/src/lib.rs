#![allow(non_snake_case)]

pub mod weft_type;
pub mod project;
pub mod node;
pub mod executor;
pub mod executor_core;
pub mod instance_registry;
pub mod sidecar;
pub mod infrastructure;
pub mod k8s_provisioner;
pub mod media_types;
pub mod weft_compiler;

pub use project::*;
pub use media_types::{Image, Video, Audio, Document, media_category_from_mime};
pub use node::*;
// Shared types from executor_core (used by the axum executor)
pub use executor_core::{
    ProjectExecutionRequest, ProjectExecutionResult,
    ProvideInputRequest,
    PendingTask, PendingTasksList, TaskType, FormField, FormSchema,
    NodeStatusMap, NodeOutputMap,
    PulseStatus, SplitFrame, Pulse, PulseTable,
    NodeExecutionStatus, NodeExecution, NodeExecutionTable,
    node_execution_summary,
    is_inside_mocked_group, sanitize_mock_output,
};
// Restate auxiliary services
pub use executor::{
    TaskRegistry, TaskRegistryImpl,
};
pub use instance_registry::{
    NodeInstanceRegistry, NodeInstanceRegistryImpl,
    NodeInstance, NodeInstanceStatus, NodeInstanceList,
    NodeExecuteRequest, NodeExecuteResponse, NodeCallbackRequest,
    WaitingMetadata,
};
pub use sidecar::{ActionRequest, ActionResponse};
pub use infrastructure::{
    InfrastructureManager, InfrastructureManagerImpl,
    InfraClient, InfraEndpointUrls,
    StartInfraRequest, InfraSetupCallback, InfraStatusResponse, InfraNodeStatus,
    infra_instance_id,
};
