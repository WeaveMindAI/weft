//! Weft Node implementations and runtime.
//!
//! This crate uses camelCase for struct fields to match JSON API conventions
//! without any serde rename translation.
#![allow(non_snake_case)]

// Core service infrastructure
pub mod service;
pub mod trigger_service;
pub mod form_input;
pub mod form_registrar;

// Unified node system
pub mod node;
pub mod registry;
pub mod runner;
pub mod constants;
pub mod infra_helpers;

// Built-in nodes (compiler-internal, not in catalog)
pub mod passthrough;

// Post-compilation enrichment
pub mod enrich;

// Node implementations
pub mod nodes;

// Re-export core types
pub use service::{NodeService, NodeServiceConfig, HealthResponse, NodeResult};
pub use form_input::{FormInputRequest, FormInputChannels};
pub use node::{
    Node, NodeMetadata, NodeFeatures, PortDef, WeftType, WeftPrimitive, ResolvedTypes, ExecutionContext, NodeEntry,
    // Trigger-related types (unified with Node)
    TriggerCategory, TriggerStatus, TriggerEvent, TriggerEventSender,
    TriggerStartConfig, TriggerHandle, TriggerError, Emitter, ShutdownSignal,
};
pub use trigger_service::{TriggerService, TriggerInfo};
pub use form_registrar::{FormRegistrar, FormSubmission};
pub use registry::NodeTypeRegistry;
pub use runner::NodeRunner;
pub use constants::{
    NODE_RUNNER_BINARY, NODE_RUNNER_PORT_ENV, NODE_RUNNER_DEFAULT_PORT,
    get_node_binary_info,
};
pub use infra_helpers::{infra_provision, infra_query_outputs};
