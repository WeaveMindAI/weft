//! Node type registry that collects all registered nodes at startup.
//!
//! Uses the `inventory` crate to automatically discover all nodes
//! that have been registered with `register_node!`.

use std::collections::HashMap;
use crate::node::{Node, NodeEntry};

/// Registry of all available node type implementations.
/// 
/// This is a local in-memory registry that maps node type names to their implementations.
/// Nodes are collected at startup from all `inventory::submit!` calls.
/// This provides O(1) lookup by node type.
/// 
/// Note: This is different from `weft_core::NodeInstanceRegistry` which is a
/// Restate virtual object that tracks running node service instances across the cluster.
pub struct NodeTypeRegistry {
    nodes: HashMap<&'static str, &'static dyn Node>,
}

impl NodeTypeRegistry {
    /// Create a new registry by collecting all registered nodes.
    pub fn new() -> Self {
        let mut nodes = HashMap::new();
        
        for entry in inventory::iter::<NodeEntry> {
            let node = entry.node;
            let node_type = node.node_type();
            
            if nodes.contains_key(node_type) {
                tracing::warn!("Duplicate node type registration: {}", node_type);
            }
            
            nodes.insert(node_type, node);
            tracing::debug!("Registered node type: {}", node_type);
        }
        
        tracing::info!("NodeTypeRegistry initialized with {} node types", nodes.len());
        
        Self { nodes }
    }
    
    /// Get a node by its type identifier.
    pub fn get(&self, node_type: &str) -> Option<&'static dyn Node> {
        self.nodes.get(node_type).copied()
    }
    
    /// Get all registered node types.
    pub fn all_types(&self) -> Vec<&'static str> {
        self.nodes.keys().copied().collect()
    }
    
    /// Get the number of registered node types.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
    
    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

impl Default for NodeTypeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_registry_creation() {
        let registry = NodeTypeRegistry::new();
        // Registry should be created without panicking
        // Actual node count depends on what's registered
        let _ = registry.len(); // Just verify it doesn't panic
    }
}
