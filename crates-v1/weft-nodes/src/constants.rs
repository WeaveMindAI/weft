//! Node runner configuration constants
//!
//! The unified node system uses a single `node-runner` binary that handles all node types.

/// The binary name for the unified node runner
pub const NODE_RUNNER_BINARY: &str = "node-runner";

/// The port environment variable for the node runner
pub const NODE_RUNNER_PORT_ENV: &str = "NODE_PORT";

/// The default port for the node runner
pub const NODE_RUNNER_DEFAULT_PORT: u16 = 9080;

/// Returns the binary name and port env var for a node type.
/// All node types use the same unified runner.
pub fn get_node_binary_info(_node_type: &str) -> (&'static str, &'static str) {
    (NODE_RUNNER_BINARY, NODE_RUNNER_PORT_ENV)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_info() {
        let (bin, port_env) = get_node_binary_info("LlmInference");
        assert_eq!(bin, "node-runner");
        assert_eq!(port_env, "NODE_PORT");
        
        // All node types return the same runner
        let (bin2, _) = get_node_binary_info("Http");
        assert_eq!(bin, bin2);
    }
}
