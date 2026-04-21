//! Unified Node Runner - Single binary that handles all node types
//!
//! This replaces the category-based binaries (node_compute, node_data, node_io, node_feedback)
//! with a single runner that uses the NodeTypeRegistry to dispatch to any registered node.

use weft_nodes::NodeRunner;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    weft_nodes::NodeService::run(NodeRunner::new()).await
}
