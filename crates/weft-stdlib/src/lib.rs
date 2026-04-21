//! Weft standard library of nodes.
//!
//! Node source lives in `catalog/` at the repository root, organized
//! in arbitrarily nested folders (e.g. `catalog/triggers/api/post/`).
//! This crate acts as the aggregator: it `#[path]`-includes each node
//! module and re-exports them for the compiler to link into user
//! binaries.
//!
//! Phase A1 exposes 5 scaffold nodes: ApiPost, HumanQuery, Text, Debug,
//! Llm. Phase A2 ports the rest from `catalog-v1/` (~100 nodes).
//!
//! User projects depend on this crate implicitly via the compiled
//! binary; the compiler only includes nodes the graph actually
//! references.

// Scaffold nodes (inline modules wired via #[path] to catalog/).

#[path = "../../../catalog/triggers/api/post/mod.rs"]
pub mod api_post;

#[path = "../../../catalog/human/query/mod.rs"]
pub mod human_query;

#[path = "../../../catalog/basic/text/mod.rs"]
pub mod text;

#[path = "../../../catalog/basic/debug/mod.rs"]
pub mod debug;

#[path = "../../../catalog/ai/llm/mod.rs"]
pub mod llm;

/// All nodes shipped in the standard library, as static trait objects.
/// The compiler uses this list (plus per-project introspection) to
/// answer describe queries and to link specific nodes into user
/// binaries.
pub fn all_nodes() -> Vec<&'static dyn weft_core::Node> {
    vec![
        &api_post::ApiPostNode as &dyn weft_core::Node,
        &human_query::HumanQueryNode,
        &text::TextNode,
        &debug::DebugNode,
        &llm::LlmNode,
    ]
}
