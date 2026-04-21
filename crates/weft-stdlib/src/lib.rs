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

use weft_core::node::{FormFieldPort, FormFieldSpec, Node, NodeCatalog};

/// All nodes shipped in the standard library, as static trait objects.
pub fn all_nodes() -> Vec<&'static dyn Node> {
    vec![
        &api_post::ApiPostNode as &dyn Node,
        &human_query::HumanQueryNode,
        &text::TextNode,
        &debug::DebugNode,
        &llm::LlmNode,
    ]
}

/// Standard library catalog. Implements NodeCatalog so the compiler
/// can resolve node-type strings to metadata and form-field specs.
pub struct StdlibCatalog;

impl NodeCatalog for StdlibCatalog {
    fn lookup(&self, node_type: &str) -> Option<&dyn Node> {
        all_nodes().into_iter().find(|n| n.node_type() == node_type)
    }

    fn all(&self) -> Vec<&'static str> {
        all_nodes().iter().map(|n| n.node_type()).collect()
    }

    fn form_field_specs(&self, node_type: &str) -> &[FormFieldSpec] {
        match node_type {
            "HumanQuery" => human_query_specs(),
            _ => &[],
        }
    }
}

// Form field specs for HumanQuery. The form-builder field produces
// output ports: for a "text" field named "name", the HumanQuery node
// emits an output `submission_name: String`. Defined as a OnceLock
// so the slice has static lifetime.
use std::sync::OnceLock;

static HUMAN_QUERY_FIELD_SPECS: OnceLock<Vec<FormFieldSpec>> = OnceLock::new();

#[allow(non_upper_case_globals)]
fn human_query_specs() -> &'static Vec<FormFieldSpec> {
    HUMAN_QUERY_FIELD_SPECS.get_or_init(|| {
        vec![
            FormFieldSpec {
                field_type: "text",
                render: serde_json::json!({}),
                adds_inputs: vec![],
                adds_outputs: vec![FormFieldPort::new("{key}", "String")],
            },
            FormFieldSpec {
                field_type: "textarea",
                render: serde_json::json!({}),
                adds_inputs: vec![],
                adds_outputs: vec![FormFieldPort::new("{key}", "String")],
            },
            FormFieldSpec {
                field_type: "number",
                render: serde_json::json!({}),
                adds_inputs: vec![],
                adds_outputs: vec![FormFieldPort::new("{key}", "Number")],
            },
            FormFieldSpec {
                field_type: "checkbox",
                render: serde_json::json!({}),
                adds_inputs: vec![],
                adds_outputs: vec![FormFieldPort::new("{key}", "Boolean")],
            },
            FormFieldSpec {
                field_type: "select",
                render: serde_json::json!({}),
                adds_inputs: vec![],
                adds_outputs: vec![FormFieldPort::new("{key}", "String")],
            },
        ]
    })
}

impl StdlibCatalog {
    /// Explicit accessor for the HumanQuery field specs so the catalog
    /// can return them with static lifetime. Keeps the
    /// `form_field_specs` match above simple.
    pub fn init() {
        let _ = human_query_specs();
    }
}
