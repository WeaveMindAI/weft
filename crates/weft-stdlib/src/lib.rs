//! Weft standard library of nodes.
//!
//! Node source lives in `catalog/` at the repository root, organized
//! in arbitrarily nested folders (e.g. `catalog/triggers/api/post/`).
//! This crate acts as the aggregator: it `#[path]`-includes each node
//! module and re-exports them for the compiler to link into user
//! binaries.
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

// LLM split into two nodes, matching v1:
//   LlmConfig (settings-only, outputs a config dict)
//   LlmInference (consumes a prompt, optionally consumes an
//                 upstream LlmConfig, calls the provider)
#[path = "../../../catalog/ai/llm/config/mod.rs"]
pub mod llm_config;
#[path = "../../../catalog/ai/llm/inference/mod.rs"]
pub mod llm_inference;

#[path = "../../../catalog/logic/gate/mod.rs"]
pub mod gate;

#[path = "../../../catalog/triggers/cron/mod.rs"]
pub mod cron;

#[path = "../../../catalog/http/request/mod.rs"]
pub mod http_request;

use weft_core::node::{FormFieldPort, FormFieldSpec, Node, NodeCatalog};

/// All nodes shipped in the standard library, as static trait objects.
pub fn all_nodes() -> Vec<&'static dyn Node> {
    vec![
        &api_post::ApiPostNode as &dyn Node,
        &human_query::HumanQueryNode,
        &text::TextNode,
        &debug::DebugNode,
        &llm_config::LlmConfigNode,
        &llm_inference::LlmInferenceNode,
        &gate::GateNode,
        &cron::CronNode,
        &http_request::HttpRequestNode,
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

// Form field specs for HumanQuery, ported verbatim from v1's
// human_form_field_specs() in catalog-v1/feedback/:human/query/backend.rs.
// The form builder reads `fieldType` on each entry and consults this
// table to know which input/output ports that field contributes.
//
// Naming note: v1 names are preserved ("text_input" not "text",
// "editable_textarea" not "editableTextarea") so existing .weft
// source keeps working.
use std::sync::OnceLock;

static HUMAN_QUERY_FIELD_SPECS: OnceLock<Vec<FormFieldSpec>> = OnceLock::new();

#[allow(non_upper_case_globals)]
fn human_query_specs() -> &'static Vec<FormFieldSpec> {
    HUMAN_QUERY_FIELD_SPECS.get_or_init(|| {
        vec![
            FormFieldSpec {
                field_type: "display",
                render: serde_json::json!({ "component": "readonly" }),
                adds_inputs: vec![FormFieldPort::any("{key}")],
                adds_outputs: vec![],
            },
            FormFieldSpec {
                field_type: "display_image",
                render: serde_json::json!({ "component": "image" }),
                adds_inputs: vec![FormFieldPort::new("{key}", "Image")],
                adds_outputs: vec![],
            },
            FormFieldSpec {
                field_type: "approve_reject",
                render: serde_json::json!({ "component": "buttons", "source": "static" }),
                adds_inputs: vec![],
                adds_outputs: vec![
                    FormFieldPort::new("{key}_approved", "Boolean"),
                    FormFieldPort::new("{key}_rejected", "Boolean"),
                ],
            },
            FormFieldSpec {
                field_type: "select",
                render: serde_json::json!({ "component": "select", "source": "static" }),
                adds_inputs: vec![],
                adds_outputs: vec![FormFieldPort::new("{key}", "String")],
            },
            FormFieldSpec {
                field_type: "multi_select",
                render: serde_json::json!({ "component": "select", "source": "static", "multiple": true }),
                adds_inputs: vec![],
                adds_outputs: vec![FormFieldPort::new("{key}", "List[String]")],
            },
            FormFieldSpec {
                field_type: "select_input",
                render: serde_json::json!({ "component": "select", "source": "input" }),
                adds_inputs: vec![FormFieldPort::new("{key}", "List[String]")],
                adds_outputs: vec![FormFieldPort::new("{key}", "String")],
            },
            FormFieldSpec {
                field_type: "multi_select_input",
                render: serde_json::json!({ "component": "select", "source": "input", "multiple": true }),
                adds_inputs: vec![FormFieldPort::new("{key}", "List[String]")],
                adds_outputs: vec![FormFieldPort::new("{key}", "List[String]")],
            },
            FormFieldSpec {
                field_type: "text_input",
                render: serde_json::json!({ "component": "text" }),
                adds_inputs: vec![],
                adds_outputs: vec![FormFieldPort::new("{key}", "String")],
            },
            FormFieldSpec {
                field_type: "textarea",
                render: serde_json::json!({ "component": "textarea" }),
                adds_inputs: vec![],
                adds_outputs: vec![FormFieldPort::new("{key}", "String")],
            },
            FormFieldSpec {
                field_type: "editable_text_input",
                render: serde_json::json!({ "component": "text", "prefilled": true }),
                adds_inputs: vec![FormFieldPort::new("{key}", "String")],
                adds_outputs: vec![FormFieldPort::new("{key}", "String")],
            },
            FormFieldSpec {
                field_type: "editable_textarea",
                render: serde_json::json!({ "component": "textarea", "prefilled": true }),
                adds_inputs: vec![FormFieldPort::new("{key}", "String")],
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
