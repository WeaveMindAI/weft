//! Shared wire shape for node-catalog JSON the CLI emits to the
//! editor. Both `weft parse` (referenced subset) and `weft
//! describe-nodes` (full catalog) key entries by node type and bundle
//! `NodeMetadata` with the node's form-field specs, so the editor
//! drives its UI from one shape. Defined once here so the two commands
//! can't drift.

use serde::Serialize;

use weft_core::node::{FormFieldSpec, NodeMetadata};

#[derive(Debug, Serialize)]
pub struct NodeCatalogEntry {
    #[serde(flatten)]
    pub metadata: NodeMetadata,
    #[serde(rename = "formFieldSpecs", default, skip_serializing_if = "Vec::is_empty")]
    pub form_field_specs: Vec<FormFieldSpec>,
}
