//! Sanity check: every stdlib node's metadata() returns a parseable
//! NodeMetadata (i.e., its metadata.json is valid and the WeftType
//! strings resolve).

use weft_core::Node;

#[test]
fn every_node_metadata_parses() {
    for node in weft_stdlib::all_nodes() {
        let meta = node.metadata();
        assert_eq!(meta.node_type, node.node_type(), "metadata type must match node_type()");
        assert!(!meta.label.is_empty(), "node {} has empty label", meta.node_type);
    }
}
