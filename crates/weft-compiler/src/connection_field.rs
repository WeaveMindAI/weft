//! Which field of a node takes a connection, and to which recipe, read
//! once per node at enrich (`weft_core::access::callers::connection_field`)
//! so the stamp on its widget, the pick on the install and an instance's
//! connect page all follow the same answer: an access node's own recipe,
//! or for a node checking its callers the recipe of the scheme its `auth`
//! names.

use weft_core::access::callers::{self, ConnectionField};
use weft_core::node::NodeMetadata;
use weft_core::project::{NodeDefinition, Span};

use crate::enrich::EnrichError;

/// The connection field of `node` (of type `meta`), or `None`. An `auth`
/// that names no scheme weft knows is refused at its line, and so is a
/// `connection` written on a node whose `auth` checks nobody: nothing
/// would ever read it.
pub fn resolve(meta: &NodeMetadata, node: &NodeDefinition, node_span: Span, errors: &mut Vec<EnrichError>) -> Option<ConnectionField> {
    let auth = meta.features.checks_callers.then(|| node.written_value(callers::AUTH_FIELD)).flatten();
    let refuse = |field: &str, message: String, errors: &mut Vec<EnrichError>| {
        errors.push(EnrichError {
            code: "enrich",
            span: node.written_span(field).map(|s| s.span).unwrap_or(node_span),
            file: node.source_file.clone(),
            subjects: vec![node.id.clone()],
            message,
        });
    };
    match callers::connection_field(meta, auth) {
        Ok(None) if meta.features.checks_callers && node.written_value(callers::CONNECTION_FIELD).is_some() => {
            refuse(
                callers::CONNECTION_FIELD,
                format!(
                    "'{id}' checks no caller (its `auth` is \"none\"), so its `{connection}` is never read; \
                     set `auth` to the scheme its callers use ({schemes}), or drop `{connection}`",
                    id = node.id,
                    connection = callers::CONNECTION_FIELD,
                    schemes = callers::auth_options()[1..].join(", "),
                ),
                errors,
            );
            None
        }
        Ok(field) => field,
        Err(why) => {
            refuse(callers::AUTH_FIELD, format!("'{}': {why}", node.id), errors);
            None
        }
    }
}
