//! GmailLabel: add and/or remove labels on a message, by NAME (the
//! ids are Gmail-internal; names are what people know). Unknown
//! add-labels are created; an unknown remove-label is a loud error (a
//! label that never existed cannot be on the message: the name is
//! wrong).

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::access::client::{get_json, url};
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

use super::gmail::API;

#[derive(NodeManifest)]
pub struct GmailLabelNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GmailLabelNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let id: String = ctx.inputs.get("id")?;
        // Never split on commas: a Gmail label name may contain one.
        let add: Vec<String> = ctx.inputs.list("add")?;
        let remove: Vec<String> = ctx.inputs.list("remove")?;

        let http = ctx.client(&account).await?;
        let listing: Value = get_json(&http, url(API, &["labels"])?.as_str(), "gmail: list labels").await?;
        let find = |name: &str| -> Option<String> {
            listing["labels"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|l| {
                    l["name"].as_str().is_some_and(|n| n.eq_ignore_ascii_case(name))
                })
                .and_then(|l| l["id"].as_str().map(str::to_string))
        };

        let mut add_ids = Vec::new();
        for name in &add {
            let label_id = match find(name) {
                Some(id) => id,
                None => {
                    // A new add-label is created on the fly: labeling
                    // is how workflows organize, and "create it first
                    // by hand" would be pure friction.
                    let made = weft::access::client::json_call(
                        http.post(url(API, &["labels"])?.as_str()).json(&json!({ "name": name })),
                        "create the label",
                    )
                    .await?;
                    weft::access::client::required_str(&made, "create the label", "id")?
                        .to_string()
                }
            };
            add_ids.push(label_id);
        }
        let mut remove_ids = Vec::new();
        for name in &remove {
            match find(name) {
                Some(id) => remove_ids.push(id),
                None => weft::node_bail!(
                    "the account has no label named '{name}' to remove; check the name"
                ),
            }
        }

        let answer = weft::access::client::json_call(
            http.post(url(API, &["messages", &id, "modify"])?.as_str())
                .json(&json!({ "addLabelIds": add_ids, "removeLabelIds": remove_ids })),
            "modify the labels",
        )
        .await?;
        ctx.pulse_downstream(NodeOutput::new().set("labels", answer["labelIds"].clone())).await
    }
}
