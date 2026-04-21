//! Apollo People Enrichment Node
//!
//! Enriches a person record with full profile data from Apollo.io.
//! Uses POST /api/v1/people/match
//!
//! Takes a person ID (from ApolloSearch), email, or name+domain and returns
//! the full profile: email, linkedin_url, phone, employment_history, location,
//! organization details, etc.
//!
//! This endpoint CONSUMES Apollo credits.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

/// Apollo enrichment: 1 credit per call.
/// Raw cost: Basic monthly plan ($59/mo, 2,500 credits/mo) = $0.0236/credit.
/// Margin is applied downstream by get_user_margin().
const APOLLO_COST_PER_CREDIT: f64 = 0.0236;

#[derive(Default)]
pub struct ApolloEnrichNode;

const APOLLO_API_BASE: &str = "https://api.apollo.io/api/v1";

#[async_trait]
impl Node for ApolloEnrichNode {
    fn node_type(&self) -> &'static str {
        "ApolloEnrich"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Apollo Enrich",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", false),
                PortDef::new("id", "String", false),
                PortDef::new("email", "String", false),
                PortDef::new("firstName", "String", false),
                PortDef::new("lastName", "String", false),
                PortDef::new("domain", "String", false),
                PortDef::new("linkedinUrl", "String", false),
            ],
            outputs: vec![
                PortDef::new("rawPerson", "JsonDict", false),
                PortDef::new("name", "String", false),
                PortDef::new("email", "String", false),
                PortDef::new("title", "String", false),
                PortDef::new("linkedinUrl", "String", false),
                PortDef::new("organization", "String", false),
                PortDef::new("city", "String", false),
                PortDef::new("state", "String", false),
                PortDef::new("country", "String", false),
                PortDef::new("headline", "String", false),
            ],
            features: NodeFeatures {
                oneOfRequired: vec![vec![
                    "id".into(), "email".into(), "linkedinUrl".into(),
                    "firstName".into(), "lastName".into(), "domain".into(),
                ]],
                ..Default::default()
            },
            fields: vec![
                FieldDef::checkbox("revealPersonalEmails"),
                FieldDef::checkbox("revealPhoneNumber")
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let config_input = ctx.input.get("config").and_then(|v| v.as_object());
        let api_key_value = config_input
            .and_then(|c| c.get("apiKey"))
            .and_then(|v| v.as_str());
        let resolved = match ctx.resolve_api_key(api_key_value, "apollo") {
            Some(r) => r,
            None => return NodeResult::failed(
                "No Apollo API key available. Connect an ApolloConfig node or set the platform key."
            ),
        };

        let api_key = &resolved.key;

        // Build match parameters from inputs
        let mut params = serde_json::Map::new();

        let field_mappings = [
            ("id", "id"),
            ("email", "email"),
            ("firstName", "first_name"),
            ("lastName", "last_name"),
            ("domain", "domain"),
            ("linkedinUrl", "linkedin_url"),
        ];

        let mut has_any_param = false;
        for (port_name, api_param) in &field_mappings {
            if let Some(v) = ctx.input.get(*port_name).and_then(|v| v.as_str()) {
                if !v.is_empty() {
                    params.insert(api_param.to_string(), serde_json::json!(v));
                    has_any_param = true;
                }
            }
        }

        if !has_any_param {
            return NodeResult::failed(
                "At least one identifier is required: id, email, firstName+domain, or linkedinUrl"
            );
        }

        // Optional: reveal personal emails and phone numbers
        let reveal_emails = ctx.config.get("revealPersonalEmails")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let reveal_phone = ctx.config.get("revealPhoneNumber")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        params.insert("reveal_personal_emails".to_string(), serde_json::json!(reveal_emails));
        params.insert("reveal_phone_number".to_string(), serde_json::json!(reveal_phone));

        let url = format!("{}/people/match", APOLLO_API_BASE);
        let client = reqwest::Client::new();

        tracing::info!("Apollo People Enrich: params={:?}", params.keys().collect::<Vec<_>>());

        let response = client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Cache-Control", "no-cache")
            .header("x-api-key", api_key)
            .json(&params)
            .send()
            .await;

        match response {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();

                if !(200..300).contains(&status) {
                    return NodeResult::failed(&format!("Apollo API error ({}): {}", status, body));
                }

                let parsed: serde_json::Value = serde_json::from_str(&body)
                    .unwrap_or(serde_json::json!({}));

                let person = parsed.get("person").cloned().unwrap_or(serde_json::Value::Null);

                // Report cost: 1 export credit per enrichment call (charged even if no match)
                let cost_usd = APOLLO_COST_PER_CREDIT;
                ctx.report_usage_cost("apollo", "people_enrich", cost_usd, resolved.is_byok, Some(serde_json::json!({
                    "creditsUsed": 1,
                }))).await;

                if person.is_null() {
                    return NodeResult::completed(serde_json::json!({
                        "rawPerson": null,
                        "name": null,
                        "email": null,
                        "title": null,
                        "linkedinUrl": null,
                        "organization": null,
                        "city": null,
                        "state": null,
                        "country": null,
                        "headline": null,
                    }));
                }

                // Extract commonly used fields into typed output ports
                let name = person.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let email = person.get("email").and_then(|v| v.as_str()).unwrap_or("");
                let title = person.get("title").and_then(|v| v.as_str()).unwrap_or("");
                let linkedin = person.get("linkedin_url").and_then(|v| v.as_str()).unwrap_or("");
                let org = person.get("organization")
                    .and_then(|o| o.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let city = person.get("city").and_then(|v| v.as_str()).unwrap_or("");
                let state = person.get("state").and_then(|v| v.as_str()).unwrap_or("");
                let country = person.get("country").and_then(|v| v.as_str()).unwrap_or("");
                let headline = person.get("headline").and_then(|v| v.as_str()).unwrap_or("");

                NodeResult::completed(serde_json::json!({
                    "rawPerson": person,
                    "name": name,
                    "email": email,
                    "title": title,
                    "linkedinUrl": linkedin,
                    "organization": org,
                    "city": city,
                    "state": state,
                    "country": country,
                    "headline": headline,
                }))
            }
            Err(e) => {
                tracing::error!("Apollo API request failed: {}", e);
                NodeResult::failed(&format!("Apollo API request failed: {}", e))
            }
        }
    }
}

register_node!(ApolloEnrichNode);
