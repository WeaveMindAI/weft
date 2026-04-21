//! Apollo Organization Enrichment Node
//!
//! Enriches an organization with full company data from Apollo.io.
//! Uses GET /api/v1/organizations/enrich?domain=<domain>
//!
//! Takes a domain (from ApolloOrgSearch) and returns the full org profile:
//! industry, employee count, description, revenue, funding, location, keywords, etc.
//!
//! This endpoint CONSUMES Apollo credits.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext};
use crate::{NodeResult, register_node};

/// Apollo enrichment: 1 credit per call.
/// Raw cost: Basic monthly plan ($59/mo, 2,500 credits/mo) = $0.0236/credit.
/// Margin is applied downstream by get_user_margin().
const APOLLO_COST_PER_CREDIT: f64 = 0.0236;

#[derive(Default)]
pub struct ApolloOrgEnrichNode;

const APOLLO_API_BASE: &str = "https://api.apollo.io/api/v1";

#[async_trait]
impl Node for ApolloOrgEnrichNode {
    fn node_type(&self) -> &'static str {
        "ApolloOrgEnrich"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Apollo Org Enrich",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", false),
                PortDef::new("domain", "String", true),
            ],
            outputs: vec![
                PortDef::new("name", "String", false),
                PortDef::new("industry", "String", false),
                PortDef::new("shortDescription", "String", false),
                PortDef::new("estimatedEmployees", "Number", false),
                PortDef::new("annualRevenue", "Number", false),
                PortDef::new("city", "String", false),
                PortDef::new("state", "String", false),
                PortDef::new("country", "String", false),
                PortDef::new("keywords", "String", false),
                PortDef::new("latestFundingStage", "String", false),
                PortDef::new("totalFunding", "Number", false),
                PortDef::new("linkedinUrl", "String", false),
                PortDef::new("websiteUrl", "String", false),
                PortDef::new("rawOrganization", "JsonDict", false),
            ],
            features: NodeFeatures {
                ..Default::default()
            },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let domain = match ctx.input.get("domain").and_then(|v| v.as_str()) {
            Some(d) if !d.is_empty() => d,
            _ => return NodeResult::failed("Domain is required (e.g. 'apollo.io')"),
        };

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

        let url = format!("{}/organizations/enrich?domain={}", APOLLO_API_BASE, domain);
        let client = reqwest::Client::new();

        tracing::info!("Apollo Org Enrich: domain={}", domain);

        let response = client
            .get(&url)
            .header("Cache-Control", "no-cache")
            .header("Content-Type", "application/json")
            .header("x-api-key", &resolved.key)
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

                let org = parsed.get("organization").cloned().unwrap_or(serde_json::Value::Null);

                // Report cost: 1 credit per enrichment call
                let cost_usd = APOLLO_COST_PER_CREDIT;
                ctx.report_usage_cost("apollo", "org_enrich", cost_usd, resolved.is_byok, Some(serde_json::json!({
                    "creditsUsed": 1,
                }))).await;

                if org.is_null() {
                    return NodeResult::completed(serde_json::json!({
                        "name": null,
                        "industry": null,
                        "shortDescription": null,
                        "estimatedEmployees": null,
                        "annualRevenue": null,
                        "city": null,
                        "state": null,
                        "country": null,
                        "keywords": null,
                        "latestFundingStage": null,
                        "totalFunding": null,
                        "linkedinUrl": null,
                        "websiteUrl": null,
                        "rawOrganization": null,
                    }));
                }

                let name = org.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let industry = org.get("industry").and_then(|v| v.as_str()).unwrap_or("");
                let short_description = org.get("short_description").and_then(|v| v.as_str()).unwrap_or("");
                let estimated_employees = org.get("estimated_num_employees").and_then(|v| v.as_i64()).unwrap_or(0);
                let annual_revenue = org.get("annual_revenue").and_then(|v| v.as_i64()).unwrap_or(0);
                let city = org.get("city").and_then(|v| v.as_str()).unwrap_or("");
                let state = org.get("state").and_then(|v| v.as_str()).unwrap_or("");
                let country = org.get("country").and_then(|v| v.as_str()).unwrap_or("");
                let keywords = org.get("keywords")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter()
                        .filter_map(|k| k.as_str())
                        .collect::<Vec<_>>()
                        .join(", "))
                    .unwrap_or_default();
                let latest_funding_stage = org.get("latest_funding_stage").and_then(|v| v.as_str()).unwrap_or("");
                let total_funding = org.get("total_funding").and_then(|v| v.as_i64()).unwrap_or(0);
                let linkedin_url = org.get("linkedin_url").and_then(|v| v.as_str()).unwrap_or("");
                let website_url = org.get("website_url").and_then(|v| v.as_str()).unwrap_or("");

                NodeResult::completed(serde_json::json!({
                    "name": name,
                    "industry": industry,
                    "shortDescription": short_description,
                    "estimatedEmployees": estimated_employees,
                    "annualRevenue": annual_revenue,
                    "city": city,
                    "state": state,
                    "country": country,
                    "keywords": keywords,
                    "latestFundingStage": latest_funding_stage,
                    "totalFunding": total_funding,
                    "linkedinUrl": linkedin_url,
                    "websiteUrl": website_url,
                    "rawOrganization": org,
                }))
            }
            Err(e) => {
                tracing::error!("Apollo API request failed: {}", e);
                NodeResult::failed(&format!("Apollo API request failed: {}", e))
            }
        }
    }
}

register_node!(ApolloOrgEnrichNode);
