//! Apollo Organization Search Node
//!
//! Searches Apollo.io's database for organizations matching filter criteria.
//! Uses POST /api/v1/mixed_companies/search
//!
//! Returns a list of organizations with basic info (name, industry, size,
//! location, domain). Useful for finding target companies first, then
//! searching for people within them via ApolloSearch with organizationIds.
//!
//! This endpoint consumes Apollo credits.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};
use rand::Rng;

/// Apollo org search: 1 credit per search request.
/// Raw cost: Basic monthly plan ($59/mo, 2,500 credits/mo) = $0.0236/credit.
/// Margin is applied downstream by get_user_margin().
const APOLLO_COST_PER_CREDIT: f64 = 0.0236;

#[derive(Default)]
pub struct ApolloOrgSearchNode;

const APOLLO_API_BASE: &str = "https://api.apollo.io/api/v1";

#[async_trait]
impl Node for ApolloOrgSearchNode {
    fn node_type(&self) -> &'static str {
        "ApolloOrgSearch"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Apollo Org Search",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", false),
                PortDef::new("organizationLocations", "List[String]", false),
                PortDef::new("employeeRanges", "List[String]", false),
                PortDef::new("industries", "List[String]", false),
                PortDef::new("keywords", "List[String]", false),
                PortDef::new("revenueMin", "Number", false),
                PortDef::new("revenueMax", "Number", false),
                PortDef::new("page", "Number", false),
            ],
            outputs: vec![
                PortDef::new("ids", "List[String]", false),
                PortDef::new("names", "List[String]", false),
                PortDef::new("domains", "List[String]", false),
                PortDef::new("websiteUrls", "List[String]", false),
                PortDef::new("linkedinUrls", "List[String]", false),
                PortDef::new("twitterUrls", "List[String]", false),
                PortDef::new("facebookUrls", "List[String]", false),
                PortDef::new("phones", "List[String]", false),
                PortDef::new("foundedYears", "List[String]", false),
                PortDef::new("languages", "List[String]", false),
                PortDef::new("totalEntries", "Number", false),
                PortDef::new("rawOrganizations", "List[JsonDict]", false),
            ],
            features: NodeFeatures {
                oneOfRequired: vec![vec![
                    "organizationLocations".into(), "employeeRanges".into(),
                    "industries".into(), "keywords".into(),
                    "revenueMin".into(), "revenueMax".into(),
                ]],
                ..Default::default()
            },
            fields: vec![
                FieldDef::number("perPage").with_default(serde_json::json!(10)).with_range(1.0, 100.0),
                FieldDef::checkbox("randomizePage").with_default(serde_json::json!(false)),
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

        let per_page = ctx.config_u64("perPage", 10).min(100) as u32;
        let randomize = ctx.config.get("randomizePage")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Page priority: input port > randomize > default 1
        let explicit_page = ctx.input.get("page")
            
            .and_then(|v| v.as_f64())
            .map(|f| (f as u32).max(1));

        let page: u32 = if let Some(p) = explicit_page {
            p
        } else if randomize {
            // Pick a random page in Apollo's valid range [1, 500]
            rand::thread_rng().gen_range(1..=500)
        } else {
            1
        };

        let mut params = serde_json::Map::new();
        params.insert("per_page".to_string(), serde_json::json!(per_page));
        params.insert("page".to_string(), serde_json::json!(page));

        let port_to_param = [
            ("organizationLocations", "organization_locations"),
            ("employeeRanges", "organization_num_employees_ranges"),
        ];

        // Industries map to q_organization_keyword_tags (not organization_industry_tag_ids)
        let industries_value = ctx.input.get("industries")
            ;
        if let Some(v) = industries_value {
            if !v.is_null() {
                let arr = if let Some(s) = v.as_str() {
                    serde_json::from_str::<serde_json::Value>(s)
                        .unwrap_or(serde_json::json!([s]))
                } else {
                    v.clone()
                };
                params.insert("q_organization_keyword_tags".to_string(), arr);
            }
        }

        for (port_name, api_param) in &port_to_param {
            let value = ctx.input.get(*port_name)
                .or_else(|| ctx.config.get(*port_name));
            if let Some(v) = value {
                if !v.is_null() {
                    let arr = if let Some(s) = v.as_str() {
                        serde_json::from_str::<serde_json::Value>(s)
                            .unwrap_or(serde_json::json!([s]))
                    } else {
                        v.clone()
                    };
                    params.insert(api_param.to_string(), arr);
                }
            }
        }

        // q_organization_keyword_tags takes an array of keyword strings
        let keywords_value = ctx.input.get("keywords")
            ;
        if let Some(v) = keywords_value {
            if !v.is_null() {
                let arr = if v.is_array() {
                    v.clone()
                } else if let Some(s) = v.as_str() {
                    serde_json::json!([s])
                } else {
                    serde_json::json!([v.to_string()])
                };
                params.insert("q_organization_keyword_tags".to_string(), arr);
            }
        }

        // revenue_range expects {min, max} integers
        let revenue_min = ctx.input.get("revenueMin")
            
            .and_then(|v| v.as_f64())
            .map(|f| f as i64);
        let revenue_max = ctx.input.get("revenueMax")
            
            .and_then(|v| v.as_f64())
            .map(|f| f as i64);
        if revenue_min.is_some() || revenue_max.is_some() {
            let mut range = serde_json::Map::new();
            if let Some(min) = revenue_min { range.insert("min".to_string(), serde_json::json!(min)); }
            if let Some(max) = revenue_max { range.insert("max".to_string(), serde_json::json!(max)); }
            params.insert("revenue_range".to_string(), serde_json::Value::Object(range));
        }

        let url = format!("{}/mixed_companies/search", APOLLO_API_BASE);
        let client = reqwest::Client::new();

        tracing::info!("Apollo Org Search: page={}, per_page={}, randomize={}", page, per_page, randomize);

        // Helper: send the search request with the given page
        let send_request = |client: &reqwest::Client, params: &serde_json::Map<String, serde_json::Value>| {
            client
                .post(&url)
                .header("Content-Type", "application/json")
                .header("Cache-Control", "no-cache")
                .header("x-api-key", api_key)
                .json(params)
                .send()
        };

        let response = send_request(&client, &params).await;

        let (parsed, raw_orgs, total) = match response {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();

                if !(200..300).contains(&status) {
                    return NodeResult::failed(&format!("Apollo API error ({}): {}", status, body));
                }

                let parsed: serde_json::Value = serde_json::from_str(&body)
                    .unwrap_or(serde_json::json!({}));

                let raw_orgs = parsed.get("organizations")
                    .or_else(|| parsed.get("accounts"))
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let total = parsed.get("total_entries").cloned().unwrap_or(serde_json::json!(0));

                (parsed, raw_orgs, total)
            }
            Err(e) => {
                tracing::error!("Apollo API request failed: {}", e);
                return NodeResult::failed(&format!("Apollo API request failed: {}", e));
            }
        };

        // If randomize was on, we got no results, and there ARE results in the
        // search space, pick a valid random page and retry. The out-of-range
        // call was free (0 credits), so this second call is the only one charged.
        let (raw_orgs, total) = if randomize && explicit_page.is_none() && raw_orgs.is_empty() {
            let total_pages = parsed.get("pagination")
                .and_then(|p| p.get("total_pages"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32;

            if total_pages == 0 {
                // Genuinely no results for this search
                (raw_orgs, total)
            } else {
                let retry_page = rand::thread_rng().gen_range(1..=total_pages);
                tracing::info!("Apollo Org Search: randomize retry, total_pages={}, retry_page={}", total_pages, retry_page);

                params.insert("page".to_string(), serde_json::json!(retry_page));
                match send_request(&client, &params).await {
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        let body = resp.text().await.unwrap_or_default();

                        if !(200..300).contains(&status) {
                            return NodeResult::failed(&format!("Apollo API error on retry ({}): {}", status, body));
                        }

                        let parsed: serde_json::Value = serde_json::from_str(&body)
                            .unwrap_or(serde_json::json!({}));

                        let raw_orgs = parsed.get("organizations")
                            .or_else(|| parsed.get("accounts"))
                            .and_then(|v| v.as_array())
                            .cloned()
                            .unwrap_or_default();
                        let total = parsed.get("total_entries").cloned().unwrap_or(serde_json::json!(0));
                        (raw_orgs, total)
                    }
                    Err(e) => {
                        tracing::error!("Apollo API retry request failed: {}", e);
                        return NodeResult::failed(&format!("Apollo API request failed: {}", e));
                    }
                }
            }
        } else {
            (raw_orgs, total)
        };

        // Report cost: 1 credit per org search request that returns results.
        // Out-of-range pages are free (confirmed empirically).
        let cost_usd = APOLLO_COST_PER_CREDIT;
        ctx.report_usage_cost("apollo", "org_search", cost_usd, resolved.is_byok, Some(serde_json::json!({
            "creditsUsed": 1,
        }))).await;

        let ids: Vec<serde_json::Value> = raw_orgs.iter()
            .map(|o| serde_json::json!(o.get("id").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let names: Vec<serde_json::Value> = raw_orgs.iter()
            .map(|o| serde_json::json!(o.get("name").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let domains: Vec<serde_json::Value> = raw_orgs.iter()
            .map(|o| serde_json::json!(o.get("primary_domain").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let website_urls: Vec<serde_json::Value> = raw_orgs.iter()
            .map(|o| serde_json::json!(o.get("website_url").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let linkedin_urls: Vec<serde_json::Value> = raw_orgs.iter()
            .map(|o| serde_json::json!(o.get("linkedin_url").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let twitter_urls: Vec<serde_json::Value> = raw_orgs.iter()
            .map(|o| serde_json::json!(o.get("twitter_url").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let facebook_urls: Vec<serde_json::Value> = raw_orgs.iter()
            .map(|o| serde_json::json!(o.get("facebook_url").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let phones: Vec<serde_json::Value> = raw_orgs.iter()
            .map(|o| serde_json::json!(o.get("phone").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let founded_years: Vec<serde_json::Value> = raw_orgs.iter()
            .map(|o| serde_json::json!(o.get("founded_year").and_then(|v| v.as_i64()).unwrap_or(0)))
            .collect();
        let languages_list: Vec<serde_json::Value> = raw_orgs.iter()
            .map(|o| {
                let langs = o.get("languages")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter()
                        .filter_map(|l| l.as_str())
                        .collect::<Vec<_>>()
                        .join(", "))
                    .unwrap_or_default();
                serde_json::json!(langs)
            })
            .collect();

        NodeResult::completed(serde_json::json!({
            "ids": ids,
            "names": names,
            "domains": domains,
            "websiteUrls": website_urls,
            "linkedinUrls": linkedin_urls,
            "twitterUrls": twitter_urls,
            "facebookUrls": facebook_urls,
            "phones": phones,
            "foundedYears": founded_years,
            "languages": languages_list,
            "totalEntries": total,
            "rawOrganizations": raw_orgs,
        }))
    }
}

register_node!(ApolloOrgSearchNode);
