//! Apollo People Search Node
//!
//! Searches Apollo.io's database for people matching filter criteria.
//! Uses POST /api/v1/mixed_people/api_search
//!
//! This endpoint is free (does not consume Apollo credits) and returns
//! lightweight person records (id, first_name, obfuscated last_name, title,
//! organization). It does NOT return emails or phone numbers.
//!
//! Use ApolloEnrich to get full profiles including contact info.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};
use rand::Rng;

#[derive(Default)]
pub struct ApolloSearchNode;

const APOLLO_API_BASE: &str = "https://api.apollo.io/api/v1";

#[async_trait]
impl Node for ApolloSearchNode {
    fn node_type(&self) -> &'static str {
        "ApolloSearch"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Apollo Search",
            inputs: vec![
                PortDef::wired_only("config", "Dict[String, String]", false),
                PortDef::new("personTitles", "List[String]", false),
                PortDef::new("personSeniorities", "List[String]", false),
                PortDef::new("personLocations", "List[String]", false),
                PortDef::new("organizationLocations", "List[String]", false),
                PortDef::new("employeeRanges", "List[String]", false),
                PortDef::new("keywords", "List[String]", false),
                PortDef::new("industries", "List[String]", false),
                PortDef::new("organizationIds", "List[String]", false),
                PortDef::new("page", "Number", false),
            ],
            outputs: vec![
                PortDef::new("ids", "List[String]", false),
                PortDef::new("firstNames", "List[String]", false),
                PortDef::new("lastNames", "List[String | Null]", false),
                PortDef::new("titles", "List[String]", false),
                PortDef::new("companyNames", "List[String]", false),
                PortDef::new("linkedinUrls", "List[String | Null]", false),
                PortDef::new("hasEmail", "List[Boolean]", false),
                PortDef::new("totalEntries", "Number", false),
                PortDef::new("rawPeople", "List[JsonDict]", false),
            ],
            features: NodeFeatures {
                oneOfRequired: vec![vec![
                    "personTitles".into(), "personSeniorities".into(),
                    "personLocations".into(), "organizationLocations".into(),
                    "employeeRanges".into(), "keywords".into(),
                    "industries".into(), "organizationIds".into(),
                ]],
                ..Default::default()
            },
            fields: vec![
                FieldDef::number("perPage").with_default(serde_json::json!(10)).with_range(1.0, 100.0),
                FieldDef::checkbox("requireEmail").with_default(serde_json::json!(true)),
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

        let explicit_page = ctx.input.get("page")
            
            .and_then(|v| v.as_f64())
            .map(|f| (f as u32).max(1));

        let page: u32 = if let Some(p) = explicit_page {
            p
        } else if randomize {
            rand::thread_rng().gen_range(1..=500)
        } else {
            1
        };

        // Build query params from inputs
        let mut params = serde_json::Map::new();
        params.insert("per_page".to_string(), serde_json::json!(per_page));
        params.insert("page".to_string(), serde_json::json!(page));

        // Optionally require people to have an email available (config: requireEmail, default true)
        let require_email = ctx.config.get("requireEmail")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if require_email {
            params.insert(
                "contact_email_status".to_string(),
                serde_json::json!(["verified", "likely to engage"]),
            );
        }

        // Map input ports to Apollo API parameter names
        let port_to_param = [
            ("personTitles", "person_titles"),
            ("personSeniorities", "person_seniorities"),
            ("personLocations", "person_locations"),
            ("organizationLocations", "organization_locations"),
            ("employeeRanges", "organization_num_employees_ranges"),
            ("organizationIds", "organization_ids"),
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

        // q_keywords: skip when organizationIds is set,the triple AND (org + titles + keywords)
        // over-constrains and returns zero results. Keywords are useful for broad searches only.
        let has_org_ids = ctx.input.get("organizationIds")
            
            .map(|v| !v.is_null() && v.as_array().map(|a| !a.is_empty()).unwrap_or(false))
            .unwrap_or(false);

        if !has_org_ids {
            let keywords_value = ctx.input.get("keywords")
                ;
            if let Some(v) = keywords_value {
                if !v.is_null() {
                    let joined = if let Some(s) = v.as_str() {
                        s.to_string()
                    } else if let Some(arr) = v.as_array() {
                        arr.iter()
                            .filter_map(|item| item.as_str())
                            .collect::<Vec<_>>()
                            .join(" ")
                    } else {
                        v.to_string()
                    };
                    if !joined.is_empty() {
                        params.insert("q_keywords".to_string(), serde_json::json!(joined));
                    }
                }
            }
        }

        let url = format!("{}/mixed_people/api_search", APOLLO_API_BASE);
        let client = reqwest::Client::new();

        tracing::info!("Apollo People Search: page={}, per_page={}, randomize={}", page, per_page, randomize);

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

        let (parsed, raw_people, total) = match response {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body = resp.text().await.unwrap_or_default();

                if !(200..300).contains(&status) {
                    return NodeResult::failed(&format!("Apollo API error ({}): {}", status, body));
                }

                let parsed: serde_json::Value = serde_json::from_str(&body)
                    .unwrap_or(serde_json::json!({}));

                let raw_people = parsed.get("people")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let total = parsed.get("total_entries").cloned().unwrap_or(serde_json::json!(0));

                (parsed, raw_people, total)
            }
            Err(e) => {
                tracing::error!("Apollo API request failed: {}", e);
                return NodeResult::failed(&format!("Apollo API request failed: {}", e));
            }
        };

        // If randomize overshot, use total_pages to pick a valid page and retry.
        // People search is free so this costs nothing.
        let (raw_people, total) = if randomize && explicit_page.is_none() && raw_people.is_empty() {
            let total_pages = parsed.get("pagination")
                .and_then(|p| p.get("total_pages"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32;

            if total_pages == 0 {
                (raw_people, total)
            } else {
                let retry_page = rand::thread_rng().gen_range(1..=total_pages);
                tracing::info!("Apollo People Search: randomize retry, total_pages={}, retry_page={}", total_pages, retry_page);

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

                        let raw_people = parsed.get("people")
                            .and_then(|v| v.as_array())
                            .cloned()
                            .unwrap_or_default();
                        let total = parsed.get("total_entries").cloned().unwrap_or(serde_json::json!(0));
                        (raw_people, total)
                    }
                    Err(e) => {
                        tracing::error!("Apollo API retry request failed: {}", e);
                        return NodeResult::failed(&format!("Apollo API request failed: {}", e));
                    }
                }
            }
        } else {
            (raw_people, total)
        };

        let ids: Vec<serde_json::Value> = raw_people.iter()
            .map(|p| serde_json::json!(p.get("id").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let first_names: Vec<serde_json::Value> = raw_people.iter()
            .map(|p| serde_json::json!(p.get("first_name").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let last_names: Vec<serde_json::Value> = raw_people.iter()
            .map(|p| match p.get("last_name").and_then(|v| v.as_str()) {
                Some(s) => serde_json::json!(s),
                None => serde_json::Value::Null,
            })
            .collect();
        let titles: Vec<serde_json::Value> = raw_people.iter()
            .map(|p| serde_json::json!(p.get("title").and_then(|v| v.as_str()).unwrap_or("")))
            .collect();
        let company_names: Vec<serde_json::Value> = raw_people.iter()
            .map(|p| {
                let name = p.get("organization")
                    .and_then(|o| o.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                serde_json::json!(name)
            })
            .collect();
        let linkedin_urls: Vec<serde_json::Value> = raw_people.iter()
            .map(|p| match p.get("linkedin_url").and_then(|v| v.as_str()) {
                Some(s) => serde_json::json!(s),
                None => serde_json::Value::Null,
            })
            .collect();
        let has_email: Vec<serde_json::Value> = raw_people.iter()
            .map(|p| serde_json::json!(p.get("has_email").and_then(|v| v.as_bool()).unwrap_or(false)))
            .collect();

        // People search is free (0 credits), but track for analytics
        ctx.report_usage_cost("apollo", "people_search", 0.0, resolved.is_byok, None).await;

        NodeResult::completed(serde_json::json!({
            "ids": ids,
            "firstNames": first_names,
            "lastNames": last_names,
            "titles": titles,
            "companyNames": company_names,
            "linkedinUrls": linkedin_urls,
            "hasEmail": has_email,
            "totalEntries": total,
            "rawPeople": raw_people,
        }))
    }
}

register_node!(ApolloSearchNode);
