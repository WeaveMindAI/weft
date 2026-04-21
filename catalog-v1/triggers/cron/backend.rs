//! Cron trigger node - fires events on a schedule defined by a cron expression.

use async_trait::async_trait;
use std::str::FromStr;

use crate::node::{
    ExecutionContext, Node, NodeFeatures, NodeMetadata, PortDef,
    TriggerCategory, TriggerContext, TriggerError, TriggerHandle,
    TriggerStartConfig, FieldDef,
};
use crate::{register_node, NodeResult};

pub struct CronNode;

#[async_trait]
impl Node for CronNode {
    fn node_type(&self) -> &'static str {
        "Cron"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Cron Schedule",
            inputs: vec![],
            outputs: vec![
                PortDef::new("scheduledTime", "String", false),
                PortDef::new("actualTime", "String", false),
            ],
            features: NodeFeatures {
                isTrigger: true,
                triggerCategory: Some(TriggerCategory::Schedule),
                ..Default::default()
            },
            fields: vec![
                FieldDef::text("cron"),
            ],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        if ctx.isTriggerSetup {
            return NodeResult::completed(ctx.input.clone());
        }
        let payload = ctx.input.get("triggerPayload")
            .cloned()
            .unwrap_or(ctx.input.clone());
        NodeResult::completed(payload)
    }

    async fn keep_alive(&self,
        config: TriggerStartConfig,
        ctx: TriggerContext,
    ) -> Result<TriggerHandle, TriggerError> {
        let raw_cron = config.require_str("cron")?;

        // The cron crate expects 6-field expressions (sec min hour dom month dow).
        // Standard 5-field cron (min hour dom month dow) is auto-converted by prepending "0" for seconds.
        let cron_expr = match raw_cron.split_whitespace().count() {
            5 => format!("0 {}", raw_cron),
            6 | 7 => raw_cron,
            n => return Err(TriggerError::Config(
                format!("Invalid cron expression: expected 5, 6, or 7 fields, got {}", n)
            )),
        };

        let schedule = cron::Schedule::from_str(&cron_expr)
            .map_err(|e| TriggerError::Config(format!("Invalid cron expression: {}", e)))?;

        ctx.spawn(&config, TriggerCategory::Schedule, move |emit, shutdown| async move {
            tokio::pin!(shutdown);
            loop {
                let next = schedule.upcoming(chrono::Utc).next()
                    .ok_or_else(|| TriggerError::Config("No upcoming schedule".to_string()))?;
                let duration = (next - chrono::Utc::now())
                    .to_std()
                    .unwrap_or(std::time::Duration::from_secs(60));

                tokio::select! {
                    _ = &mut shutdown => break,
                    _ = tokio::time::sleep(duration) => {
                        emit.emit(serde_json::json!({
                            "scheduledTime": next.to_rfc3339(),
                            "actualTime": chrono::Utc::now().to_rfc3339(),
                        }))?;
                    }
                }
            }
            Ok(())
        })
    }
}

register_node!(CronNode);
