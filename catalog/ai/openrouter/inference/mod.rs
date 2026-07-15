//! OpenRouterInference: one language-model call through OpenRouter.
//!
//! Settings come from the `config` input port (an upstream OpenRouterConfig
//! node) overlaid on this node's own config. With `parseJson`, the response is
//! JSON-repaired and its top-level keys fan onto matching declared output ports.
//!
//! The paid-call surface is two steps: open the access, make the call on the
//! metered client. The runtime routes the call and measures what it really
//! cost (the call streams internally, and a Stop mid-generation still gets
//! its actual cost resolved); this node holds no cost bookkeeping at all.

use async_trait::async_trait;
use minillmlib::{
    ChatNode, CompletionParameters, GeneratorInfo, NodeCompletionParameters, ProviderSettings,
    ReasoningConfig,
};

use serde_json::Value;

use weft_core::context::ConfigBag;
use weft_core::error::WeftError;
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct OpenRouterInferenceNode;

/// The completion parameters, deserialized straight from the config (the lib
/// takes the flat camelCase fields directly). Only `reasoning` is this node's
/// own shape (a bool plus `reasoningEffort`), mapped after.
fn completion_params(cfg: &ConfigBag) -> WeftResult<CompletionParameters> {
    let reasoning = cfg.get_optional::<bool>("reasoning")?.unwrap_or(false);
    let mut fields: serde_json::Map<String, Value> =
        cfg.values.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    fields.remove("reasoning");

    let mut cp: CompletionParameters = serde_json::from_value(Value::Object(fields))
        .map_err(|e| WeftError::Config(format!("completion parameters: {e}")))?;
    if reasoning {
        // The checkbox IS the intent to reason; the select only tunes how
        // hard. Absent, default to a real effort ("medium"), never "none"
        // (which disables reasoning, silently contradicting the checkbox).
        let effort =
            cfg.get_optional::<String>("reasoningEffort")?.unwrap_or_else(|| "medium".to_string());
        cp = cp.with_reasoning(ReasoningConfig { effort: Some(effort), max_tokens: None, exclude: None });
    }
    Ok(cp)
}

#[async_trait]
impl Node for OpenRouterInferenceNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let cfg = ctx.effective_config();
        let prompt: String = ctx.input.get("prompt")?;
        // systemPrompt: the input port wins over config.
        let system_prompt = ctx
            .input
            .raw("systemPrompt")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .or(cfg.get_optional("systemPrompt")?)
            .unwrap_or_default();
        let model = cfg
            .get_optional::<String>("model")?
            .unwrap_or_else(|| "anthropic/claude-sonnet-4.6".to_string());
        let parse_json = cfg.get_optional::<bool>("parseJson")?.unwrap_or(false);

        let mut cp = completion_params(&cfg)?;
        let mut routing = ProviderSettings::new();
        if let Some(provider) = cfg.get_optional::<String>("provider")? {
            routing = routing.with_order(vec![provider]);
        }
        if let Some(fallbacks) = cfg.get_optional::<bool>("providerFallbacks")? {
            routing = routing.with_fallbacks(fallbacks);
        }
        if routing.order.is_some() || routing.allow_fallbacks.is_some() {
            cp = cp.with_openrouter_routing(routing);
        }

        // The whole paid-call surface: open the access, build the generator
        // over the metered client. The runtime routes the call and measures
        // its real cost behind the client.
        let access = ctx.provider_access("openrouter", cfg.get_optional("apiKey")?).await?;
        let generator = GeneratorInfo::openrouter(model)
            .with_api_key(access.credential())
            .with_app_attribution("https://weavemind.ai", "Weft")
            .with_http_client(ctx.metered_client(&access)?);

        let root = ChatNode::root(system_prompt);
        let user = root.add_user(prompt);
        let params = NodeCompletionParameters::new().with_params(cp);

        // Stream so a Stop lands mid-generation instead of after it; on
        // cancel, dropping the stream is all the wrap-up there is (the
        // metered client resolves the interrupted call's cost on its own).
        let stream = user
            .complete_streaming(&generator, Some(&params))
            .await
            .map_err(|e| WeftError::NodeExecution(format!("openrouter: {e}")))?;
        let cancelled = ctx.cancellation();
        let response = tokio::select! {
            collected = stream.collect() => collected
                .map_err(|e| WeftError::NodeExecution(format!("openrouter: {e}")))?,
            _ = cancelled.cancelled() => return Err(WeftError::Cancelled),
        };

        if response.content.trim().is_empty() {
            return Err(WeftError::NodeExecution(
                "openrouter: provider returned no text content (function-call only or empty \
                 response)"
                    .into(),
            ));
        }
        let text = response.content;

        // parseJson: repair the reply with the lib's JSON repairer (the
        // streaming transport skips the lib's post-processing, so the node
        // applies it here); an unrepairable reply fails loudly.
        let response_value = if parse_json {
            let repaired = minillmlib::repair_json(&text, &minillmlib::RepairOptions::default())
                .map_err(|e| {
                    WeftError::NodeExecution(format!(
                        "openrouter: response is not repairable JSON: {e}"
                    ))
                })?;
            serde_json::from_str(&repaired).map_err(|e| {
                WeftError::NodeExecution(format!("openrouter: repaired JSON failed to parse: {e}"))
            })?
        } else {
            Value::String(text)
        };
        let output = if parse_json { ctx.fan_declared(&response_value) } else { NodeOutput::empty() }
            .set("response", response_value);
        ctx.pulse_downstream(output).await
    }
}
