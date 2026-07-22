//! OpenRouterInference: one language-model call through OpenRouter.
//!
//! ALL model settings (model, system prompt, sampling knobs, API key) come
//! from the `config` input port: an upstream OpenRouterConfig node, overlaid
//! into this node's config bag by the engine. No config node = the defaults.
//! The node's only own field is `parseJson`: the response is JSON-repaired
//! and its top-level keys fan onto matching declared output ports.
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

use weft_core::error::WeftError;
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct OpenRouterInferenceNode;

#[async_trait]
impl Node for OpenRouterInferenceNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let prompt: String = ctx.ports.get("prompt")?;
        // The system prompt has ONE home: the `systemPrompt` field of a
        // wired OpenRouterConfig node, which the engine overlays into this
        // node's config bag. Absent config node = empty system prompt.
        let system_prompt: String = ctx.config.get_or("systemPrompt", String::new())?;
        let model: String =
            ctx.config.get_or("model", "anthropic/claude-sonnet-4.6".to_string())?;
        let parse_json: bool = ctx.config.get_or("parseJson", false)?;

        // The completion parameters, deserialized straight from the config
        // bag (the lib takes the flat camelCase fields directly; ports like
        // `prompt` live in their own bag and can't leak in). Only `reasoning`
        // is this node's own shape (a bool plus `reasoningEffort`).
        let reasoning: bool = ctx.config.get_or("reasoning", false)?;
        let mut fields = ctx.config.object()?.clone();
        fields.remove("reasoning");
        let mut cp: CompletionParameters =
            serde_json::from_value(Value::Object(fields)).node_err("completion parameters")?;
        if reasoning {
            // The checkbox IS the intent to reason; the select only tunes how
            // hard. Absent, default to a real effort ("medium"), never "none"
            // (which disables reasoning, silently contradicting the checkbox).
            let effort = ctx.config.get_or("reasoningEffort", "medium".to_string())?;
            cp = cp.with_reasoning(ReasoningConfig { effort: Some(effort), max_tokens: None, exclude: None });
        }

        let mut routing = ProviderSettings::new();
        if let Some(provider) = ctx.config.opt::<String>("provider")? {
            routing = routing.with_order(vec![provider]);
        }
        if let Some(fallbacks) = ctx.config.opt::<bool>("providerFallbacks")? {
            routing = routing.with_fallbacks(fallbacks);
        }
        if routing.order.is_some() || routing.allow_fallbacks.is_some() {
            cp = cp.with_openrouter_routing(routing);
        }

        // The whole paid-call surface: open the access, build the generator
        // over the metered client. The runtime routes the call and measures
        // its real cost behind the client.
        let access = ctx.provider_access("openrouter", ctx.config.opt("apiKey")?).await?;
        let generator = GeneratorInfo::openrouter(model)
            .with_api_key(access.credential())
            .with_app_attribution("https://weavemind.ai", "WeaveMind")
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
            .node_err("openrouter")?;
        let cancelled = ctx.cancellation();
        let response = tokio::select! {
            collected = stream.collect() => collected.node_err("openrouter")?,
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
                .node_err("openrouter: response is not repairable JSON")?;
            serde_json::from_str(&repaired)
                .node_err("openrouter: repaired JSON failed to parse")?
        } else {
            Value::String(text)
        };
        let output = if parse_json { ctx.fan_declared(&response_value) } else { NodeOutput::new() }
            .set("response", response_value);
        ctx.pulse_downstream(output).await
    }
}
