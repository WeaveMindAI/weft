//! OpenRouterInference: one language-model call through OpenRouter.
//!
//! ALL model settings (model, system prompt, sampling knobs, API key) come
//! from the `config` input: ONE plain object an upstream OpenRouterConfig
//! node emits, which THIS node reads and interprets (nothing engine-side is
//! special about the input). No config node = the defaults. The node's only
//! own setting is `parseJson`: the response is JSON-repaired and its
//! top-level keys fan onto matching declared output ports.
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

use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct OpenRouterInferenceNode;

#[async_trait]
impl Node for OpenRouterInferenceNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let prompt: String = ctx.inputs.get("prompt")?;
        let parse_json: bool = ctx.inputs.get_or("parseJson", false)?;

        // The `config` input carries ONE plain object (the wired
        // OpenRouterConfig node's output); THIS node interprets it.
        // Absent = every setting at its default.
        let cfg = ctx.inputs.nested("config")?;
        let system_prompt: String = cfg.get_or("systemPrompt", String::new())?;
        let model: String =
            cfg.get_or("model", "anthropic/claude-sonnet-4.6".to_string())?;

        // The completion parameters, deserialized straight from the
        // config object (the lib takes the flat camelCase fields
        // directly and skips keys it doesn't know, so carrying
        // model/systemPrompt/apiKey along is harmless). `reasoning` is
        // this node's own shape (a bool plus `reasoningEffort`), so it
        // is removed before the lib sees the object.
        let reasoning: bool = cfg.get_or("reasoning", false)?;
        let mut fields = cfg.object()?.clone();
        fields.remove("reasoning");
        let mut cp: CompletionParameters =
            serde_json::from_value(Value::Object(fields)).node_err("completion parameters")?;
        if reasoning {
            // The checkbox IS the intent to reason; the select only tunes how
            // hard. Absent, default to a real effort ("medium"), never "none"
            // (which disables reasoning, silently contradicting the checkbox).
            let effort = cfg.get_or("reasoningEffort", "medium".to_string())?;
            cp = cp.with_reasoning(ReasoningConfig { effort: Some(effort), max_tokens: None, exclude: None });
        }

        let mut routing = ProviderSettings::new();
        if let Some(provider) = cfg.opt::<String>("provider")? {
            routing = routing.with_order(vec![provider]);
        }
        if let Some(fallbacks) = cfg.opt::<bool>("providerFallbacks")? {
            routing = routing.with_fallbacks(fallbacks);
        }
        if routing.order.is_some() || routing.allow_fallbacks.is_some() {
            cp = cp.with_openrouter_routing(routing);
        }

        // The whole paid-call surface: open the access, build the generator
        // over the metered client. The runtime routes the call and measures
        // its real cost behind the client.
        let access = ctx.provider_access("openrouter", cfg.opt("apiKey")?).await?;
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
            err = cancelled.cancelled_err() => return Err(err),
        };

        if response.content.trim().is_empty() {
            weft_core::node_bail!(
                "openrouter: provider returned no text content (function-call only or empty \
                 response)"
            );
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
