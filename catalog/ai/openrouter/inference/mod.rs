//! OpenRouterInference: one language-model call through OpenRouter.
//!
//! The CONNECTION arrives on the `account` input: an `Access` value an
//! OpenRouterAccess node emitted (the connection was picked once, over
//! there). Model settings (model, system prompt, sampling knobs) come
//! from the `config` input: ONE plain object an upstream
//! OpenRouterConfig node emits, which THIS node reads and interprets
//! (nothing engine-side is special about either input). No config node
//! = the defaults. The node's only own setting is `parseJson`: the
//! response is JSON-repaired and its top-level keys fan onto matching
//! declared output ports.
//!
//! CONVERSATIONS ride the `history` input/output (`ChatHistory`, the
//! typed minillmlib-shaped value whose media slots hold stored files):
//! the prompt (plus any `media` attachments) is appended as the newest
//! user message, the whole conversation is externalized at this call
//! boundary (images and video as fresh presigned URLs, audio inlined
//! as base64: the audio wire takes no URLs) and deserialized straight
//! into minillmlib messages, and the reply is internalized back so the
//! emitted history again holds only stored references.
//!
//! The paid-call surface is two steps: open the connection
//! (`ctx.open`, one resolve, one lease for THIS firing), make the call
//! on its client. The runtime routes the call and measures what it
//! really cost (the call streams internally, and a Stop mid-generation
//! still gets its actual cost resolved); this node holds no cost
//! bookkeeping at all.

use async_trait::async_trait;
use minillmlib::{
    ChatNode, CompletionParameters, GeneratorInfo, Message, NodeCompletionParameters,
    ProviderSettings, ReasoningConfig,
};

use serde_json::Value;

use weft::node::NodeOutput;
use weft::storage::media::{ExternalizePolicy, MediaForm};
use weft::storage::StorageScope;
use weft::{ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::chat;

#[derive(NodeManifest)]
pub struct OpenRouterInferenceNode;

#[async_trait]
impl Node for OpenRouterInferenceNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let prompt: String = ctx.inputs.get("prompt")?;
        // `parseJson` declares a metadata default, so the bag always
        // holds a value.
        let parse_json: bool = ctx.inputs.get("parseJson")?;

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

        // The whole paid-call surface: open the wired connection (the
        // pick was made on the OpenRouterAccess node; a runtime
        // credential opens HERE, inside this firing), build the
        // generator over its client. The runtime routes the call and
        // measures its real cost behind the client.
        let account = ctx.inputs.get("account")?;
        let conn = ctx.open(&account).await?;
        let generator = GeneratorInfo::openrouter(model)
            .with_api_key(conn.credential()?)
            .with_app_attribution("https://weavemind.ai", "WeaveMind")
            .with_http_client(conn.client().clone());

        // The conversation in STORED form: the wired history (or a
        // fresh one) plus this call's user message (prompt + media,
        // media slots holding the stored-file values verbatim).
        let history_ty = ctx
            .output_type("history")
            .ok_or_else(|| weft::node_error("the history output declares no type"))?;
        let mut stored: Vec<Value> = ctx.inputs.opt("history")?.unwrap_or_default();
        // The system prompt SEEDS the conversation as its first message
        // and rides the emitted history from then on (the lib treats a
        // role-system message in the list as first-class). A history
        // already opening with a system message is the truth and the
        // config never re-inserts (silently overriding it would make
        // the sent conversation disagree with the emitted one);
        // switching instructions mid-chain is an explicit act (append a
        // system message via ChatHistoryAppend). A wired history built
        // WITHOUT one still gets the seed, so a configured prompt is
        // never silently dropped.
        let opens_with_system =
            stored.first().and_then(|m| m.get("role")).and_then(|r| r.as_str()) == Some("system");
        if !opens_with_system && !system_prompt.is_empty() {
            stored.insert(0, chat::stored_message("system", &system_prompt, &[])?);
        }
        let media = chat::media_items(ctx.inputs.opt("media")?);
        stored.push(chat::stored_message("user", &prompt, &media)?);

        // The WIRE form: media slots become material the provider
        // consumes (images/video as fresh presigned URLs, audio inline:
        // the audio wire takes no URLs), and the result deserializes
        // straight into minillmlib messages because the shapes are
        // identical apart from the media slots.
        let storage = ctx.storage(StorageScope::Project);
        let wire = storage
            .externalize(
                &Value::Array(stored.clone()),
                &history_ty,
                // Links preferred where the provider takes them (image /
                // video: the provider fetches the bytes itself, nothing
                // is downloaded or inflated here); the runtime hands out
                // a link only when the deployment can serve one the open
                // internet can fetch, and inlines the bytes otherwise.
                // Audio is inline always: the chat API only takes base64.
                ExternalizePolicy { audio: MediaForm::Inline, ..ExternalizePolicy::urls() },
            )
            .await?;
        let messages: Vec<Message> =
            serde_json::from_value(wire).node_err("chat history does not fit minillmlib messages")?;
        let (_root, leaf) =
            ChatNode::from_messages(&messages).node_err("building the conversation")?;
        let params = NodeCompletionParameters::new().with_params(cp);

        // Stream so a Stop lands mid-generation instead of after it; on
        // cancel, dropping the stream is all the wrap-up there is (the
        // metered client resolves the interrupted call's cost on its own).
        let stream = leaf
            .complete_streaming(&generator, Some(&params))
            .await
            .node_err("openrouter")?;
        let cancelled = ctx.cancellation();
        let response = tokio::select! {
            collected = stream.collect() => collected.node_err("openrouter")?,
            err = cancelled.cancelled_err() => return Err(err),
        };

        if response.content.trim().is_empty() {
            weft::node_bail!(
                "openrouter: provider returned no text content (function-call only or empty \
                 response)"
            );
        }

        // The conversation INCLUDING the reply, back in stored form:
        // internalize brings any raw reply media (a generated image's
        // data URL) into storage as references; everything already
        // stored passes through untouched.
        let assistant = serde_json::to_value(response.to_assistant_message())
            .node_err("serializing the assistant message")?;
        stored.push(assistant);
        let stored_history = storage
            .internalize(&Value::Array(stored), &history_ty, None)
            .await?;

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
            .set("response", response_value)
            .set("history", stored_history);
        ctx.pulse_downstream(output).await
    }
}
