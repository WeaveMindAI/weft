//! The shared inference plumbing behind `LlmInference` and `LlmStream`.
//!
//! Both nodes make the same call and differ only in transport (buffered
//! vs deltas on a bus), so everything else lives here: reading the
//! wired `LlmProvider` object into a minillmlib generator over the
//! opened connection's client, reading the wired params object into
//! completion parameters, assembling the conversation in stored form,
//! externalizing it at the call boundary, and internalizing the reply
//! back into stored form.
//!
//! The paid-call surface is unchanged from the shape the docs describe:
//! open the connection (`ctx.open`, one resolve, one lease for THIS
//! firing), make the call on its client. The runtime routes the call
//! and measures what it really cost where a meter exists; these nodes
//! hold no cost bookkeeping at all.

use minillmlib::{
    ChatNode, CompletionParameters, GeneratorInfo, Message, NodeCompletionParameters,
    ProviderSettings, ReasoningConfig, ToolChoice, ToolDefinition,
};
use serde_json::Value;

use weft::node::NodeOutput;
use weft::storage::media::{ExternalizePolicy, MediaForm};
use weft::storage::StorageScope;
use weft::{ExecutionContext, NodeErrExt, WeftResult};

use super::chat;

/// One assembled call: the generator (riding the opened connection's
/// client), the per-call parameters, the conversation in stored form
/// including this call's user message, and the `history` output's
/// declared type (resolved up front so a missing declaration fails
/// before the paid call, and both storage round-trips read one
/// resolution). The connection itself is not held: its lease is the
/// runtime's, released when the firing's body finishes, and the
/// generator owns the signed client.
pub struct LlmCall {
    pub generator: GeneratorInfo,
    pub params: NodeCompletionParameters,
    pub stored: Vec<Value>,
    /// Automatic cache placement is request-only. Saved history contains
    /// the author's marks, so the next call can choose fresh automatic ones.
    auto_cache: bool,
    new_turn: usize,
    history_ty: weft::WeftType,
    /// Whether this provider always stamps a finish reason on a
    /// genuinely complete reply (the named services do; a bare
    /// OpenAI-compatible server may end a complete reply by just
    /// closing the connection). Decides whether a missing finish
    /// reason is proof of truncation.
    announces_finish: bool,
    /// Reasoning is off (the default): a provider refusing the call is
    /// then most likely refusing to switch reasoning off, and the
    /// error says what to change.
    reasoning_off: bool,
}

impl LlmCall {
    /// The error a failed call surfaces. A provider refusing a call
    /// with reasoning off is told in the author's terms: the model
    /// always reasons, so the switch has to go on; every other failure
    /// passes through as it came.
    pub fn call_error(&self, err: impl std::fmt::Display) -> weft::error::WeftError {
        let text = err.to_string();
        if self.reasoning_off && text.to_ascii_lowercase().contains("reason") {
            return weft::node_error(format!(
                "llm: {text}. This model always reasons; set `reasoning: true` on its params \
                 (low effort unless you pick one)"
            ));
        }
        weft::node_error(format!("llm: {text}"))
    }
}

/// Read the node's shared inputs (`provider`, `params`, `history`,
/// `prompt`, `media`, `tools`, `toolChoice`) into one ready-to-send
/// call. Fails loudly on a provider object no provider node built.
pub async fn assemble(ctx: &ExecutionContext) -> WeftResult<LlmCall> {
    // The `provider` input carries ONE `LlmProvider` object (the wired
    // provider node's output: which service, which model, and the
    // connection to spend). THIS node interprets it.
    let provider = ctx.inputs.nested("provider")?;
    let kind: String = provider.get("kind")?;
    let model: String = provider.get("model")?;

    let mut generator = match kind.as_str() {
        "openrouter" => GeneratorInfo::openrouter(&model)
            .with_app_attribution("https://weavemind.ai", "WeaveMind"),
        "anthropic" => GeneratorInfo::anthropic(&model),
        "openai" => GeneratorInfo::openai(&model),
        "custom" => {
            let base_url: String = provider.get("baseUrl")?;
            let name: String = provider.get_or("name", "Custom".to_string())?;
            GeneratorInfo::custom(name, base_url, &model)
        }
        other => weft::node_bail!("unknown LLM provider kind '{other}'"),
    };

    // The lib's constructors read an ambient env key when one is set;
    // here ALL auth is the connection's, so the generator's own auth is
    // cleared explicitly rather than relied on to be overridden.
    generator = generator.with_auth(minillmlib::Auth::None);
    // The generator's own defaults fill every parameter the call leaves
    // unset, and the lib's carries a `maxTokens`. weft sends one only
    // when the author wrote it, so the provider's default applies
    // otherwise (the native Anthropic wire, which requires the field,
    // fills it itself).
    generator = generator.with_default_params(CompletionParameters {
        max_tokens: None,
        ..CompletionParameters::default()
    });

    // The connection: every provider node embeds the picked Access
    // marker in the object it emits; only a custom endpoint may run
    // without one (a local or unauthenticated OpenAI-compatible
    // server). The generator gets the connection's signed, measured
    // client and NO key of its own: the client signs every request
    // (which also covers services whose sign-in is more than one
    // header), and with the auth cleared the lib sends none. The
    // opened connection is not kept: its lease is the runtime's,
    // released when the firing's body finishes.
    match provider.opt::<weft::Access>("account")? {
        Some(account) => {
            let conn = ctx.open(&account).await?;
            generator = generator.with_http_client(conn.client().clone());
        }
        None if kind == "custom" => {
            generator = generator.with_http_client(ctx.http().clone());
        }
        None => weft::node_bail!(
            "the wired LlmProvider object carries no connection; pick one on the provider node"
        ),
    };

    // The `params` input carries ONE plain object (the wired LlmParams
    // node's output); absent = every setting at its default. The lib
    // takes the flat camelCase fields directly and skips keys it
    // doesn't know. `reasoning` is the params node's own shape (a bool
    // plus `reasoningEffort`), so it is removed before the lib sees the
    // object.
    let params = ctx.inputs.nested("params")?;
    let reasoning: bool = params.get_or("reasoning", false)?;
    let mut fields = params.object()?.clone();
    fields.remove("reasoning");
    let wrote_max_tokens = fields.contains_key("maxTokens");
    let mut cp: CompletionParameters =
        serde_json::from_value(Value::Object(fields)).node_err("completion parameters")?;
    // The lib fills a `maxTokens` of its own for an absent key; weft
    // sends one only when the author wrote it, so the provider's own
    // default applies otherwise (a wire that requires the field, the
    // native Anthropic one, fills it itself).
    if !wrote_max_tokens {
        cp.max_tokens = None;
    }
    let effort: Option<String> = params.opt("reasoningEffort")?;
    cp = cp.with_reasoning(reasoning_config(reasoning, effort.as_deref()));
    let reasoning_off = !reasoning;

    // OpenRouter routing rides the provider object (it is a fact of
    // WHERE the call is served, not of how the model samples); the lib
    // attaches it under the request's `provider` key and other wires
    // ignore it.
    let mut routing = ProviderSettings::new();
    if let Some(pin) = provider.opt::<String>("servingProvider")? {
        routing = routing.with_order(vec![pin]);
    }
    if let Some(fallbacks) = provider.opt::<bool>("providerFallbacks")? {
        routing = routing.with_fallbacks(fallbacks);
    }
    if routing.order.is_some() || routing.allow_fallbacks.is_some() {
        cp = cp.with_openrouter_routing(routing);
    }

    // Tools: each wired value is one LlmTool node's `{ name,
    // description, parameters }` object, handed to the lib verbatim.
    let tools: Vec<Value> = ctx.inputs.list("tools")?;
    let has_tools = !tools.is_empty();
    for tool in tools {
        let def: ToolDefinition =
            serde_json::from_value(tool).node_err("a wired tool is not a tool definition")?;
        cp = cp.with_tool(def);
    }
    // `toolChoice` declares a default ("auto"), so the bag always holds
    // it; providers reject a tool_choice without tools, so it only
    // rides when tools do.
    if let (true, Some(choice)) = (has_tools, ctx.inputs.opt::<String>("toolChoice")?) {
        let choice = match choice.as_str() {
            "auto" => ToolChoice::Auto,
            "required" => ToolChoice::Required,
            "none" => ToolChoice::None,
            name => ToolChoice::Tool(name.to_string()),
        };
        cp = cp.with_tool_choice(choice);
    }

    let mut ncp = NodeCompletionParameters::new().with_params(cp);
    if let Some(prepend) = params.opt::<String>("forcePrepend")? {
        ncp = ncp.with_force_prepend(prepend);
    }

    // The conversation in STORED form: the wired history (or a fresh
    // one) plus this call's user message (prompt + media, media slots
    // holding the stored-file values verbatim). No prompt (a tool-loop
    // re-entry after appending tool results) appends nothing.
    let mut stored: Vec<Value> = ctx.inputs.opt("history")?.unwrap_or_default();
    // The system prompt SEEDS the conversation as its first message
    // and rides the emitted history from then on (the lib treats a
    // role-system message in the list as first-class). A history
    // already opening with a system message is the truth and the
    // params never re-insert (silently overriding it would make the
    // sent conversation disagree with the emitted one); switching
    // instructions mid-chain is an explicit act (append a system
    // message via ChatHistoryAppend). A wired history built WITHOUT
    // one still gets the seed, so a configured prompt is never
    // silently dropped.
    let system_prompt: String = params.get_or("systemPrompt", String::new())?;
    let opens_with_system =
        stored.first().and_then(|m| m.get("role")).and_then(|r| r.as_str()) == Some("system");
    if !opens_with_system && !system_prompt.is_empty() {
        stored.insert(0, chat::stored_message("system", &system_prompt, &[], None)?);
    }
    let prompt: Option<String> = ctx.inputs.opt("prompt")?;
    let media: Vec<Value> = ctx.inputs.list("media")?;
    let mut new_turn = 0;
    if prompt.is_some() || !media.is_empty() {
        stored.push(chat::stored_message("user", prompt.as_deref().unwrap_or(""), &media, None)?);
        new_turn = 1;
    }
    if stored.is_empty() {
        weft::node_bail!("nothing to send: no prompt, no media, and no wired history");
    }
    // Resolved here, before the paid call: a node type without a
    // declared `history` output must fail before money is spent.
    let history_ty = ctx
        .output_type("history")
        .ok_or_else(|| weft::node_error("the history output declares no type"))?;

    Ok(LlmCall {
        generator,
        params: ncp,
        stored,
        auto_cache: ctx.inputs.get_or("autoCache", true)?,
        new_turn,
        history_ty,
        announces_finish: kind != "custom",
        reasoning_off,
    })
}

/// The WIRE form of the stored conversation: media slots become
/// material the provider consumes (images/video as fresh presigned
/// URLs, audio inline: the audio wire takes no URLs), and the result
/// deserializes straight into minillmlib messages because the shapes
/// are identical apart from the media slots. Returns the conversation
/// leaf the completion runs on.
pub async fn to_wire(ctx: &ExecutionContext, llm: &LlmCall) -> WeftResult<ChatNode> {
    let mut request = llm.stored.clone();
    if llm.auto_cache {
        chat::auto_cache_marks(&mut request, llm.new_turn);
    }
    let wire = ctx
        .storage(StorageScope::Project)
        .externalize(
            &Value::Array(request),
            &llm.history_ty,
            // Links preferred where the provider takes them (image /
            // video: the provider fetches the bytes itself, nothing
            // is downloaded or inflated here); the runtime hands out
            // a link only when it can serve one the open internet can
            // fetch, and inlines the bytes otherwise. Audio is inline
            // always: the chat API only takes base64.
            ExternalizePolicy { audio: MediaForm::Inline, ..ExternalizePolicy::urls() },
        )
        .await?;
    let messages: Vec<Message> =
        serde_json::from_value(wire).node_err("chat history does not fit minillmlib messages")?;
    let (_root, leaf) =
        ChatNode::from_messages(&messages).node_err("building the conversation")?;
    // The chain builder reads each message's text and role and starts
    // every node unmarked; the cache marks live on the NODES the wire
    // reads, so they are put back one by one, leaf to root.
    let mut node = Some(leaf.clone());
    for message in messages.iter().rev() {
        let Some(current) = node else { break };
        if message.cache_breakpoint {
            current.cache_breakpoint();
        }
        node = current.parent();
    }
    Ok(leaf)
}

/// Everything both nodes do once the reply is in hand, in one place:
/// reject a reply with no substance or no finish reason, append the
/// assistant message and internalize the conversation back into stored
/// form (any raw reply media becomes storage references; everything
/// already stored passes through untouched), and set the shared
/// output ports (`history`, and `toolCalls` only when the model
/// actually called tools, so a downstream tool branch stays
/// structurally dead on a plain reply). The caller sets its own
/// `response` port on the returned output.
pub async fn finish(
    ctx: &ExecutionContext,
    llm: LlmCall,
    response: &minillmlib::CompletionResponse,
    output: NodeOutput,
) -> WeftResult<NodeOutput> {
    ensure_reply_substance(response, llm.announces_finish)?;
    let mut stored = llm.stored;
    let assistant = serde_json::to_value(response.to_assistant_message())
        .node_err("serializing the assistant message")?;
    stored.push(assistant);
    let stored_history = ctx
        .storage(StorageScope::Project)
        .internalize(&Value::Array(stored), &llm.history_ty, None)
        .await?;
    let mut output = output.set("history", stored_history);
    if let Some(calls) = &response.tool_calls {
        if !calls.is_empty() {
            let calls = serde_json::to_value(calls).node_err("serializing tool calls")?;
            output = output.set("toolCalls", calls);
        }
    }
    Ok(output)
}

/// What the `reasoning` checkbox sends, in its two states. Off (the
/// default) sends effort `none`, the lib's spelling for "off", which
/// every wire translates so that a model that is off stays off and one
/// that cannot switch off refuses the request loudly; that refusal is
/// the answer (see [`LlmCall::call_error`]). On sends the chosen
/// effort, `low` when none was picked: the checkbox IS the intent to
/// reason, and the cheap, fast effort is the one you get without
/// asking for more. There is no third state: a model never runs at a
/// default nobody wrote down.
pub fn reasoning_config(reasoning: bool, effort: Option<&str>) -> ReasoningConfig {
    let effort = if reasoning { effort.unwrap_or("low") } else { "none" };
    ReasoningConfig { effort: Some(effort.to_string()), max_tokens: None, exclude: None }
}

/// A reply with neither text, nor tool calls, nor media answered
/// nothing a downstream node can act on: fail loudly, and when the
/// usage says the model spent its whole budget thinking, say so (the
/// fix is a bigger `maxTokens` or reasoning off, never a retry). And
/// when the provider is one that always stamps a finish reason on a
/// genuinely complete reply, a missing one means the connection
/// dropped mid-generation: the reply is truncated, fail loudly rather
/// than chain a half-answer into the conversation. (A bare
/// OpenAI-compatible server may end a complete reply by just closing,
/// so absence proves nothing there and is accepted.)
fn ensure_reply_substance(
    response: &minillmlib::CompletionResponse,
    announces_finish: bool,
) -> WeftResult<()> {
    if response.content.trim().is_empty()
        && response.tool_calls.as_ref().map_or(true, |calls| calls.is_empty())
        && response.media.is_empty()
    {
        weft::node_bail!("{}", empty_reply_message(response.usage.as_ref()));
    }
    if announces_finish && response.finish_reason.is_none() {
        weft::node_bail!(
            "the provider's stream ended without a finish reason: the reply is truncated"
        );
    }
    Ok(())
}

/// The message for a reply with nothing in it. When the usage shows
/// reasoning tokens and no text, the budget went to thinking: name
/// the two knobs that change that.
pub fn empty_reply_message(usage: Option<&minillmlib::Usage>) -> String {
    match usage.and_then(|u| u.reasoning_tokens).filter(|n| *n > 0) {
        Some(reasoning) => format!(
            "the model spent {reasoning} reasoning tokens and answered 0 text tokens: raise \
             maxTokens so the answer fits after the thinking, or set reasoning off"
        ),
        None => "the provider returned an empty response (no text, tool calls, or media)".into(),
    }
}
