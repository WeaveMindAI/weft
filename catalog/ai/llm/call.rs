//! The shared inference plumbing behind `LlmInference`,
//! `LlmInferenceJson` and `LlmStream`.
//!
//! The three nodes make the same call and differ only in transport
//! (buffered vs deltas on a bus) and in what they hand out on
//! `response` (the text, or the text parsed as JSON and held to the
//! type the program declared), so everything else lives here: reading
//! the wired `LlmProvider` object into a minillmlib generator over the
//! opened connection's client, reading the wired params object into
//! completion parameters, assembling the conversation in stored form,
//! externalizing it at the call boundary, the buffered call itself, and
//! internalizing the reply back into stored form.
//!
//! The paid-call surface is unchanged from the shape the docs describe:
//! open the connection (`ctx.open`, one resolve, one lease for THIS
//! firing), make the call on its client. The runtime routes the call
//! and measures what it really cost where a meter exists; these nodes
//! hold no cost bookkeeping at all.

use minillmlib::{
    ChatNode, CompletionParameters, CompletionResponse, GeneratorInfo, Message, MiniLLMError,
    NodeCompletionParameters, OpenRouterRouting, Reasoning, ToolChoice, ToolDefinition,
};
use serde_json::Value;

use weft::node::NodeOutput;
use weft::storage::media::{ExternalizePolicy, MediaForm};
use weft::storage::{FileHandle, StorageScope};
use weft::{RunContext, NodeErrExt, WeftError, WeftResult};

use super::chat;

/// One assembled call: the generator (riding the opened connection's
/// client), the per-call parameters, the conversation in stored form
/// including this call's user message, and the package's `ChatHistory`
/// type (resolved up front so both storage round-trips read one
/// resolution). The connection itself is not held: its lease is the
/// runtime's, released when the firing's body finishes, and the
/// generator owns the signed client.
pub struct LlmCall {
    pub generator: GeneratorInfo,
    pub params: NodeCompletionParameters,
    pub stored: Vec<Value>,
    /// The conversation file (`historyFile`) this call's turn is added
    /// to, in place; `None` when no conversation came in.
    history_file: Option<FileHandle>,
    /// The system prompt the params configured: a conversation that
    /// does not open with a system message gets it first, both in what
    /// is sent and in the file the turn is written to.
    system_prompt: String,
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
    /// Reasoning was set off: a provider refusing the call is then most
    /// likely refusing to switch reasoning off, and the error says what
    /// to change.
    reasoning_off: bool,
}

impl LlmCall {
    /// Whether `response` is a whole reply ([`ensure_reply_substance`]):
    /// what [`finish`] checks first, and what a streaming call checks
    /// before telling its readers the stream finished.
    pub fn ensure_whole(&self, response: &minillmlib::CompletionResponse) -> WeftResult<()> {
        ensure_reply_substance(response, self.announces_finish)
    }

    /// The error a failed call surfaces. With reasoning set off, a provider
    /// REFUSING the request (a client error: 4xx other than 408 and 429,
    /// which are transient) whose message is about reasoning is told in
    /// the author's terms: the model always reasons, so the switch has to
    /// go on. That is a settings mistake (`Config`), which fails the run
    /// even with `_error` wired: retrying cannot help, the params must
    /// change. Every other failure (a timeout, an overload, a 5xx whose
    /// text happens to say "reason") stays a catchable node failure.
    pub fn call_error(&self, err: MiniLLMError) -> WeftError {
        if self.reasoning_off && refuses_reasoning_off(&err) {
            return WeftError::Config(format!(
                "llm: {err}. This model always reasons; set `reasoning: true` on its params \
                 (the provider's lowest effort unless you pick one)"
            ));
        }
        weft::node_error(format!("llm: {err}"))
    }
}

/// Whether `err` is the provider refusing the request over reasoning:
/// an API error carrying a client status whose provider message names
/// reasoning. The raw stream every node starts is one request (the
/// lib's retry loop is not on that path), so the error is the
/// provider's own answer.
fn refuses_reasoning_off(err: &MiniLLMError) -> bool {
    match err {
        MiniLLMError::Api { status, message } => {
            (400..500).contains(status)
                && *status != 408
                && *status != 429
                && message.to_ascii_lowercase().contains("reasoning")
        }
        _ => false,
    }
}

/// Read the node's shared inputs (`provider`, `params`, `historyFile`,
/// `prompt`, `media`, `tools`, `toolChoice`) into one ready-to-send
/// call. Fails loudly on a provider object no provider node built.
pub async fn assemble(ctx: &RunContext) -> WeftResult<LlmCall> {
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
        other => {
            return Err(WeftError::Input(format!("unknown LLM provider kind '{other}'")))
        }
    };

    // The lib's constructors read an ambient env key when one is set;
    // here ALL auth is the connection's, so the generator's own auth is
    // cleared explicitly rather than relied on to be overridden.
    generator = generator.with_auth(minillmlib::Auth::None);

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
        None => {
            return Err(WeftError::Input(
                "the wired LlmProvider object carries no connection; wire the service's Access node into its Model node"
                    .to_string(),
            ))
        }
    };

    // The `params` input carries ONE plain object (the wired LlmParams
    // node's output); absent = every setting unset. The lib takes the
    // flat camelCase fields directly, skips keys it doesn't know, and
    // sends only what is set, so an unwritten setting is the provider's
    // own default (a wire that requires `maxTokens`, the native
    // Anthropic one, fills the model's largest output itself).
    // `reasoning` is the params node's own shape (an optional bool plus
    // `reasoningEffort`), so it is removed before the lib sees the
    // object.
    let params = ctx.inputs.nested("params")?;
    let reasoning: Option<bool> = params.opt("reasoning")?;
    let mut fields = params.object()?.clone();
    fields.remove("reasoning");
    let mut cp: CompletionParameters =
        serde_json::from_value(Value::Object(fields))
            .map_err(|e| WeftError::Input(format!("completion parameters: {e}")))?;
    let effort: Option<String> = params.opt("reasoningEffort")?;
    if let Some(reasoning) = reasoning_config(reasoning, effort.as_deref()) {
        cp = cp.with_reasoning(reasoning);
    }
    let reasoning_off = reasoning == Some(false);

    // OpenRouter routing rides the provider object (it is a fact of
    // WHERE the call is served, not of how the model samples); the lib
    // attaches it under the request's `provider` key and other wires
    // ignore it.
    let mut routing = OpenRouterRouting::new();
    if let Some(pin) = provider.opt::<String>("servingProvider")? {
        routing = routing.with_order(vec![pin]);
    }
    if let Some(fallbacks) = provider.opt::<bool>("providerFallbacks")? {
        routing = routing.with_fallbacks(fallbacks);
    }
    if routing.order.is_some() || routing.allow_fallbacks.is_some() {
        cp = cp.with_openrouter_routing(routing);
    }

    // Tools: each item is an `LlmTool` record (`{ name, description?,
    // parameters }`), which is the lib's `ToolDefinition` field for
    // field, so it decodes straight into one.
    // SYNC: types.LlmTool (catalog/ai/llm/metadata.json) <->
    //       MiniLLMLibRS/src/tools/mod.rs ToolDefinition
    let tools: Vec<ToolDefinition> = ctx.inputs.opt("tools")?.unwrap_or_default();
    let has_tools = !tools.is_empty();
    for tool in tools {
        cp = cp.with_tool(tool);
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

    // Resolved here, before the paid call, so a conversation file is
    // checked against it before anything is sent.
    let history_ty = chat::history_type()?;

    // The conversation in STORED form: the one the history file holds
    // (none without one), put in shape for the provider, plus this
    // call's user message (prompt + media, media slots holding the
    // stored-file values verbatim). No prompt (a tool-loop re-entry
    // after appending tool results) appends nothing.
    let (mut stored, history_file) = chat::incoming_history(ctx, &history_ty).await?;
    let system_prompt: String = params.get_or("systemPrompt", String::new())?;
    prepare_conversation(&mut stored, &system_prompt)?;
    let prompt: Option<String> = ctx.inputs.opt("prompt")?;
    let media = chat::media_input(ctx)?;
    let mut new_turn = 0;
    if prompt.is_some() || !media.is_empty() {
        stored.push(chat::stored_message("user", prompt.as_deref().unwrap_or(""), &media, None)?);
        new_turn = 1;
    }
    if stored.is_empty() {
        return Err(WeftError::Input(
            "nothing to send: no prompt, no media, and no conversation on historyFile".to_string(),
        ));
    }
    Ok(LlmCall {
        generator,
        params: ncp,
        stored,
        history_file,
        system_prompt,
        auto_cache: ctx.inputs.get_or("autoCache", true)?,
        new_turn,
        history_ty,
        announces_finish: kind != "custom",
        reasoning_off,
    })
}

/// Put a conversation in the shape this call sends and keeps, the same
/// way whether it is the conversation just read or the file's content
/// when the turn is written (another turn may have landed in between).
/// The system prompt SEEDS a conversation that does not
/// open with a system message (the lib treats a role-system message in
/// the list as first-class); one already opening with a system message
/// is the truth and the params never re-insert, since silently
/// overriding it would make what is sent disagree with what is kept.
/// Switching instructions mid-conversation is an explicit act (append a
/// system message via ChatHistoryAppend).
fn prepare_conversation(conversation: &mut Vec<Value>, system_prompt: &str) -> WeftResult<()> {
    let opens_with_system =
        conversation.first().and_then(|m| m.get("role")).and_then(|r| r.as_str()) == Some("system");
    if !opens_with_system && !system_prompt.is_empty() {
        conversation.insert(0, chat::stored_message("system", system_prompt, &[], None)?);
    }
    Ok(())
}

/// The WIRE form of the stored conversation: media slots become
/// material the provider consumes (images/video as fresh presigned
/// URLs, audio inline: the audio wire takes no URLs), and the result
/// deserializes straight into minillmlib messages because the shapes
/// are identical apart from the media slots. Returns the conversation
/// leaf the completion runs on.
pub async fn to_wire(ctx: &RunContext, llm: &LlmCall) -> WeftResult<ChatNode> {
    let mut request = llm.stored.clone();
    // A tool call left without its answer (a run stopped between the
    // call and its result, or another run sharing the file still mid
    // tool call) is answered as cancelled in what is SENT only, so the
    // provider takes the conversation; the file never holds a synthetic
    // answer, which would give that call two once its real result lands.
    chat::answer_unanswered_tool_calls(&mut request);
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

/// The buffered call: assemble it, send it, and hand back the call with
/// the model's whole reply. It streams internally so a Stop lands
/// mid-generation instead of after it; on cancel, dropping the stream
/// is all the wrap-up there is (the metered client resolves the
/// interrupted call's real cost on its own).
pub async fn complete(ctx: &RunContext) -> WeftResult<(LlmCall, CompletionResponse)> {
    let llm = assemble(ctx).await?;
    let leaf = to_wire(ctx, &llm).await?;
    let stream = leaf
        .complete_streaming(&llm.generator, Some(&llm.params))
        .await
        .map_err(|e| llm.call_error(e))?;
    let cancelled = ctx.cancellation();
    let response = tokio::select! {
        collected = stream.collect() => collected.map_err(|e| llm.call_error(e))?,
        err = cancelled.cancelled_err() => return Err(err),
    };
    Ok((llm, response))
}

/// Everything the nodes do once the reply is in hand, in one place:
/// reject a reply with no substance or no finish reason, ask the node
/// for its `response` value (`answer`: `None` leaves the port unset),
/// append the assistant message and internalize the conversation back
/// into stored form (any raw reply media becomes storage references;
/// everything already stored passes through untouched), and set the
/// output ports (`response`, the conversation, and `toolCalls` only
/// when the model actually called tools, so a downstream tool branch
/// stays structurally dead on a plain reply).
///
/// `answer` runs BEFORE anything is written: a node that refuses the
/// reply (an answer that is not the JSON the program declared) fails
/// there, and the conversation file stays exactly as it was, so a
/// retry sends the model the same conversation again.
///
/// The conversation is kept in a file, never on a wire: this call's
/// turn (its user message and the reply) is added to the end of the
/// `historyFile` that came in, in place, and that same file goes out on
/// `historyFile`, so a conversation grows turn after turn whatever its
/// size. With no file in, a wired `historyFile` output starts a new
/// conversation file holding this turn, kept past the run; unwired, the call
/// is a one-shot and keeps nothing.
pub async fn finish(
    ctx: &RunContext,
    llm: LlmCall,
    response: &CompletionResponse,
    answer: impl FnOnce(&CompletionResponse) -> WeftResult<Option<Value>>,
) -> WeftResult<NodeOutput> {
    llm.ensure_whole(response)?;
    let mut output = NodeOutput::new();
    if let Some(value) = answer(response)? {
        output = output.set("response", value);
    }
    let assistant = serde_json::to_value(response.to_assistant_message())
        .node_err("serializing the assistant message")?;
    if llm.history_file.is_some() || ctx.is_output_wired("historyFile") {
        let mut turn = llm.stored[llm.stored.len() - llm.new_turn..].to_vec();
        turn.push(assistant);
        // Raw media in the reply becomes storage references; what is
        // already stored passes through untouched.
        let turn = ctx
            .storage(StorageScope::Project)
            .internalize(&Value::Array(turn), &llm.history_ty, None)
            .await?;
        let turn = turn.as_array().cloned().node_err("the internalized turn is not a list")?;
        let system_prompt = llm.system_prompt.clone();
        let file = chat::append_turn(ctx, llm.history_file.as_ref(), &llm.history_ty, &turn, |conversation| {
            prepare_conversation(conversation, &system_prompt)
        })
        .await?;
        output = output.set("historyFile", file);
    }
    if let Some(calls) = &response.tool_calls {
        if !calls.is_empty() {
            output = output.set_serialized("toolCalls", calls)?;
        }
    }
    Ok(output)
}

/// The `response` of a node that hands the reply out as text (the
/// [`finish`] answer of LlmInference and LlmStream): the reply's text,
/// empty on a turn that only called tools.
pub fn reply_text(reply: &CompletionResponse) -> WeftResult<Option<Value>> {
    Ok(Some(Value::String(reply.content.clone())))
}

/// What the `reasoning` setting sends, in its three states. Unset (the
/// default) sends nothing, so the provider's own default applies, as for
/// every other setting. Off asks for no reasoning, which every wire
/// translates into its own "off", so a model that is off stays off and
/// one that cannot switch off refuses the request loudly; that refusal is
/// the answer (see [`LlmCall::call_error`]). On sends the chosen effort as
/// written, or the provider's lowest effort when none was picked: on IS
/// the intent to reason, and the cheapest effort is the one you get
/// without asking for more.
pub fn reasoning_config(reasoning: Option<bool>, effort: Option<&str>) -> Option<Reasoning> {
    match reasoning? {
        false => Some(Reasoning::Off),
        true => Some(Reasoning::On(effort.map(str::to_string))),
    }
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
    response: &CompletionResponse,
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
