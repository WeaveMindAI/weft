//! OpenRouterInference: one language-model call through OpenRouter.
//!
//! Settings come from the `config` input port (an upstream OpenRouterConfig
//! node) overlaid on this node's own config. With `parseJson`, the response is
//! JSON-repaired and its top-level keys fan onto matching declared output ports.
//!
//! The call streams internally and watches the cancellation flag
//! (`interruptGrace`): on a Stop mid-generation the stream is cancelled and the
//! call's ACTUAL cost still gets settled (resolved from OpenRouter's ledger;
//! the upstream generation bills in full whether we hang up or not).

use async_trait::async_trait;
use minillmlib::{
    estimate_cost_usd, AsyncCostCallback, ChatNode, CollectOutcome, CompletionContext,
    CompletionMeta, CompletionParameters, CostInfo, GeneratorInfo, NodeCompletionParameters,
    ProviderSettings, ReasoningConfig,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;

use weft_core::context::ConfigBag;
use weft_core::error::WeftError;
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct OpenRouterInferenceNode;

/// The process-shared `GeneratorInfo` for a model. Shared because minillmlib
/// caches the model's published rates on the generator (clones share the
/// cache), so the price sheet is fetched once per TTL rather than once per
/// execution. Keyless: the calling generator, which carries the user's key,
/// is built per call instead.
fn shared_generator(model: &str) -> GeneratorInfo {
    static POOL: OnceLock<Mutex<HashMap<String, GeneratorInfo>>> = OnceLock::new();
    let fresh = GeneratorInfo::openrouter(model);
    let mut pool = POOL.get_or_init(Mutex::default).lock().expect("generator pool lock");
    pool.entry(fresh.pricing_key()).or_insert(fresh).clone()
}

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

        // Routing decides who serves the call, therefore what it costs. The
        // SAME settings drive the request and the estimate, so they can't
        // disagree; only one ordered provider with fallbacks off pins a price.
        let mut routing = ProviderSettings::new();
        if let Some(provider) = cfg.get_optional::<String>("provider")? {
            routing = routing.with_order(vec![provider]);
        }
        if let Some(fallbacks) = cfg.get_optional::<bool>("providerFallbacks")? {
            routing = routing.with_fallbacks(fallbacks);
        }
        let billing_provider = routing.billing_provider();
        if routing.order.is_some() || routing.allow_fallbacks.is_some() {
            cp = cp.with_openrouter_routing(routing);
        }

        let root = ChatNode::root(system_prompt);
        let user = root.add_user(prompt);

        // Price the call BEFORE opening access: the price sheet is keyed on the
        // model, not the access (`shared_generator` is keyless), so if pricing
        // fails there is no open access to clean up (there is none yet).
        let rates = shared_generator(&model)
            .model_rates_served_by(billing_provider.as_deref())
            .await
            .map_err(|e| {
                WeftError::NodeExecution(format!("openrouter: cannot price model {model}: {e}"))
            })?;
        let estimate = estimate_cost_usd(&user.thread(), &cp, &rates);

        // A generation (with its cost resolution) fits the default window, so
        // it needs no declaration of its own.
        let access = ctx.provider_access("openrouter", cfg.get_optional("apiKey")?).await?;
        let generator = GeneratorInfo::openrouter(model.clone());
        let base_url = access.base_url(&generator.base_url);
        let generator =
            generator.with_base_url(base_url).with_api_key(access.credential().to_string());

        // Provision the estimated upper bound before spending anything:
        // refusable on the deployment's key, a no-op on the user's own. A
        // refusal comes back with the access still open and nothing spent, so
        // give it back before returning.
        let hold = match ctx
            .provision_cost(
                &access,
                weft_core::CostProvision::estimate(estimate).with_model(model.clone()),
            )
            .await
        {
            Ok(hold) => hold,
            Err(e) => {
                // Nothing was spent, so give the access back. The provisioning
                // error is what the user must see, so it stays the primary; a
                // close that ALSO fails is folded into the message rather than
                // discarded, so a broken close is never invisible.
                return Err(match ctx.close_access(access).await {
                    Ok(()) => e,
                    Err(close) => WeftError::NodeExecution(format!(
                        "{e} (additionally, giving the provider access back failed: {close})"
                    )),
                });
            }
        };

        // The cost arrives through the tracking callback on every end of the
        // stream: a finished one books from its usage, a cancelled one from
        // OpenRouter's ledger (`TrackedStream::cancel` resolves it before
        // returning), a transport-errored one books nothing.
        let (cost_tx, mut cost_rx) = tokio::sync::mpsc::unbounded_channel();
        let on_cost: AsyncCostCallback = Arc::new(move |info: CostInfo, _meta: CompletionMeta| {
            let cost_tx = cost_tx.clone();
            Box::pin(async move {
                let _ = cost_tx.send(info);
            })
        });
        let tracking = CompletionContext::new(
            generator,
            serde_json::json!({}),
            on_cost,
            "https://weavemind.ai",
            "Weft",
        );

        let params = NodeCompletionParameters::new().with_params(cp);
        let outcome = match user.complete_streaming_tracked(&tracking, Some(&params)).await {
            Ok(stream) => {
                let cancelled = ctx.cancellation();
                stream.collect_or_cancel(async move { cancelled.cancelled().await }).await
            }
            // The request never started: nothing was spent. Same shape as a
            // stream that died on the wire, so one settle + close covers both.
            Err(e) => CollectOutcome::Failed(e),
        };
        // Settle FIRST, then close. Settling records the actual cost (the call
        // may already have spent real money) and needs nothing from the access;
        // closing only retires the stand-in. Doing settle first means a close
        // hiccup can never drop a real charge on the floor. The callback has
        // already fired by now (or booked nothing, for a transport error or a
        // request that never started).
        let interrupted = matches!(outcome, CollectOutcome::Interrupted);
        let (actual_usd, meta) = match cost_rx.try_recv() {
            Ok(info) => (
                info.cost,
                serde_json::json!({
                    "promptTokens": info.prompt_tokens,
                    "completionTokens": info.completion_tokens,
                    "estimatedUsd": estimate,
                    "resolution": format!("{:?}", info.resolution),
                    "interrupted": interrupted,
                }),
            ),
            Err(_) => (
                0.0,
                serde_json::json!({
                    "resolution": "no cost info from provider",
                    "estimatedUsd": estimate,
                    "interrupted": interrupted,
                }),
            ),
        };
        // Settle, then ALWAYS give the access back, then surface whichever
        // failed (settle first: it is the one that records real money). Running
        // both before the `?` is what keeps the two symmetric: a failing settle
        // must not skip the close and leave the access alive to its window when
        // the worker is right here, able to retire it.
        let settled = ctx.settle_cost(hold, actual_usd, meta).await;
        let closed = ctx.close_access(access).await;
        settled?;
        closed?;

        let response = match outcome {
            CollectOutcome::Finished(response) => response,
            CollectOutcome::Interrupted => return Err(WeftError::Cancelled),
            CollectOutcome::Failed(e) => {
                return Err(WeftError::NodeExecution(format!("openrouter: {e}")))
            }
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

