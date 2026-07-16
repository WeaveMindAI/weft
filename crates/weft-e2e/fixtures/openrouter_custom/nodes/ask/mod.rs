//! A project that defines its OWN provider, exercised end to end.
//!
//! This proves the project-defined provider path: a provider weft does not
//! ship (`openrouter_custom`), whose meter lives in the project itself, is
//! discovered by the worker and prices a real call. The node opens access to
//! `openrouter_custom` and calls on the metered client exactly like any paid
//! node; the only difference from the built-in OpenRouter node is the provider
//! name. This is the bare-node meter shape: the meter is registered at the
//! bottom of the node's own `mod.rs` (a bare node has no package root to hold
//! a shared file).
//!
//! The meter is a thin wrapper over weft's real OpenRouter meter: it keeps the
//! real pricing logic and the real base URL (so the call reaches OpenRouter and
//! is priced correctly) and only renames the provider to `openrouter_custom`.
//! That keeps the fixture honest (a real metered call) without copying 800
//! lines of pricing code.

use async_trait::async_trait;

use serde_json::Value;

use weft_core::error::WeftError;
use weft_core::node::NodeOutput;
use weft_core::{ExecutionContext, Node, NodeManifest, WeftResult};
use weft_providers::providers::openrouter::OPENROUTER;
use weft_providers::{CallObservation, FollowUp, MeasuredCost, ObservedCall, ProviderMeter, RouteClass};

use minillmlib::{ChatNode, GeneratorInfo, NodeCompletionParameters};

#[derive(NodeManifest)]
pub struct AskCustomNode;

#[async_trait]
impl Node for AskCustomNode {
    async fn execute(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let cfg = ctx.effective_config();
        let prompt: String = ctx.input.get("prompt")?;
        let system_prompt: String = cfg.get_optional("systemPrompt")?.unwrap_or_default();
        let model = cfg
            .get_optional::<String>("model")?
            .unwrap_or_else(|| "openai/gpt-4.1-nano".to_string());

        // The whole paid-call surface, against the PROJECT-DEFINED provider:
        // open access, build the generator over the metered client. The
        // runtime finds this project's own `openrouter_custom` meter and
        // measures the call's real cost behind the client.
        let access = ctx
            .provider_access("openrouter_custom", cfg.get_optional("apiKey")?)
            .await?;
        let generator = GeneratorInfo::openrouter(model)
            .with_api_key(access.credential())
            .with_app_attribution("https://weavemind.ai", "Weft")
            .with_http_client(ctx.metered_client(&access)?);

        let root = ChatNode::root(system_prompt);
        let user = root.add_user(prompt);
        let params = NodeCompletionParameters::new();

        // Stream so a Stop lands mid-generation; on cancel, dropping the stream
        // is all the wrap-up there is (the metered client resolves the
        // interrupted call's cost on its own).
        let stream = user
            .complete_streaming(&generator, Some(&params))
            .await
            .map_err(|e| WeftError::NodeExecution(format!("openrouter_custom: {e}")))?;
        let cancelled = ctx.cancellation();
        let response = tokio::select! {
            collected = stream.collect() => collected
                .map_err(|e| WeftError::NodeExecution(format!("openrouter_custom: {e}")))?,
            _ = cancelled.cancelled() => return Err(WeftError::Cancelled),
        };

        ctx.pulse_downstream(NodeOutput::with("response", Value::String(response.content))).await
    }
}

/// The project's own provider meter. It IS OpenRouter under the hood (real
/// pricing, real base URL), renamed so the runtime sees it as a distinct,
/// project-defined provider `openrouter_custom`. Every method delegates to the
/// shipped meter except `provider()`.
struct OpenRouterCustomMeter;

static OPENROUTER_CUSTOM: OpenRouterCustomMeter = OpenRouterCustomMeter;

#[async_trait]
impl ProviderMeter for OpenRouterCustomMeter {
    fn provider(&self) -> &'static str {
        "openrouter_custom"
    }
    fn base_url(&self) -> &'static str {
        OPENROUTER.base_url()
    }
    fn classify(&self, method: &str, path: &str) -> RouteClass {
        OPENROUTER.classify(method, path)
    }
    fn prepare(&self, path: &str, body: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        OPENROUTER.prepare(path, body)
    }
    async fn ceiling_usd(&self, path: &str, body: &[u8], http: &reqwest::Client) -> anyhow::Result<f64> {
        OPENROUTER.ceiling_usd(path, body, http).await
    }
    fn observe(&self) -> Box<dyn CallObservation> {
        OPENROUTER.observe()
    }
    async fn resolve(&self, observed: ObservedCall, follow_up: FollowUp<'_>) -> MeasuredCost {
        OPENROUTER.resolve(observed, follow_up).await
    }
}

weft_providers::register_meter!(OPENROUTER_CUSTOM);
