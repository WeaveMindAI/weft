use async_trait::async_trait;
use axum::{routing::{get, post}, Router, Json, http::StatusCode, extract::DefaultBodyLimit};
use serde::{Serialize, Deserialize};
use tower_http::cors::CorsLayer;
use weft_core::{NodeInstance, NodeInstanceStatus, NodeExecuteRequest, NodeExecutionStatus, NodeCallbackRequest, WaitingMetadata, FormSchema};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeResult {
    pub status: NodeExecutionStatus,
    pub output: Option<serde_json::Value>,
    pub waitingMetadata: Option<WaitingMetadata>,
    /// Accumulated cost in USD. Set automatically by the runner from the ExecutionContext accumulator.
    pub costUsd: f64,
}

impl NodeResult {
    pub fn completed(output: serde_json::Value) -> Self {
        Self {
            status: NodeExecutionStatus::Completed,
            output: Some(output),
            waitingMetadata: None,
            costUsd: 0.0,
        }
    }

    pub fn waiting(
        callback_id: String,
        title: Option<String>,
        description: Option<String>,
        data: Option<serde_json::Value>,
        form_schema: Option<FormSchema>,
    ) -> Self {
        Self {
            status: NodeExecutionStatus::WaitingForInput,
            output: data,
            waitingMetadata: Some(WaitingMetadata {
                callbackId: callback_id,
                title,
                description,
                formSchema: form_schema,
                metadata: serde_json::Value::Object(serde_json::Map::new()),
            }),
            costUsd: 0.0,
        }
    }

    pub fn failed(error: &str) -> Self {
        Self {
            status: NodeExecutionStatus::Failed,
            output: Some(serde_json::json!({ "error": error })),
            waitingMetadata: None,
            costUsd: 0.0,
        }
    }

}

#[derive(Clone)]
pub struct NodeServiceConfig {
    pub nodeId: String,
    pub endpoint: String,
    pub port: String,
    pub nodeTypes: Vec<String>,
    pub orchestratorUrl: String,
}

impl NodeServiceConfig {
    pub fn from_env(
        node_id_env: &str,
        port_env: &str,
        default_node_id_prefix: &str,
        default_port: &str,
        node_types: Vec<String>,
    ) -> Self {
        let node_id = std::env::var(node_id_env)
            .unwrap_or_else(|_| format!("{}-{}", default_node_id_prefix, uuid::Uuid::new_v4()));
        let port = std::env::var(port_env).unwrap_or_else(|_| default_port.to_string());
        // Restate ingress is at 8080, not the service endpoint at 9080
        let orchestrator_url = std::env::var("ORCHESTRATOR_URL")
            .unwrap_or_else(|_| "http://localhost:8080".to_string());
        // NODE_ENDPOINT overrides the default localhost endpoint (for k8s where we need the service DNS name)
        let endpoint = std::env::var("NODE_ENDPOINT")
            .unwrap_or_else(|_| format!("http://localhost:{}", port));

        Self {
            nodeId: node_id,
            endpoint,
            port,
            nodeTypes: node_types,
            orchestratorUrl: orchestrator_url,
        }
    }

    pub fn addr(&self) -> String {
        format!("0.0.0.0:{}", self.port)
    }
}

#[async_trait]
pub trait NodeService: Clone + Send + Sync + 'static {
    fn config(&self) -> &NodeServiceConfig;
    fn client(&self) -> &reqwest::Client;
    
    async fn execute(&self, req: NodeExecuteRequest) -> NodeResult;

    async fn run(self) -> anyhow::Result<()> {
        let config = self.config().clone();
        let addr = config.addr();
        
        tracing::info!("Starting {} Node Service: {}", config.nodeTypes.join("/"), config.nodeId);
        tracing::info!("Listening on {}", addr);

        // Build router and start listening *before* registering with the orchestrator.
        // Otherwise the orchestrator may retry dispatch immediately (after node_available)
        // and see the node as unreachable due to connection refused.
        let app = self.clone().build_router();
        let listener = tokio::net::TcpListener::bind(&addr).await?;

        let server_handle = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!("Node HTTP server exited: {}", e);
            }
        });

        self.register_with_orchestrator().await;
        self.start_heartbeat_task();

        // Keep running until the server task exits.
        let _ = server_handle.await;
        Ok(())
    }

    fn build_router(self) -> Router {
        Router::new()
            .route("/health", get(Self::health_handler))
            .route("/execute", post(Self::execute_handler))
            .with_state(self)
            .layer(CorsLayer::permissive())
            // Increase body size limit to 15MB (default is 2MB) to handle audio files
            .layer(DefaultBodyLimit::max(15 * 1024 * 1024))
    }

    async fn health_handler(
        axum::extract::State(service): axum::extract::State<Self>,
    ) -> Json<HealthResponse> {
        let config = service.config();
        Json(HealthResponse {
            status: "ok".to_string(),
            nodeId: config.nodeId.clone(),
            nodeTypes: config.nodeTypes.clone(),
        })
    }

    async fn execute_handler(
        axum::extract::State(service): axum::extract::State<Self>,
        Json(req): Json<NodeExecuteRequest>,
    ) -> (StatusCode, Json<NodeCallbackRequest>) {
        tracing::debug!("Executing node: {} (type: {}) for execution {} pulseId='{}'",
            req.nodeId, req.nodeType, req.executionId, req.pulseId);

        let execution_id = req.executionId.clone();
        let node_id = req.nodeId.clone();
        let pulse_id = req.pulseId.clone();

        // Execute synchronously: the orchestrator keeps the connection open
        // and gets the result directly. If we crash, the connection breaks
        // and the orchestrator's retry logic handles it.
        let result = service.execute(req).await;

        let error_msg = if result.status == NodeExecutionStatus::Failed {
            result.output.as_ref()
                .and_then(|o| o.get("error"))
                .and_then(|e| e.as_str())
                .map(|s| s.to_string())
        } else {
            None
        };

        let processed_output = result.output;
        let cost_usd = result.costUsd;

        let response = NodeCallbackRequest {
            executionId: execution_id,
            nodeId: node_id,
            status: result.status,
            output: processed_output,
            error: error_msg,
            waitingMetadata: result.waitingMetadata,
            pulseId: pulse_id,
            costUsd: cost_usd,
        };

        (StatusCode::OK, Json(response))
    }

    async fn register_with_orchestrator(&self) {
        let config = self.config();
        let client = self.client();
        
        let instance = NodeInstance {
            id: config.nodeId.clone(),
            endpoint: config.endpoint.clone(),
            nodeTypes: config.nodeTypes.clone(),
            status: NodeInstanceStatus::Online,
            registeredAt: chrono::Utc::now().to_rfc3339(),
            lastHeartbeat: chrono::Utc::now().to_rfc3339(),
        };

        let url = format!("{}/NodeInstanceRegistry/global/register", config.orchestratorUrl);

        // Retry for up to 1 hour with exponential backoff (capped at 30s)
        let max_attempts = 360; // ~1 hour with average 10s between attempts
        let mut delay_secs = 2u64;

        for attempt in 1..=max_attempts {
            match client.post(&url).json(&instance).send().await {
                Ok(response) if response.status().is_success() => {
                    tracing::info!("Registered with orchestrator successfully");
                    
                    return;
                }
                Ok(response) => {
                    tracing::warn!("Failed to register (attempt {}/{}): {}", attempt, max_attempts, response.status());
                }
                Err(e) => {
                    tracing::warn!("Failed to connect to orchestrator (attempt {}/{}): {}", attempt, max_attempts, e);
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
            delay_secs = (delay_secs * 2).min(30); // Exponential backoff, max 30s
        }
        tracing::error!("Failed to register with orchestrator after {} attempts (~1 hour)", max_attempts);
    }

    fn start_heartbeat_task(&self) {
        let config = self.config().clone();
        let client = self.client().clone();
        
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                let url = format!("{}/NodeInstanceRegistry/global/heartbeat", config.orchestratorUrl);
                let _ = client.post(&url).json(&config.nodeId).send().await;
            }
        });
    }
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub nodeId: String,
    pub nodeTypes: Vec<String>,
}


