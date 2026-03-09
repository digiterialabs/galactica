use std::convert::Infallible;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::{StreamExt, stream};
use galactica_common::inference::{
    ChatTurn, InferenceChunk, InferenceParameters, InferenceRequest, InferenceResponse,
    InferenceUsage,
};
use galactica_common::proto::{common, control};
use galactica_common::{GalacticaError, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[async_trait]
pub trait ControlPlaneApi: Send + Sync {
    async fn list_models(
        &self,
        filter_runtime: Option<&str>,
    ) -> Result<Vec<common::v1::ModelManifest>>;
    async fn infer(&self, request: InferenceRequest) -> Result<Vec<InferenceChunk>>;
}

#[derive(Clone)]
pub struct GrpcControlPlaneClient {
    endpoint: String,
}

impl GrpcControlPlaneClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }
}

#[async_trait]
impl ControlPlaneApi for GrpcControlPlaneClient {
    async fn list_models(
        &self,
        filter_runtime: Option<&str>,
    ) -> Result<Vec<common::v1::ModelManifest>> {
        let mut client =
            control::v1::control_plane_client::ControlPlaneClient::connect(self.endpoint.clone())
                .await
                .map_err(|error| {
                    GalacticaError::unavailable(format!(
                        "failed to connect to control plane: {error}"
                    ))
                })?;
        client
            .list_available_models(control::v1::ListAvailableModelsRequest {
                filter_runtime: filter_runtime.unwrap_or_default().to_string(),
            })
            .await
            .map(|response| response.into_inner().models)
            .map_err(|error| {
                GalacticaError::unavailable(format!("list_available_models failed: {error}"))
            })
    }

    async fn infer(&self, request: InferenceRequest) -> Result<Vec<InferenceChunk>> {
        let mut client =
            control::v1::control_plane_client::ControlPlaneClient::connect(self.endpoint.clone())
                .await
                .map_err(|error| {
                    GalacticaError::unavailable(format!(
                        "failed to connect to control plane: {error}"
                    ))
                })?;
        let response = client
            .infer(control::v1::InferRequest::from(request))
            .await
            .map_err(|error| GalacticaError::unavailable(format!("infer failed: {error}")))?;
        let mut stream = response.into_inner();
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next().await {
            chunks.push(chunk.map(InferenceChunk::from).map_err(|error| {
                GalacticaError::unavailable(format!("inference stream failed: {error}"))
            })?);
        }
        Ok(chunks)
    }
}

#[derive(Clone)]
pub struct GatewayState {
    client: Arc<dyn ControlPlaneApi>,
}

pub fn build_router(client: Arc<dyn ControlPlaneApi>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .with_state(GatewayState { client })
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn list_models(State(state): State<GatewayState>) -> Response {
    match state.client.list_models(None).await {
        Ok(models) => Json(ModelListResponse::from(models)).into_response(),
        Err(error) => error_response(error),
    }
}

async fn chat_completions(
    State(state): State<GatewayState>,
    Json(request): Json<ChatCompletionRequest>,
) -> Response {
    let request_id = format!("chatcmpl-{}", uuid::Uuid::new_v4());
    let inference = request.to_inference_request(request_id.clone());
    match state.client.infer(inference).await {
        Ok(chunks) => {
            if request.stream.unwrap_or(false) {
                streaming_response(request_id, request.model.clone(), chunks).into_response()
            } else {
                Json(ChatCompletionResponse::from_chunks(
                    request_id,
                    &request.model,
                    chunks,
                ))
                .into_response()
            }
        }
        Err(error) => error_response(error),
    }
}

fn streaming_response(
    request_id: String,
    model: String,
    chunks: Vec<InferenceChunk>,
) -> Sse<impl futures::Stream<Item = std::result::Result<Event, Infallible>>> {
    let events = chunks
        .into_iter()
        .enumerate()
        .map(move |(index, chunk)| {
            let payload = StreamingChatCompletionChunk::from_chunk(
                request_id.clone(),
                model.clone(),
                index == 0,
                chunk,
            );
            Ok::<Event, Infallible>(Event::default().data(serde_json::to_string(&payload).unwrap()))
        })
        .chain(std::iter::once(Ok(Event::default().data("[DONE]"))));

    Sse::new(stream::iter(events))
}

fn error_response(error: GalacticaError) -> Response {
    let status = match error {
        GalacticaError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
        GalacticaError::NotFound(_) => StatusCode::NOT_FOUND,
        GalacticaError::FailedPrecondition(_) => StatusCode::UNPROCESSABLE_ENTITY,
        GalacticaError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        GalacticaError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let body = Json(json!({
        "error": {
            "message": error.to_string(),
            "type": "invalid_request_error"
        }
    }));
    (status, body).into_response()
}

#[derive(Debug, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<OpenAiMessage>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub max_tokens: Option<u32>,
    pub stop: Option<StopSequences>,
    pub stream: Option<bool>,
    pub frequency_penalty: Option<f32>,
    pub presence_penalty: Option<f32>,
}

impl ChatCompletionRequest {
    fn to_inference_request(&self, request_id: String) -> InferenceRequest {
        InferenceRequest {
            request_id,
            model: self.model.clone(),
            messages: self
                .messages
                .iter()
                .map(|message| ChatTurn {
                    role: message.role.clone(),
                    content: message.content.clone(),
                })
                .collect(),
            params: InferenceParameters {
                temperature: self.temperature.unwrap_or(0.7),
                top_p: self.top_p.unwrap_or(1.0),
                max_tokens: self.max_tokens.unwrap_or(256),
                stop: self.stop.clone().unwrap_or_default().into_vec(),
                frequency_penalty: self.frequency_penalty.unwrap_or(0.0),
                presence_penalty: self.presence_penalty.unwrap_or(0.0),
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenAiMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum StopSequences {
    Single(String),
    Multiple(Vec<String>),
}

impl Default for StopSequences {
    fn default() -> Self {
        Self::Multiple(Vec::new())
    }
}

impl StopSequences {
    fn into_vec(self) -> Vec<String> {
        match self {
            Self::Single(value) => vec![value],
            Self::Multiple(values) => values,
        }
    }
}

#[derive(Debug, Serialize)]
struct ModelListResponse {
    object: &'static str,
    data: Vec<ModelObject>,
}

impl From<Vec<common::v1::ModelManifest>> for ModelListResponse {
    fn from(value: Vec<common::v1::ModelManifest>) -> Self {
        Self {
            object: "list",
            data: value
                .into_iter()
                .map(|manifest| ModelObject {
                    id: manifest
                        .model_id
                        .as_ref()
                        .map(|model_id| model_id.value.clone())
                        .unwrap_or_default(),
                    object: "model",
                    created: unix_timestamp(),
                    owned_by: "galactica".to_string(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ModelObject {
    id: String,
    object: &'static str,
    created: u64,
    owned_by: String,
}

#[derive(Debug, Serialize)]
struct ChatCompletionResponse {
    id: String,
    object: &'static str,
    created: u64,
    model: String,
    choices: Vec<ChatChoice>,
    usage: InferenceUsage,
}

impl ChatCompletionResponse {
    fn from_chunks(request_id: String, model: &str, chunks: Vec<InferenceChunk>) -> Self {
        let content = chunks
            .iter()
            .map(|chunk| chunk.delta_content.as_str())
            .collect::<String>();
        let finish_reason = chunks
            .last()
            .and_then(|chunk| chunk.finish_reason.clone())
            .unwrap_or_else(|| "stop".to_string());
        let usage = chunks
            .last()
            .and_then(|chunk| chunk.usage.clone())
            .unwrap_or(InferenceUsage {
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
            });
        let response = InferenceResponse {
            request_id: request_id.clone(),
            model: model.to_string(),
            content,
            finish_reason,
            usage: usage.clone(),
        };

        Self {
            id: request_id,
            object: "chat.completion",
            created: unix_timestamp(),
            model: model.to_string(),
            choices: vec![ChatChoice {
                index: 0,
                message: OpenAiResponseMessage {
                    role: "assistant".to_string(),
                    content: response.content,
                },
                finish_reason: response.finish_reason,
            }],
            usage,
        }
    }
}

#[derive(Debug, Serialize)]
struct ChatChoice {
    index: u32,
    message: OpenAiResponseMessage,
    finish_reason: String,
}

#[derive(Debug, Serialize)]
struct OpenAiResponseMessage {
    role: String,
    content: String,
}

#[derive(Debug, Serialize)]
struct StreamingChatCompletionChunk {
    id: String,
    object: &'static str,
    created: u64,
    model: String,
    choices: Vec<StreamingChoice>,
}

impl StreamingChatCompletionChunk {
    fn from_chunk(request_id: String, model: String, first: bool, chunk: InferenceChunk) -> Self {
        Self {
            id: request_id,
            object: "chat.completion.chunk",
            created: unix_timestamp(),
            model,
            choices: vec![StreamingChoice {
                index: chunk.choice_index,
                delta: StreamingDelta {
                    role: first.then_some("assistant".to_string()),
                    content: (!chunk.delta_content.is_empty()).then_some(chunk.delta_content),
                },
                finish_reason: chunk.finish_reason,
            }],
        }
    }
}

#[derive(Debug, Serialize)]
struct StreamingChoice {
    index: u32,
    delta: StreamingDelta,
    finish_reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct StreamingDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::body::to_bytes;
    use axum::http::{Method, Request};
    use galactica_artifact::LocalModelRegistry;
    use galactica_common::proto::control::v1::control_plane_server::ControlPlaneServer;
    use galactica_common::proto::node::v1::node_agent_server::NodeAgentServer;
    use galactica_control_plane::{
        ControlPlaneService, GrpcNodeExecutor, InMemoryStateStore, NodeRegistry,
    };
    use galactica_node_agent::{
        MlxBackend, NodeAgentService, default_macos_capabilities, default_system_memory,
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use tower::ServiceExt;

    use super::*;

    #[derive(Clone)]
    struct MockControlPlaneClient;

    #[async_trait]
    impl ControlPlaneApi for MockControlPlaneClient {
        async fn list_models(
            &self,
            _filter_runtime: Option<&str>,
        ) -> Result<Vec<common::v1::ModelManifest>> {
            Ok(vec![common::v1::ModelManifest {
                model_id: Some(common::v1::ModelId {
                    value: "mistral-small".to_string(),
                }),
                name: "Mistral Small".to_string(),
                family: "chat".to_string(),
                variants: Vec::new(),
                chat_template: "chatml".to_string(),
                metadata: std::collections::HashMap::new(),
            }])
        }

        async fn infer(&self, request: InferenceRequest) -> Result<Vec<InferenceChunk>> {
            Ok(vec![
                InferenceChunk {
                    request_id: request.request_id.clone(),
                    model: request.model.clone(),
                    choice_index: 0,
                    delta_content: "hello ".to_string(),
                    finish_reason: None,
                    usage: None,
                },
                InferenceChunk {
                    request_id: request.request_id,
                    model: request.model,
                    choice_index: 0,
                    delta_content: "world".to_string(),
                    finish_reason: Some("stop".to_string()),
                    usage: Some(InferenceUsage {
                        prompt_tokens: 2,
                        completion_tokens: 2,
                        total_tokens: 4,
                    }),
                },
            ])
        }
    }

    #[tokio::test]
    async fn renders_non_streaming_chat_completion() {
        let app = build_router(Arc::new(MockControlPlaneClient));
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        r#"{"model":"mistral-small","messages":[{"role":"user","content":"hello"}]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("\"object\":\"chat.completion\""));
        assert!(body.contains("hello world"));
    }

    #[tokio::test]
    async fn renders_streaming_chat_completion_as_sse() {
        let app = build_router(Arc::new(MockControlPlaneClient));
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        r#"{"model":"mistral-small","messages":[{"role":"user","content":"hello"}],"stream":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response.headers().get("content-type").unwrap();
        assert!(content_type.to_str().unwrap().contains("text/event-stream"));
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("chat.completion.chunk"));
        assert!(body.contains("[DONE]"));
    }

    #[tokio::test]
    async fn grpc_gateway_talks_to_control_plane() {
        let root = std::env::temp_dir().join(format!("galactica-gateway-{}", uuid::Uuid::new_v4()));
        let manifest_dir = root.join("mistral-small");
        std::fs::create_dir_all(&manifest_dir).unwrap();
        std::fs::write(
            manifest_dir.join("manifest.json"),
            r#"{
  "model_id": "mistral-small",
  "name": "Mistral Small",
  "family": "chat",
  "variants": [
    {
      "runtime": "mlx",
      "quantization": "4bit",
      "format": "safetensors",
      "size_bytes": 1024,
      "compatible_accelerators": [2]
    }
  ],
  "chat_template": "chatml",
  "metadata": {}
}"#,
        )
        .unwrap();

        let node_backend = Arc::new(MlxBackend::new());
        let node_agent = NodeAgentService::new(
            node_backend,
            default_macos_capabilities(),
            default_system_memory(),
        );
        let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_addr: SocketAddr = node_listener.local_addr().unwrap();
        let node_incoming = TcpListenerStream::new(node_listener);
        tokio::spawn(async move {
            Server::builder()
                .add_service(NodeAgentServer::new(node_agent))
                .serve_with_incoming(node_incoming)
                .await
                .unwrap();
        });

        let store = Arc::new(InMemoryStateStore::default());
        let registry = NodeRegistry::new(store.clone());
        registry
            .register(
                "mac-mini".to_string(),
                format!("http://{node_addr}"),
                default_macos_capabilities(),
                "0.1.0".to_string(),
            )
            .await
            .unwrap();

        let control_plane = ControlPlaneService::new(
            store,
            Arc::new(LocalModelRegistry::new(&root)),
            Arc::new(GrpcNodeExecutor),
        );
        let control_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control_addr: SocketAddr = control_listener.local_addr().unwrap();
        let control_incoming = TcpListenerStream::new(control_listener);
        tokio::spawn(async move {
            Server::builder()
                .add_service(ControlPlaneServer::new(control_plane))
                .serve_with_incoming(control_incoming)
                .await
                .unwrap();
        });

        let app = build_router(Arc::new(GrpcControlPlaneClient::new(format!(
            "http://{control_addr}"
        ))));
        let models = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/models")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(models.status(), StatusCode::OK);

        let completion = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        r#"{"model":"mistral-small","messages":[{"role":"user","content":"summarize node health"}]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(completion.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("MLX synthesized response"));

        std::fs::remove_dir_all(root).unwrap();
    }
}
