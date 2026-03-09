use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode};
use axum::middleware::{self, Next};
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
use galactica_control_plane::{AuthContext, CredentialKind, inject_auth_metadata, pool_label};
use galactica_observability::{
    MetricsRegistry, inject_trace_context_into_tonic_request, set_parent_from_http_headers,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, RwLock};
use tower_http::cors::CorsLayer;
use tracing::{Instrument, info};

const REQUEST_ID_HEADER: &str = "x-request-id";
const SESSION_ID_HEADER: &str = "x-session-id";

#[async_trait]
pub trait ControlPlaneApi: Send + Sync {
    async fn list_models(
        &self,
        filter_runtime: Option<&str>,
        auth: &AuthContext,
    ) -> Result<Vec<common::v1::ModelManifest>>;
    async fn infer(
        &self,
        request: InferenceRequest,
        auth: &AuthContext,
    ) -> Result<Vec<InferenceChunk>>;
    async fn authenticate(&self, api_key: &str) -> Result<AuthContext>;
    async fn get_cluster_state(
        &self,
        auth: &AuthContext,
    ) -> Result<control::v1::GetClusterStateResponse>;
    async fn list_events(
        &self,
        since_sequence: u64,
        auth: &AuthContext,
    ) -> Result<control::v1::ListEventsResponse>;
    async fn list_audit_records(
        &self,
        since: Option<chrono::DateTime<chrono::Utc>>,
        limit: u32,
        auth: &AuthContext,
    ) -> Result<Vec<control::v1::AuditRecord>>;
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
        auth: &AuthContext,
    ) -> Result<Vec<common::v1::ModelManifest>> {
        let mut client =
            control::v1::control_plane_client::ControlPlaneClient::connect(self.endpoint.clone())
                .await
                .map_err(connect_error("control plane"))?;
        let mut request = tonic::Request::new(control::v1::ListAvailableModelsRequest {
            filter_runtime: filter_runtime.unwrap_or_default().to_string(),
        });
        inject_auth_metadata(&mut request, auth)?;
        inject_trace_context_into_tonic_request(&mut request);
        client
            .list_available_models(request)
            .await
            .map(|response| response.into_inner().models)
            .map_err(rpc_unavailable("list_available_models"))
    }

    async fn infer(
        &self,
        request: InferenceRequest,
        auth: &AuthContext,
    ) -> Result<Vec<InferenceChunk>> {
        let mut client =
            control::v1::control_plane_client::ControlPlaneClient::connect(self.endpoint.clone())
                .await
                .map_err(connect_error("control plane"))?;
        let mut request = tonic::Request::new(control::v1::InferRequest::from(request));
        inject_auth_metadata(&mut request, auth)?;
        inject_trace_context_into_tonic_request(&mut request);
        let response = client
            .infer(request)
            .await
            .map_err(rpc_unavailable("infer"))?;
        let mut stream = response.into_inner();
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next().await {
            chunks.push(
                chunk
                    .map(InferenceChunk::from)
                    .map_err(rpc_stream_unavailable("infer"))?,
            );
        }
        Ok(chunks)
    }

    async fn authenticate(&self, api_key: &str) -> Result<AuthContext> {
        let mut client =
            control::v1::control_plane_client::ControlPlaneClient::connect(self.endpoint.clone())
                .await
                .map_err(connect_error("control plane"))?;
        let mut request = tonic::Request::new(control::v1::AuthenticateRequest {
            credential: Some(control::v1::authenticate_request::Credential::ApiKey(
                api_key.to_string(),
            )),
        });
        inject_trace_context_into_tonic_request(&mut request);
        let response = client
            .authenticate(request)
            .await
            .map_err(|error| {
                GalacticaError::unauthorized(format!("authentication failed: {error}"))
            })?
            .into_inner();
        if !response.authenticated {
            return Err(GalacticaError::unauthorized("authentication rejected"));
        }
        let tenant_id = response
            .tenant_id
            .as_ref()
            .map(|tenant_id| tenant_id.value.clone())
            .ok_or_else(|| GalacticaError::unauthorized("control plane did not return a tenant"))?;
        let expires_at = response
            .expires_at
            .map(galactica_common::timestamp_to_chrono)
            .unwrap_or_else(|| chrono::Utc::now() + chrono::Duration::hours(1));
        let policy = response.policy.unwrap_or(control::v1::AuthPolicy {
            tenant_id: response.tenant_id.clone(),
            require_mtls: false,
            max_requests_per_minute: 0,
            allowed_models: Vec::new(),
            allowed_node_pools: Vec::new(),
        });
        Ok(AuthContext {
            tenant_id: tenant_id.clone(),
            scopes: response.scopes,
            expires_at,
            require_mtls: policy.require_mtls,
            max_requests_per_minute: policy.max_requests_per_minute,
            allowed_models: policy.allowed_models,
            allowed_node_pools: policy.allowed_node_pools,
            actor: if response.actor.is_empty() {
                format!("tenant:{tenant_id}")
            } else {
                response.actor
            },
            credential_kind: match response.credential_kind.as_str() {
                "certificate" => CredentialKind::CertificateFingerprint,
                _ => CredentialKind::ApiKey,
            },
        })
    }

    async fn get_cluster_state(
        &self,
        auth: &AuthContext,
    ) -> Result<control::v1::GetClusterStateResponse> {
        let mut client =
            control::v1::control_plane_client::ControlPlaneClient::connect(self.endpoint.clone())
                .await
                .map_err(connect_error("control plane"))?;
        let mut request = tonic::Request::new(control::v1::GetClusterStateRequest {});
        inject_auth_metadata(&mut request, auth)?;
        inject_trace_context_into_tonic_request(&mut request);
        client
            .get_cluster_state(request)
            .await
            .map(|response| response.into_inner())
            .map_err(rpc_unavailable("get_cluster_state"))
    }

    async fn list_events(
        &self,
        since_sequence: u64,
        auth: &AuthContext,
    ) -> Result<control::v1::ListEventsResponse> {
        let mut client =
            control::v1::control_plane_client::ControlPlaneClient::connect(self.endpoint.clone())
                .await
                .map_err(connect_error("control plane"))?;
        let mut request = tonic::Request::new(control::v1::ListEventsRequest { since_sequence });
        inject_auth_metadata(&mut request, auth)?;
        inject_trace_context_into_tonic_request(&mut request);
        client
            .list_events(request)
            .await
            .map(|response| response.into_inner())
            .map_err(rpc_unavailable("list_events"))
    }

    async fn list_audit_records(
        &self,
        since: Option<chrono::DateTime<chrono::Utc>>,
        limit: u32,
        auth: &AuthContext,
    ) -> Result<Vec<control::v1::AuditRecord>> {
        let mut client =
            control::v1::control_plane_client::ControlPlaneClient::connect(self.endpoint.clone())
                .await
                .map_err(connect_error("control plane"))?;
        let mut request = tonic::Request::new(control::v1::ListAuditRecordsRequest {
            since: since.map(galactica_common::chrono_to_timestamp),
            limit,
        });
        inject_auth_metadata(&mut request, auth)?;
        inject_trace_context_into_tonic_request(&mut request);
        client
            .list_audit_records(request)
            .await
            .map(|response| response.into_inner().records)
            .map_err(rpc_unavailable("list_audit_records"))
    }
}

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub global_requests_per_minute: u32,
    pub default_tenant_requests_per_minute: u32,
    pub session_history_limit: usize,
    pub audit_limit: u32,
    pub deployment_mode: String,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            global_requests_per_minute: 600,
            default_tenant_requests_per_minute: 120,
            session_history_limit: 64,
            audit_limit: 100,
            deployment_mode: "hybrid".to_string(),
        }
    }
}

#[derive(Clone)]
pub struct GatewayState {
    client: Arc<dyn ControlPlaneApi>,
    config: GatewayConfig,
    metrics: MetricsRegistry,
    rate_limiter: Arc<GatewayRateLimiter>,
    sessions: Arc<SessionStore>,
}

#[derive(Debug, Clone)]
struct RequestContext {
    request_id: String,
    started_at: Instant,
}

pub fn build_router(client: Arc<dyn ControlPlaneApi>) -> Router {
    build_router_with_config(client, GatewayConfig::default())
}

pub fn build_router_with_config(client: Arc<dyn ControlPlaneApi>, config: GatewayConfig) -> Router {
    let state = GatewayState {
        client,
        rate_limiter: Arc::new(GatewayRateLimiter::new(
            config.global_requests_per_minute,
            config.default_tenant_requests_per_minute,
        )),
        sessions: Arc::new(SessionStore::new(config.session_history_limit)),
        metrics: MetricsRegistry::new(),
        config,
    };

    let protected = middleware::from_fn_with_state(state.clone(), auth_middleware);

    let v1_routes = Router::new()
        .route("/models", get(list_models))
        .route("/chat/completions", post(chat_completions))
        .route_layer(protected.clone());

    let admin_routes = Router::new()
        .route("/cluster", get(cluster_state))
        .route("/models", get(admin_models))
        .route("/events", get(admin_events))
        .route("/audit", get(admin_audit))
        .route("/sessions", get(list_sessions))
        .route("/sessions/{session_id}", get(get_session))
        .route("/settings", get(admin_settings))
        .route_layer(protected);

    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .nest("/v1", v1_routes)
        .nest("/admin", admin_routes)
        .layer(CorsLayer::permissive())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            request_context_middleware,
        ))
        .with_state(state)
}

async fn request_context_middleware(
    State(state): State<GatewayState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let request_id = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    request.extensions_mut().insert(RequestContext {
        request_id: request_id.clone(),
        started_at: Instant::now(),
    });
    let span = tracing::info_span!(
        "gateway.request",
        otel.kind = "server",
        request_id = %request_id,
        method = %method,
        path = %path
    );
    set_parent_from_http_headers(&span, request.headers());

    async move {
        let mut response = next.run(request).await;
        insert_header(&mut response, REQUEST_ID_HEADER, &request_id);
        state
            .metrics
            .record_http_request(response.status().as_u16());
        info!(
            request_id,
            method = %method,
            path,
            status = response.status().as_u16(),
            "gateway request completed"
        );
        response
    }
    .instrument(span)
    .await
}

async fn auth_middleware(
    State(state): State<GatewayState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let auth = match authenticate_headers(&state, request.headers()).await {
        Ok(auth) => auth,
        Err(error) => return error_response(error),
    };

    if !state
        .rate_limiter
        .allow(&auth.tenant_id, auth.max_requests_per_minute, &state.config)
        .await
    {
        state.metrics.record_rate_limit_rejection();
        return rate_limit_response();
    }

    request.extensions_mut().insert(auth.clone());
    let mut response = next.run(request).await;
    insert_header(&mut response, "x-galactica-tenant-id", &auth.tenant_id);
    response
}

async fn health(State(state): State<GatewayState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "deployment_mode": state.config.deployment_mode,
    }))
}

async fn metrics(State(state): State<GatewayState>) -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        state.metrics.render(),
    )
        .into_response()
}

async fn list_models(
    State(state): State<GatewayState>,
    Extension(auth): Extension<AuthContext>,
) -> Response {
    match state.client.list_models(None, &auth).await {
        Ok(models) => Json(ModelListResponse::from(models)).into_response(),
        Err(error) => error_response(error),
    }
}

async fn admin_models(
    State(state): State<GatewayState>,
    Extension(auth): Extension<AuthContext>,
) -> Response {
    match state.client.list_models(None, &auth).await {
        Ok(models) => Json(json!({
            "models": models.into_iter().map(model_manifest_json).collect::<Vec<_>>(),
        }))
        .into_response(),
        Err(error) => error_response(error),
    }
}

async fn cluster_state(
    State(state): State<GatewayState>,
    Extension(auth): Extension<AuthContext>,
) -> Response {
    match state.client.get_cluster_state(&auth).await {
        Ok(cluster) => {
            let pool_breakdown = cluster
                .nodes
                .iter()
                .filter_map(|node| node.capabilities.as_ref().map(pool_label))
                .fold(BTreeMap::<String, usize>::new(), |mut pools, pool| {
                    *pools.entry(pool).or_default() += 1;
                    pools
                });
            state.metrics.set_cluster_snapshot(
                cluster.nodes.len() as i64,
                cluster.loaded_models.len() as i64,
            );
            Json(json!({
                "overview": {
                    "node_count": cluster.nodes.len(),
                    "loaded_model_count": cluster.loaded_models.len(),
                    "pool_breakdown": pool_breakdown,
                },
                "nodes": cluster.nodes.into_iter().map(node_info_json).collect::<Vec<_>>(),
                "loaded_models": cluster.loaded_models.into_iter().map(loaded_model_json).collect::<Vec<_>>(),
            }))
            .into_response()
        }
        Err(error) => error_response(error),
    }
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    since_sequence: Option<u64>,
}

async fn admin_events(
    State(state): State<GatewayState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<EventsQuery>,
) -> Response {
    let since_sequence = query.since_sequence.unwrap_or(0);
    match state.client.list_events(since_sequence, &auth).await {
        Ok(response) => {
            state
                .metrics
                .record_cluster_events(response.events.len() as u64);
            Json(json!({
                "last_sequence": response.last_sequence,
                "events": response.events.into_iter().map(sequenced_event_json).collect::<Vec<_>>(),
            }))
            .into_response()
        }
        Err(error) => error_response(error),
    }
}

#[derive(Debug, Deserialize)]
struct AuditQuery {
    since: Option<String>,
    limit: Option<u32>,
}

async fn admin_audit(
    State(state): State<GatewayState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<AuditQuery>,
) -> Response {
    let since = match query.since.as_deref() {
        Some(value) => match chrono::DateTime::parse_from_rfc3339(value) {
            Ok(value) => Some(value.with_timezone(&chrono::Utc)),
            Err(error) => {
                return error_response(GalacticaError::invalid_argument(format!(
                    "invalid audit since timestamp: {error}"
                )));
            }
        },
        None => None,
    };
    let limit = query.limit.unwrap_or(state.config.audit_limit);
    match state.client.list_audit_records(since, limit, &auth).await {
        Ok(records) => {
            state.metrics.record_audit_records(records.len() as u64);
            Json(json!({
                "records": records.into_iter().map(audit_record_json).collect::<Vec<_>>(),
            }))
            .into_response()
        }
        Err(error) => error_response(error),
    }
}

async fn list_sessions(State(state): State<GatewayState>) -> Response {
    let sessions = state.sessions.list().await;
    state.metrics.set_active_sessions(sessions.len() as i64);
    Json(json!({ "sessions": sessions })).into_response()
}

async fn get_session(
    State(state): State<GatewayState>,
    Path(session_id): Path<String>,
) -> Response {
    match state.sessions.get(&session_id).await {
        Some(session) => Json(json!(session)).into_response(),
        None => error_response(GalacticaError::not_found(format!(
            "unknown session: {session_id}"
        ))),
    }
}

async fn admin_settings(
    State(state): State<GatewayState>,
    Extension(auth): Extension<AuthContext>,
) -> Response {
    Json(json!({
        "deployment_mode": state.config.deployment_mode,
        "auth": {
            "tenant_id": auth.tenant_id,
            "require_mtls": auth.require_mtls,
            "max_requests_per_minute": auth.max_requests_per_minute,
            "allowed_models": auth.allowed_models,
            "allowed_node_pools": auth.allowed_node_pools,
        },
        "gateway": {
            "global_requests_per_minute": state.config.global_requests_per_minute,
            "default_tenant_requests_per_minute": state.config.default_tenant_requests_per_minute,
            "session_history_limit": state.config.session_history_limit,
        }
    }))
    .into_response()
}

async fn chat_completions(
    State(state): State<GatewayState>,
    Extension(auth): Extension<AuthContext>,
    Extension(context): Extension<RequestContext>,
    Json(request): Json<ChatCompletionRequest>,
) -> Response {
    let completion_id = format!("chatcmpl-{}", context.request_id);
    let session_id = request
        .session_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    state
        .sessions
        .record_prompt(
            &session_id,
            &auth.tenant_id,
            &request.model,
            request.messages.last().cloned(),
        )
        .await;

    let inference = request.to_inference_request(context.request_id.clone());
    match state.client.infer(inference, &auth).await {
        Ok(chunks) => {
            let usage = chunks
                .last()
                .and_then(|chunk| chunk.usage.clone())
                .unwrap_or(InferenceUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                });
            state.metrics.record_inference(
                context.started_at.elapsed(),
                u64::from(usage.prompt_tokens),
                u64::from(usage.completion_tokens),
            );
            let response_text = chunks
                .iter()
                .map(|chunk| chunk.delta_content.as_str())
                .collect::<String>();
            state
                .sessions
                .record_response(&session_id, &response_text)
                .await;
            state
                .metrics
                .set_active_sessions(state.sessions.len().await as i64);

            if request.stream.unwrap_or(false) {
                let mut response = streaming_response(completion_id, request.model.clone(), chunks)
                    .into_response();
                insert_header(&mut response, SESSION_ID_HEADER, &session_id);
                response
            } else {
                let mut response = Json(ChatCompletionResponse::from_chunks(
                    completion_id,
                    &request.model,
                    Some(session_id.clone()),
                    chunks,
                ))
                .into_response();
                insert_header(&mut response, SESSION_ID_HEADER, &session_id);
                response
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
        GalacticaError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
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

fn rate_limit_response() -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(json!({
            "error": {
                "message": "rate limit exceeded",
                "type": "rate_limit_error",
            }
        })),
    )
        .into_response()
}

async fn authenticate_headers(state: &GatewayState, headers: &HeaderMap) -> Result<AuthContext> {
    let api_key = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
                .map(str::to_string)
        })
        .ok_or_else(|| GalacticaError::unauthorized("missing API key"))?;
    state.client.authenticate(&api_key).await
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
    pub session_id: Option<String>,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
}

impl ChatCompletionResponse {
    fn from_chunks(
        request_id: String,
        model: &str,
        session_id: Option<String>,
        chunks: Vec<InferenceChunk>,
    ) -> Self {
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
            session_id,
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

#[derive(Debug, Clone, Serialize)]
struct SessionSnapshot {
    session_id: String,
    tenant_id: String,
    model: String,
    created_at: String,
    updated_at: String,
    turns: Vec<SessionTurn>,
}

#[derive(Debug, Clone, Serialize)]
struct SessionSummary {
    session_id: String,
    tenant_id: String,
    model: String,
    updated_at: String,
    turn_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct SessionTurn {
    role: String,
    content: String,
    timestamp: String,
}

struct SessionStore {
    sessions: RwLock<HashMap<String, SessionSnapshot>>,
    max_turns: usize,
}

impl SessionStore {
    fn new(max_turns: usize) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            max_turns,
        }
    }

    async fn record_prompt(
        &self,
        session_id: &str,
        tenant_id: &str,
        model: &str,
        message: Option<OpenAiMessage>,
    ) {
        let now = chrono::Utc::now().to_rfc3339();
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .entry(session_id.to_string())
            .or_insert_with(|| SessionSnapshot {
                session_id: session_id.to_string(),
                tenant_id: tenant_id.to_string(),
                model: model.to_string(),
                created_at: now.clone(),
                updated_at: now.clone(),
                turns: Vec::new(),
            });
        session.updated_at = now.clone();
        session.model = model.to_string();
        if let Some(message) = message {
            session.turns.push(SessionTurn {
                role: message.role,
                content: message.content,
                timestamp: now,
            });
        }
        trim_session_turns(session, self.max_turns);
    }

    async fn record_response(&self, session_id: &str, response: &str) {
        let now = chrono::Utc::now().to_rfc3339();
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            session.updated_at = now.clone();
            session.turns.push(SessionTurn {
                role: "assistant".to_string(),
                content: response.to_string(),
                timestamp: now,
            });
            trim_session_turns(session, self.max_turns);
        }
    }

    async fn get(&self, session_id: &str) -> Option<SessionSnapshot> {
        self.sessions.read().await.get(session_id).cloned()
    }

    async fn list(&self) -> Vec<SessionSummary> {
        self.sessions
            .read()
            .await
            .values()
            .map(|session| SessionSummary {
                session_id: session.session_id.clone(),
                tenant_id: session.tenant_id.clone(),
                model: session.model.clone(),
                updated_at: session.updated_at.clone(),
                turn_count: session.turns.len(),
            })
            .collect()
    }

    async fn len(&self) -> usize {
        self.sessions.read().await.len()
    }
}

#[derive(Debug)]
struct TokenBucket {
    capacity: f64,
    refill_per_second: f64,
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(limit_per_minute: u32) -> Self {
        let capacity = f64::from(limit_per_minute.max(1));
        Self {
            capacity,
            refill_per_second: capacity / 60.0,
            tokens: capacity,
            last_refill: Instant::now(),
        }
    }

    fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_second).min(self.capacity);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

struct GatewayRateLimiter {
    global: Mutex<TokenBucket>,
    tenants: Mutex<HashMap<String, TokenBucket>>,
    default_tenant_limit: u32,
}

impl GatewayRateLimiter {
    fn new(global_limit: u32, default_tenant_limit: u32) -> Self {
        Self {
            global: Mutex::new(TokenBucket::new(global_limit.max(1))),
            tenants: Mutex::new(HashMap::new()),
            default_tenant_limit: default_tenant_limit.max(1),
        }
    }

    async fn allow(&self, tenant_id: &str, tenant_limit: u32, config: &GatewayConfig) -> bool {
        {
            let mut global = self.global.lock().await;
            if global.capacity != f64::from(config.global_requests_per_minute.max(1)) {
                *global = TokenBucket::new(config.global_requests_per_minute.max(1));
            }
            if !global.allow() {
                return false;
            }
        }

        let mut tenants = self.tenants.lock().await;
        let effective_limit = if tenant_limit == 0 {
            self.default_tenant_limit
        } else {
            tenant_limit
        };
        let bucket = tenants
            .entry(tenant_id.to_string())
            .or_insert_with(|| TokenBucket::new(effective_limit));
        if bucket.capacity != f64::from(effective_limit) {
            *bucket = TokenBucket::new(effective_limit);
        }
        bucket.allow()
    }
}

fn trim_session_turns(session: &mut SessionSnapshot, max_turns: usize) {
    if session.turns.len() > max_turns {
        let remove = session.turns.len() - max_turns;
        session.turns.drain(0..remove);
    }
}

fn insert_header(response: &mut Response, key: &str, value: &str) {
    if let (Ok(name), Ok(value)) = (
        HeaderName::from_bytes(key.as_bytes()),
        HeaderValue::from_str(value),
    ) {
        response.headers_mut().insert(name, value);
    }
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn connect_error(service: &'static str) -> impl FnOnce(tonic::transport::Error) -> GalacticaError {
    move |error| GalacticaError::unavailable(format!("failed to connect to {service}: {error}"))
}

fn rpc_unavailable(operation: &'static str) -> impl FnOnce(tonic::Status) -> GalacticaError {
    move |error| GalacticaError::unavailable(format!("{operation} failed: {error}"))
}

fn rpc_stream_unavailable(operation: &'static str) -> impl FnOnce(tonic::Status) -> GalacticaError {
    move |error| GalacticaError::unavailable(format!("{operation} stream failed: {error}"))
}

fn node_info_json(node: common::v1::NodeInfo) -> Value {
    let capabilities = node.capabilities.unwrap_or_default();
    json!({
        "node_id": node.node_id.map(|node_id| node_id.value).unwrap_or_default(),
        "hostname": node.hostname,
        "status": node_status_label(node.status),
        "last_heartbeat": node.last_heartbeat.map(galactica_common::timestamp_to_chrono).map(|time| time.to_rfc3339()),
        "version": node.version,
        "pool": pool_label(&capabilities),
        "runtime_backends": capabilities.runtime_backends,
        "accelerators": capabilities.accelerators.into_iter().map(|accelerator| json!({
            "type": accelerator.r#type,
            "name": accelerator.name,
            "compute_capability": accelerator.compute_capability,
            "vram_bytes": accelerator.vram.map(|memory| memory.total_bytes).unwrap_or_default(),
        })).collect::<Vec<_>>(),
        "locality": capabilities.locality,
        "system_memory": capabilities.system_memory.map(|memory| json!({
            "total_bytes": memory.total_bytes,
            "available_bytes": memory.available_bytes,
        })),
    })
}

fn loaded_model_json(model: control::v1::LoadedModel) -> Value {
    json!({
        "instance_id": model.instance_id.map(|id| id.value).unwrap_or_default(),
        "model_id": model.model_id.map(|id| id.value).unwrap_or_default(),
        "node_id": model.node_id.map(|id| id.value).unwrap_or_default(),
        "runtime": model.runtime,
    })
}

fn model_manifest_json(model: common::v1::ModelManifest) -> Value {
    json!({
        "model_id": model.model_id.map(|id| id.value).unwrap_or_default(),
        "name": model.name,
        "family": model.family,
        "chat_template": model.chat_template,
        "metadata": model.metadata,
        "variants": model.variants.into_iter().map(|variant| json!({
            "runtime": variant.runtime,
            "quantization": variant.quantization,
            "format": variant.format,
            "size_bytes": variant.size_bytes,
            "compatible_accelerators": variant.compatible_accelerators,
        })).collect::<Vec<_>>(),
    })
}

fn sequenced_event_json(event: control::v1::SequencedClusterEvent) -> Value {
    let inner = event.event.unwrap_or_default();
    json!({
        "sequence": event.sequence,
        "event_id": inner.event_id,
        "timestamp": inner.timestamp.map(galactica_common::timestamp_to_chrono).map(|time| time.to_rfc3339()),
        "payload": cluster_event_payload(inner),
    })
}

fn cluster_event_payload(event: common::v1::ClusterEvent) -> Value {
    match event.event {
        Some(common::v1::cluster_event::Event::NodeJoined(joined)) => json!({
            "type": "node_joined",
            "node": joined.node.map(node_info_json),
        }),
        Some(common::v1::cluster_event::Event::NodeLeft(left)) => json!({
            "type": "node_left",
            "node_id": left.node_id.map(|id| id.value).unwrap_or_default(),
            "reason": left.reason,
        }),
        Some(common::v1::cluster_event::Event::ModelLoaded(loaded)) => json!({
            "type": "model_loaded",
            "node_id": loaded.node_id.map(|id| id.value).unwrap_or_default(),
            "model_id": loaded.model_id.map(|id| id.value).unwrap_or_default(),
            "instance_id": loaded.instance_id.map(|id| id.value).unwrap_or_default(),
        }),
        Some(common::v1::cluster_event::Event::ModelUnloaded(unloaded)) => json!({
            "type": "model_unloaded",
            "node_id": unloaded.node_id.map(|id| id.value).unwrap_or_default(),
            "instance_id": unloaded.instance_id.map(|id| id.value).unwrap_or_default(),
        }),
        Some(common::v1::cluster_event::Event::TaskCompleted(task)) => json!({
            "type": "task_completed",
            "task_id": task.task_id.map(|id| id.value).unwrap_or_default(),
            "status": task.status,
        }),
        None => json!({ "type": "unknown" }),
    }
}

fn audit_record_json(record: control::v1::AuditRecord) -> Value {
    json!({
        "event_id": record.event_id,
        "timestamp": record.timestamp.map(galactica_common::timestamp_to_chrono).map(|time| time.to_rfc3339()),
        "actor": record.actor,
        "action": record.action,
        "resource": record.resource,
        "outcome": record.outcome,
        "details": record.details,
    })
}

fn node_status_label(status: i32) -> &'static str {
    match common::v1::NodeStatus::try_from(status).unwrap_or(common::v1::NodeStatus::Unspecified) {
        common::v1::NodeStatus::Registering => "registering",
        common::v1::NodeStatus::Online => "online",
        common::v1::NodeStatus::Offline => "offline",
        common::v1::NodeStatus::Draining => "draining",
        common::v1::NodeStatus::Removed => "removed",
        common::v1::NodeStatus::Unspecified => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::body::to_bytes;
    use axum::http::{Method, Request};
    use galactica_artifact::LocalModelRegistry;
    use galactica_common::proto::control::v1::control_plane_client::ControlPlaneClient;
    use galactica_common::proto::control::v1::control_plane_server::ControlPlaneServer;
    use galactica_common::proto::node::v1::node_agent_server::NodeAgentServer;
    use galactica_control_plane::{
        ControlPlaneService, GrpcNodeExecutor, InMemoryStateStore, TenantRecord,
        inject_node_fingerprint,
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

    #[derive(Clone)]
    struct LimitedMockControlPlaneClient;

    fn sample_tenant_auth() -> AuthContext {
        AuthContext {
            tenant_id: "tenant-dev".to_string(),
            scopes: vec!["inference:read".to_string(), "inference:write".to_string()],
            expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
            require_mtls: false,
            max_requests_per_minute: 120,
            allowed_models: Vec::new(),
            allowed_node_pools: Vec::new(),
            actor: "tenant:tenant-dev".to_string(),
            credential_kind: CredentialKind::ApiKey,
        }
    }

    #[async_trait]
    impl ControlPlaneApi for MockControlPlaneClient {
        async fn list_models(
            &self,
            _filter_runtime: Option<&str>,
            _auth: &AuthContext,
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

        async fn infer(
            &self,
            request: InferenceRequest,
            _auth: &AuthContext,
        ) -> Result<Vec<InferenceChunk>> {
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

        async fn authenticate(&self, _api_key: &str) -> Result<AuthContext> {
            Ok(sample_tenant_auth())
        }

        async fn get_cluster_state(
            &self,
            _auth: &AuthContext,
        ) -> Result<control::v1::GetClusterStateResponse> {
            Ok(control::v1::GetClusterStateResponse {
                nodes: vec![common::v1::NodeInfo {
                    node_id: Some(common::v1::NodeId {
                        value: "node-a".to_string(),
                    }),
                    hostname: "mac-mini".to_string(),
                    capabilities: Some(default_macos_capabilities()),
                    status: common::v1::NodeStatus::Online as i32,
                    last_heartbeat: Some(galactica_common::chrono_to_timestamp(chrono::Utc::now())),
                    version: "0.1.0".to_string(),
                }],
                loaded_models: vec![control::v1::LoadedModel {
                    instance_id: Some(common::v1::InstanceId {
                        value: "inst-a".to_string(),
                    }),
                    model_id: Some(common::v1::ModelId {
                        value: "mistral-small".to_string(),
                    }),
                    node_id: Some(common::v1::NodeId {
                        value: "node-a".to_string(),
                    }),
                    runtime: "mlx".to_string(),
                }],
            })
        }

        async fn list_events(
            &self,
            since_sequence: u64,
            _auth: &AuthContext,
        ) -> Result<control::v1::ListEventsResponse> {
            Ok(control::v1::ListEventsResponse {
                events: vec![control::v1::SequencedClusterEvent {
                    sequence: since_sequence + 1,
                    event: Some(common::v1::ClusterEvent {
                        event_id: "evt-1".to_string(),
                        timestamp: Some(galactica_common::chrono_to_timestamp(chrono::Utc::now())),
                        event: Some(common::v1::cluster_event::Event::NodeJoined(
                            common::v1::NodeJoined {
                                node: Some(common::v1::NodeInfo {
                                    node_id: Some(common::v1::NodeId {
                                        value: "node-a".to_string(),
                                    }),
                                    hostname: "mac-mini".to_string(),
                                    capabilities: Some(default_macos_capabilities()),
                                    status: common::v1::NodeStatus::Online as i32,
                                    last_heartbeat: Some(galactica_common::chrono_to_timestamp(
                                        chrono::Utc::now(),
                                    )),
                                    version: "0.1.0".to_string(),
                                }),
                            },
                        )),
                    }),
                }],
                last_sequence: since_sequence + 1,
            })
        }

        async fn list_audit_records(
            &self,
            _since: Option<chrono::DateTime<chrono::Utc>>,
            _limit: u32,
            _auth: &AuthContext,
        ) -> Result<Vec<control::v1::AuditRecord>> {
            Ok(vec![control::v1::AuditRecord {
                event_id: "audit-1".to_string(),
                timestamp: Some(galactica_common::chrono_to_timestamp(chrono::Utc::now())),
                actor: "tenant:tenant-dev".to_string(),
                action: "auth.authenticate".to_string(),
                resource: "tenant:tenant-dev".to_string(),
                outcome: "success".to_string(),
                details: HashMap::from([("credential_kind".to_string(), "api_key".to_string())]),
            }])
        }
    }

    #[async_trait]
    impl ControlPlaneApi for LimitedMockControlPlaneClient {
        async fn list_models(
            &self,
            filter_runtime: Option<&str>,
            auth: &AuthContext,
        ) -> Result<Vec<common::v1::ModelManifest>> {
            MockControlPlaneClient
                .list_models(filter_runtime, auth)
                .await
        }

        async fn infer(
            &self,
            request: InferenceRequest,
            auth: &AuthContext,
        ) -> Result<Vec<InferenceChunk>> {
            MockControlPlaneClient.infer(request, auth).await
        }

        async fn authenticate(&self, _api_key: &str) -> Result<AuthContext> {
            let mut auth = sample_tenant_auth();
            auth.max_requests_per_minute = 1;
            Ok(auth)
        }

        async fn get_cluster_state(
            &self,
            auth: &AuthContext,
        ) -> Result<control::v1::GetClusterStateResponse> {
            MockControlPlaneClient.get_cluster_state(auth).await
        }

        async fn list_events(
            &self,
            since_sequence: u64,
            auth: &AuthContext,
        ) -> Result<control::v1::ListEventsResponse> {
            MockControlPlaneClient
                .list_events(since_sequence, auth)
                .await
        }

        async fn list_audit_records(
            &self,
            since: Option<chrono::DateTime<chrono::Utc>>,
            limit: u32,
            auth: &AuthContext,
        ) -> Result<Vec<control::v1::AuditRecord>> {
            MockControlPlaneClient
                .list_audit_records(since, limit, auth)
                .await
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
                    .header("x-api-key", "galactica-dev-key")
                    .body(axum::body::Body::from(
                        r#"{"model":"mistral-small","messages":[{"role":"user","content":"hello"}],"session_id":"sess-1"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key(REQUEST_ID_HEADER));
        assert_eq!(response.headers().get(SESSION_ID_HEADER).unwrap(), "sess-1");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("\"object\":\"chat.completion\""));
        assert!(body.contains("hello world"));
        assert!(body.contains("\"session_id\":\"sess-1\""));
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
                    .header("x-api-key", "galactica-dev-key")
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
    async fn rate_limits_per_tenant_requests() {
        let app = build_router_with_config(
            Arc::new(LimitedMockControlPlaneClient),
            GatewayConfig {
                global_requests_per_minute: 100,
                default_tenant_requests_per_minute: 1,
                ..GatewayConfig::default()
            },
        );

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/models")
                    .header("x-api-key", "galactica-dev-key")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let second = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/models")
                    .header("x-api-key", "galactica-dev-key")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn exposes_admin_endpoints_and_session_tracking() {
        let app = build_router(Arc::new(MockControlPlaneClient));

        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .header("x-api-key", "galactica-dev-key")
                    .body(axum::body::Body::from(
                        r#"{"model":"mistral-small","messages":[{"role":"user","content":"hello"}],"session_id":"sess-admin"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let cluster = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/admin/cluster")
                    .header("x-api-key", "galactica-dev-key")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cluster.status(), StatusCode::OK);

        let session = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/admin/sessions/sess-admin")
                    .header("x-api-key", "galactica-dev-key")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(session.status(), StatusCode::OK);
        let session_body = to_bytes(session.into_body(), usize::MAX).await.unwrap();
        let session_body = String::from_utf8(session_body.to_vec()).unwrap();
        assert!(session_body.contains("sess-admin"));
        assert!(session_body.contains("assistant"));

        let events = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/admin/events?since_sequence=0")
                    .header("x-api-key", "galactica-dev-key")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(events.status(), StatusCode::OK);

        let metrics = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/metrics")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metrics.status(), StatusCode::OK);
        let metrics_body = to_bytes(metrics.into_body(), usize::MAX).await.unwrap();
        let metrics_body = String::from_utf8(metrics_body.to_vec()).unwrap();
        assert!(metrics_body.contains("gateway_http_requests_total"));
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
        let control_plane = ControlPlaneService::new(
            store.clone(),
            Arc::new(LocalModelRegistry::new(&root)),
            Arc::new(GrpcNodeExecutor),
        );
        control_plane
            .seed_tenant(TenantRecord {
                tenant_id: "tenant-dev".to_string(),
                api_key: "galactica-dev-key".to_string(),
                scopes: vec!["inference:read".to_string(), "inference:write".to_string()],
                require_mtls: false,
                max_requests_per_minute: 120,
                allowed_models: vec!["mistral-small".to_string()],
                allowed_node_pools: vec!["macos-metal-arm64".to_string()],
                expires_at: None,
            })
            .await
            .unwrap();
        let control_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control_addr: SocketAddr = control_listener.local_addr().unwrap();
        let control_incoming = TcpListenerStream::new(control_listener);
        let control_plane_server = control_plane.clone();
        tokio::spawn(async move {
            Server::builder()
                .add_service(ControlPlaneServer::new(control_plane_server))
                .serve_with_incoming(control_incoming)
                .await
                .unwrap();
        });

        let mut client = ControlPlaneClient::connect(format!("http://{control_addr}"))
            .await
            .unwrap();
        let enrollment_token = control_plane
            .mint_enrollment_token(
                "tenant-dev",
                vec!["node".to_string()],
                std::time::Duration::from_secs(300),
            )
            .await
            .unwrap();
        let enrolled = client
            .enroll_node(control::v1::EnrollNodeRequest {
                enrollment_token,
                hostname: "mac-mini".to_string(),
                capabilities: Some(default_macos_capabilities()),
            })
            .await
            .unwrap()
            .into_inner();
        let mut register = tonic::Request::new(control::v1::RegisterNodeRequest {
            hostname: "mac-mini".to_string(),
            capabilities: Some(default_macos_capabilities()),
            version: "0.1.0".to_string(),
            agent_endpoint: format!("http://{node_addr}"),
        });
        inject_node_fingerprint(
            &mut register,
            &galactica_control_plane::sha256_hex(&enrolled.identity.unwrap().certificate_pem),
        )
        .unwrap();
        client.register_node(register).await.unwrap();

        let app = build_router(Arc::new(GrpcControlPlaneClient::new(format!(
            "http://{control_addr}"
        ))));
        let models = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/v1/models")
                    .header("x-api-key", "galactica-dev-key")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(models.status(), StatusCode::OK);

        let completion = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .header("x-api-key", "galactica-dev-key")
                    .body(axum::body::Body::from(
                        r#"{"model":"mistral-small","messages":[{"role":"user","content":"summarize node health"}],"session_id":"sess-grpc"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(completion.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("\"chat.completion\""));

        let cluster = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/admin/cluster")
                    .header("x-api-key", "galactica-dev-key")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cluster.status(), StatusCode::OK);

        let audit = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/admin/audit")
                    .header("x-api-key", "galactica-dev-key")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(audit.status(), StatusCode::OK);
    }
}
