use axum::{Json, Router, routing::get};
use serde::Serialize;

use crate::server::AppState;

#[derive(Debug, Serialize)]
pub struct ListModelsResponse {
    pub object: String,
    pub data: Vec<ModelObject>,
}

#[derive(Debug, Serialize)]
pub struct ModelObject {
    pub id: String,
    pub object: String,
    pub created: u64,
    pub owned_by: String,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/models", get(list_models))
}

async fn list_models() -> Json<ListModelsResponse> {
    // TODO: query control plane for available models
    Json(ListModelsResponse {
        object: "list".to_string(),
        data: Vec::new(),
    })
}
