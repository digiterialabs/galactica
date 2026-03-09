use std::net::SocketAddr;

use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::api;

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub control_plane_addr: String,
}

/// Build the axum router with all routes and middleware.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(api::openai::routes())
        .merge(api::models::routes())
        .merge(api::health::routes())
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

/// Start the HTTP server.
pub async fn run_http_server(
    listen_addr: SocketAddr,
    state: AppState,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = build_router(state);

    tracing::info!(%listen_addr, "starting HTTP server");

    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
