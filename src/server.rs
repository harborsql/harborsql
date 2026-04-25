use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{
    config::Config,
    engine::{QueryEngine, QueryResult},
    error::{HarborError, Result},
};

#[derive(Clone)]
struct AppState {
    config: Config,
    engine: QueryEngine,
}

pub async fn serve(config: Config, engine: QueryEngine) -> Result<()> {
    let state = Arc::new(AppState {
        config: config.clone(),
        engine,
    });
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/v1/query", post(query))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!("listening on http://{}", config.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn query(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>> {
    let token = bearer_token(&headers)?;
    let catalog = request
        .catalog
        .as_deref()
        .unwrap_or(&state.config.default_catalog);
    let schema = request
        .schema
        .as_deref()
        .unwrap_or(&state.config.default_schema);
    let result = state
        .engine
        .execute(token, &request.sql, catalog, schema)
        .await?;
    Ok(Json(QueryResponse { result }))
}

fn bearer_token(headers: &HeaderMap) -> Result<&str> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(HarborError::MissingBearerToken)?;
    value
        .strip_prefix("Bearer ")
        .filter(|token| !token.trim().is_empty())
        .ok_or(HarborError::MissingBearerToken)
}

#[derive(Debug, Deserialize)]
struct QueryRequest {
    sql: String,
    catalog: Option<String>,
    schema: Option<String>,
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    result: QueryResult,
}
