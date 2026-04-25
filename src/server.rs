use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{
    config::Config,
    engine::{QueryEngine, QueryResult},
    error::{HarborError, Result},
    thrift::{DatabricksThriftService, QueryHistory},
};

#[derive(Clone)]
struct AppState {
    config: Config,
    engine: QueryEngine,
    thrift: DatabricksThriftService,
}

pub async fn serve(config: Config, engine: QueryEngine) -> Result<()> {
    let thrift = DatabricksThriftService::new(config.clone(), engine.clone());
    let state = Arc::new(AppState {
        config: config.clone(),
        engine,
        thrift,
    });
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/v1/query", post(query))
        .route(
            "/api/2.0/connector-service/feature-flags/PYTHON/{version}",
            get(feature_flags),
        )
        .route(
            "/api/2.0/sql/history/queries/{query_id}",
            get(query_history),
        )
        .route("/{*path}", post(thrift_rpc))
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

async fn thrift_rpc(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let token = bearer_token(&headers)?;
    let body = state.thrift.handle(token, &body).await?;
    Ok(([(header::CONTENT_TYPE, "application/x-thrift")], body).into_response())
}

async fn query_history(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(query_id): Path<String>,
) -> Result<Json<QueryHistory>> {
    let _token = bearer_token(&headers)?;
    state
        .thrift
        .query_history(&query_id)
        .await
        .map(Json)
        .ok_or_else(|| HarborError::Query(format!("unknown query id `{query_id}`")))
}

async fn feature_flags(Path(_version): Path<String>) -> Json<FeatureFlagsResponse> {
    Json(FeatureFlagsResponse {
        flags: Vec::new(),
        ttl_seconds: 900,
    })
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

#[derive(Debug, Serialize)]
struct FeatureFlagsResponse {
    flags: Vec<FeatureFlag>,
    ttl_seconds: u64,
}

#[derive(Debug, Serialize)]
struct FeatureFlag {
    name: String,
    value: String,
}
