use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tower_http::{ServiceBuilderExt, request_id::MakeRequestUuid, trace::TraceLayer};
use tracing::{Span, info, info_span};

use crate::{
    config::Config,
    engine::{QueryEngine, QueryResult},
    error::{HarborError, Result},
    observability,
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
    let _cleanup_task = thrift.spawn_cleanup_task();
    let state = Arc::new(AppState {
        config: config.clone(),
        engine,
        thrift,
    });
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
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
        .with_state(state)
        .layer(
            ServiceBuilder::new()
                .set_x_request_id(MakeRequestUuid)
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(|request: &Request<Body>| {
                            let request_id = header_value(request.headers(), "x-request-id");
                            info_span!(
                                "http_request",
                                request_id,
                                method = %request.method(),
                                route = route_label(request.uri().path()),
                            )
                        })
                        .on_response(
                            |response: &Response<Body>, latency: Duration, _span: &Span| {
                                info!(
                                    status = response.status().as_u16(),
                                    latency_ms = latency.as_millis() as u64,
                                    "http request completed"
                                );
                            },
                        ),
                )
                .propagate_x_request_id()
                .layer(middleware::from_fn(record_http_metrics)),
        )
        .layer(DefaultBodyLimit::max(config.request_body_limit_bytes));

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!("listening on http://{}", config.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn metrics() -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        observability::get().metrics().render_prometheus(),
    )
        .into_response()
}

async fn record_http_metrics(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let route = route_label(request.uri().path());
    let started = Instant::now();
    let response = next.run(request).await;
    observability::get().metrics().observe_http(
        method.as_str(),
        route,
        response.status().as_u16(),
        started.elapsed(),
    );
    response
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

fn route_label(path: &str) -> &'static str {
    if path == "/healthz" {
        "/healthz"
    } else if path == "/metrics" {
        "/metrics"
    } else if path == "/api/v1/query" {
        "/api/v1/query"
    } else if path.starts_with("/api/2.0/connector-service/feature-flags/PYTHON/") {
        "/api/2.0/connector-service/feature-flags/PYTHON/{version}"
    } else if path.starts_with("/api/2.0/sql/history/queries/") {
        "/api/2.0/sql/history/queries/{query_id}"
    } else {
        "/{*path}"
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string()
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
    let token = bearer_token(&headers)?;
    state
        .thrift
        .query_history(token, &query_id)
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
    let (scheme, token) = value
        .trim()
        .split_once(' ')
        .ok_or(HarborError::MissingBearerToken)?;
    if scheme.eq_ignore_ascii_case("bearer") && !token.trim().is_empty() {
        Ok(token.trim())
    } else {
        Err(HarborError::MissingBearerToken)
    }
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
