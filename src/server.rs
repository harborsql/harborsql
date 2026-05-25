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
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
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
    let token = auth_token(&headers)?;
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
        .execute(&token, &request.sql, catalog, schema)
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
    let token = auth_token(&headers)?;
    let body = state.thrift.handle(&token, &body).await?;
    Ok(([(header::CONTENT_TYPE, "application/x-thrift")], body).into_response())
}

async fn query_history(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(query_id): Path<String>,
) -> Result<Json<QueryHistory>> {
    let token = auth_token(&headers)?;
    state
        .thrift
        .query_history(&token, &query_id)
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

fn auth_token(headers: &HeaderMap) -> Result<String> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(HarborError::MissingBearerToken)?;

    if let Some(token) = bearer_auth_token(value) {
        return Ok(token.to_string());
    }
    if let Some(token) = databricks_basic_auth_token(value) {
        return Ok(token);
    }

    Err(HarborError::MissingBearerToken)
}

fn bearer_auth_token(value: &str) -> Option<&str> {
    let (scheme, token) = value.trim().split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") && !token.trim().is_empty() {
        Some(token.trim())
    } else {
        None
    }
}

fn databricks_basic_auth_token(value: &str) -> Option<String> {
    let value = value.trim();
    // Databricks JDBC 2.x emits PAT credentials as Basic auth, and some builds omit
    // the usual space after the auth scheme.
    let rest = value.get(..5).and_then(|scheme| {
        if scheme.eq_ignore_ascii_case("basic") {
            Some(&value[5..])
        } else {
            None
        }
    })?;
    let encoded = rest.trim_start();
    if encoded.is_empty() {
        return None;
    }
    let decoded = BASE64_STANDARD.decode(encoded).ok()?;
    let credentials = String::from_utf8(decoded).ok()?;
    let (username, password) = credentials.split_once(':')?;
    if username == "token" && !password.trim().is_empty() {
        Some(password.to_string())
    } else {
        None
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

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};

    use super::auth_token;

    #[test]
    fn auth_token_accepts_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer local-token"),
        );

        assert_eq!(auth_token(&headers).unwrap(), "local-token");
    }

    #[test]
    fn auth_token_accepts_databricks_pat_basic_auth() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic dG9rZW46bG9jYWwtdG9rZW4="),
        );

        assert_eq!(auth_token(&headers).unwrap(), "local-token");
    }

    #[test]
    fn auth_token_accepts_legacy_databricks_basic_auth_without_space() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("BasicdG9rZW46bG9jYWwtdG9rZW4="),
        );

        assert_eq!(auth_token(&headers).unwrap(), "local-token");
    }

    #[test]
    fn auth_token_rejects_basic_auth_with_non_token_user() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic dXNlcjpsb2NhbC10b2tlbg=="),
        );

        assert!(auth_token(&headers).is_err());
    }
}
