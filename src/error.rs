use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

pub type Result<T> = std::result::Result<T, HarborError>;

#[derive(Debug, thiserror::Error)]
pub enum HarborError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("authorization header is missing or is not a bearer token")]
    MissingBearerToken,
    #[error("unsupported SQL for MVP: {0}")]
    UnsupportedSql(String),
    #[error("Unity Catalog error: {0}")]
    Unity(String),
    #[error("query execution error: {0}")]
    Query(String),
    #[error("HTTP client error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("URL error: {0}")]
    Url(#[from] url::ParseError),
    #[error("Delta error: {0}")]
    Delta(#[from] deltalake::errors::DeltaTableError),
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("Arrow JSON error: {0}")]
    ArrowJson(#[from] datafusion::arrow::error::ArrowError),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("logger setup error: {0}")]
    Logger(#[from] tracing_subscriber::filter::ParseError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl HarborError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::MissingBearerToken => StatusCode::UNAUTHORIZED,
            Self::Config(_) | Self::UnsupportedSql(_) => StatusCode::BAD_REQUEST,
            Self::Unity(_) => StatusCode::BAD_GATEWAY,
            Self::Query(_) | Self::Delta(_) | Self::DataFusion(_) | Self::ArrowJson(_) => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            Self::Http(_) | Self::Url(_) | Self::Json(_) | Self::Logger(_) | Self::Io(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for HarborError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = Json(ErrorBody {
            error: self.to_string(),
        });
        (status, body).into_response()
    }
}
