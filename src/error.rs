use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use regex::Regex;
use serde::Serialize;
use std::sync::OnceLock;

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
    #[error("Databricks Thrift protocol error: {0}")]
    Thrift(String),
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
            Self::Config(_) | Self::UnsupportedSql(_) | Self::Thrift(_) => StatusCode::BAD_REQUEST,
            Self::Unity(_) => StatusCode::BAD_GATEWAY,
            Self::Query(_) | Self::Delta(_) | Self::DataFusion(_) | Self::ArrowJson(_) => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            Self::Http(_) | Self::Url(_) | Self::Json(_) | Self::Logger(_) | Self::Io(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    pub fn client_error(&self) -> ClientError {
        match self {
            Self::Config(_) => ClientError::new("CONFIG_INVALID", "invalid configuration"),
            Self::MissingBearerToken => ClientError::new("UNAUTHORIZED", "missing bearer token"),
            Self::UnsupportedSql(_) => ClientError::new("UNSUPPORTED_SQL", "unsupported SQL"),
            Self::Unity(_) => {
                ClientError::new("UNITY_CATALOG_ERROR", "Unity Catalog request failed")
            }
            Self::Query(_) => ClientError::new("QUERY_FAILED", "query execution failed"),
            Self::Thrift(_) => ClientError::new("THRIFT_PROTOCOL_ERROR", "invalid Thrift request"),
            Self::Http(_) => {
                ClientError::new("UPSTREAM_HTTP_ERROR", "upstream HTTP request failed")
            }
            Self::Url(_) => ClientError::new("INVALID_URL", "invalid URL"),
            Self::Delta(_) => ClientError::new("DELTA_TABLE_ERROR", "Delta table read failed"),
            Self::DataFusion(_) => {
                ClientError::new("DATAFUSION_ERROR", "query planning or execution failed")
            }
            Self::ArrowJson(_) => {
                ClientError::new("RESULT_ENCODING_ERROR", "result encoding failed")
            }
            Self::Json(_) => ClientError::new("JSON_ERROR", "JSON processing failed"),
            Self::Logger(_) | Self::Io(_) => ClientError::internal(),
        }
    }

    pub fn redacted_internal_message(&self) -> String {
        redact_sensitive(&self.to_string())
    }

    pub fn log_internal(&self, context: &'static str) {
        let client_error = self.client_error();
        tracing::error!(
            error_code = client_error.code,
            http_status = self.status_code().as_u16(),
            internal_error = %self.redacted_internal_message(),
            context,
            "request failed"
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ClientError {
    pub code: &'static str,
    pub message: &'static str,
}

impl ClientError {
    pub const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    pub const fn internal() -> Self {
        Self::new("INTERNAL_ERROR", "internal server error")
    }

    pub fn status_message(self) -> String {
        format!("{}: {}", self.code, self.message)
    }
}

pub fn redact_sensitive(value: &str) -> String {
    let mut redacted = value.to_string();
    for (pattern, replacement) in redaction_patterns() {
        redacted = pattern.replace_all(&redacted, *replacement).into_owned();
    }
    redacted
}

pub fn redact_and_truncate(value: &str, max_len: usize) -> String {
    truncate(&redact_sensitive(value), max_len)
}

fn redaction_patterns() -> &'static [(Regex, &'static str)] {
    static PATTERNS: OnceLock<Vec<(Regex, &'static str)>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        [
            (
                r#"(?i)\b(?:s3a?|abfss?|wasbs?|gs|dbfs)://[^\s"')\]}>,]+"#,
                "[REDACTED_PATH]",
            ),
            (
                r#"(?i)\bhttps?://[^\s"')\]}>,]+"#,
                "[REDACTED_URL]",
            ),
            (
                r#"(?i)/(?:Volumes|dbfs|mnt|__unitystorage)(?:/[^\s"')\]}>,]+)+"#,
                "[REDACTED_PATH]",
            ),
            (
                r#"(?i)\b(authorization\s*[:=]\s*bearer\s+)[^\s,;]+"#,
                "$1[REDACTED]",
            ),
            (
                r#"(?i)\b(bearer\s+)[A-Za-z0-9._~+/\-=]+"#,
                "$1[REDACTED]",
            ),
            (
                r#"\bdapi[A-Za-z0-9]{16,}\b"#,
                "[REDACTED_DATABRICKS_TOKEN]",
            ),
            (
                r#"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"#,
                "[REDACTED_TOKEN]",
            ),
            (
                r#"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"#,
                "[REDACTED_AWS_ACCESS_KEY]",
            ),
            (
                r#"(?i)("?(?:access[_-]?key[_-]?id|aws[_-]?access[_-]?key[_-]?id|secret[_-]?access[_-]?key|aws[_-]?secret[_-]?access[_-]?key|session[_-]?token|aws[_-]?session[_-]?token|security[_-]?token|x-amz-credential|x-amz-signature|x-amz-security-token|databricks-token|authorization|access[_-]?token|refresh[_-]?token|id[_-]?token|token|client[_-]?secret|password|workspace[_-]?id|workspace[_-]?url|warehouse[_-]?id|table[_-]?id|full[_-]?name|catalog[_-]?name|schema[_-]?name|storage[_-]?location|credential[_-]?name|external[_-]?location|url|path)"?\s*[:=]\s*)"?[^"',}\]\s]+"#,
                "$1[REDACTED]",
            ),
            (
                r#"(?is)\b(sql|query|statement)\b\s*[:=]\s*"[^"]*""#,
                "$1=[REDACTED_SQL]",
            ),
            (
                r#"(?is)\b(sql|query|statement)\b\s*[:=]\s*'[^']*'"#,
                "$1=[REDACTED_SQL]",
            ),
            (
                r#"(?is)\b(?:select|with|insert|update|delete|merge|create|drop|alter|describe|show|explain)\b[^;]{0,2000};"#,
                "[REDACTED_SQL]",
            ),
        ]
        .into_iter()
        .map(|(pattern, replacement)| {
            (
                Regex::new(pattern).expect("redaction regex should compile"),
                replacement,
            )
        })
        .collect()
    })
}

fn truncate(value: &str, max_len: usize) -> String {
    if value.len() <= max_len {
        value.to_string()
    } else {
        let mut end = max_len;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &value[..end])
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
    message: &'static str,
}

impl IntoResponse for HarborError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let client_error = self.client_error();
        self.log_internal("http response");
        let body = Json(ErrorBody {
            error: client_error.code,
            message: client_error.message,
        });
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::{body::to_bytes, response::IntoResponse};

    use super::*;

    #[test]
    fn redacts_sensitive_values() {
        let value = r#"Authorization: Bearer dapi123
dapi0123456789abcdef0123456789abcdef
eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.signature_value
https://dbc.example.com/api/2.1/unity-catalog/tables/main.default.people
s3://bucket/private/path/file.parquet
/Volumes/main/default/secret/path.parquet
aws_secret_access_key="secret-value"
session_token: "session-value"
access_key_id: "AKIAABCDEFGHIJKLMNOP"
full_name: "main.default.people"
statement="select * from main.default.people"
select * from secret.table where token = 'abc';"#;

        let redacted = redact_sensitive(value);

        assert!(!redacted.contains("dapi123"));
        assert!(!redacted.contains("dapi0123456789abcdef0123456789abcdef"));
        assert!(!redacted.contains("eyJhbGciOiJIUzI1NiJ9"));
        assert!(!redacted.contains("dbc.example.com"));
        assert!(!redacted.contains("bucket/private"));
        assert!(!redacted.contains("/Volumes/main"));
        assert!(!redacted.contains("secret-value"));
        assert!(!redacted.contains("session-value"));
        assert!(!redacted.contains("AKIAABCDEFGHIJKLMNOP"));
        assert!(!redacted.contains("main.default.people"));
        assert!(!redacted.contains("secret.table"));
        assert!(redacted.contains("[REDACTED_URL]"));
        assert!(redacted.contains("[REDACTED_PATH]"));
        assert!(redacted.contains("[REDACTED]"));
        assert!(redacted.contains("[REDACTED_SQL]"));
    }

    #[tokio::test]
    async fn http_response_uses_client_error() {
        let response = HarborError::Query(
            "failed to read s3://bucket/private/path for select * from secret.table;".into(),
        )
        .into_response();

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let raw_body = String::from_utf8(body.to_vec()).unwrap();
        let body: serde_json::Value = serde_json::from_str(&raw_body).unwrap();

        assert_eq!(body["error"], "QUERY_FAILED");
        assert_eq!(body["message"], "query execution failed");
        assert!(!raw_body.contains("bucket/private"));
        assert!(!raw_body.contains("secret.table"));
    }
}
