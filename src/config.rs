use std::{env, net::SocketAddr, time::Duration};

use crate::error::{HarborError, Result};

pub const DEFAULT_MAX_RESULT_ROWS: usize = 100_000;
pub const DEFAULT_MAX_RESULT_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_UNITY_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_QUERY_TIMEOUT_SECONDS: u64 = 300;
pub const DEFAULT_IDLE_SESSION_TIMEOUT_SECONDS: u64 = 30 * 60;
pub const DEFAULT_COMPLETED_OPERATION_TTL_SECONDS: u64 = 10 * 60;
pub const DEFAULT_CLEANUP_INTERVAL_SECONDS: u64 = 60;
pub const DEFAULT_MAX_SESSIONS: usize = 256;
pub const DEFAULT_MAX_OPERATIONS: usize = 512;
pub const DEFAULT_REQUEST_BODY_LIMIT_BYTES: usize = 1024 * 1024;
pub const DEFAULT_PARQUET_PUSHDOWN_FILTERS: bool = true;
pub const DEFAULT_MIN_TARGET_PARTITIONS: usize = 32;

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub databricks_host: String,
    pub default_catalog: String,
    pub default_schema: String,
    pub aws_region: String,
    pub max_result_rows: Option<usize>,
    pub max_result_bytes: Option<usize>,
    pub unity_request_timeout: Duration,
    pub query_timeout: Duration,
    pub idle_session_timeout: Duration,
    pub completed_operation_ttl: Duration,
    pub cleanup_interval: Duration,
    pub max_sessions: usize,
    pub max_operations: usize,
    pub request_body_limit_bytes: usize,
    pub parquet_pushdown_filters: bool,
    pub parquet_reorder_filters: bool,
    pub target_partitions: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env::var("HARBORSQL_BIND_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:1992".to_string())
            .parse()
            .map_err(|err| HarborError::Config(format!("invalid HARBORSQL_BIND_ADDR: {err}")))?;

        let databricks_host = env::var("HARBORSQL_DATABRICKS_HOST")
            .or_else(|_| env::var("DATABRICKS_HOST"))
            .map_err(|_| {
                HarborError::Config(
                    "HARBORSQL_DATABRICKS_HOST or DATABRICKS_HOST is required".into(),
                )
            })?;
        let parquet_pushdown_filters = parse_bool_env(
            "HARBORSQL_PARQUET_PUSHDOWN_FILTERS",
            DEFAULT_PARQUET_PUSHDOWN_FILTERS,
        )?;
        let parquet_reorder_filters = parse_bool_env(
            "HARBORSQL_PARQUET_REORDER_FILTERS",
            parquet_pushdown_filters,
        )?;
        let default_target_partitions = default_target_partitions();

        Ok(Self {
            bind_addr,
            databricks_host: normalize_host(&databricks_host)?,
            default_catalog: env::var("HARBORSQL_DEFAULT_CATALOG")
                .or_else(|_| env::var("DATABRICKS_CATALOG"))
                .unwrap_or_else(|_| "workspace".into()),
            default_schema: env::var("HARBORSQL_DEFAULT_SCHEMA")
                .or_else(|_| env::var("DATABRICKS_SCHEMA"))
                .unwrap_or_else(|_| "default".into()),
            aws_region: env::var("HARBORSQL_AWS_REGION").unwrap_or_else(|_| "us-west-2".into()),
            max_result_rows: parse_optional_usize_env(
                "HARBORSQL_MAX_RESULT_ROWS",
                Some(DEFAULT_MAX_RESULT_ROWS),
            )?,
            max_result_bytes: parse_optional_usize_env(
                "HARBORSQL_MAX_RESULT_BYTES",
                Some(DEFAULT_MAX_RESULT_BYTES),
            )?,
            unity_request_timeout: parse_duration_seconds_env(
                "HARBORSQL_UNITY_TIMEOUT_SECONDS",
                DEFAULT_UNITY_TIMEOUT_SECONDS,
            )?,
            query_timeout: parse_duration_seconds_env(
                "HARBORSQL_QUERY_TIMEOUT_SECONDS",
                DEFAULT_QUERY_TIMEOUT_SECONDS,
            )?,
            idle_session_timeout: parse_duration_seconds_env(
                "HARBORSQL_IDLE_SESSION_TIMEOUT_SECONDS",
                DEFAULT_IDLE_SESSION_TIMEOUT_SECONDS,
            )?,
            completed_operation_ttl: parse_duration_seconds_env(
                "HARBORSQL_COMPLETED_OPERATION_TTL_SECONDS",
                DEFAULT_COMPLETED_OPERATION_TTL_SECONDS,
            )?,
            cleanup_interval: parse_duration_seconds_env(
                "HARBORSQL_CLEANUP_INTERVAL_SECONDS",
                DEFAULT_CLEANUP_INTERVAL_SECONDS,
            )?,
            max_sessions: parse_usize_env("HARBORSQL_MAX_SESSIONS", DEFAULT_MAX_SESSIONS)?,
            max_operations: parse_usize_env("HARBORSQL_MAX_OPERATIONS", DEFAULT_MAX_OPERATIONS)?,
            request_body_limit_bytes: parse_usize_env(
                "HARBORSQL_REQUEST_BODY_LIMIT_BYTES",
                DEFAULT_REQUEST_BODY_LIMIT_BYTES,
            )?,
            parquet_pushdown_filters,
            parquet_reorder_filters,
            target_partitions: parse_usize_env(
                "HARBORSQL_TARGET_PARTITIONS",
                default_target_partitions,
            )?,
        })
    }
}

fn default_target_partitions() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get().max(DEFAULT_MIN_TARGET_PARTITIONS))
        .unwrap_or(DEFAULT_MIN_TARGET_PARTITIONS)
}

fn parse_optional_usize_env(name: &str, default: Option<usize>) -> Result<Option<usize>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(|err| HarborError::Config(format!("invalid {name}: {err}"))),
        Err(_) => Ok(default),
    }
}

fn parse_usize_env(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value
                .parse()
                .map_err(|err| HarborError::Config(format!("invalid {name}: {err}")))?;
            if parsed == 0 {
                return Err(HarborError::Config(format!(
                    "{name} must be greater than zero"
                )));
            }
            Ok(parsed)
        }
        Err(_) => Ok(default),
    }
}

fn parse_duration_seconds_env(name: &str, default: u64) -> Result<Duration> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value
                .parse()
                .map_err(|err| HarborError::Config(format!("invalid {name}: {err}")))?;
            if parsed == 0 {
                return Err(HarborError::Config(format!(
                    "{name} must be greater than zero"
                )));
            }
            Ok(Duration::from_secs(parsed))
        }
        Err(_) => Ok(Duration::from_secs(default)),
    }
}

fn parse_bool_env(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) => parse_bool_value(name, &value),
        Err(_) => Ok(default),
    }
}

fn parse_bool_value(name: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(HarborError::Config(format!(
            "invalid {name}: expected true/false"
        ))),
    }
}

fn normalize_host(host: &str) -> Result<String> {
    let trimmed = host.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(HarborError::Config(
            "Databricks host cannot be empty".into(),
        ));
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("https://{trimmed}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bool_values() {
        for value in ["1", "true", "yes", "on", "TRUE", " Yes "] {
            assert!(parse_bool_value("HARBORSQL_TEST_BOOL", value).unwrap());
        }

        for value in ["0", "false", "no", "off", "FALSE", " Off "] {
            assert!(!parse_bool_value("HARBORSQL_TEST_BOOL", value).unwrap());
        }
    }
}
