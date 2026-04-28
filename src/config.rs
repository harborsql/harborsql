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
pub const DEFAULT_SKIP_PARTIAL_AGGREGATION_PROBE_ROWS_THRESHOLD: usize = 10_000;
pub const DEFAULT_SKIP_PARTIAL_AGGREGATION_PROBE_RATIO_THRESHOLD: f64 = 0.8;
pub const DEFAULT_TABLE_CACHE_TTL_SECONDS: u64 = 300;
pub const DEFAULT_TABLE_CACHE_MAX_ENTRIES: usize = 1024;
pub const TABLE_CACHE_CREDENTIAL_EXPIRY_SKEW_SECONDS: u64 = 60;
pub const DEFAULT_UNSAFE_ALLOW_HTTP_DATABRICKS_HOST: bool = false;

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
    pub skip_partial_aggregation_probe_rows_threshold: usize,
    pub skip_partial_aggregation_probe_ratio_threshold: f64,
    pub table_cache_ttl: Duration,
    pub table_cache_max_entries: usize,
    pub table_cache_credential_expiry_skew: Duration,
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
        let unsafe_allow_http_databricks_host = parse_bool_env(
            "HARBORSQL_UNSAFE_ALLOW_HTTP_DATABRICKS_HOST",
            DEFAULT_UNSAFE_ALLOW_HTTP_DATABRICKS_HOST,
        )?;
        let default_target_partitions = default_target_partitions();

        Ok(Self {
            bind_addr,
            databricks_host: normalize_host(&databricks_host, unsafe_allow_http_databricks_host)?,
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
            skip_partial_aggregation_probe_rows_threshold: parse_usize_env(
                "HARBORSQL_SKIP_PARTIAL_AGGREGATION_PROBE_ROWS_THRESHOLD",
                DEFAULT_SKIP_PARTIAL_AGGREGATION_PROBE_ROWS_THRESHOLD,
            )?,
            skip_partial_aggregation_probe_ratio_threshold: parse_ratio_env(
                "HARBORSQL_SKIP_PARTIAL_AGGREGATION_PROBE_RATIO_THRESHOLD",
                DEFAULT_SKIP_PARTIAL_AGGREGATION_PROBE_RATIO_THRESHOLD,
            )?,
            table_cache_ttl: parse_duration_seconds_env_allow_zero(
                "HARBORSQL_TABLE_CACHE_TTL_SECONDS",
                DEFAULT_TABLE_CACHE_TTL_SECONDS,
            )?,
            table_cache_max_entries: parse_usize_env_allow_zero(
                "HARBORSQL_TABLE_CACHE_MAX_ENTRIES",
                DEFAULT_TABLE_CACHE_MAX_ENTRIES,
            )?,
            table_cache_credential_expiry_skew: Duration::from_secs(
                TABLE_CACHE_CREDENTIAL_EXPIRY_SKEW_SECONDS,
            ),
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

fn parse_usize_env_allow_zero(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|err| HarborError::Config(format!("invalid {name}: {err}"))),
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

fn parse_duration_seconds_env_allow_zero(name: &str, default: u64) -> Result<Duration> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map(Duration::from_secs)
            .map_err(|err| HarborError::Config(format!("invalid {name}: {err}"))),
        Err(_) => Ok(Duration::from_secs(default)),
    }
}

fn parse_bool_env(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) => parse_bool_value(name, &value),
        Err(_) => Ok(default),
    }
}

fn parse_ratio_env(name: &str, default: f64) -> Result<f64> {
    match env::var(name) {
        Ok(value) => {
            let parsed: f64 = value
                .parse()
                .map_err(|err| HarborError::Config(format!("invalid {name}: {err}")))?;
            if !(0.0..=1.0).contains(&parsed) {
                return Err(HarborError::Config(format!(
                    "{name} must be between 0 and 1"
                )));
            }
            Ok(parsed)
        }
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

fn normalize_host(host: &str, unsafe_allow_http: bool) -> Result<String> {
    let trimmed = host.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(HarborError::Config(
            "Databricks host cannot be empty".into(),
        ));
    }

    if trimmed.starts_with("https://") {
        Ok(trimmed.to_string())
    } else if trimmed.starts_with("http://") {
        if unsafe_allow_http {
            Ok(trimmed.to_string())
        } else {
            Err(HarborError::Config(
                "Databricks host must use https://; set HARBORSQL_UNSAFE_ALLOW_HTTP_DATABRICKS_HOST=true only for local development against a non-Databricks test endpoint".into(),
            ))
        }
    } else if trimmed.contains("://") {
        Err(HarborError::Config(
            "Databricks host must use https://".into(),
        ))
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

    #[test]
    fn rejects_invalid_ratio_values() {
        temp_remove_var("HARBORSQL_TEST_RATIO");
        temp_set_var("HARBORSQL_TEST_RATIO", "1.2");
        assert!(parse_ratio_env("HARBORSQL_TEST_RATIO", 0.8).is_err());
        temp_set_var("HARBORSQL_TEST_RATIO", "-0.1");
        assert!(parse_ratio_env("HARBORSQL_TEST_RATIO", 0.8).is_err());
        temp_remove_var("HARBORSQL_TEST_RATIO");
    }

    #[test]
    fn databricks_hosts_default_to_https() {
        assert_eq!(
            normalize_host("workspace.cloud.databricks.com/", false).unwrap(),
            "https://workspace.cloud.databricks.com"
        );
        assert_eq!(
            normalize_host("https://workspace.cloud.databricks.com/", false).unwrap(),
            "https://workspace.cloud.databricks.com"
        );
    }

    #[test]
    fn databricks_hosts_reject_http_without_explicit_unsafe_opt_in() {
        assert!(normalize_host("http://127.0.0.1:8080", false).is_err());
        assert_eq!(
            normalize_host("http://127.0.0.1:8080", true).unwrap(),
            "http://127.0.0.1:8080"
        );
    }

    #[test]
    fn databricks_hosts_reject_other_url_schemes() {
        assert!(normalize_host("ftp://workspace.cloud.databricks.com", false).is_err());
    }

    #[test]
    fn cache_config_allows_zero_to_disable_cache() {
        temp_set_var("HARBORSQL_TEST_TABLE_CACHE_TTL_SECONDS", "0");
        temp_set_var("HARBORSQL_TEST_TABLE_CACHE_MAX_ENTRIES", "0");

        assert_eq!(
            parse_duration_seconds_env_allow_zero("HARBORSQL_TEST_TABLE_CACHE_TTL_SECONDS", 300)
                .unwrap(),
            Duration::ZERO
        );
        assert_eq!(
            parse_usize_env_allow_zero("HARBORSQL_TEST_TABLE_CACHE_MAX_ENTRIES", 1024).unwrap(),
            0
        );

        temp_remove_var("HARBORSQL_TEST_TABLE_CACHE_TTL_SECONDS");
        temp_remove_var("HARBORSQL_TEST_TABLE_CACHE_MAX_ENTRIES");
    }

    fn temp_set_var(key: &str, value: &str) {
        unsafe {
            env::set_var(key, value);
        }
    }

    fn temp_remove_var(key: &str) {
        unsafe {
            env::remove_var(key);
        }
    }
}
