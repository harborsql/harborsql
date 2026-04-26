use std::{env, net::SocketAddr};

use crate::error::{HarborError, Result};

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub databricks_host: String,
    pub default_catalog: String,
    pub default_schema: String,
    pub aws_region: String,
    pub max_result_rows: Option<usize>,
    pub max_result_bytes: Option<usize>,
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
            max_result_rows: parse_optional_usize_env("HARBORSQL_MAX_RESULT_ROWS")?,
            max_result_bytes: parse_optional_usize_env("HARBORSQL_MAX_RESULT_BYTES")?,
        })
    }
}

fn parse_optional_usize_env(name: &str) -> Result<Option<usize>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(|err| HarborError::Config(format!("invalid {name}: {err}"))),
        Err(_) => Ok(None),
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
