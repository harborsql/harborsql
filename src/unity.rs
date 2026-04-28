use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};

use crate::error::{HarborError, Result, redact_and_truncate};

#[derive(Clone)]
pub struct UnityCatalogClient {
    http: Client,
    host: String,
}

impl UnityCatalogClient {
    pub fn new(host: String, request_timeout: Duration) -> Self {
        Self {
            http: Client::builder()
                .timeout(request_timeout)
                .connect_timeout(request_timeout.min(Duration::from_secs(10)))
                .build()
                .expect("Unity Catalog HTTP client configuration should be valid"),
            host,
        }
    }

    pub async fn table(&self, bearer_token: &str, full_name: &str) -> Result<TableInfo> {
        let encoded = urlencoding::encode(full_name);
        let url = format!(
            "{}/api/2.1/unity-catalog/tables/{}?include_delta_metadata=true&include_manifest_capabilities=true",
            self.host, encoded
        );
        self.get(bearer_token, &url).await
    }

    pub async fn temporary_table_credentials(
        &self,
        bearer_token: &str,
        table_id: &str,
    ) -> Result<TemporaryTableCredentials> {
        let url = format!(
            "{}/api/2.1/unity-catalog/temporary-table-credentials",
            self.host
        );
        self.post(
            bearer_token,
            &url,
            &TemporaryTableCredentialsRequest {
                table_id,
                operation: "READ",
            },
        )
        .await
    }

    async fn get<T>(&self, bearer_token: &str, url: &str) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = self.http.get(url).bearer_auth(bearer_token).send().await?;
        decode_response(response).await
    }

    async fn post<T, B>(&self, bearer_token: &str, url: &str, body: &B) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
        B: Serialize + ?Sized,
    {
        let response = self
            .http
            .post(url)
            .bearer_auth(bearer_token)
            .json(body)
            .send()
            .await?;
        decode_response(response).await
    }
}

async fn decode_response<T>(response: reqwest::Response) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status();
    if status == StatusCode::OK {
        return response.json::<T>().await.map_err(Into::into);
    }

    let body = response.text().await.unwrap_or_default();
    if let Ok(error) = serde_json::from_str::<DatabricksError>(&body) {
        let detail = format!(
            "{}: {}",
            error.error_code.unwrap_or_else(|| status.to_string()),
            error.message
        );
        return Err(HarborError::Unity(redact_and_truncate(&detail, 600)));
    }
    let detail = format!("HTTP {} from Unity Catalog: {}", status, body);
    Err(HarborError::Unity(redact_and_truncate(&detail, 600)))
}

#[derive(Debug, Deserialize)]
struct DatabricksError {
    error_code: Option<String>,
    message: String,
}

#[derive(Debug, Deserialize)]
pub struct TableInfo {
    pub table_id: String,
    pub full_name: String,
    pub data_source_format: Option<String>,
    pub storage_location: Option<String>,
}

#[derive(Serialize)]
struct TemporaryTableCredentialsRequest<'a> {
    table_id: &'a str,
    operation: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct TemporaryTableCredentials {
    pub aws_temp_credentials: AwsTempCredentials,
    pub expiration_time: i64,
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct AwsTempCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}
