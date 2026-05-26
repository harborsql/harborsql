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

    pub async fn catalogs(&self, bearer_token: &str) -> Result<Vec<CatalogInfo>> {
        let mut catalogs = Vec::new();
        let mut page_token = None;
        loop {
            let mut url = format!("{}/api/2.1/unity-catalog/catalogs?max_results=0", self.host);
            append_page_token(&mut url, page_token.as_deref());
            let response: ListCatalogsResponse = self.get(bearer_token, &url).await?;
            catalogs.extend(response.catalogs);
            let Some(next_page_token) = response.next_page_token.filter(|token| !token.is_empty())
            else {
                break;
            };
            page_token = Some(next_page_token);
        }
        Ok(catalogs)
    }

    pub async fn schemas(&self, bearer_token: &str, catalog_name: &str) -> Result<Vec<SchemaInfo>> {
        let encoded_catalog = urlencoding::encode(catalog_name);
        let mut schemas = Vec::new();
        let mut page_token = None;
        loop {
            let mut url = format!(
                "{}/api/2.1/unity-catalog/schemas?catalog_name={encoded_catalog}&max_results=0",
                self.host
            );
            append_page_token(&mut url, page_token.as_deref());
            let response: ListSchemasResponse = self.get(bearer_token, &url).await?;
            schemas.extend(response.schemas);
            let Some(next_page_token) = response.next_page_token.filter(|token| !token.is_empty())
            else {
                break;
            };
            page_token = Some(next_page_token);
        }
        Ok(schemas)
    }

    pub async fn tables(
        &self,
        bearer_token: &str,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<TableInfo>> {
        let encoded_catalog = urlencoding::encode(catalog_name);
        let encoded_schema = urlencoding::encode(schema_name);
        let mut tables = Vec::new();
        let mut page_token = None;
        loop {
            let mut url = format!(
                "{}/api/2.1/unity-catalog/tables?catalog_name={encoded_catalog}&schema_name={encoded_schema}&max_results=0&omit_columns=true&omit_properties=true&omit_username=true",
                self.host
            );
            append_page_token(&mut url, page_token.as_deref());
            let response: ListTablesResponse = self.get(bearer_token, &url).await?;
            tables.extend(response.tables);
            let Some(next_page_token) = response.next_page_token.filter(|token| !token.is_empty())
            else {
                break;
            };
            page_token = Some(next_page_token);
        }
        Ok(tables)
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

fn append_page_token(url: &mut String, page_token: Option<&str>) {
    if let Some(page_token) = page_token {
        url.push_str("&page_token=");
        url.push_str(&urlencoding::encode(page_token));
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
    pub table_id: Option<String>,
    pub full_name: String,
    pub name: Option<String>,
    pub catalog_name: Option<String>,
    pub schema_name: Option<String>,
    pub table_type: Option<String>,
    pub data_source_format: Option<String>,
    pub storage_location: Option<String>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CatalogInfo {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct SchemaInfo {
    pub name: String,
    pub full_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListCatalogsResponse {
    catalogs: Vec<CatalogInfo>,
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListSchemasResponse {
    schemas: Vec<SchemaInfo>,
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListTablesResponse {
    tables: Vec<TableInfo>,
    next_page_token: Option<String>,
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

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use axum::{
        Router,
        extract::State,
        http::{HeaderMap, StatusCode, Uri, header},
        response::{IntoResponse, Response},
        routing::get,
    };
    use tokio::{net::TcpListener, task::JoinHandle};

    use super::*;

    #[tokio::test]
    async fn list_catalogs_forwards_authorization_across_pages() {
        let server = TestUnityServer::new(None).await;
        let client = UnityCatalogClient::new(server.host.clone(), Duration::from_secs(5));

        let catalogs = client.catalogs("page-token").await.unwrap();

        assert_eq!(
            catalogs
                .iter()
                .map(|catalog| catalog.name.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| { request.authorization.as_deref() == Some("Bearer page-token") })
        );
        assert_eq!(requests[0].query, "max_results=0");
        assert_eq!(requests[1].query, "max_results=0&page_token=next%20token");
    }

    #[tokio::test]
    async fn metadata_requests_encode_names_and_include_expected_query_parameters() {
        let server = TestUnityServer::new(None).await;
        let client = UnityCatalogClient::new(server.host.clone(), Duration::from_secs(5));

        client
            .schemas("query-token", "main catalog/finance")
            .await
            .unwrap();
        client
            .tables("query-token", "main.catalog", "sales schema/2026")
            .await
            .unwrap();
        client
            .table("query-token", "main.sales.table/name")
            .await
            .unwrap();

        let requests = server.requests();
        assert_eq!(requests.len(), 3);
        assert!(
            requests
                .iter()
                .all(|request| { request.authorization.as_deref() == Some("Bearer query-token") })
        );
        assert_eq!(requests[0].path, "/api/2.1/unity-catalog/schemas");
        assert!(
            requests[0]
                .query
                .contains("catalog_name=main%20catalog%2Ffinance")
        );
        assert!(requests[0].query.contains("max_results=0"));

        assert_eq!(requests[1].path, "/api/2.1/unity-catalog/tables");
        assert!(requests[1].query.contains("catalog_name=main.catalog"));
        assert!(
            requests[1]
                .query
                .contains("schema_name=sales%20schema%2F2026")
        );
        assert!(requests[1].query.contains("omit_columns=true"));
        assert!(requests[1].query.contains("omit_properties=true"));
        assert!(requests[1].query.contains("omit_username=true"));

        assert_eq!(
            requests[2].path,
            "/api/2.1/unity-catalog/tables/main.sales.table%2Fname"
        );
        assert!(requests[2].query.contains("include_delta_metadata=true"));
        assert!(
            requests[2]
                .query
                .contains("include_manifest_capabilities=true")
        );
    }

    #[tokio::test]
    async fn unity_errors_are_redacted_and_truncated() {
        let long_message = format!(
            "denied at https://workspace.example.com/private/path with authorization: bearer secret-token {}",
            "x".repeat(800)
        );
        let server = TestUnityServer::new(Some((
            StatusCode::FORBIDDEN,
            format!(r#"{{"error_code":"PERMISSION_DENIED","message":"{long_message}"}}"#),
        )))
        .await;
        let client = UnityCatalogClient::new(server.host.clone(), Duration::from_secs(5));

        let err = client.catalogs("secret-token").await.unwrap_err();

        let HarborError::Unity(message) = err else {
            panic!("expected Unity error");
        };
        assert!(message.contains("PERMISSION_DENIED"));
        assert!(message.contains("[REDACTED_URL]"));
        assert!(!message.contains("workspace.example.com"));
        assert!(!message.contains("secret-token"));
        assert!(message.len() <= 603, "{message}");
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecordedRequest {
        path: String,
        query: String,
        authorization: Option<String>,
    }

    struct TestUnityState {
        requests: Mutex<Vec<RecordedRequest>>,
        error_response: Option<(StatusCode, String)>,
    }

    struct TestUnityServer {
        host: String,
        state: Arc<TestUnityState>,
        task: JoinHandle<()>,
    }

    impl TestUnityServer {
        async fn new(error_response: Option<(StatusCode, String)>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let state = Arc::new(TestUnityState {
                requests: Mutex::new(Vec::new()),
                error_response,
            });
            let app = Router::new()
                .route("/{*path}", get(unity_test_handler))
                .with_state(state.clone());
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self {
                host: format!("http://{}", socket_addr(addr)),
                state,
                task,
            }
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            self.state.requests.lock().unwrap().clone()
        }
    }

    impl Drop for TestUnityServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn unity_test_handler(
        State(state): State<Arc<TestUnityState>>,
        headers: HeaderMap,
        uri: Uri,
    ) -> Response {
        let path = uri.path().to_string();
        let query = uri.query().unwrap_or("").to_string();
        let authorization = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        state.requests.lock().unwrap().push(RecordedRequest {
            path: path.clone(),
            query: query.clone(),
            authorization,
        });

        if let Some((status, body)) = &state.error_response {
            return json_response(*status, body.clone());
        }

        match path.as_str() {
            "/api/2.1/unity-catalog/catalogs" => {
                if query.contains("page_token=next%20token") {
                    json_response(StatusCode::OK, r#"{"catalogs":[{"name":"second"}]}"#.into())
                } else {
                    json_response(
                        StatusCode::OK,
                        r#"{"catalogs":[{"name":"first"}],"next_page_token":"next token"}"#.into(),
                    )
                }
            }
            "/api/2.1/unity-catalog/schemas" => json_response(
                StatusCode::OK,
                r#"{"schemas":[{"name":"sales","full_name":"main.sales"}]}"#.into(),
            ),
            "/api/2.1/unity-catalog/tables" => json_response(
                StatusCode::OK,
                r#"{"tables":[{"full_name":"main.sales.fact_sales","name":"fact_sales","table_type":"MANAGED"}]}"#.into(),
            ),
            path if path.starts_with("/api/2.1/unity-catalog/tables/") => json_response(
                StatusCode::OK,
                r#"{"table_id":"table-id","full_name":"main.sales.fact_sales","name":"fact_sales","table_type":"MANAGED","data_source_format":"DELTA"}"#.into(),
            ),
            _ => json_response(StatusCode::NOT_FOUND, r#"{"message":"not found"}"#.into()),
        }
    }

    fn json_response(status: StatusCode, body: String) -> Response {
        (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
    }

    fn socket_addr(addr: SocketAddr) -> String {
        format!("{}:{}", addr.ip(), addr.port())
    }
}
