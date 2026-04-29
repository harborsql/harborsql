use std::{collections::HashMap, sync::Arc, time::Instant};

use async_trait::async_trait;
use deltalake::{logstore::LogStore, open_table_with_storage_options};
use object_store::path::Path as ObjectPath;
use url::Url;

use crate::{
    config::Config,
    error::{HarborError, Result},
    table_cache::{CachedTable, expires_at_from_unity_expiration_ms},
    unity::{TableInfo, TemporaryTableCredentials, UnityCatalogClient},
};

#[async_trait]
pub(super) trait UnityCatalog: Send + Sync {
    async fn table(&self, bearer_token: &str, full_name: &str) -> Result<TableInfo>;

    async fn temporary_table_credentials(
        &self,
        bearer_token: &str,
        table_id: &str,
    ) -> Result<TemporaryTableCredentials>;
}

#[async_trait]
impl UnityCatalog for UnityCatalogClient {
    async fn table(&self, bearer_token: &str, full_name: &str) -> Result<TableInfo> {
        UnityCatalogClient::table(self, bearer_token, full_name).await
    }

    async fn temporary_table_credentials(
        &self,
        bearer_token: &str,
        table_id: &str,
    ) -> Result<TemporaryTableCredentials> {
        UnityCatalogClient::temporary_table_credentials(self, bearer_token, table_id).await
    }
}

#[async_trait]
pub(super) trait TableOpener: Send + Sync {
    async fn open(
        &self,
        credentials: &TemporaryTableCredentials,
        aws_region: &str,
        credential_expires_at: Instant,
    ) -> Result<CachedTable>;
}

#[derive(Debug, Default)]
pub(super) struct DeltaTableOpener;

#[async_trait]
impl TableOpener for DeltaTableOpener {
    async fn open(
        &self,
        credentials: &TemporaryTableCredentials,
        aws_region: &str,
        credential_expires_at: Instant,
    ) -> Result<CachedTable> {
        let delta = open_table_with_storage_options(
            Url::parse(&credentials.url)?,
            storage_options(credentials, aws_region),
        )
        .await?;
        let (object_store_url, object_prefix) = table_object_store_route(&credentials.url)?;
        let object_store = delta.log_store().root_object_store(None);
        let provider = delta.table_provider().await?;

        Ok(CachedTable::new(
            provider,
            object_store_url,
            object_prefix,
            object_store,
            credential_expires_at,
        ))
    }
}

pub(super) async fn load_cached_table(
    unity: Arc<dyn UnityCatalog>,
    table_opener: Arc<dyn TableOpener>,
    config: Config,
    bearer_token: &str,
    full_name: &str,
) -> Result<CachedTable> {
    let table = unity.table(bearer_token, full_name).await?;
    ensure_delta_table(&table)?;

    let credentials = unity
        .temporary_table_credentials(bearer_token, &table.table_id)
        .await?;
    let credential_expires_at = expires_at_from_unity_expiration_ms(
        credentials.expiration_time,
        config.table_cache_credential_expiry_skew,
    );

    table_opener
        .open(&credentials, &config.aws_region, credential_expires_at)
        .await
}

pub(super) fn ensure_delta_table(table: &TableInfo) -> Result<()> {
    let format_ok = table
        .data_source_format
        .as_deref()
        .is_some_and(|format| format.eq_ignore_ascii_case("DELTA"));
    let storage_ok = table.storage_location.is_some();
    if format_ok && storage_ok {
        return Ok(());
    }

    Err(HarborError::UnsupportedSql(format!(
        "table {} is not an externally readable Delta table",
        table.full_name
    )))
}

fn storage_options(
    credentials: &TemporaryTableCredentials,
    aws_region: &str,
) -> HashMap<String, String> {
    HashMap::from([
        (
            "AWS_ACCESS_KEY_ID".to_string(),
            credentials.aws_temp_credentials.access_key_id.clone(),
        ),
        (
            "AWS_SECRET_ACCESS_KEY".to_string(),
            credentials.aws_temp_credentials.secret_access_key.clone(),
        ),
        (
            "AWS_SESSION_TOKEN".to_string(),
            credentials.aws_temp_credentials.session_token.clone(),
        ),
        ("AWS_REGION".to_string(), aws_region.to_string()),
    ])
}

pub(super) fn table_object_store_route(table_url: &str) -> Result<(Url, String)> {
    let parsed = Url::parse(table_url)?;
    if parsed.host_str().is_none() {
        return Err(HarborError::Query(format!(
            "table storage URL has no host: {table_url}"
        )));
    }

    let prefix = ObjectPath::from_url_path(parsed.path())
        .map_err(|err| HarborError::Query(format!("invalid table storage path: {err}")))?
        .as_ref()
        .to_string();

    let mut object_store_url = parsed;
    object_store_url.set_path("");
    object_store_url.set_query(None);
    object_store_url.set_fragment(None);

    Ok((object_store_url, prefix))
}
