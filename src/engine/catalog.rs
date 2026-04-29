use std::{
    any::Any,
    collections::{BTreeSet, HashMap},
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};

use async_trait::async_trait;
use datafusion::{
    catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider},
    error::DataFusionError,
};
use deltalake::{logstore::LogStore, open_table_with_storage_options};
use object_store::{ObjectStore, path::Path as ObjectPath};
use tracing::Instrument;
use url::Url;

use crate::{
    config::Config,
    error::{HarborError, Result},
    observability,
    table_cache::{CachedTable, TableCache, expires_at_from_unity_expiration_ms},
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
        let started = Instant::now();
        let identifier_hash = observability::stable_hash(full_name);
        let result = UnityCatalogClient::table(self, bearer_token, full_name)
            .instrument(tracing::info_span!(
                "unity_call",
                stage = "table",
                identifier_hash = %identifier_hash
            ))
            .await;
        observe_unity_call("unity_table", full_name, started, &result);
        result
    }

    async fn temporary_table_credentials(
        &self,
        bearer_token: &str,
        table_id: &str,
    ) -> Result<TemporaryTableCredentials> {
        let started = Instant::now();
        let identifier_hash = observability::stable_hash(table_id);
        let result = UnityCatalogClient::temporary_table_credentials(self, bearer_token, table_id)
            .instrument(tracing::info_span!(
                "unity_call",
                stage = "temporary_table_credentials",
                identifier_hash = %identifier_hash
            ))
            .await;
        observe_unity_call("unity_credentials", table_id, started, &result);
        result
    }
}

#[derive(Clone)]
pub(super) struct UnityCatalogProviderList {
    unity: Arc<dyn UnityCatalog>,
    table_opener: Arc<dyn TableOpener>,
    config: Config,
    bearer_token: Arc<str>,
    table_cache: TableCache,
    routes: ObjectStoreRouteRegistry,
    catalogs: Arc<Mutex<HashMap<String, Arc<dyn CatalogProvider>>>>,
}

impl UnityCatalogProviderList {
    pub(super) fn new(
        unity: Arc<dyn UnityCatalog>,
        table_opener: Arc<dyn TableOpener>,
        config: Config,
        bearer_token: &str,
        table_cache: TableCache,
        routes: ObjectStoreRouteRegistry,
    ) -> Self {
        Self {
            unity,
            table_opener,
            config,
            bearer_token: Arc::from(bearer_token),
            table_cache,
            routes,
            catalogs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl fmt::Debug for UnityCatalogProviderList {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnityCatalogProviderList")
            .field("catalogs", &self.catalog_names())
            .finish_non_exhaustive()
    }
}

impl CatalogProviderList for UnityCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        lock_unchecked(&self.catalogs).insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        lock_unchecked(&self.catalogs).keys().cloned().collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let mut catalogs = lock_unchecked(&self.catalogs);
        if let Some(catalog) = catalogs.get(name) {
            return Some(catalog.clone());
        }

        let catalog = Arc::new(UnityCatalogProvider {
            catalog_name: name.to_string(),
            unity: self.unity.clone(),
            table_opener: self.table_opener.clone(),
            config: self.config.clone(),
            bearer_token: self.bearer_token.clone(),
            table_cache: self.table_cache.clone(),
            routes: self.routes.clone(),
            schemas: Mutex::new(HashMap::new()),
        });
        catalogs.insert(name.to_string(), catalog.clone());
        Some(catalog)
    }
}

struct UnityCatalogProvider {
    catalog_name: String,
    unity: Arc<dyn UnityCatalog>,
    table_opener: Arc<dyn TableOpener>,
    config: Config,
    bearer_token: Arc<str>,
    table_cache: TableCache,
    routes: ObjectStoreRouteRegistry,
    schemas: Mutex<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl fmt::Debug for UnityCatalogProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnityCatalogProvider")
            .field("catalog_name", &self.catalog_name)
            .field("schemas", &self.schema_names())
            .finish_non_exhaustive()
    }
}

impl CatalogProvider for UnityCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        lock_unchecked(&self.schemas).keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let mut schemas = lock_unchecked(&self.schemas);
        if let Some(schema) = schemas.get(name) {
            return Some(schema.clone());
        }

        let schema = Arc::new(UnitySchemaProvider {
            catalog_name: self.catalog_name.clone(),
            schema_name: name.to_string(),
            unity: self.unity.clone(),
            table_opener: self.table_opener.clone(),
            config: self.config.clone(),
            bearer_token: self.bearer_token.clone(),
            table_cache: self.table_cache.clone(),
            routes: self.routes.clone(),
            known_tables: Mutex::new(BTreeSet::new()),
        });
        schemas.insert(name.to_string(), schema.clone());
        Some(schema)
    }
}

struct UnitySchemaProvider {
    catalog_name: String,
    schema_name: String,
    unity: Arc<dyn UnityCatalog>,
    table_opener: Arc<dyn TableOpener>,
    config: Config,
    bearer_token: Arc<str>,
    table_cache: TableCache,
    routes: ObjectStoreRouteRegistry,
    known_tables: Mutex<BTreeSet<String>>,
}

impl fmt::Debug for UnitySchemaProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnitySchemaProvider")
            .field("catalog_name", &self.catalog_name)
            .field("schema_name", &self.schema_name)
            .field("known_tables", &self.table_names())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for UnitySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        lock_unchecked(&self.known_tables).iter().cloned().collect()
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let full_name = format!("{}.{}.{}", self.catalog_name, self.schema_name, name);
        let unity = self.unity.clone();
        let table_opener = self.table_opener.clone();
        let config = self.config.clone();
        let bearer_token = self.bearer_token.clone();
        let bearer_token_for_load = bearer_token.clone();
        let full_name_for_load = full_name.clone();
        let cached_table = self
            .table_cache
            .get_or_load(
                &bearer_token,
                &full_name,
                &self.config.aws_region,
                || async move {
                    load_cached_table(
                        unity,
                        table_opener,
                        config,
                        &bearer_token_for_load,
                        &full_name_for_load,
                    )
                    .await
                },
            )
            .await
            .map_err(to_datafusion_error)?;

        self.routes
            .record(&cached_table)
            .map_err(to_datafusion_error)?;
        lock_unchecked(&self.known_tables).insert(name.to_string());
        Ok(Some(cached_table.provider.clone()))
    }

    fn table_exist(&self, name: &str) -> bool {
        lock_unchecked(&self.known_tables).contains(name)
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
        let started = Instant::now();
        let table_url_hash = observability::stable_hash(&credentials.url);
        let result: Result<CachedTable> = async {
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
        .instrument(tracing::info_span!(
            "delta_open",
            table_url_hash = %table_url_hash
        ))
        .await;

        let duration = started.elapsed();
        let metrics = observability::get().metrics();
        metrics.observe_duration("delta_open", duration);
        match &result {
            Ok(_) => metrics.increment("harborsql_delta_open_succeeded_total"),
            Err(error) => {
                metrics.increment("harborsql_delta_open_failed_total");
                tracing::warn!(
                    table_url_hash = %table_url_hash,
                    duration_ms = duration.as_millis() as u64,
                    internal_error = %error.redacted_internal_message(),
                    "Delta table open failed"
                );
            }
        }
        if result.is_ok() {
            tracing::info!(
                table_url_hash = %table_url_hash,
                duration_ms = duration.as_millis() as u64,
                "Delta table opened"
            );
        }
        result
    }
}

fn observe_unity_call<T>(
    stage: &'static str,
    identifier: &str,
    started: Instant,
    result: &Result<T>,
) {
    let duration = started.elapsed();
    let metrics = observability::get().metrics();
    metrics.observe_duration(stage, duration);
    match result {
        Ok(_) => metrics.increment("harborsql_unity_requests_succeeded_total"),
        Err(error) => {
            metrics.increment("harborsql_unity_requests_failed_total");
            tracing::warn!(
                stage,
                identifier_hash = %observability::stable_hash(identifier),
                duration_ms = duration.as_millis() as u64,
                internal_error = %error.redacted_internal_message(),
                "Unity Catalog call failed"
            );
        }
    }
    if result.is_ok() {
        tracing::info!(
            stage,
            identifier_hash = %observability::stable_hash(identifier),
            duration_ms = duration.as_millis() as u64,
            "Unity Catalog call completed"
        );
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

#[derive(Clone)]
pub(super) struct ObjectStoreRoute {
    pub(super) prefix: String,
    pub(super) store: Arc<dyn ObjectStore>,
}

impl fmt::Debug for ObjectStoreRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectStoreRoute")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Default)]
pub(super) struct ObjectStoreRouteRegistry {
    routes: Arc<Mutex<ObjectStoreRoutes>>,
}

type ObjectStoreRoutes = HashMap<String, (Url, Vec<ObjectStoreRoute>)>;

impl ObjectStoreRouteRegistry {
    pub(super) fn record(&self, cached_table: &CachedTable) -> Result<()> {
        lock_checked(&self.routes)?
            .entry(cached_table.object_store_url.to_string())
            .or_insert_with(|| (cached_table.object_store_url.clone(), Vec::new()))
            .1
            .push(ObjectStoreRoute {
                prefix: cached_table.object_prefix.clone(),
                store: cached_table.object_store.clone(),
            });
        Ok(())
    }

    pub(super) fn routes(&self) -> Result<Vec<(Url, Vec<ObjectStoreRoute>)>> {
        Ok(lock_checked(&self.routes)?.values().cloned().collect())
    }
}

fn to_datafusion_error(error: HarborError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn lock_checked<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| HarborError::Query("Unity catalog provider lock was poisoned".into()))
}

fn lock_unchecked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("Unity catalog provider lock should not be poisoned")
}
